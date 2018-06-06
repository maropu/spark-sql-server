/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.server.service.livy

import scala.util.control.NonFatal

import org.apache.livy.{Job, JobContext}

import org.apache.spark.SparkEnv
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.server.{CustomOptimizerRuleInitializer, SQLServerEnv}
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.{ExecutorImpl, NOP, Operation, SessionContext, SessionState, SQLContextHolder}
import org.apache.spark.sql.server.service.postgresql.{PgCatalogInitializer, PgCatalogUpdater, PgSessionInitializer, PgUtils}
import org.apache.spark.sql.types.StructType


// Messages between a SQL server and a Livy job
case class SchemaRequest(sql: String)
case class SchemaResponse(schema: StructType)
case class PrepareRequest(statementId: String, sql: String, params: Map[Int, Literal])
case class PrepareResponse()
case class ExecuteExtendedQuery(statementId: String)
case class ExecuteSimpleQuery(statementId: String, sql: String)
case class ResultSetResponse(resultRows: Seq[InternalRow])
case class IncrementalCollectStart()
case class IncrementalCollectEnd()
case class RequestNextResultSet(statementId: String)
case class ErrorResponse(e: Throwable)
case class CancelRequest(statementId: String)
case class CancelResponse()

private class LivySessionState extends SessionState

private object LivySessionState {

  def apply(sessionId: Int, context: SessionContext): LivySessionState = {
    val state = new LivySessionState()
    state._sessionId = sessionId
    state._context = context
    // TODO: Needs to implement this?
    state._servListener = None
    state._uiTab = None
    state._schedulePool = None
    state
  }
}

private class ExecutorEndpoint(override val rpcEnv: RpcEnv, sessionState: LivySessionState)
    extends RpcEndpoint {

  private val executorImpl = {
    val catalogUpdater = (sqlContext: SQLContext, analyzedPlan: LogicalPlan) => {
      PgCatalogUpdater(sqlContext, analyzedPlan)
    }
    new ExecutorImpl(catalogUpdater)
  }

  private val sqlContext = sessionState._context match {
    case SQLContextHolder(ctx) => ctx
    case ctx => sys.error(s"${this.getClass.getSimpleName} cannot handle $ctx")
  }

  private val useIncrementalCollect = sqlContext.conf.sqlServerIncrementalCollectEnabled

  @volatile private var activeOperation: Operation = NOP

  private def analyzePlan(query: String): LogicalPlan = {
    val sesseionSpecificAnalyzer = sqlContext.sessionState.analyzer
    sesseionSpecificAnalyzer.execute(PgUtils.parse(query))
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SchemaRequest(sql) =>
      try { context.reply(SchemaResponse(analyzePlan(sql).schema)) } catch {
        case NonFatal(e) => context.reply(ErrorResponse(e))
      }

    case PrepareRequest(statementId, sql, params) =>
      try {
        val logicalPlan = PgUtils.parse(sql)
        activeOperation = executorImpl.newOperation(sessionState, statementId, (sql, logicalPlan))
        activeOperation.prepare(params)
        context.reply(PrepareResponse())
      } catch {
        case NonFatal(e) => context.reply(ErrorResponse(e))
      }

    case ExecuteExtendedQuery(statementId) =>
      require(statementId == activeOperation.statementId())
      try {
        if (useIncrementalCollect) {
          context.reply(IncrementalCollectStart())
        } else {
          val rowIter = activeOperation.run()
          // To make it serializable, uses `toArray`
          context.reply(ResultSetResponse(rowIter.toArray.toSeq))
        }
      } catch {
        case NonFatal(e) => context.reply(ErrorResponse(e))
      }

    case ExecuteSimpleQuery(statementId, sql) =>
      try {
        val logicalPlan = PgUtils.parse(sql)
        activeOperation = executorImpl.newOperation(sessionState, statementId, (sql, logicalPlan))
        if (useIncrementalCollect) {
          context.reply(IncrementalCollectStart())
        } else {
          val rowIter = activeOperation.run()
          // To make it serializable, uses `toArray`
          context.reply(ResultSetResponse(rowIter.toArray.toSeq))
        }
      } catch {
        case NonFatal(e) => context.reply(ErrorResponse(e))
      }

    case RequestNextResultSet(statementId) =>
      require(statementId == activeOperation.statementId())
      try {
        // TODO: Returns partition-by-partition instead of row-by-row
        val iter = activeOperation.run()
        if (iter.hasNext) {
          val result = iter.next()
          context.reply(ResultSetResponse(result :: Nil))
        } else {
          context.reply(IncrementalCollectEnd())
        }
      } catch {
        case NonFatal(e) => context.reply(ErrorResponse(e))
      }

    // TODO: Currently, the cancel request doesn't work well...
    case CancelRequest(statementId) =>
      if (statementId == activeOperation.statementId()) {
        activeOperation.cancel()
        context.reply(CancelResponse)
      } else {
        context.reply(ErrorResponse(new IllegalStateException(
          s"Failed to cancel a query with `$statementId`: " +
          s"the running query with `${activeOperation.statementId()}` found.")))
      }
  }
}

class OpenSessionJob(sessionId: Int, dbName: String) extends Job[RpcEndpointRef] {

  override def call(jobContext: JobContext): RpcEndpointRef = {
    val sqlContext = jobContext.sparkSession[SparkSession].sqlContext
    require(sqlContext != null, "SQLContext cannot be initialized")
    SQLServerEnv.withSQLContext(sqlContext)

    // Initializes custom optimizer rules
    CustomOptimizerRuleInitializer(sqlContext)

    // Initializes a catalog state for a SQL server
    PgCatalogInitializer(sqlContext)
    PgSessionInitializer(dbName, sqlContext)

    val rpcEnv = SparkEnv.get.rpcEnv
    val sessionState = LivySessionState(sessionId, SQLContextHolder(sqlContext))
    val endpoint = new ExecutorEndpoint(rpcEnv, sessionState)
    val endpointRef = rpcEnv.setupEndpoint(OpenSessionJob.ENDPOINT_NAME, endpoint)
    endpointRef
  }
}

object OpenSessionJob {

  val ENDPOINT_NAME = "sql-server-session"
}
