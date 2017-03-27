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

package org.apache.spark.sql.server.service

import java.sql.SQLException
import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.command.SetCommand
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.server.SQLServer
import org.apache.spark.sql.server.service.postgresql.{Metadata => PgMetadata}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{Utils => SparkUtils}

/** The states of an [[ExecuteStatementOperation]]. */
private[server] sealed trait OperationState
private[server] case object INITIALIZED extends OperationState
private[server] case object RUNNING extends OperationState
private[server] case object FINISHED extends OperationState
private[server] case object CANCELED extends OperationState
private[server] case object CLOSED extends OperationState
private[server] case object ERROR extends OperationState
private[server] case object UNKNOWN extends OperationState
private[server] case object PENDING extends OperationState

private[server] abstract class Operation(conf: SQLConf) {

  private val timeout = conf.sqlServerIdleOperationTimeout

  protected[this] var state: OperationState = INITIALIZED
  private var lastAccessTime: Long = System.currentTimeMillis()

  def run(): Unit
  def cancel(): Unit
  def close(): Unit

  protected[this] def setState(newState: OperationState): Unit = {
    lastAccessTime = System.currentTimeMillis()
    state = newState
  }

  private[service] def isTimeOut(current: Long): Boolean = {
    if (timeout == 0) {
      true
    } else if (timeout > 0) {
      Seq(FINISHED, CANCELED, CLOSED, ERROR).contains(state) &&
        lastAccessTime + timeout <= current
    } else {
      lastAccessTime + -timeout <= current
    }
  }
}

private[server] case class ExecuteStatementOperation(
    sessionId: Int,
    statement: String)
   (sqlContext: SQLContext,
    activePools: java.util.Map[Int, String]) extends Operation(sqlContext.conf) with Logging {

  private[service] val statementId = UUID.randomUUID().toString()

  private[service] var resultSet: DataFrame = _
  private[service] var rowIter: Iterator[InternalRow] = _

  override def cancel(): Unit = {
    logInfo(s"Cancelling '$statement' with $statementId")
    if (statementId != null) {
      sqlContext.sparkContext.cancelJobGroup(statementId)
    }
    setState(CANCELED)
    SQLServer.listener.onStatementCanceled(statementId)
  }

  def schema(): StructType = {
    Option(resultSet).map(_.schema).getOrElse(StructType(Seq.empty))
  }

  def iterator(): Iterator[InternalRow] = {
    Option(rowIter).getOrElse(Iterator.empty)
  }

  override def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    sqlContext.sparkContext.clearJobGroup()
    logDebug(s"CLOSING $statementId")
    setState(CLOSED)
  }

  override def run(): Unit = {
    logInfo(s"Running query '$statement' with $statementId")
    setState(RUNNING)

    // Always use the latest class loader provided by SQLContext's state.
    Thread.currentThread().setContextClassLoader(sqlContext.sharedState.jarClassLoader)

    SQLServer.listener.onStatementStart(statementId, sessionId, statement, statementId)
    sqlContext.sparkContext.setJobGroup(statementId, statement, true)

    if (activePools.containsKey(sessionId)) {
      val pool = activePools.get(sessionId)
      sqlContext.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
    }

    try {
      resultSet = sqlContext.sql(statement)
      logDebug(resultSet.queryExecution.toString())

      resultSet.queryExecution.logical match {
        case SetCommand(Some((SQLServerConf.SQLSERVER_POOL.key, Some(value)))) =>
          activePools.put(sessionId, value)
          logInfo(s"Setting spark.scheduler.pool=$value for future statements in this session.")
        case CreateTable(desc, _, _) =>
          PgMetadata.registerTableInCatalog(desc.identifier.table, desc.schema, sqlContext)
          logInfo(s"Registering ${desc.identifier.table} in a system catalog `pg_class`")
        case _ =>
      }

      SQLServer.listener.onStatementParsed(statementId, resultSet.queryExecution.toString())
      rowIter = {
        val useIncrementalCollect = sqlContext.conf.sqlServerIncrementalCollectEnabled
        if (useIncrementalCollect) {
          // resultSet.toLocalIterator.asScala
          throw new UnsupportedOperationException("`useIncrementalCollect` not supported yet")
        } else {
          resultSet.queryExecution.executedPlan.executeCollect().iterator
        }
      }
    } catch {
      case e: Throwable =>
        if (state != CANCELED) {
          logError(s"Error executing query, currentState $state, ", e)
          setState(ERROR)
          SQLServer.listener.onStatementError(
            statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw new SQLException(e.toString)
        } else {
          logWarning(s"Cancelled query '$statement' with $statementId")
          throw new SQLException(e.toString)
        }
    }
    setState(FINISHED)
    SQLServer.listener.onStatementFinish(statementId)
  }
}
