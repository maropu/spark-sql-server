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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.SetCommand
import org.apache.spark.sql.server.SQLServerConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.catalyst.expressions.ParameterPlaceHolder
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{Utils => SparkUtils}


private class OperationImpl(
    sessionState: SessionState,
    // `query._1` is the query string that might have parameterized variables (`$1`, `$2`, ...)
    // and `query._2` is the logical plan that might have parameter holders
    // (`ParameterPlaceHolder`) for the variables.
    query: (String, LogicalPlan))(
    _statementId: String,
    catalogUpdater: (SQLContext, LogicalPlan) => Unit) extends Operation with Logging {

  import sessionState._

  private val sqlContext = sessionState._context match {
    case SQLContextHolder(ctx) => ctx
    case ctx => sys.error(s"${this.getClass.getSimpleName} cannot handle $ctx")
  }

  protected val useIncrementalCollect = sqlContext.conf.sqlServerIncrementalCollectEnabled

  // If `prepare` called, sets the result here
  private var _boundPlan: Option[LogicalPlan] = None

  private lazy val analyzedPlan: LogicalPlan = _boundPlan.getOrElse {
    sqlContext.sessionState.analyzer.execute(query._2)
  }

  override def statementId(): String = _statementId

  override def cancel(): Unit = {
    logInfo(
      s"""Cancelling query with $statementId:
         |Query:
         |${query._1}
         |Analyzed Plan:
         |$analyzedPlan
       """.stripMargin)
    sqlContext.sparkContext.cancelJobGroup(statementId)
    _servListener.foreach(_.onStatementCanceled(statementId))
    setState(CANCELED)
  }

  override def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    sqlContext.sparkContext.clearJobGroup()
    logDebug(s"CLOSING $statementId")
    setState(CLOSED)
  }

  override def outputSchema(): StructType = {
    require(!hasParamHolder(analyzedPlan))
    analyzedPlan.schema
  }

  private def hasParamHolder(plan: LogicalPlan): Boolean = {
    plan.collectFirst {
      case p if p.expressions.exists(_.isInstanceOf[ParameterPlaceHolder]) => p
    }.isDefined
  }

  override def prepare(params: Map[Int, Literal]): Unit = {
    // First, binds parameters in a logical plan then analyzes it
    val boundPlan = ParamBinder.bind(query._2, params)
    val analyzed = sqlContext.sessionState.analyzer.execute(boundPlan)
    _boundPlan = Some(analyzed)
  }

  private[this] var _cachedRowIterator: Iterator[InternalRow] = _

  protected def executeInternal(): DataFrame = {
    require(!hasParamHolder(analyzedPlan))

    setState(RUNNING)
    logInfo(
      s"""Running query with $statementId:
         |Query:
         |${query._1}
         |Analyzed Plan:
         |$analyzedPlan
       """.stripMargin)

    // Always uses the latest class loader provided by SQLContext's state.
    Thread.currentThread().setContextClassLoader(sqlContext.sharedState.jarClassLoader)

    _servListener.foreach(_.onStatementStart(statementId, _sessionId, query._1, statementId))
    sqlContext.sparkContext.setJobGroup(statementId, query._1, true)

    // Initializes a value in fair Scheduler Pools
    _schedulePool.foreach { pool =>
      sqlContext.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
    }

    val resultDf = try {
      val df = Dataset.ofRows(sqlContext.sparkSession, analyzedPlan)
      logDebug(df.queryExecution.toString())
      _servListener.foreach(_.onStatementParsed(statementId, df.queryExecution.toString()))

      // Updates configurations based on SET commands
      analyzedPlan match {
        case SetCommand(Some((SQLServerConf.SQLSERVER_POOL.key, Some(pool)))) =>
          logInfo(s"Setting spark.scheduler.pool=$pool for future statements in this session.")
          _schedulePool = Some(pool)
        case _ =>
      }

      // Updates an internal catalog based on DDL commands
      catalogUpdater(sqlContext, analyzedPlan)

      df
    } catch {
      case NonFatal(e) =>
        if (state == CANCELED) {
          val errMsg =
            s"""Cancelled query with $statementId
               |Query:
               |${query._1}
               |Analyzed Plan:
               |$analyzedPlan
             """.stripMargin
          logWarning(errMsg)
          throw new SQLException(errMsg)
        } else {
          setState(ERROR)
          val exceptionString = SparkUtils.exceptionString(e)
          val errMsg =
            s"""Caught an error executing query with with $statementId:
               |Query:
               |${query._1}
               |Analyzed Plan:
               |$analyzedPlan
               |Exception message:
               |$exceptionString
             """.stripMargin
          logError(errMsg)
          _servListener.foreach(_.onStatementError(statementId, e.getMessage, exceptionString))
          throw new SQLException(errMsg)
        }
    }

    _servListener.foreach(_.onStatementFinish(statementId))
    setState(FINISHED)

    resultDf
  }

  override def run(): Iterator[InternalRow] = {
    if (state == INITIALIZED) {
      val df = executeInternal()

      val resultRowIterator = if (useIncrementalCollect) {
        df.queryExecution.executedPlan.executeToIterator()
      } else {
        // Needs to use `List` so that `Iterator#take` can proceed an internal cursor, e.g.,
        //
        // scala> val iter = Array(1, 2, 3, 4, 5, 6).toIterator
        // scala> iter.take(1).next
        // res2: Int = 1
        // scala> iter.take(1).next
        // res3: Int = 1
        // ...
        // scala> val iter = Array(1, 2, 3, 4, 5, 6).toList.toIterator
        // scala> iter.take(1).next
        // res4: Int = 1
        // scala> iter.take(1).next
        // res5: Int = 2
        // ...
        df.queryExecution.executedPlan.executeCollect().toList.toIterator
      }

      _cachedRowIterator = resultRowIterator
      resultRowIterator
    } else {
      // Since this operation already has been done, just returns the cached result
      _cachedRowIterator
    }
  }
}

private[service] class ExecutorImpl(catalogUpdater: (SQLContext, LogicalPlan) => Unit)
    extends OperationExecutor {

  // Creates a new instance for service-specific operations
  override def newOperation(
      sessionState: SessionState,
      statementId: String,
      query: (String, LogicalPlan)): Operation = {
    new OperationImpl(sessionState, query)(statementId, catalogUpdater)
  }
}
