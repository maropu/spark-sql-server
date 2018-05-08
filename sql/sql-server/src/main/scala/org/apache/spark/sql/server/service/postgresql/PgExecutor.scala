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

package org.apache.spark.sql.server.service.postgresql

import java.security.PrivilegedExceptionAction
import java.sql.SQLException
import java.util.UUID

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.server.SQLServerConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service._
import org.apache.spark.sql.server.service.{Operation, OperationExecutor}
import org.apache.spark.util.{Utils => SparkUtils}


private[postgresql] case class PgOperation(
    sessionState: SessionState,
    query: (String, LogicalPlan)) extends Operation with Logging {
  import sessionState._

  private[this] val statementId = UUID.randomUUID().toString

  override def cancel(): Unit = {
    logInfo(
      s"""Cancelling query with $statementId:
         |Query:
         |${query._1}
         |Analyzed Plan:
         |${query._2}
       """.stripMargin)
    _sqlContext.sparkContext.cancelJobGroup(statementId)
    _servListener.onStatementCanceled(statementId)
    setState(CANCELED)
  }

  override def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    _sqlContext.sparkContext.clearJobGroup()
    logDebug(s"CLOSING $statementId")
    setState(CLOSED)
  }

  private[this] var _cachedRowIterator: Iterator[InternalRow] = _

  private def executeInternal(): Iterator[InternalRow] = {
    if (state == INITIALIZED) {
      setState(RUNNING)
      logInfo(
        s"""Running query with $statementId:
           |Query:
           |${query._1}
           |Analyzed Plan:
           |${query._2}
         """.stripMargin)

      // Always uses the latest class loader provided by SQLContext's state.
      Thread.currentThread().setContextClassLoader(_sqlContext.sharedState.jarClassLoader)

      _servListener.onStatementStart(statementId, _sessionId, query._1, statementId)
      _sqlContext.sparkContext.setJobGroup(statementId, query._1, true)

      // Initializes a value in fair Scheduler Pools
      _schedulePool.foreach { pool =>
        _sqlContext.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
      }

      val resultRowIterator = try {
        val df = Dataset.ofRows(_sqlContext.sparkSession, query._2)
        logDebug(df.queryExecution.toString())
        _servListener.onStatementParsed(statementId, df.queryExecution.toString())

        val useIncrementalCollect = sessionState._sqlContext.conf.sqlServerIncrementalCollectEnabled
        val rowIter = if (useIncrementalCollect) {
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

        // Based on the assumption that DDL commands succeed, we then update internal states
        query._2 match {
          case SetCommand(Some((SQLServerConf.SQLSERVER_POOL.key, Some(pool)))) =>
            logInfo(s"Setting spark.scheduler.pool=$pool for future statements in this session.")
            _schedulePool = Some(pool)
          case CreateDatabaseCommand(dbName, _, _, _, _) =>
            PgMetadata.registerDatabase(dbName, _sqlContext)
          case CreateTable(desc, _, _) =>
            val dbName = desc.identifier.database.getOrElse("default")
            val tableName = desc.identifier.table
            PgMetadata.registerTable(dbName, tableName, desc.schema, desc.tableType, _sqlContext)
          case CreateTableCommand(table, _) =>
            val dbName = table.identifier.database.getOrElse("default")
            val tableName = table.identifier.table
            PgMetadata.registerTable(dbName, tableName, table.schema, table.tableType, _sqlContext)
          case CreateDataSourceTableCommand(table, _) =>
            val dbName = table.identifier.database.getOrElse("default")
            val tableName = table.identifier.table
            PgMetadata.registerTable(dbName, tableName, table.schema, table.tableType, _sqlContext)
          case CreateViewCommand(table, _, _, _, _, child, _, _, _) =>
            val dbName = table.database.getOrElse("default")
            val tableName = table.identifier
            val qe = _sqlContext.sparkSession.sessionState.executePlan(child)
            val schema = qe.analyzed.schema
            PgMetadata.registerTable(dbName, tableName, schema, CatalogTableType.VIEW, _sqlContext)
          case CreateFunctionCommand(dbNameOption, funcName, _, _, _, _, _) =>
            val dbName = dbNameOption.getOrElse("default")
            PgMetadata.registerFunction(dbName, funcName, _sqlContext)
          case DropDatabaseCommand(dbName, _, _) =>
            logInfo(s"Drop a database `$dbName` and refresh database catalog information")
            PgMetadata.refreshDatabases(dbName, _sqlContext)
          case DropTableCommand(table, _, _, _) =>
            val dbName = table.database.getOrElse("default")
            val tableName = table.identifier
            logInfo(s"Drop a table `$dbName.$tableName` and refresh table catalog information")
            PgMetadata.refreshTables(dbName, _sqlContext)
          case DropFunctionCommand(dbNameOption, funcName, _, _) =>
            val dbName = dbNameOption.getOrElse("default")
            logInfo(s"Drop a function `$dbName.$funcName` and refresh function catalog information")
            PgMetadata.refreshFunctions(dbName, _sqlContext)
          case _ =>
        }
        rowIter
      } catch {
        case NonFatal(e) =>
          if (state == CANCELED) {
            val errMsg =
              s"""Cancelled query with $statementId
                 |Query:
                 |${query._1}
                 |Analyzed Plan:
                 |${query._2}
               """.stripMargin
            logWarning(errMsg)
            throw new SQLException(errMsg)
          } else {
            val exceptionString = SparkUtils.exceptionString(e)
            val errMsg =
              s"""Caught an error executing query with with $statementId:
                 |Query:
                 |${query._1}
                 |Analyzed Plan:
                 |${query._2}
                 |Exception message:
                 |$exceptionString
               """.stripMargin
            logError(errMsg)
            _servListener.onStatementError(statementId, e.getMessage, exceptionString)
            setState(ERROR)
            throw new SQLException(errMsg)
          }
      }

      _servListener.onStatementFinish(statementId)
      setState(FINISHED)
      _cachedRowIterator = resultRowIterator
      resultRowIterator
    } else {
      // Since this operation already has been done, just returns the cached result
      _cachedRowIterator
    }
  }

  override def run(): Iterator[InternalRow] = {
    _ugi.map { ugi =>
      ugi.doAs(new PrivilegedExceptionAction[Iterator[InternalRow]]() {
        override def run() = {
          executeInternal()
        }
      })
    }.getOrElse {
      executeInternal()
    }
  }
}

private[server] class PgExecutor extends OperationExecutor {

  // Creates a new instance for service-specific operations
  override def newOperation(sessionState: SessionState, query: (String, LogicalPlan)): Operation = {
    new PgOperation(sessionState, query)
  }
}
