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

package org.apache.spark.sql.server

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.sql.internal.SQLConf


/**
 * A parameter set of a SQL JDBC/ODBC server.
 */
object SQLServerConf {

  /**
   * Implicitly inject the [[SQLServerConf]] into [[SQLConf]].
   */
  implicit def SQLConfToSQLServerConf(conf: SQLConf): SQLServerConf = new SQLServerConf(conf)

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key)

  val SQLSERVER_PORT = buildConf("spark.sql.server.port")
    .doc("Port number of SQLServer interface.")
    .intConf
    .createWithDefault(5432)

  val SQLSERVER_WORKER_THREADS = buildConf("spark.sql.server.worker.threads")
    .doc("Number of SQLServer worker threads.")
    .intConf
    .createWithDefault(4)

   val SQLSERVER_INCREMENTAL_COLLECT_ENABLE =
     buildConf("spark.sql.server.incrementalCollect.enabled")
    .doc("When set to true, Spark collects result rows partition-by-partition.")
    .booleanConf
    .createWithDefault(false)

   val SQLSERVER_SSL_ENABLED = buildConf("spark.sql.server.ssl.enabled")
    .doc("When set to true, SQLServer enables SSL encryption.")
    .booleanConf
    .createWithDefault(false)

  val SQLSERVER_POOL = buildConf("spark.sql.server.scheduler.pool")
    .doc("Set a Fair Scheduler pool for a JDBC client session.")
    .stringConf
    .createWithDefault("FIFO")

  val SQLSERVER_UI_STATEMENT_LIMIT = buildConf("spark.sql.server.ui.retainedStatements")
    .doc("The number of SQL statements kept in the JDBC/ODBC web UI history.")
    .intConf
    .createWithDefault(200)

  val SQLSERVER_UI_SESSION_LIMIT = buildConf("spark.sql.server.ui.retainedSessions")
    .doc("The number of SQL client sessions kept in the JDBC/ODBC web UI history.")
    .intConf
    .createWithDefault(200)

  val SQLSERVER_SINGLE_SESSION_ENABLED = buildConf("spark.sql.server.singleSession")
    .doc("When true, create a session per each user.")
    .booleanConf
    .createWithDefault(false)

  val SQLSERVER_IDLE_OPERATION_TIMEOUT = buildConf("spark.sql.server.idleOperationTimeout")
      .doc("Operation will be closed when it's not accessed for this duration of time," +
        " which can be disabled by setting to zero value. With positive value," +
        " it's checked for operations in terminal state only (FINISHED, CANCELED, CLOSED, ERROR)." +
        " With negative value, it's checked for all of the operations regardless of state.")
      .longConf
      .createWithDefault(3600 * 5)
}

class SQLServerConf(conf: SQLConf) {
  import SQLServerConf._

  def sqlServerPort: Int = conf.getConf(SQLSERVER_PORT)

  def sqlServerWorkerThreads: Int = conf.getConf(SQLSERVER_WORKER_THREADS)

  def sqlServerIncrementalCollectEnabled: Boolean =
    conf.getConf(SQLSERVER_INCREMENTAL_COLLECT_ENABLE)

  def sqlServerSslEnabled: Boolean = conf.getConf(SQLSERVER_SSL_ENABLED)

  def sqlServerPool: String = conf.getConf(SQLSERVER_POOL)

  def sqlServerUiStatementLimit: Int = conf.getConf(SQLSERVER_UI_STATEMENT_LIMIT)

  def sqlServerUiSessionLimit: Int = conf.getConf(SQLSERVER_UI_SESSION_LIMIT)

  def sqlServerSingleSessionEnabled: Boolean = conf.getConf(SQLSERVER_SINGLE_SESSION_ENABLED)

  def sqlServerIdleOperationTimeout: Long = conf.getConf(SQLSERVER_IDLE_OPERATION_TIMEOUT)
}
