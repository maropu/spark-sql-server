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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder

/**
 * A parameter set of a SQL JDBC/ODBC server.
 */
private[server] object SQLServerConf {

  /**
   * Port number of SQLServer interface.
   * The default value is `5432`.
   */
  val PORT = "spark.sql.server.port"

  /**
   * Number of SQLServer worker threads.
   * The default value is `4`.
   */
  val WORKER_THREADS = "spark.sql.server.worker.threads"

  /**
   * When set to true, Spark collects result rows partition-by-partition.
   * The default value is `false`.
   */
  val INCREMENTAL_COLLECT = "spark.sql.server.incrementalCollect"

  /**
   * When set to true, SQLServer enables SSL encryption.
   * The default value is `false`.
   */
  val SSL_ENABLED = "spark.sql.server.ssl.enabled"

  val SQLSERVER_POOL = SQLConf.THRIFTSERVER_POOL

  val SQLSERVER_UI_STATEMENT_LIMIT = SQLConf.THRIFTSERVER_UI_STATEMENT_LIMIT

  val SQLSERVER_UI_SESSION_LIMIT = SQLConf.THRIFTSERVER_UI_SESSION_LIMIT

  val SQLSERVER_SINGLE_SESSION = SQLConfigBuilder("spark.sql.sqlserver.singleSession")
    .doc("When true, create a session per each user.")
    .booleanConf
    .createWithDefault(false)

  val SQLSERVER_IDLE_OPERATION_TIMEOUT =
    SQLConfigBuilder("spark.sql.sqlserver.idleOperationTimeout")
      .doc("Operation will be closed when it's not accessed for this duration of time," +
        " which can be disabled by setting to zero value. With positive value," +
        " it's checked for operations in terminal state only (FINISHED, CANCELED, CLOSED, ERROR)." +
        " With negative value, it's checked for all of the operations regardless of state.")
      .longConf
      .createWithDefault(3600 * 5)
}
