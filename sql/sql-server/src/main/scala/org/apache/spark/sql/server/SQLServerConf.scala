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

import java.util.concurrent.TimeUnit

import scala.language.implicitConversions

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, ConfigReader}
import org.apache.spark.sql.internal.SQLConf


object SQLServerConf {

  /**
   * Implicitly inject the [[SQLServerConf]] into [[SQLConf]].
   */
  implicit def SQLConfToSQLServerConf(conf: SQLConf): SQLServerConf = new SQLServerConf(conf)

  private val sqlConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, ConfigEntry[_]]())

  private def register(entry: ConfigEntry[_]): Unit = sqlConfEntries.synchronized {
    require(!sqlConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    sqlConfEntries.put(entry.key, entry)
  }

  // For testing only
  // TODO: Need to add tests for the configurations
  private[sql] def unregister(entry: ConfigEntry[_]): Unit = sqlConfEntries.synchronized {
    sqlConfEntries.remove(entry.key)
  }

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  val SQLSERVER_PORT = buildConf("spark.sql.server.port")
    .doc("Port number of SQLServer interface.")
    .intConf
    .createWithDefault(5432)

  // scalastyle:off line.size.limit
  // `server_version` decides how to handle metadata between JDBC clients and servers.
  // See an URL below for valid version numbers:
  // https://github.com/pgjdbc/pgjdbc/blob/REL42.2.2/pgjdbc/src/main/java/org/postgresql/core/ServerVersion.java
  // scalastyle:on line.size.limit
  val SQLSERVER_VERSION = buildConf("spark.sql.server.version")
    .internal()
    .stringConf
    // Keeps "7.4" for tests
    .checkValue(version => Seq("7.4", "8.4", "9.6", "10").contains(version),
      "The server version must be 8.4, 9.6, or 10")
    .createWithDefault("8.4")

  // This option is mainly used for interactive tests
  val SQLSERVER_PSQL_ENABLED = buildConf("spark.sql.server.psql.enabled")
    .internal()
    .doc("When set to true, the Spark SQL server accepts requests from psql clients.")
    .booleanConf
    .createWithDefault(false)

  val SQLSERVER_WORKER_THREADS = buildConf("spark.sql.server.worker.threads")
    .doc("Number of SQLServer worker threads.")
    .intConf
    .createWithDefault(4)

  val SQLSERVER_BINARY_TRANSFER_MODE = buildConf("spark.sql.server.binaryTransferMode")
    .doc("Whether binary transfer mode is enabled.")
    .booleanConf
    .createWithDefault(true)

  val SQLSERVER_INCREMENTAL_COLLECT_ENABLED =
     buildConf("spark.sql.server.incrementalCollect.enabled")
    .doc("When set to true, Spark collects result rows partition-by-partition.")
    .booleanConf
    .createWithDefault(false)

  val SQLSERVER_RECOVERY_MODE = buildConf("spark.sql.server.recoveryMode")
    .doc("Set to ZOOKEEPER to enable recovery mode with Zookeeper.")
    .stringConf
    .createOptional

  val SQLSERVER_RECOVERY_DIR = buildConf("spark.sql.server.recoveryDirectory")
    .doc("The directory in which Spark will store recovery state, accessible " +
      "from the Spark SQL server's perspective.")
    .stringConf
    .createWithDefault("/spark-sql-server")

  val SQLSERVER_IDLE_SESSION_CLEANUP_DELAY =
    buildConf("spark.sql.server.idleSessionCleanupDelay")
      .doc("How long in milliseconds an idle session is removed from a session manager.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.HOURS.toMillis(1)) // 1 hour

  val SQLSERVER_SSL_ENABLED = buildConf("spark.sql.server.ssl.enabled")
    .doc("When set to true, SQLServer enables SSL encryption.")
    .booleanConf
    .createWithDefault(false)

  val SQLSERVER_SSL_KEYSTORE_PATH = buildConf("spark.sql.server.ssl.keystore.path")
    .doc("Keystore path")
    .stringConf
    .createOptional

  val SQLSERVER_SSL_KEYSTORE_PASSWD = buildConf("spark.sql.server.ssl.keystore.passwd")
    .doc("Keystore password")
    .stringConf
    .createOptional

  val SQLSERVER_POOL = buildConf("spark.sql.server.scheduler.pool")
    .doc("Set a Fair Scheduler pool for a JDBC client session.")
    .stringConf
    .createWithDefault("FIFO")

  val SQLSERVER_DOAS_ENABLED = buildConf("spark.sql.yarn.doAs.enabled")
    .doc("Whether authentication impersonates connected users.")
    .booleanConf
    .createWithDefault(true)

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

  val SQLSERVER_MESSAGE_BUFFER_SIZE_IN_BYTES =
    buildConf("spark.sql.server.messageBufferSizeInBytes")
      .doc("Maximum bytes of a single record we assume when converting Spark internal rows " +
        "into binary data in the PostgreSQL V3 protocol")
      .internal()
      .intConf
      .createWithDefault(3 * 1024 * 1024)
}

class SQLServerConf(conf: SQLConf) {
  import SQLServerConf._

  private val reader = new ConfigReader(conf.settings)

  def sqlServerPort: Int = getConf(SQLSERVER_PORT)

  def sqlServerVersion: String = getConf(SQLSERVER_VERSION)

  def sqlServerPsqlEnabled: Boolean = getConf(SQLSERVER_PSQL_ENABLED)

  def sqlServerWorkerThreads: Int = getConf(SQLSERVER_WORKER_THREADS)

  def sqlServerBinaryTransferMode: Boolean = getConf(SQLSERVER_BINARY_TRANSFER_MODE)

  def sqlServerIncrementalCollectEnabled: Boolean = getConf(SQLSERVER_INCREMENTAL_COLLECT_ENABLED)

  def sqlServerRecoveryMode: Option[String] = getConf(SQLSERVER_RECOVERY_MODE)

  def sqlServerRecoveryDir: String = getConf(SQLSERVER_RECOVERY_DIR)

  def sqlServerIdleSessionCleanupDelay: Long = getConf(SQLSERVER_IDLE_SESSION_CLEANUP_DELAY)

  def sqlServerSslEnabled: Boolean = getConf(SQLSERVER_SSL_ENABLED)

  def sqlServerSslKeyStorePath: Option[String] = getConf(SQLSERVER_SSL_KEYSTORE_PATH)

  def sqlServerSslKeyStorePasswd: Option[String] = getConf(SQLSERVER_SSL_KEYSTORE_PASSWD)

  def sqlServerPool: String = getConf(SQLSERVER_POOL)

  def sqlServerDoAsEnabled: Boolean = getConf(SQLSERVER_DOAS_ENABLED)

  def sqlServerUiStatementLimit: Int = getConf(SQLSERVER_UI_STATEMENT_LIMIT)

  def sqlServerUiSessionLimit: Int = getConf(SQLSERVER_UI_SESSION_LIMIT)

  def sqlServerSingleSessionEnabled: Boolean = getConf(SQLSERVER_SINGLE_SESSION_ENABLED)

  def sqlServerIdleOperationTimeout: Long = getConf(SQLSERVER_IDLE_OPERATION_TIMEOUT)

  def sqlServerMessageBufferSizeInBytes: Int = getConf(SQLSERVER_MESSAGE_BUFFER_SIZE_IN_BYTES)

  /** ********************** SQLConf functionality methods ************ */

  /**
   * Return the value of SQL server configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[ConfigEntry]].
   */
  private def getConf[T](entry: ConfigEntry[T]): T = {
    require(sqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    entry.readFrom(reader)
  }
}
