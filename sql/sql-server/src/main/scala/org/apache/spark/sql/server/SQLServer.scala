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

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.master.{LeaderElectable, MonarchyLeaderAgent, ZooKeeperLeaderElectionAgentAccessor}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.{CompositeService, SparkSQLServiceManager}
import org.apache.spark.sql.server.util.SQLServerUtils
import org.apache.spark.util.ShutdownHookManager
import org.apache.spark.util.Utils._


/**
 * A SQL gateway server that uses a PostgreSQL message-based protocol for communication between
 * frontends and backends (clients and servers).
 */
object SQLServer extends Logging {

  /**
   * :: DeveloperApi ::
   * Starts a new SQL server with the given context.
   */
  @DeveloperApi
  def startWithContext(sqlContext: SQLContext, blocking: Boolean = false): Unit = {
    if (blocking) {
      _doStartWithContext(sqlContext)
    } else {
      val serverThread = new Thread() {
          override def run(): Unit = {
            try { _doStartWithContext(sqlContext) } catch {
              case NonFatal(e) => logError("Error running SQLServer", e)
            }
          }
        }
      serverThread.setName(s"SparkSQL::${localHostName()}")
      serverThread.setDaemon(true)
      serverThread.start()
    }
  }

  private def _doStartWithContext(sqlContext: SQLContext): Unit = {
    SQLServerEnv.withSQLContext(sqlContext)
    val sqlServer = new SQLServer()
    // Initialize a Spark SQL server with given configurations
    sqlServer.init(sqlContext.conf)
    sqlServer.start()
    ShutdownHookManager.addShutdownHook { () =>
      SQLServerEnv.uiTab.foreach(_.detach())
      sqlServer.stop()
    }
  }

  private def mergeSparkConf(sqlConf: SQLConf, sparkConf: SparkConf): Unit = {
    sparkConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
    }
  }

  def main(args: Array[String]) {
    initDaemon(log)

    // Initializes Spark variables depending on execution modes
    val sqlConf = if (SQLServerUtils.checkIfMultiContextModeEnabled(SQLServerEnv.sparkConf)) {
      val newSqlConf = new SQLConf()
      mergeSparkConf(newSqlConf, SQLServerEnv.sparkConf)
      newSqlConf
    } else {
      SQLServerEnv.sqlServListener
      SQLServerEnv.uiTab
      ShutdownHookManager.addShutdownHook { () =>
        SQLServerEnv.uiTab.foreach(_.detach())
        SQLServerEnv.sparkContext.stop()
      }
      SQLServerEnv.sqlConf
    }

    val sqlServer = new SQLServer()
    try {
      // Initializes a Spark SQL server with given configurations
      sqlServer.init(sqlConf)
      sqlServer.start()
    } catch {
      case NonFatal(e) =>
        logError("Error starting SQLServer", e)
        System.exit(-1)
    } finally {
     sqlServer.stop()
    }
  }
}

class SQLServer extends CompositeService with LeaderElectable {

  private val RECOVERY_MODE = SQLServerEnv.sqlConf.sqlServerRecoveryMode
  private val RECOVERY_DIR = SQLServerEnv.sqlConf.sqlServerRecoveryDir + "/leader_election"

  // A server state is tracked internally so that the server only attempts to shut down if it
  // successfully started, and then once only.
  @volatile private var started = false
  @volatile private var state = RecoveryState.STANDBY

  addService(new SparkSQLServiceManager())

  override def doInit(conf: SQLConf): Unit = {
    require(conf.getConfString("spark.sql.crossJoin.enabled") == "true",
      "`spark.sql.crossJoin.enabled` must be true because general DBMS-like engines can " +
        "handle cross joins in SQL queries.")

    // Settings for a Kerberos secure cluster
    if (SQLServerUtils.checkIfKerberosEnabled(conf)) {
      // To authenticate the SQL server, the following 2 params must be set
      val principalName = conf.getConfString("spark.yarn.keytab")
      val keytabFilename = conf.getConfString("spark.yarn.principal")
      SparkHadoopUtil.get.loginUserFromKeytab(principalName, keytabFilename)
    }
  }

  override def doStart(): Unit = {
    RECOVERY_MODE match {
      case Some("ZOOKEEPER") =>
        logInfo("Recovery mode 'ZOOKEEPER' enabled and wait for leader election")
        val sparkConf = SQLServerEnv.sparkConf
        new ZooKeeperLeaderElectionAgentAccessor(this, sparkConf, RECOVERY_DIR)
        // If recovery modes enabled, we should activate a single `SQLContext` across candidates
        synchronized { wait() }
      case Some(mode) =>
        throw new IllegalArgumentException(s"Unknown recovery mode: $mode")
      case None =>
        state = RecoveryState.ALIVE
        new MonarchyLeaderAgent(this)
    }
    require(state == RecoveryState.ALIVE)
    started = true
    logInfo(s"Started the SQL server and state is: $state")
  }

  override def electedLeader(): Unit = {
    state = RecoveryState.ALIVE
    logInfo(s"I have been elected leader! New state: $state")
    synchronized { notifyAll() }
  }

  override def revokedLeadership(): Unit = {
    logError("Leadership has been revoked -- SQLServer shutting down.")
    System.exit(-1)
  }

  override def doStop(): Unit = {
    if (started) {
      started = false
    }
  }
}
