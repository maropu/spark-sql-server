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

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import scala.util.control.NonFatal

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.master.{LeaderElectable, MonarchyLeaderAgent, ZooKeeperLeaderElectionAgentAccessor}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.{CompositeService, SparkSQLServiceManager}
import org.apache.spark.sql.server.util.{ShutdownHookManager, SQLServerUtils}
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

  def main(args: Array[String]) {
    initDaemon(log)

    val sqlServer = new SQLServer()

    if (SQLServerEnv.sqlConf.sqlServerExecutionMode == "multi-context") {
      logWarning(s"Although multi-context mode enabled, but this mode is experimental")
    }

    // Initializes Spark variables depending on execution modes
    SQLServerEnv.sqlConf.sqlServerExecutionMode match {
      case "single-session" | "multi-session" =>
        ShutdownHookManager.addShutdownHook { () =>
          sqlServer.stop()
          SQLServerEnv.uiTab.foreach(_.detach())
          SQLServerEnv.sparkContext.stop()
        }

      case "multi-context" =>
        ShutdownHookManager.addShutdownHook { () =>
          sqlServer.stop()
        }
    }

    try {
      // Initializes a Spark SQL server with given configurations
      sqlServer.init(SQLServerEnv.sqlConf)
      sqlServer.start()
    } catch {
      case NonFatal(e) =>
        logError("Error starting SQLServer", e)
        System.exit(-1)
    }
  }
}

class SQLServer extends CompositeService with LeaderElectable {

  private val KERBEROS_REFRESH_INTERVAL = TimeUnit.HOURS.toMillis(1)
  private val KERBEROS_FAIL_THRESHOLD = 5

  private val RECOVERY_MODE = SQLServerEnv.sqlConf.sqlServerRecoveryMode
  private val RECOVERY_DIR = SQLServerEnv.sqlConf.sqlServerRecoveryDir + "/leader_election"

  // A server state is tracked internally so that the server only attempts to shut down if it
  // successfully started, and then once only.
  @volatile private var started = false
  @volatile private var state = RecoveryState.STANDBY

  private var kinitExecutor: ScheduledExecutorService = _
  private var kinitFailCount: Int = 0

  addService(new SparkSQLServiceManager())

  private def runKinit(keytab: String, principal: String): Boolean = {
    val commands = Seq("kinit", "-kt", keytab, principal)
    val proc = new ProcessBuilder(commands: _*).inheritIO().start()
    proc.waitFor() match {
      case 0 =>
        logInfo("Ran kinit command successfully.")
        kinitFailCount = 0
        true
      case _ =>
        logWarning("Fail to run kinit command.")
        kinitFailCount += 1
        false
    }
  }

  private def startKinitThread(keytab: String, principal: String): Unit = {
    val kinitTask = new Runnable() {
      override def run(): Unit = {
        if (runKinit(keytab, principal)) {
          // Schedules another kinit run with a fixed delay
          kinitExecutor.schedule(this, KERBEROS_REFRESH_INTERVAL, TimeUnit.MILLISECONDS)
        } else {
          // Schedules another retry at once or fails if too many kinit failures happen
          if (kinitFailCount >= KERBEROS_FAIL_THRESHOLD) {
            kinitExecutor.submit(this)
            // throw new RuntimeException(s"kinit failed $KERBEROS_FAIL_THRESHOLD times.")
          } else {
            kinitExecutor.submit(this)
          }
        }
      }
    }
    kinitExecutor.schedule(
      kinitTask, KERBEROS_REFRESH_INTERVAL, TimeUnit.MILLISECONDS)
  }

  override def doInit(conf: SQLConf): Unit = {
    require(conf.getConfString("spark.sql.crossJoin.enabled") == "true",
      "`spark.sql.crossJoin.enabled` must be true because general DBMS-like engines can " +
        "handle cross joins in SQL queries.")

    // If Kerberos enabled, run kinit periodically. runKinit() should be called before any Hadoop
    // operation, otherwise Kerberos exception will be thrown.
    if (SQLServerUtils.isKerberosEnabled(conf)) {
      kinitExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
          override def newThread(r: Runnable): Thread = {
            val thread = new Thread(r)
            thread.setName("kinit-thread")
            thread.setDaemon(true)
            thread
          }
        }
      )
      val keytabFilename = SQLServerUtils.kerberosKeytab(conf)
      val principalName = SQLServerUtils.kerberosPrincipal(conf)
      if (!runKinit(keytabFilename, principalName)) {
        // throw new RuntimeException("Failed to run kinit.")
      }
      startKinitThread(keytabFilename, principalName)
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
