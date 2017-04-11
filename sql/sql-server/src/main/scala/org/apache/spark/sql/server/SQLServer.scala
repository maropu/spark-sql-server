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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.master.{LeaderElectable, MonarchyLeaderAgent, ZooKeeperLeaderElectionAgentAccessor}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobStart}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.{CompositeService, SparkSQLCLIService}
import org.apache.spark.sql.server.service.postgresql.PostgreSQLService
import org.apache.spark.sql.server.ui.SQLServerTab
import org.apache.spark.util.{ShutdownHookManager, Utils}


/**
 * A SQL gateway server that uses a PostgreSQL message-based protocol for communication between
 * frontends and backends (clients and servers).
 */
object SQLServer extends Logging {

  var uiTab: Option[SQLServerTab] = None
  var listener: SQLServerListener = _

  private def prepareWith(sqlServer: SQLServer): Unit = {
    listener = new SQLServerListener(sqlServer, SQLServerEnv.sparkConf)
    uiTab = SQLServerEnv.sparkConf.getBoolean("spark.ui.enabled", true) match {
      case true => Some(new SQLServerTab(SQLServerEnv.sqlContext.sparkContext))
      case _ => None
    }
    ShutdownHookManager.addShutdownHook { () =>
      SQLServerEnv.cleanup()
      uiTab.foreach(_.detach())
    }
  }

  /**
   * :: DeveloperApi ::
   * Starts a new SQL server with the given context.
   */
  @DeveloperApi
  def startWithContext(sqlContext: SQLContext): Unit = {
    SQLServerEnv.withSQLContext(sqlContext)

    val sqlServer = new SQLServer()
    prepareWith(sqlServer)
    // Initialize a Spark SQL server with given configurations
    sqlServer.init(SQLServerEnv.sparkConf)
    sqlServer.start()
  }

  def main(args: Array[String]) {
    Utils.initDaemon(log)

    val sqlServer = new SQLServer()
    prepareWith(sqlServer)
    try {
      // Initialize a Spark SQL server with given configurations
      sqlServer.init(SQLServerEnv.sparkConf)
      sqlServer.start()
    } catch {
      case NonFatal(e) =>
        logError("Error starting SQLServer", e)
        System.exit(-1)
    }

    // If application was killed before SQLServer start successfully then SparkSubmit process
    // can not exit, so check whether if SparkContext was stopped.
    if (SQLServerEnv.sparkContext.stopped.get()) {
      logError("SparkContext has stopped even if SQLServer has started, so exit")
      System.exit(-1)
    }
  }

  private[server] class SessionInfo(
      val sessionId: Int,
      val startTimestamp: Long,
      val userName: String,
      val ipAddr: String) {
    var finishTimestamp: Long = 0L
    var totalExecution: Int = 0
    def totalTime: Long = {
      if (finishTimestamp == 0L) {
        System.currentTimeMillis - startTimestamp
      } else {
        finishTimestamp - startTimestamp
      }
    }
  }

  private[server] object ExecutionState extends Enumeration {
    val STARTED, COMPILED, FAILED, CANCELED, FINISHED = Value
    type ExecutionState = Value
  }

  private[server] class ExecutionInfo(
      val statement: String,
      val sessionId: Int,
      val startTimestamp: Long,
      val userName: String) {
    var finishTimestamp: Long = 0L
    var executePlan: String = ""
    var detail: String = ""
    var state: ExecutionState.Value = ExecutionState.STARTED
    val jobId: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()
    var groupId: String = ""
    def totalTime: Long = {
      if (finishTimestamp == 0L) {
        System.currentTimeMillis - startTimestamp
      } else {
        finishTimestamp - startTimestamp
      }
    }
  }

  /** An inner sparkListener called in sc.stop to clean up the SQLServer. */
  private[server] class SQLServerListener(
      val server: SQLServer,
      val conf: SparkConf) extends SparkListener {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      server.stop()
    }
    private var onlineSessionNum: Int = 0
    private val sessionList = new mutable.LinkedHashMap[Int, SessionInfo]
    private val executionList = new mutable.LinkedHashMap[String, ExecutionInfo]
    private val retainedStatements = conf.sqlServerUiStatementLimit
    private val retainedSessions = conf.sqlServerUiSessionLimit
    private var totalRunning = 0

    def getOnlineSessionNum: Int = synchronized { onlineSessionNum }

    def getTotalRunning: Int = synchronized { totalRunning }

    def getSessionList: Seq[SessionInfo] = synchronized { sessionList.values.toSeq }

    def getSession(sessionId: Int): Option[SessionInfo] = synchronized {
      sessionList.get(sessionId)
    }

    def getExecutionList: Seq[ExecutionInfo] = synchronized { executionList.values.toSeq }

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
      for {
        props <- Option(jobStart.properties)
        groupId <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
        (_, info) <- executionList if info.groupId == groupId
      } {
        info.jobId += jobStart.jobId.toString
        info.groupId = groupId
      }
    }

    def onSessionCreated(sessionId: Int, userName: String, ipAddr: String): Unit = {
      synchronized {
        val info = new SessionInfo(sessionId, System.currentTimeMillis, userName, ipAddr)
        sessionList.put(sessionId, info)
        onlineSessionNum += 1
        trimSessionIfNecessary()
      }
    }

    def onSessionClosed(sessionId: Int): Unit = synchronized {
      sessionList(sessionId).finishTimestamp = System.currentTimeMillis
      onlineSessionNum -= 1
      trimSessionIfNecessary()
    }

    def onStatementStart(
        id: String,
        sessionId: Int,
        statement: String,
        groupId: String): Unit = synchronized {
      val info = new ExecutionInfo(statement, sessionId, System.currentTimeMillis,
        sessionList.get(sessionId).map(_.userName).getOrElse("UNKNOWN"))
      info.state = ExecutionState.STARTED
      executionList.put(id, info)
      trimExecutionIfNecessary()
      sessionList(sessionId).totalExecution += 1
      executionList(id).groupId = groupId
      totalRunning += 1
    }

    def onStatementParsed(id: String, executionPlan: String): Unit = synchronized {
      executionList(id).executePlan = executionPlan
      executionList(id).state = ExecutionState.COMPILED
    }

    def onStatementCanceled(id: String): Unit = synchronized {
      executionList(id).finishTimestamp = System.currentTimeMillis
      executionList(id).state = ExecutionState.CANCELED
      totalRunning -= 1
      trimExecutionIfNecessary()
    }

    def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit = {
      synchronized {
        executionList(id).finishTimestamp = System.currentTimeMillis
        executionList(id).detail = errorMessage
        executionList(id).state = ExecutionState.FAILED
        totalRunning -= 1
        trimExecutionIfNecessary()
      }
    }

    def onStatementFinish(id: String): Unit = synchronized {
      executionList(id).finishTimestamp = System.currentTimeMillis
      executionList(id).state = ExecutionState.FINISHED
      totalRunning -= 1
      trimExecutionIfNecessary()
    }

    private def trimExecutionIfNecessary() = {
      if (executionList.size > retainedStatements) {
        val toRemove = math.max(retainedStatements / 10, 1)
        executionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
          executionList.remove(s._1)
        }
      }
    }

    private def trimSessionIfNecessary() = {
      if (sessionList.size > retainedSessions) {
        val toRemove = math.max(retainedSessions / 10, 1)
        sessionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
          sessionList.remove(s._1)
        }
      }
    }
  }
}

private[sql] class SQLServer extends CompositeService with LeaderElectable {

  private val RECOVERY_MODE = SQLServerEnv.sparkConf.sqlServerRecoveryMode
  private val RECOVERY_DIR = SQLServerEnv.sparkConf.sqlServerRecoveryDir + "/leader_election"

  // A server state is tracked internally so that the server only attempts to shut down if it
  // successfully started, and then once only.
  @volatile private var started: Boolean = false
  @volatile private var state = RecoveryState.STANDBY

  override def init(conf: SparkConf): Unit = {
    val cliService = new SparkSQLCLIService(this)
    addService(cliService)
    addService(new PostgreSQLService(this, cliService))
    super.init(conf)
  }

  override def start(): Unit = {
    require(SQLServerEnv.serverVersion.nonEmpty)
    logInfo(s"Try to start the Spark SQL server with version=${SQLServerEnv.serverVersion}...")
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
    SQLServerEnv.sparkContext.addSparkListener(SQLServer.listener)
    started = true
    logInfo(s"Started the SQL server and state is: $state")
    super.start()
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

  override def stop(): Unit = {
    if (started) {
      super.stop()
      started = false
    }
  }
}
