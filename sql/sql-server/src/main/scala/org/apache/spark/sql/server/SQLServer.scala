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

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobStart}
import org.apache.spark.sql.server.service.{CompositeService, SparkSQLCLIService}
import org.apache.spark.sql.server.service.postgresql.PostgreSQLService
import org.apache.spark.sql.server.ui.SQLServerTab
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * A SQL gateway server that uses a PostgreSQL message-based protocol for communication between
 * frontends and backends (clients and servers).
 */
object SQLServer extends Logging {

  var uiTab: Option[SQLServerTab] = _
  var listener: SQLServerListener = _

  /**
   * :: DeveloperApi ::
   * Starts a new SQL server with the given context.
   */
  @DeveloperApi
  def startWithContext(sqlContext: SQLContext): Unit = {
    val server = new SQLServer(sqlContext)
    server.init(sqlContext)
    listener = new SQLServerListener(server, sqlContext.conf)
    sqlContext.sparkContext.addSparkListener(listener)
    uiTab = if (sqlContext.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
      Some(new SQLServerTab(sqlContext.sparkContext))
    } else {
      None
    }
    server.start()
  }

  def main(args: Array[String]) {
    Utils.initDaemon(log)

    // TODO: Need to process given options

    logInfo("Starting SparkContext")
    SQLServerEnv.init()

    ShutdownHookManager.addShutdownHook { () =>
      SQLServerEnv.stop()
      uiTab.foreach(_.detach())
    }

    try {
      val server = new SQLServer(SQLServerEnv.sqlContext)
      server.init(SQLServerEnv.sqlContext)
      logInfo("SQLServer started")
      listener = new SQLServerListener(server, SQLServerEnv.sqlContext.conf)
      SQLServerEnv.sparkContext.addSparkListener(listener)
      uiTab = if (SQLServerEnv.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
        Some(new SQLServerTab(SQLServerEnv.sparkContext))
      } else {
        None
      }

      server.start()

      // If application was killed before SQLServer start successfully then SparkSubmit process
      // can not exit, so check whether if SparkContext was stopped.
      if (SQLServerEnv.sparkContext.stopped.get()) {
        logError("SparkContext has stopped even if SQLServer has started, so exit")
        System.exit(-1)
      }
    } catch {
      case e: Exception =>
        logError("Error starting SQLServer", e)
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

  /**
   * An inner sparkListener called in sc.stop to clean up the SQLServer
   */
  private[server] class SQLServerListener(
      val server: SQLServer,
      val conf: SQLConf) extends SparkListener {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      server.stop()
    }
    private var onlineSessionNum: Int = 0
    private val sessionList = new mutable.LinkedHashMap[Int, SessionInfo]
    private val executionList = new mutable.LinkedHashMap[String, ExecutionInfo]
    private val retainedStatements = conf.getConf(SQLServerConf.SQLSERVER_UI_STATEMENT_LIMIT)
    private val retainedSessions = conf.getConf(SQLServerConf.SQLSERVER_UI_SESSION_LIMIT)
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

private[sql] class SQLServer(sqlContext: SQLContext) extends CompositeService {
  // A server state is tracked internally so that the server only attempts to shut down if it
  // successfully started, and then once only.
  private val started = new AtomicBoolean(false)

  override def init(sqlContext: SQLContext): Unit = {
    val cliService = new SparkSQLCLIService(this, sqlContext)
    addService(cliService)
    addService(new PostgreSQLService(cliService))
    super.init(sqlContext)
  }

  override def start(): Unit = {
    super.start()
    started.set(true)
  }

  override def stop(): Unit = {
    if (started.getAndSet(false)) {
      super.stop()
    }
  }
}
