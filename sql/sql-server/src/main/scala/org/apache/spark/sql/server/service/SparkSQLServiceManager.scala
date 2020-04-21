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

import java.util.{HashMap => jHashMap, UUID}
import java.util.Collections.{synchronizedMap => jSyncMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.SQLServerEnv
import org.apache.spark.sql.server.SQLServerListener
import org.apache.spark.sql.server.service.livy.{LivyProxyContext, LivyProxyExecutor, LivyServerService}
import org.apache.spark.sql.server.service.postgresql.{PgCatalogInitializer, PgProtocolService, PgSessionInitializer}
import org.apache.spark.sql.server.ui.SQLServerTab
import org.apache.spark.sql.server.util.RecurringTimer
import org.apache.spark.util.SystemClock

// Base trait for `SQLContext` and `LivyProxyContext`
trait SessionContext

case class SQLContextHolder(sqlContext: SQLContext) extends SessionContext

trait SessionState {

  // Holds a session-specific context
  private[service] var _sessionId: Int = _

  private[service] var _context: SessionContext = _

  private[service] var _servListener: Option[SQLServerListener] = _
  private[service] var _uiTab: Option[SQLServerTab] = None
  private[service] var _schedulePool: Option[String] = None

  // Called when an idle session cleaner closes this session
  def closeWithException(msg: String): Unit = close()

  def close(): Unit = {
    _context match {
      case ctx: LivyProxyContext => ctx.stop()
      case _ =>
    }
  }
}

trait SessionService {
  def openSession(userName: String, passwd: String, ipAddress: String, dbName: String,
    state: SessionState): Int
  def getSessionState(sessionId: Int): SessionState
  def closeSession(sessionId: Int): Unit
  def executeStatement(sessionId: Int, query: (String, LogicalPlan)): Operation
}

private[service] case class TimeStampedValue[V](value: V, timestamp: Long)

private[service] class SessionManager(
    frontend: FrontendService,
    initSession: (String, SQLContext) => Unit) extends CompositeService with Logging {

  private val nextSessionId = new AtomicInteger(0)
  private val sessionIdToState = jSyncMap(new jHashMap[Int, TimeStampedValue[SessionState]]())

  SQLServerEnv.sqlConf.sqlServerExecutionMode match {
    case "multi-context" =>
      addService(new LivyServerService(frontend))
    case _ =>
  }

  // For functions to initialize sessions
  private var getSessionContext: (Int, String, String) => SessionContext = _
  private var getServerListener: () => Option[SQLServerListener] = _
  private var getUiTab: () => Option[SQLServerTab] = _

  private var idleSessionCleanupDelay: Long = _
  private var idleSessionCleaner: RecurringTimer = _

  override def doInit(conf: SQLConf): Unit = {
    idleSessionCleanupDelay = conf.sqlServerIdleSessionCleanupDelay
    if (idleSessionCleanupDelay > 0) {
      // For testing
      val idleSessionCleanupInterval =
        conf.getConfString("spark.sql.server.idleSessionCleanupInterval", "300000").toLong
      idleSessionCleaner = startIdleSessionCleanupThread(period = idleSessionCleanupInterval)
    }

    // Initializes functions depending on execution modes
    getSessionContext = conf.sqlServerExecutionMode match {
      case "single-session" =>
        (_: Int, _: String, _: String) => {
          SQLContextHolder(SQLServerEnv.sqlContext)
        }
      case "multi-session" =>
        (_: Int, _: String, dbName: String) => {
          val sqlContext = SQLServerEnv.sqlContext.newSession()
          initSession(dbName, sqlContext)
          SQLContextHolder(sqlContext)
        }
      case "multi-context" =>
        (sessionId: Int, userName: String, dbName: String) => {
          val livyService = services.headOption.map {
            case s: LivyServerService => s
            case other =>
              sys.error(s"${classOf[LivyServerService].getSimpleName} expected, but " +
                s"${other.getClass.getSimpleName} found.")
          }.getOrElse {
            sys.error(s"No service attached as a child in ${this.getClass.getSimpleName}.")
          }
          val livyContext = new LivyProxyContext(conf, livyService)
          livyContext.init(s"rpc-service-session-$sessionId", sessionId, userName, dbName)
          livyContext.connect()
          livyContext
        }
    }

    getServerListener = conf.sqlServerExecutionMode match {
      case "single-session" | "multi-session" =>
        () => SQLServerEnv.sqlServListener
      case "multi-context" =>
        () => None
    }

    getUiTab = conf.sqlServerExecutionMode match {
      case "single-session" | "multi-session" =>
        () => SQLServerEnv.uiTab
      case "multi-context" =>
        () => None
    }
  }

  override def doStart(): Unit = {
    SQLServerEnv.sqlContext.conf.sqlServerExecutionMode match {
      case "single-session" =>
        initSession("default", SQLServerEnv.sqlContext)
      case _ =>
    }
  }

  override def doStop(): Unit = {
    if (sessionIdToState.size() > 0) {
      logWarning(s"${this.getClass.getSimpleName} stopped though, " +
        s"${sessionIdToState.size()} opened sessions still existed")
    }
  }

  private def newSessionId(): Int = nextSessionId.getAndIncrement

  def openSession(userName: String, passwd: String, ipAddress: String, dbName: String,
      state: SessionState): Int = {
    val sessionId = newSessionId()
    val sessionContext = getSessionContext(sessionId, userName, dbName)
    val servListener = getServerListener()
    state._sessionId = sessionId
    state._context = sessionContext
    state._servListener = servListener
    state._uiTab = getUiTab()
    sessionContext match {
      case SQLContextHolder(sqlContext) =>
        sqlContext.sharedState.externalCatalog.setCurrentDatabase(dbName)
      case _ =>
    }
    sessionIdToState.put(sessionId, TimeStampedValue(state, currentTime))
    servListener.foreach(_.onSessionCreated(sessionId, userName, ipAddress))
    sessionId
  }

  def closeSession(sessionId: Int): Unit = {
    val value = sessionIdToState.remove(sessionId)
    if (value != null) {
      val TimeStampedValue(state, _) = value
      state._servListener.foreach(_.onSessionClosed(sessionId))
      state.close()
    } else {
      logError(s"A session (sessionId=$sessionId) has been already closed")
    }
  }

  def getSession(sessionId: Int): SessionState = {
    val value = sessionIdToState.get(sessionId)
    if (value != null) {
      val TimeStampedValue(state, _) = value
      // Update timestamp
      sessionIdToState.replace(sessionId, TimeStampedValue(state, currentTime))
      state
    } else {
      logError(s"A session (sessionId=$sessionId) has been already closed")
      null
    }
  }

  private def currentTime: Long = System.currentTimeMillis

  private def closeSessionWithException(sessionId: Int, msg: String): Unit = {
    val value = sessionIdToState.remove(sessionId)
    if (value != null) {
      val TimeStampedValue(state, _) = value
      state._servListener.foreach(_.onSessionClosed(sessionId))
      state.closeWithException(msg)
    } else {
      // Just ignore it
    }
  }

  private def checkIdleSessions(): Unit = {
    val sessionsToRemove = mutable.ArrayBuffer[(Int, Long)]()
    val checkTime = currentTime
    val eIter = sessionIdToState.entrySet().iterator()
    while (eIter.hasNext) {
      val e = eIter.next()
      val (sessionId, value) = (e.getKey, e.getValue)
      val sessionState = value.value
      val idleTime = checkTime - value.timestamp
      if (idleTime > idleSessionCleanupDelay) {
        logWarning("Closing an idle session... " +
          s"(idleTime=$idleTime sessionId=$sessionId $sessionState)")
        sessionsToRemove += ((sessionId, idleTime))
      } else {
        logInfo("Found an active session " +
          s"(idleTime=$idleTime sessionId=$sessionId $sessionState)")
      }
    }

    sessionsToRemove.foreach { case (sessionId, idleTime) =>
      closeSessionWithException(sessionId,
        s"Closed this session because of long idle time (idleTime=$idleTime)")
    }
  }

  private def startIdleSessionCleanupThread(period: Long): RecurringTimer = {
    val threadName = "Idle Session Cleaner"
    val timer = new RecurringTimer(new SystemClock(), period, _ => checkIdleSessions(), threadName)
    timer.start()
    logDebug(s"Started idle session cleanup thread: $threadName")
    timer
  }
}

private[server] class SparkSQLServiceManager extends CompositeService with SessionService {

  private val frontendService = new PgProtocolService(this)
  private val sessionManager = new SessionManager(
    frontendService,
    (dbName: String, sqlContext: SQLContext) => {
      PgSessionInitializer(dbName, sqlContext)
    })

  private val executorImpl = SQLServerEnv.sqlConf.sqlServerExecutionMode match {
    case "single-session" | "multi-session" => new ExecutorImpl()
    case "multi-context" => new LivyProxyExecutor()
  }

  // We must start `PgProtocolService` in the end of services
  SQLServerEnv.sqlConf.sqlServerExecutionMode match {
    case "single-session" | "multi-session" =>
      addService(new PgCatalogInitializer())
    case _ =>
  }
  addService(sessionManager)
  addService(frontendService)

  override def openSession(userName: String, passwd: String, ipAddress: String, dbName: String,
      state: SessionState): Int = {
    sessionManager.openSession(userName, passwd, ipAddress, dbName, state)
  }

  override def getSessionState(sessionId: Int): SessionState = {
    sessionManager.getSession(sessionId)
  }

  override def closeSession(sessionId: Int): Unit = {
    sessionManager.closeSession(sessionId)
  }

  override def executeStatement(sessionId: Int, plan: (String, LogicalPlan)): Operation = {
    executorImpl.newOperation(
      sessionState = sessionManager.getSession(sessionId),
      statementId = UUID.randomUUID().toString,
      plan)
  }
}
