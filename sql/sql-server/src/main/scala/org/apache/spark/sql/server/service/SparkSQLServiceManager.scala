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

import java.util.{HashMap => jHashMap}
import java.util.Collections.{synchronizedMap => jSyncMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.SQLServerEnv
import org.apache.spark.sql.server.SQLServerListener
import org.apache.spark.sql.server.service.postgresql.{PgCatalogInitializer, PgExecutor, PgProtocolService, PgSessionInitializer}
import org.apache.spark.sql.server.ui.SQLServerTab
import org.apache.spark.sql.server.util.{RecurringTimer, SQLServerUtils}
import org.apache.spark.util.SystemClock


trait SessionInitializer {
  def apply(dbName: String, sqlContext: SQLContext): Unit
}

trait SessionState {

  // Holds a session-specific context
  private[service] var _sessionId: Int = _

  private[service] var _sqlContext: SQLContext = _
  private[service] var _servListener: SQLServerListener = _

  private[service] var _uiTab: Option[SQLServerTab] = None
  private[service] var _schedulePool: Option[String] = None
  private[service] var _ugi: Option[UserGroupInformation] = None

  // Called when an idle session cleaner closes this session
  def closeWithException(msg: String): Unit = close()

  def close(): Unit = {
    // If multi-context mode enabled, stops a per-session context
    if (SQLServerUtils.checkIfMultiContextModeEnabled(_sqlContext.conf)) {
      _uiTab.foreach(_.detach())
      _sqlContext.sparkContext.stop()
    }
  }
}

trait SessionService {
  def openSession(userName: String, passwd: String, ipAddress: String, dbName: String,
    state: SessionState): Int
  def getSessionState(sessionId: Int): SessionState
  def closeSession(sessionId: Int): Unit
  def executeStatement(sessionId: Int, plan: (String, LogicalPlan)): Operation
}

private[service] case class TimeStampedValue[V](value: V, timestamp: Long)

private[service] class SessionManager(initSession: SessionInitializer)
    extends CompositeService with Logging {

  private val nextSessionId = new AtomicInteger(0)
  private val sessionIdToState = jSyncMap(new jHashMap[Int, TimeStampedValue[SessionState]]())

  // For functions to initialize sessions
  private var getSessionContext: String => SQLContext = _
  private var getServerListener: SQLContext => SQLServerListener = _
  private var getUiTab: (SQLContext, SQLServerListener) => Option[SQLServerTab] = _

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
    val singleSessionModeEnabled = conf.sqlServerSingleSessionEnabled
    val multiContextModeEnabled = SQLServerUtils.checkIfMultiContextModeEnabled(conf)

    getSessionContext = if (singleSessionModeEnabled) {
      // Single-session mode
      (_: String) => SQLServerEnv.sqlContext
    } else {
      if (multiContextModeEnabled) {
        // Multi-context mode for Kerberos impersonation
        (dbName: String) => {
          val sqlContext = SQLServerEnv.newSQLContext()
          initSession(dbName, sqlContext)
          sqlContext
        }
      } else {
        // Multi-session mode
         (dbName: String) => {
          val sqlContext = SQLServerEnv.sqlContext.newSession()
          initSession(dbName, sqlContext)
          sqlContext
        }
      }
    }
    getServerListener = if (multiContextModeEnabled) {
      // Multi-context mode for Kerberos impersonation
      (sqlContext: SQLContext) => SQLServerEnv.newSQLServerListener(sqlContext)
    } else {
      // Single/Multi-session mode
      (_: SQLContext) => SQLServerEnv.sqlServListener
    }
    getUiTab = if (multiContextModeEnabled) {
      // Multi-context mode for Kerberos impersonation
      (sqlContext: SQLContext, listener: SQLServerListener) => {
        SQLServerEnv.newUiTab(sqlContext, listener)
      }
    } else {
      // Single/Multi-session mode
      (_: SQLContext, _: SQLServerListener) => SQLServerEnv.uiTab
    }
  }

  override def doStart(): Unit = {
    if (SQLServerEnv.sqlContext.conf.sqlServerSingleSessionEnabled) {
      initSession("default", SQLServerEnv.sqlContext)
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
    val sqlContext = getSessionContext(dbName)
    val servListener = getServerListener(sqlContext)
    state._sessionId = sessionId
    state._sqlContext = sqlContext
    state._servListener = servListener
    state._uiTab = getUiTab(sqlContext, servListener)
    sqlContext.sharedState.externalCatalog.setCurrentDatabase(dbName)
    sessionIdToState.put(sessionId, TimeStampedValue(state, currentTime))
    servListener.onSessionCreated(sessionId, userName, ipAddress)
    sessionId
  }

  def closeSession(sessionId: Int): Unit = {
    val value = sessionIdToState.remove(sessionId)
    if (value != null) {
      val TimeStampedValue(state, _) = value
      state._servListener.onSessionClosed(sessionId)
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
      state._servListener.onSessionClosed(sessionId)
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

  private val operationExecutor = new PgExecutor()
  private val sessionManager = new SessionManager(new PgSessionInitializer())

  // We must start `PgProtocolService` in the end of services
  addService(new PgCatalogInitializer())
  addService(sessionManager)
  addService(new PgProtocolService(this))

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
    operationExecutor.newOperation(sessionManager.getSession(sessionId), plan)
  }
}
