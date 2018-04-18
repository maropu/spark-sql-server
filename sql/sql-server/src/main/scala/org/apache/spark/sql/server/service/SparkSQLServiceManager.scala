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
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.{SQLServer, SQLServerEnv}
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.util.RecurringTimer
import org.apache.spark.util.SystemClock


trait SessionInitializer {
  def apply(dbName: String, sqlContext: SQLContext): Unit
}

trait SessionState {

  // Holds a session-specific context
  private[service] var _sessionId: Int = _
  private[service] var _sqlContext: SQLContext = _

  // Called when an idel session cleaner closes this session
  def closeWithException(msg: String): Unit = close()
  def close(): Unit = {}
}

trait SessionService {
  def openSession(userName: String, passwd: String, ipAddress: String, dbName: String,
    state: SessionState): Int
  def getSessionState(sessionId: Int): SessionState
  def closeSession(sessionId: Int): Unit
  def executeStatement(sessionId: Int, plan: (String, LogicalPlan)): Operation
}

private[service] case class TimeStampedValue[V](value: V, timestamp: Long)

private[service] class SessionManager(pgServer: SQLServer, init: SessionInitializer)
    extends CompositeService with Logging {
  import SQLServer.{listener => servListener}

  private val IDLE_SESSION_CLEANUP_PERIOD = TimeUnit.MINUTES.toMillis(5) // 5min

  private val sessionIdToState = jSyncMap(new jHashMap[Int, TimeStampedValue[SessionState]]())

  private var getSession: String => SQLContext = _
  private var idleSessionCleanupDelay: Long = _
  private var idleSessionCleaner: RecurringTimer = _

  override def init(conf: SQLConf): Unit = {
    idleSessionCleanupDelay = conf.sqlServerIdleSessionCleanupDelay
    if (idleSessionCleanupDelay > 0) {
      idleSessionCleaner = startIdleSessionCleanupThread(IDLE_SESSION_CLEANUP_PERIOD)
    }
    getSession = if (conf.sqlServerSingleSessionEnabled) {
      (dbName: String) => {
        SQLServerEnv.sqlContext
      }
    } else {
      (dbName: String) => {
        val sqlContext = SQLServerEnv.sqlContext.newSession()
        init(dbName, sqlContext)
        sqlContext
      }
    }
  }

  // Just for sanity check
  override def start(): Unit = { require(SQLServerEnv.sqlContext != null) }

  override def stop(): Unit = {
    if (sessionIdToState.size() > 0) {
      logWarning(s"this service stopped though, ${sessionIdToState.size()} open sessions exist")
    }
  }

  def openSession(userName: String, passwd: String, ipAddress: String, dbName: String,
      state: SessionState): Int = {
    val sessionId = SQLServerEnv.newSessionId()
    val sqlContext = getSession(dbName)
    state._sessionId = sessionId
    state._sqlContext = sqlContext
    sqlContext.sharedState.externalCatalog.setCurrentDatabase(dbName)
    sessionIdToState.put(sessionId, TimeStampedValue(state, currentTime))
    servListener.onSessionCreated(sessionId, userName, ipAddress)
    sessionId
  }

  def closeSession(sessionId: Int): Unit = {
    val value = sessionIdToState.remove(sessionId)
    if (value != null) {
      val TimeStampedValue(state, _) = value
      servListener.onSessionClosed(sessionId)
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
      servListener.onSessionClosed(sessionId)
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

private[server] class SparkSQLServiceManager(
    sqlServer: SQLServer,
    executor: OperationExecutor,
    initializer: SessionInitializer) extends CompositeService with SessionService {

  private var sessionManager: SessionManager = _
  private var operationManager: OperationManager = _

  override def init(conf: SQLConf) {
    sessionManager = new SessionManager(sqlServer, initializer)
    addService(sessionManager)
    operationManager = new OperationManager(sqlServer, executor)
    addService(operationManager)
    super.init(conf)
  }

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
    operationManager.newExecuteStatementOperation(
      sessionManager.getSession(sessionId)._sqlContext, sessionId, plan)
  }
}
