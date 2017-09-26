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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.{SQLServer, SQLServerEnv}
import org.apache.spark.sql.server.SQLServerConf._


trait SessionInitializer {
  def apply(dbName: String, sqlContext: SQLContext): Unit
}

trait SessionState {
  def close(): Unit = {}
}

trait SessionService {
  def openSession(userName: String, passwd: String, ipAddress: String, dbName: String,
    state: SessionState): Int
  def getSessionState(sessionId: Int): SessionState
  def closeSession(sessionId: Int): Unit
  def executeStatement(sessionId: Int, plan: (String, LogicalPlan), isCursor: Boolean): Operation
}

private[service] class SessionManager(pgServer: SQLServer, init: SessionInitializer)
    extends CompositeService {

  private val sessionIdToState = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Int, (SQLContext, SessionState)]())

  private var getSession: String => SQLContext = _

  override def init(conf: SQLConf): Unit = {
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
    SQLServer.listener.onSessionCreated(sessionId, userName, ipAddress)
    val sqlContext = getSession(dbName)
    sqlContext.sharedState.externalCatalog.setCurrentDatabase(dbName)
    sessionIdToState.put(sessionId, (sqlContext, state))
    sessionId
  }

  def closeSession(sessionId: Int): Unit = {
    require(sessionIdToState.containsKey(sessionId))
    SQLServer.listener.onSessionClosed(sessionId)
    val state = sessionIdToState.remove(sessionId)
    state._2.close()
  }

  def getSession(sessionId: Int): (SQLContext, SessionState) = {
    require(sessionIdToState.containsKey(sessionId))
    sessionIdToState.get(sessionId)
  }
}

private[server] class SparkSQLSessionService(
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
    sessionManager.getSession(sessionId)._2
  }

  override def closeSession(sessionId: Int): Unit = {
    sessionManager.closeSession(sessionId)
  }

  override def executeStatement(
      sessionId: Int, plan: (String, LogicalPlan), isCursor: Boolean): Operation = {
    operationManager.newExecuteStatementOperation(
      sessionManager.getSession(sessionId)._1,
      sessionId,
      plan,
      isCursor)
  }
}
