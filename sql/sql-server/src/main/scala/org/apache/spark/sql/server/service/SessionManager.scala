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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.server.{SQLServer, SQLServerEnv}
import org.apache.spark.sql.server.SQLServerConf._


private[server] class SessionManager(pgServer: SQLServer) extends CompositeService {

  private val sessionIdToContext = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Int, SQLContext]())

  private var getSession: () => SQLContext = _

  override def init(conf: SparkConf): Unit = {
    getSession = if (conf.sqlServerSingleSessionEnabled) {
      () => SQLServerEnv.sqlContext
    } else {
      () => {
        val sqlCtx = SQLServerEnv.sqlContext.newSession()
        // TODO: Re-think the design
        postgresql.Metadata.initSystemFunctions(sqlCtx)
        sqlCtx
      }
    }
  }

  // Just for sanity check
  override def start(): Unit = { require(SQLServerEnv.sqlContext != null) }

  override def stop(): Unit = {
    if (sessionIdToContext.size() > 0) {
      logWarning(s"this service stopped though, ${sessionIdToContext.size()} open sessions exist")
    }
  }

  def openSession(userName: String, passwd: String, ipAddress: String): Int = {
    val sessionId = SQLServerEnv.newSessionId()
    SQLServer.listener.onSessionCreated(sessionId, userName, ipAddress)
    sessionIdToContext.put(sessionId, getSession())
    sessionId
  }

  def closeSession(sessionId: Int): Unit = {
    SQLServer.listener.onSessionClosed(sessionId)
    sessionIdToContext.remove(sessionId)
  }

  def getSession(sessionId: Int): SQLContext = {
    require(sessionIdToContext.containsKey(sessionId))
    sessionIdToContext.get(sessionId)
  }
}
