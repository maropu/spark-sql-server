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

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf._


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

private[server] class SQLServerListener(conf: SQLConf) extends SparkListener {

  private var onlineSessionNum = 0
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
