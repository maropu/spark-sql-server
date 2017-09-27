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

import java.util.{Map => jMap}
import javax.annotation.concurrent.ThreadSafe

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.server.{SQLServer, SQLServerEnv}
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.types.StructType


sealed trait OperationState
case object INITIALIZED extends OperationState
case object RUNNING extends OperationState
case object FINISHED extends OperationState
case object CANCELED extends OperationState
case object CLOSED extends OperationState
case object ERROR extends OperationState
case object UNKNOWN extends OperationState
case object PENDING extends OperationState

sealed trait OperationType {
  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

object BEGIN extends OperationType
object FETCH extends OperationType
object SELECT extends OperationType

@ThreadSafe
trait Operation {

  private val timeout = SQLServerEnv.sqlConf.sqlServerIdleOperationTimeout
  private var lastAccessTime: Long = System.currentTimeMillis()

  protected var state: OperationState = INITIALIZED

  // TODO: Remove this IF because I think we could get a query schema before running queries
  def outputSchema(): StructType
  def run(): Iterator[InternalRow]
  def cancel(): Unit
  def close(): Unit

  protected def setState(newState: OperationState): Unit = {
    lastAccessTime = System.currentTimeMillis()
    state = newState
  }

  def isTimeOut(current: Long): Boolean = {
    if (timeout == 0) {
      true
    } else if (timeout > 0) {
      Seq(FINISHED, CANCELED, CLOSED, ERROR).contains(state) &&
        lastAccessTime + timeout <= current
    } else {
      lastAccessTime + -timeout <= current
    }
  }
}

trait OperationExecutor {

  // Create a new instance for service-specific operations
  def newOperation(
    sqlContext: SQLContext,
    sessionId: Int,
    plan: (String, LogicalPlan), // (query statement, analyzed logical plan)
    activePools: jMap[Int, String]): Operation
}

private[service] class OperationManager(pgServer: SQLServer, executor: OperationExecutor)
    extends CompositeService {

  private val sessionIdToOperations = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Int, java.util.ArrayList[Operation]]())
  private val sessionIdToActivePool = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Int, String]())

  def newExecuteStatementOperation(
      sqlContext: SQLContext,
      sessionId: Int,
      plan: (String, LogicalPlan)): Operation = {
    val op = executor.newOperation(sqlContext, sessionId, plan, sessionIdToActivePool)
    if (!sessionIdToOperations.containsKey(sessionId)) {
      sessionIdToOperations.put(sessionId, new java.util.ArrayList())
    }
    sessionIdToOperations.get(sessionId).add(op)
    op
  }
}
