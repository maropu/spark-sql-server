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
import org.apache.spark.sql.server.SQLServer


private[server] class OperationManager(pgServer: SQLServer) extends CompositeService {

  private val sessionIdToOperations = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Int, java.util.ArrayList[Operation]]())
  private val sessionIdToActivePool = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Int, String]())

  def newExecuteStatementOperation(
      sqlContext: SQLContext,
      sessionId:
      Int, statement: String): ExecuteStatementOperation = {
    val operation = new ExecuteStatementOperation(
      sessionId, statement)(sqlContext, sessionIdToActivePool)
    if (!sessionIdToOperations.containsKey(sessionId)) {
      sessionIdToOperations.put(sessionId, new java.util.ArrayList())
    }
    sessionIdToOperations.get(sessionId).add(operation)
    logDebug(s"Created Operation for $statement with sessionId=$sessionId")
    operation
  }
}
