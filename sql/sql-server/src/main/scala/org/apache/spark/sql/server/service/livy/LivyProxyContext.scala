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

package org.apache.spark.sql.server.service.livy

import java.net.URI

import scala.util.Random

import org.apache.livy.{LivyClient, LivyClientBuilder}

import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.sql.server.SQLServerEnv
import org.apache.spark.sql.server.service.SessionContext
import org.apache.spark.util.Utils


class LivyProxyContext extends SessionContext with Logging {

  private var livyClient: LivyClient = _
  private var rpcEnv: RpcEnv = _

  // TODO: Add logics to reopen a session in case of any failure (e.g., Spark job crushes)
  private[livy] var rpcEndpoint: RpcEndpointRef = _

  def init(serviceName: String, sessionId: Int, dbName: String): Unit = {
    livyClient = LivyProxyContext.retryRandom(numRetriesLeft = 32, maxBackOffMillis = 10000) {
      new LivyClientBuilder()
        .setURI(new URI(LivyServerService.LIVY_SERVER_URI))
        .build()
    }
    val conf = SQLServerEnv.sparkConf
    val startServiceFunc = (port: Int) => {
      val service = RpcEnv.create(serviceName, "localhost", port, conf, new SecurityManager(conf))
      (service, port)
    }
    val (_rpcEnv, port) = Utils.startServiceOnPort[RpcEnv](
      startPort = 10000, startServiceFunc, conf, serviceName)
    logInfo(s"RpcEnv '$serviceName' started on port $port")
    val endpointRef = livyClient.submit(new OpenSessionJob(sessionId, dbName)).get()
    rpcEnv = _rpcEnv
    rpcEndpoint = rpcEnv.setupEndpointRef(endpointRef.address, OpenSessionJob.ENDPOINT_NAME)
  }

  def stop(): Unit = {
    if (rpcEnv != null) {
      rpcEnv.shutdown()
    }
    if (livyClient != null) {
      livyClient.stop(true)
    }
  }
}

object LivyProxyContext extends Logging {

  @annotation.tailrec
  def retryRandom[T](numRetriesLeft: Int, maxBackOffMillis: Int)(expression: => T): T = {
    util.Try { expression } match {
      // The function succeeded, evaluate to x
      case util.Success(x) => x

      // The function failed, either retry or throw the exception
      case util.Failure(e) => e match {
        // Retry: Throttling or other Retryable exception has occurred
        case _: RuntimeException if numRetriesLeft > 1 =>
          val backOffMillis = Random.nextInt(maxBackOffMillis)
          Thread.sleep(backOffMillis)
          logWarning(s"Retryable Exception: Random backOffMillis=$backOffMillis")
          retryRandom(numRetriesLeft - 1, maxBackOffMillis)(expression)

        // Throw: Unexpected exception has occurred
        case _ => throw e
      }
    }
  }
}
