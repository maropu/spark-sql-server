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
import java.util.concurrent.ExecutionException

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.livy.{LivyClient, LivyClientBuilder}

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.SessionContext
import org.apache.spark.sql.server.util.SQLServerUtils


class LivyProxyContext(sqlConf: SQLConf, livyService: LivyServerService)
    extends SessionContext with Logging {

  private var connectMethod: Option[() => Unit] = None
  private var livyClient: LivyClient = _
  private var rpcEndpoint: RpcEndpointRef = _

  private val sparkConfBlacklist: Seq[String] = Seq(
    "spark.sql.server.",
    "spark.master",
    "spark.jars",
    "spark.submit.deployMode"
  )

  def init(serviceName: String, sessionId: Int, userName: String, dbName: String): Unit = {
    logInfo(s"serviceName=$serviceName sessionId=$sessionId dbName=$dbName")

    // Configurations that Livy passes into `SQLContext`
    val sparkConf = sqlConf.settings.asScala.filterNot {
      case (key, _) => sparkConfBlacklist.exists(key.contains)
    } ++ Map(
      "spark.rpc.askTimeout" -> "720s"
      // We need to have at least two threads for Spark Netty RPC: task thread + cancel thread
      // "spark.rpc.netty.dispatcher.numThreads" -> "2",
      // "spark.rpc.io.numConnectionsPerPeer" -> "2",
      // "spark.rpc.io.threads" -> "2"
    )
    val livyClientConf = Seq(
      "job-cancel.trigger-interval" -> "100ms",
      "job-cancel.timeout" -> "24h"
    )
    logInfo(
      s"""Spark properties for the SQLContext that Livy launches:
         |  ${sparkConf.map { case (k, v) => s"key=$k value=$v" }.mkString("\n  ")}
       """.stripMargin)

    val _connectMethod = () => {
      // Submits a job to open a session, initializes a RPC endpoint in the session, and
      // registers the endpoint in `RpcEnv`.
      val (_livyClient, _rpcEndpoint) =
          LivyProxyContext.retryRandom(numRetriesLeft = 4, maxBackOffMillis = 10000) {
        // Starts a Livy session
        val livyUrl = s"http://${sqlConf.sqlServerLivyHost}:${sqlConf.sqlServerLivyPort}"
        var builder = new LivyClientBuilder().setURI(new URI(livyUrl))

        (livyClientConf ++ sparkConf).foreach { case (key, value) =>
          builder = builder.setConf(key, value)
        }

        // If Kerberos and impersonation enabled, sets `userName` at `proxy-user`
        if (SQLServerUtils.isKerberosEnabled(sqlConf) && sqlConf.sqlServerImpersonationEnabled) {
          logInfo(s"Kerberos and impersonation enabled: proxy-user=$userName")
          builder = builder.setConf("proxy-user", userName)
        }

        val client = builder.build()

        val endpoint = try {
          // Submits a job to open a session and initializes a RPC endpoint
          val endpointRef = client.submit(new OpenSessionJob(sessionId, dbName)).get()
          // Then, registers the endpoint in `RpcEnv`
          livyService.rpcEnv.setupEndpointRef(
            endpointRef.address, OpenSessionJob.ENDPOINT_NAME)
        } catch {
          case e: Throwable =>
            client.stop(true)
            throw e
        }

        (client, endpoint)
      }

      livyClient = _livyClient
      rpcEndpoint = _rpcEndpoint
    }

    connectMethod = Some(_connectMethod)
  }

  def connect(): Unit = {
    val func = connectMethod.getOrElse {
      sys.error("init() should be called before connect() called.")
    }
    func()
  }

  def ask(message: AnyRef): AnyRef = {
    var failCount = 0
    var success = false
    var result: AnyRef = null
    while (!success) {
      try {
        result = rpcEndpoint.askSync[AnyRef](message)
        success = true
      } catch {
        case NonFatal(_) if failCount < 3 =>
          logWarning(s"Livy RPC failed, so try to start a Spark job again...")
          failCount += 1
          connect()
        case e =>
          logError(s"Livy RPC failed ${sqlConf.sqlServerLivyRpcFailThreshold} times")
          throw e
      }
    }
    result
  }

  def stop(): Unit = {
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
        case _: RuntimeException | _: ExecutionException if numRetriesLeft > 1 =>
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
