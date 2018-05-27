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

import org.apache.livy.{LivyClient, LivyClientBuilder}

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.service.SessionContext


class LivyProxyContext(sqlConf: SQLConf, livyService: LivyServerService)
    extends SessionContext with Logging {

  private var livyClient: LivyClient = _

  // TODO: Adds logics to reopen a session in case of any failure (e.g., Spark job crushes)
  private[livy] var rpcEndpoint: RpcEndpointRef = _

  private def sparkConfBlacklist: Seq[String] = Seq(
    "spark.sql.server.",
    "spark.master",
    "spark.jars",
    "spark.submit.deployMode"
  )

  def init(serviceName: String, sessionId: Int, dbName: String): Unit = {
    livyClient = LivyProxyContext.retryRandom(numRetriesLeft = 32, maxBackOffMillis = 10000) {
      var builder = new LivyClientBuilder()
        .setURI(new URI(LivyServerService.LIVY_SERVER_URI))

      // Passes configurations in the SQL server into `SQLContext` that Livy launches
      val sparkConfMap = sqlConf.settings.asScala.filterNot {
        case (key, _) => sparkConfBlacklist.exists(key.contains)
      }
      logInfo(
        s"""Spark properties for the SQLContext that Livy launches:
           |  ${sparkConfMap.map { case (k, v) => s"key=$k value=$v" }.mkString("\n  ")}
         """.stripMargin)
      sparkConfMap.foreach { case (key, value) =>
        builder = builder.setConf(key, value)
      }
      builder.build()
    }
    val endpointRef = livyClient.submit(new OpenSessionJob(sessionId, dbName)).get()
    rpcEndpoint = livyService.rpcEnv.setupEndpointRef(
      endpointRef.address, OpenSessionJob.ENDPOINT_NAME)
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
