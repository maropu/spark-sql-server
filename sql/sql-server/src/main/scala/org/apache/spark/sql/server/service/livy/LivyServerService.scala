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

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.TimeUnit.SECONDS

import scala.collection.mutable

import com.google.common.io.Files

import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.SQLServerEnv
import org.apache.spark.sql.server.service.{CompositeService, FrontendService}
import org.apache.spark.sql.server.util.SQLServerUtils
import org.apache.spark.util.Utils


private[service] class LivyServerService(frontend: FrontendService) extends CompositeService {
  import LivyServerService._

  // TODO: Makes this variable configurable
  private val LIVY_PROCESS_FAIL_THRESHOLD = 5

  private var sparkJar: String = _
  private var livyHome: String = _
  private var livyConfDir: String = _
  private var livyStartScript: String = _
  private var livyProcess: Process = _
  private var livyProcessFailCount: Int = 0
  private var livyThread: Thread = _

  private[livy] var rpcEnv: RpcEnv = _

  private def kerberosParams(conf: SQLConf): String = {
    Seq(
      "livy.server.auth.type" -> "kerberos",
      "livy.impersonation.enabled" -> conf.sqlServerImpersonationEnabled,
      "livy.server.auth.kerberos.principal" -> SQLServerUtils.kerberosPrincipal(conf),
      "livy.server.auth.kerberos.keytab" -> SQLServerUtils.kerberosKeytab(conf),
      "livy.server.launch.kerberos.principal" -> SQLServerUtils.kerberosPrincipal(conf),
      "livy.server.launch.kerberos.keytab" -> SQLServerUtils.kerberosKeytab(conf)
    ).map { case (key, value) =>
      s"$key = $value"
    }.mkString("\n")
  }

  override def doInit(conf: SQLConf): Unit = {
    sparkJar = conf.settings.get("spark.jars")
    livyStartScript = s"${conf.sqlServerLivyHome}/$LIVY_START_SCRIPT"
    livyHome = conf.sqlServerLivyHome
    if (!new File(livyStartScript).exists()) {
      throw new IllegalArgumentException(s"'$livyStartScript' not found: " +
        s"`${SQLServerConf.SQLSERVER_LIVY_HOME.key}` not defined correctly.")
    }
    val livyLogDir = new File(conf.sqlServerLivyHome, "logs")
    if (!livyLogDir.exists && !livyLogDir.mkdir) {
      throw new RuntimeException("Cannot create a livy log directory.")
    }

    val sparkHome = sys.env.getOrElse("SPARK_HOME", {
      throw new IllegalArgumentException("SPARK_HOME not defined correctly.")
    })

    val hiveSiteFileOption = SQLServerUtils.findFileOnClassPath("hive-site.xml")
    if (hiveSiteFileOption.isEmpty) {
      val hiveSiteFile = new File(s"$sparkHome/conf", "hive-site.xml")
      // If `hive-site.xml` not found in classpath, uses an in-memory derby metastore
      val metastoreURL = s"jdbc:derby:memory:;databaseName=${UUID.randomUUID()};create=true"
      val hiveSite =
        s"""<configuration>
           |  <property>
           |    <name>javax.jdo.option.ConnectionURL</name>
           |    <value>$metastoreURL</value>
           |  </property>
           |</configuration>
         """.stripMargin
      Files.write(hiveSite, hiveSiteFile, StandardCharsets.UTF_8)
      logInfo(
        s"""Created a hive-site.xml for Hive context in $sparkHome/conf:
           |$hiveSite
         """.stripMargin)
    } else {
      logInfo(s"hive-site.xml found in ${hiveSiteFileOption.map(_.getParentFile.getAbsolutePath)}")
    }

    // TODO: Reconsiders this: Why Livy doesn't set these jars correctly?
    val livyRscJars = {
      val rscJarsDir = new File(livyHome, "rsc-jars")
      require(rscJarsDir.isDirectory, "Cannot find 'client-jars' directory under LIVY_HOME.")
      val jars = mutable.ArrayBuffer[String]()
      for (f <- rscJarsDir.listFiles()) {
        jars += f.getAbsolutePath
      }
      jars.mkString(",")
    }

    val master = conf.settings.get("spark.master")
    val sparkVersion = org.apache.spark.SPARK_VERSION
    val scalaVersion = sys.env.getOrElse("SPARK_SCALA_VERSION", {
      throw new IllegalArgumentException("SPARK_SCALA_VERSION not defined correctly.")
    })
    // Basically, `SessionManager` automatically closes idle Livy sessions, so we double a
    // `spark.sql.server.idleSessionCleanupDelay` value and set the value
    // at `livy.server.session.timeout`.
    val livySessionTimeout = 2 * conf.sqlServerIdleSessionCleanupDelay

    livyConfDir = {
      val tempDir = Utils.createTempDir(namePrefix = "livy").getCanonicalPath

      // Creates a state-store directory for recovery
      val stateStoreDir = new File(tempDir, "state-store")
      if (!stateStoreDir.mkdir()) {
        throw new RuntimeException("Cannot create a state-store directory for recovery.")
      }

      import SQLServerUtils._
      val livyConfPath = s"$tempDir/livy.conf"
      val livyConf =
        s"""# Livy settings
           |livy.server.spark-home = $sparkHome
           |livy.spark.master = $master
           |# livy.spark.deploy-mode = cluster
           |# livy.spark.scala-version = $scalaVersion
           |livy.spark.version = $sparkVersion
           |livy.server.host = $LIVY_SERVER_HOST
           |livy.server.port = $LIVY_SERVER_PORT
           |livy.ui.enabled = false
           |# The SQL server needs the Hive support
           |livy.repl.enable-hive-context = true
           |livy.rsc.jars = $livyRscJars,$sparkJar
           |livy.server.session.timeout-check = true
           |livy.server.session.timeout = $livySessionTimeout
           |livy.server.session.state-retain.sec = 0s
           |# Recovery settings
           |livy.server.recovery.mode = off
           |# livy.server.recovery.mode = ${if (isRunningOnYarn(conf)) "on" else "off"}
           |livy.server.recovery.state-store = filesystem
           |livy.server.recovery.state-store.url = file://${stateStoreDir.getAbsolutePath}
           |# Kerberos settings
           |${if (isKerberosEnabled(conf)) kerberosParams(conf) else ""}
           |
           |# Spark settings
           |spark.jars = $sparkJar
         """.stripMargin
      Files.write(livyConf, new File(livyConfPath), StandardCharsets.UTF_8)
      logInfo(
        s"""Created a conf file for Livy in '$livyConfPath':
           |$livyConf
         """.stripMargin)
      tempDir
    }
    SQLServerUtils.injectEnvVar("LIVY_HOME", livyHome)
    SQLServerUtils.injectEnvVar("LIVY_CONF_DIR", livyConfDir)
  }

  private def fail(e: Throwable): Unit = {
    frontend.bossGroup.next().newPromise().tryFailure(e)
    frontend.bossGroup.shutdownGracefully()
  }

  // TODO: Adds logics to attach to an existing Livy process
  override def doStart(): Unit = {
    val livyTask = new Runnable() {
      override def run(): Unit = {
        try {
          while (livyProcessFailCount < LIVY_PROCESS_FAIL_THRESHOLD) {
            livyProcess = Utils.executeCommand(
              command = livyStartScript :: Nil,
              extraEnvironment = Map("LIVY_HOME" -> livyHome, "LIVY_CONF_DIR" -> livyConfDir))
            val exitCode = livyProcess.waitFor()
            logWarning(s"Livy process exited with code $exitCode and try to restart...")
            livyProcessFailCount += 1
          }
          val errMsg = s"Livy failed $LIVY_PROCESS_FAIL_THRESHOLD times and " +
            "try to stop the SQL server..."
          logWarning(errMsg)
          fail(new IOException(errMsg))
        } catch {
          case _: InterruptedException =>
            // we expect `doStop` throws this exception
            logInfo("Waiting thread interrupted for shutdown.")
        }
      }
    }
    livyThread = new Thread(livyTask)
    livyThread.setDaemon(true)
    livyThread.setName(this.getClass.getSimpleName)
    livyThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        logError(s"Thread threw exception.", e)
        fail(e)
      }
    })
    livyThread.start()

    // Creates `RpcEnv` for connections to a Livy server
    val sparkConf = SQLServerEnv.sparkConf
    val startServiceFunc = (port: Int) => {
      val service = RpcEnv.create(LivyServerService.LIVY_SERVICE_NAME, "localhost", port, sparkConf,
        new SecurityManager(sparkConf))
      (service, port)
    }
    // TODO: startPort = 0?
    val (_rpcEnv, port) = Utils.startServiceOnPort[RpcEnv](
      startPort = 12345, startServiceFunc, sparkConf, LivyServerService.LIVY_SERVICE_NAME)
    logInfo(s"RpcEnv '${LivyServerService.LIVY_SERVICE_NAME}' started on port $port")
    rpcEnv = _rpcEnv
  }

  override def doStop(): Unit = {
    require(livyProcess != null && livyThread != null)
    if (rpcEnv != null) {
      rpcEnv.shutdown()
    }
    if (livyProcess != null) {
      livyProcess.destroy()
    }
    if (livyThread != null) {
      livyThread.interrupt()
      try {
        livyThread.join(SECONDS.toMillis(10))
      } catch {
        case _: InterruptedException =>
          logWarning("Interrupted before Livy thread was finished.");
      }
      if (livyThread.isAlive) {
        logWarning("Failed to stop Livy thread.")
      }
    }
  }
}

private[livy] object LivyServerService extends Logging {

  val LIVY_START_SCRIPT = "bin/livy-server"
  val LIVY_SERVER_HOST = "0.0.0.0"
  val LIVY_SERVER_PORT = 8998
  val LIVY_SERVER_URI = s"http://$LIVY_SERVER_HOST:$LIVY_SERVER_PORT"
  val LIVY_SERVICE_NAME = "livy-server-service-rpc"
}
