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

import java.io.File
import java.nio.charset.StandardCharsets
import java.sql._
import java.util.Properties

import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Random
import scala.util.Try
import scala.util.control.NonFatal

import com.google.common.io.Files

import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

class SparkPostgreSQLServerTest(
    name: String,
    val ssl: Boolean = false,
    singleSession: Boolean = false,
    options: Map[String, String] = Map.empty)
  extends Logging {

  private val className = SQLServer.getClass.getCanonicalName.stripSuffix("$")
  private val logFileMask = s"starting $className, logging to "
  private val successStartLines = Set(
    "PostgreSQLService: Start running the SQL server",
    "Recovery mode 'ZOOKEEPER' enabled"
  )
  private val startScript = "../../sbin/start-sql-server.sh".split("/").mkString(File.separator)
  private val stopScript = "../../sbin/stop-sql-server.sh".split("/").mkString(File.separator)
  private val pidDir = Utils.createTempDir(s"sqlserver-$name-pid")

  private var logTailingProcess: Process = _
  private var diagnosisBuffer = mutable.ArrayBuffer.empty[String]

  var logPath: File = _
  var listeningPort: Int = _

  def start(): Unit = {
    // Chooses a random port between 10000 and 19999
    listeningPort = 10000 + Random.nextInt(10000)

    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 3).foldLeft(Try(tryToStart(listeningPort, 0))) { case (started, attempt) =>
      started.orElse {
        listeningPort += 1
        stop()
        Try(tryToStart(listeningPort, attempt))
      }
    }.recover {
      case NonFatal(e) =>
        dumpServerLogs()
        throw e
    }.get

    logInfo("SQLServer started successfully")
  }

  private def serverStartCommand(port: Int) = {
    val testTempDirPath = {
      // Writes a temporary log4j.properties and prepend it to driver classpath, so that it
      // overrides all other potential log4j configurations contained in other dependency jar files
      val tempDir = Utils.createTempDir().getCanonicalPath
      Files.write(
        """log4j.rootCategory=INFO, console
          |log4j.appender.console=org.apache.log4j.ConsoleAppender
          |log4j.appender.console.target=System.err
          |log4j.appender.console.layout=org.apache.log4j.PatternLayout
          |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
        """.stripMargin,
        new File(s"$tempDir/log4j.properties"),
        StandardCharsets.UTF_8)

      tempDir
    }

    s"""$startScript
       |  --master local
       |  --driver-class-path $testTempDirPath
       |  --driver-java-options -Dlog4j.debug
       |  --conf spark.ui.enabled=false
       |  --conf spark.sql.warehouse.dir=$testTempDirPath/spark-warehouse
       |  --conf ${SQLServerConf.SQLSERVER_PORT.key}=$port
       |  --conf ${SQLServerConf.SQLSERVER_SSL_ENABLED.key}=$ssl
       |  --conf ${SQLServerConf.SQLSERVER_SINGLE_SESSION_ENABLED.key}=$singleSession
     """.stripMargin.split("\\s+").toSeq ++
      options.flatMap { case (k, v) => Iterator("--conf", s"$k=$v") }
  }

  private def tryToStart(port: Int, attempt: Int): Unit = {
    logPath = null
    logTailingProcess = null

    val command = serverStartCommand(port)

    diagnosisBuffer ++=
      s"""
         |### Attempt $attempt ###
         |SQLServer command line: $command
         |Listening port: $port
       """.stripMargin.split("\n")

    logInfo(s"Trying to start SQLServer: port=$port, attempt=$attempt")

    logPath = {
      val lines = Utils.executeAndGetOutput(
        command = command,
        extraEnvironment = Map(
          // Disables SPARK_TESTING to exclude log4j.properties in test directories
          "SPARK_TESTING" -> "0",
          // But set SPARK_SQL_TESTING to make spark-class happy
          "SPARK_SQL_TESTING" -> "1",
          // Points SPARK_PID_DIR to SPARK_HOME, otherwise only 1 SQL server instance can be
          // started at a time, which is not Jenkins friendly
          "SPARK_PID_DIR" -> pidDir.getCanonicalPath,
          // For submit multiple jobs
          "SPARK_IDENT_STRING" -> name
        ),
        redirectStderr = true)

      logInfo(s"COMMAND: $command")
      logInfo(s"OUTPUT: $lines")
      lines.split("\n").collectFirst {
        case line if line.contains(logFileMask) => new File(line.drop(logFileMask.length))
      }.getOrElse {
        throw new RuntimeException("Failed to find SQLServer log file.")
      }
    }

    val serverStarted = Promise[Unit]()

    // Ensures that the following "tail" command won't fail
    logPath.createNewFile()

    logTailingProcess = {
      val command = s"/usr/bin/env tail -n +0 -f ${logPath.getCanonicalPath}".split(" ")
      // Using "-n +0" to make sure all lines in the log file are checked
      val builder = new ProcessBuilder(command: _*)
      val captureOutput: (String) => Unit = (line: String) => diagnosisBuffer.synchronized {
        diagnosisBuffer += line
        if (successStartLines.exists(line.contains(_))) {
          serverStarted.trySuccess(())
        }
      }
      val process = builder.start()
      new ProcessOutputCapturer(process.getInputStream, captureOutput).start()
      new ProcessOutputCapturer(process.getErrorStream, captureOutput).start()
      process
    }

    ThreadUtils.awaitResult(serverStarted.future, 1.minutes)
  }

  def stop(): Unit = {
    // The `spark-daemon.sh' script uses kill, which is not synchronous, have to wait for a while
    Utils.executeAndGetOutput(
      command = Seq(stopScript),
      extraEnvironment = Map(
        "SPARK_PID_DIR" -> pidDir.getCanonicalPath,
        "SPARK_IDENT_STRING" -> name
      ))
    Thread.sleep(3.seconds.toMillis)

    Option(logPath).foreach(_.delete())
    logPath = null
    Option(logTailingProcess).foreach(_.destroy())
    logTailingProcess = null
  }

  def dumpServerLogs(): Unit = {
    logError(
      s"""
         |=====================================
         |PostgreSQLJdbcSuite  failure output
         |=====================================
         |${diagnosisBuffer.mkString("\n")}
         |=========================================
         |End PostgreSQLJdbcSuite failure output
         |=========================================
       """.stripMargin)
  }
}

trait PostgreSQLJdbcTestBase {

  // Register a JDBC driver for PostgreSQL
  Utils.classForName(classOf[org.postgresql.Driver].getCanonicalName)

  private lazy val jdbcUri = s"jdbc:postgresql://localhost:${serverInstance.listeningPort}/default"

  def serverInstance: SparkPostgreSQLServerTest

  private def getJdbcConnect(): Connection = {
    val props = new Properties()
    props.put("user", System.getProperty("user.name"))
    props.put("password", "")
    if (serverInstance.ssl) {
      props.put("ssl", "true")
      props.put("sslfactory", "org.postgresql.ssl.NonValidatingFactory")
    }
    DriverManager.getConnection(jdbcUri, props)
  }

  def testMultipleConnectionJdbcStatement(fs: (Statement => Unit)*) {
    val connections = fs.map { _ => getJdbcConnect() }
    val statements = connections.map(_.createStatement())
    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  def testJdbcStatement(f: Statement => Unit): Unit = {
    testMultipleConnectionJdbcStatement(f)
  }

  def testJdbcStatementWitConf(options: (String, String)*)(f: Statement => Unit) {
    val connection = getJdbcConnect()
    val statement = connection.createStatement()

    val (keys, values) = options.unzip
    val currentValues = keys.map { key =>
      val rs = statement.executeQuery(s"SET $key")
      if (rs.next()) { rs.getString(2) } else { assert(false, s"Invalue key detected: $key") }
    }
    options.foreach { case (key, value) =>
      statement.execute(s"SET $key=$value")
    }
    try f(statement) finally {
      keys.zip(currentValues).foreach {
        case (key, value) => statement.execute(s"SET $key=$value")
      }
      statement.close()
      connection.close()
    }
  }
}
