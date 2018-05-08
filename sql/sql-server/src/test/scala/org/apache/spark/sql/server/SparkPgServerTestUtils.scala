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
import java.util.{Properties, UUID}

import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.{Random, Try}
import scala.util.control.NonFatal

import com.google.common.io.Files
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}


/**
 * A base class that manages a pair of JDBC driver connections and a SQL server instance.
 */
class PgJdbcTest(
    override val pgVersion: String = "9.6",
    override val ssl: Boolean = false,
    override val queryQueryMode: String = "extended",
    override val singleSession: Boolean = false,
    override val incrementalCollect: Boolean = true) extends SQLServerTest with PgJdbcTestBase {

  override val serverInstance: SparkPgSQLServerTest = server
}

/**
 * A base class for a SQL server instance.
 */
abstract class SQLServerTest extends SparkFunSuite with BeforeAndAfterAll with Logging {

  // Parameters for the Spark SQL server
  val pgVersion: String
  val ssl: Boolean
  val singleSession: Boolean
  val incrementalCollect: Boolean

  protected val server = new SparkPgSQLServerTest(
    name = this.getClass.getSimpleName,
    pgVersion = pgVersion,
    ssl = ssl,
    singleSession = singleSession,
    incrementalCollect = incrementalCollect)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    server.start()
    logInfo("SQLServer started successfully")
  }

  override protected def afterAll(): Unit = {
    try {
      server.stop()
      logInfo("SQLServer stopped")
    } finally {
      super.afterAll()
    }
  }
}

class SparkPgSQLServerTest(
    name: String,
    pgVersion: String,
    val ssl: Boolean,
    singleSession: Boolean,
    incrementalCollect: Boolean,
    options: Map[String, String] = Map.empty)
  extends Logging {

  private val className = SQLServer.getClass.getCanonicalName.stripSuffix("$")
  private val logFileMask = s"starting $className, logging to "
  private val successStartLines = Set(
    "PgProtocolService: Start running the SQL server",
    "Recovery mode 'ZOOKEEPER' enabled"
  )
  private val startScript = "../../sbin/start-sql-server.sh".split("/").mkString(File.separator)
  private val stopScript = "../../sbin/stop-sql-server.sh".split("/").mkString(File.separator)

  private val testTempDir = {
     val tempDir = Utils.createTempDir(namePrefix = UUID.randomUUID().toString).getCanonicalPath

     // Write a hive-site.xml containing a setting of `hive.metastore.warehouse.dir`
     val metastoreURL =
       s"jdbc:derby:memory:;databaseName=$tempDir;create=true"
     Files.write(
       s"""
         |<configuration>
         |  <property>
         |    <name>javax.jdo.option.ConnectionURL</name>
         |    <value>$metastoreURL</value>
         |  </property>
         |</configuration>
       """.stripMargin,
       new File(s"$tempDir/hive-site.xml"),
       StandardCharsets.UTF_8)

    // Writes a temporary log4j.properties and prepend it to driver classpath, so that it
    // overrides all other potential log4j configurations contained in other dependency jar files
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
    s"""$startScript
       | --master local
       | --driver-class-path $testTempDir
       | --driver-java-options -Dlog4j.debug
       | --conf spark.ui.enabled=false
       | --conf spark.sql.warehouse.dir=$testTempDir/spark-warehouse
       | --conf ${SQLServerConf.SQLSERVER_PORT.key}=$port
       | --conf ${SQLServerConf.SQLSERVER_VERSION.key}=$pgVersion
       | --conf ${SQLServerConf.SQLSERVER_SSL_ENABLED.key}=$ssl
       | --conf ${SQLServerConf.SQLSERVER_SINGLE_SESSION_ENABLED.key}=$singleSession
       | --conf ${SQLServerConf.SQLSERVER_PSQL_ENABLED.key}=true
       | --conf ${SQLServerConf.SQLSERVER_INCREMENTAL_COLLECT_ENABLED.key}=$incrementalCollect
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
          "SPARK_PID_DIR" -> testTempDir,
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
        "SPARK_PID_DIR" -> testTempDir,
        "SPARK_IDENT_STRING" -> name
      ))
    Thread.sleep(3.seconds.toMillis)

    Option(logPath).foreach(_.delete())
    logPath = null
    Option(logTailingProcess).foreach(_.destroy())
    logTailingProcess = null
  }

  private def dumpServerLogs(): Unit = {
    logError(
      s"""
         |=====================================
         |PgJdbcSuite  failure output
         |=====================================
         |${diagnosisBuffer.mkString("\n")}
         |=========================================
         |End PgJdbcSuite failure output
         |=========================================
       """.stripMargin)
  }
}

/**
 * A trait for JDBC driver connections.
 */
trait PgJdbcTestBase {
  self: SparkFunSuite =>

  // Server instance that this JDBC driver connects to
  protected val serverInstance: SparkPgSQLServerTest

  // Register a JDBC driver for PostgreSQL
  Utils.classForName(classOf[org.postgresql.Driver].getCanonicalName)

  private lazy val jdbcUri = s"jdbc:postgresql://localhost:${serverInstance.listeningPort}/default"

  // Connection parameters refer to ones of PostgreSQL JDBC driver v42.x:
  // https://jdbc.postgresql.org/documentation/head/connect.html#connection-parameters

  // `preferQueryMode` has been supported until PostgreSQL JDBC drivers v9.4.1210
  val queryQueryMode: String

  // Set this threshold at 1 for tests (5 by default)
  val prepareThreshold: Int = 1

  // Set this threshold at 2 for tests (256 by default)
  val preparedStatementCacheQueries: Int = 2

  // This value of 0 disables the cache
  val preparedStatementCacheSizeMiB: Int = 0

  // The value of 0 means that in `ResultSet` will be fetch all rows at once
  val defaultRowFetchSize: Int = 0

  val sendBufferSize: Int = 146988
  val recvBufferSize: Int = 408300

  val binaryTransferEnable: String = Seq(
    "TIMESTAMPTZ",
    "UUID",
    "INT2_ARRAY",
    "INT4_ARRAY",
    "BYTEA",
    "TEXT_ARRAY",
    "TIMETZ",
    "INT8",
    "INT2",
    "INT4",
    "VARCHAR_ARRAY",
    "INT8_ARRAY",
    "POINT",
    "TIMESTAMP",
    "TIME",
    "BOX",
    "FLOAT4",
    "FLOAT8",
    "FLOAT4_ARRAY",
    "FLOAT8_ARRAY").mkString(",")

  val binaryTransferDisable: String = ""

  // These parameters are fixed in the tests
  private val stringtype: String = "VARCHAR"
  private val protocolVersion: Int = 3

  private def getJdbcConnect(): Connection = {
    val props = new Properties()
    props.put("user", System.getProperty("user.name"))
    props.put("password", "")
    props.put("protocolVersion", protocolVersion.toString)
    props.put("stringtype", stringtype)
    props.put("prepareThreshold", prepareThreshold.toString)
    props.put("preparedStatementCacheQueries", preparedStatementCacheQueries.toString)
    props.put("preparedStatementCacheSizeMiB", preparedStatementCacheSizeMiB.toString)
    props.put("defaultRowFetchSize", defaultRowFetchSize.toString)
    props.put("sendBufferSize", sendBufferSize.toString)
    props.put("recvBufferSize", recvBufferSize.toString)
    props.put("preferQueryMode", queryQueryMode)
    props.put("binaryTransferEnable", binaryTransferEnable)
    props.put("binaryTransferDisable", binaryTransferDisable)
    // props.put("loggerLevel", "TRACE")
    if (serverInstance.ssl) {
      props.put("ssl", "true")
      props.put("sslfactory", "org.postgresql.ssl.NonValidatingFactory")
    }

    DriverManager.getConnection(jdbcUri, props)
  }

  private val jdbcQueryTimeout = 180

  def testMultipleConnectionJdbcStatement(fs: (Statement => Unit)*) {
    val connections = fs.map { _ => getJdbcConnect() }
    val statements = connections.map { c =>
      val stmt = c.createStatement()
      stmt.setQueryTimeout(jdbcQueryTimeout)
      stmt
    }
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

  def testJdbcPreparedStatement(sql: String)(f: PreparedStatement => Unit): Unit = {
    val connection = getJdbcConnect()
    val statement = connection.prepareStatement(sql)
    statement.setQueryTimeout(jdbcQueryTimeout)
    try {
      f(statement)
    } finally {
      statement.close()
      connection.close()
    }
  }

  def testJdbcStatementWitConf(options: (String, String)*)(f: Statement => Unit) {
    val jdbcOptions = Seq("autoCommitModeEnabled", "fetchSize")
    val (sparkOptions, otherOptions) = options.partition(ops => !jdbcOptions.contains(ops._1))
    val connection = otherOptions.find(_._1 == "autoCommitModeEnabled").map { case (_, v) =>
      val conn = getJdbcConnect()
      conn.setAutoCommit(java.lang.Boolean.valueOf(v))
      conn
    }.getOrElse {
      getJdbcConnect()
    }

    val statement = otherOptions.find(_._1 == "fetchSize").map { case (_, v) =>
      val stmt = connection.createStatement()
      stmt.setFetchSize(java.lang.Integer.valueOf(v))
      stmt
    }.getOrElse {
      connection.createStatement()
    }

    statement.setQueryTimeout(jdbcQueryTimeout)

    val (keys, _) = sparkOptions.unzip
    val currentValues = keys.map { key =>
      val rs = statement.executeQuery(s"SET $key")
      if (rs.next()) { rs.getString(2) } else { assert(false, s"Invalid key detected: $key") }
    }
    sparkOptions.foreach { case (key, value) =>
      statement.execute(s"SET $key=$value")
    }

    try f(statement) finally {
      keys.zip(currentValues).foreach {
        case (key, value) =>
          statement.execute(s"SET $key=$value")
      }
      statement.close()
      connection.close()
    }
  }

  private def testSelectiveModeOnly(testMode: String, testName: String)(testBody: => Unit): Unit = {
    if (queryQueryMode == testMode) {
      test(testName) { testBody }
    } else {
      ignore(s"$testName [skipped when $queryQueryMode mode enabled]")(testBody)
    }
  }

  def testSimpleQueryModeOnly(testName: String)(testBody: => Unit): Unit =
    testSelectiveModeOnly("simple", s"$testName - simple query mode only")(testBody)

  def testExtendedQueryModeOnly(testName: String)(testBody: => Unit): Unit =
    testSelectiveModeOnly("extended", s"$testName - extended query mode only")(testBody)
}
