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
import java.math.BigDecimal
import java.net.URL
import java.nio.charset.StandardCharsets
import java.sql._
import java.util.Properties

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Random
import scala.util.Try

import com.google.common.io.Files
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

object TestData {
  val smallKv = getTestDataFilePath("small_kv.txt")
  val smallKvWithNull = getTestDataFilePath("small_kv_with_null.txt")

  private def getTestDataFilePath(name: String): URL = {
    Thread.currentThread().getContextClassLoader.getResource(s"data/files/$name")
  }
}

class PostgreSQLJdbcSuite extends PostgreSQLJdbcTestBase(ssl = false) {

  test("server version") {
    withJdbcStatement { statement =>
      val protoInfo = statement.getConnection.asInstanceOf[org.postgresql.jdbc.PgConnection]
      // A server version decides how to handle metadata between jdbc clients and servers.
      // Since it is hard to handle PostgreSQL dialects, we use the simplest settings;
      // when the value is set at an empty string, the interaction is the simplest.
      assert("" === protoInfo.getDBVersionNumber)
    }
  }

  test("DatabaseMetaData tests") {
    withJdbcStatement { statement =>
      val databaseMetaData = statement.getConnection.getMetaData
      val supportedTypeInfo = new Iterator[(String, String)] {
        val typeInfo = databaseMetaData.getTypeInfo

        def hasNext = typeInfo.next()

        def next() = {
          val typeName = typeInfo.getString("TYPE_NAME")
          val mappedJdbcTypeName = typeInfo.getShort("DATA_TYPE").toInt match {
            case java.sql.Types.BIT => "BIT"
            case java.sql.Types.CHAR => "CHAR"
            case java.sql.Types.SMALLINT => "SMALLINT"
            case java.sql.Types.INTEGER => "INTEGER"
            case java.sql.Types.BIGINT => "BIGINT"
            case java.sql.Types.REAL => "REAL"
            case java.sql.Types.DOUBLE => "DOUBLE"
            case java.sql.Types.VARCHAR => "VARCHAR"
            case java.sql.Types.DATE => "DATE"
            case java.sql.Types.TIMESTAMP => "TIMESTAMP"
            case java.sql.Types.NUMERIC => "NUMERIC"
            case java.sql.Types.BINARY => "BINARY"
            case java.sql.Types.ARRAY => "ARRAY"
            case java.sql.Types.OTHER => "OTHER"
            case typeId =>
              fail(s"Unexpected typed value detected: typeId=$typeId")
          }
          (typeName, mappedJdbcTypeName)
        }
      }

      val expectedTypeInfo = Set(
        ("bool", "BIT"),
        ("char", "CHAR"),
        ("name", "VARCHAR"),
        ("serial", "INTEGER"),
        ("bigserial", "BIGINT"),
        ("byte", "OTHER"),
        ("int2", "SMALLINT"),
        ("int4", "INTEGER"),
        ("int8", "BIGINT"),
        ("tid", "OTHER"),
        ("float4", "REAL"),
        ("float8", "DOUBLE"),
        ("varchar", "VARCHAR"),
        ("date", "DATE"),
        ("timestamp", "TIMESTAMP"),
        ("numeric", "NUMERIC"),
        ("bytea", "BINARY"),
        ("map", "OTHER"),
        ("struct", "OTHER"),
        ("_bool", "ARRAY"),
        ("_int2", "ARRAY"),
        ("_int4", "ARRAY"),
        ("_int8", "ARRAY"),
        ("_float4", "ARRAY"),
        ("_float8", "ARRAY"),
        ("_varchar", "ARRAY"),
        ("_date", "ARRAY"),
        ("_timestamp", "ARRAY"),
        ("_numeric", "ARRAY")
      )

      assert(expectedTypeInfo === supportedTypeInfo.toSet)

      Seq(
        "DROP TABLE IF EXISTS test1",
        "DROP TABLE IF EXISTS test2",
        """
          |CREATE TABLE test1(
          |  key STRING,
          |  value DOUBLE
          |)
          """.stripMargin,
        """
          |CREATE TABLE test2(
          |  id INT,
          |  name STRING,
          |  address STRING,
          |  salary FLOAT
          |)
          """.stripMargin
      ).foreach(statement.execute)

      Seq("test1", "test2").foreach { tableName =>
        val mdTable = databaseMetaData.getTables(null, null, tableName, scala.Array("TABLE"))
        assert(mdTable.next())
        assert(tableName === mdTable.getString("TABLE_NAME"))
        assert(!mdTable.next())
      }

      val getTableSchema = (tableName: String) => new Iterator[(String, String)] {
        val schemaInfo = databaseMetaData.getColumns(null, null, tableName, "%")
        def hasNext = schemaInfo.next()
        def next() = (schemaInfo.getString("COLUMN_NAME"), schemaInfo.getString("TYPE_NAME"))
      }

      assert(Set(("key", "varchar"), ("value", "float8")) === getTableSchema("test1").toSet)
      assert(Set(("id", "int4"), ("name", "varchar"), ("address", "varchar"), ("salary", "float4"))
        === getTableSchema("test2").toSet)
    }
  }

  test("primitive types") {
    withJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS test",
        """
          |CREATE TABLE test(
          |  col0 BOOLEAN,
          |  col1 SHORT,
          |  col2 INT,
          |  col3 LONG,
          |  col4 FLOAT,
          |  col5 DOUBLE,
          |  col6 STRING,
          |  col7 DATE,
          |  col8 TIMESTAMP,
          |  col9 DECIMAL
          |)
          """.stripMargin,
        "INSERT INTO test SELECT false, 25, 32, 15, 3.2, 8.9, 'test', '2016-08-04', " +
          "'2016-08-04 00:17:13.0', 32"
      ).foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT * FROM test")
      val resultSetMetaData = resultSet.getMetaData

      assert(10 === resultSetMetaData.getColumnCount)

      val expectedRow = Seq(false, 25, 32, 15, 3.2f, 8.9, "test", Date.valueOf("2016-08-04"),
        Timestamp.valueOf("2016-08-04 00:17:13"), BigDecimal.valueOf(32))

      def getTypedValue(offset: Int): Any = {
        val (typeName, value) = resultSetMetaData.getColumnType(offset) match {
          case java.sql.Types.BIT =>
            ("bool", resultSet.getBoolean(offset))
          case java.sql.Types.SMALLINT =>
            ("int2", resultSet.getShort(offset))
          case java.sql.Types.INTEGER =>
            ("int4", resultSet.getInt(offset))
          case java.sql.Types.BIGINT =>
            ("int8", resultSet.getLong(offset))
          case java.sql.Types.REAL =>
            ("float4", resultSet.getFloat(offset))
          case java.sql.Types.DOUBLE =>
            ("float8", resultSet.getDouble(offset))
          case java.sql.Types.VARCHAR =>
            ("varchar", resultSet.getString(offset))
          case java.sql.Types.DATE =>
            ("date", resultSet.getDate(offset))
          case java.sql.Types.TIMESTAMP =>
            ("timestamp", resultSet.getTimestamp(offset))
          case java.sql.Types.NUMERIC =>
            ("numeric", resultSet.getBigDecimal(offset))
          case typeId =>
            fail(s"Unexpected typed value detected: offset=$offset, " +
              s"typeId=$typeId, typeName=${resultSetMetaData.getColumnTypeName(offset)}")
        }
        assert(typeName === resultSetMetaData.getColumnTypeName(offset))
        value
      }

      assert(resultSet.next())

      expectedRow.zipWithIndex.foreach { case (expected, index) =>
        val offset = index + 1
        assert(s"col${index}" === resultSetMetaData.getColumnName(offset))
        assert(expected === getTypedValue(offset))
      }

      assert(!resultSet.next())
    }
  }

  test("array types") {
    withJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS test",
        """
          |CREATE TABLE test(
          |  col0 ARRAY<BOOLEAN>,
          |  col1 ARRAY<SHORT>,
          |  col2 ARRAY<INT>,
          |  col3 ARRAY<LONG>,
          |  col4 ARRAY<FLOAT>,
          |  col5 ARRAY<DOUBLE>,
          |  col6 ARRAY<STRING>,
          |  col7 ARRAY<DATE>,
          |  col8 ARRAY<TIMESTAMP>,
          |  col9 ARRAY<DECIMAL>
          |)
          """.stripMargin,
        """
          |INSERT INTO test
          |  SELECT
          |    array(true, true, false),
          |    array(3, 8, 1, -7),
          |    array(2, 1, -9, 2, 5, 6),
          |    array(0, 1, -7, 3),
          |    array(0.1, -3.2, 2.9, -5.8, 3.9),
          |    array(-3.2, 8.2),
          |    array('abc', 'defg', 'h', 'ij'),
          |    array('2016-08-04', '2016-08-05', '2016-08-06'),
          |    array('2016-08-04 00:17:13'),
          |    array(12, 86, 35)
          """.stripMargin
      ).foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT * FROM test")
      val resultSetMetaData = resultSet.getMetaData

      assert(10 === resultSetMetaData.getColumnCount)

      val expectedRow = Seq(
        Seq(true, true, false),
        Seq(3, 8, 1, -7),
        Seq(2, 1, -9, 2, 5, 6),
        Seq(0, 1, -7, 3),
        Seq(0.1f, -3.2f, 2.9f, -5.8f, 3.9f),
        Seq(-3.2, 8.2),
        Seq("abc", "defg", "h", "ij"),
        Seq(Date.valueOf("2016-08-04"), Date.valueOf("2016-08-05"), Date.valueOf("2016-08-06")),
        Seq(Timestamp.valueOf("2016-08-04 00:17:13")),
        Seq(BigDecimal.valueOf(12), BigDecimal.valueOf(86), BigDecimal.valueOf(35))
      )

      def getTypedArray(offset: Int): Seq[Any] = {
        assert(java.sql.Types.ARRAY === resultSetMetaData.getColumnType(offset))
        val resultArray = resultSet.getArray(offset)
        val elementTypeName = resultArray.getBaseType match {
          case java.sql.Types.BIT => "bool"
          case java.sql.Types.SMALLINT => "int2"
          case java.sql.Types.INTEGER => "int4"
          case java.sql.Types.BIGINT => "int8"
          case java.sql.Types.REAL => "float4"
          case java.sql.Types.DOUBLE => "float8"
          case java.sql.Types.VARCHAR => "varchar"
          case java.sql.Types.DATE => "date"
          case java.sql.Types.TIMESTAMP => "timestamp"
          case java.sql.Types.NUMERIC => "numeric"
          case typeId =>
            fail(s"Unexpected typed value detected: offset=$offset, " +
              s"typeId=$typeId, typeName=${resultSetMetaData.getColumnTypeName(offset)}")
        }
        assert(s"_${elementTypeName}" === resultSetMetaData.getColumnTypeName(offset))
        assert(elementTypeName === resultArray.getBaseTypeName)
        resultArray.getArray.asInstanceOf[scala.Array[Object]].toSeq
      }

      assert(resultSet.next())

      expectedRow.zipWithIndex.foreach { case (expected, index) =>
        val offset = index + 1
        assert(s"col$index" === resultSetMetaData.getColumnName(offset))
        assert(expected === getTypedArray(offset))
      }

      assert(!resultSet.next())
    }
  }

  test("binary types") {
    withJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS test",
        "CREATE TABLE test(val STRING)",
        "INSERT INTO test SELECT 'abcdefghijklmn'"
      ).foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT CAST(val AS BINARY) FROM test")
      val resultSetMetaData = resultSet.getMetaData

      assert(1 === resultSetMetaData.getColumnCount)
      assert(resultSet.next())
      assert("abcdefghijklmn".getBytes === resultSet.getBytes(1))
      assert(!resultSet.next())
    }
  }

  test("custom types (BYTE, STRUCT, MAP)") {
    withJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS test",
        """
          |CREATE TABLE test(
          |  col0 BYTE,
          |  col1 STRUCT<val0: INT, val1: STRUCT<val11: FLOAT, val12: STRING>>,
          |  col2 MAP<INT, STRING>
          |)
          """.stripMargin,
        "INSERT INTO test SELECT -1, (0, (0.1, 'test')), map(0, 'value0', 1, 'value1')"
      ).foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT * FROM test")
      val resultSetMetaData = resultSet.getMetaData

      assert(3 === resultSetMetaData.getColumnCount)

      val expectedRow = Seq(-1, """{"val0":0,"val1":{"val11":0.1,"val12":"test"}}""",
        """{0:"value0",1:"value1"}""")

      def getCustomTypedValue(offset: Int): Any = {
        assert("PGobject" === resultSet.getObject(offset).getClass.getSimpleName)
        resultSetMetaData.getColumnType(offset) match {
          case java.sql.Types.OTHER =>
            if (resultSetMetaData.getColumnTypeName(offset) == "byte") {
              resultSet.getByte(offset)
            } else {
              // Just return the value as a string
              resultSet.getString(offset)
            }
          case typeId =>
            fail(s"Unexpected typed value detected: offset=$offset, " +
              s"typeId=$typeId, typeName=${resultSetMetaData.getColumnTypeName(offset)}")
        }
      }

      assert(resultSet.next())

      expectedRow.zipWithIndex.foreach { case (expected, index) =>
        val offset = index + 1
        assert(s"col$index" === resultSetMetaData.getColumnName(offset))
        assert(expected === getCustomTypedValue(offset))
      }

      assert(!resultSet.next())
    }
  }

  test("simple query execution") {
    withJdbcStatement { statement =>
      Seq(
        "SET spark.sql.shuffle.partitions=3",
        "DROP TABLE IF EXISTS test",
        "CREATE TABLE test(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test",
        "CACHE TABLE test"
      ).foreach(statement.execute)

      assertResult(5, "Row count mismatch") {
        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
  }

  test("result set containing NULL") {
    withJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS test_null",
        "CREATE TABLE test_null(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKvWithNull}' OVERWRITE INTO TABLE test_null"
      ).foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT * FROM test_null WHERE key IS NULL")

      (0 until 5).foreach { _ =>
        resultSet.next()
        assert(0 === resultSet.getInt(1))
        assert(resultSet.wasNull())
      }

      assert(!resultSet.next())
    }
  }

  test("multiple session") {
    import org.apache.spark.sql.internal.SQLConf
    var defaultV1: String = null
    var defaultV2: String = null
    var data: mutable.ArrayBuffer[Int] = null

    withMultipleConnectionJdbcStatement(
      // create table, insert data, and fetch them
      { statement =>

        Seq(
          "DROP TABLE IF EXISTS test_map",
          "CREATE TABLE test_map(key INT, value STRING)",
          s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map",
          "CACHE TABLE test_table AS SELECT key FROM test_map ORDER BY key DESC",
          "CREATE DATABASE IF NOT EXISTS db1"
        ).foreach(statement.execute)

        val plan = statement.executeQuery("EXPLAIN SELECT * FROM test_table")
        assert(plan.next())
        assert(plan.getString(1).contains("InMemoryTableScan"))

        val rs1 = statement.executeQuery("SELECT key FROM test_table ORDER BY KEY DESC")
        val buf1 = new mutable.ArrayBuffer[Int]()
        while (rs1.next()) {
          buf1 += rs1.getInt(1)
        }
        rs1.close()

        val rs2 = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        val buf2 = new mutable.ArrayBuffer[Int]()
        while (rs2.next()) {
          buf2 += rs2.getInt(1)
        }
        rs2.close()

        assert(buf1 === buf2)

        data = buf1
      },

      // get the default value of the session status
      { statement =>
        val rs = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        assert(rs.next())
        assert(SQLConf.SHUFFLE_PARTITIONS.key === rs.getString("key"))
        defaultV1 = rs.getString("value")
        assert("200" === defaultV1)
        rs.close()
      },

      // update the session status
      { statement =>

        statement.execute(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}=291")

        val rs = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        assert(rs.next())
        assert(SQLConf.SHUFFLE_PARTITIONS.key === rs.getString("key"))
        assert("291" === rs.getString("value"))
        rs.close()
      },

      // get the latest session status, supposed to be the default value
      { statement =>

        val rs = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        assert(rs.next())
        assert(SQLConf.SHUFFLE_PARTITIONS.key === rs.getString("key"))
        defaultV1 = rs.getString("value")
        assert("200" === defaultV1)
        rs.close()
      },

      // try to access the cached data in another session
      { statement =>

        val plan = statement.executeQuery("EXPLAIN SELECT key FROM test_map ORDER BY key DESC")
        assert(plan.next())
        assert(plan.getString(1).contains("InMemoryTableScan"))

        val rs = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        val buf = new mutable.ArrayBuffer[Int]()
        while (rs.next()) {
          buf += rs.getInt(1)
        }
        rs.close()
        assert(buf === data)
      },

      // switch another database
      { statement =>
        statement.execute("USE db1")

        // there is no test_map table in db1
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        }

        Seq(
          "DROP TABLE IF EXISTS test_map2",
          "CREATE TABLE test_map2(key INT, value STRING)"
        ).foreach(statement.execute)
      },

      // access default database
      { statement =>

        // current database should still be `default`
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_map2")
        }

        statement.execute("USE db1")
        // access test_map2
        statement.executeQuery("SELECT key from test_map2")
      }
    )
  }

  test("jdbc cancellation") {
    withJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS t",
        "CREATE TABLE t(key INT, value STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE t"
      ).foreach(statement.execute)

      implicit val ec = ExecutionContext.fromExecutorService(
        ThreadUtils.newDaemonSingleThreadExecutor("test-jdbc-cancel"))
      try {
        // Start a very-long-running query that will take hours to finish, then cancel it in order
        // to demonstrate that cancellation works.
        val f = Future {
          statement.executeQuery("SET spark.sql.crossJoin.enabled=true")
          val query = "SELECT COUNT(*) FROM t " + (0 until 10).map(_ => "join t").mkString(" ")
          val rs = statement.executeQuery(query)
          // Try to fetch a first line of results
          rs.next()
        }
        // Note that this is slightly race-prone: if the cancel is issued before the statement
        // begins executing then we'll fail with a timeout. As a result, this fixed delay is set
        // slightly more conservatively than may be strictly necessary.
        Thread.sleep(3000)
        statement.cancel()
        val e = intercept[SparkException] {
          ThreadUtils.awaitResult(f, 3.minute)
        }.getCause
        assert(e.isInstanceOf[SQLException])
        assert(e.getMessage.contains("cancelled"))
      } finally {
        ec.shutdownNow()
      }
    }
  }
}

class PostgreSQLJdbcWithSslSuite extends PostgreSQLJdbcTestBase(ssl = true) {

  test("query execution via SSL") {
    val testFunc = (statement: Statement) => {
      Seq(
        "SET spark.sql.shuffle.partitions=3",
        "DROP TABLE IF EXISTS test",
        "CREATE TABLE test(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test",
        "CACHE TABLE test"
      ).foreach(statement.execute)

      assertResult(5, "Row count mismatch") {
        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
    withJdbcStatement(testFunc)
  }
}

abstract class PostgreSQLJdbcTestBase(ssl: Boolean = false) extends SQLServerTest(ssl) {
  Utils.classForName(classOf[org.postgresql.Driver].getCanonicalName)

  private lazy val jdbcUri = s"jdbc:postgresql://localhost:${serverPort}/default"

  def withMultipleConnectionJdbcStatement(fs: (Statement => Unit)*) {
    val props = new Properties()
    props.put("user", System.getProperty("user.name"))
    props.put("password", "")
    if (ssl) {
      props.put("ssl", "true")
      props.put("sslfactory", "org.postgresql.ssl.NonValidatingFactory")
    }
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUri, props) }
    val statements = connections.map(_.createStatement())
    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  def withJdbcStatement(f: Statement => Unit) {
    withMultipleConnectionJdbcStatement(f)
  }
}

abstract class SQLServerTest(ssl: Boolean)
    extends SparkFunSuite with BeforeAndAfterAll with Logging {

  private val CLASS_NAME = SQLServer.getClass.getCanonicalName.stripSuffix("$")
  private val LOG_FILE_MARK = s"starting $CLASS_NAME, logging to "

  protected val startScript = "../../sbin/start-sql-server.sh".split("/").mkString(File.separator)
  protected val stopScript = "../../sbin/stop-sql-server.sh".split("/").mkString(File.separator)

  private var listeningPort: Int = _
  protected def serverPort: Int = listeningPort

  protected def user = System.getProperty("user.name")

  private val pidDir: File = Utils.createTempDir("sqlserver-pid")
  protected var logPath: File = _
  protected var operationLogPath: File = _
  private var logTailingProcess: Process = _

  protected def serverStartCommand(port: Int) = {
    val driverClassPath = {
      // Writes a temporary log4j.properties and prepend it to driver classpath, so that it
      // overrides all other potential log4j configurations contained in other dependency jar files.
      val tempLog4jConf = Utils.createTempDir().getCanonicalPath

      Files.write(
        """log4j.rootCategory=INFO, console
          |log4j.appender.console=org.apache.log4j.ConsoleAppender
          |log4j.appender.console.target=System.err
          |log4j.appender.console.layout=org.apache.log4j.PatternLayout
          |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
        """.stripMargin,
        new File(s"$tempLog4jConf/log4j.properties"),
        StandardCharsets.UTF_8)

      tempLog4jConf
    }

    s"""$startScript
       |  --master local
       |  --driver-class-path $driverClassPath
       |  --driver-java-options -Dlog4j.debug
       |  --conf spark.ui.enabled=false
       |  --conf ${SQLServerConf.SQLSERVER_PORT.key}=$port
       |  --conf ${SQLServerConf.SQLSERVER_SSL_ENABLED.key}=$ssl
     """.stripMargin.split("\\s+").toSeq
  }

  val SERVER_STARTUP_TIMEOUT = 1.minutes

  private def startSQLServer(port: Int, attempt: Int) = {
    operationLogPath = Utils.createTempDir()
    operationLogPath.delete()
    logPath = null
    logTailingProcess = null

    val command = serverStartCommand(port)

    logPath = {
      val lines = Utils.executeAndGetOutput(
        command = command,
        extraEnvironment = Map(
          // Disables SPARK_TESTING to exclude log4j.properties in test directories.
          "SPARK_TESTING" -> "0",
          // But set SPARK_SQL_TESTING to make spark-class happy.
          "SPARK_SQL_TESTING" -> "1",
          // Points SPARK_PID_DIR to SPARK_HOME, otherwise only 1 Thrift server instance can be
          // started at a time, which is not Jenkins friendly.
          "SPARK_PID_DIR" -> pidDir.getCanonicalPath),
        redirectStderr = true)

      logInfo(s"COMMAND: $command")
      logInfo(s"OUTPUT: $lines")
      lines.split("\n").collectFirst {
        case line if line.contains(LOG_FILE_MARK) => new File(line.drop(LOG_FILE_MARK.length))
      }.getOrElse {
        throw new RuntimeException("Failed to find SQLServer log file.")
      }
    }

    val serverStarted = Promise[Unit]()

    // Ensures that the following "tail" command won't fail.
    logPath.createNewFile()

    logTailingProcess = {
      val command = s"/usr/bin/env tail -n +0 -f ${logPath.getCanonicalPath}".split(" ")
      // Using "-n +0" to make sure all lines in the log file are checked.
      new ProcessBuilder(command: _*).start()
    }

    // ThreadUtils.awaitResult(serverStarted.future, SERVER_STARTUP_TIMEOUT)
    // TODO: Stupid waiting here, so this needs to be fixed ASAP
    Thread.sleep(SERVER_STARTUP_TIMEOUT.toMillis)
  }

  private def stopSQLServer(): Unit = {
    // The `spark-daemon.sh' script uses kill, which is not synchronous, have to wait for a while.
    Utils.executeAndGetOutput(
      command = Seq(stopScript),
      extraEnvironment = Map("SPARK_PID_DIR" -> pidDir.getCanonicalPath))
    Thread.sleep(3.seconds.toMillis)

    operationLogPath.delete()
    operationLogPath = null

    Option(logPath).foreach(_.delete())
    logPath = null

    Option(logTailingProcess).foreach(_.destroy())
    logTailingProcess = null
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Chooses a random port between 10000 and 19999
    listeningPort = 10000 + Random.nextInt(10000)

    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 3).foldLeft(Try(startSQLServer(listeningPort, 0))) { case (started, attempt) =>
      started.orElse {
        listeningPort += 1
        stopSQLServer()
        Try(startSQLServer(listeningPort, attempt))
      }
    }.recover {
      case cause: Throwable =>
        throw cause
    }.get

    logInfo("SQLServer started successfully")
  }

  override protected def afterAll(): Unit = {
    try {
      stopSQLServer()
      logInfo("SQLServer stopped")
    } finally {
      super.afterAll()
    }
  }
}
