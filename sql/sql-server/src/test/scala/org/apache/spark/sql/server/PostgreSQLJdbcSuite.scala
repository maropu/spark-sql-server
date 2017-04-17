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

import java.io.{InputStream, IOException}
import java.math.BigDecimal
import java.net.URL
import java.sql._

import org.postgresql.util.PSQLException
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.sys.process.BasicIO

import org.apache.spark.SparkException
import org.apache.spark.util.{ThreadUtils, Utils}

object TestData {
  val smallKv = getTestDataFilePath("small_kv.txt")
  val smallKvWithNull = getTestDataFilePath("small_kv_with_null.txt")

  private def getTestDataFilePath(name: String): URL = {
    Thread.currentThread().getContextClassLoader.getResource(s"data/files/$name")
  }
}

class ProcessOutputCapturer(stream: InputStream, capture: String => Unit) extends Thread {
  this.setDaemon(true)

  override def run(): Unit = {
    try {
      BasicIO.processFully(capture)(stream)
    } catch { case _: IOException =>
      // Ignores the IOException thrown when the process termination, which closes the input
      // stream abruptly.
    }
  }
}

class PostgreSQLV9_6JdbcSuite extends PostgreSQLJdbcSuite("9.6")
class PostgreSQLV8_0JdbcSuite extends PostgreSQLJdbcSuite("8.0")
class PostgreSQLV7_4JdbcSuite extends PostgreSQLJdbcSuite("7.4")

abstract class PostgreSQLJdbcSuite(pgVersion: String)
  extends PostgreSQLJdbcTest(pgVersion = pgVersion, ssl = false) {

  val hiveVersion = "1.2.1"

  test("server version") {
    testJdbcStatement { statement =>
      val protoInfo = statement.getConnection.asInstanceOf[org.postgresql.jdbc.PgConnection]
      assert(pgVersion === protoInfo.getDBVersionNumber)
    }
  }

  def assertTable(tableName: String, expectedSchema: Set[(String, String)], m: DatabaseMetaData)
    : Unit = {
    val mdTable = m.getTables(null, null, tableName, scala.Array("TABLE"))
    assert(mdTable.next())
    assert(tableName === mdTable.getString("TABLE_NAME"))
    assert(!mdTable.next())
    val schema = new Iterator[(String, String)] {
      val schemaInfo = m.getColumns (null, null, tableName, "%")
      def hasNext = schemaInfo.next()
      def next() = (schemaInfo.getString("COLUMN_NAME"), schemaInfo.getString("TYPE_NAME"))
    }
    assert(expectedSchema === schema.toSet)
  }

  test("DatabaseMetaData tests") {
    testJdbcStatement { statement =>
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
          """,
        """
          |CREATE TABLE test2(
          |  id INT,
          |  name STRING,
          |  address STRING,
          |  salary FLOAT
          |)
          """
      ).foreach { sqlText =>
        assert(statement.execute(sqlText.stripMargin))
      }

      assertTable(
        "test1",
        Set(("key", "varchar"), ("value", "float8")),
        databaseMetaData
      )
      assertTable(
        "test2",
        Set(("id", "int4"), ("name", "varchar"), ("address", "varchar"), ("salary", "float4")),
        databaseMetaData
      )
    }
  }

  test("primitive types") {
    testJdbcStatement { statement =>
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
          """,
        """
          |INSERT INTO test
          |  SELECT false, 25, 32, 15, 3.2, 8.9, 'test', '2016-08-04', '2016-08-04 00:17:13.0', 32
        """
      ).foreach { sqlText =>
        assert(statement.execute(sqlText.stripMargin))
      }

      val rs = statement.executeQuery("SELECT * FROM test")
      val rsMetaData = rs.getMetaData

      assert(10 === rsMetaData.getColumnCount)

      val expectedRow = Seq(false, 25, 32, 15, 3.2f, 8.9, "test", Date.valueOf("2016-08-04"),
        Timestamp.valueOf("2016-08-04 00:17:13"), BigDecimal.valueOf(32))

      def getTypedValue(offset: Int): Any = {
        val (typeName, value) = rsMetaData.getColumnType(offset) match {
          case java.sql.Types.BIT =>
            ("bool", rs.getBoolean(offset))
          case java.sql.Types.SMALLINT =>
            ("int2", rs.getShort(offset))
          case java.sql.Types.INTEGER =>
            ("int4", rs.getInt(offset))
          case java.sql.Types.BIGINT =>
            ("int8", rs.getLong(offset))
          case java.sql.Types.REAL =>
            ("float4", rs.getFloat(offset))
          case java.sql.Types.DOUBLE =>
            ("float8", rs.getDouble(offset))
          case java.sql.Types.VARCHAR =>
            ("varchar", rs.getString(offset))
          case java.sql.Types.DATE =>
            ("date", rs.getDate(offset))
          case java.sql.Types.TIMESTAMP =>
            ("timestamp", rs.getTimestamp(offset))
          case java.sql.Types.NUMERIC =>
            ("numeric", rs.getBigDecimal(offset))
          case typeId =>
            fail(s"Unexpected typed value detected: offset=$offset, " +
              s"typeId=$typeId, typeName=${rsMetaData.getColumnTypeName(offset)}")
        }
        assert(typeName === rsMetaData.getColumnTypeName(offset))
        value
      }

      assert(rs.next())

      expectedRow.zipWithIndex.foreach { case (expected, index) =>
        val offset = index + 1
        assert(s"col${index}" === rsMetaData.getColumnName(offset))
        assert(expected === getTypedValue(offset))
      }

      assert(!rs.next())
      rs.close()
    }
  }

  test("array types") {
    testJdbcStatement { statement =>
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
          """,
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
          """
      ).foreach { sqlText =>
        assert(statement.execute(sqlText.stripMargin))
      }

      val rs = statement.executeQuery("SELECT * FROM test")
      val rsMetaData = rs.getMetaData

      assert(10 === rsMetaData.getColumnCount)

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
        assert(java.sql.Types.ARRAY === rsMetaData.getColumnType(offset))
        val resultArray = rs.getArray(offset)
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
              s"typeId=$typeId, typeName=${rsMetaData.getColumnTypeName(offset)}")
        }
        assert(s"_${elementTypeName}" === rsMetaData.getColumnTypeName(offset))
        assert(elementTypeName === resultArray.getBaseTypeName)
        resultArray.getArray.asInstanceOf[scala.Array[Object]].toSeq
      }

      assert(rs.next())

      expectedRow.zipWithIndex.foreach { case (expected, index) =>
        val offset = index + 1
        assert(s"col$index" === rsMetaData.getColumnName(offset))
        assert(expected === getTypedArray(offset))
      }

      assert(!rs.next())
      rs.close()
    }
  }

  test("binary types") {
    testJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS test",
        "CREATE TABLE test(val STRING)",
        "INSERT INTO test SELECT 'abcdefghijklmn'"
      ).foreach { sqlText =>
        assert(statement.execute(sqlText))
      }

      val rs = statement.executeQuery("SELECT CAST(val AS BINARY) FROM test")
      val rsMetaData = rs.getMetaData

      assert(1 === rsMetaData.getColumnCount)
      assert(rs.next())
      assert("abcdefghijklmn".getBytes === rs.getBytes(1))
      assert(!rs.next())
      rs.close()
    }
  }

  test("custom types (BYTE, STRUCT, MAP)") {
    testJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS test",
        """
          |CREATE TABLE test(
          |  col0 BYTE,
          |  col1 STRUCT<val0: INT, val1: STRUCT<val11: FLOAT, val12: STRING>>,
          |  col2 MAP<INT, STRING>
          |)
          """,
        """
          |INSERT INTO test
          |  SELECT -1, (0, (0.1, 'test')), map(0, 'value0', 1, 'value1')
        """
      ).foreach { sqlText =>
        assert(statement.execute(sqlText.stripMargin))
      }

      val rs = statement.executeQuery("SELECT * FROM test")
      val rsMetaData = rs.getMetaData

      assert(3 === rsMetaData.getColumnCount)

      val expectedRow = Seq(-1, """{"val0":0,"val1":{"val11":0.1,"val12":"test"}}""",
        """{0:"value0",1:"value1"}""")

      def getCustomTypedValue(offset: Int): Any = {
        assert("PGobject" === rs.getObject(offset).getClass.getSimpleName)
        rsMetaData.getColumnType(offset) match {
          case java.sql.Types.OTHER =>
            if (rsMetaData.getColumnTypeName(offset) == "byte") {
              rs.getByte(offset)
            } else {
              // Just return the value as a string
              rs.getString(offset)
            }
          case typeId =>
            fail(s"Unexpected typed value detected: offset=$offset, " +
              s"typeId=$typeId, typeName=${rsMetaData.getColumnTypeName(offset)}")
        }
      }

      assert(rs.next())

      expectedRow.zipWithIndex.foreach { case (expected, index) =>
        val offset = index + 1
        assert(s"col$index" === rsMetaData.getColumnName(offset))
        assert(expected === getCustomTypedValue(offset))
      }

      assert(!rs.next())
      rs.close()
    }
  }

  test("simple query execution") {
    testJdbcStatement { statement =>
      Seq(
        "SET spark.sql.shuffle.partitions=3",
        "DROP TABLE IF EXISTS test",
        "CREATE TABLE test(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test",
        "CACHE TABLE test"
      ).foreach { sqlText =>
        assert(statement.execute(sqlText))
      }

      val rs = statement.executeQuery("SELECT COUNT(*) FROM test")
      assert(rs.next())
      assert(5 === rs.getInt(1), "Row count mismatch")
      assert(!rs.next())
      rs.close()
    }
  }

  test("result set containing NULL") {
    testJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS test_null",
        "CREATE TABLE test_null(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKvWithNull}' OVERWRITE INTO TABLE test_null"
      ).foreach { sqlText =>
        assert(statement.execute(sqlText))
      }

      val rs = statement.executeQuery("SELECT * FROM test_null WHERE key IS NULL")

      (0 until 5).foreach { _ =>
        rs.next()
        assert(0 === rs.getInt(1))
        assert(rs.wasNull())
      }

      assert(!rs.next())
      rs.close()
    }
  }

  test("SPARK-17112 SELECT NULL via JDBC triggers IllegalArgumentException") {
    testJdbcStatement { statement =>
      val rs1 = statement.executeQuery("SELECT NULL")
      rs1.next()
      assert(0 === rs1.getInt(1))
      assert(rs1.wasNull())
      rs1.close()

      val rs2 = statement.executeQuery("SELECT IF(TRUE, NULL, NULL)")
      rs2.next()
      assert(0 === rs2.getInt(1))
      assert(rs2.wasNull())
      rs2.close()
    }
  }

  test("Checks Hive version via SET -v") {
    testJdbcStatement { statement =>
      val rs = statement.executeQuery("SET -v")
      val conf = mutable.Map.empty[String, String]
      while (rs.next()) {
        conf += rs.getString(1) -> rs.getString(2)
      }
      assert(conf.get("spark.sql.hive.version") === Some(hiveVersion))
      rs.close()
    }
  }

  test("Checks Hive version") {
    testJdbcStatement { statement =>
      val rs = statement.executeQuery("SET spark.sql.hive.version")
      rs.next()
      assert(rs.getString(1) === "spark.sql.hive.version")
      assert(rs.getString(2) === hiveVersion)
      rs.close()
    }
  }

  test("multiple session") {
    import org.apache.spark.sql.internal.SQLConf
    var defaultVal: String = null
    var data: mutable.ArrayBuffer[Int] = null

    testMultipleConnectionJdbcStatement(
      // Create table, insert data, and fetch them
      { statement =>

        Seq(
          "DROP TABLE IF EXISTS test_map",
          "CREATE TABLE test_map(key INT, value STRING)",
          s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map",
          "CACHE TABLE test_table AS SELECT key FROM test_map ORDER BY key DESC",
          "CREATE DATABASE IF NOT EXISTS db1"
        ).foreach { sqlText =>
          assert(statement.execute(sqlText))
        }

        val plan = statement.executeQuery("EXPLAIN SELECT * FROM test_table")
        assert(plan.next())
        assert(plan.getString(1).contains("InMemoryTableScan"))
        plan.close()

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

      // Get the default value of the session status
      { statement =>
        val rs = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        assert(rs.next())
        assert(SQLConf.SHUFFLE_PARTITIONS.key === rs.getString("key"))
        defaultVal = rs.getString("value")
        assert("200" === defaultVal)
        rs.close()
      },

      // Update the session status
      { statement =>

        assert(statement.execute(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}=291"))

        val rs = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        assert(rs.next())
        assert(SQLConf.SHUFFLE_PARTITIONS.key === rs.getString("key"))
        assert("291" === rs.getString("value"))
        rs.close()
      },

      // Get the latest session status, supposed to be the default value
      { statement =>

        val rs = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        assert(rs.next())
        assert(SQLConf.SHUFFLE_PARTITIONS.key === rs.getString("key"))
        defaultVal = rs.getString("value")
        assert("200" === defaultVal)
        rs.close()
      },

      // Try to access the cached data in another session
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

      // Switch another database
      { statement =>
        assert(statement.execute("USE db1"))

        // There is no test_map table in db1
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        }

        Seq(
          "DROP TABLE IF EXISTS test_map2",
          "CREATE TABLE test_map2(key INT, value STRING)"
        ).foreach { sqlText =>
          assert(statement.execute(sqlText))
        }
      },

      // Access default database
      { statement =>

        // Current database should still be `default`
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_map2")
        }

        assert(statement.execute("USE db1"))
        // Access test_map2
        val rs = statement.executeQuery("SELECT key from test_map2")
        assert(!rs.next())
        rs.close()
      }
    )
  }

  test("test ADD JAR") {
    testMultipleConnectionJdbcStatement(
      { statement =>
        val jarPath = "src/test/resources/hive-hcatalog-core-0.13.1.jar"
        val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"
        assert(statement.execute(s"ADD JAR $jarURL"))
      },

      { statement =>
        Seq(
          "DROP TABLE IF EXISTS smallKv",
          "CREATE TABLE smallKv(key INT, val STRING)",
          s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE smallKv",
          "DROP TABLE IF EXISTS addJar",
          """
            |CREATE TABLE addJar(key string)
            |  ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
          """,
          "INSERT INTO TABLE addJar SELECT 'k1' as key FROM smallKV limit 1"
        ).foreach { sqlText =>
          assert(statement.execute(sqlText.stripMargin))
        }

        val actualResult = statement.executeQuery("SELECT key FROM addJar")
        val actualResultBuffer = new collection.mutable.ArrayBuffer[String]()
        while (actualResult.next()) {
          actualResultBuffer += actualResult.getString(1)
        }
        actualResult.close()

        val expectedResult = statement.executeQuery("SELECT 'k1'")
        val expectedResultBuffer = new collection.mutable.ArrayBuffer[String]()
        while (expectedResult.next()) {
          expectedResultBuffer += expectedResult.getString(1)
        }
        expectedResult.close()

        assert(expectedResultBuffer === actualResultBuffer)
      }
    )
  }

  test("collect mode") {
    Set("true", "false").map { mode =>
      testJdbcStatementWitConf("spark.sql.server.incrementalCollect.enabled" -> mode) { statement =>
        // Create a table with many rows
        assert(statement.execute(
          """
            |CREATE OR REPLACE TEMPORARY VIEW t AS
            |  SELECT id, 1 AS value FROM range(0, 100000, 1, 32)
          """.stripMargin))
        val rs = statement.executeQuery("SELECT id, COUNT(value) FROM t GROUP BY id")
        (0 until 100000).foreach { i =>
          assert(rs.next())
          assert(rs.getInt(2) == 1)
        }
        assert(!rs.next())
        rs.close()
      }
    }
  }

  test("independent state across JDBC connections") {
    testMultipleConnectionJdbcStatement(
      { statement =>
        val jarPath = "src/test/resources/TestUDTF.jar"
        val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"

        // Configurations and temporary functions added in this session should be visible to all
        // the other sessions.
        Seq(
          "SET foo=bar",
          s"ADD JAR $jarURL",
          "DROP TEMPORARY FUNCTION IF EXISTS udtf_count2",
          s"""
             |CREATE TEMPORARY FUNCTION udtf_count2
             |  AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           """
        ).foreach { sqlText =>
          assert(statement.execute(sqlText.stripMargin))
        }
      },

      { statement =>
        val rs1 = statement.executeQuery("SET foo")
        assert(rs1.next())
        assert(rs1.getString(1) === "foo")
        assert(rs1.getString(2) !== "bar")
        rs1.close()

        val rs2 = statement.executeQuery("DESCRIBE FUNCTION udtf_count2")
        assert(rs2.next())
        assert(rs2.getString(1) === "Function: udtf_count2 not found.")
        rs2.close()
      }
    )
  }

  // This test often hangs and then times out, leaving the hanging processes.
  // Let's ignore it and improve the test.
  ignore("jdbc cancellation") {
    testJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS t",
        "CREATE TABLE t(key INT, value STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE t"
      ).foreach { sqlText =>
        assert(statement.execute(sqlText))
      }

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

  test("ADD JAR with input path having URL scheme") {
    testJdbcStatement { statement =>
      try {
        val jarPath = "src/test/resources/TestUDTF.jar"
        val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"

        Seq(
          s"ADD JAR $jarURL",
          s"""CREATE TEMPORARY FUNCTION udtf_count2
             |  AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           """
        ).foreach { sqlText =>
          assert(statement.execute(sqlText.stripMargin))
        }

        val rs1 = statement.executeQuery("DESCRIBE FUNCTION udtf_count2")
        assert(rs1.next())
        assert(rs1.getString(1) === "Function: udtf_count2")
        assert(rs1.next())
        assertResult("Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2") {
          rs1.getString(1)
        }
        assert(rs1.next())
        assert(rs1.getString(1) === "Usage: N/A.")
        rs1.close()

        val dataPath = "src/test/resources/data/files/kv1.txt"

        Seq(
          "DROP TABLE IF EXISTS test_udtf",
          "CREATE TABLE test_udtf(key INT, value STRING)",
          s"LOAD DATA LOCAL INPATH '$dataPath' OVERWRITE INTO TABLE test_udtf"
        ).foreach { sqlText =>
          assert(statement.execute(sqlText))
        }

        val rs2 = statement.executeQuery(
          "SELECT key, cc FROM test_udtf LATERAL VIEW udtf_count2(value) dd AS cc")

        assert(rs2.next())
        assert(rs2.getInt(1) === 97)
        assert(rs2.getInt(2) === 500)
        assert(rs2.next())
        assert(rs2.getInt(1) === 97)
        assert(rs2.getInt(2) === 500)
        rs2.close()
      } finally {
        assert(statement.execute("DROP TEMPORARY FUNCTION IF EXISTS udtf_count2"))
      }
    }
  }

  // TODO: This test is flaky, so we should revisit this
  ignore("CREATE/DROP tables between connections") {
    testJdbcStatement { statement =>
      Seq(
        "DROP TABLE IF EXISTS test1",
        "DROP TABLE IF EXISTS test2",
        "CREATE TABLE test1(a INT)",
        "CREATE TABLE test2(key STRING, value DOUBLE)"
      ).foreach { sqlText =>
        assert(statement.execute(sqlText))
      }

      val dbMeta = statement.getConnection.getMetaData
      assertTable("test1", Set(("a", "int4")), dbMeta)
      assertTable("test2", Set(("key", "varchar"), ("value", "float8")), dbMeta)
    }

    testJdbcStatement { statement =>
      val dbMeta = statement.getConnection.getMetaData
      assertTable("test1", Set(("a", "int4")), dbMeta)
      assertTable("test2", Set(("key", "varchar"), ("value", "float8")), dbMeta)
      statement.execute("DROP TABLE test1")
      assertTable("test2", Set(("key", "varchar"), ("value", "float8")), dbMeta)
      assert(!dbMeta.getTables(null, null, "test1", scala.Array("TABLE")).next())
    }

    testJdbcStatement { statement =>
      val dbMeta = statement.getConnection.getMetaData
      assertTable("test2", Set(("key", "varchar"), ("value", "float8")), dbMeta)
      assert(!dbMeta.getTables(null, null, "test1", scala.Array("TABLE")).next())
    }
  }

  test("setMaxRows") {
    testJdbcStatement { statement =>
      assert(statement.execute(
        """
          |CREATE OR REPLACE TEMPORARY VIEW t AS
          |  SELECT id AS value FROM range(0, 100, 1)
        """.stripMargin))
      statement.setMaxRows(1)
      val rs = statement.executeQuery("SELECT * FROM t")
      assert(rs.next())
      assert(!rs.next())
      rs.close()
    }
  }

  test("BEGIN non-supported") {
    // Since Spark does not support transaction blocks with `BEGIN`, we cannot implement
    // some operations like `Statement#setFetchSize`.
    val conn = getJdbcConnect()
    conn.setAutoCommit(false)
    val stmt = conn.createStatement()
    try {
      val errMsg = intercept[PSQLException] {
        stmt.execute("SELECT 1")
      }
      assert(errMsg.getMessage.contains("Cannot handle transaction blocks with `BEGIN`"))
    } finally {
      stmt.close()
      conn.close()
    }
  }
}

class PostgreSQLJdbcWithSslSuite extends PostgreSQLJdbcTest(ssl = true) {

  test("query execution via SSL") {
    testJdbcStatement { statement =>
      Seq(
        "SET spark.sql.shuffle.partitions=3",
        "DROP TABLE IF EXISTS test",
        "CREATE TABLE test(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test",
        "CACHE TABLE test"
      ).foreach { sqlText =>
        assert(statement.execute(sqlText))
      }

      val rs = statement.executeQuery("SELECT COUNT(*) FROM test")
      assert(rs.next())
      assert(5 === rs.getInt(1), "Row count mismatch")
      assert(!rs.next())
      rs.close()
    }

    // Check SSL used
    val bufferSrc = Source.fromFile(server.logPath)
    Utils.tryWithSafeFinally {
      assert(bufferSrc.getLines().exists(_.contains("SSL-encrypted connection enabled")))
    } {
      bufferSrc.close()
    }
  }
}

class PostgreSQLJdbcSingleSessionSuite extends PostgreSQLJdbcTest(singleSession = true) {

  test("share the temporary functions across JDBC connections") {
    testMultipleConnectionJdbcStatement(
      { statement =>
        val jarPath = "src/test/resources/TestUDTF.jar"
        val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"

        // Configurations and temporary functions added in this session should be visible to all
        // the other sessions.
        Seq(
          "SET foo=bar",
          s"ADD JAR $jarURL",
          s"""
             |CREATE TEMPORARY FUNCTION udtf_count2
             |  AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           """
        ).foreach { sqlText =>
          assert(statement.execute(sqlText.stripMargin))
        }
      },

      { statement =>
        try {
          val rs1 = statement.executeQuery("SET foo")
          assert(rs1.next())
          assert(rs1.getString(1) === "foo")
          assert(rs1.getString(2) === "bar")
          rs1.close()

          val rs2 = statement.executeQuery("DESCRIBE FUNCTION udtf_count2")
          assert(rs2.next())
          assert(rs2.getString(1) === "Function: udtf_count2")
          assert(rs2.next())
          assertResult("Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2") {
            rs2.getString(1)
          }
          assert(rs2.next())
          assert(rs2.getString(1) === "Usage: N/A.")
          rs2.close()
        } finally {
          assert(statement.execute("DROP TEMPORARY FUNCTION udtf_count2"))
        }
      }
    )
  }
}
