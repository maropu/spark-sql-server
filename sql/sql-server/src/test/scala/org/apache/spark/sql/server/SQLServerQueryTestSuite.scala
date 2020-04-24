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
import java.nio.file.Paths
import java.sql.{SQLException, Statement, Timestamp}
import java.util.{Locale, MissingFormatArgumentException}
import java.util.regex.Pattern

import scala.util.control.NonFatal

import org.apache.commons.lang3.exception.ExceptionUtils
import org.postgresql.util.PSQLException

import org.apache.spark.SparkException
import org.apache.spark.sql.SQLQueryTestSuite
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.execution.HiveResult
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Run all the tests in SQLQueryTestSuite via the SQL server.
 */
class SQLServerQueryTestSuite extends SQLQueryTestSuite with SQLServerTest with PgJdbcTestBase {

  override def pgVersion: String = "9.6"
  override def ssl: Boolean = false
  override def queryMode: String = "extended"
  override def executionMode: String = "multi-session"
  override def incrementalCollect: Boolean = true
  override def isTesting: Boolean = true

  override val serverInstance: SparkPgSQLServerTest = server

  private def _baseResourcePath =
    Paths.get("src", "test", "resources", "sql-tests").toFile
  private def _inputFilePath =
    new File(_baseResourcePath, "inputs").getAbsolutePath
  private def _goldenFilePath =
    new File(_baseResourcePath, "results").getAbsolutePath

  override def beforeAll(): Unit = {
    super.beforeAll()

    withJdbcStatement { statement =>
      createTestTables(statement)
    }
  }

  override def afterAll(): Unit = {
    try {
      withJdbcStatement(removeTestTables)
    } finally {
      super.afterAll()
    }
  }

  /** Load built-in test tables. */
  private def createTestTables(statement: Statement): Unit = {
    // Prepare the data
    statement.execute(
      """
        |CREATE TABLE testdata AS
        |SELECT id AS key, CAST(id AS string) AS value FROM range(1, 101)
      """.stripMargin)
    statement.execute(
      """
        |CREATE TABLE arraydata AS
        |SELECT * FROM VALUES
        |(ARRAY(1, 2, 3), ARRAY(ARRAY(1, 2, 3))),
        |(ARRAY(2, 3, 4), ARRAY(ARRAY(2, 3, 4))) AS v(arraycol, nestedarraycol)
      """.stripMargin)
    statement.execute(
      """
        |CREATE TABLE mapdata AS
        |SELECT * FROM VALUES
        |MAP(1, 'a1', 2, 'b1', 3, 'c1', 4, 'd1', 5, 'e1'),
        |MAP(1, 'a2', 2, 'b2', 3, 'c2', 4, 'd2'),
        |MAP(1, 'a3', 2, 'b3', 3, 'c3'),
        |MAP(1, 'a4', 2, 'b4'),
        |MAP(1, 'a5') AS v(mapcol)
      """.stripMargin)
    statement.execute(
      s"""
         |CREATE TABLE aggtest
         |  (a int, b float)
         |USING csv
         |OPTIONS (path '${_baseResourcePath.getParent}/test-data/postgresql/agg.data',
         |  header 'false', delimiter '\t')
      """.stripMargin)
    statement.execute(
      s"""
         |CREATE TABLE onek
         |  (unique1 int, unique2 int, two int, four int, ten int, twenty int, hundred int,
         |    thousand int, twothousand int, fivethous int, tenthous int, odd int, even int,
         |    stringu1 string, stringu2 string, string4 string)
         |USING csv
         |OPTIONS (path '${_baseResourcePath.getParent}/test-data/postgresql/onek.data',
         |  header 'false', delimiter '\t')
      """.stripMargin)
    statement.execute(
      s"""
         |CREATE TABLE tenk1
         |  (unique1 int, unique2 int, two int, four int, ten int, twenty int, hundred int,
         |    thousand int, twothousand int, fivethous int, tenthous int, odd int, even int,
         |    stringu1 string, stringu2 string, string4 string)
         |USING csv
         |  OPTIONS (path '${_baseResourcePath.getParent}/test-data/postgresql/tenk.data',
         |  header 'false', delimiter '\t')
      """.stripMargin)
  }


  private def removeTestTables(statement: Statement): Unit = {
    statement.execute("DROP TABLE IF EXISTS testdata")
    statement.execute("DROP TABLE IF EXISTS arraydata")
    statement.execute("DROP TABLE IF EXISTS mapdata")
    statement.execute("DROP TABLE IF EXISTS aggtest")
    statement.execute("DROP TABLE IF EXISTS onek")
    statement.execute("DROP TABLE IF EXISTS tenk1")
  }

  /** List of test cases to ignore, in lower cases. */
  override def blackList: Set[String] = super.blackList ++ Set(
    // Missing UDF
    "postgreSQL/boolean.sql",
    "postgreSQL/case.sql",
    // SPARK-28624
    "date.sql",
    // SPARK-28620
    "postgreSQL/float4.sql",
    // SPARK-28636
    "decimalArithmeticOperations.sql",
    "literals.sql",
    "subquery/scalar-subquery/scalar-subquery-predicate.sql",
    "subquery/in-subquery/in-limit.sql",
    "subquery/in-subquery/in-group-by.sql",
    "subquery/in-subquery/simple-in.sql",
    "subquery/in-subquery/in-order-by.sql",
    "subquery/in-subquery/in-set-operations.sql",

    // TODO: Needs to check the failure causes below
    // unsupported nested arrays
    "array.sql",
    // analysis failure: Table or view not found: pg_namespace
    "columnresolution.sql",
    // exception happened in `StructColumnTextWriter`
    "csv-functions.sql",
    // output mismatch, e.g., `207[8] hours 48 minutes 47...` and `207[7] hours 48 minutes 47...`
    "datetime.sql",
    // analysis failure: java.lang.ClassNotFoundException: Failed to find data source:
    // org.apache.spark.sql.sources.DDLScanSource
    "describe.sql",
    // analysis failure
    "group-by.sql",
    // unsupported nested arrays
    "higher-order-functions.sql",
    // output mismatch, e.g., `[0, 1]` and `{0, 1}`
    "inline-table.sql",
    // output mismatch
    "intersect-all.sql",
    // interval parser failure
    "interval-display-iso_8601.sql",
    "interval-display-sql_standard.sql",
    "interval-display.sql",
    "interval.sql",
    // exception happened in `StructColumnTextWriter`
    "json-functions.sql",
    // unsupported nested arrays
    "limit.sql",
    // interval parser failure
    "misc-functions.sql",
    // output mismatch, e.g., `[0, 1]` and `{0, 1}`
    "pivot.sql",
    // analysis failure
    "query_regex_column.sql",
    // exception happened in `StructColumnTextWriter`
    "string-functions.sql",
    // output mismatch
    "struct.sql",
    // analysis failure: Can not load class 'test.org.apache.spark.sql.MyDoubleAvg'
    "udaf.sql",
    // exception happened in `StructColumnTextWriter`
    "union.sql",
    // output mismatch
    "window.sql",
    // output mismatch
    "postgreSQL/aggregates_part1.sql",
    // parser failure
    "postgreSQL/int8.sql",
    // analysis exception
    "postgreSQL/text.sql",
    // output mismatch
    "postgreSQL/timestamp.sql",
    // output mismatch
    "postgreSQL/window_part2.sql",
    // runtime failure
    "typeCoercion/native/booleanEquality.sql",
    "typeCoercion/native/caseWhenCoercion.sql",
    // unsupported nested arrays
    "typeCoercion/native/concat.sql",
    // runtime failure
    "typeCoercion/native/decimalPrecision.sql",
    // analysis failure
    "typeCoercion/native/division.sql",
    // runtime failure
    "typeCoercion/native/ifCoercion.sql",
    "typeCoercion/native/inConversion.sql",
    "typeCoercion/native/mapZipWith.sql",
    "typeCoercion/native/mapconcat.sql",
    "typeCoercion/native/promoteStrings.sql",
    // exception happened in `StructColumnTextWriter`
    "typeCoercion/native/stringCastAndExpressions.sql",
    // runtime failure
    "typeCoercion/native/widenSetOperationTypes.sql",
    "typeCoercion/native/windowFrameCoercion.sql"
  )

  override def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      configSet: Seq[(String, String)]): Unit = {
    // We do not test with configSet.
    withJdbcStatement { statement =>

      configSet.foreach { case (k, v) =>
        statement.execute(s"SET $k = $v")
      }

      testCase match {
        case _: PgSQLTest | _: AnsiTest =>
          statement.execute(s"SET ${SQLConf.ANSI_ENABLED.key} = true")
        case _ =>
          statement.execute(s"SET ${SQLConf.ANSI_ENABLED.key} = false")
      }

      // Run the SQL queries preparing them for comparison.
      val outputs: Seq[QueryOutput] = queries.map { sql =>
        val (_, output) = handleExceptions(getNormalizedResult(statement, sql))
        // We might need to do some query canonicalization in the future.
        QueryOutput(
          sql = sql,
          schema = "",
          output = output.mkString("\n").replaceAll("\\s+$", ""))
      }

      // Read back the golden file.
      val expectedOutputs: Seq[QueryOutput] = {
        val goldenOutput = fileToString(new File(testCase.resultFile))
        val segments = goldenOutput.split("-- !query.*\n")

        // each query has 3 segments, plus the header
        assert(segments.size == outputs.size * 3 + 1,
          s"Expected ${outputs.size * 3 + 1} blocks in result file but got ${segments.size}. " +
            "Try regenerate the result files.")
        Seq.tabulate(outputs.size) { i =>
          val sql = segments(i * 3 + 1).trim
          val schema = segments(i * 3 + 2).trim
          val originalOut = segments(i * 3 + 3)
          val output = if (schema != emptySchema && isNeedSort(sql)) {
            originalOut.split("\n").sorted.mkString("\n")
          } else {
            originalOut
          }
          QueryOutput(
            sql = sql,
            schema = "",
            output = output.replaceAll("\\s+$", "")
          )
        }
      }

      // Compare results.
      assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
        outputs.size
      }

      outputs.zip(expectedOutputs).zipWithIndex.foreach { case ((output, expected), i) =>
        assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
          output.sql
        }

        expected match {
          // Skip desc command, see HiveResult.hiveResultString
          case d if d.sql.toUpperCase(Locale.ROOT).startsWith("DESC ")
            || d.sql.toUpperCase(Locale.ROOT).startsWith("DESC\n")
            || d.sql.toUpperCase(Locale.ROOT).startsWith("DESCRIBE ")
            || d.sql.toUpperCase(Locale.ROOT).startsWith("DESCRIBE\n") =>

          // Skip show command, see HiveResult.hiveResultString
          case s if s.sql.toUpperCase(Locale.ROOT).startsWith("SHOW ")
            || s.sql.toUpperCase(Locale.ROOT).startsWith("SHOW\n") =>

          case _ if output.output.startsWith(classOf[NoSuchTableException].getPackage.getName) =>
            assert(expected.output.startsWith(classOf[NoSuchTableException].getPackage.getName),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          case _ if output.output.startsWith(classOf[SparkException].getName) &&
            output.output.contains("overflow") =>
            assert(expected.output.contains(classOf[ArithmeticException].getName) &&
              expected.output.contains("overflow"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          case _ if output.output.startsWith(classOf[RuntimeException].getName) =>
            assert(expected.output.contains("Exception"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          case _ if output.output.startsWith(classOf[ArithmeticException].getName) &&
            output.output.contains("causes overflow") =>
            assert(expected.output.contains(classOf[ArithmeticException].getName) &&
              expected.output.contains("causes overflow"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          case _ if output.output.startsWith(classOf[MissingFormatArgumentException].getName) &&
            output.output.contains("Format specifier") =>
            assert(expected.output.contains(classOf[MissingFormatArgumentException].getName) &&
              expected.output.contains("Format specifier"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          // SQLException should not exactly match. We only assert the result contains Exception.
          case _ if output.output.startsWith(classOf[SQLException].getName) =>
            assert(expected.output.contains("Exception"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          // PSQLException should not exactly match. We only assert the result contains Exception.
          case _ if output.output.startsWith(classOf[PSQLException].getName) =>
            assert(expected.output.contains("Exception"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          // TODO: Needs to check why `super.replaceNotIncludedMsg` cannot correctly
          // handle the cases below.
          case _ if testCase.name == "explain.sql" =>
            assertResult(expected.output, s"Result did not match for query #$i\n${expected.sql}") {
              output.output.replaceAll(
                s"Location.*/spark-warehouse/",
                s"Location [not included in comparison]/{warehouse_dir}/")
            }

          // TODO: Numbers after a floating point between expected and query output are different,
          // e.g., 0.272380105814572[9] and 0.272380105814572[67]
          case _ if testCase.name == "group-by.sql" =>
            val p = Pattern.compile("""([0-9]+\.[0-9]{14})([0-9]+)""")
            val normalize = (s: String) => p.matcher(s).replaceAll("$1")
            assertResult(normalize(expected.output),
                s"Result did not match for query #$i\n${expected.sql}") {
              normalize(output.output)
            }

          case _ =>
            assertResult(expected.output, s"Result did not match for query #$i\n${expected.sql}") {
              output.output
            }
        }
      }
    }
  }

  override def createScalaTestCase(testCase: TestCase): Unit = {
    if (blackList.exists(t =>
      testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      // Create a test case to ignore this case.
      ignore(testCase.name) { /* Do nothing */ }
    } else {
      // Create a test case to run this case.
      test(testCase.name) {
        runTest(testCase)
      }
    }
  }

  override lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(_inputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(_inputFilePath, _goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(_inputFilePath).stripPrefix(File.separator)

      if (file.getAbsolutePath.startsWith(s"${_inputFilePath}${File.separator}udf")) {
        Seq.empty
      } else if (file.getAbsolutePath.startsWith(s"${_inputFilePath}${File.separator}postgreSQL")) {
        PgSQLTestCase(testCaseName, absPath, resultFile) :: Nil
      } else if (file.getAbsolutePath.startsWith(s"${_inputFilePath}${File.separator}ansi")) {
        AnsiTestCase(testCaseName, absPath, resultFile) :: Nil
      } else {
        RegularTestCase(testCaseName, absPath, resultFile) :: Nil
      }
    }
  }

  test("Check if the SQL server can work") {
    withJdbcStatement { statement =>
      val rs = statement.executeQuery("select 1L")
      rs.next()
      assert(rs.getLong(1) === 1L)
    }
  }

  /** The SQL server wraps the root exception, so it needs to be extracted. */
  override def handleExceptions(result: => (String, Seq[String])): (String, Seq[String]) = {
    super.handleExceptions {
      try {
        result
      } catch {
        case NonFatal(e) => throw ExceptionUtils.getRootCause(e)
      }
    }
  }

  private def getNormalizedResult(statement: Statement, sql: String): (String, Seq[String]) = {
    val rs = statement.executeQuery(sql)
    val cols = rs.getMetaData.getColumnCount
    val buildStr = () => (for (i <- 1 to cols) yield {
      getHiveResult(rs.getObject(i))
    }).mkString("\t")

    val answer = Iterator.continually(rs.next()).takeWhile(identity).map(_ => buildStr()).toSeq
      .map(replaceNotIncludedMsg)
    if (isNeedSort(sql)) {
      ("", answer.sorted)
    } else {
      ("", answer)
    }
  }

  // Returns true if sql is retrieving data.
  private def isNeedSort(sql: String): Boolean = {
    val upperCase = sql.toUpperCase(Locale.ROOT)
    upperCase.startsWith("SELECT ") || upperCase.startsWith("SELECT\n") ||
      upperCase.startsWith("WITH ") || upperCase.startsWith("WITH\n") ||
      upperCase.startsWith("VALUES ") || upperCase.startsWith("VALUES\n") ||
      // postgreSQL/union.sql
      upperCase.startsWith("(")
  }

  // This is copyed from `DecimalType`
  private def fromDecimal(d: Decimal): DecimalType = DecimalType(d.precision, d.scale)

  private def getHiveResult(obj: Object): String = {
    obj match {
      case null =>
        HiveResult.toHiveString((null, StringType))
      case d: java.sql.Date =>
        HiveResult.toHiveString((d, DateType))
      case t: Timestamp =>
        HiveResult.toHiveString((t, TimestampType))
      case i: org.postgresql.util.PGInterval =>
        TestUtils.toSparkIntervalString(i.toString)
      case d: java.math.BigDecimal =>
        HiveResult.toHiveString((d, fromDecimal(Decimal(d))))
      case bin: Array[Byte] =>
        HiveResult.toHiveString((bin, BinaryType))
      case other =>
        other.toString
    }
  }
}
