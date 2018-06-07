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

package org.apache.spark.sql.benchmark

import java.io.File
import java.nio.file.Paths
import java.sql.{DriverManager, ResultSet, Statement}
import java.util.Properties

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.benchmark.Utils._


/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 * {{{
 *   scala -cp <this test jar> <this class>
 * }}}
 *
 * This class could validate the result rows of the queries. By default, golden result files in
 * "spark-sql-server/sql/tpcds/src/main/resources/tpcds/results" have expected results with sf=1.
 * To enable this:
 * {{
 *   SPARK_VALIDATE_OUTPUT=1 scala -cp <this test jar> <this class>
 * }}
 *
 * To re-generate golden files, run:
 * {{
 *   SPARK_GENERATE_GOLDEN_FILES=1 scala -cp <this test jar> <this class>
 * }}
 */
object TPCDSQueryBenchmark extends Logging {

  // Register a JDBC driver for PostgreSQL
  classForName(classOf[org.postgresql.Driver].getCanonicalName)

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"
  private val validateOutput: Boolean = System.getenv("SPARK_VALIDATE_OUTPUT") == "1"
  private val validationModeEnabled = regenerateGoldenFiles || validateOutput

  private val baseResourcePath = {
    // If regenerateGoldenFiles is true, we must be running this in SBT and we use hard-coded
    // relative path. Otherwise, we use classloader's getResource to find the location.
    //
    // if (regenerateGoldenFiles) {
    //   Paths.get("sql", "tpcds", "src", "main", "resources", "tpcds").toFile
    // } else {
    //   val res = getClass.getClassLoader.getResource("tpcds")
    //   new File(res.getFile)
    // }

    // TODO: Can't resolve this path by classloader's getResource
    Paths.get("sql", "tpcds", "src", "main", "resources", "tpcds").toFile
  }

  private val queryFilePath = new File(baseResourcePath, "queries").getAbsolutePath
  private val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  case class TpcdsQueries(
      statement: Statement,
      queries: Seq[(String, Seq[String])],
      dataLocation: String) {

    private val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
      "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
      "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
      "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
      "time_dim", "web_page")

    private def getTypedValues(rs: ResultSet): Seq[Any] = {
      val rsMetaData = rs.getMetaData
      (1 to rsMetaData.getColumnCount).map { offset =>
        rs.getMetaData.getColumnType(offset) match {
          case java.sql.Types.BIT => rs.getBoolean(offset)
          case java.sql.Types.SMALLINT => rs.getShort(offset)
          case java.sql.Types.INTEGER => rs.getInt(offset)
          case java.sql.Types.BIGINT => rs.getLong(offset)
          case java.sql.Types.REAL => rs.getFloat(offset)
          case java.sql.Types.DOUBLE => rs.getDouble(offset)
          case java.sql.Types.VARCHAR => rs.getString(offset)
          case java.sql.Types.DATE => rs.getDate(offset)
          case java.sql.Types.TIMESTAMP => rs.getTimestamp(offset)
          case java.sql.Types.NUMERIC => rs.getBigDecimal(offset)
          case _ => "unknown"
        }
      }
    }

    private def doSql[T](sql: String)(f: ResultSet => T): Seq[T] = {
      val results = new mutable.ArrayBuffer[T]()
      val rs = statement.executeQuery(sql)
      while (rs.next()) {
        results.append(f(rs))
      }
      rs.close()
      results
    }

    case class QueryCase(queryName: String, sql: String, resultFile: File)

    private def runTPCDSQueries(q: QueryCase): Unit = {
      val rawResults = new mutable.ArrayBuffer[Seq[Any]]()
      val rs = statement.executeQuery(q.sql)
      val rsMetaData = rs.getMetaData
      while (rs.next()) {
        if (validationModeEnabled) {
          val row = getTypedValues(rs)
          rawResults.append(row)
        }
      }
      rs.close()

      if (validationModeEnabled) {
        val fieldNames = (1 to rsMetaData.getColumnCount).map(rsMetaData.getColumnName)
        val output = formatOutput(
          rawResults,
          fieldNames,
          _numRows = Int.MaxValue,
          truncate = Int.MaxValue,
          vertical = true
        ).trim

        val header = s"-- Automatically generated by ${getClass.getSimpleName}"

        if (regenerateGoldenFiles) {
          val goldenOutput =
            s"""$header
               |$output
             """.stripMargin
          val parent = q.resultFile.getParentFile
          if (!parent.exists()) {
            assert(parent.mkdirs(), "Could not create directory: " + parent)
          }
          stringToFile(q.resultFile, goldenOutput)
        }

        // Read back the golden file
        val expectedOutput = fileToString(q.resultFile).replace(s"$header\n", "").trim
        if (expectedOutput != output) {
          logError(
            s"""Unmatched output found in ${q.queryName}:
               |expected:
               |$expectedOutput
               |actual:
               |$output
             """.stripMargin)
        }
      }
    }

    private def setupTables(dataLocation: String): Map[String, Long] = {
      tables.map { tableName =>
        statement.execute(s"""
            |CREATE OR REPLACE TEMPORARY VIEW $tableName
            |  USING parquet OPTIONS (path '$dataLocation/$tableName')
           """.stripMargin)

        val rows = doSql[Long](s"SELECT COUNT(1) FROM $tableName") { rs =>
          rs.getLong(1)
        }
        tableName -> rows.head
      }.toMap
    }

    def run(): Unit = {
      val tableSizes = setupTables(dataLocation)
      queries.foreach { case (name, queryRelations) =>
        try {
          val queryString = fileToString(new File(queryFilePath, s"$name.sql"))
          val resultFile = new File(goldenFilePath, s"$name.sql.out")
          if (!regenerateGoldenFiles) {
            val numRows = queryRelations.map { r =>
              tableSizes.getOrElse(r, {
                logWarning(s"$r does not exist in TPCDS tables")
                0L
              })
            }.sum
            val benchmark = new Benchmark(s"TPCDS Snappy", numRows, 5)
            benchmark.addCase(name) { i =>
              runTPCDSQueries(QueryCase(name, queryString, resultFile))
            }
            benchmark.run()
          } else {
            runTPCDSQueries(QueryCase(name, queryString, resultFile))
          }
        } catch {
          case NonFatal(e) =>
            logWarning(s"Skip the query $name because: ${e.getMessage}")
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val benchmarkArgs = new TPCDSQueryBenchmarkArguments(args)

    // List of all TPC-DS queries
    val tpcdsAllQueries = Seq(
      // scalastyle:off line.size.limit
      ("q1", Seq("date_dim", "store", "store_returns", "customer")),
      ("q2", Seq("web_sales", "date_dim", "catalog_sales")),
      ("q3", Seq("item", "date_dim", "store_sales")),
      ("q4", Seq("web_sales", "date_dim", "catalog_sales", "customer", "store_sales")),
      ("q5", Seq("web_sales", "date_dim", "store", "web_site", "catalog_sales", "store_returns", "catalog_page", "catalog_returns", "web_returns", "store_sales")),
      ("q6", Seq("item", "customer_address", "date_dim", "customer", "store_sales")),
      ("q7", Seq("item", "date_dim", "customer_demographics", "promotion", "store_sales")),
      ("q8", Seq("customer_address", "date_dim", "store", "customer", "store_sales")),
      ("q9", Seq("reason", "store_sales")),
      ("q10", Seq("web_sales", "date_dim", "customer_address", "catalog_sales", "customer_demographics", "customer", "store_sales")),
      ("q11", Seq("web_sales", "date_dim", "customer", "store_sales")),
      ("q12", Seq("item", "web_sales", "date_dim")),
      ("q13", Seq("customer_address", "date_dim", "store", "customer_demographics", "household_demographics", "store_sales")),
      ("q14a", Seq("item", "web_sales", "date_dim", "catalog_sales", "store_sales")),
      ("q14b", Seq("item", "web_sales", "date_dim", "catalog_sales", "store_sales")),
      ("q15", Seq("customer_address", "date_dim", "catalog_sales", "customer")),
      ("q16", Seq("customer_address", "date_dim", "catalog_sales", "catalog_returns", "call_center")),
      ("q17", Seq("item", "date_dim", "store", "catalog_sales", "store_returns", "store_sales")),
      ("q18", Seq("item", "customer_address", "date_dim", "catalog_sales", "customer_demographics", "customer")),
      ("q19", Seq("item", "customer_address", "date_dim", "store", "customer", "store_sales")),
      ("q20", Seq("item", "date_dim", "catalog_sales")),
      ("q21", Seq("item", "date_dim", "warehouse", "inventory")),
      ("q22", Seq("item", "date_dim", "warehouse", "inventory")),
      ("q23a", Seq("item", "web_sales", "date_dim", "catalog_sales", "customer", "store_sales")),
      ("q23b", Seq("item", "web_sales", "date_dim", "catalog_sales", "customer", "store_sales")),
      ("q24a", Seq("item", "customer_address", "store", "store_returns", "customer", "store_sales")),
      ("q24b", Seq("item", "customer_address", "store", "store_returns", "customer", "store_sales")),
      ("q25", Seq("item", "date_dim", "store", "catalog_sales", "store_returns", "store_sales")),
      ("q26", Seq("item", "date_dim", "catalog_sales", "customer_demographics", "promotion")),
      ("q27", Seq("item", "date_dim", "store", "customer_demographics", "store_sales")),
      ("q28", Seq("store_sales")),
      ("q29", Seq("item", "date_dim", "store", "catalog_sales", "store_returns", "store_sales")),
      ("q30", Seq("customer_address", "date_dim", "customer", "web_returns")),
      ("q31", Seq("customer_address", "date_dim", "web_sales", "store_sales")),
      ("q32", Seq("item", "date_dim", "catalog_sales")),
      ("q33", Seq("item", "customer_address", "date_dim", "web_sales", "catalog_sales", "store_sales")),
      ("q34", Seq("date_dim", "store", "customer", "household_demographics", "store_sales")),
      ("q35", Seq("web_sales", "date_dim", "customer_address", "catalog_sales", "customer_demographics", "customer", "store_sales")),
      ("q36", Seq("item", "date_dim", "store", "store_sales")),
      ("q37", Seq("item", "date_dim", "catalog_sales", "inventory")),
      ("q38", Seq("web_sales", "date_dim", "catalog_sales", "customer", "store_sales")),
      ("q39a", Seq("item", "date_dim", "warehouse", "inventory")),
      ("q39b", Seq("item", "date_dim", "warehouse", "inventory")),
      ("q40", Seq("item", "date_dim", "warehouse", "catalog_sales", "catalog_returns")),
      ("q41", Seq("item")),
      ("q42", Seq("item", "date_dim", "store_sales")),
      ("q43", Seq("date_dim", "store", "store_sales")),
      ("q44", Seq("item", "store_sales")),
      ("q45", Seq("item", "web_sales", "customer_address", "date_dim", "customer")),
      ("q46", Seq("customer_address", "date_dim", "store", "customer", "household_demographics", "store_sales")),
      ("q47", Seq("item", "date_dim", "store", "store_sales")),
      ("q48", Seq("customer_address", "date_dim", "store", "customer_demographics", "store_sales")),
      ("q49", Seq("web_sales", "date_dim", "catalog_sales", "store_returns", "catalog_returns", "web_returns", "store_sales")),
      ("q50", Seq("date_dim", "store", "store_returns", "store_sales")),
      ("q51", Seq("web_sales", "date_dim", "store_sales")),
      ("q52", Seq("item", "date_dim", "store_sales")),
      ("q53", Seq("item", "date_dim", "store", "store_sales")),
      ("q54", Seq("item", "web_sales", "date_dim", "customer_address", "store", "catalog_sales", "customer", "store_sales")),
      ("q55", Seq("item", "date_dim", "store_sales")),
      ("q56", Seq("item", "customer_address", "date_dim", "web_sales", "catalog_sales", "store_sales")),
      ("q57", Seq("item", "date_dim", "catalog_sales", "call_center")),
      ("q58", Seq("item", "web_sales", "date_dim", "catalog_sales", "store_sales")),
      ("q59", Seq("date_dim", "store", "store_sales")),
      ("q60", Seq("item", "customer_address", "date_dim", "web_sales", "catalog_sales", "store_sales")),
      ("q61", Seq("item", "customer_address", "date_dim", "store", "promotion", "customer", "store_sales")),
      ("q62", Seq("ship_mode", "web_sales", "web_site", "warehouse", "date_dim")),
      ("q63", Seq("item", "date_dim", "store", "store_sales")),
      ("q64", Seq("customer_address", "catalog_sales", "customer_demographics", "income_band", "store_sales", "item", "date_dim", "store", "store_returns", "customer", "promotion", "catalog_returns", "household_demographics")),
      ("q65", Seq("item", "date_dim", "store", "store_sales")),
      ("q66", Seq("ship_mode", "web_sales", "date_dim", "warehouse", "catalog_sales", "time_dim")),
      ("q67", Seq("item", "date_dim", "store", "store_sales")),
      ("q68", Seq("customer_address", "date_dim", "store", "customer", "household_demographics", "store_sales")),
      ("q69", Seq("web_sales", "date_dim", "customer_address", "catalog_sales", "customer_demographics", "customer", "store_sales")),
      ("q70", Seq("date_dim", "store", "store_sales")),
      ("q71", Seq("item", "web_sales", "date_dim", "catalog_sales", "time_dim", "store_sales")),
      ("q72", Seq("item", "date_dim", "warehouse", "catalog_sales", "customer_demographics", "promotion", "inventory", "catalog_returns", "household_demographics")),
      ("q73", Seq("date_dim", "store", "customer", "household_demographics", "store_sales")),
      ("q74", Seq("web_sales", "date_dim", "customer", "store_sales")),
      ("q75", Seq("item", "web_sales", "date_dim", "catalog_sales", "store_returns", "catalog_returns", "web_returns", "store_sales")),
      ("q76", Seq("item", "web_sales", "date_dim", "catalog_sales", "store_sales")),
      ("q77", Seq("catalog_sales", "web_returns", "store_sales", "web_page", "web_sales", "date_dim", "store", "store_returns", "catalog_returns")),
      ("q78", Seq("web_sales", "date_dim", "catalog_sales", "store_returns", "catalog_returns", "web_returns", "store_sales")),
      ("q79", Seq("date_dim", "store", "customer", "household_demographics", "store_sales")),
      ("q80", Seq("catalog_sales", "web_returns", "store_sales", "item", "web_sales", "date_dim", "store", "web_site", "store_returns", "promotion", "catalog_page", "catalog_returns")),
      ("q81", Seq("customer_address", "date_dim", "customer", "catalog_returns")),
      ("q82", Seq("item", "date_dim", "inventory", "store_sales")),
      ("q83", Seq("item", "date_dim", "store_returns", "catalog_returns", "web_returns")),
      ("q84", Seq("customer_address", "customer_demographics", "store_returns", "customer", "income_band", "household_demographics")),
      ("q85", Seq("web_page", "web_sales", "customer_address", "date_dim", "customer_demographics", "reason", "web_returns")),
      ("q86", Seq("item", "web_sales", "date_dim")),
      ("q87", Seq("web_sales", "date_dim", "catalog_sales", "customer", "store_sales")),
      ("q88", Seq("store", "time_dim", "household_demographics", "store_sales")),
      ("q89", Seq("item", "date_dim", "store", "store_sales")),
      ("q90", Seq("web_page", "web_sales", "time_dim", "household_demographics")),
      ("q91", Seq("customer_address", "date_dim", "customer_demographics", "customer", "catalog_returns", "call_center", "household_demographics")),
      ("q92", Seq("item", "web_sales", "date_dim")),
      ("q93", Seq("reason", "store_returns", "store_sales")),
      ("q94", Seq("web_sales", "date_dim", "customer_address", "web_site", "web_returns")),
      ("q95", Seq("web_sales", "date_dim", "customer_address", "web_site", "web_returns")),
      ("q96", Seq("store", "time_dim", "household_demographics", "store_sales")),
      ("q97", Seq("date_dim", "catalog_sales", "store_sales")),
      ("q98", Seq("item", "date_dim", "store_sales")),
      ("q99", Seq("ship_mode", "date_dim", "warehouse", "catalog_sales", "call_center"))
      // scalastyle:on line.size.limit
    )

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesToRun = if (benchmarkArgs.queryFilter.nonEmpty) {
      val queries = tpcdsAllQueries.filter { case (queryName, _) =>
        benchmarkArgs.queryFilter.contains(queryName)
      }
      if (queries.isEmpty) {
        throw new RuntimeException(
          s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
      }
      queries
    } else {
      tpcdsAllQueries
    }

    val props = new Properties()
    props.put("user", System.getProperty("user.name"))
    props.put("password", "")
    val conn = DriverManager.getConnection(
      s"jdbc:postgresql://${benchmarkArgs.host}/default", props)
    conn.setAutoCommit(false)
    val stmt = conn.createStatement()
    stmt.setFetchSize(100000)
    val tpcdsQueries = TpcdsQueries(stmt, queriesToRun, benchmarkArgs.dataLocation)
    tpcdsQueries.run()
  }
}
