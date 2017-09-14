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

import java.sql.{DriverManager, ResultSet, Statement}
import java.util.Properties

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.benchmark.Utils._


/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 *  scala -cp <this test jar> org.apache.spark.sql.server.benchmark.TPCDSQueryBenchmark
 */
object TPCDSQueryBenchmark extends Logging {

  // Register a JDBC driver for PostgreSQL
  classForName(classOf[org.postgresql.Driver].getCanonicalName)

  case class TpcdsQueries(statement: Statement, queries: Seq[String], dataLocation: String) {

    private val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
      "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
      "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
      "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
      "time_dim", "web_page")

    private def doSql[T](sql: String)(f: ResultSet => T): Seq[T] = {
      val results = new mutable.ArrayBuffer[T]()
      val logs = new mutable.ArrayBuffer[Seq[Any]]()
      val rs = statement.executeQuery(sql)
      val rsMetaData = rs.getMetaData

      def getTypedValue(offset: Int): Any = {
        rsMetaData.getColumnType(offset) match {
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

      while (rs.next()) {
        if (log.isInfoEnabled) {
          val row = (1 to rsMetaData.getColumnCount).map(getTypedValue)
          logs.append(row)
        }
        results.append(f(rs))
      }

      if (log.isInfoEnabled) {
        val fieldNames = (1 to rsMetaData.getColumnCount).map(rsMetaData.getColumnName)
        val logOutput = showString(logs, fieldNames, _numRows = 10, truncate = 60, vertical = true)
        logInfo(s"\n$logOutput")
      }

      rs.close()
      results.toSeq
    }

    private def setupTables(dataLocation: String): Map[String, Long] = {
      tables.map { tableName =>
        statement.execute(s"""
            | CREATE OR REPLACE TEMPORARY VIEW $tableName
            |   USING parquet OPTIONS (path '$dataLocation/$tableName')
           """.stripMargin)

        val rows = doSql[Long](s"SELECT COUNT(1) FROM $tableName") { rs =>
          rs.getLong(1)
        }
        tableName -> rows.head
      }.toMap
    }

    def run(): Unit = {
      val tableSizes = setupTables(dataLocation)
      queries.foreach { name =>
        val queryString = resourceToString(s"tpcds/$name.sql")
        // TODO: How to check input #rows from `tableSizes`
        val numRows = 1
        val benchmark = new Benchmark(s"TPCDS Snappy", numRows, 5)
        benchmark.addCase(name) { i =>
          doSql[Long](queryString) { _ => 1}
        }
        try { benchmark.run() } catch {
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
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
      "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
      "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
      "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesToRun = if (benchmarkArgs.queryFilter.nonEmpty) {
      val queries = tpcdsAllQueries.filter { case queryName =>
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
