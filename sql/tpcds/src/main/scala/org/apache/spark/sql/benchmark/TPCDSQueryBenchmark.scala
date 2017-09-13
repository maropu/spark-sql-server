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

import java.io.ByteArrayOutputStream
import java.sql.{DriverManager, ResultSet, Statement}
import java.util.Properties

import scala.collection.mutable

import org.apache.commons.lang3.StringUtils


/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 *  scala -cp <this test jar> org.apache.spark.sql.server.benchmark.TPCDSQueryBenchmark
 */
object TPCDSQueryBenchmark extends Logging {

  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, Thread.currentThread.getContextClassLoader)
    // scalastyle:on classforname
  }

  // Register a JDBC driver for PostgreSQL
  classForName(classOf[org.postgresql.Driver].getCanonicalName)

  case class TpcdsQueries(statement: Statement, queries: Seq[String], dataLocation: String) {

    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")

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

      def logResultRows(_numRows: Int, truncate: Int = 20, vertical: Boolean = false): Unit = {
        val numRows = _numRows.max(0)
        val hasMoreData = logs.length > numRows
        val data = logs.take(numRows)

        // For array values, replace Seq and Array with square brackets
        // For cells that are beyond `truncate` characters, replace it with the
        // first `truncate-3` and "..."
        val fieldNames = (1 to rsMetaData.getColumnCount).map(rsMetaData.getColumnName)
        val rows: Seq[Seq[String]] = fieldNames +: data.map { row =>
          row.map { cell =>
            val str = cell match {
              case null => "null"
              case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
              case array: Array[_] => array.mkString("[", ", ", "]")
              case seq: Seq[_] => seq.mkString("[", ", ", "]")
              case _ => cell.toString
            }
            if (truncate > 0 && str.length > truncate) {
              // do not show ellipses for strings shorter than 4 characters.
              if (truncate < 4) str.substring(0, truncate)
              else str.substring(0, truncate - 3) + "..."
            } else {
              str
            }
          }: Seq[String]
        }

        val sb = new StringBuilder
        val numCols = rsMetaData.getColumnCount
        // We set a minimum column width at '3'
        val minimumColWidth = 3

        if (!vertical) {
          // Initialise the width of each column to a minimum value
          val colWidths = Array.fill(numCols)(minimumColWidth)

          // Compute the width of each column
          for (row <- rows) {
            for ((cell, i) <- row.zipWithIndex) {
              colWidths(i) = math.max(colWidths(i), cell.length)
            }
          }

          // Create SeparateLine
          val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

          // column names
          rows.head.zipWithIndex.map { case (cell, i) =>
            if (truncate > 0) {
              StringUtils.leftPad(cell, colWidths(i))
            } else {
              StringUtils.rightPad(cell, colWidths(i))
            }
          }.addString(sb, "|", "|", "|\n")

          sb.append(sep)

          // data
          rows.tail.foreach {
            _.zipWithIndex.map { case (cell, i) =>
              if (truncate > 0) {
                StringUtils.leftPad(cell.toString, colWidths(i))
              } else {
                StringUtils.rightPad(cell.toString, colWidths(i))
              }
            }.addString(sb, "|", "|", "|\n")
          }

          sb.append(sep)
        } else {
          // Extended display mode enabled
          val fieldNames = rows.head
          val dataRows = rows.tail

          // Compute the width of field name and data columns
          val fieldNameColWidth = fieldNames.foldLeft(minimumColWidth) { case (curMax, fieldName) =>
            math.max(curMax, fieldName.length)
          }
          val dataColWidth = dataRows.foldLeft(minimumColWidth) { case (curMax, row) =>
            math.max(curMax, row.map(_.length).reduceLeftOption[Int] { case (cellMax, cell) =>
              math.max(cellMax, cell)
            }.getOrElse(0))
          }

          dataRows.zipWithIndex.foreach { case (row, i) =>
            // "+ 5" in size means a character length except for padded names and data
            val rowHeader = StringUtils.rightPad(
              s"-RECORD $i", fieldNameColWidth + dataColWidth + 5, "-")
            sb.append(rowHeader).append("\n")
            row.zipWithIndex.map { case (cell, j) =>
              val fieldName = StringUtils.rightPad(fieldNames(j), fieldNameColWidth)
              val data = StringUtils.rightPad(cell, dataColWidth)
              s" $fieldName | $data "
            }.addString(sb, "", "\n", "\n")
          }
        }

        // Print a footer
        if (vertical && data.isEmpty) {
          // In a vertical mode, print an empty row set explicitly
          sb.append("(0 rows)\n")
        } else if (hasMoreData) {
          // For Data that has more than "numRows" records
          val rowsString = if (numRows == 1) "row" else "rows"
          sb.append(s"only showing top $numRows $rowsString\n")
        }

        logInfo(s"\n${sb.toString()}")
      }

      while (rs.next()) {
        if (log.isInfoEnabled) {
          val row = (1 to rsMetaData.getColumnCount).map(getTypedValue)
          logs.append(row)
        }
        results.append(f(rs))
      }

      logResultRows(_numRows = 10, truncate = 20, vertical = true)
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

    private def resourceToBytes(resource: String): Array[Byte] = {
      val inStream = Thread.currentThread.getContextClassLoader.getResourceAsStream(resource)
      val outStream = new ByteArrayOutputStream
      try {
        var reading = true
        while (reading) {
          inStream.read() match {
            case -1 => reading = false
            case c => outStream.write(c)
          }
        }
        outStream.flush()
      }
      finally {
        inStream.close()
      }
      outStream.toByteArray
    }

    private def resourceToString(resource: String): String = {
      new String(resourceToBytes(resource), "UTF-8")
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
        benchmark.run()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      // scalastyle:off
      println(
        s"""
           |Usage: scala -cp <this test jar> <this class> <TPCDS data location> [<server URL>] [<query filter>]
           |
           |In order to run this benchmark, the value of <TPCDS data location> needs to be set
           |to the location where the generated TPCDS data is stored.
         """.stripMargin)
      // scalastyle:on
      System.exit(1)
    }

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

    val queryFilter = args.drop(2).headOption.map(_.split(",").map(_.trim).toSet)
      .getOrElse(Set.empty)
    val queriesToRun = if (queryFilter.nonEmpty) {
      val queries = tpcdsAllQueries.filter { case queryName => queryFilter.contains(queryName) }
      if (queries.isEmpty) {
        throw new RuntimeException("Bad query name filter: " + queryFilter)
      }
      queries
    } else {
      tpcdsAllQueries
    }

    val props = new Properties()
    props.put("user", System.getProperty("user.name"))
    props.put("password", "")
    val servUrl = args.drop(1).headOption.getOrElse("localhost:5432")
    val conn = DriverManager.getConnection(s"jdbc:postgresql://$servUrl/default", props)
    conn.setAutoCommit(false)
    val stmt = conn.createStatement()
    stmt.setFetchSize(100000)
    val tpcdsQueries = TpcdsQueries(stmt, queriesToRun, dataLocation = args.head)
    tpcdsQueries.run()
  }
}
