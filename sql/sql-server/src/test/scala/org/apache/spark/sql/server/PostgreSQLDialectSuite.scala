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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.server.service.postgresql.Metadata
import org.apache.spark.sql.test.SharedSQLContext

class PostgreSQLDialectSuite extends SparkFunSuite with SharedSQLContext with BeforeAndAfterAll {

  lazy val parser = SQLServerEnv.sqlParser

  override protected def beforeAll() : Unit = {
    super.beforeAll()
    SQLServerEnv.withSQLContext(sqlContext)
    Metadata.initSystemFunctions(sqlContext)
  }

  def assertValidSQLString(pgSql: String, sparkSql: String): Unit = {
    assert(parser.parsePlan(pgSql) === parser.parsePlan(sparkSql))
  }

  def assertQueryExecutionInPgParser(sql: String, expected: Seq[Row]): Unit = {
    val ds = Dataset.ofRows(sqlContext.sparkSession, parser.parsePlan(sql))
    assert(ds.collect === expected)
  }

  test("~") {
    assertValidSQLString(
      "SELECT * FROM testData WHERE value ~ 'abc'",
      "SELECT * FROM testData WHERE value RLIKE 'abc'"
    )
  }

  test("::") {
    assertValidSQLString("SELECT 3 :: INT", "SELECT CAST(3 AS INT)")
    assertValidSQLString("SELECT 8 :: TEXT", "SELECT CAST(8 AS STRING)")
  }

  test("::regproc") {
    sqlContext.udf.register("testUdf", () => "test")
    val ds = Dataset.ofRows(sqlContext.sparkSession, parser.parsePlan("SELECT 'testUdf'::regproc"))
    assert(ds.collect === Seq(Row("test")))
  }

  test("pg internal functions") {
    assert(sqlContext.sql("SELECT ANY(array('abc', 'de'))").collect === Seq(Row("abc")))
    assert(sqlContext.sql("SELECT current_schemas(false)").collect === Seq(Row(Seq("spark"))))
    assert(sqlContext.sql("SELECT array_upper(current_schemas(false), 1)").collect === Seq(Row(1)))
    assert(sqlContext.sql("SELECT array_in()").collect === Seq(Row("array_in")))
    assert(sqlContext.sql("SELECT pg_catalog.obj_description(0, '')").collect === Seq(Row("")))
    assert(sqlContext.sql("SELECT pg_catalog.pg_get_expr('', 0)").collect === Seq(Row("")))
    assert(sqlContext.sql("SELECT pg_catalog.pg_table_is_visible(0)").collect === Seq(Row(true)))
    assert(sqlContext.sql("SELECT pg_catalog.pg_get_userbyid(0)").collect === Seq(Row("")))
    assertQueryExecutionInPgParser("SELECT * FROM generate_series(0, 1)", Row(0) :: Row(1) :: Nil)
    assertQueryExecutionInPgParser("SELECT * FROM generate_series(0, 10, 5)",
      Row(0) :: Row(5) :: Row(10) :: Nil)
  }
}
