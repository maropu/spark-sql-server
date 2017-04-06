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
import org.apache.spark.sql.test.SharedSQLContext

class PostgreSQLParserSuite extends SparkFunSuite with SharedSQLContext with BeforeAndAfterAll {

  lazy val parser = SQLServerEnv.sqlParser

  override protected def beforeAll() : Unit = {
    super.beforeAll()
    SQLServerEnv.withSQLContext(sqlContext)
  }

  def assertValidSQLString(pgSql: String, sparkSql: String): Unit = {
    assert(parser.parsePlan(pgSql) === parser.parsePlan(sparkSql))
  }

  test("~") {
    assertValidSQLString(
      "SELECT * FROM testData WHERE value ~ 'abc%'",
      "SELECT * FROM testData WHERE value LIKE 'abc%'"
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
}
