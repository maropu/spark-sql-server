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

/**
 * A test suite for psql commands.
 * See an URL below for detailed options;
 *  https://www.postgresql.org/docs/9.6/static/app-psql.html
 */
class PsqlCommandV7_4Suite extends PostgreSQLJdbcTest with BeforeAndAfterAll {

  override protected def beforeAll() : Unit = {
    super.beforeAll()

    testJdbcStatement { statement =>
      Seq(
        "CREATE DATABASE d1",
        "CREATE TABLE t1(a INT, b STRING, c DOUBLE)",
        "CREATE TABLE t2(key STRING, value DOUBLE)"
      ).foreach { sqlText =>
        assert(statement.execute(sqlText))
      }
    }
  }

  override protected def afterAll() : Unit = {
    try {
      testJdbcStatement { statement =>
        Seq(
          "DROP TABLE IF EXISTS t1",
          "DROP TABLE IF EXISTS t2",
          "DROP DATABASE IF EXISTS d1"
        ).foreach { sqlText =>
          assert(statement.execute(sqlText))
        }
      }
    } finally {
      super.afterAll()
    }
  }

  test("""\l""") {
    testJdbcStatement { statement =>
      val rs = statement.executeQuery(
        """
          |SELECT
          |  d.datname as "Name",
          |  pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
          |  pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding",
          |  pg_catalog.array_to_string(d.datacl, '\n') AS "Access privileges"
          |FROM
          |  pg_catalog.pg_database d
          |ORDER BY
          |  1
         """.stripMargin
      )

      assert(rs.next())
      assert("d1" === rs.getString(1))
      assert(rs.next())
      assert("default" === rs.getString(1))
      assert(rs.next())
      assert("pg_catalog" === rs.getString(1))
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\d""") {
    testJdbcStatement { statement =>
      val rs = statement.executeQuery(
        """
          |SELECT
          |  n.nspname as "Schema",
          |  c.relname as "Name",
          |  CASE c.relkind
          |    WHEN 'r' THEN 'table'
          |    WHEN 'v' THEN 'view'
          |    WHEN 'm' THEN 'materialized view'
          |    WHEN 'i' THEN 'index'
          |    WHEN 'S' THEN 'sequence'
          |    WHEN 's' THEN 'special'
          |    WHEN 'f' THEN 'foreign table'
          |  END as "Type",
          |  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
          |FROM
          |  pg_catalog.pg_class c
          |LEFT JOIN
          |  pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          |WHERE
          |  c.relkind IN ('r','v','m','S','f','')
          |    AND n.nspname <> 'pg_catalog'
          |    AND n.nspname <> 'information_schema'
          |    AND n.nspname !~ '^pg_toast'
          |    AND pg_catalog.pg_table_is_visible(c.oid)
          |ORDER BY
          |  1,2
         """.stripMargin
      )

      assert(rs.next())
      // `pg_namespace` is implicitly added from `metadata.scala` for workaround
      assert("spark" === rs.getString(1))
      assert("pg_namespace" === rs.getString(2))
      assert("table" === rs.getString(3))
      assert("" === rs.getString(4))
      assert(rs.next())

      assert("spark" === rs.getString(1))
      assert("t1" === rs.getString(2))
      assert("table" === rs.getString(3))
      assert("" === rs.getString(4))
      assert(rs.next())
      assert("spark" === rs.getString(1))
      assert("t2" === rs.getString(2))
      assert("table" === rs.getString(3))
      assert("" === rs.getString(4))
      assert(!rs.next())
      rs.close()
    }
  }

  ignore("""\d <table name>""") {
    testJdbcStatement { statement =>
      val rs1 = statement.executeQuery(
        """
          |SELECT
          |  c.oid, n.nspname, c.relname
          |FROM
          |  pg_catalog.pg_class c
          |LEFT JOIN
          |  pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          |WHERE
          |  c.relname ~ '^(t1)$'
          |    AND pg_catalog.pg_table_is_visible(c.oid)
          |ORDER BY
          |  2, 3
        """.stripMargin
      )

      assert(rs1.next())
      assert("spark" === rs1.getString(2))
      assert("t1" === rs1.getString(3))

      // Get an OID number for a table `t1`
      val relOid = rs1.getInt(1)

      assert(!rs1.next())
      rs1.close()

      val rs2 = statement.executeQuery(
        """
          |SELECT
          |  relchecks,
          |  relkind,
          |  relhasindex,
          |  relhasrules,
          |  reltriggers <> 0,
          |  false,
          |  false,
          |  relhasoids,
          |  '',
          |  ''
          |FROM
          |  pg_catalog.pg_class
          |WHERE
          |  oid = '$relOid'
        """.stripMargin
      )

      assert(rs2.next())
      assert(0 === rs2.getInt(1))
      assert("r" === rs2.getString(2))
      assert(!rs2.getBoolean(3))
      assert(!rs2.getBoolean(4))
      assert(!rs2.getBoolean(5))
      assert(!rs2.getBoolean(6))
      assert(!rs2.getBoolean(7))
      assert(!rs2.getBoolean(8))
      assert("" === rs2.getString(9))
      assert("" === rs2.getString(10))
      rs2.close()

      // TODO: Spark-2.1 cannot handle sub-queries without aggregate for Hive SerDe tables.
      // So, we do not support `\d <table name>` now.
      val rs3 = statement.executeQuery(
        """
          |SELECT
          |  a.attname,
          |  pg_catalog.format_type(a.atttypid, a.atttypmod),
          |  (
          |    SELECT
          |      substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
          |    FROM
          |      pg_catalog.pg_attrdef d
          |    WHERE
          |      d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef
          |   ),
          |  a.attnotnull,
          |  a.attnum,
          |  NULL AS attcollation,
          |  NULL AS indexdef,
          |  NULL AS attfdwoptions
          |FROM
          |  pg_catalog.pg_attribute a
          |WHERE
          |  a.attrelid = '6208' AND a.attnum > 0 AND NOT a.attisdropped
          |ORDER BY
          |  a.attnum
        """.stripMargin
      )

      assert(rs3.next())
      rs3.close()
    }
  }

  test("""\df""") {
    testJdbcStatement { statement =>
      // Define a temporary function
      val jarPath = "src/test/resources/TestUDTF.jar"
      val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"
      Seq(
        s"ADD JAR $jarURL",
        "DROP TEMPORARY FUNCTION IF EXISTS udtf",
        "CREATE TEMPORARY FUNCTION udtf AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'"
      ).foreach { sqlText =>
        assert(statement.execute(sqlText))
      }

      // TODO: Since Spark-2.1 cannot use `||` for string concatenation, so we should fix
      // SPARK-19951 to parse this SQL string.
      val rs = statement.executeQuery(
        """
          |SELECT n.nspname as "Schema",
          |  p.proname as "Name",
          |  CASE
          |    WHEN p.proretset THEN 'SETOF '
          |    ELSE ''
          |  END || pg_catalog.format_type(p.prorettype, NULL) as "Result data type",
          |  pg_catalog.oidvectortypes(p.proargtypes) as "Argument data types",
          |  CASE
          |    WHEN p.proisagg THEN 'agg'
          |    WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
          |    ELSE 'normal'
          |  END AS "Type"
          |FROM pg_catalog.pg_proc p
          |  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
          |WHERE pg_catalog.pg_function_is_visible(p.oid)
          |  AND n.nspname <> 'pg_catalog'
          |  AND n.nspname <> 'information_schema'
          |ORDER BY 1, 2, 4
        """.stripMargin
      )

      assert(rs.next())
      assert("udtf" === rs.getString(2))
      assert(!rs.next())
      rs.close()
    }
  }
}
