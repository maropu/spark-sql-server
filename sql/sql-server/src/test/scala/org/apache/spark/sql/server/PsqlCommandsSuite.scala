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
 *  https://www.postgresql.org/docs/current/static/app-psql.html
 */
class PsqlCommandsV10Suite extends PgJdbcTest(pgVersion = "10") with BeforeAndAfterAll {

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
          |  d.datcollate as "Collate",
          |  d.datctype as "Ctype",
          |  pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges"
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
          |    WHEN 'p' THEN 'table'
          |  END as "Type",
          |  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
          |FROM
          |  pg_catalog.pg_class c
          |LEFT JOIN
          |  pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          |WHERE
          |  c.relkind IN ('r','p','v','m','S','f','')
          |    AND n.nspname <> 'pg_catalog'
          |    AND n.nspname <> 'information_schema'
          |    AND n.nspname !~ '^pg_toast'
          |    AND pg_catalog.pg_table_is_visible(c.oid)
          |    /* TODO: See PgMetadata.initSystemCatalogTables */
          |    AND c.relname != 'pg_namespace'
          |ORDER BY
          |  1,2
         """.stripMargin
      )

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

  test("""\d <table name>""") {
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
          |  c.relname OPERATOR(pg_catalog.~) '^(t1)$'
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
        s"""
          |SELECT
          |  c.relchecks,
          |  c.relkind,
          |  c.relhasindex,
          |  c.relhasrules,
          |  c.relhastriggers,
          |  c.relrowsecurity,
          |  c.relforcerowsecurity,
          |  c.relhasoids,
          |  '',
          |  c.reltablespace,
          |  CASE
          |    WHEN c.reloftype = 0 THEN ''
          |    ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text
          |  END,
          |  c.relpersistence,
          |  c.relreplident
          |FROM
          |  pg_catalog.pg_class c
          |LEFT JOIN
          |  pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
          |WHERE
          |  c.oid = '$relOid'
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
      assert(0 === rs2.getInt(10))
      assert(!rs2.next())
      rs2.close()

      val rs3 = statement.executeQuery(
        s"""
          |SELECT
          |  a.attname,
          |  pg_catalog.format_type(a.atttypid, a.atttypmod),
          |  (
          |    SELECT
          |      substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
          |    FROM
          |      pg_catalog.pg_attrdef d
          |    WHERE
          |      d.adrelid = a.attrelid
          |        AND d.adnum = a.attnum
          |        AND a.atthasdef
          |  ),
          |  a.attnotnull,
          |  a.attnum,
          |  (
          |    SELECT
          |      c.collname
          |    FROM
          |      pg_catalog.pg_collation c,
          |      pg_catalog.pg_type t
          |    WHERE
          |      c.oid = a.attcollation
          |        AND t.oid = a.atttypid
          |        AND a.attcollation <> t.typcollation
          |  ) AS attcollation,
          |  a.attidentity,
          |  NULL AS indexdef,
          |  NULL AS attfdwoptions
          |FROM
          |  pg_catalog.pg_attribute a
          |WHERE
          |  a.attrelid = '$relOid'
          |    AND a.attnum > 0
          |    AND NOT a.attisdropped
          |ORDER BY
          |  a.attnum
        """.stripMargin
      )

      assert(rs3.next())
      assert("a" === rs3.getString(1))
      assert("int4" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(1 === rs3.getInt(5))
      assert(rs3.next())
      assert("b" === rs3.getString(1))
      assert("varchar" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(2 === rs3.getInt(5))
      assert(rs3.next())
      assert("c" === rs3.getString(1))
      assert("float8" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(3 === rs3.getInt(5))
      assert(!rs3.next())
      rs3.close()

      val rs4 = statement.executeQuery(
        s"""
          |SELECT
          |  inhparent::pg_catalog.regclass,
          |  pg_catalog.pg_get_expr(c.relpartbound, inhrelid)
          |FROM
          |  pg_catalog.pg_class c
          |JOIN
          |  pg_catalog.pg_inherits i ON c.oid = inhrelid
          |WHERE
          |  c.oid = '$relOid'
          |    AND c.relispartition
        """.stripMargin
      )

      assert(!rs4.next())

      // In PostgreSQL, `ARRAY` is not a function but a syntax defined in parser definition
      // (https://github.com/postgres/postgres/blob/master/src/backend/parser/gram.y#L13166).
      // So, Spark-2.3 can't handle a query below:
      //
      // val rs5 = statement.executeQuery(
      //   s"""
      //     |SELECT
      //     |  pol.polname,
      //     |  pol.polpermissive,
      //     |  CASE
      //     |    WHEN pol.polroles = '{0}' THEN NULL
      //     |    ELSE
      //     |      pg_catalog.array_to_string(
      //     |        array(
      //     |          select
      //     |            rolname
      //     |          from
      //     |            pg_catalog.pg_roles
      //     |          where
      //     |            oid = any (pol.polroles)
      //     |          order by 1
      //     |        ),
      //     |      ',')
      //     |  END,
      //     |  pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
      //     |  pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
      //     |  CASE pol.polcmd
      //     |    WHEN 'r' THEN 'SELECT'
      //     |    WHEN 'a' THEN 'INSERT'
      //     |    WHEN 'w' THEN 'UPDATE'
      //     |    WHEN 'd' THEN 'DELETE'
      //     |    END AS cmd
      //     |FROM
      //     |  pg_catalog.pg_policy pol
      //     |WHERE
      //     |  pol.polrelid = '$relOid'
      //     |ORDER BY
      //     |  1
      //   """.stripMargin
      // )
      //
      // assert(!rs5.next())
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

      val rs = statement.executeQuery(
        """
          |SELECT
          |  n.nspname as "Schema",
          |  p.proname as "Name",
          |  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
          |  pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
          |  CASE
          |    WHEN p.proisagg THEN 'agg'
          |    WHEN p.proiswindow THEN 'window'
          |    WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
          |    ELSE 'normal'
          |  END as "Type"
          |FROM
          |  pg_catalog.pg_proc p
          |LEFT JOIN
          |  pg_catalog.pg_namespace n ON n.oid = p.pronamespace
          |WHERE
          |  pg_catalog.pg_function_is_visible(p.oid)
          |    AND n.nspname <> 'pg_catalog'
          |    AND n.nspname <> 'information_schema'
          |ORDER BY
          |  1, 2, 4
        """.stripMargin
      )

      assert(rs.next())
      assert("udtf" === rs.getString(2))
      assert(!rs.next())
      rs.close()
    }
  }
}

class PsqlCommandsV9_6Suite extends PgJdbcTest(pgVersion = "9.6") with BeforeAndAfterAll {

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
          |    /* TODO: See PgMetadata.initSystemCatalogTables */
          |    AND c.relname != 'pg_namespace'
          |ORDER BY
          |  1,2
         """.stripMargin
      )

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

  test("""\d <table name>""") {
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
        s"""
          |SELECT
          |  c.relchecks,
          |  c.relkind,
          |  c.relhasindex,
          |  c.relhasrules,
          |  c.relhastriggers,
          |  c.relrowsecurity,
          |  c.relforcerowsecurity,
          |  c.relhasoids,
          |  '',
          |  c.reltablespace,
          |  CASE
          |    WHEN c.reloftype = 0 THEN ''
          |    ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text
          |  END,
          |  c.relpersistence,
          |  c.relreplident
          |FROM
          |  pg_catalog.pg_class c
          |LEFT JOIN
          |  pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
          |WHERE
          |  c.oid = '$relOid'
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
      assert(0 === rs2.getInt(10))
      assert(!rs2.next())
      rs2.close()

      val rs3 = statement.executeQuery(
        s"""
          |SELECT
          |  a.attname,
          |  pg_catalog.format_type(a.atttypid, a.atttypmod),
          |  (
          |    SELECT
          |      substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
          |    FROM
          |      pg_catalog.pg_attrdef d
          |    WHERE
          |      d.adrelid = a.attrelid
          |        AND d.adnum = a.attnum
          |        AND a.atthasdef
          |  ),
          |  a.attnotnull,
          |  a.attnum,
          |  (
          |    SELECT
          |      c.collname
          |    FROM
          |      pg_catalog.pg_collation c, pg_catalog.pg_type t
          |    WHERE
          |      c.oid = a.attcollation
          |        AND t.oid = a.atttypid
          |        AND a.attcollation <> t.typcollation
          |  ) AS attcollation,
          |  NULL AS indexdef,
          |  NULL AS attfdwoptions
          |FROM
          |  pg_catalog.pg_attribute a
          |WHERE
          |  a.attrelid = '$relOid'
          |    AND a.attnum > 0
          |    AND NOT a.attisdropped
          |ORDER BY
          |  a.attnum
        """.stripMargin
      )

      assert(rs3.next())
      assert("a" === rs3.getString(1))
      assert("int4" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(1 === rs3.getInt(5))
      assert(rs3.next())
      assert("b" === rs3.getString(1))
      assert("varchar" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(2 === rs3.getInt(5))
      assert(rs3.next())
      assert("c" === rs3.getString(1))
      assert("float8" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(3 === rs3.getInt(5))
      assert(!rs3.next())
      rs3.close()

      // In PostgreSQL, `ARRAY` is not a function but a syntax defined in parser definition
      // (https://github.com/postgres/postgres/blob/master/src/backend/parser/gram.y#L13166).
      // So, Spark-2.3 can't handle a query below:
      //
      // val rs4 = statement.executeQuery(
      //   s"""
      //     |SELECT
      //     |  pol.polname,
      //     |  CASE
      //     |    WHEN pol.polroles = '{0}' THEN NULL
      //     |    ELSE
      //     |      array_to_string(
      //     |        array(
      //     |          select
      //     |            rolname
      //     |          from
      //     |            pg_roles
      //     |          where
      //     |            oid = any (pol.polroles)
      //     |          order by
      //     |            1
      //     |        ),
      //     |      ',')
      //     |  END,
      //     |  pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
      //     |  pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
      //     |  CASE pol.polcmd
      //     |    WHEN 'r' THEN 'SELECT'
      //     |    WHEN 'a' THEN 'INSERT'
      //     |    WHEN 'w' THEN 'UPDATE'
      //     |    WHEN 'd' THEN 'DELETE'
      //     |    WHEN '*' THEN 'ALL'
      //     |  END AS cmd
      //     |FROM
      //     |  pg_catalog.pg_policy pol
      //     |WHERE
      //     |  pol.polrelid = '$relOid'
      //     |ORDER BY
      //     |  1
      //     """.stripMargin)
      //
      // assert(!rs4.next())
      // rs4.close()
    }
  }
}

class PsqlCommandsV8_4Suite extends PgJdbcTest(pgVersion = "8.4") with BeforeAndAfterAll {

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
          |    /* TODO: See PgMetadata.initSystemCatalogTables */
          |    AND c.relname != 'pg_namespace'
          |ORDER BY
          |  1,2
         """.stripMargin
      )

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

  test("""\d <table name>""") {
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
        s"""
          |SELECT
          |  relchecks, relkind, relhasindex, relhasrules, reltriggers <> 0, false, false,
          |  relhasoids, '', reltablespace
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
      assert(0 === rs2.getInt(10))
      assert(!rs2.next())
      rs2.close()

      val rs3 = statement.executeQuery(
        s"""
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
          |  ),
          |  a.attnotnull,
          |  a.attnum,
          |  NULL AS attcollation,
          |  NULL AS indexdef,
          |  NULL AS attfdwoptions
          |FROM
          |  pg_catalog.pg_attribute a
          |WHERE
          |  a.attrelid = '$relOid' AND a.attnum > 0 AND NOT a.attisdropped
          |ORDER BY
          |  a.attnum
        """.stripMargin
      )

      assert(rs3.next())
      assert("a" === rs3.getString(1))
      assert("int4" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(1 === rs3.getInt(5))
      assert(rs3.next())
      assert("b" === rs3.getString(1))
      assert("varchar" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(2 === rs3.getInt(5))
      assert(rs3.next())
      assert("c" === rs3.getString(1))
      assert("float8" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(3 === rs3.getInt(5))
      assert(!rs3.next())
      rs3.close()

      val rs4 = statement.executeQuery(
        s"""
          |SELECT
          |  c.oid::pg_catalog.regclass
          |FROM
          |  pg_catalog.pg_class c, pg_catalog.pg_inherits i
          |WHERE
          |  c.oid=i.inhparent AND i.inhrelid = '$relOid'
          |ORDER BY
          |  inhseqno
         """.stripMargin
      )

      assert(!rs4.next())
      rs4.close()

      val rs5 = statement.executeQuery(
        s"""
          |SELECT
          |  c.oid::pg_catalog.regclass
          |FROM
          |  pg_catalog.pg_class c, pg_catalog.pg_inherits i
          |WHERE
          |  c.oid=i.inhrelid AND i.inhparent = '6217'
          |ORDER BY
          |  c.relname
         """.stripMargin
      )

      assert(!rs5.next())
      rs5.close()
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

class PsqlCommandsV7_4Suite extends PgJdbcTest(pgVersion = "7.4") with BeforeAndAfterAll {

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
          |    /* TODO: See PgMetadata.initSystemCatalogTables */
          |    AND c.relname != 'pg_namespace'
          |ORDER BY
          |  1,2
         """.stripMargin
      )

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

  test("""\d <table name>""") {
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
        s"""
          |SELECT
          |  relchecks, relkind, relhasindex, relhasrules, reltriggers <> 0, false, false,
          |  relhasoids, '', ''
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
      assert(!rs2.next())
      rs2.close()

      val rs3 = statement.executeQuery(
        s"""
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
          |  ),
          |  a.attnotnull,
          |  a.attnum,
          |  NULL AS attcollation,
          |  NULL AS indexdef,
          |  NULL AS attfdwoptions
          |FROM
          |  pg_catalog.pg_attribute a
          |WHERE
          |  a.attrelid = '$relOid' AND a.attnum > 0 AND NOT a.attisdropped
          |ORDER BY
          |  a.attnum
        """.stripMargin
      )

      assert(rs3.next())
      assert("a" === rs3.getString(1))
      assert("int4" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(1 === rs3.getInt(5))
      assert(rs3.next())
      assert("b" === rs3.getString(1))
      assert("varchar" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(2 === rs3.getInt(5))
      assert(rs3.next())
      assert("c" === rs3.getString(1))
      assert("float8" === rs3.getString(2))
      assert(false === rs3.getBoolean(4))
      assert(3 === rs3.getInt(5))
      assert(!rs3.next())
      rs3.close()

      val rs4 = statement.executeQuery(
        s"""
          |SELECT
          |  c.oid::pg_catalog.regclass
          |FROM
          |  pg_catalog.pg_class c, pg_catalog.pg_inherits i
          |WHERE
          |  c.oid=i.inhparent AND i.inhrelid = '$relOid'
          |ORDER BY
          |  inhseqno
         """.stripMargin
      )

      assert(!rs4.next())
      rs4.close()
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
