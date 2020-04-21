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

/**
 * This test suite just checks if psql commands can be accepted and return an empty result set.
 * See an URL below for detailed options;
 *  https://www.postgresql.org/docs/current/static/app-psql.html
 */
class PsqlCommandsV10Suite extends PgJdbcTest(pgVersion = "10") {

  test("""\l""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\d""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\d <table name>""") {
    withJdbcStatement { statement =>
      val rs = statement.executeQuery(
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\df""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }
}

class PsqlCommandsV9_6Suite extends PgJdbcTest(pgVersion = "9.6") {

  test("""\d""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\d <table name>""") {
    withJdbcStatement { statement =>
      val rs = statement.executeQuery(
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
      assert(!rs.next())
      rs.close()
    }
  }
}

class PsqlCommandsV8_4Suite extends PgJdbcTest(pgVersion = "8.4") {

  test("""\l""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\d""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\d <table name>""") {
    withJdbcStatement { statement =>
      val rs = statement.executeQuery(
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\df""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }
}

class PsqlCommandsV7_4Suite extends PgJdbcTest(pgVersion = "7.4") {

  test("""\l""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\d""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\d <table name>""") {
    withJdbcStatement { statement =>
      val rs = statement.executeQuery(
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
      assert(!rs.next())
      rs.close()
    }
  }

  test("""\df""") {
    withJdbcStatement { statement =>
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
      assert(!rs.next())
      rs.close()
    }
  }
}
