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

package org.apache.spark.sql.server.service.postgresql

import java.sql.SQLException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._


/** This is the PostgreSQL system information such as catalog tables and functions. */
object Metadata extends Logging {

  // Since v7.3, all the catalog tables have been moved in a `pg_catalog` database
  private val catalogDbName = "pg_catalog"
  private val catalogTables = Seq(
    "pg_database",
    "pg_namespace",
    "pg_type",
    "pg_roles",
    "pg_user",
    "pg_class",
    "pg_attribute",
    "pg_index",
    "pg_proc",
    "pg_description",
    "pg_depend",
    "pg_constraint",
    "pg_attrdef"
  )

  // `src/include/catalog/unused_oids` in a PostgreSQL source repository prints unused oids; 2-9,
  // 3300, 3308-3309, 3315-3328, 3330-3381, 3394-3453, 3577-3579, 3997-3999, 4066, 4083, 4099-4101,
  // 4109-4565,  4569-5999, and 6015-9999. So, we take the values greater than and equal to 6200
  // for new entries in catalog tables.
  private var _nextUnusedOid = 6200

  private def nextUnusedOid = {
    val nextOid = _nextUnusedOid
    _nextUnusedOid = _nextUnusedOid + 1
    nextOid
  }

  case class PgType(oid: Int, name: String, len: Int, elemOid: Int, input: String)

  // scalastyle:off
  val PgBoolType               = PgType(            16,       "bool",  1,                   0,      "boolin")
  val PgByteaType              = PgType(            17,      "bytea", -1,                   0,     "byteain")
  val PgCharType               = PgType(            18,       "char",  1,                   0,      "charin")
  val PgNameType               = PgType(            19,       "name", 64,      PgCharType.oid,      "namein")
  val PgInt8Type               = PgType(            20,       "int8",  8,                   0,      "int8in")
  val PgInt2Type               = PgType(            21,       "int2",  2,                   0,      "int2in")
  val PgInt4Type               = PgType(            23,       "int4",  4,                   0,      "int4in")
  val PgTidType                = PgType(            27,        "tid",  6,                   0,       "tidin")
  val PgFloat4Type             = PgType(           700,     "float4",  4,                   0,    "float4in")
  val PgFloat8Type             = PgType(           701,     "float8",  8,                   0,    "float8in")
  val PgBoolArrayType          = PgType(          1000,      "_bool", -1,      PgBoolType.oid,    "array_in")
  val PgInt2ArrayType          = PgType(          1005,      "_int2", -1,      PgInt2Type.oid,    "array_in")
  val PgInt4ArrayType          = PgType(          1007,      "_int4", -1,      PgInt4Type.oid,    "array_in")
  val PgInt8ArrayType          = PgType(          1016,      "_int8", -1,      PgInt8Type.oid,    "array_in")
  val PgFloat4ArrayType        = PgType(          1021,    "_float4", -1,    PgFloat4Type.oid,    "array_in")
  val PgFloat8ArrayType        = PgType(          1022,    "_float8", -1,    PgFloat8Type.oid,    "array_in")
  val PgVarCharType            = PgType(          1043,    "varchar", -1,                   0,   "varcharin")
  val PgVarCharArrayType       = PgType(          1015,   "_varchar", -1,   PgVarCharType.oid,    "array_in")
  val PgDateType               = PgType(          1082,       "date", -1,                   0,      "datein")
  val PgTimestampType          = PgType(          1114,  "timestamp",  8,                   0, "timestampin")
  val PgTimestampTypeArrayType = PgType(          1115, "_timestamp", -1, PgTimestampType.oid,    "array_in")
  val PgDateArrayType          = PgType(          1182,      "_date", -1,      PgDateType.oid,    "array_in")
  val PgNumericType            = PgType(          1700,    "numeric", -1,                   0,   "numericin")
  val PgNumericArrayType       = PgType(          1231,   "_numeric", -1,   PgNumericType.oid,    "array_in")
  // A `pg_type` catalog table has new three entries below for ByteType, MapType, and StructType
  val PgByteType               = PgType( nextUnusedOid,       "byte",  1,                   0,      "bytein")
  val PgMapType                = PgType( nextUnusedOid,        "map", -1,                   0,       "mapin")
  val PgStructType             = PgType( nextUnusedOid,     "struct", -1,                   0,    "structin")
  // scalastyle:on

  private val supportedPgTypes: Seq[PgType] = Seq(
    PgBoolType, PgByteaType, PgCharType, PgNameType, PgInt8Type, PgInt2Type, PgInt4Type, PgTidType,
    PgFloat4Type, PgFloat8Type, PgBoolArrayType, PgInt2ArrayType, PgInt4ArrayType,
    PgVarCharArrayType, PgInt8ArrayType, PgFloat4ArrayType, PgFloat8ArrayType, PgVarCharType,
    PgDateType, PgTimestampType, PgTimestampTypeArrayType, PgDateArrayType, PgNumericArrayType,
    PgNumericType, PgByteType, PgMapType, PgStructType)

  // We assume the namespace of all entities is `spark`
  private val defaultSparkNamespace = (nextUnusedOid, "spark")

  private val userRoleOid = nextUnusedOid

  def initSystemFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("ANY", (arg: Seq[String]) => arg.head)
    sqlContext.udf.register("current_schemas", (arg: Boolean) => Seq(defaultSparkNamespace._2))
    sqlContext.udf.register("array_upper", (ar: Seq[String], n: Int) => ar.size)
    sqlContext.udf.register("array_in", () => "array_in")
    sqlContext.udf.register(s"$catalogDbName.obj_description", (oid: Int, tableName: String) => "")
    sqlContext.udf.register(s"$catalogDbName.pg_get_expr", (adbin: String, adrelid: Int) => "")
    sqlContext.udf.register(s"$catalogDbName.pg_table_is_visible", (oid: Int) => true)
    sqlContext.udf.register(s"$catalogDbName.pg_get_userbyid", (userid: Int) => "")
    sqlContext.udf.register(s"$catalogDbName.format_type", (type_oid: Int, typemod: String) => "")
    sqlContext.udf.register(s"$catalogDbName.pg_encoding_to_char", (encoding: Int) => "")
    sqlContext.udf.register(s"$catalogDbName.array_to_string",
      (ar: Seq[String], delim: String) => if (ar != null) ar.mkString(delim) else "")
  }

  def initSystemCatalogTables(sqlContext: SQLContext): Unit = {
    val externalCatalog = sqlContext.sharedState.externalCatalog

    def safeCreateTable(catalogTable: String)(f: String => Seq[String]): Unit = {
      assert(catalogTables.contains(catalogTable))
      assert(!externalCatalog.tableExists(catalogDbName, catalogTable))
      f(s"$catalogDbName.$catalogTable").foreach { sqlText =>
        sqlContext.sql(sqlText.stripMargin)
      }
    }

    // TODO: Make this initialization transactional
    if (!externalCatalog.databaseExists(catalogDbName)) {
      try {
        sqlContext.sql(s"CREATE DATABASE $catalogDbName")

        safeCreateTable("pg_namespace") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   oid INT,
            |   nspname STRING
            | )
          """ ::
          s"""
            | INSERT INTO $cTableName
            |   VALUES(${defaultSparkNamespace._1}, '${defaultSparkNamespace._2}')
          """ ::
          Nil
        }

        // TODO: The PostgreSQL JDBC driver (`SQLSERVER_VERSION` >= 8.0) issues a query below and
        // it uses `default.pg_namespace instead of `pg_catalog.pg_namespace`.
        // So, we currently create `pg_namespace` in both `default` and `pg_catalog`.
        //
        // SELECT typinput='array_in'::regproc, typtype
        //   FROM pg_catalog.pg_type
        //   LEFT JOIN (
        //     select ns.oid as nspoid, ns.nspname, r.r
        //       from pg_namespace as ns
        //            ^^^^^^^^^^^^
        //       join (
        //         select s.r, (current_schemas(false))[s.r] as nspname
        //           from generate_series(1, array_upper(current_schemas(false), 1)) as s(r)
        //       ) as r using ( nspname )
        //   ) as sp
        //   ON sp.nspoid = typnamespace
        //   WHERE typname = 'byte'
        //   ORDER BY sp.r, pg_type.oid DESC
        //   LIMIT 1;
        //
        {
          "DROP TABLE IF EXISTS pg_namespace" ::
          s"""
            | CREATE TABLE pg_namespace(
            |   oid INT,
            |   nspname STRING
            | )
          """ ::
          s"""
            | INSERT INTO pg_namespace
            |   VALUES(${defaultSparkNamespace._1}, '${defaultSparkNamespace._2}')
          """ ::
          Nil
        }.foreach { sqlText =>
          sqlContext.sql(sqlText.stripMargin)
        }

        safeCreateTable("pg_roles") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   oid INT,
            |   rolname STRING
            | )
          """ ::
          s"""
            | INSERT INTO $cTableName VALUES($userRoleOid, 'spark-user')
          """ ::
          Nil
        }

        safeCreateTable("pg_user") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   usename STRING,
            |   usesysid INT
            | )
          """ ::
          s"""
            | INSERT INTO $cTableName VALUES('spark-user', $userRoleOid)
          """ ::
          Nil
        }

        safeCreateTable("pg_type") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   oid INT,
            |   typname STRING,
            |   typtype STRING,
            |   typlen INT,
            |   typnotnull BOOLEAN,
            |   typelem INT,
            |   typdelim STRING,
            |   typinput STRING,
            |   typrelid INT,
            |   typbasetype INT,
            |   typnamespace INT
            | )
          """ +:
          supportedPgTypes.map { tpe =>
            // `b` in `typtype` means a primitive type and all the entries in `supportedPgTypes`
            // are primitive types.
            s"""
              | INSERT INTO $cTableName VALUES(
              |   %d, '%s', 'b', %d, false, %d, ',', '%s', 0, 0, %d
              | )
            """.format(
              tpe.oid,
              tpe.name,
              tpe.len,
              tpe.elemOid,
              tpe.input,
              defaultSparkNamespace._1
            )
          }
        }

        /**
         * Six empty catalog tables are defined below to prevent the PostgreSQL JDBC drivers from
         * throwing meaningless exceptions.
         */
        safeCreateTable("pg_index") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   oid INT,
            |   indrelid INT,
            |   indexrelid INT,
            |   indisprimary BOOLEAN
            | )
          """ ::
          Nil
        }

        safeCreateTable("pg_proc") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   oid INT,
            |   proname STRING,
            |   prorettype INT,
            |   proargtypes ARRAY<INT>,
            |   pronamespace INT
            | )
          """ ::
          Nil
        }

        safeCreateTable("pg_description") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   objoid INT,
            |   classoid INT,
            |   objsubid INT,
            |   description STRING
            | )
          """ ::
          Nil
        }

        safeCreateTable("pg_depend") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   objid INT,
            |   classid INT,
            |   refobjid INT,
            |   refclassid INT
            | )
          """ ::
          Nil
        }

        safeCreateTable("pg_constraint") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   oid INT,
            |   confupdtype STRING,
            |   confdeltype STRING,
            |   conname STRING,
            |   condeferrable BOOLEAN,
            |   condeferred BOOLEAN,
            |   conkey ARRAY<INT>,
            |   confkey ARRAY<INT>,
            |   confrelid INT,
            |   conrelid INT,
            |   contype STRING
            | )
          """ ::
          Nil
        }

        safeCreateTable("pg_attrdef") { cTableName =>
          s"""
            | CREATE TABLE $cTableName(
            |   adrelid INT,
            |   adnum SHORT,
            |   adbin STRING
            | )
          """ ::
          Nil
        }
      } catch {
        case e: Throwable =>
          catalogTables.foreach { tableName =>
            sqlContext.sql(s"DROP TABLE IF EXISTS $catalogDbName.$tableName")
          }
          sqlContext.sql(s"DROP DATABASE IF EXISTS $catalogDbName")
          throw e
      } finally {
        // require(catalogTables.forall(externalCatalog.tableExists(catalogDbName, _)))
      }
    }
  }

  def initSessionCatalogTables(sqlContext: SQLContext, dbName: String): Unit = {
    refreshDatabases(dbName, sqlContext)
    refreshTables(dbName, sqlContext)
  }

  def refreshDatabases(dbName: String, sqlContext: SQLContext): Unit = {
    val externalCatalog = sqlContext.sharedState.externalCatalog

    def safeCreateTable(catalogTable: String)(f: String => String): Unit = {
      assert(catalogTables.contains(catalogTable))
      sqlContext.sql(s"DROP TABLE IF EXISTS $catalogDbName.$catalogTable")
      sqlContext.sql(f(s"$catalogDbName.$catalogTable").stripMargin)
    }

    safeCreateTable("pg_database") { cTableName =>
      s"""
        | CREATE TABLE $cTableName(
        |   datname STRING,
        |   datdba INT,
        |   encoding INT,
        |   datacl Array<STRING>
        | )
      """
    }

    externalCatalog.listDatabases.foreach { dbName =>
      doRegisterDatabase(dbName, sqlContext)
    }
  }

  def refreshTables(dbName: String, sqlContext: SQLContext): Unit = {
    val externalCatalog = sqlContext.sharedState.externalCatalog

    def safeCreateTable(catalogTable: String)(f: String => String): Unit = {
      assert(catalogTables.contains(catalogTable))
      sqlContext.sql(s"DROP TABLE IF EXISTS $catalogDbName.$catalogTable")
      sqlContext.sql(f(s"$catalogDbName.$catalogTable").stripMargin)
    }

    safeCreateTable("pg_class") { cTableName =>
      s"""
        | CREATE TABLE $cTableName(
        |   oid INT,
        |   relname STRING,
        |   relkind STRING,
        |   relnamespace INT,
        |   relowner INT,
        |   relacl ARRAY<STRING>,
        |   relchecks SHORT,
        |   relhasindex BOOLEAN,
        |   relhasrules BOOLEAN,
        |   reltriggers SHORT,
        |   relhasoids BOOLEAN
        | )
      """
    }

    safeCreateTable("pg_attribute") { cTableName =>
      s"""
        | CREATE TABLE $cTableName(
        |   oid INT,
        |   attrelid INT,
        |   attname STRING,
        |   atttypid INT,
        |   attnotnull BOOLEAN,
        |   atttypmod INT,
        |   attlen INT,
        |   attnum INT,
        |   attisdropped BOOLEAN
        | )
      """
    }

    externalCatalog.listTables(dbName).foreach { tableName =>
      doRegisterTable(
        dbName,
        tableName,
        externalCatalog.getTable(dbName, tableName).schema,
        sqlContext
      )
    }
  }

  def registerDatabase(dbName: String, sqlContext: SQLContext): Unit = {
    require(dbName != catalogDbName, s"$dbName is reserved for system use")
    doRegisterDatabase(dbName, sqlContext)
  }

  private def doRegisterDatabase(dbName: String, sqlContext: SQLContext): Unit = {
    logInfo(s"Registering a database `$dbName` in a system catalog `pg_database`")
    sqlContext.sql(s"INSERT INTO $catalogDbName.pg_database VALUES('$dbName', 0, 0, null)")
  }

  private def isReservedTableName(dbName: String, tableName: String): Boolean = {
    catalogTables.map { reserved =>
      if (dbName != catalogDbName) {
        s"$catalogDbName.$reserved"
      } else {
        reserved
      }
    }.contains(tableName)
  }

  def registerTable(dbName: String, tableName: String, schema: StructType, sqlContext: SQLContext)
    : Unit = {
    require(!isReservedTableName(tableName, dbName), s"$tableName is reserved for system use")
    doRegisterTable(dbName, tableName, schema, sqlContext)
  }

  private def doRegisterTable(
      dbName: String, tableName: String, schema: StructType, sqlContext: SQLContext)
    : Unit = {
    logInfo(s"Registering a table `$tableName(${schema.sql}})` in a system catalog `pg_class`")
    val tableOid = nextUnusedOid
    val sqlTexts =
      s"""
        | INSERT INTO $catalogDbName.pg_class VALUES(
        |   %d, '%s', '%s', %d, %d, %s, 0, false, false, 0, false
        | )
      """.format(
        tableOid,
        tableName,
        "r",
        defaultSparkNamespace._1,
        userRoleOid,
        "null"
      ) +:
      schema.zipWithIndex.map { case (field, index) =>
        val pgType = getPgType(field.dataType)
          s"""
          | INSERT INTO $catalogDbName.pg_attribute VALUES(
          |   %d, %d, '%s', %d, %b, %d, %d, %d, false
          | )
        """.format(
          nextUnusedOid,
          tableOid,
          field.name,
          pgType.oid,
          !field.nullable,
          0,
          pgType.len,
          1 + index
        )
      }

    sqlTexts.foreach { sqlText =>
      sqlContext.sql(sqlText.stripMargin)
    }
  }

  def getPgType(catalystType: DataType): PgType = catalystType match {
    // scalastyle:off
    case BooleanType             => PgBoolType
    case ByteType                => PgByteType
    case BinaryType              => PgByteaType
    case ShortType               => PgInt2Type
    case IntegerType             => PgInt4Type
    case LongType                => PgInt8Type
    case FloatType               => PgFloat4Type
    case DoubleType              => PgFloat8Type
    case StringType              => PgVarCharType
    case DateType                => PgDateType
    case TimestampType           => PgTimestampType
    case DecimalType.Fixed(_, _) => PgNumericType
    case ArrayType(elemType, _)  => getPgArrayType(elemType)
    case MapType(_, _, _)        => PgMapType
    case StructType(_)           => PgStructType
    case NullType                => PgVarCharType
    case _ => throw new SQLException("Unsupported type " + catalystType)
    // scalastyle:on
  }

  private def getPgArrayType(elemType: DataType): PgType = elemType match {
    // scalastyle:off
    case BooleanType             => PgBoolArrayType
    case ByteType                => PgByteaType
    case ShortType               => PgInt2ArrayType
    case IntegerType             => PgInt4ArrayType
    case LongType                => PgInt8ArrayType
    case FloatType               => PgFloat4ArrayType
    case DoubleType              => PgFloat8ArrayType
    case StringType              => PgVarCharArrayType
    case DateType                => PgDateArrayType
    case TimestampType           => PgTimestampTypeArrayType
    case DecimalType.Fixed(_, _) => PgNumericArrayType
    case _ => throw new SQLException("Unsupported array type " + elemType)
    // scalastyle:on
  }
}
