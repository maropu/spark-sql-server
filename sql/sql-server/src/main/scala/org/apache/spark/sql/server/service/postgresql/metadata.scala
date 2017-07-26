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
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types._


/** This is the PostgreSQL system information such as catalog tables and functions. */
object Metadata extends Logging {

  // Since v7.3, all the catalog tables have been moved in a `pg_catalog` database
  private[sql] val catalogDbName = "pg_catalog"

  case class PgSystemTable(oid: Int, table: TableIdentifier)
  case class PgType(oid: Int, name: String, len: Int, elemOid: Int, input: String)
  case class PgSystemFunction(oid: Int, func: FunctionIdentifier, doRegister: SQLContext => Unit)

  // `src/include/catalog/unused_oids` in a PostgreSQL source repository prints unused oids; 2-9,
  // 3300, 3308-3309, 3315-3328, 3330-3381, 3394-3453, 3577-3579, 3997-3999, 4066, 4083, 4099-4101,
  // 4109-4565,  4569-5999, and 6015-9999. So, we take the values greater than and equal to 6200
  // for new entries in catalog tables.
  private val baseUnusedOid = 6200

  // Since multiple threads possibly access this variable, we use atomic one
  private val _nextUnusedOid = new AtomicInteger(baseUnusedOid)

  private def nextUnusedOid = _nextUnusedOid.getAndIncrement()

  // Catalog tables and they are immutable
  private val _catalogTables1 = Seq(
    // scalastyle:off
    PgSystemTable(          2615, TableIdentifier(  "pg_namespace", Some(catalogDbName))),
    PgSystemTable( nextUnusedOid, TableIdentifier(  "pg_namespace",               None)),
    PgSystemTable(          1247, TableIdentifier(       "pg_type", Some(catalogDbName))),
    PgSystemTable(         11631, TableIdentifier(      "pg_roles", Some(catalogDbName))),
    PgSystemTable(         11642, TableIdentifier(       "pg_user", Some(catalogDbName))),
    PgSystemTable(          2610, TableIdentifier(      "pg_index", Some(catalogDbName))),
    PgSystemTable(          2609, TableIdentifier("pg_description", Some(catalogDbName))),
    PgSystemTable(          2608, TableIdentifier(     "pg_depend", Some(catalogDbName))),
    PgSystemTable(          2606, TableIdentifier( "pg_constraint", Some(catalogDbName))),
    PgSystemTable(          2604, TableIdentifier(    "pg_attrdef", Some(catalogDbName))),
    PgSystemTable(          2611, TableIdentifier(   "pg_inherits", Some(catalogDbName)))
    // scalastyle:on
  )

  // Catalog tables that are updated every databases/tables created
  private val _catalogTables2 = Seq(
    // scalastyle:off
    PgSystemTable( 1262, TableIdentifier( "pg_database", Some(catalogDbName))),
    PgSystemTable( 1259, TableIdentifier(    "pg_class", Some(catalogDbName))),
    PgSystemTable( 1249, TableIdentifier("pg_attribute", Some(catalogDbName))),
    PgSystemTable( 1255, TableIdentifier(     "pg_proc", Some(catalogDbName)))
    // scalastyle:on
  )

  private val catalogTables = _catalogTables1 ++ _catalogTables2

  private val pgCatalogOidMap: Map[Int, PgSystemTable] = catalogTables.map(t => t.oid -> t).toMap


  // scalastyle:off
  val PgUnspecifiedType        = PgType(             0, "unspecified",  0,                   0,            "")
  val PgBoolType               = PgType(            16,        "bool",  1,                   0,      "boolin")
  val PgByteaType              = PgType(            17,       "bytea", -1,                   0,     "byteain")
  val PgCharType               = PgType(            18,        "char",  1,                   0,      "charin")
  val PgNameType               = PgType(            19,        "name", 64,      PgCharType.oid,      "namein")
  val PgInt8Type               = PgType(            20,        "int8",  8,                   0,      "int8in")
  val PgInt2Type               = PgType(            21,        "int2",  2,                   0,      "int2in")
  val PgInt4Type               = PgType(            23,        "int4",  4,                   0,      "int4in")
  val PgTidType                = PgType(            27,         "tid",  6,                   0,       "tidin")
  val PgFloat4Type             = PgType(           700,      "float4",  4,                   0,    "float4in")
  val PgFloat8Type             = PgType(           701,      "float8",  8,                   0,    "float8in")
  val PgBoolArrayType          = PgType(          1000,       "_bool", -1,      PgBoolType.oid,    "array_in")
  val PgInt2ArrayType          = PgType(          1005,       "_int2", -1,      PgInt2Type.oid,    "array_in")
  val PgInt4ArrayType          = PgType(          1007,       "_int4", -1,      PgInt4Type.oid,    "array_in")
  val PgInt8ArrayType          = PgType(          1016,       "_int8", -1,      PgInt8Type.oid,    "array_in")
  val PgFloat4ArrayType        = PgType(          1021,     "_float4", -1,    PgFloat4Type.oid,    "array_in")
  val PgFloat8ArrayType        = PgType(          1022,     "_float8", -1,    PgFloat8Type.oid,    "array_in")
  val PgVarCharType            = PgType(          1043,     "varchar", -1,                   0,   "varcharin")
  val PgVarCharArrayType       = PgType(          1015,    "_varchar", -1,   PgVarCharType.oid,    "array_in")
  val PgDateType               = PgType(          1082,        "date", -1,                   0,      "datein")
  val PgTimestampType          = PgType(          1114,   "timestamp",  8,                   0, "timestampin")
  val PgTimestampTypeArrayType = PgType(          1115,  "_timestamp", -1, PgTimestampType.oid,    "array_in")
  val PgDateArrayType          = PgType(          1182,       "_date", -1,      PgDateType.oid,    "array_in")
  val PgNumericType            = PgType(          1700,     "numeric", -1,                   0,   "numericin")
  val PgNumericArrayType       = PgType(          1231,    "_numeric", -1,   PgNumericType.oid,    "array_in")
  // A `pg_type` catalog table has new three entries below for ByteType, MapType, and StructType
  val PgByteType               = PgType( nextUnusedOid,        "byte",  1,                   0,      "bytein")
  val PgMapType                = PgType( nextUnusedOid,         "map", -1,                   0,       "mapin")
  val PgStructType             = PgType( nextUnusedOid,      "struct", -1,                   0,    "structin")
  // scalastyle:on

  private[sql] val pgTypes: Seq[PgType] = Seq(
    PgBoolType, PgByteaType, PgCharType, PgNameType, PgInt8Type, PgInt2Type, PgInt4Type, PgTidType,
    PgFloat4Type, PgFloat8Type, PgBoolArrayType, PgInt2ArrayType, PgInt4ArrayType,
    PgVarCharArrayType, PgInt8ArrayType, PgFloat4ArrayType, PgFloat8ArrayType, PgVarCharType,
    PgDateType, PgTimestampType, PgTimestampTypeArrayType, PgDateArrayType, PgNumericArrayType,
    PgNumericType, PgByteType, PgMapType, PgStructType
  )

  private val pgTypeOidMap: Map[Int, PgType] = pgTypes.map(tpe => tpe.oid -> tpe).toMap

  private def getPgTypeNameFromOid(typeoid: Int): String = {
    pgTypeOidMap.get(typeoid).map(_.name).getOrElse("unknown")
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


  // We assume the namespace of all entities is `spark`
  private val defaultSparkNamespace = (nextUnusedOid, "spark")
  private val userRoleOid = nextUnusedOid

  private val pgSystemFunctions = Seq(
    // scalastyle:off
    PgSystemFunction(          384, FunctionIdentifier(       "array_to_string", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.array_to_string", (ar: Seq[String], delim: String) => if (ar != null) ar.mkString(delim) else "") }),
    PgSystemFunction(          750, FunctionIdentifier(              "array_in",                None), { c => c.udf.register("array_in", () => "array_in") }),
    PgSystemFunction(         1081, FunctionIdentifier(           "format_type", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.format_type", (type_oid: Int, typemod: String) => pgTypeOidMap.get(type_oid).map(_.name).getOrElse("unknown")) }),
    PgSystemFunction(         1215, FunctionIdentifier(       "obj_description", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.obj_description", (oid: Int, tableName: String) => "") }),
    PgSystemFunction(         1402, FunctionIdentifier(       "current_schemas",                None), { c => c.udf.register("current_schemas", (arg: Boolean) => Seq(defaultSparkNamespace._2)) }),
    PgSystemFunction(         1597, FunctionIdentifier(   "pg_encoding_to_char", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.pg_encoding_to_char", (encoding: Int) => "") }),
    PgSystemFunction(         1642, FunctionIdentifier(       "pg_get_userbyid", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.pg_get_userbyid", (userid: Int) => "") }),
    PgSystemFunction(         1716, FunctionIdentifier(           "pg_get_expr", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.pg_get_expr", (adbin: String, adrelid: Int) => "") }),
    PgSystemFunction(         2079, FunctionIdentifier(   "pg_table_is_visible", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.pg_table_is_visible", (tableoid: Int) => !pgCatalogOidMap.get(tableoid).isDefined) }),
    PgSystemFunction(         2081, FunctionIdentifier("pg_function_is_visible", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.pg_function_is_visible", (functionoid: Int) => !pgSystemFunctionOidMap.get(functionoid).isDefined) }),
    PgSystemFunction(         2092, FunctionIdentifier(           "array_upper",                None), { c => c.udf.register("array_upper", (ar: Seq[String], n: Int) => ar.size) }),
    PgSystemFunction(         2420, FunctionIdentifier(        "oidvectortypes", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.oidvectortypes", (typeoids: Seq[Int]) =>  if (typeoids != null) typeoids.map(getPgTypeNameFromOid).mkString(", ") else "") }),

    // Entries below is not a kind of functions though, we need to process some interactions
    // between clients and JDBC drivers.
    PgSystemFunction(nextUnusedOid, FunctionIdentifier(                   "ANY",                None), { c => c.udf.register("ANY", (arg: Seq[String]) => arg.head) }),
    PgSystemFunction(nextUnusedOid, FunctionIdentifier(               "regtype", Some(catalogDbName)), { c => c.udf.register(s"$catalogDbName.regtype", (typeoid: Int) => getPgTypeNameFromOid(typeoid)) })
    // scalastyle:on
  )

  private val pgSystemFunctionOidMap: Map[Int, PgSystemFunction] =
    pgSystemFunctions.map(f => f.oid -> f).toMap
  private val pgSystemFunctionNameMap: Map[String, PgSystemFunction] =
    pgSystemFunctions.map(f => f.func.unquotedString.toLowerCase -> f).toMap

  private def safeCreateCatalogTable(name: String, sqlContext: SQLContext)(f: String => Seq[String])
    : Unit = {
    assert(catalogTables.exists { case PgSystemTable(oid, table) => table.identifier == name })
    val sqlTexts = s"DROP TABLE IF EXISTS $catalogDbName.$name" +:
      f(s"$catalogDbName.$name")
    sqlTexts.foreach { sqlText =>
      sqlContext.sql(sqlText.stripMargin)
    }
  }

  def initSystemFunctions(sqlContext: SQLContext): Unit = {
    pgSystemFunctions.foreach { case PgSystemFunction(_, _, doRegister) => doRegister(sqlContext) }
  }

  def initSystemCatalogTables(sqlContext: SQLContext): Unit = {
    // TODO: Make this initialization transactional
    if (!sqlContext.sessionState.catalog.databaseExists(catalogDbName)) {
      try {
        sqlContext.sql(s"CREATE DATABASE $catalogDbName")

        safeCreateCatalogTable("pg_namespace", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  oid INT,
            |  nspname STRING
            |)
          """ ::
          s"""
            |INSERT INTO $cTableName
            |  VALUES(${defaultSparkNamespace._1}, '${defaultSparkNamespace._2}')
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
            |CREATE TABLE pg_namespace(
            |  oid INT,
            |  nspname STRING
            |)
          """ ::
          s"""
            |INSERT INTO pg_namespace
            |  VALUES(${defaultSparkNamespace._1}, '${defaultSparkNamespace._2}')
          """ ::
          Nil
        }.foreach { sqlText =>
          sqlContext.sql(sqlText.stripMargin)
        }

        safeCreateCatalogTable("pg_roles", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  oid INT,
            |  rolname STRING
            |)
          """ ::
          s"""
            |INSERT INTO $cTableName VALUES($userRoleOid, 'spark-user')
          """ ::
          Nil
        }

        safeCreateCatalogTable("pg_user", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  usename STRING,
            |  usesysid INT
            |)
          """ ::
          s"""
            |INSERT INTO $cTableName VALUES('spark-user', $userRoleOid)
          """ ::
          Nil
        }

        safeCreateCatalogTable("pg_type", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  oid INT,
            |  typname STRING,
            |  typtype STRING,
            |  typlen INT,
            |  typnotnull BOOLEAN,
            |  typelem INT,
            |  typdelim STRING,
            |  typinput STRING,
            |  typrelid INT,
            |  typbasetype INT,
            |  typnamespace INT
            |)
          """ +: pgTypes.map { case PgType(oid, name, len, elemOid, input) =>
            // `b` in `typtype` means a primitive type and all the entries in `supportedPgTypes`
            // are primitive types.
            s"""
              |INSERT INTO $cTableName VALUES(
              |  $oid, '$name', 'b', $len, false, $elemOid, ',', '$input', 0, 0,
              |  ${defaultSparkNamespace._1}
              |)
            """
          }
        }

        /**
         * Six empty catalog tables are defined below to prevent the PostgreSQL JDBC drivers from
         * throwing meaningless exceptions.
         */
        safeCreateCatalogTable("pg_index", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  oid INT,
            |  indrelid INT,
            |  indexrelid INT,
            |  indisprimary BOOLEAN
            |)
          """ ::
          Nil
        }

        safeCreateCatalogTable("pg_description", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  objoid INT,
            |  classoid INT,
            |  objsubid INT,
            |  description STRING
            |)
          """ ::
          Nil
        }

        safeCreateCatalogTable("pg_depend", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  objid INT,
            |  classid INT,
            |  refobjid INT,
            |  refclassid INT
            |)
          """ ::
          Nil
        }

        safeCreateCatalogTable("pg_constraint", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  oid INT,
            |  confupdtype STRING,
            |  confdeltype STRING,
            |  conname STRING,
            |  condeferrable BOOLEAN,
            |  condeferred BOOLEAN,
            |  conkey ARRAY<INT>,
            |  confkey ARRAY<INT>,
            |  confrelid INT,
            |  conrelid INT,
            |  contype STRING
            |)
          """ ::
          Nil
        }

        safeCreateCatalogTable("pg_attrdef", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  adrelid INT,
            |  adnum SHORT,
            |  adbin STRING
            |)
          """ ::
          Nil
        }

        safeCreateCatalogTable("pg_inherits", sqlContext) { cTableName =>
          s"""
            |CREATE TABLE $cTableName(
            |  inhrelid INT,
            |  inhparent INT,
            |  inhseqno INT
            |)
          """ ::
          Nil
        }
      } catch {
        case e: Throwable =>
          val sqlTexts = catalogTables.map { case PgSystemTable(_, TableIdentifier(name, _)) =>
            s"DROP TABLE IF EXISTS $catalogDbName.$name"
          } :+ s"DROP DATABASE IF EXISTS $catalogDbName"
          sqlTexts.foreach { sqlText =>
            sqlContext.sql(sqlText)
          }
          throw e
      } finally {
        require(_catalogTables1.forall { case PgSystemTable(_, table) =>
          sqlContext.sessionState.catalog.tableExists(table)
        })
      }
    }
  }

  def initSessionCatalogTables(sqlContext: SQLContext, dbName: String): Unit = {
    refreshDatabases(dbName, sqlContext)
    refreshTables(dbName, sqlContext)
    refreshFunctions(dbName, sqlContext)
  }

  def refreshDatabases(dbName: String, sqlContext: SQLContext): Unit = {
    safeCreateCatalogTable("pg_database", sqlContext) { cTableName =>
      s"""
        |CREATE TABLE $cTableName(
        |  datname STRING,
        |  datdba INT,
        |  encoding INT,
        |  datacl Array<STRING>
        |)
      """ ::
      Nil
    }

    sqlContext.sessionState.catalog.listDatabases.foreach { dbName =>
      doRegisterDatabase(dbName, sqlContext)
    }
  }

  def refreshTables(dbName: String, sqlContext: SQLContext): Unit = {
    safeCreateCatalogTable("pg_class", sqlContext) { cTableName =>
      s"""
        |CREATE TABLE $cTableName(
        |  oid INT,
        |  reltablespace INT,
        |  relname STRING,
        |  relkind STRING,
        |  relnamespace INT,
        |  relowner INT,
        |  relacl ARRAY<STRING>,
        |  relchecks SHORT,
        |  relhasindex BOOLEAN,
        |  relhasrules BOOLEAN,
        |  reltriggers SHORT,
        |  relhasoids BOOLEAN
        |)
      """ ::
      Nil
    }

    safeCreateCatalogTable("pg_attribute", sqlContext) { cTableName =>
      s"""
        |CREATE TABLE $cTableName(
        |  oid INT,
        |  attrelid INT,
        |  attname STRING,
        |  atttypid INT,
        |  attnotnull BOOLEAN,
        |  atthasdef BOOLEAN,
        |  atttypmod INT,
        |  attlen INT,
        |  attnum INT,
        |  attisdropped BOOLEAN
        |)
      """ ::
      Nil
    }

    val catalog = sqlContext.sessionState.catalog
    catalog.listTables(dbName).foreach { case table: TableIdentifier =>

      doRegisterTable(
        dbName,
        table.identifier,
        catalog.getTableMetadata(table).schema,
        catalog.getTableMetadata(table).tableType,
        sqlContext
      )
    }
  }

  def refreshFunctions(dbName: String, sqlContext: SQLContext): Unit = {
    safeCreateCatalogTable("pg_proc", sqlContext) { cTableName =>
      s"""
        |CREATE TABLE $cTableName(
        |  oid INT,
        |  proname STRING,
        |  prorettype INT,
        |  proargtypes ARRAY<INT>,
        |  pronamespace INT,
        |  proisagg BOOLEAN,
        |  proretset BOOLEAN
        |)
      """ ::
      Nil
    }

    sqlContext.sessionState.catalog.listFunctions(dbName, "*").foreach {
      case (func, "USER") =>
        // TODO: We should put system functions in a database `pg_catalog`
        val sysFuncOption = pgSystemFunctionNameMap.get(func.unquotedString.toLowerCase)
        if (sysFuncOption.isDefined) {
          sysFuncOption.foreach { case PgSystemFunction(oid, func, _) =>
            doRegisterFunction(func, oid, sqlContext)
          }
        } else {
          doRegisterFunction(func, nextUnusedOid, sqlContext)
        }
      case _ =>
        // If `scope` is "SYSTEM", ignore it
    }
  }

  // TODO: We should refresh catalog tables for databases/tables every updates
  // by using per-session temporary tables.
  def registerDatabase(dbName: String, sqlContext: SQLContext): Unit = {
    require(dbName != catalogDbName, s"$dbName is reserved for system use")
    doRegisterDatabase(dbName, sqlContext)
  }

  private def doRegisterDatabase(dbName: String, sqlContext: SQLContext): Unit = {
    logInfo(s"Registering a database `$dbName` in a system catalog `pg_database`")
    sqlContext.sql(s"INSERT INTO $catalogDbName.pg_database VALUES('$dbName', 0, 0, null)")
  }

  private def isReservedName(dbName: String, identifier: String): Boolean = {
    catalogTables.map { case PgSystemTable(_, reserved) =>
      if (dbName != catalogDbName) {
        s"$catalogDbName.$reserved"
      } else {
        reserved
      }
    }.contains(identifier)
  }

  def registerTable(dbName: String, tableName: String, schema: StructType,
    tableType: CatalogTableType, sqlContext: SQLContext)
    : Unit = {
    require(!isReservedName(tableName, dbName), s"$tableName is reserved for system use")
    doRegisterTable(dbName, tableName, schema, tableType, sqlContext)
  }

  private def doRegisterTable(
      dbName: String, tableName: String, schema: StructType, tableType: CatalogTableType,
      sqlContext: SQLContext)
    : Unit = {
    logInfo(s"Registering a table `$dbName.$tableName(${schema.sql}})` " +
      "in a system catalog `pg_class`")
    val tableOid = nextUnusedOid
    val tableTypeId = tableType match {
      case CatalogTableType.MANAGED => "r"
      case CatalogTableType.VIEW => "v"
      case CatalogTableType.EXTERNAL => "f"
    }
    val sqlTexts =
      s"""
        |INSERT INTO $catalogDbName.pg_class VALUES(
        |  $tableOid, 0, '$tableName', '$tableTypeId', ${defaultSparkNamespace._1}, $userRoleOid,
        |  null, 0, false, false, 0, false
        |)
      """ +: schema.zipWithIndex.map { case (field, index) =>
        val pgType = getPgType(field.dataType)
          s"""
            |INSERT INTO $catalogDbName.pg_attribute VALUES(
            |  $nextUnusedOid, $tableOid, '${field.name}', ${pgType.oid}, ${!field.nullable}, true,
            |  0, ${pgType.len}, ${1 + index}, false
            |)
          """
      }

    sqlTexts.foreach { sqlText =>
      sqlContext.sql(sqlText.stripMargin)
    }
  }

  def registerFunction(dbName: String, funcName: String, sqlContext: SQLContext): Unit = {
    require(!isReservedName(funcName, dbName), s"$funcName is reserved for system use")
    doRegisterFunction(FunctionIdentifier(funcName, Option(dbName)), nextUnusedOid, sqlContext)
  }

  private def doRegisterFunction(func: FunctionIdentifier, oid: Int, sqlContext: SQLContext)
    : Unit = {
    logInfo(s"Registering a function `$func` in a system catalog `pg_proc`")
    val sqlText = s"INSERT INTO $catalogDbName.pg_proc VALUES(%d, '%s', %d, null, %d, false, false)"
      .format(oid, func.identifier, 0, defaultSparkNamespace._1)
    sqlContext.sql(sqlText.stripMargin)
  }
}
