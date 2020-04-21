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
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.server.SQLServerEnv
import org.apache.spark.sql.server.service.CompositeService
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils._

private[service] class PgCatalogInitializer extends CompositeService {

  override def doStart(): Unit = {
    // We must load system catalogs for the PostgreSQL v3 protocol before
    // `PgProtocolService.start`.
    PgCatalogInitializer(SQLServerEnv.sqlContext)
  }
}

private[service] object PgCatalogInitializer {

  def apply(sqlContext: SQLContext): Unit = {
    PgMetadata.initSystemCatalogTables(sqlContext)
  }
}

private[service] object PgSessionInitializer {

  def apply(dbName: String, sqlContext: SQLContext): Unit = {
    PgMetadata.initSystemFunctions(sqlContext)
  }
}

/**
 * This is the PostgreSQL system information such as catalog tables and functions.
  *
  * TODO: Adds `private[postgresql]`
 */
private[server] object PgMetadata extends Logging {

  // Since v7.3, all the catalog tables have been moved in a `pg_catalog` database
  private[sql] val catalogDbName = "pg_catalog"

  case class PgSystemTable(oid: Int, table: TableIdentifier, schema: String)
  case class PgType(oid: Int, name: String, len: Int, elemOid: Int, input: String)
  case class PgSystemFunction(oid: Int, name: FunctionIdentifier, udf: Unit => UserDefinedFunction)

  // `src/include/catalog/unused_oids` in a PostgreSQL source repository prints unused oids; 2-9,
  // 3300, 3308-3309, 3315-3328, 3330-3381, 3394-3453, 3577-3579, 3997-3999, 4066, 4083, 4099-4101,
  // 4109-4565,  4569-5999, and 6015-9999. So, we take the values greater than and equal to 6200
  // for new entries in catalog tables.
  private val baseUnusedOid = 6200

  // Since multiple threads possibly access this variable, we use atomic one
  private val _nextUnusedOid = new AtomicInteger(baseUnusedOid)

  private def nextUnusedOid = _nextUnusedOid.getAndIncrement

  // Catalog tables and they are immutable (these table names should be reserved for a parse)
  // TODO: Loads the catalog tables that PostgreSQL dumps via
  private val staticCatalogTables = Seq(
    // scalastyle:off
    PgSystemTable(  1247, TableIdentifier(        "pg_type", Some(catalogDbName)), "oid INT, typname STRING, typtype STRING, typlen INT, typnotnull BOOLEAN, typelem INT, typdelim STRING, typinput STRING, typrelid INT, typbasetype INT, typcollation INT, typnamespace INT"),
    PgSystemTable(  2604, TableIdentifier(     "pg_attrdef", Some(catalogDbName)), "adrelid INT, adnum SHORT, adbin STRING"),
    PgSystemTable(  2606, TableIdentifier(  "pg_constraint", Some(catalogDbName)), "oid INT, confupdtype STRING, confdeltype STRING, conname STRING, condeferrable BOOLEAN, condeferred BOOLEAN, conkey ARRAY<INT>, confkey ARRAY<INT>, confrelid INT, conrelid INT, contype STRING"),
    PgSystemTable(  2608, TableIdentifier(      "pg_depend", Some(catalogDbName)), "objid INT, classid INT, refobjid INT, refclassid INT"),
    PgSystemTable(  2609, TableIdentifier( "pg_description", Some(catalogDbName)), "objoid INT, classoid INT, objsubid INT, description STRING"),
    PgSystemTable(  2610, TableIdentifier(       "pg_index", Some(catalogDbName)), "oid INT, indrelid INT, indexrelid INT, indisprimary BOOLEAN"),
    PgSystemTable(  2611, TableIdentifier(    "pg_inherits", Some(catalogDbName)), "inhrelid INT, inhparent INT, inhseqno INT"),
    PgSystemTable(  2615, TableIdentifier(   "pg_namespace", Some(catalogDbName)), "oid INT, nspname STRING"),
    PgSystemTable( -2615, TableIdentifier(   "pg_namespace",                None), "oid INT, nspname STRING"),
    PgSystemTable(  3256, TableIdentifier(      "pg_policy", Some(catalogDbName)), "polname STRING, polrelid INT, polcmd STRING, polroles STRING, polqual STRING, polwithcheck STRING"),
    PgSystemTable(  3456, TableIdentifier(   "pg_collation", Some(catalogDbName)), "oid INT, collname STRING"),
    PgSystemTable( 11631, TableIdentifier(       "pg_roles", Some(catalogDbName)), "oid INT, rolname STRING"),
    PgSystemTable( 11642, TableIdentifier(        "pg_user", Some(catalogDbName)), "usename STRING, usesysid INT")
    // scalastyle:on
  )

  // TODO: Catalog tables that should be updated every databases/tables created
  private val runtimeCatalogTables = Seq(
    // scalastyle:off
    PgSystemTable( 1249, TableIdentifier( "pg_attribute", Some(catalogDbName)), "oid INT, attrelid INT, attname STRING, atttypid INT, attnotnull BOOLEAN, atthasdef BOOLEAN, atttypmod INT, attlen INT, attnum INT, attidentity STRING, attisdropped BOOLEAN, attcollation INT"),
    PgSystemTable( 1255, TableIdentifier(      "pg_proc", Some(catalogDbName)), "oid INT, proname STRING, prorettype INT, proargtypes ARRAY<INT>, pronamespace INT, proisagg BOOLEAN, proiswindow BOOLEAN, proretset BOOLEAN"),
    PgSystemTable( 1259, TableIdentifier(     "pg_class", Some(catalogDbName)), "oid INT, reltablespace INT, relname STRING, reloftype INT, relpersistence STRING, relkind STRING, relnamespace INT, relowner INT, relacl ARRAY<STRING>, relchecks SHORT, reltoastrelid INT, relhasindex BOOLEAN, relhasrules BOOLEAN, relhastriggers BOOLEAN, relrowsecurity BOOLEAN, relforcerowsecurity BOOLEAN, relreplident STRING, reltriggers SHORT, relhasoids BOOLEAN, relispartition BOOLEAN, relpartbound STRING"),
    PgSystemTable( 1262, TableIdentifier(  "pg_database", Some(catalogDbName)), "datname STRING, datdba INT, encoding INT, datcollate STRING, datctype STRING, datacl Array<STRING>")
    // scalastyle:on
  )

  // `COPY pg_catalog.* TO '*.csv' WITH CSV HEADER`.
  val catalogTables = staticCatalogTables ++ runtimeCatalogTables

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
    // TODO: Needs to support nested array types, e.g., ArrayType(ArrayType(IntegerType))
    case _ => throw new SQLException("Unsupported array type " + elemType)
    // scalastyle:on
  }


  // We assume the namespace of all entities is `spark`
  private val defaultSparkNamespace = (nextUnusedOid, "spark")
  private val userRoleOid = nextUnusedOid

  // SPARK-23793 Spark-2.3.0 can't handle database names to register UDFs in public APIs
  private def registerFunction(
      ctx: SQLContext, func: FunctionIdentifier, udf: UserDefinedFunction): Unit = {
    def builder(children: Seq[Expression]) = udf.apply(children.map(Column.apply) : _*).expr
    ctx.sessionState.functionRegistry.registerFunction(func, builder)
  }

  private val pgSystemFunctions = Seq(
    // scalastyle:off
    PgSystemFunction(           384, FunctionIdentifier(           "array_to_string", Some(catalogDbName)), _ => udf { (ar: Seq[String], delim: String) => if (ar != null) ar.mkString(delim) else "" }),
    PgSystemFunction(           750, FunctionIdentifier(                  "array_in",                None), _ => udf { () => "array_in" }),
    PgSystemFunction(          1081, FunctionIdentifier(               "format_type", Some(catalogDbName)), _ => udf { (type_oid: Int, typemod: String) => pgTypeOidMap.get(type_oid).map(_.name).getOrElse("unknown") }),
    PgSystemFunction(          1215, FunctionIdentifier(           "obj_description", Some(catalogDbName)), _ => udf { (oid: Int, tableName: String) => "" }),
    PgSystemFunction(          1402, FunctionIdentifier(           "current_schemas",                None), _ => udf { (arg: Boolean) => Seq(defaultSparkNamespace._2) }),
    PgSystemFunction(          1597, FunctionIdentifier(       "pg_encoding_to_char", Some(catalogDbName)), _ => udf { (encoding: Int) => "" }),
    PgSystemFunction(          1642, FunctionIdentifier(           "pg_get_userbyid", Some(catalogDbName)), _ => udf { (userid: Int) => "" }),
    PgSystemFunction(          1716, FunctionIdentifier(               "pg_get_expr", Some(catalogDbName)), _ => udf { (adbin: String, adrelid: Int) => "" }),
    PgSystemFunction(          2079, FunctionIdentifier(       "pg_table_is_visible", Some(catalogDbName)), _ => udf { (tableoid: Int) => !pgCatalogOidMap.get(tableoid).isDefined }),
    PgSystemFunction(          2081, FunctionIdentifier(    "pg_function_is_visible", Some(catalogDbName)), _ => udf { (functionoid: Int) => !pgSystemFunctionOidMap.get(functionoid).isDefined }),
    PgSystemFunction(          2092, FunctionIdentifier(               "array_upper",                None), _ => udf { (ar: Seq[String], n: Int) => ar.size }),
    PgSystemFunction(          2162, FunctionIdentifier( "pg_get_function_arguments", Some(catalogDbName)), _ => udf { (oid: Int) => "" }),
    PgSystemFunction(          2165, FunctionIdentifier(    "pg_get_function_result", Some(catalogDbName)), _ => udf { (oid: Int) => "" }),
    PgSystemFunction(          2420, FunctionIdentifier(            "oidvectortypes", Some(catalogDbName)), _ => udf { (typeoids: Seq[Int]) => if (typeoids != null) typeoids.map(getPgTypeNameFromOid).mkString(", ") else "" }),

    // Entries below is not a kind of functions though, we need to process some interactions
    // between clients and JDBC drivers.
    PgSystemFunction( nextUnusedOid, FunctionIdentifier(                       "ANY",                None), _ => udf { (arg: Seq[String]) => arg.head }),
    PgSystemFunction( nextUnusedOid, FunctionIdentifier(                   "regtype", Some(catalogDbName)), _ => udf { (typeoid: Int) => getPgTypeNameFromOid(typeoid) })
    // scalastyle:on
  )

  private val pgSystemFunctionOidMap: Map[Int, PgSystemFunction] =
    pgSystemFunctions.map(f => f.oid -> f).toMap

  private def safeCreateCatalogTable(
      tableIdentifierWithDb: String,
      sqlContext: SQLContext)(
      stmtToInsertRows: String => Seq[String] = _ => Nil): Unit = {
    val catalogTable = catalogTables.find { case PgSystemTable(_, table, _) =>
      table.unquotedString == tableIdentifierWithDb
    }
    assert(catalogTable.isDefined)
    logInfo(s"Creating a dummy PostgreSQL catalog table: $tableIdentifierWithDb...")
    val dropTable = s"DROP TABLE IF EXISTS $tableIdentifierWithDb"
    val createTable = s"CREATE TABLE $tableIdentifierWithDb(${catalogTable.get.schema})"
    val insertInto = stmtToInsertRows(catalogTable.get.table.quotedString)
    (Seq(dropTable, createTable) ++ insertInto).foreach { sqlText =>
      sqlContext.sql(sqlText.stripMargin)
    }
  }

  def initSystemFunctions(sqlContext: SQLContext): Unit = {
    pgSystemFunctions.foreach { case PgSystemFunction(_, name, udf) =>
      registerFunction(sqlContext, name, udf())
    }
  }

  private val initLock = new ReentrantReadWriteLock

  private def writeLock[A](f: => A): A = {
    val lock = initLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  // TODO: To save initialization time, translates these static tables into JSON files
  def initSystemCatalogTables(sqlContext: SQLContext): Unit = writeLock {
    if (!sqlContext.sessionState.catalog.databaseExists(catalogDbName)) {
      try {
        sqlContext.sql(s"CREATE DATABASE $catalogDbName")

        // TODO: Some frontends (e.g., the PostgreSQL JDBC drivers) implicitly
        // run queries on `pg_namespace` without the database name.
        //
        // For example, the drivers run a query below with `spark.sql.server.version=8.4`;
        //
        // SELECT typinput='array_in'::regproc, typtype
        // FROM pg_catalog.pg_type
        // LEFT JOIN (
        //   select ns.oid as nspoid, ns.nspname, r.r
        // from pg_namespace as ns
        //   join (
        //     select s.r, (current_schemas(false))[s.r] as nspname
        // from generate_series(1, array_upper(current_schemas(false), 1)) as s(r)
        // ) as r
        //   using ( nspname )
        // ) as sp
        //   ON sp.nspoid = typnamespace
        // WHERE typname = $1
        // ORDER BY sp.r, pg_type.oid DESC LIMIT 1;
        safeCreateCatalogTable(s"pg_namespace", sqlContext) { cTableName =>
          s"""
             |INSERT INTO $cTableName
             |  VALUES(${defaultSparkNamespace._1}, '${defaultSparkNamespace._2}')
           """.stripMargin ::
          Nil
        }

        safeCreateCatalogTable(s"$catalogDbName.pg_namespace", sqlContext) { cTableName =>
          s"""
             |INSERT INTO $cTableName
             |  VALUES(${defaultSparkNamespace._1}, '${defaultSparkNamespace._2}')
           """.stripMargin ::
          Nil
        }

        safeCreateCatalogTable(s"$catalogDbName.pg_roles", sqlContext) { cTableName =>
          s"""
             |INSERT INTO $cTableName VALUES($userRoleOid, 'spark-user')
           """ ::
          Nil
        }

        safeCreateCatalogTable(s"$catalogDbName.pg_user", sqlContext) { cTableName =>
          s"""
             |INSERT INTO $cTableName VALUES('spark-user', $userRoleOid)
           """ ::
          Nil
        }

        safeCreateCatalogTable(s"$catalogDbName.pg_type", sqlContext) { cTableName =>
          pgTypes.map { case PgType(oid, name, len, elemOid, input) =>
            // `b` in `typtype` means a primitive type and all the entries in `supportedPgTypes`
            // are primitive types.
            s"""
               |INSERT INTO $cTableName VALUES(
               |  $oid, '$name', 'b', $len, false, $elemOid, ',', '$input', 0, 0, 0,
               |  ${defaultSparkNamespace._1}
               |)
             """
          }
        }

        /**
         * The 6 static and 4 runtime catalog tables are defined below to prevent the PostgreSQL
         * JDBC drivers from throwing meaningless exceptions.
         */
        safeCreateCatalogTable(s"$catalogDbName.pg_index", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_description", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_depend", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_constraint", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_attrdef", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_inherits", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_collation", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_policy", sqlContext)()

        safeCreateCatalogTable(s"$catalogDbName.pg_attribute", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_proc", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_class", sqlContext)()
        safeCreateCatalogTable(s"$catalogDbName.pg_database", sqlContext)()
      } catch {
        case NonFatal(e) =>
          sqlContext.sql(s"DROP DATABASE IF EXISTS $catalogDbName CASCADE")
          throw new SQLException(exceptionString(e))
      }
    }

    // Checks if all the catalog tables exist in the Spark catalog
    require(catalogTables.forall { case PgSystemTable(_, table, _) =>
      sqlContext.sessionState.catalog.tableExists(table)
    })
  }
}
