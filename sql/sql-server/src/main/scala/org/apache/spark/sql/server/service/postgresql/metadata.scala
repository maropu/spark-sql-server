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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

/**
 * This is the PostgreSQL system information such as catalog tables and functions.
 */
private[service] object Metadata {

  // Since v7.3, all the catalog tables have been moved in a `pg_catalog` database
  private val catalogDbName = "default"

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
    sqlContext.udf.register("ANY", (arg: String) => arg)
    sqlContext.udf.register("current_schemas", (arg: Boolean) => defaultSparkNamespace._2)
    // sqlContext.udf.register("array_in", () => "array_in")
    sqlContext.udf.register(s"${catalogDbName}.obj_description", (oid: Int, tableName: String) => "")
  }

  def initCatalogTables(sqlContext: SQLContext): Unit = {
    sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS ${catalogDbName}")

    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_namespace")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_namespace(
        |   oid INT,
        |   nspname STRING
        | )
      """.stripMargin)
    sqlContext.sql(
      s"""
        | INSERT INTO ${catalogDbName}.pg_namespace
        |   VALUES(${defaultSparkNamespace._1}, '${defaultSparkNamespace._2}')
      """.stripMargin)

    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_roles")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_roles(
        |   oid INT,
        |   rolname STRING
        | )
      """.stripMargin)
    sqlContext.sql(
      s"""
        | INSERT INTO ${catalogDbName}.pg_roles VALUES(%d, 'spark-user')
      """.stripMargin
      .format(userRoleOid))

    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_user")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_user(
        |   usename STRING,
        |   usesysid INT
        | )
      """.stripMargin)
    sqlContext.sql(
      s"""
        | INSERT INTO ${catalogDbName}.pg_user VALUES('spark-user', ${userRoleOid})
      """.stripMargin)

    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_type")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_type(
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
      """.stripMargin)

    supportedPgTypes.map { tpe =>
      // `b` in `typtype` means a primitive type and all the entries in `supportedPgTypes`
      // are primitive types.
      sqlContext.sql(
        s"""
          | INSERT INTO ${catalogDbName}.pg_type
          |   SELECT %d, '%s', 'b', %d, false, %d, ',', '%s', 0, 0, %d
        """.stripMargin
        .format(tpe.oid, tpe.name, tpe.len, tpe.elemOid, tpe.input, defaultSparkNamespace._1))
    }

    // TODO: Need to load entries for existing tables in a database
    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_class")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_class(
        |   oid INT,
        |   relname STRING,
        |   relkind STRING,
        |   relnamespace INT,
        |   relowner INT,
        |   relacl ARRAY<STRING>
        | )
      """.stripMargin)

    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_attribute")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_attribute(
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
      """.stripMargin)

    /**
     * Five empty catalog tables are defined below to prevent the PostgreSQL JDBC drivers from
     * throwing meaningless exceptions.
     */
    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_index")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_index(
        |   oid INT,
        |   indrelid INT,
        |   indexrelid INT,
        |   indisprimary BOOLEAN
        | )
      """.stripMargin)

    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_proc")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_proc(
        |   oid INT,
        |   proname STRING,
        |   prorettype INT,
        |   proargtypes ARRAY<INT>,
        |   pronamespace INT
        | )
      """.stripMargin)

    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_description")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_description(
        |   objoid INT,
        |   classoid INT,
        |   description STRING
        | )
      """.stripMargin)

    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_depend")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_depend(
        |   objid INT,
        |   classid INT,
        |   refobjid INT,
        |   refclassid INT
        | )
      """.stripMargin)

    sqlContext.sql(s"DROP TABLE IF EXISTS ${catalogDbName}.pg_constraint")
    sqlContext.sql(
      s"""
        | CREATE TABLE ${catalogDbName}.pg_constraint(
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
      """.stripMargin)
  }

  def registerTableInCatalog(
      tableName: String, schema: StructType, sqlContext: SQLContext): Unit = {
    if (isReservedTableName(tableName, sqlContext.sparkSession.catalog.currentDatabase)) {
      throw new IllegalArgumentException(s"${tableName} is reserved for system use")
    }

    val tableOid = nextUnusedOid
    sqlContext.sql(
      s"""
        | INSERT INTO ${catalogDbName}.pg_class VALUES(%d, '%s', '%s', %d, %d, %s)
      """.stripMargin
      .format(tableOid, tableName, "r", defaultSparkNamespace._1, userRoleOid, "null"))

    schema.zipWithIndex.map { case (field, index) =>
      val pgType = getPgType(field.dataType)
      sqlContext.sql(
        s"""
          | INSERT INTO ${catalogDbName}.pg_attribute
          |   VALUES(%d, %d, '%s', %d, %b, %d, %d, %d, false)
        """.stripMargin
        .format(nextUnusedOid, tableOid, field.name, pgType.oid, !field.nullable,
          0, pgType.len, 1 + index))
    }
  }

  private def isReservedTableName(tableName: String, dbName: String): Boolean = {
    Seq("pg_namespace", "pg_type", "pg_roles", "pg_user", "pg_class", "pg_attribute", "pg_index",
        "pg_procs", "pg_description", "pg_depend", "pg_constraint").map { reserved =>
      if (dbName != catalogDbName) {
        s"${catalogDbName}.${reserved}"
      } else {
        reserved
      }
    }.contains(tableName)
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
    // case MapType              =>
    // case StructType           =>
    case _ => throw new SQLException("Unsupported array type " + elemType)
    // scalastyle:on
  }
}
