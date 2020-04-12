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

package org.apache.spark.sql.server.service.postgresql.protocol.v3

import java.io.CharArrayWriter
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.util.TimeZone

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.service.postgresql.PgMetadata._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A [[PgParamConverters]] is used to convert binary in the `Bind` message into [[Literal]] to
 * bind them in a prepared statement.
 */
object PgParamConverters {

  def apply(params: Seq[Array[Byte]], types: Seq[Int], formats: Seq[Int]): Seq[(Int, Literal)] = {
    params.zipWithIndex.map { case (param, i) =>
      val value = (types(i), formats(i)) match {
        case (PgUnspecifiedType.oid, format) =>
          throw new SQLException(s"Unspecified type unsupported: format=$format")
        case (PgBoolType.oid, 0) =>
          val value = if (Seq(49, 84, 116).contains(param(0))) {
            // In case of 49('1'), 84('T'), or 116('t')
            true
          } else if (Seq(48, 70, 102).contains(param(0))) {
            // In case of 48('0'), 70('F'), or 102('f')
            false
          } else {
            throw new SQLException(s"Unknown bool parameter: '${param(0)}'")
          }
          Literal(value, BooleanType)
        case (PgBoolType.oid, 1) =>
          Literal(if (param(0) == 1) true else false, BooleanType)
        case (PgNumericType.oid, 0) =>
          val value = Decimal(new String(param, StandardCharsets.UTF_8))
          Literal(value, new DecimalType())
        case (PgInt2Type.oid, 0) =>
          val value = new String(param, StandardCharsets.UTF_8).toShort
          Literal(value, ShortType)
        case (PgInt2Type.oid, 1) =>
          val value = ByteBuffer.wrap(param).getShort
          Literal(value, ShortType)
        case (PgInt4Type.oid, 0) =>
          val value = new String(param, StandardCharsets.UTF_8).toInt
          Literal(value, IntegerType)
        case (PgInt4Type.oid, 1) =>
          val value = ByteBuffer.wrap(param).getInt
          Literal(value, IntegerType)
        case (PgInt8Type.oid, 0) =>
          val value = new String(param, StandardCharsets.UTF_8).toLong
          Literal(value, LongType)
        case (PgInt8Type.oid, 1) =>
          val value = ByteBuffer.wrap(param).getLong
          Literal(value, LongType)
        case (PgFloat4Type.oid, 0) =>
          val value = new String(param, StandardCharsets.UTF_8).toFloat
          Literal(value, FloatType)
        case (PgFloat4Type.oid, 1) =>
          val value = ByteBuffer.wrap(param).getFloat
          Literal(value, FloatType)
        case (PgFloat8Type.oid, 0) =>
          val value = new String(param, StandardCharsets.UTF_8).toDouble
          Literal(value, DoubleType)
        case (PgFloat8Type.oid, 1) =>
          val value = ByteBuffer.wrap(param).getDouble
          Literal(value, DoubleType)
        case (PgVarCharType.oid, _) =>
          val value = UTF8String.fromBytes(param)
          Literal(value, StringType)
        // TODO: Need to support other types, e.g., `Date` and `Timestamp`
        case (paramId, format) =>
          throw new SQLException(s"Cannot bind param: paramId=$paramId, format=$format")
      }
      (i + 1) -> value
    }
  }
}

/**
 * A [[PgRowConverters]] is used to convert [[InternalRow]]s into PostgreSQL V3 records.
 */
object PgRowConverters {

  type RowWriter = (InternalRow, ByteBuffer) => Int

  def apply(conf: SQLConf, schema: StructType, outputFormats: Seq[Boolean]): RowWriter = {
    require(schema.length == outputFormats.size, "format must have the same length with schema")
    val columnWriters = schema.fields.zipWithIndex.map { case (field, ordinal) =>
      ColumnWriter(field, ordinal, outputFormats(ordinal), conf)
    }
    (row: InternalRow, byteBuffer: ByteBuffer) => {
      require(row.numFields == schema.length)
      val base = byteBuffer.position()
      (0 until row.numFields).foreach { index =>
        columnWriters(index).write(row, byteBuffer)
      }
      // Return # of written bytes in `byteBuffer`
      byteBuffer.position - base
    }
  }
}

/**
 * Interface for converting InternalRows to a byte array.
 */
abstract class ColumnWriter(ordinal: Int) {

  private val NULL = -1

  def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit

  final def write(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    if (!row.isNullAt(ordinal)) {
      nullSafeWriter(row, byteBuffer)
    } else {
      byteBuffer.putInt(NULL)
    }
  }
}

class NullColumnWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    throw new SQLException("Invalid call to nullSafeWriter on NullColumnWriter")
  }
}

/**
 * Special class for primitive types with a text mode enabled. This class converts input to strings
 * without special cares.
 */
class ColumnTextWriter(dataType: DataType, ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val value = row.get(ordinal, dataType)
    val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
    byteBuffer.putInt(bytes.length)
    byteBuffer.put(bytes)
  }
}

class BooleanColumnTextWriter(ordinal: Int) extends ColumnTextWriter(BooleanType, ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(1)
    byteBuffer.put(if (row.getBoolean(ordinal)) 't'.toByte else 'f'.toByte)
  }
}

class BooleanColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(1)
    // scalastyle:off line.size.limit
    // TODO: Reconsider this code because this behaivour depends on JVM implementations:
    // https://stackoverflow.com/questions/8248925/internally-in-java-primitive-booleans-are-treated-like-1
    // scalastyle:on line.size.limit
    byteBuffer.put(if (row.getBoolean(ordinal)) 1.toByte else 0.toByte)
  }
}

class ShortColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(2)
    byteBuffer.putShort(row.getShort(ordinal))
  }
}

class IntegerColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(4)
    byteBuffer.putInt(row.getInt(ordinal))
  }
}

class LongColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(8)
    byteBuffer.putLong(row.getLong(ordinal))
  }
}

class FloatColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(4)
    byteBuffer.putFloat(row.getFloat(ordinal))
  }
}

class DoubleColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(8)
    byteBuffer.putDouble(row.getDouble(ordinal))
  }
}

class ByteColumnWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(1)
    byteBuffer.put(row.get(ordinal, ByteType).asInstanceOf[Byte])
  }
}

class UTF8StringColumnWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val utf8string = row.getUTF8String(ordinal)
    val numBytes = utf8string.numBytes()
    byteBuffer.putInt(numBytes)
    utf8string.writeTo(byteBuffer)
  }
}

class BinaryColumnWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val bytes = row.getBinary(ordinal)
    val numBytes = bytes.length
    byteBuffer.putInt(numBytes)
    byteBuffer.put(bytes)
  }
}

abstract class DateColumnWriter(ordinal: Int, conf: SQLConf) extends ColumnWriter(ordinal) {

  protected val timezone = TimeZone.getTimeZone(conf.sessionLocalTimeZone)

  // Converts the given java seconds to PostgreSQL seconds
  def toPgSecs(secs: Long): Long = {
    // java epoc to postgres epoc
    val pgSecs = secs - 946684800L

    // Julian/Greagorian calendar cutoff point
    if (pgSecs < -13165977600L) { // October 15, 1582 -> October 4, 1582
      val localSecs = pgSecs - 86400 * 10
      if (localSecs < -15773356800L) { // 1500-03-01 -> 1500-02-28
        var years = (localSecs + 15773356800L) / -3155823050L
        years += 1
        years -= years / 4
        localSecs + years * 86400
      } else {
        localSecs
      }
    } else {
      pgSecs
    }
  }
}

class DateColumnTextWriter(ordinal: Int, conf: SQLConf)
    extends DateColumnWriter(ordinal, conf) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val date = DateTimeUtils.toJavaDate(row.getInt(ordinal))
    val bytes = date.toString.getBytes(StandardCharsets.UTF_8)
    byteBuffer.putInt(bytes.length)
    byteBuffer.put(bytes)
  }
}

class DateColumnBinaryWriter(ordinal: Int, conf: SQLConf)
    extends DateColumnWriter(ordinal, conf) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val date = DateTimeUtils.toJavaDate(row.getInt(ordinal))
    val millis = date.getTime + timezone.getOffset(date.getTime)
    val days = toPgSecs(millis / 1000) / 86400
    byteBuffer.putInt(4)
    byteBuffer.putInt(days.asInstanceOf[Int])
  }
}

class TimestampColumnTextWriter(ordinal: Int, conf: SQLConf)
    extends DateColumnWriter(ordinal, conf) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val timestamp = DateTimeUtils.toJavaTimestamp(row.getLong(ordinal))
    val bytes = timestamp.toString.getBytes(StandardCharsets.UTF_8)
    byteBuffer.putInt(bytes.length)
    byteBuffer.put(bytes)
  }
}

class TimestampColumnBinaryWriter(ordinal: Int, conf: SQLConf)
    extends DateColumnWriter(ordinal, conf) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val timestamp = DateTimeUtils.toJavaTimestamp(row.getLong(ordinal))
    val millis = timestamp.getTime + timezone.getRawOffset
    // PostgreSQL TIMESTAMP only supports microseconds
    val micros = timestamp.getNanos / 1000
    val pgTime = toPgSecs(millis / 1000) * 1000000L + micros
    byteBuffer.putInt(8)
    byteBuffer.putLong(pgTime)
  }
}

abstract class ComplexTypeColumnTextWriter(field: StructField, ordinal: Int, conf: SQLConf)
    extends ColumnWriter(ordinal) {

  private val writer = new CharArrayWriter
  private val jsonGenerator = {
    val outputSchema = new StructType().add(field)
    // Set a timestamp format so that JDBC drivers can parse data
    val options = new JSONOptions(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"),
      conf.sessionLocalTimeZone)
    new JacksonGenerator(outputSchema, writer, options)
  }

  val extractInnerJson = s"""\\{"${field.name}":(.*)\\}""".r

  def toJson(row: InternalRow): String = {
    jsonGenerator.write(row)
    jsonGenerator.flush()
    val json = writer.toString
    writer.reset()
    json
  }

  def convertRow(row: InternalRow): Array[Byte]

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val bytes = convertRow(row)
    byteBuffer.putInt(bytes.length)
    byteBuffer.put(bytes)
  }
}

class ArrayColumnTextWriter(field: StructField, ordinal: Int, conf: SQLConf)
    extends ComplexTypeColumnTextWriter(field, ordinal, conf) {

  private val arrayRow = new GenericInternalRow(Array[Any](1))

  override def convertRow(row: InternalRow): Array[Byte] = {
    arrayRow.update(0, row.getArray(ordinal))
    val jsonString = toJson(arrayRow) match {
      case extractInnerJson(json) =>
        val extractArrayElements = s"""\\[(.*)\\]""".r
        json match {
          case extractArrayElements(elems) => s"{$elems}"
        }
      case s =>
        sys.error(s"Hit unexpected json format: $s")
    }
    jsonString.getBytes(StandardCharsets.UTF_8)
  }
}

class MapColumnTextWriter(field: StructField, ordinal: Int, conf: SQLConf)
    extends ComplexTypeColumnTextWriter(field, ordinal, conf) {

  private val mapRow = new GenericInternalRow(Array[Any](1))

  override def convertRow(row: InternalRow): Array[Byte] = {
    mapRow.update(0, row.getMap(ordinal))
    val jsonString = toJson(mapRow) match {
      case extractInnerJson(json) => json
      case s => sys.error(s"Hit unexpected json format: $s")
    }
    jsonString.getBytes(StandardCharsets.UTF_8)
  }
}

class StructColumnTextWriter(field: StructField, ordinal: Int, conf: SQLConf)
    extends ComplexTypeColumnTextWriter(field, ordinal, conf) {

  override def convertRow(row: InternalRow): Array[Byte] = {
    val data = row.getStruct(ordinal, 1)
    val jsonString = toJson(data) match {
      case extractInnerJson(json) => json
      case s => sys.error(s"Hit unexpected json format: $s")
    }
    jsonString.getBytes(StandardCharsets.UTF_8)
  }
}

object ColumnWriter {

  private def isPrimitive(dataType: DataType): Boolean = {
    Seq(BooleanType, ShortType, IntegerType, LongType, FloatType, DoubleType, ByteType)
      .contains(dataType)
  }

  /**
   * Create an PostgreSQL V3 [[ColumnWriter]] given the type and ordinal of row.
   */
  def apply(field: StructField, ordinal: Int, isBinary: Boolean, conf: SQLConf): ColumnWriter = {
    (field.dataType, isBinary) match {
      case (NullType, _) => new NullColumnWriter(ordinal)
      case (BooleanType, true) => new BooleanColumnBinaryWriter(ordinal)
      case (BooleanType, false) => new BooleanColumnTextWriter(ordinal)
      case (ShortType, true) => new ShortColumnBinaryWriter(ordinal)
      case (IntegerType, true) => new IntegerColumnBinaryWriter(ordinal)
      case (LongType, true) => new LongColumnBinaryWriter(ordinal)
      case (FloatType, true) => new FloatColumnBinaryWriter(ordinal)
      case (DoubleType, true) => new DoubleColumnBinaryWriter(ordinal)
      case (ByteType, true) => new ByteColumnWriter(ordinal)
      case (StringType, _) => new UTF8StringColumnWriter(ordinal)
      case (BinaryType, _) => new BinaryColumnWriter(ordinal)
      case (DateType, true) => new DateColumnBinaryWriter(ordinal, conf)
      case (TimestampType, true) => new TimestampColumnBinaryWriter(ordinal, conf)

      case (tpe, false) if isPrimitive(tpe) => new ColumnTextWriter(tpe, ordinal)
      case (tpe: DecimalType, false) => new ColumnTextWriter(tpe, ordinal)
      case (DateType, false) => new DateColumnTextWriter(ordinal, conf)
      case (TimestampType, false) => new TimestampColumnTextWriter(ordinal, conf)
      case (_: ArrayType, false) => new ArrayColumnTextWriter(field, ordinal, conf)
      case (_: MapType, false) => new MapColumnTextWriter(field, ordinal, conf)
      case (_: StructType, false) => new StructColumnTextWriter(field, ordinal, conf)

      // Handles all UDTs as strings
      case (udt: UserDefinedType[_], false) => new ColumnTextWriter(udt, ordinal)

      case _ => throw new SQLException(
        s"Cannot convert value: type=${field.dataType}, isBinary=$isBinary")
    }
  }
}
