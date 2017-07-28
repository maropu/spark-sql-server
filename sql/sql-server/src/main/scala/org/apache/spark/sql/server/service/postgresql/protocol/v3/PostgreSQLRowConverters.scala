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
import java.util.TimeZone

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._


/**
 * A [[PostgreSQLRowConverters]] is used to convert [[InternalRow]]s into PostgreSQL V3 records.
 */
private[v3] object PostgreSQLRowConverters {

  type RowWriter = (InternalRow, ByteBuffer) => Int

  def apply(schema: StructType, formatsOption: Option[Seq[Boolean]] = None): RowWriter = {
    require(formatsOption.isEmpty || schema.length == formatsOption.get.size,
      "format must have the same length with schema")
    val outputFormats = formatsOption.getOrElse {
      // If `formatsOption` not defined, we assume output as texts
      Seq.fill(schema.length)(false)
    }
    val columnWriters = schema.fields.zipWithIndex.map { case (field, ordinal) =>
      ColumnWriter(field, ordinal, outputFormats(ordinal))
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
private abstract class ColumnWriter(ordinal: Int) {

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

private class NullColumnWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    throw new UnsupportedOperationException("Invalid call to nullSafeWriter on NullColumnWriter")
  }
}

/**
 * Special class for primitive types with a text mode enabled. This class converts input to strings
 * without special cares.
 */
private class ColumnTextWriter(dataType: DataType, ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val value = row.get(ordinal, dataType)
    val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
    byteBuffer.putInt(bytes.length)
    byteBuffer.put(bytes)
  }
}

private class BooleanColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(1)
    // scalastyle:off line.size.limit
    // TODO: Reconsider this code because this behaivour depends on JVM implementations:
    // https://stackoverflow.com/questions/8248925/internally-in-java-primitive-booleans-are-treated-like-1
    // scalastyle:on line.size.limit
    byteBuffer.put(if (row.getBoolean(ordinal)) 1.toByte else 0.toByte)
  }
}

private class ShortColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(2)
    byteBuffer.putShort(row.getShort(ordinal))
  }
}

private class IntegerColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(4)
    byteBuffer.putInt(row.getInt(ordinal))
  }
}

private class LongColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(8)
    byteBuffer.putLong(row.getLong(ordinal))
  }
}

private class FloatColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(4)
    byteBuffer.putFloat(row.getFloat(ordinal))
  }
}

private class DoubleColumnBinaryWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(8)
    byteBuffer.putDouble(row.getDouble(ordinal))
  }
}

private class ByteColumnWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(1)
    byteBuffer.put(row.get(ordinal, ByteType).asInstanceOf[Byte])
  }
}

private class UTF8StringColumnWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val utf8string = row.getUTF8String(ordinal)
    val numBytes = utf8string.numBytes()
    byteBuffer.putInt(numBytes)
    utf8string.writeTo(byteBuffer)
  }
}

private class BinaryColumnWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val bytes = row.getBinary(ordinal)
    val numBytes = bytes.length
    byteBuffer.putInt(numBytes)
    byteBuffer.put(bytes)
  }
}

private abstract class DateColumnWriter(ordinal: Int) extends ColumnWriter(ordinal) {

  val timezone = TimeZone.getDefault

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

private class DateColumnTextWriter(ordinal: Int) extends DateColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val date = DateTimeUtils.toJavaDate(row.getInt(ordinal))
    val bytes = date.toString.getBytes(StandardCharsets.UTF_8)
    byteBuffer.putInt(bytes.length)
    byteBuffer.put(bytes)
  }
}

private class DateColumnBinaryWriter(ordinal: Int) extends DateColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val date = DateTimeUtils.toJavaDate(row.getInt(ordinal))
    val millis = date.getTime + timezone.getOffset(date.getTime)
    val days = toPgSecs(millis / 1000) / 86400
    byteBuffer.putInt(4)
    byteBuffer.putInt(days.asInstanceOf[Int])
  }
}

private class TimestampColumnTextWriter(ordinal: Int) extends DateColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val timestamp = DateTimeUtils.toJavaTimestamp(row.getLong(ordinal))
    val bytes = timestamp.toString.getBytes(StandardCharsets.UTF_8)
    byteBuffer.putInt(bytes.length)
    byteBuffer.put(bytes)
  }
}

private class TimestampColumnBinaryWriter(ordinal: Int) extends DateColumnWriter(ordinal) {

  override def nullSafeWriter(row: InternalRow, byteBuffer: ByteBuffer): Unit = {
    val timestamp = DateTimeUtils.toJavaTimestamp(row.getLong(ordinal))
    val mills = timestamp.getTime + timezone.getOffset(timestamp.getTime)
    byteBuffer.putInt(8)
    byteBuffer.putLong(mills)
  }
}

private abstract class ComplexTypeColumnTextWriter(field: StructField, ordinal: Int)
    extends ColumnWriter(ordinal) {

  private val writer = new CharArrayWriter
  private val jsonGenerator = {
    val outputSchema = new StructType().add(field)
    // Set a timestamp format so that JDBC drivers can parse data
    val options = new JSONOptions(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"),
      TimeZone.getDefault.getID)
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

private class ArrayColumnTextWriter(field: StructField, ordinal: Int)
    extends ComplexTypeColumnTextWriter(field, ordinal) {

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

private class MapColumnTextWriter(field: StructField, ordinal: Int)
    extends ComplexTypeColumnTextWriter(field, ordinal) {

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

private class StructColumnTextWriter(field: StructField, ordinal: Int)
    extends ComplexTypeColumnTextWriter(field, ordinal) {

  override def convertRow(row: InternalRow): Array[Byte] = {
    val data = row.getStruct(ordinal, 1)
    val jsonString = toJson(data) match {
      case extractInnerJson(json) => json
      case s => sys.error(s"Hit unexpected json format: $s")
    }
    jsonString.getBytes(StandardCharsets.UTF_8)
  }
}

private[v3] object ColumnWriter {

  private def isPrimitive(dataType: DataType): Boolean = {
    Seq(BooleanType, ShortType, IntegerType, LongType, FloatType, DoubleType, ByteType)
      .contains(dataType)
  }

  /**
   * Create an PostgreSQL V3 [[ColumnWriter]] given the type and ordinal of row.
   */
  def apply(field: StructField, ordinal: Int, isBinary: Boolean): ColumnWriter = {
    (field.dataType, isBinary) match {
      case (NullType, _) => new NullColumnWriter(ordinal)
      case (BooleanType, true) => new BooleanColumnBinaryWriter(ordinal)
      case (ShortType, true) => new ShortColumnBinaryWriter(ordinal)
      case (IntegerType, true) => new IntegerColumnBinaryWriter(ordinal)
      case (LongType, true) => new LongColumnBinaryWriter(ordinal)
      case (FloatType, true) => new FloatColumnBinaryWriter(ordinal)
      case (DoubleType, true) => new DoubleColumnBinaryWriter(ordinal)
      case (ByteType, true) => new ByteColumnWriter(ordinal)
      case (StringType, _) => new UTF8StringColumnWriter(ordinal)
      case (BinaryType, _) => new BinaryColumnWriter(ordinal)
      case (DateType, true) => new DateColumnBinaryWriter(ordinal)
      case (TimestampType, true) => new TimestampColumnBinaryWriter(ordinal)

      case (tpe, false) if isPrimitive(tpe) => new ColumnTextWriter(tpe, ordinal)
      case (tpe: DecimalType, false) => new ColumnTextWriter(tpe, ordinal)
      case (DateType, false) => new DateColumnTextWriter(ordinal)
      case (TimestampType, false) => new TimestampColumnTextWriter(ordinal)
      case (_: ArrayType, false) => new ArrayColumnTextWriter(field, ordinal)
      case (_: MapType, false) => new MapColumnTextWriter(field, ordinal)
      case (_: StructType, false) => new StructColumnTextWriter(field, ordinal)

      case _ => throw new UnsupportedOperationException(
        s"Cannot convert value: type=${field.dataType}, isBinary=$isBinary")
    }
  }
}
