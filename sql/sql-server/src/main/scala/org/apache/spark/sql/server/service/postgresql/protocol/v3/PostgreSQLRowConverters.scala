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
import java.sql.SQLException
import java.util.TimeZone

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions}
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.types._


/**
 * A [[PostgreSQLRowConverters]] is used to convert [[InternalRow]]s into PostgreSQL V3 records.
 */
private[v3] object PostgreSQLRowConverters {

  def buildRowConverter(schema: StructType, formatsOption: Option[Seq[Int]] = None)
    : (InternalRow) => Seq[Array[Byte]] = {
    require(formatsOption.isEmpty || schema.length == formatsOption.get.size,
      "format must have the same length with schema")
    def toBytes(s: String) = s.getBytes("US-ASCII")
    val outputFormats =  formatsOption.getOrElse {
      // If `formatsOption` not defined, test mode enabled
      Seq.fill(schema.length)(0)
    }
    val attrs = schema.toAttributes
    val fieldsConverter: Seq[InternalRow => Array[Byte]] = attrs.zipWithIndex.map {
      case (attrRef @ AttributeReference(name, tpe, _, _), i) =>
        val proj = UnsafeProjection.create(attrRef :: Nil, attrs)
        (tpe, outputFormats(i)) match {
          case (BinaryType, 1) =>
            (row: InternalRow) => {
              val field = proj(row)
              field.get(0, BinaryType).asInstanceOf[Array[Byte]]
            }
          case (TimestampType, 0) =>
            val buf = new Array[Byte](8)
            (row: InternalRow) => {
              val field = proj(row)
              val timestamp = toJavaTimestamp(field.get(0, TimestampType).asInstanceOf[Long])
              toBytes(s"$timestamp")
            }
          case (binaryTimeType @ (DateType | TimestampType), 1) =>
            val timezone = TimeZone.getDefault
            // Converts the given java seconds to postgresql seconds
            def toPgSecs(secs: Long) = {
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

            binaryTimeType match {
              case DateType =>
                val buf = new Array[Byte](4)
                val writer = ByteBuffer.wrap(buf)
                (row: InternalRow) => {
                  val field = proj(row)
                  val date = toJavaDate(field.get(0, DateType).asInstanceOf[Int])
                  val millis = date.getTime + timezone.getOffset(date.getTime)
                  val days = toPgSecs(millis / 1000) / 86400
                  writer.putInt(days.asInstanceOf[Int])
                  writer.flip()
                  buf
                }
              case TimestampType =>
                val buf = new Array[Byte](8)
                val writer = ByteBuffer.wrap(buf)
                (row: InternalRow) => {
                  val field = proj(row)
                  val timestamp = toJavaTimestamp(field.get(0, TimestampType).asInstanceOf[Long])
                  val mills = timestamp.getTime + timezone.getOffset(timestamp.getTime)
                  writer.putLong(toPgSecs(mills / 1000) * 1000000L)
                  writer.flip()
                  buf
                }
            }
          case (textComplexType @ (_: StructType | _: MapType | _: ArrayType), 0) =>
            val outputSchema = new StructType().add(schema.fields(i))
            val writer = new CharArrayWriter
            // Set a timestamp format so that JDBC drivers can parse data
            val options = new JSONOptions(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"),
              TimeZone.getDefault.getID)
            val jsonGenerator = new JacksonGenerator(outputSchema, writer, options)
            def toJson(row: InternalRow): String = {
              jsonGenerator.write(row)
              jsonGenerator.flush()
              val json = writer.toString
              writer.reset()
              json
            }

            (row: InternalRow) => {
              val field = proj(row)
              val extractInnerJson = s"""\\{"$name":(.*)\\}""".r
              val json = toJson(field) match {
                case extractInnerJson(json) if textComplexType.isInstanceOf[ArrayType] =>
                  val extractArrayElements = s"""\\[(.*)\\]""".r
                  json match {
                    case extractArrayElements(elems) => s"{$elems}"
                  }
                case extractInnerJson(json) =>
                  json
              }
              toBytes(json)
            }
          case (_, 0) => // In text mode, it's ok to just print it
            (row: InternalRow) => {
              toBytes(s"${proj(row).get(0, tpe)}")
            }
          case (ShortType, 1) =>
            val buf = new Array[Byte](2)
            val writer = ByteBuffer.wrap(buf)
            (row: InternalRow) => {
              val value = proj(row).get(0, ShortType)
              writer.putShort(value.asInstanceOf[Short])
              writer.flip()
              buf
            }
          case (binary4ByteType @ (IntegerType | FloatType), 1) =>
            val buf = new Array[Byte](4)
            val writer = ByteBuffer.wrap(buf)
            (row: InternalRow) => {
              val value = proj(row).get(0, binary4ByteType)
              binary4ByteType match {
                case IntegerType => writer.putInt(value.asInstanceOf[Int])
                case FloatType => writer.putFloat(value.asInstanceOf[Float])
              }
              writer.flip()
              buf
            }
          case (binary8ByteType @ (LongType | DoubleType), 1) =>
            val buf = new Array[Byte](8)
            val writer = ByteBuffer.wrap(buf)
            (row: InternalRow) => {
              val value = proj(row).get(0, binary8ByteType)
              binary8ByteType match {
                case LongType => writer.putLong(value.asInstanceOf[Long])
                case DoubleType => writer.putDouble(value.asInstanceOf[Double])
              }
              writer.flip()
              buf
            }
          case (tpe, format) =>
            throw new SQLException(s"Cannot convert param: type=$tpe, format=$format")
        }
    }

    (row: InternalRow) => {
      require(row.numFields == schema.length)
      (0 until row.numFields).map { index =>
        if (!row.isNullAt(index)) {
          fieldsConverter(index)(row)
        } else {
          Array.empty[Byte]
        }
      }
    }
  }
}
