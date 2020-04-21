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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.{Date, SQLException, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// Wrapped in an object to check Scala compatibility. See SPARK-13929
object UDT {

  @SQLUserDefinedType(udt = classOf[MyDenseVectorUDT])
  class MyDenseVector(val data: Array[Double]) extends Serializable {
    override def hashCode(): Int = java.util.Arrays.hashCode(data)

    override def equals(other: Any): Boolean = other match {
      case v: MyDenseVector => java.util.Arrays.equals(this.data, v.data)
      case _ => false
    }

    override def toString: String = data.mkString("(", ", ", ")")
  }

  class MyDenseVectorUDT extends UserDefinedType[MyDenseVector] {

    override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

    override def serialize(features: MyDenseVector): ArrayData = {
      new GenericArrayData(features.data.map(_.asInstanceOf[Any]))
    }

    override def deserialize(datum: Any): MyDenseVector = {
      datum match {
        case data: ArrayData =>
          new MyDenseVector(data.toDoubleArray())
      }
    }

    override def userClass: Class[MyDenseVector] = classOf[MyDenseVector]

    private[spark] override def asNullable: MyDenseVectorUDT = this

    override def hashCode(): Int = getClass.hashCode()

    override def equals(other: Any): Boolean = other.isInstanceOf[MyDenseVectorUDT]
  }
}

class PgRowConvertersSuite extends SparkFunSuite {

  private val conf = new SQLConf()

  test("primitive types") {
    Seq(
      (false, "f", "BOOLEAN", (b: Array[Byte]) => b(0) == 1),
      (13.toByte, "13", "BYTE", (b: Array[Byte]) => b(0)),
      (2392.toShort, "2392", "SHORT", (b: Array[Byte]) => ByteBuffer.wrap(b).getShort),
      (813, "813", "INT", (b: Array[Byte]) => ByteBuffer.wrap(b).getInt),
      (18923L, "18923", "LONG", (b: Array[Byte]) => ByteBuffer.wrap(b).getLong),
      (1.0f, "1.0", "FLOAT", (b: Array[Byte]) => ByteBuffer.wrap(b).getFloat),
      (8.0, "8.0", "DOUBLE", (b: Array[Byte]) => ByteBuffer.wrap(b).getDouble)
    ).foreach { case (data, expectedTextData, tpe, readData) =>
      Seq(true, false).foreach { binaryMode =>
        val fieldType = StructType.fromDDL(s"a $tpe")(0)
        val inputRow = new GenericInternalRow(1)
        inputRow.update(0, data)
        val dataSize = if (binaryMode) fieldType.dataType.defaultSize else expectedTextData.length
        val buf = new Array[Byte](4 + dataSize)
        val byteBuffer = ByteBuffer.wrap(buf)
        val writer = ColumnWriter(fieldType, 0, isBinary = binaryMode, conf)
        writer.write(inputRow, byteBuffer)
        byteBuffer.rewind()

        // Check data field size
        assert(byteBuffer.getInt === dataSize, s"type=$tpe isBinary=$binaryMode")

        // Check data itself
        val slicedBytes = buf.slice(4, 4 + dataSize)
        val actualData = if (binaryMode) readData(slicedBytes) else slicedBytes
        val expectedData = if (!binaryMode) {
          expectedTextData.getBytes(StandardCharsets.UTF_8)
        } else {
          data
        }
        assert(actualData === expectedData, s"type=$tpe isBinary=$binaryMode")
      }
    }
  }

  test("null") {
    val fieldType = StructField("a", NullType)
    val byteBuffer = ByteBuffer.allocate(1)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, 0)
    val writer = ColumnWriter(fieldType, 0, isBinary = false, conf)
    val errMsg = intercept[SQLException] {
      writer.write(inputRow, byteBuffer)
    }.getMessage
    assert(errMsg.contains("Invalid call to nullSafeWriter on NullColumnWriter"))
  }

  test("decimal") {
    val fieldType = StructType.fromDDL("a DECIMAL")(0)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, BigDecimal.decimal(3.0))
    val buf = new Array[Byte](7)
    val byteBuffer = ByteBuffer.wrap(buf)
    val writer = ColumnWriter(fieldType, 0, isBinary = false, conf)
    writer.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check data field size
    assert(byteBuffer.getInt === 3)

    // Check data itself
    val actualData = buf.slice(4, 7)
    assert(UTF8String.fromBytes(actualData).toString === "3.0")

    val errMsg = intercept[SQLException] {
      ColumnWriter(fieldType, 0, isBinary = true, conf)
    }.getMessage
    assert(errMsg.contains("Cannot convert value: type=DecimalType(10,0), isBinary=true"))
  }

  test("date") {
    val fieldType = StructType.fromDDL("a DATE")(0)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, DateTimeUtils.fromJavaDate(Date.valueOf("2017-08-04")))
    val buf = new Array[Byte](14)
    val byteBuffer = ByteBuffer.wrap(buf)

    val textWriter = ColumnWriter(fieldType, 0, isBinary = false, conf)
    textWriter.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check the result with text mode
    assert(byteBuffer.getInt === 10)
    val actualData1 = buf.slice(4, 14)
    assert(UTF8String.fromBytes(actualData1).toString === "2017-08-04")
    byteBuffer.rewind()

    val binaryWriter = ColumnWriter(fieldType, 0, isBinary = true, conf)
    binaryWriter.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check the result with binary mode
    assert(byteBuffer.getInt === 4)
    assert(byteBuffer.getInt === 6425)
  }

  test("timestamp") {
    val fieldType = StructType.fromDDL("a TIMESTAMP")(0)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2016-08-04 00:17:13")))
    val buf = new Array[Byte](128)
    val byteBuffer = ByteBuffer.wrap(buf)

    val textWriter = ColumnWriter(fieldType, 0, isBinary = false, conf)
    textWriter.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check the result with text mode
    assert(byteBuffer.getInt === 21)
    val actualData1 = buf.slice(4, 25)
    assert(UTF8String.fromBytes(actualData1).toString === "2016-08-04 00:17:13.0")
    byteBuffer.rewind()

    val binaryWriter = ColumnWriter(fieldType, 0, isBinary = true, conf)
    binaryWriter.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check the result with binary mode
    assert(byteBuffer.getInt === 8)
    assert(byteBuffer.getLong === 523585033000000L)
  }

  test("interval") {
    val fieldType = StructField("a", CalendarIntervalType)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, IntervalUtils.stringToInterval(UTF8String.fromString("1 month 3 days")))
    val buf = new Array[Byte](128)
    val byteBuffer = ByteBuffer.wrap(buf)

    val textWriter = ColumnWriter(fieldType, 0, isBinary = false, conf)
    textWriter.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check the result with text mode
    assert(byteBuffer.getInt === 15)
    val actualData1 = buf.slice(4, 19)
    assert(UTF8String.fromBytes(actualData1).toString === "1 months 3 days")
    byteBuffer.rewind()

    val binaryWriter = ColumnWriter(fieldType, 0, isBinary = true, conf)
    binaryWriter.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check the result with binary mode
    assert(byteBuffer.getInt === 1)
    assert(byteBuffer.getInt === 3)
    assert(byteBuffer.getLong === 0)
  }

  test("array") {
    val fieldType = StructType.fromDDL("a ARRAY<INT>")(0)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, ArrayData.toArrayData(Array(0, 1, 2, 3, 4)))
    val buf = new Array[Byte](15)
    val byteBuffer = ByteBuffer.wrap(buf)
    val writer = ColumnWriter(fieldType, 0, isBinary = false, conf)
    writer.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check data field size
    assert(byteBuffer.getInt === 11)

    // Check data itself
    val actualData = buf.slice(4, 15)
    assert(UTF8String.fromBytes(actualData).toString === "{0,1,2,3,4}")

    val errMsg = intercept[SQLException] {
      ColumnWriter(fieldType, 0, isBinary = true, conf)
    }.getMessage
    assert(errMsg.contains("Cannot convert value: type=ArrayType(IntegerType,true), isBinary=true"))
  }

  test("map") {
    val fieldType = StructType.fromDDL("a MAP<STRING, INT>")(0)
    val inputRow = new GenericInternalRow(1)
    val keys = ArrayData.toArrayData(Array("k1", "k2", "k3"))
    val values = ArrayData.toArrayData(Array(1, 2, 3))
    inputRow.update(0, new ArrayBasedMapData(keys, values))
    val buf = new Array[Byte](26)
    val byteBuffer = ByteBuffer.wrap(buf)
    val writer = ColumnWriter(fieldType, 0, isBinary = false, conf)
    writer.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check data field size
    assert(byteBuffer.getInt === 22)

    // Check data itself
    val actualData = buf.slice(4, 26)
    assert(UTF8String.fromBytes(actualData).toString ===  """{"k1":1,"k2":2,"k3":3}""")

    val errMsg = intercept[SQLException] {
      ColumnWriter(fieldType, 0, isBinary = true, conf)
    }.getMessage
    assert(errMsg.contains(
      "Cannot convert value: type=MapType(StringType,IntegerType,true), isBinary=true"))
  }

  test("struct") {
    val fieldType = StructType.fromDDL("a STRUCT<c0: INT, c1: STRING>")(0)
    val inputRow = new GenericInternalRow(1)
    val testData = new GenericInternalRow(2)
    testData.update(0, 7)
    testData.update(1, UTF8String.fromString("abc"))
    inputRow.update(0, new GenericInternalRow(Array[Any](testData)))
    val buf = new Array[Byte](23)
    val byteBuffer = ByteBuffer.wrap(buf)
    val writer = ColumnWriter(fieldType, 0, isBinary = false, conf)
    writer.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check data field size
    assert(byteBuffer.getInt === 19)

    // Check data itself
    val actualData = buf.slice(4, 23)
    assert(UTF8String.fromBytes(actualData).toString ===  """{"c0":7,"c1":"abc"}""")

    val errMsg = intercept[SQLException] {
      ColumnWriter(fieldType, 0, isBinary = true, conf)
    }.getMessage
    assert(errMsg.contains(
      "Cannot convert value: type=StructType(StructField(c0,IntegerType,true), " +
        "StructField(c1,StringType,true)), isBinary=true"))
  }

  test("udt") {
    val udt = new UDT.MyDenseVectorUDT()
    val vector = new UDT.MyDenseVector(Array(1.0, 3.0, 5.0, 7.0, 9.0))
    val data = udt.serialize(vector)
    val fieldType = StructField("a", udt)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, Literal(data, udt))
    val buf = new Array[Byte](25)
    val byteBuffer = ByteBuffer.wrap(buf)
    val writer = ColumnWriter(fieldType, 0, isBinary = false, conf)
    writer.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check data field size
    assert(byteBuffer.getInt === 21)

    // Check data itself
    val actualData = buf.slice(4, 25)
    assert(UTF8String.fromBytes(actualData).toString === "[1.0,3.0,5.0,7.0,9.0]")

    val errMsg = intercept[SQLException] {
      ColumnWriter(fieldType, 0, isBinary = true, conf)
    }.getMessage
    assert(errMsg.contains("Cannot convert value: type=" +
      "org.apache.spark.sql.server.service.postgresql.protocol.v3.UDT$MyDenseVectorUDT"))
  }
}
