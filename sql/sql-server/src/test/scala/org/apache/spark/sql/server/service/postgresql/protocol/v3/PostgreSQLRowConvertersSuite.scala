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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.{NullType, StructField}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class PostgreSQLRowConvertersSuite extends SparkFunSuite {

  test("primitive types") {
    Seq(
      (false, "BOOLEAN", (b: Array[Byte]) => b(0) == 1),
      (13.toByte, "BYTE", (b: Array[Byte]) => b(0)),
      (2392.toShort, "SHORT", (b: Array[Byte]) => ByteBuffer.wrap(b).getShort),
      (813, "INT", (b: Array[Byte]) => ByteBuffer.wrap(b).getInt),
      (18923L, "LONG", (b: Array[Byte]) => ByteBuffer.wrap(b).getLong),
      (1.0f, "FLOAT", (b: Array[Byte]) => ByteBuffer.wrap(b).getFloat),
      (8.0, "DOUBLE", (b: Array[Byte]) => ByteBuffer.wrap(b).getDouble)
    ).foreach { case (data, tpe, readData) =>
      Seq(true, false).foreach { binaryMode =>
        val fieldType = StructType.fromDDL(s"a $tpe")(0)
        val writer = ColumnWriter(fieldType, 0, isBinary = binaryMode)
        val inputRow = new GenericInternalRow(1)
        inputRow.update(0, data)
        val dataSize = if (binaryMode) fieldType.dataType.defaultSize else data.toString.length
        val buf = new Array[Byte](4 + dataSize)
        val byteBuffer = ByteBuffer.wrap(buf)
        writer.write(inputRow, byteBuffer)
        byteBuffer.rewind()

        // Check data field size
        assert(byteBuffer.getInt === dataSize, s"type=$tpe isBinary=$binaryMode")

        // Check data itself
        val slicedBytes = buf.slice(4, 4 + dataSize)
        val actualData = if (binaryMode) readData(slicedBytes) else slicedBytes
        val expectedData = if (binaryMode) data else data.toString.getBytes(StandardCharsets.UTF_8)
        assert(actualData === expectedData, s"type=$tpe isBinary=$binaryMode")
      }
    }
  }

  test("null") {
    val fieldType = StructField("a", NullType)
    val writer = ColumnWriter(fieldType, 0, isBinary = false)
    val byteBuffer = ByteBuffer.allocate(1)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, 0)
    val errMsg = intercept[UnsupportedOperationException] {
      writer.write(inputRow, byteBuffer)
    }.getMessage
    assert(errMsg.contains("Invalid call to nullSafeWriter on NullColumnWriter"))
  }

  test("decimal") {
    val fieldType = StructType.fromDDL("a DECIMAL")(0)
    val writer = ColumnWriter(fieldType, 0, isBinary = false)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, BigDecimal.decimal(3.0))
    val buf = new Array[Byte](7)
    val byteBuffer = ByteBuffer.wrap(buf)
    writer.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check data field size
    assert(byteBuffer.getInt === 3)

    // Check data itself
    val actualData = buf.slice(4, 7)
    assert(actualData === "3.0".getBytes(StandardCharsets.UTF_8))

    val errMsg = intercept[UnsupportedOperationException] {
      ColumnWriter(fieldType, 0, isBinary = true)
    }.getMessage
    assert(errMsg.contains("Cannot convert value: type=DecimalType(10,0), isBinary=true"))
  }

  test("array") {
    val fieldType = StructType.fromDDL("a ARRAY<INT>")(0)
    val writer = ColumnWriter(fieldType, 0, isBinary = false)
    val inputRow = new GenericInternalRow(1)
    inputRow.update(0, ArrayData.toArrayData(Array(0, 1, 2, 3, 4)))
    val buf = new Array[Byte](15)
    val byteBuffer = ByteBuffer.wrap(buf)
    writer.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check data field size
    assert(byteBuffer.getInt === 11)

    // Check data itself
    val actualData = buf.slice(4, 15)
    assert(actualData === "{0,1,2,3,4}".getBytes(StandardCharsets.UTF_8))

    val errMsg = intercept[UnsupportedOperationException] {
      ColumnWriter(fieldType, 0, isBinary = true)
    }.getMessage
    assert(errMsg.contains("Cannot convert value: type=ArrayType(IntegerType,true), isBinary=true"))
  }

  test("map") {
    val fieldType = StructType.fromDDL("a MAP<STRING, INT>")(0)
    val writer = ColumnWriter(fieldType, 0, isBinary = false)
    val inputRow = new GenericInternalRow(1)
    val keys = ArrayData.toArrayData(Array("k1", "k2", "k3"))
    val values = ArrayData.toArrayData(Array(1, 2, 3))
    inputRow.update(0, new ArrayBasedMapData(keys, values))
    val buf = new Array[Byte](26)
    val byteBuffer = ByteBuffer.wrap(buf)
    writer.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check data field size
    assert(byteBuffer.getInt === 22)

    // Check data itself
    val actualData = buf.slice(4, 26)
    assert(actualData ===  """{"k1":1,"k2":2,"k3":3}""".getBytes(StandardCharsets.UTF_8))

    val errMsg = intercept[UnsupportedOperationException] {
      ColumnWriter(fieldType, 0, isBinary = true)
    }.getMessage
    assert(errMsg.contains(
      "Cannot convert value: type=MapType(StringType,IntegerType,true), isBinary=true"))
  }

  test("struct") {
    val fieldType = StructType.fromDDL("a STRUCT<c0: INT, c1: STRING>")(0)
    val writer = ColumnWriter(fieldType, 0, isBinary = false)
    val inputRow = new GenericInternalRow(1)
    val testData = new GenericInternalRow(2)
    testData.update(0, 7)
    testData.update(1, UTF8String.fromString("abc"))
    inputRow.update(0, new GenericInternalRow(Array[Any](testData)))
    val buf = new Array[Byte](23)
    val byteBuffer = ByteBuffer.wrap(buf)
    writer.write(inputRow, byteBuffer)
    byteBuffer.rewind()

    // Check data field size
    assert(byteBuffer.getInt === 19)

    // Check data itself
    val actualData = buf.slice(4, 23)
    assert(actualData ===  """{"c0":7,"c1":"abc"}""".getBytes(StandardCharsets.UTF_8))

    val errMsg = intercept[UnsupportedOperationException] {
      ColumnWriter(fieldType, 0, isBinary = true)
    }.getMessage
    assert(errMsg.contains(
      "Cannot convert value: type=StructType(StructField(c0,IntegerType,true), " +
        "StructField(c1,StringType,true)), isBinary=true"))
  }
}
