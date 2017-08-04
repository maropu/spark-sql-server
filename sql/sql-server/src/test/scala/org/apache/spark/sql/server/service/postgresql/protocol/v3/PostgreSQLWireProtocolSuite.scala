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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class PostgreSQLWireProtocolSuite extends SparkFunSuite {

  test("DataRow") {
    val conf = new SQLConf()
    conf.setConfString("spark.sql.server.messageBufferSizeInBytes", "65536")
    val v3Protocol = new PostgreSQLWireProtocol(conf)
    val row = new GenericInternalRow(2)
    row.update(0, 8)
    row.update(1, UTF8String.fromString("abcdefghij"))
    val schema = StructType.fromDDL("a INT, b STRING")
    val rowConverters = PostgreSQLRowConverters(conf, schema, Seq(true, false))
    val data = v3Protocol.DataRow(row, rowConverters)
    val bytes = ByteBuffer.wrap(data)
    assert(bytes.get() === 'D'.toByte)
    assert(bytes.getInt === 28)
    assert(bytes.getShort === 2)
    assert(bytes.getInt === 4)
    assert(bytes.getInt === 8)
    assert(bytes.getInt === 10)
    assert(data.slice(19, 30) === "abcdefghij".getBytes(StandardCharsets.UTF_8))
  }

  test("Fails when message buffer overflowed") {
    val conf = new SQLConf()
    conf.setConfString("spark.sql.server.messageBufferSizeInBytes", "4")
    val v3Protocol = new PostgreSQLWireProtocol(conf)
    val row = new GenericInternalRow(1)
    row.update(0, UTF8String.fromString("abcdefghijk"))
    val schema = StructType.fromDDL("a STRING")
    val rowConverters = PostgreSQLRowConverters(conf, schema, Seq(false, false))
    val errMsg = intercept[SparkException] {
      v3Protocol.DataRow(row, rowConverters)
    }.getMessage
    assert(errMsg.contains(
      "Cannot generate a V3 protocol message because buffer is not enough for the message. " +
        "To avoid this exception, you might set higher value at " +
        "`spark.sql.server.messageBufferSizeInBytes`")
    )
  }
}
