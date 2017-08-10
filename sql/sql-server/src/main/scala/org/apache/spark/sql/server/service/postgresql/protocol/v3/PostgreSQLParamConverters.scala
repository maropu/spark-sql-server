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
import java.sql.SQLException

import org.apache.spark.sql.server.service.postgresql.Metadata._


/**
 * A [[PostgreSQLParamConverters]] is used to convert binary in the [[Bind]] message
 * into strings to bind them in a prepared statement.
 */
object PostgreSQLParamConverters {

  def apply(params: Seq[Array[Byte]], types: Seq[Int], formats: Seq[Int]): Seq[(Int, String)] = {
    params.zipWithIndex.map { case (param, i) =>
      val value = (types(i), formats(i)) match {
        case (PgUnspecifiedType.oid, format) =>
          throw new SQLException(s"Unspecified type unsupported: format=$format")
        case (_, 0) =>
          // If text mode given, prints input as a single-quoted string
          s"'${new String(param, StandardCharsets.UTF_8)}'"
        case (PgBoolType.oid, 1) =>
          // '1' (49) means true; otherwise false
          if (param(0) == 49) "true" else "false"
        case (PgNumericType.oid, 1) =>
          new String(param, StandardCharsets.UTF_8)
        case (PgInt2Type.oid, 1) =>
          ByteBuffer.wrap(param).getShort
        case (PgInt4Type.oid, 1) =>
          ByteBuffer.wrap(param).getInt
        case (PgInt8Type.oid, 1) =>
          ByteBuffer.wrap(param).getLong
        case (PgFloat4Type.oid, 1) =>
          ByteBuffer.wrap(param).getFloat
        case (PgFloat8Type.oid, 1) =>
          ByteBuffer.wrap(param).getDouble
        // TODO: Need to support other types, e.g., `Date` and `Timestamp`
        case (paramId, format) =>
          throw new SQLException(s"Cannot bind param: paramId=$paramId, format=$format")
      }
      (i + 1) -> value.toString
    }
  }
}
