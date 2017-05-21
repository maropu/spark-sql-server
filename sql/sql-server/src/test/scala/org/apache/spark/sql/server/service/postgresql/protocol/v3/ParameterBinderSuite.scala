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

import org.apache.spark.SparkFunSuite

class ParameterBinderSuite extends SparkFunSuite {

  test("bind parameters") {
    assert("SELECT * FROM t WHERE a = 18" ===
      ParameterBinder.bind("SELECT * FROM t WHERE a = $1", Map(1 -> "18")))
    assert("SELECT * FROM t WHERE a = 42" ===
      ParameterBinder.bind("SELECT * FROM t WHERE a = $300", Map(300 -> "42")))
    assert("SELECT * FROM t WHERE a = -1 AND b = 8" ===
      ParameterBinder.bind("SELECT * FROM t WHERE a = $1 AND b = $2", Map(1 -> "-1", 2 -> "8")))

    val e = intercept[IllegalArgumentException] {
      ParameterBinder.bind("SELECT * FROM t WHERE a = $1", Map.empty)
    }
    assert(e.getMessage === "A value of the param 1 does not exist")
  }
}
