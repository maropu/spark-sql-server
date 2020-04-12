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

import java.sql.SQLException

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.server.catalyst.expressions.ParameterPlaceHolder
import org.apache.spark.sql.server.service.ParamBinder
import org.apache.spark.sql.types._

class ParameterBinderSuite extends PlanTest {

  test("bind parameters") {
    val c0 = 'a.int
    val c1 = 'b.int
    val r1 = LocalRelation(c0, c1)

    val param1 = Literal(18, IntegerType)
    val lp1 = Filter(EqualTo(c0, ParameterPlaceHolder(1)), r1)
    val expected1 = Filter(EqualTo(c0, param1), r1)
    comparePlans(expected1, ParamBinder.bind(lp1, Map(1 -> param1)))

    val param2 = Literal(42, IntegerType)
    val lp2 = Filter(EqualTo(c0, ParameterPlaceHolder(300)), r1)
    val expected2 = Filter(EqualTo(c0, param2), r1)
    comparePlans(expected2, ParamBinder.bind(lp2, Map(300 -> param2)))

    val param3 = Literal(-1, IntegerType)
    val param4 = Literal(48, IntegerType)
    val lp3 = Filter(
      And(
        EqualTo(c0, ParameterPlaceHolder(1)),
        EqualTo(c1, ParameterPlaceHolder(2))
      ), r1)
    val expected3 = Filter(
      And(
        EqualTo(c0, param3),
        EqualTo(c1, param4)
      ), r1)
    comparePlans(expected3, ParamBinder.bind(lp3, Map(1 -> param3, 2 -> param4)))

    val errMsg1 = intercept[SQLException] {
      ParamBinder.bind(lp1, Map.empty)
    }.getMessage
    assert(errMsg1 == "Unresolved parameters found: $1")
    val errMsg2 = intercept[SQLException] {
      ParamBinder.bind(lp2, Map.empty)
    }.getMessage
    assert(errMsg2 == "Unresolved parameters found: $300")
    val errMsg3 = intercept[SQLException] {
      ParamBinder.bind(lp3, Map.empty)
    }.getMessage
    assert(errMsg3 == "Unresolved parameters found: $1, $2")
  }
}
