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

package org.apache.spark.sql.server.service

import java.sql.SQLException

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.server.catalyst.expressions.ParameterPlaceHolder

/**
 * A helper class that binds parameters using numbers like `$1`, `$2`, ...
 */
object ParamBinder {

  def bind(logicalPlan: LogicalPlan, params: Map[Int, Literal]): LogicalPlan = {
    val boundPlan = logicalPlan.transformAllExpressions {
      case ParameterPlaceHolder(id) if params.contains(id) =>
        params(id)
    }
    val unresolvedParams = boundPlan.flatMap { plan =>
      plan.expressions.flatMap { _.flatMap {
        case ParameterPlaceHolder(id) => Some(id)
        case _ => None
      }}
    }
    if (unresolvedParams.nonEmpty) {
      throw new SQLException("Unresolved parameters found: " +
        unresolvedParams.map(n => s"$$$n").mkString(", "))
    }
    boundPlan
  }
}
