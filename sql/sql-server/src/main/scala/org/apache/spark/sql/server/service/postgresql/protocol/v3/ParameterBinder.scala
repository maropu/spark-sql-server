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

import scala.util.matching.Regex


/**
 * A helper class that binds parameters using numbers like `$1`, `$2`, ...
 */
object ParameterBinder {

  val REF_RE = "\\$([1-9][0-9]*)".r

  def bind(input: String, params: Map[Int, String]): String = {
    if (input != null) {
      REF_RE.replaceAllIn(input, { m =>
        val refNumber = m.group(1).toInt
        params.get(refNumber).map(Regex.quoteReplacement).getOrElse {
          throw new IllegalArgumentException(s"A value of the param $refNumber does not exist")
        }
      })
    } else {
      input
    }
  }
}
