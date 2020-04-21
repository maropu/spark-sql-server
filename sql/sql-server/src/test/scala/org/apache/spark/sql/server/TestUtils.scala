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

package org.apache.spark.sql.server

import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.unsafe.types.UTF8String

object TestUtils {

  def toSparkIntervalString(pgIntervalString: String): String = {
    // See: org.apache.spark.sql.catalyst.util.IntervalUtils.IntervalUnit
    val sparkIntervalString = UTF8String.fromString(pgIntervalString
      .replace("years", "year")
      .replace("mons", "month")
      .replace("days", "day")
      .replace("hours", "hour")
      .replace("mins", "minute")
      .replace("secs", "second")
    )
    IntervalUtils.stringToInterval(sparkIntervalString).toString
  }
}
