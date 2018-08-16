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

package org.apache.spark

import org.apache.spark.catalyst.{EmptyRule1, EmptyRule2}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.util.Utils

object ExtensionBuilderExample extends (SparkSessionExtensions => Unit) with Logging {

  override def apply(f: SparkSessionExtensions): Unit = {
    // f.injectPostHocResolutionRule(...)
    // f.injectOptimizerRule(...)
    logWarning(s"${Utils.getFormattedClassName(this)} called to inject user-defined extensions")
    f.injectOptimizerRule(_ => EmptyRule1)
    f.injectOptimizerRule(_ => EmptyRule2)
  }
}
