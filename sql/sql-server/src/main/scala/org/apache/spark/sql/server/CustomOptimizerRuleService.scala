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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.CompositeService
import org.apache.spark.util.Utils


/** An entry point for custom optimizer rules. */
private[server] class CustomOptimizerRuleService extends CompositeService {

  override def doInit(conf: SQLConf): Unit = {
    CustomOptimizerRuleInitializer(SQLServerEnv.sqlContext)
  }
}

private[server] object CustomOptimizerRuleInitializer extends Logging {

  def apply(sqlContext: SQLContext): Unit = {
    val optRulesOption = sqlContext.conf.sqlServerExtraOptimizerRules
    optRulesOption.foreach { rules =>
      logInfo(s"Trying to install extra optimizer rules: $rules")
    }

    sqlContext.experimental.extraOptimizations ++= optRulesOption.map { rules =>
      rules.split(",").flatMap { ruleClassName =>
        val objName = ruleClassName + (if (!ruleClassName.endsWith("$")) "$" else "")
        try {
          val clazz = Utils.classForName(objName)
          val filed = clazz.getDeclaredField("MODULE$")
          val module = filed.get(null)
          Some(module.asInstanceOf[Rule[LogicalPlan]])
        } catch {
          case NonFatal(_) =>
            logWarning(s"Failed to install the rule: ${objName.replace("$", "")}")
            None
        }
      }.toSeq
    }.getOrElse(Nil)
  }
}
