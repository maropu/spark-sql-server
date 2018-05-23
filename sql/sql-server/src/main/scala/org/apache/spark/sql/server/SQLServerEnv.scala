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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.ui.SQLServerTab
import org.apache.spark.util.Utils


object SQLServerEnv extends Logging {

  // For test use
  private var _sqlContext: Option[SQLContext] = None

  @DeveloperApi
  def withSQLContext(sqlContext: SQLContext): Unit = {
    require(sqlContext != null)
    _sqlContext = Option(sqlContext)
    sqlServListener
    uiTab
  }

  private def mergeSparkConf(sqlConf: SQLConf, sparkConf: SparkConf): Unit = {
    sparkConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
    }
  }

  lazy val sparkConf: SparkConf = _sqlContext.map(_.sparkContext.conf).getOrElse {
    val sparkConf = new SparkConf(loadDefaults = true)

    // If user doesn't specify the appName, we want to get [SparkSQL::localHostName]
    // instead of the default appName [SQLServer].
    val maybeAppName = sparkConf
      .getOption("spark.app.name")
      .filterNot(_ == classOf[SQLServer].getName)
    sparkConf
      .setAppName(maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))
      .set("spark.sql.crossJoin.enabled", "true")
  }

  lazy val sqlConf: SQLConf = _sqlContext.map(_.conf).getOrElse {
    val newSqlConf = new SQLConf()
    mergeSparkConf(newSqlConf, sparkConf)
    newSqlConf
  }

  lazy val sqlContext: SQLContext = _sqlContext.getOrElse(newSQLContext())
  lazy val sparkContext: SparkContext = sqlContext.sparkContext
  lazy val sqlServListener: SQLServerListener = newSQLServerListener(sqlContext)
  lazy val uiTab: Option[SQLServerTab] = newUiTab(sqlContext, sqlServListener)

  def newSQLContext(): SQLContext = {
    SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate().sqlContext
  }
  def newSQLServerListener(sqlContext: SQLContext): SQLServerListener = {
    val listener = new SQLServerListener(sqlContext.conf)
    sqlContext.sparkContext.addSparkListener(listener)
    listener
  }
  def newUiTab(sqlContext: SQLContext, listener: SQLServerListener): Option[SQLServerTab] = {
    sqlContext.sparkContext.conf.getBoolean("spark.ui.enabled", true) match {
      case true => Some(SQLServerTab(SQLServerEnv.sqlContext.sparkContext, listener))
      case _ => None
    }
  }
}
