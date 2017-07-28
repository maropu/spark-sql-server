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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.postgresql.PostgreSQLParser
import org.apache.spark.util.Utils


private[server] object SQLServerEnv extends Logging {

  private val nextSessionId = new AtomicInteger(0)

  private var _sqlContext: Option[SQLContext] = None

  lazy val sparkConf = _sqlContext.map { ctx =>
    ctx.sparkContext.conf
  }.getOrElse {
    val sparkConf = new SparkConf(loadDefaults = true)

    // If user doesn't specify the appName, we want to get [SparkSQL::localHostName]
    // instead of the default appName [SQLServer].
    val maybeAppName = sparkConf
      .getOption("spark.app.name")
      .filterNot(_ == classOf[SQLServer].getName)
    sparkConf.setAppName(
      maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))

    // Set `true` at `spark.sql.crossJoin.enabled`
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    logInfo("Explicitly set `true` at `spark.sql.crossJoin.enabled` because PostgreSQL " +
      "JDBC drivers handle metadata by using SQL queries with cross joins.")

    sparkConf
  }

  lazy val sparkContext = sqlContext.sparkContext

  lazy val sqlContext = _sqlContext.getOrElse {
    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.sqlContext
  }

  lazy val sqlConf = sqlContext.conf

  lazy val sqlParser = new PostgreSQLParser(sqlConf)

  lazy val serverVersion = {
    // scalastyle:off
    // `server_version` decides how to handle metadata between jdbc clients and servers.
    // See an URL below for valid version numbers:
    // https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/core/ServerVersion.java
    // scalastyle:on
    val validNumers = Seq("7.4", "8.0", "9.6")
    validNumers.find(_ == sparkConf.sqlServerVersion).getOrElse {
      throw new IllegalArgumentException(
        s"You need to select a server version from $validNumers, but got " +
          sparkConf.sqlServerVersion)
    }
  }

  def withSQLContext(sqlContext: SQLContext): Unit = {
    require(sqlContext != null)
    require(_sqlContext.isEmpty)
    _sqlContext = Option(sqlContext)
  }

  def newSessionId(): Int = nextSessionId.getAndIncrement()

  def cleanup() {
    _sqlContext.map(_.sparkContext.stop())
    _sqlContext = None
  }
}
