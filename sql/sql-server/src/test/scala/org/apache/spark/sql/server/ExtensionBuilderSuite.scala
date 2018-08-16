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

import java.net.URL

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.util.Utils

class ExtensionBuilderSuite extends SparkFunSuite with BeforeAndAfterAll {

  var _sqlContext: SQLContext = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf(loadDefaults = true)
      .setMaster("local[1]")
      .setAppName("spark-sql-server-test")
      .set("spark.sql.server.extensions.builder", "org.apache.spark.ExtensionBuilderExample")
    _sqlContext = SQLServerEnv.newSQLContext(conf)
  }

  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_sqlContext != null) {
          _sqlContext.sparkContext.stop()
          _sqlContext = null
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  // TODO: This method works only in Java8
  private def addJarInClassPath(jarURLString: String): Unit = {
    // val cl = ClassLoader.getSystemClassLoader
    val cl = Utils.getSparkClassLoader
    val clazz = cl.getClass
    val method = clazz.getSuperclass.getDeclaredMethod("addURL", Seq(classOf[URL]): _*)
    method.setAccessible(true)
    method.invoke(cl, Seq[Object](new URL(jarURLString)): _*)
  }

  test("extensions") {
    // First, adds a jar for an extension builder
    val jarPath = "src/test/resources/extensions_2.11_2.3.1_0.1.7-spark2.3-SNAPSHOT.jar"
    val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"
    // sqlContext.sparkContext.addJar(jarURL)
    addJarInClassPath(jarURL)
    val rules = Seq("org.apache.spark.examples.EmptyRule1", "org.apache.spark.examples.EmptyRule2")

    val optimizerRuleNames = _sqlContext.sessionState.optimizer.batches
      .flatMap(_.rules.map(_.ruleName))
    rules.foreach { expectedRuleName =>
      assert(optimizerRuleNames.contains(expectedRuleName))
    }
  }
}
