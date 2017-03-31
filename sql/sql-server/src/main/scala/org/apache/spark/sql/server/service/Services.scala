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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext

private[server] abstract class Service(name: String) extends Logging {

  /** Initialize the service. */
  def init(sqlContext: SQLContext): Unit

  /** Start the service. */
  def start(): Unit

  /** Stop the service. */
  def stop(): Unit

}

private[server] class CompositeService(name: String = getClass().getSimpleName)
    extends Service(name) {

  private val services = new mutable.ArrayBuffer[Service]()

  protected[this] def addService(service: Service): Unit = services += service

  override def init(sqlContext: SQLContext): Unit = services.foreach(_.init(sqlContext))

  override def start(): Unit = services.foreach(_.start())

  override def stop(): Unit = services.foreach(_.stop())

}
