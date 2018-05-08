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
import org.apache.spark.sql.internal.SQLConf


abstract class Service extends Logging {

  def init(conf: SQLConf): Unit
  def start(): Unit
  def stop(): Unit
}

abstract class CompositeService extends Service {

  private val services = new mutable.ArrayBuffer[Service]()

  protected def addService(service: Service): Unit = services += service

  // Initializes services in a bottom-up way
  final override def init(conf: SQLConf): Unit = {
    services.foreach(_.init(conf))
    doInit(conf)
  }

  // Starts services in a bottom-up way
  final override def start(): Unit = {
    services.foreach(_.start())
    doStart()
  }

  // Stops services in a top-down way
  final override def stop(): Unit = {
    doStop()
    services.foreach(_.stop())
  }

  def doInit(conf: SQLConf): Unit = {}
  def doStart(): Unit = {}
  def doStop(): Unit = {}
}
