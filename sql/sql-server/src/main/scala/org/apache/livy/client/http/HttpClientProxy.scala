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

package org.apache.livy.client.http

import java.net.URI
import java.util.Properties

import org.apache.livy.client.common.HttpMessages.SessionInfo
import org.apache.livyclient.common.CreateClientRequestWithProxyUser


// Workaround: For starting a Livy session with `proxyUser`
object HttpClientProxy {

  def start(uri: URI, proxyUser: String, params: Map[String, String]): Int = {
    val httpConf = new HttpConf(new Properties())
    val sessionConf = new java.util.HashMap[String, String]()
    params.foreach { case (key, value) =>
      sessionConf.put(key, value)
    }
    val create = new CreateClientRequestWithProxyUser(proxyUser, sessionConf)
    new LivyConnection(uri, httpConf).post(create, classOf[SessionInfo], "/").id
  }
}
