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

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.UUID

import scala.sys.process._

import com.google.common.io.Files
import org.xerial.snappy.OSInfo

import org.apache.spark.sql.server.PostgreSQLJdbcTest
import org.apache.spark.util.Utils


class PostgreSQLV3ProtocolTest extends PostgreSQLJdbcTest {

  private val isOsSupported = Seq("Linux", "Mac").contains(OSInfo.getOSName)
  private val isArchSupported = Seq("x86_64").contains(OSInfo.getArchName)

  private val tempDirPath = Utils.createTempDir().getCanonicalPath
  private val cmdPath = {
    val resourcePath = s"pgproto/${OSInfo.getOSName}/${OSInfo.getArchName}/pgproto"
    val classLoader = Thread.currentThread().getContextClassLoader
    val _cmdPath = classLoader.getResource(resourcePath).getPath
    // Set a executable flag explicitly here
    new File(_cmdPath).setExecutable(true)
    _cmdPath
  }

  /** Run the test if os/arch is supported or ignore the test */
  def testIfSupported(testName: String)(testBody: => Unit) {
    if (isOsSupported && isArchSupported) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [not supported in env: " +
        s"os=${OSInfo.getOSName} arch=${OSInfo.getArchName}]")(testBody)
    }
  }

  def checkV3Protocol(messages: String, expected: String): Unit = {
    val msgDescriptionPath = s"$tempDirPath/${UUID.randomUUID().toString}.pgproto"
    val serverPort = serverInstance.listeningPort
    val command = s"$cmdPath -h localhost -d default -p $serverPort -f $msgDescriptionPath 2>&1"

    def normalize(s: String): String = s.trim.stripLineEnd.replaceAll("^\n", "")

    // Write a file containing messages in the temporary dir
    Files.write(normalize(messages), new File(msgDescriptionPath), StandardCharsets.UTF_8)

    val output = ("bash" :: "-c" :: command :: Nil).lineStream
    val actual = output.mkString("\n")
    assert(actual === normalize(expected))
  }
}
