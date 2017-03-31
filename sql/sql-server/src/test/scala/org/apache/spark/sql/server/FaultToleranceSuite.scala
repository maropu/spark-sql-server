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

import java.io.File
import java.net.ServerSocket

import scala.collection.mutable
import scala.concurrent.duration._
import scala.io.Source
import scala.util.control.NonFatal

import org.apache.commons.lang3.RandomUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.Utils


/** This suite tests the fault tolerance of the Spark SQL server. */
class FaultToleranceSuite extends SparkFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private val conf = new SparkConf()
  private val servers = mutable.ListBuffer[SparkPostgreSQLServerTest]()

  private var zkTestServer: TestingServer = _

  override protected def beforeAll() : scala.Unit = {
    super.beforeEach()
    // TestingServer logs the port conflict exception rather than throwing an exception.
    // So we have to find a free port by ourselves. This approach cannot guarantee always starting
    // zkTestServer successfully because there is a time gap between finding a free port and
    // starting zkTestServer. But the failure possibility should be very low.
    zkTestServer = new TestingServer(findFreePort(conf), new File("/tmp"))
  }

  override protected def afterAll() : scala.Unit = {
    try {
      zkTestServer.stop()
    } finally {
      super.afterAll()
    }
  }

  override protected def afterEach() : scala.Unit = {
    try {
      killAll()
    } finally {
      super.afterAll()
    }
  }

  test("sanity-basic") {
    addServers(1)
    delay(30.seconds)
    assertValidClusterState()
  }

  test("sanity-many-masters") {
    addServers(3)
    delay(30.seconds)
    assertValidClusterState()
  }

  test("single-master-halt") {
    addServers(3)
    delay(30.seconds)
    assertValidClusterState()

    // Need to wait for a new leader
    killLeader()
    delay(180.seconds)
    assertValidClusterState()
  }

  test("single-master-restart") {
    addServers(1)
    delay(30.seconds)
    assertValidClusterState()

    killLeader()
    addServers(1)
    delay(30.seconds)
    assertValidClusterState()

    killLeader()
    addServers(1)
    delay(30.seconds)
    assertValidClusterState()
  }

  test("cluster-failure") {
    addServers(2)
    delay(30.seconds)
    assertValidClusterState()

    killAll()
    addServers(2)
    delay(30.seconds)
    assertValidClusterState()
  }

  test("rolling-outage") {
    addServers(1)
    addServers(1)
    addServers(1)
    assert(getLeader == servers.head)

    (1 to 3).foreach { i =>
      killLeader()
      delay(180.seconds)
      assert(getLeader == servers.head)
      addServers(1)
    }
  }

  private var _servCounter: Int = 0

  private def addServers(num: Int): Unit = {
    val serverOption = Map(
      "spark.sql.server.recoveryMode" -> "ZOOKEEPER",
      "spark.deploy.zookeeper.url" -> zkTestServer.getConnectString
    )
    (1 to num).foreach { _ =>
      val serv = new SparkPostgreSQLServerTest(
        s"fault-tolrance-test-${_servCounter}", options = serverOption)
      _servCounter += 1
      serv.start()
      servers += serv
    }
  }

  private def getLeader: SparkPostgreSQLServerTest = {
    val leaders = servers.filter { serv =>
      val bufferSrc = Source.fromFile(serv.logPath)
      Utils.tryWithSafeFinally {
        bufferSrc.getLines().exists(_.contains("I have been elected leader! New state:"))
      } {
        bufferSrc.close()
      }
    }
    assert(leaders.size == 1)
    leaders(0)
  }

  private def killLeader(): Unit = {
    val leader = getLeader
    servers -= leader
    leader.stop()
  }

  private def killAll(): Unit = {
    servers.foreach { serv =>
      try { serv.stop() } catch { case NonFatal(_) => }
    }
    servers.clear()
  }

  private def delay(secs: Duration = 5.seconds) = Thread.sleep(secs.toMillis)

  /**
   * Asserts that the cluster is usable and that the expected masters and workers
   * are all alive in a proper configuration (e.g., only one leader).
   */
  private def assertValidClusterState(): Unit = { getLeader }

  private def findFreePort(conf: SparkConf): Int = {
    val candidatePort = RandomUtils.nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, conf)._2
  }
}
