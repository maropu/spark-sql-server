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

  private val envVarNameForEnablingTests = "ENABLE_FAULT_TOLERANCE_TESTS"

  private val conf = new SparkConf()
  private val servers = mutable.ListBuffer[SparkPgSQLServerTest]()

  private var zkTestServer: TestingServer = _

  override protected def beforeAll() : Unit = {
    super.beforeAll()
    // TestingServer logs the port conflict exception rather than throwing an exception.
    // So we have to find a free port by ourselves. This approach cannot guarantee always starting
    // zkTestServer successfully because there is a time gap between finding a free port and
    // starting zkTestServer. But the failure possibility should be very low.
    //
    // TODO: In travis, `TestingServer` does not work well, so all the tests are testIfEnabledd now
    zkTestServer = new TestingServer(findFreePort(conf), new File("/tmp"))
  }

  override protected def afterAll() : Unit = {
    try {
      zkTestServer.stop()
    } finally {
      super.afterAll()
    }
  }

  override protected def afterEach() : Unit = {
    try {
      killAll()
    } finally {
      super.afterEach()
    }
  }

  lazy val shouldRunTests = sys.env.get(envVarNameForEnablingTests) == Some("1")

  /** Run the test if environment variable is set or ignore the test */
  def testIfEnabled(testName: String)(testBody: => Unit) {
    if (shouldRunTests) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var $envVarNameForEnablingTests=1]")(testBody)
    }
  }

  testIfEnabled("sanity-basic") {
    addServers(1)
    delay()
    assertValidClusterState()
  }

  testIfEnabled("sanity-many-masters") {
    addServers(3)
    delay()
    assertValidClusterState()
  }

  testIfEnabled("single-master-halt") {
    addServers(3)
    delay()
    assertValidClusterState()

    // Need to wait for a new leader
    killLeader()
    delay(180.seconds)
    assertValidClusterState()
  }

  testIfEnabled("single-master-restart") {
    addServers(1)
    delay()
    assertValidClusterState()

    killLeader()
    addServers(1)
    delay()
    assertValidClusterState()

    killLeader()
    addServers(1)
    delay()
    assertValidClusterState()
  }

  testIfEnabled("cluster-failure") {
    addServers(2)
    delay()
    assertValidClusterState()

    killAll()
    addServers(2)
    delay()
    assertValidClusterState()
  }

  testIfEnabled("rolling-outage") {
    addServers(1)
    delay()
    addServers(1)
    delay()
    addServers(1)
    delay()
    assertValidClusterState()
    assert(getLeader == servers.head)

    (1 to 3).foreach { i =>
      killLeader()
      delay(180.seconds)
      assertValidClusterState()
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
      val serv = new SparkPgSQLServerTest(
        name = s"fault-tolrance-test-${_servCounter}",
        pgVersion = "9.6",
        ssl = false,
        singleSession = false,
        incrementalCollect = true,
        options = serverOption)
      _servCounter += 1
      serv.start()
      servers += serv
    }
  }

  private def getLeader: SparkPgSQLServerTest = {
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

  private def delay(secs: Duration = 120.seconds) = Thread.sleep(secs.toMillis)

  /**
   * Asserts that the cluster is usable and that the expected masters and workers
   * are all alive in a proper configuration (e.g., only one leader).
   */
  private def assertValidClusterState(): Unit = {
    val (leader, slaves) = servers.partition { serv =>
      val bufferSrc = Source.fromFile(serv.logPath)
      Utils.tryWithSafeFinally {
        assert(bufferSrc.getLines().exists { line =>
          // Check if `TestingServer` works well
          line.contains("ZooKeeperLeaderElectionAgentAccessor: Starting ZooKeeper LeaderElection")
        })
        bufferSrc.getLines().exists(_.contains("I have been elected leader! New state:"))
      } {
        bufferSrc.close()
      }
    }
    logInfo(s"#leader=${leader.size} #slaves=${slaves.size}")
    assert(leader.size == 1)
  }

  private def findFreePort(conf: SparkConf): Int = {
    val candidatePort = RandomUtils.nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, conf)._2
  }
}
