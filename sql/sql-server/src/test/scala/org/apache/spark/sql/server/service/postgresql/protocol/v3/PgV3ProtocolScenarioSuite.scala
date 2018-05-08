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


class PgV3ProtocolScenarioSuite extends PgV3ProtocolTest {

  testIfSupported("simple query") {
    checkV3Protocol(
      messages = s"""
         |'Q' "SELECT * FROM VALUES (1, 1), (2, 2);"
         |'Y'
       """.stripMargin,
      expected = s"""
         |FE=> Query(query="SELECT * FROM VALUES (1, 1), (2, 2);")
         |<= BE RowDescription
         |<= BE DataRow
         |<= BE DataRow
         |<= BE CommandComplete(SELECT 2)
         |<= BE ReadyForQuery(I)
       """.stripMargin
    )
  }

  testIfSupported("unsupported SQL strings") {
    def expectedErrorMessage(message: String, cmd: String): Seq[String] =
      Seq(s"Exception detected in '$message'", s"Operation not allowed: $cmd")

    Seq("COMMIT", "ROLLBACK").foreach { cmd =>
      val e1 = intercept[Exception] {
        checkV3Protocol(
          messages = s"""
             |'Q' "$cmd;"
             |'Y'
           """.stripMargin,
          expected = ""
        )
      }.getMessage
      assert(expectedErrorMessage("Query", cmd).forall(e1.contains))

      val e2 = intercept[Exception] {
        checkV3Protocol(
          messages = s"""
             |'P' "s1" "$cmd" 0
             |'Y'
           """.stripMargin,
          expected = ""
        )
      }.getMessage
      assert(expectedErrorMessage("Parse", cmd).forall(e2.contains))
    }
  }

  testIfSupported("extended query") {
    checkV3Protocol(
      messages = s"""
         |'P' "s1" "SELECT * FROM VALUES (1, 1), (2, 2) t(a, b) WHERE a = $$1" 1 23
         |'B' "" "s1" 1 1 1 4 "1" 2 23 23
         |'E' "" 0
         |'S'
         |'Y'
       """.stripMargin,
      // scalastyle:off line.size.limit
      expected = s"""
         |FE=> Parse(stmt="s1", query="SELECT * FROM VALUES (1, 1), (2, 2) t(a, b) WHERE a = $$1"), oids={23}
         |FE=> Bind(stmt="s1", portal="")
         |FE=> Execute(portal="")
         |FE=> Sync
         |<= BE ParseComplete
         |<= BE BindComplete
         |<= BE DataRow
         |<= BE CommandComplete(SELECT 1)
         |<= BE ReadyForQuery(I)
       """.stripMargin
      // scalastyle:off line.size.limit
    )
  }
}
