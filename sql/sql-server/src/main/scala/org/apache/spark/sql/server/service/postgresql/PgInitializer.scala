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

package org.apache.spark.sql.server.service.postgresql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.server.SQLServerEnv
import org.apache.spark.sql.server.service.{CompositeService, SessionInitializer}


private[service] class PgCatalogInitializer extends CompositeService {

  override def doStart(): Unit = {
    // We must load system catalogs for the PostgreSQL v3 protocol before
    // `PgProtocolService.start`.
    PgMetadata.initSystemCatalogTables(SQLServerEnv.sqlContext)
  }
}

private[service] class PgSessionInitializer extends SessionInitializer {

  override def apply(dbName: String, sqlContext: SQLContext): Unit = {
    PgMetadata.initSystemFunctions(sqlContext)
    PgMetadata.initSessionCatalogTables(sqlContext, dbName)
  }
}
