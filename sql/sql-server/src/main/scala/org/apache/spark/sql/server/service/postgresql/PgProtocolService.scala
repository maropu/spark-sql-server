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

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.{LoggingHandler, LogLevel}

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.CompositeService
import org.apache.spark.sql.server.service.SessionService
import org.apache.spark.sql.server.service.postgresql.protocol.v3.PgV3MessageInitializer


private[service] class PgProtocolService(cli: SessionService) extends CompositeService {

  var port: Int = _
  var workerThreads: Int = _
  var msgHandlerInitializer: ChannelInitializer[SocketChannel] = _

  override def doInit(conf: SQLConf): Unit = {
    port = conf.sqlServerPort
    workerThreads = conf.sqlServerWorkerThreads
    msgHandlerInitializer = new PgV3MessageInitializer(cli, conf)
  }

  override def doStart(): Unit = {
    val bossGroup = new NioEventLoopGroup(1)
    val workerGroup = new NioEventLoopGroup(workerThreads)
    try {
      val b = new ServerBootstrap()
        // .option(ChannelOption.SO_KEEPALIVE, true)
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(msgHandlerInitializer)

      // Bind and start to accept incoming connections
      val f = b.bind(port).sync()

      // Blocked until the server socket is closed
      logInfo(s"Start running the SQL server (port=$port, workerThreads=$workerThreads)")
      f.channel().closeFuture().sync();
    } finally {
      bossGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
    }
  }
}
