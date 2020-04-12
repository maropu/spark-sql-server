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

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.{LoggingHandler, LogLevel}

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf._

private[service] abstract class FrontendService extends CompositeService {

  var port: Int = _
  var workerThreads: Int = _
  var bossGroup: NioEventLoopGroup = _
  var workerGroup: NioEventLoopGroup = _

  def messageHandler: ChannelInitializer[SocketChannel]

  override def doInit(conf: SQLConf): Unit = {
    port = conf.sqlServerPort
    workerThreads = conf.sqlServerWorkerThreads
    bossGroup = new NioEventLoopGroup(1)
    workerGroup = new NioEventLoopGroup(workerThreads)
  }

  override def doStart(): Unit = {
    try {
      val b = new ServerBootstrap()
        // .option(ChannelOption.SO_KEEPALIVE, true)
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(messageHandler)

      // Binds and starts to accept incoming connections
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
