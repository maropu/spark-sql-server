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

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.{KeyStore, PrivilegedExceptionAction}
import java.sql.SQLException
import java.util.{HashMap => jHashMap}
import java.util.Collections.{synchronizedMap => jSyncMap}
import javax.net.ssl.{KeyManagerFactory, SSLContext}

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}
import io.netty.handler.ssl.{SslContext, SslHandler}
import io.netty.handler.ssl.util.SelfSignedCertificate
import org.apache.hadoop.security.UserGroupInformation
import org.ietf.jgss.{GSSContext, GSSCredential, GSSException, GSSManager, Oid}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.{SQLServerConf, SQLServerEnv}
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.{BEGIN, FETCH, Operation, SELECT, SessionService, SessionState}
import org.apache.spark.sql.server.service.postgresql.PgMetadata._
import org.apache.spark.sql.server.service.postgresql.PgParser
import org.apache.spark.sql.server.service.postgresql.execution.command.BeginCommand
import org.apache.spark.sql.server.service.postgresql.protocol.v3.PgRowConverters._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils._


/**
 * This is the implementation of the PostgreSQL V3 client/server protocol.
 * The V3 protocol is used in PostgreSQL 7.4 and later.
 * A specification of the V3 protocol can be found in an URL:
 *
 * https://www.postgresql.org/docs/current/static/protocol.html
 */
case class PgWireProtocol(bufferSizeInBytes: Int) {

  private val messageBuffer = new Array[Byte](bufferSizeInBytes)
  private val messageWriter = ByteBuffer.wrap(messageBuffer)

  private def withMessageBuffer(f: ByteBuffer => Int): Array[Byte] = {
    try {
      val messageLen = f(messageWriter)
      messageBuffer.slice(0, messageLen)
    } catch {
      case NonFatal(e) =>
        throw new SQLException(
          "Cannot generate a V3 protocol message because buffer is not enough for the message. " +
            s"To avoid this exception, you might set higher value at " +
            s"`${SQLServerConf.SQLSERVER_MESSAGE_BUFFER_SIZE_IN_BYTES.key}`")
    } finally {
      messageWriter.rewind()
    }
  }

  /**
   * Response messages sent back to clients (Parameterized messages only).
   */

  /**
   * An ASCII code 'R' is an identifier of authentication messages.
   * If we receive the `StartupMessage` message from a client and we have no failure,
   * we send one of these messages for user's verification.
   */
  def AuthenticationGSSContinue(token: Array[Byte]): Array[Byte] = {
    withMessageBuffer { buf =>
      buf.put('R'.toByte).putInt(8 + token.size).putInt(8).put(token)
      9 + token.size
    }
  }

  /**
   * An ASCII code 'K' is an identifier of this [[BackendKeyData]] message.
   * If we receive the `CancelRequest` message from a client and we have no failure,
   * we send this message to the client.
   */
  def BackendKeyData(channelId: Int, secretKey: Int): Array[Byte] = {
    withMessageBuffer { buf =>
      buf.put('K'.toByte).putInt(12).putInt(channelId).putInt(secretKey)
      13
    }
  }

  /**
   * An ASCII code 'C' is an identifier of this [[CommandComplete]] message.
   * If any command request (e.g., `Query` and `Execute`) is finished successfully,
   * we send this message to the client.
   */
  def CommandComplete(tag: String): Array[Byte] = {
    withMessageBuffer { buf =>
      buf.put('C'.toByte)
        .putInt(5 + tag.length)
        .put(tag.getBytes(StandardCharsets.UTF_8))
        .put(0.toByte)

      6 + tag.length
    }
  }

  /**
   * An ASCII code 'D' is an identifier of this [[DataRow]] message.
   * If any command request (e.g., `Query` and `Execute`) is finished successfully and we have
   * result rows, we send the results as the [[DataRow]]s.
   */
  def DataRow(row: InternalRow, rowWriter: RowWriter): Array[Byte] = {
    withMessageBuffer { buf =>
      buf.position(7)
      val rowLength = rowWriter(row, buf)
      buf.rewind()
      buf.put('D'.toByte).putInt(6 + rowLength).putShort(row.numFields.toShort)
      7 + rowLength
    }
  }

  /**
   * An ASCII code 'V' is an identifier of this [[FunctionCallResponse]] message.
   * If we receive the `FunctionCall` message from a client and we have no failure,
   * we send this message to the client.
   */
  def FunctionCallResponse(result: Array[Byte]): Array[Byte] = {
    withMessageBuffer { buf =>
      buf.put('V'.toByte).putInt(8 + result.size).putInt(result.size).put(result)
      9 + result.size
    }
  }

  def ParameterStatus(key: String, value: String): Array[Byte] = {
    withMessageBuffer { buf =>
      val paramLen = key.length + value.length
      buf.put('S'.toByte)
        .putInt(6 + paramLen)
        .put(key.getBytes(StandardCharsets.UTF_8)).put(0.toByte)
        .put(value.getBytes(StandardCharsets.UTF_8)).put(0.toByte)

      7 + paramLen
    }
  }

  /**
   * An ASCII code 'T' is an identifier of this [[RowDescription]] message.
   * If we receive the `Describe` message from a client and we have no failure,
   * we send this message to the client.
   */
  def RowDescription(schema: StructType): Array[Byte] = {
    withMessageBuffer { buf =>
      if (schema.size == 0) {
        buf.put('T'.toByte).putInt(6).putShort(0)
        7
      } else {
        val length = 6 + schema.map(_.name.length + 19).reduce(_ + _)
        buf.put('T'.toByte).putInt(length).putShort(schema.size.toShort)
        // Each column has length(field.name) + 19 bytes
        schema.toSeq.zipWithIndex.map { case (field, index) =>
          val sparkType = field.dataType
          val pgType = getPgType(sparkType)
          val mode = PgWireProtocol.binaryFormatTypes.find(_ == sparkType).map(_ => 1).getOrElse(0)
          buf.put(field.name.getBytes(StandardCharsets.UTF_8)).put(0.toByte) // field name
            .putInt(0)                        // object ID of the table
            .putShort((index + 1).toShort)    // attribute number of the column
            .putInt(pgType.oid)               // object ID of the field's data type
            .putShort(pgType.len.toShort)     // data type size
            .putInt(0)                        // type modifier
            .putShort(mode.toShort)           // 1 for binary; otherwise 0
        }
        1 + length
      }
    }
  }
}

object PgWireProtocol extends Logging {

  // An identifier for `StartupMessage`
  val V3_PROTOCOL_VERSION: Int = 196608

  // An identifier for `SSLRequest`
  val SSL_REQUEST_CODE: Int = 80877103

  // An identifier for `CancelRequest`
  val CANCEL_REQUEST_CODE: Int = 80877102

  // A type list for binary formats
  val binaryFormatTypes: Seq[AbstractDataType] = Seq(
    BinaryType, ShortType, IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType
  )

  private def formatsInSimpleQueryMode(schema: StructType): Seq[Boolean] = {
    Seq.fill(schema.length)(false)
  }

  private def formatsInExtendedQueryMode(schema: StructType): Seq[Boolean] = {
    schema.map { f => binaryFormatTypes.contains(f.dataType) }
  }

  /**
   * A string in messages is a null-terminated one (C-style string) and there is no specific
   * length limitation on strings.
   */
  private def extractString(msg: ByteBuffer): String = {
    val origPos = msg.position()
    var len = 0
    // Search null from a current position
    while (msg.hasRemaining() && msg.get() != 0.toByte) {
      len += 1
    }
    if (len != 0) {
      val localBuf = new Array[Byte](len)
      msg.position(origPos)
      msg.get(localBuf, 0, len)
      msg.get()
      new String(localBuf, StandardCharsets.UTF_8)
    } else {
      ""
    }
  }

  // An URL string in PostgreSQL JDBC drivers is something like
  // "jdbc:postgresql://[host]/[database]?user=[name]&kerberosServerName=spark"
  private def doGSSAuthentication(
      ctx: ChannelHandlerContext,
      state: SessionV3State,
      serverPrincipal: String,
      token: Array[Byte]): Boolean = {
    UserGroupInformation.getCurrentUser()
        .doAs(new PrivilegedExceptionAction[Boolean] {

      override def run(): Boolean = {
        import state.v3Protocol.AuthenticationGSSContinue

        // Get own Kerberos credentials for accepting connection
        val manager = GSSManager.getInstance()
        var gssContext: GSSContext = null
        try {
          // This Oid for Kerberos GSS-API mechanism
          val kerberosMechOid = new Oid("1.2.840.113554.1.2.2")
          // Oid for kerberos principal name
          val krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1")

          val serverName = manager.createName(serverPrincipal, krb5PrincipalOid)
          val serverCreds = manager.createCredential(
            serverName, GSSCredential.DEFAULT_LIFETIME, kerberosMechOid,
            GSSCredential.ACCEPT_ONLY)

          gssContext = manager.createContext(serverCreds)

          val outToken = gssContext.acceptSecContext(token, 0, token.length)
          if (!gssContext.isEstablished) {
            ctx.write(AuthenticationGSSContinue(outToken))
            ctx.flush()
            false
          } else {
            true
          }
        } catch {
          case e: GSSException => throw e
        } finally {
          if (gssContext != null) {
            try {
              gssContext.dispose()
            } catch {
              case NonFatal(_) => // No-op
            }
          }
        }
      }
    })
  }

  // TODO: Needs to change `Any` into `Unit`
  private type MessageProcessorType = (ChannelHandlerContext, Int, SessionV3State) => Any

  /**
   * Internal registry of client message processors.
   */
  private val messageProcessors: Map[Int, ByteBuffer => (String, MessageProcessorType)] = Map(
    // An ASCII code of the `Bind` message is 'B'(66)
    66 -> { msg =>
      val portalName = extractString(msg)
      val queryName = extractString(msg)
      val numFormats = msg.getShort()
      val formats = if (numFormats > 0) {
        val arrayBuf = new Array[Int](numFormats)
        (0 until numFormats).foreach(i => arrayBuf(i) = msg.getShort())
        arrayBuf.toSeq
      } else {
        Seq.empty[Int]
      }
      val numParams = msg.getShort()
      val params = if (numParams > 0) {
        val arrayBuf = new Array[Array[Byte]](numParams)
        (0 until numParams).foreach { i =>
          val byteLen = msg.getInt()
          val byteArray = new Array[Byte](byteLen)
          msg.get(byteArray)
          arrayBuf(i) = byteArray
        }
        arrayBuf.toSeq
      } else {
        Seq.empty[Array[Byte]]
      }
      val numResultFormats = msg.getShort()
      val resultFormats = if (numResultFormats > 0) {
        val arrayBuf = new Array[Int](numResultFormats)
        (0 until numResultFormats).foreach { i =>
          arrayBuf(i) = msg.getShort()
        }
        arrayBuf.toSeq
      } else {
        Seq.empty[Int]
      }

      logInfo(s"Bind: portalName='$portalName' queryName='$queryName' formats=$formats "
        + s"params=$params resultFormats=$resultFormats")

      ("Bind", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        import sessionState._
        val queryState = sessionState.queries(queryName)

        // Convert `params` to string parameters
        val strParams = PgParamConverters(params, queryState.paramIds, formats)
        strParams.foreach { case (index, param) =>
          logInfo( s"""Bind param: $$$index -> $param""")
        }
        val boundQuery = ParameterBinder.bind(queryState.statement, strParams.toMap)
        val boundPlan = queryState.logicalPlan
        logInfo(s"Bound plan:\n$boundPlan")

        val plan = (boundQuery, boundPlan)
        // val plan = (queryState.statement, boundPlan)
        val execState = cli.executeStatement(sessionId, plan)

        val rowIter = execState.run()
        val schema = execState.outputSchema()
        val outputFormats = formatsInExtendedQueryMode(schema)
        val rowWriter = PgRowConverters(conf, schema, outputFormats)
        val portalState = PortalState(
          queryState.withRowWriter(rowWriter), execState, !portalName.isEmpty)
        portalState.resultRowIter = rowIter
        if (portalState.isCursorMode) {
          logInfo(s"Cursor mode enabled: portalName='$portalName'")
        }

        sessionState.portals(portalName) = portalState
        sessionState.activePortal = Some(portalName)

        ctx.write(BindComplete)
        ctx.flush()
      })
    },

    // An ASCII code of the `Close` message is 'C'(67)
    67 -> { msg =>
      val (tpe, name) = (msg.get(), extractString(msg))

      ("Close", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        if (tpe == 83) { // Close a prepared statement
          sessionState.queries.remove(name)
          logInfo(s"Close the '$name' prepared statement in this session (id:$sessionId)")
        } else if (tpe == 80) { // Close a portal
          sessionState.portals.remove(name)
          logInfo(s"Close the '$name' portal in this session (id:$sessionId)")
        } else {
          logWarning(s"Unknown type received in 'Close`: $tpe")
        }
      })
    },

    // An ASCII code of the `Describe` message is 'D'(68)
    68 -> { msg =>
      val (tpe, name) = (msg.get(), extractString(msg))

      ("Describe", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        import sessionState.v3Protocol._
        import sessionState._

        if (tpe == 83) { // Describe a prepared statement
          logInfo(s"Describe the '$name' prepared statement in this session (id:$sessionId)")
          val queryState = sessionState.queries(name)

          // To get a schema, run a query with default params
          val defaultParams = queryState.paramIds.zipWithIndex.map {
            case (_, i) => (i + 1) -> s"''"
          }
          val boundQuery = ParameterBinder.bind(queryState.statement, defaultParams.toMap)
          val execState = cli.executeStatement(sessionId, (boundQuery, null))
          execState.run()
          ctx.write(RowDescription(execState.outputSchema()))
          // ctx.write(RowDescription(queryState.logicalPlan.schema))
          ctx.flush()
        } else if (tpe == 80) { // Describe a portal
          logInfo(s"Describe the '$name' portal in this session (id:$sessionId)")
          val portalState = sessionState.portals(name)
          ctx.write(RowDescription(portalState.execState.outputSchema()))
          ctx.flush()
        } else {
          logWarning(s"Unknown type received in 'Describe`: $tpe")
        }
      })
    },

    // An ASCII code of the `Execute` message is 'E'(69)
    69 -> { msg =>
      val (portalName, maxRows) = (extractString(msg), msg.getInt())

      logInfo(s"Execute: portalName='$portalName', maxRows=$maxRows")

      ("Execute", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        try {
          import sessionState.v3Protocol._

          val portalState = sessionState.portals(portalName)
          val rowConveter = portalState.queryState.rowWriter.get
          val rowIter = if (portalState.numFetched == 0) {
            // val iter = sessionState.execState.run()
            // sessionState.resultRowIter = iter
            // iter
            portalState.resultRowIter
          } else {
            portalState.resultRowIter
          }

          var numRows = 0
          if (maxRows == 0) {
            rowIter.foreach { iter =>
              ctx.write(DataRow(iter, rowConveter))
              numRows += 1
            }
          } else {
            rowIter.take(maxRows).foreach { iter =>
              ctx.write(DataRow(iter, rowConveter))
              numRows += 1
            }
            // Accumulate fetched #rows in this query
            portalState.numFetched += numRows
          }

          // Sends back a complete message depending on a portal state
          val logicalPlan = portalState.queryState.logicalPlan
          logicalPlan match {
            case BeginCommand() =>
              ctx.write(CommandComplete(s"BEGIN"))
            case _ if !portalState.isCursorMode =>
              ctx.write(CommandComplete(s"SELECT $numRows"))
            case _ =>
              if (numRows == 0) {
                ctx.write(CommandComplete(s"FETCH ${portalState.numFetched}"))
              } else {
                ctx.write(PortalSuspended)
              }
          }
          ctx.flush()
        } catch {
          case NonFatal(e) =>
            sessionState.portals.remove(portalName)
            throw e
        }
      })
    },

    // An ASCII code of the `FunctionCall` message is 'F'(70)
    70 -> { msg =>
      val objId = msg.getInt()
      val numFormats = msg.getShort()
      val formats = if (numFormats > 0) {
        val arrayBuf = new Array[Int](numFormats)
        (0 until numFormats).foreach(i => arrayBuf(i) = msg.getShort())
        arrayBuf.toSeq
      } else {
        Seq.empty[Int]
      }
      val numParams = msg.getShort()
      val params = if (numParams > 0) {
        val arrayBuf = new Array[Array[Byte]](numParams)
        (0 until numParams).foreach { i =>
          val byteLen = msg.getInt()
          val byteArray = new Array[Byte](byteLen)
          msg.get(byteArray)
          arrayBuf(i) = byteArray
        }
        arrayBuf.toSeq
      } else {
        Seq.empty[Array[Byte]]
      }
      val resultFormat = msg.getShort()

      ("FunctionCall",
          (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        throw new UnsupportedOperationException("Not supported yet")
      })
    },

    // An ASCII code of the `Flush` message is 'H'(72)
    72 -> { msg =>
      ("Flush", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        throw new UnsupportedOperationException("Not supported yet")
      })
    },

    // An ASCII code of the `Parse` message is 'P'(80)
    80 -> { msg =>
      val queryName = extractString(msg)
      val query = extractString(msg)
      val numParams = msg.getShort()
      val params = if (numParams > 0) {
        val arrayBuf = new Array[Int](numParams)
        (0 until numParams).foreach(i => arrayBuf(i) = msg.getInt())
        arrayBuf.toSeq
      } else {
        Seq.empty[Int]
      }

      logInfo(s"Parse: queryName='$queryName' query='$query' objIds=$params")

      ("Parse", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        // Checks if `PostgreSqlParser` can parse the input query
        val analyzedPlan = PgV3MessageHandler.analyzePlan(query)
        sessionState.queries(queryName) = QueryState(query, analyzedPlan, params)
        ctx.write(ParseComplete)
        ctx.flush()
      })
    },

    // An ASCII code of the `Query` message is 'Q'(81)
    81 -> { msg =>
      val byteArray = new Array[Byte](msg.remaining)
      msg.get(byteArray)
      // Since a query string could contain several queries (separated by semicolons),
      // there might be several such response sequences before the backend finishes processing
      // the query string.
      val statements = new String(byteArray, StandardCharsets.UTF_8).split(";").map(_.trim).init

      ("Query", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        import sessionState.v3Protocol._
        import sessionState._

        if (statements.size > 0) {
          logDebug(s"input queries are ${statements.mkString(", ")}")
          // If a completely empty (no contents other than whitespace) query string is received,
          // the response is EmptyQueryResponse followed by ReadyForQuery.
          if (statements.length == 1 && statements(0).isEmpty) {
            ctx.write(EmptyQueryResponse)
          } else if (statements.size > 1) {
            // TODO: Support multiple queries
            throw new UnsupportedOperationException(
              s"multi-query execution unsupported: ${statements.mkString(", ")}")
          } else {
            val query = statements.head

            // Checks if `PostgreSqlParser` can parse the input query and executes the query
            // in `PostgreSQLExecutor`.
            val analyzedPlan = PgV3MessageHandler.analyzePlan(query)
            val plan = (query, analyzedPlan)
            val execState = cli.executeStatement(sessionId, plan)
            val resultRowIter = execState.run()

            // The response to a SELECT query (or other queries that return row sets, such as
            // EXPLAIN or SHOW) normally consists of RowDescription, zero or more DataRow
            // messages, and then CommandComplete.
            val schema = execState.outputSchema()
            ctx.write(RowDescription(schema))

            val formats = formatsInSimpleQueryMode(schema)
            val rowWriter = PgRowConverters(conf, schema, formats)
            var numRows = 0
            resultRowIter.foreach { iter =>
              ctx.write(DataRow(iter, rowWriter))
              numRows += 1
            }
            ctx.write(CommandComplete(s"SELECT $numRows"))
          }
        }
        ctx.write(ReadyForQuery)
        ctx.flush()
      })
    },

    // An ASCII code of the `Sync` message is 'S'(83)
    83 -> { msg =>
      ("Sync", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        ctx.write(ReadyForQuery)
        ctx.flush()
      })
    },

    // An ASCII code of the `Terminate` message is 'X'(88)
    88 -> { msg =>
      ("Terminate", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        ctx.close()
      })
    },

    // An ASCII code of the `CopyDone` message is 'c'(99)
    99 -> { msg =>
      ("CopyDone", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        throw new UnsupportedOperationException("Not supported yet")
      })
    },

    // An ASCII code of the `CopyData` message is 'd'(100)
    100 -> { msg =>
      val data = new Array[Byte](msg.getInt())
      msg.get(data)

      ("CopyData", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        throw new UnsupportedOperationException("Not supported yet")
      })
    },

    // An ASCII code of the `CopyFail` message is 'f'(102)
    102 -> { msg =>
      ("CopyFail", (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        throw new UnsupportedOperationException("Not supported yet")
      })
    },

    // An ASCII code of the `PasswordMessage` message is 'p'(112)
    112 -> { msg =>
      val token = new Array[Byte](msg.remaining)
      msg.get(token)

      ("PasswordMessage",
          (ctx: ChannelHandlerContext, sessionId: Int, sessionState: SessionV3State) => {
        val principal = PgV3MessageHandler.kerberosServerPrincipal
        if (doGSSAuthentication(ctx, sessionState, principal, token)) {
          sendAuthenticationOk(ctx, sessionState)
        }
      })
    }
  )

  /**
   * Get a pair of a single client message type name and the processor from a current position
   * in given `msgBuffer`. Since `msgBuffer` could have multiple client messages,
   * we update the position to point to a next message.
   */
  def extractClientMessageProcessor(msgBuffer: ByteBuffer): (String, MessageProcessorType) = {
    val messageId = msgBuffer.get().toInt
    val basePos = msgBuffer.position()
    val msgLen = msgBuffer.getInt()
    val (msgTypeName, func) = messageProcessors.get(messageId).map(_(msgBuffer)).getOrElse {
      throw new SQLException(s"Unknown message type: $messageId")
    }
    msgBuffer.position(basePos + msgLen)
    (msgTypeName, func)
  }


  /**
   * Response messages sent back to clients (Messages with no parameter).
   */

  /**
   * An ASCII code 'R' is an identifier of authentication messages.
   * If we receive the `StartupMessage` message from a client and we have no failure,
   * we send one of these messages for user's verification.
   */
  lazy val AuthenticationOk = {
    val buf = ByteBuffer.allocate(9)
    buf.put('R'.toByte).putInt(8).putInt(0)
    buf.array()
  }

  def sendAuthenticationOk(ctx: ChannelHandlerContext, sessionState: SessionV3State): Unit = {
    import sessionState.v3Protocol._
    import sessionState._

    ctx.write(AuthenticationOk)
    // Pass server settings into a JDBC driver
    Seq(
      "application_name" -> "spark-sql-server",
      "server_encoding" -> "UTF-8",
      "server_version" -> conf.sqlServerVersion,
      "TimeZone" -> conf.sessionLocalTimeZone,
      "integer_datetimes" -> "on"
    ).foreach { case (key, value) =>
      ctx.write(ParameterStatus(key, value))
    }
    ctx.write(BackendKeyData(PgV3MessageHandler.getUniqueChannelId(ctx), sessionState.secretKey))
    ctx.write(ReadyForQuery)
    ctx.flush()
  }

  lazy val AuthenticationGSS = {
    val buf = ByteBuffer.allocate(9)
    buf.put('R'.toByte).putInt(8).putInt(7)
    buf.array()
  }

  /**
   * To initiate a SSL-encrypted connection, the frontend initially sends an `SSLRequest` message
   * rather than `StartupMessage`. Then, we send a single byte containing 'S' or 'N',
   * indicating that it is willing or unwilling to perform SSL, respectively.
   */
  lazy val SupportSSL = {
    val buf = ByteBuffer.allocate(1)
    buf.put('S'.toByte)
    buf.array()
  }

  lazy val NoSSL = {
    val buf = ByteBuffer.allocate(1)
    buf.put('N'.toByte)
    buf.array()
  }

  /**
   * An ASCII code '2' is an identifier of this [[BindComplete]] message.
   * If we receive the `Bind` message from a client and we have no failure,
   * we send this message to the client.
   */
  lazy val BindComplete = {
    val buf = ByteBuffer.allocate(5)
    buf.put('2'.toByte).putInt(4)
    buf.array()
  }

  /**
   * An ASCII code '3' is an identifier of this [[CloseComplete]] message.
   * If we receive the `Close` message from a client and we have no failure,
   * we send this message to the client.
   */
  lazy val CloseComplete = {
    val buf = ByteBuffer.allocate(5)
    buf.put('3'.toByte).putInt(4)
    buf.array()
  }

  /**
   * An ASCII code 'I' is an identifier of this [[EmptyQueryResponse]] message.
   * If we receive an empty query string (no contents other than whitespace) in command requests,
   * we send this message to the client.
   */
  lazy val EmptyQueryResponse = {
    val buf = ByteBuffer.allocate(5)
    buf.put('I'.toByte).putInt(4)
    buf.array()
  }

  /**
   * An ASCII code 'n' is an identifier of this [[NoData]] message.
   * If any command request (e.g., `Query` and `Execute`) is finished successfully and
   * it has no result row, we send this message to the client.
   */
  lazy val NoData = {
    val buf = ByteBuffer.allocate(5)
    buf.put('n'.toByte).putInt(4)
    buf.array()
  }

  /**
   * An ASCII code '1' is an identifier of this [[ParseComplete]] message.
   * If we receive the `Parse` message from a client and we have no failure,
   * we send this message to the client.
   */
  lazy val ParseComplete = {
    val buf = ByteBuffer.allocate(5)
    buf.put('1'.toByte).putInt(4)
    buf.array()
  }

  /**
   * An ASCII code 's' is an identifier of this [[PortalSuspended]] message.
   * This is the message as a portal-suspended indicator.
   * Note this only appears if an `Execute` message's row-count limit was reached.
   */
  lazy val PortalSuspended = {
    val buf = ByteBuffer.allocate(5)
    buf.put('s'.toByte).putInt(4)
    buf.array()
  }

  /**
   * An ASCII code 'Z' is an identifier of this [[ReadyForQuery]] message.
   * This message informs the frontend that it can safely send a new command.
   */
  lazy val ReadyForQuery = {
    val buf = ByteBuffer.allocate(6)
    buf.put('Z'.toByte).putInt(5).put('I'.toByte)
    buf.array()
  }

  /**
   * An ASCII code 'E' is an identifier of this [[ErrorResponse]] message.
   * If we have any failure, we send this message to the client.
   */
  def ErrorResponse(msg: String): Array[Byte] = {
    // Since this function is placed in many places, we put this companion object
    val errMsg = if (msg != null) msg else ""
    val buf = ByteBuffer.allocate(8 + errMsg.length)
    buf.put('E'.toByte)
      .putInt(7 + errMsg.length)
      // 'M' indicates a human-readable message
      .put('M'.toByte)
      .put(errMsg.getBytes(StandardCharsets.UTF_8))
      .put(0.toByte)
      .put(0.toByte)
    buf.array()
  }
}

// scalastyle:off line.size.limit
/**
 * This is a special class to avoid a following exception;
 * "ByteArrayDecoder is not @Sharable handler, so can't be added or removed multiple times"
 *
 *  http://stackoverflow.com/questions/34068315/bytearraydecoder-is-not-sharable-handler-so-cant-be-added-or-removed-multiple
 */
// scalastyle:on line.size.limit
@ChannelHandler.Sharable
class SharableByteArrayDecode extends ByteArrayDecoder {}

/** Creates a newly configured [[io.netty.channel.ChannelPipeline]] for a new channel. */
class PgV3MessageInitializer(cli: SessionService, conf: SQLConf)
    extends ChannelInitializer[SocketChannel] with Logging {

  private val msgDecoder = new SharableByteArrayDecode()
  private val msgEncoder = new ByteArrayEncoder()
  private val msgHandler = new PgV3MessageHandler(cli, conf)

  // SSL configuration variables
  private val keyStorePathOption = conf.sqlServerSslKeyStorePath
  private val keyStorePasswd = conf.sqlServerSslKeyStorePasswd.getOrElse("")

  private def createSslContext(ch: SocketChannel): SslHandler = if (keyStorePathOption.isDefined) {
    val keyStore = KeyStore.getInstance("JKS")
    keyStore.load(new FileInputStream(keyStorePathOption.get), keyStorePasswd.toCharArray)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(keyStore, keyStorePasswd.toCharArray)
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(kmf.getKeyManagers, null, null)
    val sslEngine = sslContext.createSSLEngine()
    sslEngine.setUseClientMode(false)
    new SslHandler(sslEngine)
  } else {
    val ssc = new SelfSignedCertificate()
    SslContext.newServerContext(ssc.certificate(), ssc.privateKey()).newHandler(ch.alloc)
  }

  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()
    if (conf.sqlServerSslEnabled) {
      // If an SSL-encrypted connection enabled, the server first needs to handle the `SSLRequest`
      // message from the frontend. Then, the frontend starts an SSL start up handshake
      // with the server. Therefore, `SSLRequestHandler` processes the first message
      // and pass through following messages.
      pipeline.addLast(new SslRequestHandler(), createSslContext(ch))
      logInfo("SSL-encrypted connection enabled")
    }
    pipeline.addLast(msgDecoder, msgEncoder, msgHandler)
  }
}

// This SSL handler class is built for each connection in `PostgreSQLV3MessageInitializer`
class SslRequestHandler() extends ChannelInboundHandlerAdapter with Logging {
  import PgWireProtocol._

  // Once SSL established, the handler passes through following messages
  private var isEstablished: Boolean = false

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    if (!isEstablished) {
      val msgBuf = msg.asInstanceOf[ByteBuf]

      // Check an SSL request code
      val byteArray = new Array[Byte](msgBuf.readableBytes())
      msgBuf.getBytes(0, byteArray)
      val byteBuf = ByteBuffer.wrap(byteArray)
      byteBuf.position(byteBuf.position() + 4)
      byteBuf.getInt() match {
        case SSL_REQUEST_CODE =>
          ctx.write(Unpooled.wrappedBuffer(SupportSSL), ctx.newPromise())
          ctx.flush()
          isEstablished = true
        case _ =>
          val sockAddr = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
          val hostName = s"${sockAddr.getHostName()}:${sockAddr.getPort()}"
          logWarning(s"Non-SSL Connection requested from $hostName though, " +
            "this SQL server is currently running with a SSL mode")
          ctx.close()
      }
    } else {
      // Just pass through the message
      ctx.fireChannelRead(msg)
    }
  }
}

// Manage cursor states in a session
case class QueryState(
  statement: String,
  logicalPlan: LogicalPlan,
  paramIds: Seq[Int],
  // This writer is initialized in `Bind` messages because it depends
  // on `outputFormats` provided by the messages.
  rowWriter: Option[RowWriter] = None) {

  def withRowWriter(rowWriter: RowWriter): QueryState = {
    QueryState(statement, logicalPlan, paramIds, Some(rowWriter))
  }
}

case class PortalState(queryState: QueryState, execState: Operation, isCursorMode: Boolean) {

  // Holds an iterator of operations results
  var resultRowIter: Iterator[InternalRow] = Iterator.empty

  // Number of the rows that this portal state returns
  var numFetched: Int = 0
}

case class SessionV3State(
  cli: SessionService,
  conf: SQLConf,
  v3Protocol: PgWireProtocol,
  secretKey: Int) extends SessionState {

  // Holds multiple prepared statements inside a session
  val queries: mutable.Map[String, QueryState] = mutable.Map.empty
  val portals: mutable.Map[String, PortalState] = mutable.Map.empty

  // Holds a current active portal of query execution and this variable possibly accessed
  // by asynchronous JDBC cancellation requests.
  @volatile var activePortal: Option[String] = None

  // Holds unprocessed bytes for incomming V3 messages
  var pendingBytes: Array[Byte] = Array.empty

  override def close(): Unit = {}
}

object SessionV3State {

  def resetState(state: SessionV3State): Unit = {
    state.activePortal = None
    state.pendingBytes = Array.empty
  }
}

/**
 * Since this handler class is shared between connections in `PostgreSQLV3MessageInitializer`,
 * this class is thread-safe. If multiple threads in a client shares a single JDBC
 * connection, this class works well because the PostgreSQL JDBC drivers wait until another thread
 * has finished its current SQL operation. See a document below for details:
 *
 * - Chapter 10. Using the Driver in a Multithreaded or a Servlet Environment
 *  https://jdbc.postgresql.org/documentation/92/thread.html
 */
@ChannelHandler.Sharable
class PgV3MessageHandler(cli: SessionService, conf: SQLConf)
    extends SimpleChannelInboundHandler[Array[Byte]] with Logging {
  import PgV3MessageHandler._
  import PgWireProtocol._

  private val channelIdToSessionId = jSyncMap(new jHashMap[Int, Int]())

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val msgBuffer = ByteBuffer.wrap(msg)
    if (!channelIdToSessionId.containsKey(getUniqueChannelId(ctx))) {
      acceptStartupMessage(ctx, msgBuffer)
    } else {
      // Once the connection established, following messages are processed here
      handleV3Messages(ctx, msgBuffer)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logError(s"Exception detected: ${exceptionString(cause)}")
    handleException(ctx, cause.getMessage)
    closeSession(channelId = getUniqueChannelId(ctx))
    ctx.close()
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    closeSession(channelId = getUniqueChannelId(ctx))
    ctx.close()
  }

  private def openSession(
      channelId: Int,
      secretKey: Int,
      userName: String,
      passwd: String,
      hostAddr: String,
      dbName: String): SessionState = {
    val v3Protocol = PgWireProtocol(conf.sqlServerMessageBufferSizeInBytes)
    val state = SessionV3State(cli, conf, v3Protocol, secretKey)
    val sessionId = cli.openSession(userName, passwd, hostAddr, dbName, state)
    channelIdToSessionId.put(channelId, sessionId)
    logInfo(s"Open a session (sessionId=$sessionId, channelId=$channelId " +
      s"userName=$userName hostAddr=$hostAddr)")
    state
  }

  private def closeSession(channelId: Int): Unit = {
    // `exceptionCaught` possibly calls this function
    if (channelIdToSessionId.containsKey(channelId)) {
      val sessionId = channelIdToSessionId.remove(channelId)
      logInfo(s"Close the session (sessionId=$sessionId, channelId=$channelId)")
      cli.closeSession(sessionId)
    }
  }

  private def getSessionId(ctx: ChannelHandlerContext): Int = {
    val channelId = getUniqueChannelId(ctx)
    channelIdToSessionId.get(channelId)
  }

  private def getSessionState(sessionId: Int): SessionV3State = {
    cli.getSessionState(sessionId).asInstanceOf[SessionV3State]
  }

  private def handleException(ctx: ChannelHandlerContext, errMsg: String): Unit = {
    // In an exception happens, ErrorResponse is issued followed by ReadyForQuery.
    // All further processing of the query string is aborted by ErrorResponse (even if more
    // queries remained in it).
    logError(errMsg)
    ctx.write(ErrorResponse(errMsg))
    ctx.write(ReadyForQuery)
    ctx.flush()
  }

  private def acceptStartupMessage(ctx: ChannelHandlerContext, msgBuffer: ByteBuffer): Unit = {
    // Skip a message length because it is not used
    msgBuffer.position(msgBuffer.position() + 4)

    val magic = msgBuffer.getInt()

    // If this handler receives `SSLRequest`, return `NoSSL`
    if (magic == SSL_REQUEST_CODE) {
      val sockAddr = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
      val hostName = s"${sockAddr.getHostName()}:${sockAddr.getPort()}"
      logWarning(s"SSL Connection requested from $hostName though, " +
        "this SQL server is currently running with non-SSL mode")
      ctx.write(NoSSL)
      ctx.close()
      return
    } else if (magic == CANCEL_REQUEST_CODE) {
      val channelId = msgBuffer.getInt()
      val secretKey = msgBuffer.getInt()
      val sessionState = Some(channelIdToSessionId.get(channelId)).map(getSessionState).getOrElse {
          handleException(ctx, s"Unknown cancel request: channelId=$channelId")
          return
        }
      if (sessionState.secretKey != secretKey) {
        handleException(ctx, s"Illegal secretKey: channelId=$channelId")
        return
      }
      logWarning(s"Canceling the running query: channelId=$channelId")
      sessionState.activePortal.foreach { portalName =>
        sessionState.portals(portalName).execState.cancel()
      }
      ctx.close()
      return
    } else if (magic != V3_PROTOCOL_VERSION) {
      // The protocol version number. The most significant 16 bits are the major version number
      // (3 for the protocol described here). The least significant 16 bits are
      // the minor version number (0 for the protocol described here).
      handleException(ctx, s"Protocol version $magic unsupported")
      return
    }

    // This message includes the names of the user and of the database the user wants to
    // connect to; it also identifies the particular protocol version to be used.
    // (Optionally, the startup message can include additional settings for run-time parameters.)
    val byteArray = new Array[Byte](msgBuffer.remaining)
    msgBuffer.get(byteArray)
    val propStr = new String(byteArray).split('\u0000')
    val (keys, values) = propStr.zipWithIndex.partition(_._2 % 2 == 0) match {
      case (a, b) => (a.map(_._1), b.map(_._1))
    }
    val props = keys.zip(values).toMap
    logDebug("Received properties from client: "
      + props.map { case (key, value) => s"$key=$value" }.mkString(", "))
    val userName = props.getOrElse("user", "UNKNOWN")
    val passwd = props.getOrElse("passwd", "")
    val hostAddr = ctx.channel().localAddress().asInstanceOf[InetSocketAddress].getHostName()
    val secretKey = new Random(System.currentTimeMillis).nextInt
    val dbName = props.getOrElse("database", "default")
    val sessionState = openSession(
      getUniqueChannelId(ctx), secretKey, userName, passwd, hostAddr, dbName
    ).asInstanceOf[SessionV3State]

    // Check if Kerberos authentication is enabled
    if (conf.contains("spark.yarn.keytab")) {
      ctx.write(AuthenticationGSS)
      ctx.flush()
    } else {
      sendAuthenticationOk(ctx, sessionState)
    }
  }

  private def getBytesToProcess(msgBuffer: ByteBuffer, state: SessionV3State) = {
    // Since a message possibly spans over multiple in-coming data in `channelRead0` calls,
    // we need to check enough data to process first. We push complete messages into
    // `bytesToProcess`; otherwise it puts left data in `pendingBytes` for a next call.
    val len = state.pendingBytes.size + msgBuffer.remaining()
    val bytesToProcess = mutable.ArrayBuffer[Array[Byte]]()
    val uncheckedBuffer = ByteBuffer.allocate(len)
    uncheckedBuffer.put(state.pendingBytes)
    uncheckedBuffer.put(msgBuffer.array)
    state.pendingBytes = Array.empty
    uncheckedBuffer.rewind()
    while (uncheckedBuffer.hasRemaining) {
      val (basePos, _) = (uncheckedBuffer.position(), uncheckedBuffer.get())
      if (uncheckedBuffer.remaining() >= 4) {
        val msgLen = 1 + uncheckedBuffer.getInt()
        uncheckedBuffer.position(basePos)
        if (uncheckedBuffer.remaining() >= msgLen) {
          // Okay to process
          val buf = new Array[Byte](msgLen)
          uncheckedBuffer.get(buf)
          bytesToProcess.append(buf)
        } else {
          val pendingBytes = new Array[Byte](uncheckedBuffer.remaining())
          uncheckedBuffer.get(pendingBytes)
          state.pendingBytes = pendingBytes
        }
      } else {
        uncheckedBuffer.position(basePos)
        val pendingBytes = new Array[Byte](uncheckedBuffer.remaining())
        uncheckedBuffer.get(pendingBytes)
        state.pendingBytes = pendingBytes
      }
    }

    logDebug(s"#bytesToProcess=${bytesToProcess.size} #pendingBytes=${state.pendingBytes.size}")
    bytesToProcess
  }

  private def handleV3Messages(ctx: ChannelHandlerContext, msgBuffer: ByteBuffer): Unit = {
    val sessionId = getSessionId(ctx)
    val sessionState = getSessionState(sessionId)
    val bytesToProcess = getBytesToProcess(msgBuffer, sessionState)
    for (msg <- bytesToProcess) {
      val (msgTypeName, processor) = try {
        extractClientMessageProcessor(ByteBuffer.wrap(msg))
      } catch {
        case NonFatal(e) =>
          handleException(ctx, exceptionString(e))
          SessionV3State.resetState(sessionState)
          return
      }
      try { processor(ctx, sessionId, sessionState) } catch {
        case NonFatal(e) =>
          handleException(ctx, s"Exception detected in `$msgTypeName`: ${exceptionString(e)}")
          SessionV3State.resetState(sessionState)
          return
      }
    }
  }
}

object PgV3MessageHandler {

  private val conf = SQLServerEnv.sqlConf
  private val parser = new PgParser(conf)
  private val analyzer = SQLServerEnv.sqlContext.sessionState.analyzer

  // Returns Kerberos principal like 'spark/fully.qualified.domain.name@YOUR-REALM.COM'
  def kerberosServerPrincipal: String = conf.getConfString("spark.yarn.principal")

  def analyzePlan(query: String): LogicalPlan = try {
    // analyzer.execute(parser.parsePlan(query))
    parser.parsePlan(query)
  } catch {
    case e: ParseException if e.command.isDefined =>
      throw new SQLException(s"Cannot handle command ${e.command.get}")
  }

  def getUniqueChannelId(ctx: ChannelHandlerContext): Int = {
    // A netty developer said we could use `Channel#hashCode()` as an unique id in:
    //  http://stackoverflow.com/questions/18262926/howto-get-some-id-for-a-netty-channel
    ctx.channel().hashCode()
  }
}
