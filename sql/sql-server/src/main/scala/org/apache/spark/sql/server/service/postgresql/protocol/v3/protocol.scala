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
import java.security.KeyStore
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.SetCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.SQLServerConf
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.{Operation, SessionService, SessionState}
import org.apache.spark.sql.server.service.postgresql.PgMetadata._
import org.apache.spark.sql.server.service.postgresql.PgUtils
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
            s"'${SQLServerConf.SQLSERVER_MESSAGE_BUFFER_SIZE_IN_BYTES.key}'")
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
      if (schema.isEmpty) {
        buf.put('T'.toByte).putInt(6).putShort(0)
        7
      } else {
        val length = 6 + schema.map(_.name.length + 19).sum
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

  private def resultDataFormatsFor(schema: StructType, state: V3SessionState): Seq[Boolean] = {
    if (state.appName == "psql") {
      // TODO: 'psql' always needs a text transfer mode?
      Seq.fill(schema.length)(false)
    } else if (state.conf.sqlServerBinaryTransferMode) {
      schema.map { f => binaryFormatTypes.contains(f.dataType) }
    } else {
      Seq.fill(schema.length)(false)
    }
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

  // TODO: Needs to change `Any` into `Unit`
  private type MessageProcessorType = (V3SessionState) => Any

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

      ("Bind", (sessionState: V3SessionState) => {
        import sessionState._

        val queryState = sessionState.queries(queryName)
        val execState = cli.executeStatement(
          sessionState._sessionId, query = (queryState.statement, queryState.logicalPlan))

        // Converts `params` to [[Literal]] parameters
        val litParams = PgParamConverters(params, queryState.paramIds, formats)
        execState.prepare(litParams.toMap)
        logInfo(
          s"""Bound params:
             |  ${litParams.map {case (idx, param) => s"""$$$idx -> $param""" }.mkString("\n  ")}
           """.stripMargin)

        val schema = execState.outputSchema()
        val outputFormats = resultDataFormatsFor(schema, sessionState)
        val rowWriter = PgRowConverters(conf, schema, outputFormats)
        val newQueryState = queryState.withRowWriter(schema, rowWriter)
        val portalState = PortalState(newQueryState, execState, !portalName.isEmpty)
        if (portalState.isCursorMode) {
          logInfo(s"Cursor mode enabled: portalName='$portalName'")
        }

        // Updates a session state
        sessionState.queries(queryName) = newQueryState
        sessionState.portals(portalName) = portalState
        sessionState.activePortal = Some(portalName)

        ctx.write(BindComplete)
        ctx.flush()
      })
    },

    // An ASCII code of the `Close` message is 'C'(67)
    67 -> { msg =>
      val (tpe, name) = (msg.get(), extractString(msg))

      ("Close", (sessionState: V3SessionState) => {
        if (tpe == 83) { // Close a prepared statement
          sessionState.queries.remove(name)
          logInfo(s"Close the '$name' prepared statement (id:${sessionState._sessionId})")
        } else if (tpe == 80) { // Close a portal
          sessionState.portals.remove(name).foreach(_.execState.close())
          logInfo(s"Close the '$name' portal (id:${sessionState._sessionId}})")
        } else {
          logWarning(s"Unknown type received in 'Close': $tpe")
        }
      })
    },

    // An ASCII code of the `Describe` message is 'D'(68)
    68 -> { msg =>
      val (tpe, name) = (msg.get(), extractString(msg))

      ("Describe", (sessionState: V3SessionState) => {
        import sessionState._
        import sessionState.v3Protocol._

        if (tpe == 83) { // Describe a prepared statement
          logInfo(s"Describe the '$name' prepared statement (id:${sessionState._sessionId})")
          val queryState = sessionState.queries(name)
          ctx.write(RowDescription(queryState._schema))
          ctx.flush()
        } else if (tpe == 80) { // Describe a portal
          logInfo(s"Describe the '$name' portal i(id:${sessionState._sessionId})")
          val queryState = sessionState.portals(name).queryState
          ctx.write(RowDescription(queryState._schema))
          ctx.flush()
        } else {
          logWarning(s"Unknown type received in 'Describe': $tpe")
        }
      })
    },

    // An ASCII code of the `Execute` message is 'E'(69)
    69 -> { msg =>
      val (portalName, maxRows) = (extractString(msg), msg.getInt())

      logInfo(s"Execute: portalName='$portalName', maxRows=$maxRows")

      ("Execute", (sessionState: V3SessionState) => {
        try {
          import sessionState._
          import sessionState.v3Protocol._

          val portalState = sessionState.portals(portalName)
          val rowConveter = portalState.queryState._rowWriter.get
          val rowIter = if (portalState.numFetched == 0) {
            val iter = portalState.execState.run()
            portalState.resultRowIter = iter
            iter
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
            case BeginCommand(_) =>
              ctx.write(CommandComplete("BEGIN"))
            case SetCommand(kv) =>
              kv.map { case (k, v) =>
                ctx.write(CommandComplete(s"SET $k=$v"))
              }.getOrElse {
                ctx.write(CommandComplete("SET"))
              }
            case _ if !portalState.isCursorMode =>
              ctx.write(CommandComplete(s"SELECT $numRows"))
            case _ =>
              if (!rowIter.hasNext) {
                ctx.write(CommandComplete(s"FETCH ${portalState.numFetched}"))
              } else {
                ctx.write(PortalSuspended)
              }
          }
          ctx.flush()
        } catch {
          case NonFatal(e) =>
            sessionState.portals.remove(portalName).foreach(_.execState.close())
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

      ("FunctionCall", (sessionState: V3SessionState) => {
        throw new UnsupportedOperationException("Not supported yet")
      })
    },

    // An ASCII code of the `Flush` message is 'H'(72)
    72 -> { msg =>
      ("Flush", (sessionState: V3SessionState) => {
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

      ("Parse", (sessionState: V3SessionState) => {
        import sessionState._

        val parsed = PgUtils.parse(query)
        val schema = {
          import org.apache.spark.sql.server.service.SQLContextHolder
          _context match {
            case SQLContextHolder(ctx) =>
              val sesseionSpecificAnalyzer = ctx.sessionState.analyzer
              sesseionSpecificAnalyzer.execute(parsed).schema
            case _ =>
              new StructType()
          }
        }
        sessionState.queries(queryName) = QueryState(query, parsed, params, schema)
        ctx.write(ParseComplete)
        ctx.flush()
      })
    },

    // An ASCII code of the `Query` message is 'Q'(81)
    81 -> { msg =>
      val queries = extractString(msg)
      // Since a query string could contain several queries (separated by semicolons),
      // there might be several such response sequences before the backend finishes processing
      // the query string.
      val statements = queries.split(";").map(_.trim)

      logInfo(s"Query: statements=${statements.mkString(", ")}")

      ("Query", (sessionState: V3SessionState) => {
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
            val plan = (query, PgUtils.parse(query))
            val execState = cli.executeStatement(sessionState._sessionId, plan)
            val resultRowIter = execState.run()

            // The response to a SELECT query (or other queries that return row sets, such as
            // EXPLAIN or SHOW) normally consists of RowDescription, zero or more DataRow
            // messages, and then CommandComplete.
            val schema = execState.outputSchema()
            ctx.write(RowDescription(schema))

            val formats = resultDataFormatsFor(schema, sessionState)
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
      ("Sync", (sessionState: V3SessionState) => {
        import sessionState._
        ctx.write(ReadyForQuery)
        ctx.flush()
      })
    },

    // An ASCII code of the `Terminate` message is 'X'(88)
    88 -> { msg =>
      ("Terminate", (_: V3SessionState) => {
        // Since `PgV3MessageHandler.channelInactive` releases resources corresponding
        // to a current session, do nothing here.
      })
    },

    // An ASCII code of the `CopyDone` message is 'c'(99)
    99 -> { msg =>
      ("CopyDone", (_: V3SessionState) => {
        throw new UnsupportedOperationException("Not supported yet")
      })
    },

    // An ASCII code of the `CopyData` message is 'd'(100)
    100 -> { msg =>
      val data = new Array[Byte](msg.getInt())
      msg.get(data)

      ("CopyData", (_: V3SessionState) => {
        throw new UnsupportedOperationException("Not supported yet")
      })
    },

    // An ASCII code of the `CopyFail` message is 'f'(102)
    102 -> { msg =>
      ("CopyFail", (_: V3SessionState) => {
        throw new UnsupportedOperationException("Not supported yet")
      })
    },

    // An ASCII code of the `PasswordMessage` message is 'p'(112)
    112 -> { msg =>
      val token = new Array[Byte](msg.remaining)
      msg.get(token)

      ("PasswordMessage", (_: V3SessionState) => {
        throw new UnsupportedOperationException("Not supported yet")
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
    logDebug(s"Processing $msgTypeName: msgLen=$msgLen")
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

  def sendAuthenticationOk(sessionState: V3SessionState): Unit = {
    import sessionState.v3Protocol._
    import sessionState._

    ctx.write(AuthenticationOk)
    // Pass server settings into a JDBC driver
    Seq(
      "application_name" -> "spark-sql-server",
      "server_encoding" -> "UTF-8",
      "client_encoding" -> "UTF-8",
      "server_version" -> conf.sqlServerVersion,
      "TimeZone" -> conf.sessionLocalTimeZone,
      "session_authorization" -> sessionState.userName,
      "IntervalStyle" -> "postgres",
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
  logicalPlan: LogicalPlan, // Parsed, but not analyzed
  paramIds: Seq[Int],
  // Schema for `rowWriter`
  _schema: StructType = new StructType(),
  // This writer is initialized in `Bind` messages because it depends
  // on `outputFormats` provided by the messages.
  _rowWriter: Option[RowWriter] = None) {

  def withRowWriter(schema: StructType, rowWriter: RowWriter): QueryState = {
    QueryState(statement, logicalPlan, paramIds, schema, Some(rowWriter))
  }
}

case class PortalState(queryState: QueryState, execState: Operation, isCursorMode: Boolean) {

  // Holds an iterator of operations results
  var resultRowIter: Iterator[InternalRow] = Iterator.empty

  // Number of the rows that this portal state returns
  var numFetched: Int = 0
}

case class V3SessionState(
    ctx: ChannelHandlerContext,
    cli: SessionService,
    v3Protocol: PgWireProtocol,
    conf: SQLConf,
    userName: String,
    appName: String,
    secretKey: Int,
    closeFunc: () => Unit) extends SessionState {

  // Holds multiple prepared statements inside a session
  val queries: mutable.Map[String, QueryState] = mutable.Map.empty
  val portals: mutable.Map[String, PortalState] = mutable.Map.empty

  // Holds a current active portal of query execution and this variable possibly accessed
  // by asynchronous JDBC cancellation requests.
  @volatile var activePortal: Option[String] = None

  // Holds unprocessed bytes for incoming V3 messages
  var pendingBytes: Array[Byte] = Array.empty

  override def closeWithException(msg: String): Unit = {
    ctx.write(PgWireProtocol.ErrorResponse(msg))
    ctx.write(PgWireProtocol.ReadyForQuery)
    ctx.flush()
    close()
  }

  override def close(): Unit = {
    closeFunc()
    ctx.close()
    super.close()
  }

  override def toString(): String = {
    s"channelId=${PgV3MessageHandler.getUniqueChannelId(ctx)} " +
      s"userName=$userName appName=$appName"
  }
}

object V3SessionState {

  def resetState(state: V3SessionState): Unit = {
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
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    closeSession(channelId = getUniqueChannelId(ctx))
  }

  private def openSession(
      ctx: ChannelHandlerContext,
      channelId: Int,
      secretKey: Int,
      appName: String,
      userName: String,
      passwd: String,
      hostAddr: String,
      dbName: String): SessionState = {
    val v3Protocol = PgWireProtocol(conf.sqlServerMessageBufferSizeInBytes)
    val closeFunc: () => Unit = () => { channelIdToSessionId.remove(channelId) }
    val state = V3SessionState(ctx, cli, v3Protocol, conf, userName, appName, secretKey, closeFunc)
    val sessionId = cli.openSession(userName, passwd, hostAddr, dbName, state)
    channelIdToSessionId.put(channelId, sessionId)
    logInfo(s"Open a session (sessionId=$sessionId, channelId=$channelId " +
      s"userName=$userName hostAddr=$hostAddr)")
    state
  }

  private def closeSession(channelId: Int): Unit = {
    // `exceptionCaught` possibly calls this function
    if (channelIdToSessionId.containsKey(channelId)) {
      val sessionId = channelIdToSessionId.get(channelId)
      logInfo(s"Close the session (sessionId=$sessionId, channelId=$channelId)")
      // Resource cleanup will be done in `SessionState.close()`
      cli.closeSession(sessionId)
    }
  }

  private def getSessionState(ctx: ChannelHandlerContext): V3SessionState = {
    val channelId = getUniqueChannelId(ctx)
    val sessionId = channelIdToSessionId.get(channelId)
    cli.getSessionState(sessionId).asInstanceOf[V3SessionState]
  }

  private def getSessionState(sessionId: Int): V3SessionState = {
    cli.getSessionState(sessionId).asInstanceOf[V3SessionState]
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
    val appName = props.get("application_name").map { name =>
      if (name == "psql" && !conf.sqlServerPsqlEnabled) {
        handleException(ctx, "Rejected requests from psql. To accept psql requests, " +
          s"you should set true at ${SQLServerConf.SQLSERVER_PSQL_ENABLED.key}")
      } else {
        logWarning("Found a psql request accepted, but since this is mainly used for interactive " +
          "tests, you don't use this in your production environments")
      }
      name
    }.getOrElse("unknown")
    val userName = props.getOrElse("user", "UNKNOWN")
    val passwd = props.getOrElse("passwd", "")
    val hostAddr = ctx.channel().localAddress().asInstanceOf[InetSocketAddress].getHostName()
    val secretKey = new Random(System.currentTimeMillis).nextInt
    val dbName = props.getOrElse("database", "default")
    val sessionState = openSession(
      ctx, getUniqueChannelId(ctx), secretKey, appName, userName, passwd, hostAddr, dbName
    ).asInstanceOf[V3SessionState]

    // Check if Kerberos authentication is enabled
    if (conf.contains("spark.yarn.keytab")) {
      ctx.write(AuthenticationGSS)
      ctx.flush()
    } else {
      sendAuthenticationOk(sessionState)
    }
  }

  private def getBytesToProcess(msgBuffer: ByteBuffer, state: V3SessionState) = {
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
    val sessionState = getSessionState(ctx)
    val bytesToProcess = getBytesToProcess(msgBuffer, sessionState)
    for (msg <- bytesToProcess) {
      val (msgTypeName, processor) = try {
        extractClientMessageProcessor(ByteBuffer.wrap(msg))
      } catch {
        case NonFatal(e) =>
          handleException(ctx, exceptionString(e))
          V3SessionState.resetState(sessionState)
          return
      }
      try { processor(sessionState) } catch {
        case NonFatal(e) =>
          handleException(ctx, s"Exception detected in '$msgTypeName': ${exceptionString(e)}")
          V3SessionState.resetState(sessionState)
          return
      }
    }
  }
}

object PgV3MessageHandler {

  def getUniqueChannelId(ctx: ChannelHandlerContext): Int = {
    // A netty developer said we could use `Channel#hashCode()` as an unique id in:
    //  http://stackoverflow.com/questions/18262926/howto-get-some-id-for-a-netty-channel
    ctx.channel().hashCode()
  }
}
