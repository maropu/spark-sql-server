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

import java.io.{CharArrayWriter, FileInputStream}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.security.{KeyStore, PrivilegedExceptionAction}
import java.sql.SQLException
import javax.net.ssl.{KeyManagerFactory, SSLContext}

import scala.collection.mutable
import scala.util.control.NonFatal

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandler, ChannelInboundHandlerAdapter, ChannelInitializer}
import io.netty.channel.socket.SocketChannel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}
import io.netty.handler.ssl.{SslContext, SslHandler}
import io.netty.handler.ssl.util.SelfSignedCertificate
import org.apache.hadoop.security.UserGroupInformation
import org.ietf.jgss.{GSSContext, GSSCredential, GSSException, GSSManager, Oid}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JacksonGenerator
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.server.SQLServerConf._
import org.apache.spark.sql.server.service.CLI
import org.apache.spark.sql.server.service.ExecuteStatementOperation
import org.apache.spark.sql.server.service.postgresql.{Metadata => PgMetadata}
import org.apache.spark.sql.types._


/**
 * This is the implementation of the PostgreSQL V3 client/server protocol.
 * The V3 protocol is used in PostgreSQL 7.4 and later.
 * A specification of the V3 protocol can be found in an URL:
 *
 * https://www.postgresql.org/docs/9.5/static/protocol.html
 */
private object PostgreSQLWireProtocol {

  /** An identifier for `StartupMessage`. */
  val V3_PROTOCOL_VERSION: Int = 196608

  /** An identifier for `SSLRequest`. */
  val SSL_REQUEST_CODE: Int = 80877103

  /** An identifier for `CancelRequest`. */
  val CANCEL_REQUEST_CODE: Int = 80877102

  /**
   * Message types received from clients.
   * NOTE: We need special handling for the three messages: `CancelRequest`, `SSLRequest`,
   * and `StartupMessage`.
   */
  sealed trait ClientMessageType

  /** An ASCII code 'B' is an identifier of this [[Bind]] message. */
  case class Bind(portalName: String, queryName: String, formats: Seq[Int],
    params: Seq[Array[Byte]], resultFormats: Seq[Int]) extends ClientMessageType

  /** An ASCII code 'C' is an identifier of this [[Close]] message. */
  case class Close(tpe: Int, name: String) extends ClientMessageType

  /** An ASCII code 'd' is an identifier of this [[CopyData]] message. */
  case class CopyData(data: Array[Byte]) extends ClientMessageType

  /** An ASCII code 'c' is an identifier of this [[CopyDone]] message. */
  case class CopyDone() extends ClientMessageType

  /** An ASCII code 'f' is an identifier of this [[CopyFail]] message. */
  case class CopyFail(cause: String) extends ClientMessageType

  /** An ASCII code 'D' is an identifier of this [[Describe]] message. */
  case class Describe(tpe: Int, name: String) extends ClientMessageType

  /** An ASCII code 'E' is an identifier of this [[Execute]] message. */
  case class Execute(portalName: String, maxRows: Int) extends ClientMessageType

  /** An ASCII code 'H' is an identifier of this [[Flush]] message. */
  case class Flush() extends ClientMessageType

  /** An ASCII code 'F' is an identifier of this [[FunctionCall]] message. */
  case class FunctionCall(objId: Int, formats: Seq[Int], params: Seq[Array[Byte]],
    resultFormat: Int) extends ClientMessageType

  /** An ASCII code 'P' is an identifier of this [[Parse]] message. */
  case class Parse(name: String, query: String, objIds: Seq[Int]) extends ClientMessageType

  /** An ASCII code 'p' is an identifier of this [[PasswordMessage]] message. */
  case class PasswordMessage(token: Array[Byte]) extends ClientMessageType

  /** An ASCII code 'Q' is an identifier of this [[Query]] message. */
  case class Query(queries: Seq[String]) extends ClientMessageType

  /** An ASCII code 'S' is an identifier of this [[Sync]] message. */
  case class Sync() extends ClientMessageType

  /** An ASCII code 'F' is an identifier of this [[Sync]] message. */
  case class CancelRequest(channelId: Int, secretId: Int) extends ClientMessageType

  /** An ASCII code 'X' is an identifier of this [[Terminate]] message. */
  case class Terminate() extends ClientMessageType

  /** This type represents the other unknown types. */
  case class UnknownType(typeId: Int) extends ClientMessageType


  /**
   * Response messages sent back to clients.
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

  lazy val AuthenticationGSS = {
    val buf = ByteBuffer.allocate(9)
    buf.put('R'.toByte).putInt(8).putInt(7)
    buf.array()
  }

  def AuthenticationGSSContinue(token: Array[Byte]): Array[Byte] = {
    val buf = ByteBuffer.allocate(9 + token.size)
    buf.put('R'.toByte).putInt(8 + token.size).putInt(8).put(token)
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
   * If we receive the [[Bind]] message from a client and we have no failure,
   * we send this message to the client.
   */
  lazy val BindComplete = {
    val buf = ByteBuffer.allocate(5)
    buf.put('2'.toByte).putInt(4)
    buf.array()
  }

  /**
   * An ASCII code '3' is an identifier of this [[CloseComplete]] message.
   * If we receive the [[Close]] message from a client and we have no failure,
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
   * If any command request (e.g., [[Query]] and [[Execute]]) is finished successfully and
   * it has no result row, we send this message to the client.
   */
  lazy val NoData = {
    val buf = ByteBuffer.allocate(5)
    buf.put('n'.toByte).putInt(4)
    buf.array()
  }

  /**
   * An ASCII code '1' is an identifier of this [[ParseComplete]] message.
   * If we receive the [[Parse]] message from a client and we have no failure,
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
   * Note this only appears if an [[Execute]] message's row-count limit was reached.
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
   * An ASCII code 'K' is an identifier of this [[BackendKeyData]] message.
   * If we receive the `CancelRequest` message from a client and we have no failure,
   * we send this message to the client.
   */
  def BackendKeyData(channelId: Int, secretKey: Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(13)
    buf.put('K'.toByte).putInt(12).putInt(channelId).putInt(secretKey)
    buf.array()
  }

  /**
   * An ASCII code 'C' is an identifier of this [[CommandComplete]] message.
   * If any command request (e.g., [[Query]] and [[Execute]]) is finished successfully,
   * we send this message to the client.
   */
  def CommandComplete(tag: String): Array[Byte] = {
    val buf = ByteBuffer.allocate(6 + tag.length)
    buf.put('C'.toByte).putInt(5 + tag.length).put(tag.getBytes("US-ASCII")).put(0.toByte)
    buf.array()
  }

  /**
   * An ASCII code 'D' is an identifier of this [[DataRow]] message.
   * If any command request (e.g., [[Query]] and [[Execute]]) is finished successfully and we have
   * result rows, we send the results as the [[DataRow]]s.
   */
  def DataRow(internalRow: InternalRow, state: PortalState): Array[Byte] = {
    val schema = state.execState.schema
    val row = internalRow.toSeq(schema)
    val rowBytes = row.zipWithIndex.map { case (data, index) =>
      if (!internalRow.isNullAt(index)) {
        if (schema(index).dataType == BinaryType) {
          // Send back as binary data
          data.asInstanceOf[Array[Byte]]
        } else {
          // Send back as text data
          val rowStr = schema(index).dataType match {
            case s: StructType =>
              val key = schema(index).name
              val jsonState = state.jsonStates.getOrElse(key, {
                val writer = new CharArrayWriter
                val js = JsonState(writer, new JacksonGenerator(s, writer))
                state.jsonStates.put(key, js)
                js
              })
              toJson(data.asInstanceOf[InternalRow], jsonState)
            case MapType(keyType, valueType, _) =>
              val mapData = data.asInstanceOf[MapData]
              val keyArray = mapData.keyArray()
              val valueArray = mapData.valueArray()
              // TODO: Use `JacksonGenerator` here
              (0 until mapData.numElements).map { index =>
                val keyData = keyType match {
                  case StringType => s""""${keyArray.get(index, StringType)}""""
                  case _ => s"${keyArray.get(index, keyType)}"
                }
                val valueData = valueType match {
                  case StringType => s""""${valueArray.get(index, StringType)}""""
                  case _ => s"${valueArray.get(index, valueType)}"
                }
                s"$keyData:$valueData"
              }.mkString("{", ",", "}")
            case ArrayType(tpe, _) =>
              val arrayData = data.asInstanceOf[ArrayData]
              (0 until arrayData.numElements).map { index =>
                tpe match {
                  case TimestampType =>
                    val timestampData = DateTimeUtils.toJavaTimestamp(
                      arrayData.get(index, TimestampType).asInstanceOf[Long])
                    s""""$timestampData""""
                  case DateType =>
                    val dateData = DateTimeUtils.toJavaDate(
                      arrayData.get(index, DateType).asInstanceOf[Int])
                    s"$dateData"
                  case _ =>
                    s"${arrayData.get(index, tpe)}"
                }
              }.mkString("{", ",", "}")
            case TimestampType =>
              val timestampData = DateTimeUtils.toJavaTimestamp(data.asInstanceOf[Long])
              s"$timestampData"
            case DateType =>
              val dateData = DateTimeUtils.toJavaDate(data.asInstanceOf[Int])
              s"$dateData"
            case _ =>
              s"$data"
          }
          rowStr.getBytes("US-ASCII")
        }
      } else {
        Array.empty[Byte]
      }
    }
    val length = 6 + row.zipWithIndex.map { case (data, index) =>
        4 + rowBytes(index).length
      }.reduce(_ + _)
    val buf = ByteBuffer.allocate(1 + length)
    buf.put('D'.toByte).putInt(length).putShort(internalRow.numFields.toShort)
    schema.zipWithIndex.map { case (field, index) =>
      if (!internalRow.isNullAt(index)) {
        buf.putInt(rowBytes(index).length)
        buf.put(rowBytes(index))
      } else {
        // '-1' indicates a NULL column value
        buf.putInt(-1)
      }
    }
    buf.array()
  }

  private def toJson(structRow: InternalRow, state: JsonState): String = {
    val (gen, writer) = (state.generator, state.writer)
    gen.write(structRow)
    gen.flush()
    val json = writer.toString
    writer.reset()
    json
  }

  /**
   * An ASCII code 'E' is an identifier of this [[ErrorResponse]] message.
   * If we have any failure, we send this message to the client.
   */
  def ErrorResponse(msg: String): Array[Byte] = {
    val buf = ByteBuffer.allocate(8 + msg.length)
    buf.put('E'.toByte).putInt(7 + msg.length)
      // 'M' indicates a human-readable message
      // TODO: Need to support other types
      .put('M'.toByte).put(msg.getBytes("US-ASCII")).put(0.toByte)
      .put(0.toByte)
    buf.array()
  }

  /**
   * An ASCII code 'V' is an identifier of this [[FunctionCallResponse]] message.
   * If we receive the `FunctionCall` message from a client and we have no failure,
   * we send this message to the client.
   */
  def FunctionCallResponse(result: Array[Byte]): Array[Byte] = {
    val buf = ByteBuffer.allocate(9 + result.size)
    buf.put('V'.toByte).putInt(8 + result.size).putInt(result.size).put(result)
    buf.array()
  }

  /**
   * TODO: Support `NoticeResponse`, `NotificationResponse`, and `ParameterDescription`.
   */

  def ParameterStatus(key: String, value: String): Array[Byte] = {
    val paramLen = key.length + value.length
    val buf = ByteBuffer.allocate(7 + paramLen)
    buf.put('S'.toByte)
      .putInt(6 + paramLen)
      .put(key.getBytes("US-ASCII")).put(0.toByte)
      .put(value.getBytes("US-ASCII")).put(0.toByte)
    buf.array()
  }

  /**
   * An ASCII code 'T' is an identifier of this [[RowDescription]] message.
   * If we receive the [[Describe]] message from a client and we have no failure,
   * we send this message to the client.
   */
  def RowDescription(schema: StructType): Array[Byte] = {
    if (schema.size == 0) {
      val buf = ByteBuffer.allocate(7)
      buf.put('T'.toByte).putInt(6).putShort(0)
      buf.array()
    } else {
      val length = 6 + schema.map(_.name.length + 19).reduce(_ + _)
      val buf = ByteBuffer.allocate(1 + length)
      buf.put('T'.toByte).putInt(length).putShort(schema.size.toShort)
      // Each column has length(field.name) + 19 bytes
      schema.toSeq.zipWithIndex.map { case (field, index) =>
        val pgType = PgMetadata.getPgType(field.dataType)
        val mode = if (field.dataType == BinaryType) 1 else 0
        buf.put(field.name.getBytes("US-ASCII")).put(0.toByte) // field name
          .putInt(0)                        // object ID of the table
          .putShort((index + 1).toShort)    // attribute number of the column
          .putInt(pgType.oid)               // object ID of the field's data type
          .putShort(pgType.len.toShort)     // data type size
          .putInt(0)                        // type modifier
          .putShort(mode.toShort)           // 1 for binary; otherwise 0
      }
      buf.array()
    }
  }

  /**
   * Extract a single client message from a current position in given `msgBuffer`.
   * Since `msgBuffer` could have multiple client messages, we update the position to point
   * to a next message.
   */
  def extractClientMessageType(msgBuffer: ByteBuffer): ClientMessageType = {
    // A string in messages is a null-terminated one (C-style string) and there is no specific
    // length limitation on strings.
    def extractString(): String = {
      val origPos = msgBuffer.position()
      var len = 0
      // Search null from a crrent position
      while (msgBuffer.hasRemaining() && msgBuffer.get() != 0.toByte) {
        len = len + 1
      }
      if (len != 0) {
        val localBuf = new Array[Byte](len)
        msgBuffer.position(origPos)
        msgBuffer.get(localBuf, 0, len)
        msgBuffer.get()
        new String(localBuf, "US-ASCII")
      } else {
        ""
      }
    }
    val messageId = msgBuffer.get().toInt
    val basePos = msgBuffer.position()
    val msgLen = msgBuffer.getInt()
    val message = messageId match {
      case 66 =>
        val portalName = extractString()
        val queryName = extractString()
        val numFormats = msgBuffer.getShort()
        val formats = if (numFormats > 0) {
          val arrayBuf = new Array[Int](numFormats)
          (0 until numFormats).foreach(i => arrayBuf(i) = msgBuffer.getShort())
          arrayBuf.toSeq
        } else {
          Seq.empty[Int]
        }
        val numParams = msgBuffer.getShort()
        val params = if (numParams > 0) {
          val arrayBuf = new Array[Array[Byte]](numParams)
          (0 until numParams).foreach { i =>
            val byteLen = msgBuffer.getInt()
            val byteArray = new Array[Byte](byteLen)
            msgBuffer.get(byteArray)
            arrayBuf(i) = byteArray
          }
          arrayBuf.toSeq
        } else {
          Seq.empty[Array[Byte]]
        }
        val numResultFormats = msgBuffer.getShort()
        val resultFormats = if (numResultFormats > 0) {
          val arrayBuf = new Array[Int](numParams)
          (0 until numResultFormats).foreach(i => arrayBuf(i) = msgBuffer.getShort())
          arrayBuf.toSeq
        } else {
          Seq.empty[Int]
        }
        Bind(portalName, queryName, formats, params, resultFormats)
      case 67 =>
        Close(msgBuffer.get(), extractString())
      case 68 =>
        Describe(msgBuffer.get(), extractString())
      case 69 =>
        Execute(extractString(), msgBuffer.getInt())
      case 70 =>
        val objId = msgBuffer.getInt()
        val numFormats = msgBuffer.getShort()
        val formats = if (numFormats > 0) {
          val arrayBuf = new Array[Int](numFormats)
          (0 until numFormats).foreach(i => arrayBuf(i) = msgBuffer.getShort())
          arrayBuf.toSeq
        } else {
          Seq.empty[Int]
        }
        val numParams = msgBuffer.getShort()
        val params = if (numParams > 0) {
          val arrayBuf = new Array[Array[Byte]](numParams)
          (0 until numParams).foreach { i =>
            val byteLen = msgBuffer.getInt()
            val byteArray = new Array[Byte](byteLen)
            msgBuffer.get(byteArray)
            arrayBuf(i) = byteArray
          }
          arrayBuf.toSeq
        } else {
          Seq.empty[Array[Byte]]
        }
        val resultFormat = msgBuffer.getShort()
        FunctionCall(objId, formats, params, resultFormat)
      case 72 =>
        Flush()
      case 80 =>
        val portalName = extractString()
        val query = extractString()
        val numParams = msgBuffer.getShort()
        val params = if (numParams > 0) {
          val arrayBuf = new Array[Int](numParams)
          (0 until numParams).foreach(i => arrayBuf(i) = msgBuffer.getInt())
          arrayBuf.toSeq
        } else {
          Seq.empty[Int]
        }
        Parse(portalName, query, params.toSeq)
      case 81 =>
        val byteArray = new Array[Byte](msgBuffer.remaining)
        msgBuffer.get(byteArray)
        // Since a query string could contain several queries (separated by semicolons),
        // there might be several such response sequences before the backend finishes processing
        // the query string.
        Query(new String(byteArray, "US-ASCII").split(";").init.map(_.trim))
      case 83 =>
        Sync()
      case 88 =>
        Terminate()
      case 99 =>
        CopyDone()
      case 100 =>
        val byteArray = new Array[Byte](msgBuffer.getInt())
        msgBuffer.get(byteArray)
        CopyData(byteArray)
      case 102 =>
        CopyFail(extractString())
      case 112 =>
        val byteArray = new Array[Byte](msgBuffer.remaining)
        msgBuffer.get(byteArray)
        PasswordMessage(byteArray)
      case _ =>
        UnknownType(messageId)
    }
    msgBuffer.position(basePos + msgLen)
    message
  }
}

// scalastyle:off
/**
 * This is a special class to avoid a following exception;
 * "ByteArrayDecoder is not @Sharable handler, so can't be added or removed multiple times"
 *
 * http://stackoverflow.com/questions/34068315/bytearraydecoder-is-not-sharable-handler-so-cant-be-added-or-removed-multiple
 */
// scalastyle:on
@ChannelHandler.Sharable
private[v3] class SharableByteArrayDecode extends ByteArrayDecoder {}

/** Creates a newly configured [[io.netty.channel.ChannelPipeline]] for a new channel. */
private[service] class PostgreSQLV3MessageInitializer(cli: CLI, conf: SparkConf)
    extends ChannelInitializer[SocketChannel] with Logging {

  val msgDecoder = new SharableByteArrayDecode()
  val msgEncoder = new ByteArrayEncoder()
  val msgHandler = new PostgreSQLV3MessageHandler(cli, conf)

  // SSL configuration variables
  val keyStorePathOption = conf.sqlServerSslKeyStorePath
  val keyStorePasswd = conf.sqlServerSslKeyStorePasswd.getOrElse("")

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

@ChannelHandler.Sharable
private[v3] class SslRequestHandler() extends ChannelInboundHandlerAdapter with Logging {
  import PostgreSQLWireProtocol._

  // Once SSL established, the handler passes through following messages
  var isSslEstablished: Boolean = false

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    if (!isSslEstablished) {
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
          isSslEstablished = true
        case _ =>
          val sockAddr = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
          val hostName = s"${sockAddr.getHostName()}:${sockAddr.getPort()}"
          logWarning(s"Non-SSL Connection requested from ${hostName} though, " +
            "this SQL server is currently running with a SSL mode")
          ctx.close()
      }
    } else {
      // Just pass through the message
      ctx.fireChannelRead(msg)
    }
  }
}

/** A state to convert struct-typed data into json */
private case class JsonState(writer: CharArrayWriter, generator: JacksonGenerator)

/**
 * Manage a cursor state in a session.
 *
 * TODO: We need to recheck threading policies in PostgreSQL JDBC drivers and related documents:
 * - Chapter 10. Using the Driver in a Multithreaded or a Servlet Environment
 *  https://jdbc.postgresql.org/documentation/92/thread.html
 */
private case class PortalState(sessionId: Int, secretKey: Int) {
  var pendingBytes: Array[Byte] = Array.empty
  // `execState` possibly accessed by asynchronous JDBC cancellation requests
  @volatile var execState: ExecuteStatementOperation = _
  val queries: mutable.Map[String, String] = mutable.Map.empty
  val jsonStates: mutable.Map[String, JsonState] = mutable.Map.empty
}

@ChannelHandler.Sharable
private[v3] class PostgreSQLV3MessageHandler(cli: CLI, conf: SparkConf)
    extends SimpleChannelInboundHandler[Array[Byte]] with Logging {

  import PostgreSQLWireProtocol._

  // A format is like 'spark/fully.qualified.domain.name@YOUR-REALM.COM'
  private lazy val kerberosServerPrincipal = conf.get("spark.yarn.principal")

  private val channelIdToPortalState = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Int, PortalState]())

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val msgBuffer = ByteBuffer.wrap(msg)
    if (!channelIdToPortalState.containsKey(getUniqueChannelId(ctx))) {
      acceptStartupMessage(ctx, msgBuffer)
    } else {
      // Once the connection established, following messages are processed here
      handleV3Messages(ctx, msgBuffer)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logError(s"Exception detected: ${cause.getMessage}")
    handleException(ctx, cause.getMessage)
    closeSession(channelId = getUniqueChannelId(ctx))
    ctx.close()
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    closeSession(channelId = getUniqueChannelId(ctx))
    ctx.close()
  }

  private def openSession(channelId: Int, secretKey: Int, userName: String, passwd: String,
      hostAddr: String): PortalState = {
    val sessionId = cli.openSession(userName, passwd, hostAddr)
    val portalState = PortalState(sessionId, secretKey)
    channelIdToPortalState.put(channelId, portalState)
    logInfo(s"Open a session (sessionId=${sessionId}, channelId=${channelId} " +
      s"userName=${userName} hostAddr=${hostAddr})")
    portalState
  }

  private def closeSession(channelId: Int): Unit = {
    if (channelIdToPortalState.containsKey(channelId)) {
      val portalState = channelIdToPortalState.get(channelId)
      logInfo(s"Close the session (sessionId=${portalState.sessionId}, channelId=${channelId}})")
      portalState.jsonStates.map(_._2.generator.close)
      cli.closeSession(portalState.sessionId)
      channelIdToPortalState.remove(channelId)
    }
  }

  private def getUniqueChannelId(ctx: ChannelHandlerContext): Int = {
    // A netty developer said we could use `Channel#hashCode()` as an unique id in:
    // http://stackoverflow.com/questions/18262926/howto-get-some-id-for-a-netty-channel
    ctx.channel().hashCode()
  }

  private def handleException(ctx: ChannelHandlerContext, errMsg: String): Unit = {
    // In an exception happens, ErrorResponse is issued followed by ReadyForQuery.
    // All further processing of the query string is aborted by ErrorResponse (even if more
    // queries remained in it).
    logWarning(errMsg)
    ctx.write(ErrorResponse(errMsg))
    ctx.write(ReadyForQuery)
    ctx.flush()
  }

  // An URL string in PostgreSQL JDBC drivers is something like
  // "jdbc:postgresql://[host]/[database]?user=[name]&kerberosServerName=spark"
  private def handleGSSAuthentication(ctx: ChannelHandlerContext, token: Array[Byte]): Boolean = {
    UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction[Boolean] {
      override def run(): Boolean = {
        // Get own Kerberos credentials for accepting connection
        val manager = GSSManager.getInstance()
        var gssContext: GSSContext = null
        try {
          // This Oid for Kerberos GSS-API mechanism
          val kerberosMechOid = new Oid("1.2.840.113554.1.2.2")
          // Oid for kerberos principal name
          val krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1")

          val serverName = manager.createName(kerberosServerPrincipal, krb5PrincipalOid)
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
          case e: GSSException =>
            throw new SQLException("Kerberos authentication failed: ", e)
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

  private def handleAuthenticationOk(ctx: ChannelHandlerContext, portalState: PortalState): Unit = {
    ctx.write(AuthenticationOk)
    ctx.write(ParameterStatus("application_name", "spark-sql-server"))
    ctx.write(ParameterStatus("server_encoding", "UTF-8"))
    // `server_version` decides how to handle metadata between jdbc clients and servers.
    // In case of `server_version` >= 8.0, `DatabaseMetaData#getTypeInfo` implicitly
    // generates a query below;
    //
    //  SELECT
    //    typinput='array_in'::regproc,
    //    typtype
    //  FROM
    //     pg_catalog.pg_type LEFT JOIN (
    //      SELECT
    //        ns.oid as nspoid,
    //        ns.nspname,
    //        r.r
    //      FROM
    //        pg_namespace as ns JOIN (
    //          SELECT
    //            s.r,
    //            (current_schemas(false))[s.r] AS nspname
    //          FROM
    //            generate_series(1, array_upper(current_schemas(false), 1)) AS s(r)
    //        ) AS r USING (nspname)
    //    ) AS sp ON sp.nspoid = typnamespace
    //  WHERE typname = 'tid'
    //  ORDER BY sp.r, pg_type.oid
    //  DESC LIMIT 1
    //
    // However, it is difficult to handle this query in spark because of the PostgreSQL dialect.
    // Currently, we only support the dialect where the server version is '7.4'.
    //
    // scalastyle:off
    //
    // See an URL below for valid version numbers:
    // https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/core/ServerVersion.java
    //
    // scalastyle:on
    ctx.write(ParameterStatus("server_version", "7.4"))
    ctx.write(ParameterStatus("TimeZone", java.util.TimeZone.getDefault().getID()))
    ctx.write(BackendKeyData(getUniqueChannelId(ctx), portalState.secretKey))
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
      logWarning(s"SSL Connection requested from ${hostName} though, " +
        "this SQL server is currently running with a non-SSL mode")
      ctx.write(NoSSL)
      ctx.close()
      return
    } else if (magic == CANCEL_REQUEST_CODE) {
      val channelId = msgBuffer.getInt()
      val secretKey = msgBuffer.getInt()
      val portal = Some(channelIdToPortalState.get(channelId)).getOrElse {
        throw new SQLException(s"Unknown cancel request: channelId=${channelId}")
      }
      if (portal.secretKey != secretKey) {
        throw new SQLException(s"Illegal secretKey: channelId=${channelId}")
      }
      logWarning(s"Canceling the running query: channelId=${channelId}")
      portal.execState.cancel()
      ctx.close()
      return
    } else if (magic != V3_PROTOCOL_VERSION) {
      // The protocol version number. The most significant 16 bits are the major version number
      // (3 for the protocol described here). The least significant 16 bits are
      // the minor version number (0 for the protocol described here).
      throw new SQLException("Unsupported a protocol version")
    }

    // This message includes the names of the user and of the database the user wants to
    // connect to; it also identifies the particular protocol version to be used.
    // (Optionally, the startup message can include additional settings for run-time parameters.)
    val byteArray = new Array[Byte](msgBuffer.remaining)
    msgBuffer.get(byteArray)
    val propStr = new String(byteArray).split('\0')
    val (keys, values) = propStr.zipWithIndex.partition(_._2 % 2 == 0) match {
      case (a, b) => (a.map(_._1), b.map(_._1))
    }
    val props = keys.zip(values).toMap
    logDebug("Received properties from a client: "
      + props.map { case (key, value) => s"${key}=${value}" }.mkString(", "))
    // props.get("application_name").map { appName =>
    //   if (appName == "psql") {
    //     ctx.write(ErrorResponse("`psql` not supported in Spark SQL server"))
    //     logWarning("Close the connection from `psql` because of unsupported client")
    //     ctx.flush()
    //     ctx.close()
    //     return
    //   }
    // }
    val userName = props.getOrElse("user", "UNKNOWN")
    val passwd = props.getOrElse("passwd", "")
    val hostAddr = ctx.channel().localAddress().asInstanceOf[InetSocketAddress].getHostName()
    val secretKey = 0 // TODO: Set random value
    val portalState = openSession(getUniqueChannelId(ctx), secretKey, userName, passwd, hostAddr)

    // Check if Kerberos authentication is enabled
    if (conf.contains("spark.yarn.keytab")) {
      ctx.write(AuthenticationGSS)
      ctx.flush()
    } else {
      handleAuthenticationOk(ctx, portalState)
    }
  }

  private def handleV3Messages(ctx: ChannelHandlerContext, msgBuffer: ByteBuffer): Unit = {
    val channelId = getUniqueChannelId(ctx)
    val portalState = channelIdToPortalState.get(channelId)

    // Since a message possible spans over multiple in-coming data in `channelRead0` calls,
    // we need to check enough data to process. We put complete messages in `procBuffer` for message
    // processing; otherwise it puts left data in `pendingBytes` for a next call.
    val len = portalState.pendingBytes.size + msgBuffer.remaining()
    val uncheckedBuffer = ByteBuffer.allocate(len)
    val procBuffer = ByteBuffer.allocate(len)
    uncheckedBuffer.put(portalState.pendingBytes)
    uncheckedBuffer.put(msgBuffer.array)
    portalState.pendingBytes = Array.empty
    uncheckedBuffer.flip()
    while (uncheckedBuffer.hasRemaining) {
      val basePos = uncheckedBuffer.position()
      val msgType = uncheckedBuffer.get()
      if (uncheckedBuffer.remaining() >= 4) {
        val msgLen = 1 + uncheckedBuffer.getInt()
        uncheckedBuffer.position(basePos)
        if (uncheckedBuffer.remaining() >= msgLen) {
          // Okay to process
          val buf = new Array[Byte](msgLen)
          uncheckedBuffer.get(buf)
          procBuffer.put(buf)
        } else {
          val pendingBytes = new Array[Byte](uncheckedBuffer.remaining())
          uncheckedBuffer.get(pendingBytes)
          portalState.pendingBytes = pendingBytes
        }
      } else {
        uncheckedBuffer.position(basePos)
        val pendingBytes = new Array[Byte](uncheckedBuffer.remaining())
        uncheckedBuffer.get(pendingBytes)
        portalState.pendingBytes = pendingBytes
      }
    }

    procBuffer.flip()
    logDebug(s"#processed=${procBuffer.remaining} #pending=${portalState.pendingBytes.size}")

    while (procBuffer.remaining() > 0) {
      extractClientMessageType(procBuffer) match {
        case PasswordMessage(token) =>
          if (handleGSSAuthentication(ctx, token)) {
            handleAuthenticationOk(ctx, portalState)
          }
          return
        case Bind(portalName, queryName, formats, params, resultFormats) =>
          logInfo(s"Bind: portalName=${portalName},queryName=${queryName},formats=${formats},"
            + s"params=${params},resultFormats=${resultFormats}")
          var query = portalState.queries.getOrElse(queryName, {
            throw new SQLException(s"Unknown query specified: ${queryName}")
          })
          formats.zipWithIndex.foreach { case (format, index) =>
            val target = "$" + s"${index + 1}"
            val param = format match {
              case 0 => // text
                new String(params(index), "US-ASCII")
              case 1 => // binary
                val data = params(index)
                data.length match {
                  case 4 => s"${ByteBuffer.wrap(params(index)).getInt}"
                }
            }
            logDebug(s"binds param: ${target}->${param}")
            query = query.replace(target, s"'${param}'")
          }
          logInfo(s"Bound query: ${query}")
          try {
            portalState.execState = cli.executeStatement(portalState.sessionId, query)
            portalState.execState.run()
          } catch {
            case NonFatal(e) =>
              handleException(ctx, s"Exception detected during message `Bind`: ${e}")
              return
          }
          ctx.write(BindComplete)
          ctx.flush()
        case Close(tpe, name) =>
          if (tpe == 83) { // Close a prepared statement
            portalState.queries.remove(name)
            logInfo(s"Close the '${name}' prepared statement in this session "
              + "(id:${portalState.sessionId}")
          } else if (tpe == 80) { // Close a portal
            // Do nothing
          } else {
            logWarning(s"Unknown type id received in message 'Close`: ${tpe}")
          }
        case Describe(tpe, name) =>
          // The response to a SELECT query (or other queries that return row sets, such as
          // EXPLAIN or SHOW) normally consists of RowDescription, zero or more DataRow
          // messages, and then CommandComplete.
          ctx.write(RowDescription(portalState.execState.schema()))
          ctx.flush()
        case Execute(portalName, maxRows) =>
          try {
            var numRows = 0
            if (maxRows == 0) {
              portalState.execState.iterator().foreach { iter =>
                ctx.write(DataRow(iter, portalState))
                numRows = numRows + 1
              }
            } else {
              portalState.execState.iterator().take(maxRows).foreach { iter =>
                ctx.write(DataRow(iter, portalState))
                numRows = numRows + 1
              }
            }
            ctx.write(CommandComplete(s"SELECT ${numRows}"))
          } catch {
            case NonFatal(e) =>
              handleException(ctx, s"Exception detected during message `Execute`: ${e}")
              return
          }
          ctx.flush()
        case FunctionCall(objId, formats, params, resultFormat) =>
          handleException(ctx, "Message 'FunctionCall' not supported")
          return
        case Flush() =>
          handleException(ctx, "Message 'Flush' not supported")
          return
        case Parse(name, query, objIds) =>
          portalState.queries(name) = query
          logInfo(s"Parse: name=${portalState.queries(name)},query=${query},objIds=${objIds}")
          ctx.write(ParseComplete)
          ctx.flush()
        case Query(queries) =>
          require(queries.size > 0)
          logDebug(s"input quries are ${queries.mkString(", ")}")
          // If a completely empty (no contents other than whitespace) query string is received,
          // the response is EmptyQueryResponse followed by ReadyForQuery.
          if (queries.length == 1 && queries(0).isEmpty) {
            ctx.write(EmptyQueryResponse)
          } else {
            // TODO: Support multiple queries
            val query = queries(0)
            val execState = cli.executeStatement(portalState.sessionId, query)
            portalState.execState = execState
            try {
              execState.run()

              // The response to a SELECT query (or other queries that return row sets, such as
              // EXPLAIN or SHOW) normally consists of RowDescription, zero or more DataRow
              // messages, and then CommandComplete.
              ctx.write(RowDescription(execState.schema))
              var numRows = 0
              execState.iterator().foreach { iter =>
                ctx.write(DataRow(iter, portalState))
                numRows += 1
              }
              ctx.write(CommandComplete(s"SELECT ${numRows}"))
            } catch {
              case NonFatal(e) =>
                handleException(ctx, s"Exception detected during message `Query`: ${e}")
                return
            }
          }
          // ReadyForQuery is issued when the entire string has been processed
          // and the backend is ready to accept a new query string.
          ctx.write(ReadyForQuery)
          ctx.flush()
        case Sync() =>
          ctx.write(ReadyForQuery)
          ctx.flush()
        case Terminate() =>
          closeSession(channelId)
          return
        case msg =>
          handleException(ctx, s"Unsupported message: $msg")
          return
      }
    }
  }
}
