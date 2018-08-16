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

package org.apache.spark.sql.server.service.livy

import java.io.{ByteArrayInputStream, DataInputStream}
import java.sql.SQLException

import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.server.SQLServerEnv
import org.apache.spark.sql.server.service._
import org.apache.spark.sql.types.{ArrayType, ByteType, StructType}
import org.apache.spark.util.{CompletionIterator, Utils => SparkUtils}

private class LivyProxyOperation(
    sessionState: SessionState,
    query: (String, LogicalPlan))(
    _statementId: String) extends Operation with Logging {

  private val livyContext = sessionState._context match {
    case ctx: LivyProxyContext => ctx
    case other => sys.error(s"${this.getClass.getSimpleName} cannot handle $other")
  }

  override def statementId: String = _statementId

  override lazy val outputSchema: StructType = {
    livyContext.ask(SchemaRequest(query._1)) match {
      case SchemaResponse(schema) => schema
      case ErrorResponse(e) => throw new SQLException(SparkUtils.exceptionString(e))
    }
  }

  private var simpleMode: Boolean = true

  override def prepare(params: Map[Int, Literal]): Unit = {
    livyContext.ask(PrepareRequest(statementId, query._1, params)) match {
      case PrepareResponse() =>
        simpleMode = false
      case ErrorResponse(e) =>
        throw new SQLException(SparkUtils.exceptionString(e))
    }
  }

  private[this] var _cachedRowIterator: Iterator[InternalRow] = _

  override def run(): Iterator[InternalRow] = {
    if (state == INITIALIZED) {
      setState(RUNNING)
      val message = if (simpleMode) {
        ExecuteSimpleQuery(statementId, query._1)
      } else {
        ExecuteExtendedQuery(statementId)
      }
      val resultRowIterator = livyContext.ask(message) match {
        case ResultSetResponse(rows) =>
          setState(FINISHED)
          rows.toList.toIterator

        case IncrementalCollectStart() =>
          val localIterator = new Iterator[InternalRow] {

            /**
             * Decodes the byte arrays back to UnsafeRows and put them into buffer.
             */
            private def decodeUnsafeRows(bytes: InternalRow): Iterator[InternalRow] = {
              val nFields = outputSchema.length

              val codec = CompressionCodec.createCodec(SQLServerEnv.sparkConf)
              val byteArray = bytes.get(0, ArrayType(ByteType)).asInstanceOf[Array[Byte]]
              logInfo(s"fetched byte size: ${byteArray.length}")
              val bis = new ByteArrayInputStream(byteArray)
              val ins = new DataInputStream(codec.compressedInputStream(bis))

              new Iterator[InternalRow] {
                private var sizeOfNextRow = ins.readInt()
                override def hasNext: Boolean = sizeOfNextRow >= 0
                override def next(): InternalRow = {
                  val bs = new Array[Byte](sizeOfNextRow)
                  ins.readFully(bs)
                  val row = new UnsafeRow(nFields)
                  row.pointTo(bs, sizeOfNextRow)
                  sizeOfNextRow = ins.readInt()
                  row
                }
              }
            }

            private def fetchNextIter(): Iterator[InternalRow] = {
              livyContext.ask(RequestNextResultSet(statementId)) match {
                case ResultSetResponse(rows) if rows.size == 1 => decodeUnsafeRows(rows.head)
                case ResultSetResponse(_) =>
                  sys.error("`ResultSetResponse` should have a non-empty iterator")
                case IncrementalCollectEnd() => Iterator.empty
                case ErrorResponse(e) =>
                  setState(ERROR)
                  throw new SQLException(SparkUtils.exceptionString(e))
              }
            }

            private var currentIter = fetchNextIter()

            override def hasNext: Boolean = {
              val _hasNext = currentIter.hasNext
              if (!_hasNext) {
                currentIter = fetchNextIter()
                currentIter.hasNext
              } else {
                _hasNext
              }
            }

            override def next(): InternalRow = currentIter.next()
          }

          CompletionIterator[InternalRow, Iterator[InternalRow]](localIterator, setState(FINISHED))

        case ErrorResponse(e) =>
          setState(ERROR)
          throw new SQLException(SparkUtils.exceptionString(e))
      }
      _cachedRowIterator = resultRowIterator
      resultRowIterator
    } else {
      // Since this operation already has been done, just returns the cached result
      _cachedRowIterator
    }
  }

  override def cancel(): Unit = {
    // Since Spark jobs might be invoked in a state `FINISHED`, we accept it here
    if (state == RUNNING || state == FINISHED) {
      logInfo(
        s"""Cancelling query with $statementId:
           |Query:
           |${query._1}
           |Analyzed Plan:
           |${query._2}
         """.stripMargin)
      livyContext.ask(CancelRequest(statementId)) match {
        case CancelResponse =>
          setState(CANCELED)
        case ErrorResponse(e) =>
          val errMsg = SparkUtils.exceptionString(e)
          logWarning(errMsg)
          throw new SQLException(errMsg)
      }
    } else {
      logWarning(s"Tried to cancel query with $statementId though, this is not a running state.")
    }
  }

  override def close(): Unit = {
    setState(CLOSED)
  }
}

private[service] class LivyProxyExecutor extends OperationExecutor {

  // Creates a new instance for service-specific operations
  override def newOperation(
      sessionState: SessionState,
      statementId: String,
      query: (String, LogicalPlan)): Operation = {
    new LivyProxyOperation(sessionState, query)(statementId)
  }
}
