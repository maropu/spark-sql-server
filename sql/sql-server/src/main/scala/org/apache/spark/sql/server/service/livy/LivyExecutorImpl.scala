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

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.server.service._

private class LivyOperation(
    sessionState: SessionState,
    query: (String, LogicalPlan))(
    _statementId: String,
    catalogUpdater: (SQLContext, LogicalPlan) => Unit)
  extends OperationImpl(sessionState, query)(_statementId, catalogUpdater) {

  /**
   * Packing the UnsafeRows into byte array for faster serialization.
   * The byte arrays are in the following format:
   * [size] [bytes of UnsafeRow] [size] [bytes of UnsafeRow] ... [-1]
   *
   * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
   * compressed.
   */
  private def getByteArrayRdd(rdd: RDD[InternalRow]): RDD[InternalRow] = {
    rdd.mapPartitionsInternal { iter =>
      val buffer = new Array[Byte](4 << 10) // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))
      while (iter.hasNext) {
        val row = iter.next().asInstanceOf[UnsafeRow]
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator(InternalRow.fromSeq(bos.toByteArray :: Nil))
    }
  }

  private[this] var _cachedRowIterator: Iterator[InternalRow] = _

  override def run(): Iterator[InternalRow] = {
    if (state == INITIALIZED) {
      val df = executeInternal()

      val resultRowIterator = if (useIncrementalCollect) {
        val plan = df.queryExecution.executedPlan
        val outputAttrs = plan.output
        if (outputAttrs.nonEmpty) {
          // To make rows `UnsafeRow`, wraps `ProjectExec` here
          getByteArrayRdd(ProjectExec(outputAttrs, plan).execute()).toLocalIterator
        } else {
          getByteArrayRdd(plan.execute()).toLocalIterator
        }
      } else {
        // Needs to use `List` so that `Iterator#take` can proceed an internal cursor, e.g.,
        //
        // scala> val iter = Array(1, 2, 3, 4, 5, 6).toIterator
        // scala> iter.take(1).next
        // res2: Int = 1
        // scala> iter.take(1).next
        // res3: Int = 1
        // ...
        // scala> val iter = Array(1, 2, 3, 4, 5, 6).toList.toIterator
        // scala> iter.take(1).next
        // res4: Int = 1
        // scala> iter.take(1).next
        // res5: Int = 2
        // ...
        df.queryExecution.executedPlan.executeCollect().toList.toIterator
      }

      _cachedRowIterator = resultRowIterator
      resultRowIterator
    } else {
      // Since this operation already has been done, just returns the cached result
      _cachedRowIterator
    }
  }
}

private[livy] class LivyExecutorImpl(catalogUpdater: (SQLContext, LogicalPlan) => Unit)
    extends OperationExecutor {

  // Creates a new instance for service-specific operations
  override def newOperation(
      sessionState: SessionState,
      statementId: String,
      query: (String, LogicalPlan)): Operation = {
    new LivyOperation(sessionState, query)(statementId, catalogUpdater)
  }
}
