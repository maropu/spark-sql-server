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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.execution.SparkSqlAstBuilder
import org.apache.spark.sql.server.execution.SparkSqlParser
import org.apache.spark.sql.server.parser._
import org.apache.spark.sql.server.parser.SqlBaseParser._
import org.apache.spark.sql.types._


/**
 * Concrete parser for PostgreSQL statements.
 *
 * TODO: We just copy Spark parser files into `org.apache.spark.sql.server.parser.*` and build
 * a new parser for PostgreSQL. So, we should fix this in a pluggable way.
 */
class PostgreSQLParser(conf: SQLConf) extends SparkSqlParser(conf) {

  override val astBuilder = new PostgreSqlAstBuilder(conf)
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
class PostgreSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf) {
  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    try {
      super.visitPrimitiveDataType(ctx)
    } catch {
      case e: ParseException =>
        (ctx.identifier.getText.toLowerCase, ctx.INTEGER_VALUE().asScala.toList) match {
          case ("text", Nil) => StringType
          case _ => throw e
        }
    }
  }

  override def visitPgStyleCast(ctx: PgStyleCastContext): Expression = withOrigin(ctx) {
    ctx.pgDataType.getText.toLowerCase match {
      case "regproc" =>
        val extractName = """['|"](.*)['|"]""".r
        val funcName = ctx.primaryExpression.getText match {
          case extractName(n) => n
          case n => n
        }
        UnresolvedFunction(funcName, Seq.empty, false)
      case _ =>
        Cast(expression(ctx.primaryExpression), typedVisit(ctx.pgDataType()))
    }
  }
}
