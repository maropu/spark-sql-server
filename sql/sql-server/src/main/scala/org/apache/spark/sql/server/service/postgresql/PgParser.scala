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

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, First}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.server.catalyst.expressions.ParameterPlaceHolder
import org.apache.spark.sql.server.execution.{SparkSqlAstBuilder, SparkSqlParser}
import org.apache.spark.sql.server.parser.SqlBaseParser._
import org.apache.spark.sql.server.service.postgresql.execution.command.BeginCommand
import org.apache.spark.sql.types._


/**
 * Concrete parser for PostgreSQL statements.
 *
 * TODO: We just copy Spark parser files into `org.apache.spark.sql.server.parser.*` and build
 * a new parser for PostgreSQL. So, we should fix this in a pluggable way.
 */
private[postgresql] class PgParser(conf: SQLConf) extends SparkSqlParser(conf) {
  override val astBuilder = new PgAstBuilder(conf)
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
private[postgresql] class PgAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf)
    with PredicateHelper {
  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  override def visitBeginTransaction(ctx: BeginTransactionContext): LogicalPlan = {
    BeginCommand()
  }

  override def visitParamPlaceHolder(ctx: ParamPlaceHolderContext): Expression = withOrigin(ctx) {
    ParameterPlaceHolder()
  }

  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    try {
      super.visitPrimitiveDataType(ctx)
    } catch {
      case e: ParseException =>
        val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
        (dataType, ctx.INTEGER_VALUE().asScala.toList) match {
          case ("text", Nil) => StringType
          case _ => throw e
        }
    }
  }

  override def visitSubqueryExpression(
      ctx: SubqueryExpressionContext): Expression = withOrigin(ctx) {
    val subQuery = super.visitSubqueryExpression(ctx).asInstanceOf[ScalarSubquery]
    val proj = subQuery.plan.transformDown {
      // TODO: The PostgreSQL JDBC driver (`SQLSERVER_VERSION` = 7.4) issues a query below, but
      // Spark-v2.1 cannot correctly handle correlated sub-queries without aggregate.
      // So, we currently insert a `First` aggregate when hitting that kind of sub-queries.
      //
      // SELECT
      //   a.attname,
      //   pg_catalog.format_type(a.atttypid, a.atttypmod),
      //   (
      //     SELECT
      //       substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
      //     FROM
      //       pg_catalog.pg_attrdef d
      //     WHERE
      //       d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef
      //   ),
      //   a.attnotnull,
      //   a.attnum,
      //   NULL AS attcollation,
      //   NULL AS indexdef,
      //   NULL AS attfdwoptions
      // FROM
      //   pg_catalog.pg_attribute a
      // WHERE
      //   a.attrelid = '6205' AND a.attnum > 0 AND NOT a.attisdropped
      // ORDER BY
      //   a.attnum;
      //
      case p @ Project(ne :: Nil, child)
        if ne.find(_.isInstanceOf[AggregateFunction]).isEmpty &&
          ne.find {
            case uf: UnresolvedFunction =>
              uf.name.funcName == "pg_get_expr"
            case e =>
              false
          }.isDefined =>
        ne.find(_.isInstanceOf[UnresolvedAttribute]).map { attr =>
          val first = First(attr, ignoreNullsExpr = Literal(true))
          val newChild = child.transform {
            case f @ Filter(cond, _) =>
              val origPreds = splitConjunctivePredicates(cond)
              val (newPreds, droppedPreds) = origPreds.partition { expr =>
                expr.isInstanceOf[EqualTo] || expr.isInstanceOf[EqualNullSafe]
              }
              if (droppedPreds.nonEmpty) {
                logWarning(
                  s"""Spark-2.2 does not allow correlated sub-queries to have non-equal predicates,
                     |so we drop non-supported predicates to pass JDBC metadata operations.
                     |The dropped predicates are:
                     |${droppedPreds.mkString(" ")}
                   """.stripMargin)
              }
              f.copy(condition = newPreds.reduce(And))
          }
          val projWithAggregate = Aggregate(
            groupingExpressions = Nil,
            aggregateExpressions = UnresolvedAlias(first.toAggregateExpression()) :: Nil,
            child = newChild)
          logWarning(
            s"""Found a sub-query without aggregate, so we add `First` in the projection:
               |$projWithAggregate
             """.stripMargin)
          projWithAggregate
        }.getOrElse {
          p
        }
    }
    ScalarSubquery(proj)
  }

  override def visitPgStyleCast(ctx: PgStyleCastContext): Expression = withOrigin(ctx) {
    val dataType = ctx.pgDataType.dataType.getText
    dataType.toLowerCase match {
      case "regproc" =>
        val extractName = """['|"](.*)['|"]""".r
        val funcName = ctx.primaryExpression.getText match {
          case extractName(n) => n
          case n => n
        }
        UnresolvedFunction(funcName, Seq.empty, false)
      case "regtype" =>
        val args = expression(ctx.primaryExpression()) :: Nil
        val funcName = if (ctx.pgDataType.identifier != null) {
          s"${ctx.pgDataType.identifier.getText}.$dataType"
        } else {
          dataType
        }
        UnresolvedFunction(funcName, args, false)
      case "regclass" =>
        expression(ctx.primaryExpression)
      case _ =>
        Cast(expression(ctx.primaryExpression), typedVisit(ctx.pgDataType().dataType()))
    }
  }

  override def visitConcat(ctx: ConcatContext): Expression = {
    val exprs = ctx.primaryExpression().asScala
    Concat(expression(exprs.head) +: exprs.drop(1).map(expression))
  }

  private def toSparkRange(start: Expression, end: Expression, intvl: Option[Expression]) = {
    // Fill a gap between PostgreSQL `generate_series` and Spark `range` here
    val e = Add(end, Literal(1, IntegerType))
    val args = intvl.map(i => start :: e :: i :: Nil).getOrElse(start :: e :: Nil)
    UnresolvedTableValuedFunction("range", args)
  }

  override def visitSubstringInternalFunc(ctx: SubstringInternalFuncContext): Expression = {
    withOrigin(ctx) {
      val expr = expression(ctx.primaryExpression)
      val pos = Literal(0, IntegerType)
      val forNum = ctx.INTEGER_VALUE().asScala.toList match {
        case from :: Nil => from
        case _ => throw new ParseException("substring has not an enough parameter for `from`", ctx)
      }
      val len = Literal(forNum, IntegerType)
      Substring(expr, pos, len)
    }
  }

  override def visitTableValuedFunction(ctx: TableValuedFunctionContext): LogicalPlan = {
    withOrigin(ctx) {
      val funcPlan = (ctx.identifier(0).getText, ctx.expression.asScala.map(expression)) match {
        case ("generate_series", Buffer(start, end)) => toSparkRange(start, end, None)
        case ("generate_series", Buffer(start, end, step)) => toSparkRange(start, end, Some(step))
        case _ => super.visitTableValuedFunction(ctx)
      }
      if (ctx.identifier().size > 1) {
        val prefix = ctx.identifier(1).getText
        // This workaround is needed to parse a SQL syntax below:
        //
        // SELECT
        //   s.r, (current_schemas(false))[s.r] AS nspname
        // FROM
        //   generate_series(1, array_upper(current_schemas(false), 1)) AS s(r)
        //
        if (ctx.identifierList != null) {
          val aliases = visitIdentifierList(ctx.identifierList)
          // TODO: Since there is currently one table function `range`, we just assign an output
          // name here. But, we need to make this logic more general in future.
          val projectList = Alias(UnresolvedAttribute("id"), aliases.head)() :: Nil
          SubqueryAlias(prefix, Project(projectList, funcPlan))
        } else {
          SubqueryAlias(prefix, funcPlan)
        }
      } else {
        funcPlan
      }
    }
  }
}
