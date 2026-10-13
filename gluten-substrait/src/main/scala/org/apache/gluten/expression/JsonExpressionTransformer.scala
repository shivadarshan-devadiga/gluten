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
package org.apache.gluten.expression

import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.`type`.ListNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions.{Expression, JsonToStructs, StructsToJson}

import com.google.common.collect.Lists

import scala.collection.JavaConverters._

case class JsonTupleExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer {

  override def doTransform(context: SubstraitContext): ExpressionNode = {
    val jsonExpr = children.head
    val fields = children.tail
    val jsonExprNode = jsonExpr.doTransform(context)
    val expressNodes = Lists.newArrayList(jsonExprNode)
    fields.foreach(f => expressNodes.add(f.doTransform(context)))
    val functionName =
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.REQ)
    val functionId = context.registerFunction(functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    typeNode match {
      case node: ListNode =>
        val elementType = node.getNestedType
        ExpressionBuilder.makeScalarFunction(functionId, expressNodes, elementType)
      case _ =>
        ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
    }
  }
}

case class ToJsonTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: StructsToJson)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = {
    val timeZoneId = original.timeZoneId.map(timeZoneId => LiteralTransformer(timeZoneId))
    Seq(child) ++ timeZoneId
  }
}

/**
 * Spark's from_json matches JSON keys against the schema field names case-sensitively, whatever
 * `spark.sql.caseSensitive` is, so the schema passed to the native function keeps the original
 * field names. The result is then cast (by position) to the type the rest of the plan uses, as the
 * native side compares struct field names in some type checks, e.g. the branches of `if`.
 */
case class FromJsonTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: JsonToStructs)
  extends ExpressionTransformer {

  override def doTransform(context: SubstraitContext): ExpressionNode = {
    val funcName =
      ConverterUtils.makeFuncName(substraitExprName, original.children.map(_.dataType))
    val functionId = context.registerFunction(funcName)
    val childNodes = children.map(_.doTransform(context)).asJava
    val schemaTypeNode =
      ConverterUtils.getTypeNode(dataType, nullable, structFieldToLowerCase = false)
    val fromJsonNode = ExpressionBuilder.makeScalarFunction(functionId, childNodes, schemaTypeNode)
    val planTypeNode = ConverterUtils.getTypeNode(dataType, nullable)
    if (planTypeNode.toProtobuf == schemaTypeNode.toProtobuf) {
      fromJsonNode
    } else {
      ExpressionBuilder.makeCast(planTypeNode, fromJsonNode, false)
    }
  }
}
