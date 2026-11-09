/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import io.trino.client.FailureInfo
import io.trino.sql.parser.ParsingException
import io.trino.sql.parser.SqlParser
import io.trino.sql.tree.ArithmeticBinaryExpression
import io.trino.sql.tree.ComparisonPredicate
import io.trino.sql.tree.Expression
import io.trino.sql.tree.Node
import io.trino.sql.tree.NodeLocation
import io.trino.sql.tree.Predicated
import io.trino.sql.tree.StringLiteral

internal object TrinoReplayLiteralMismatch {
    private val operatorError = Regex("""line \d+:\d+: Cannot apply operator: (.+) ([<>=!+*/%\-]+) (.+)""")

    fun isLiteralOperand(sql: String, failure: FailureInfo): Boolean {
        val location = failure.errorLocation ?: return false
        val match = operatorError.matchEntire(failure.message.orEmpty().lineSequence().first()) ?: return false
        val (leftType, operator, rightType) = match.destructured
        val tree = try {
            SqlParser().createStatement(sql)
        } catch (_: ParsingException) {
            return false
        }
        return nodes(tree).any { node ->
            val at = node.location.orElse(null)
            if (at != NodeLocation(location.lineNumber, location.columnNumber)) return@any false
            when (node) {
                is Predicated -> {
                    val comparison = node.predicate as? ComparisonPredicate
                    comparison?.operator?.let(::analyzerOperator) == operator &&
                        literalMatches(node.value, leftType, comparison.right, rightType)
                }
                is ArithmeticBinaryExpression -> node.operator.value == operator &&
                    literalMatches(node.left, leftType, node.right, rightType)
                else -> false
            }
        }
    }

    // ExpressionAnalyzer resolves > as LESS_THAN and != as EQUAL, without swapping argument types.
    private fun analyzerOperator(operator: ComparisonPredicate.Operator): String = when (operator) {
        ComparisonPredicate.Operator.EQUAL, ComparisonPredicate.Operator.NOT_EQUAL -> "="
        ComparisonPredicate.Operator.LESS_THAN, ComparisonPredicate.Operator.GREATER_THAN -> "<"
        ComparisonPredicate.Operator.LESS_THAN_OR_EQUAL, ComparisonPredicate.Operator.GREATER_THAN_OR_EQUAL -> "<="
    }

    private fun literalMatches(left: Expression, leftType: String, right: Expression, rightType: String): Boolean =
        literalMatches(left, leftType) || literalMatches(right, rightType)

    private fun literalMatches(operand: Expression, type: String): Boolean =
        operand is StringLiteral && type == "varchar(${operand.length()})"

    private fun nodes(node: Node): Sequence<Node> = sequence {
        yield(node)
        node.children.forEach { yieldAll(nodes(it)) }
    }
}
