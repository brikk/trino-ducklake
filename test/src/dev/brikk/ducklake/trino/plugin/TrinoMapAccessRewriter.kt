/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import dev.brikk.house.sql.ast.Bracket
import dev.brikk.house.sql.ast.Column
import dev.brikk.house.sql.ast.DType
import dev.brikk.house.sql.ast.DataType
import dev.brikk.house.sql.ast.Dot
import dev.brikk.house.sql.ast.Expression
import dev.brikk.house.sql.ast.Identifier
import dev.brikk.house.sql.ast.Literal
import dev.brikk.house.sql.ast.parts
import dev.brikk.house.sql.dialects.Dialects
import dev.brikk.house.sql.optimizer.MappingSchema
import dev.brikk.house.sql.optimizer.annotateTypes
import dev.brikk.house.sql.optimizer.qualify

/** Test-only DuckDB string-key MAP-dot adaptation after ordinary DuckDB -> Trino transpilation. */
object TrinoMapAccessRewriter {
    private const val ORIGINAL_NAME = "trino_corpus_original_name"
    private const val MAP_VALUE_TYPE = "trino_corpus_map_value_type"

    fun rewrite(
            sql: String,
            defaultCatalog: String? = null,
            defaultSchema: String? = null,
            columns: (String) -> Map<String, String>): String {
        val dialect = Dialects.TRINO
        val tree = dialect.parseOne(sql)
        val hasAccess = tree.findAll(Dot::class, Column::class).any {
            it is Dot || (it is Column && it.parts.size > 1)
        }
        if (!hasAccess) return sql
        tree.findAll(Identifier::class).forEach { it.meta[ORIGINAL_NAME] = it.name }
        val schema = bindSchema(tree, defaultCatalog, defaultSchema, columns) ?: return sql
        var changed = false
        // Each successful pass removes at least one MAP Dot. The extra pass establishes that
        // dependent accesses in nested expressions and CTEs have all been processed.
        val maxPasses = tree.findAll(Dot::class).count() + 1
        repeat(maxPasses) {
            // The annotator handles ARRAY brackets but not MAP-column brackets. Supply the MAP
            // value type proven below, and recompute the dependent expression/scope types.
            tree.walk().forEach { node -> node.typeSlot = node.meta[MAP_VALUE_TYPE] as? Expression }
            annotateTypes(tree, schema = schema, dialect = dialect, overwriteTypes = false)
            val accesses = tree.findAll(Dot::class, bfs = false).filter(::isStringMapAccess).toList()
            if (accesses.isEmpty()) return if (changed) dialect.generate(tree) else sql
            accesses.asReversed().forEach(::replaceMapAccess)
            changed = true
        }
        error("MAP access rewriting did not converge")
    }

    private fun bindSchema(
            tree: Expression,
            defaultCatalog: String?,
            defaultSchema: String?,
            columns: (String) -> Map<String, String>): MappingSchema? {
        val dialect = Dialects.TRINO
        val schema = MappingSchema(dialect = dialect)
        val probed = mutableMapOf<String, Map<String, String>>()
        val schemasByName = mutableMapOf<List<String>, List<Pair<String, String>>>()
        var conflictingSchemas = false
        qualify(tree, dialect = dialect, catalog = defaultCatalog, db = defaultSchema, schema = schema,
                expandStars = false, allowPartialQualification = true, validateQualifyColumns = false, onQualify = { table ->
                    if (table.thisArg is Identifier) {
                        val reference = dialect.generate(table.copy().apply { set("alias", null) })
                        val types = probed.getOrPut(reference) { columns(reference) }
                        val orderedTypes = types.toList()
                        val previous = schemasByName.putIfAbsent(table.parts.map { it.name }, orderedTypes)
                        if (previous != null && previous != orderedTypes) conflictingSchemas = true
                        schema.addTable(table, types)
                    }
                })
        // MappingSchema is name-keyed, not snapshot-keyed. Do not guess when one query binds
        // different historical schemas of the same table; execute its original SQL normally.
        return schema.takeUnless { conflictingSchemas }
    }

    private fun isStringMapAccess(node: Expression): Boolean {
        if (node !is Dot || node.expressionArg !is Identifier) return false
        val type = (node.thisArg as? Expression)?.type as? DataType ?: return false
        if (!type.isType(DType.MAP)) return false
        return (type.expressionsArg.firstOrNull() as? DataType)?.thisArg in DataType.TEXT_TYPES
    }

    private fun replaceMapAccess(access: Expression) {
        val name = access.expressionArg as Identifier
        val key = name.meta[ORIGINAL_NAME] as? String ?: name.name
        val base = access.thisArg as Expression
        val valueType = (base.type as DataType).expressionsArg[1] as Expression
        // A safe Bracket emits ELEMENT_AT in Trino: an absent key yields NULL, not an exception.
        val replacement = Bracket(mapOf("this" to base.copy(),
                "expressions" to listOf(Literal.string(key)), "safe" to true))
        replacement.meta[MAP_VALUE_TYPE] = valueType.copy()
        access.replace(replacement)
    }
}
