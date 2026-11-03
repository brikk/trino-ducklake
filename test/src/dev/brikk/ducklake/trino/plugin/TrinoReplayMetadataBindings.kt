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

import dev.brikk.house.sql.ast.Table
import dev.brikk.house.sql.ast.Expression
import dev.brikk.house.sql.ast.toIdentifier
import dev.brikk.house.sql.dialects.Dialects
import dev.brikk.house.sql.optimizer.traverseScope

/** Bind only the schema of known rewritten metadata catalogs, never the logical lake or SQL text literals. */
internal object TrinoReplayMetadataBindings {
    fun rewrite(sql: String, metadataSchemas: Map<String, String>): String {
        if (metadataSchemas.keys.none { sql.contains(it, ignoreCase = true) || sql.contains(it.replace("\"", "\"\""), ignoreCase = true) }) return sql
        val tree = Dialects.DUCKDB.parseOne(sql)
        val rebound = bindTables(tree, metadataSchemas)
        if (rebound.isEmpty()) return sql
        bindColumns(tree, metadataSchemas, rebound)
        return Dialects.DUCKDB.generate(tree)
    }

    private fun bindTables(tree: Expression, metadataSchemas: Map<String, String>): Set<String> {
        val rebound = mutableSetOf<String>()
        for (table in tree.findAll(Table::class).filterIsInstance<Table>()) {
            val target = metadataSchemas.entries.firstOrNull { matches(table.catalog, it.key) }
            if (target != null && matches(table.db, "main")) {
                table.set("db", toIdentifier(target.value))
                rebound.add(target.key)
            }
        }
        return rebound
    }

    private fun bindColumns(tree: Expression, metadataSchemas: Map<String, String>, rebound: Set<String>) {
        for (scope in traverseScope(tree)) {
            val shadowed = scope.selectedSources.keys
            for (column in scope.columns) {
                val catalog = rebound.firstOrNull { matches(column.catalog, it) }
                if (catalog != null && shadowed.none { matches(it, catalog) } && matches(column.db, "main"))
                    column.set("db", toIdentifier(metadataSchemas.getValue(catalog)))
            }
        }
    }

    private fun matches(left: String, right: String): Boolean = left.length == right.length && left.indices.all { i ->
        val a = left[i].let { if (it in 'A'..'Z') it.lowercaseChar() else it }
        val b = right[i].let { if (it in 'A'..'Z') it.lowercaseChar() else it }
        a == b
    }
}
