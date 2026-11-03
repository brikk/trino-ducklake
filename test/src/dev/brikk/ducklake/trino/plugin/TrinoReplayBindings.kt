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

import dev.brikk.house.sql.ast.Column
import dev.brikk.house.sql.ast.Identifier
import dev.brikk.house.sql.ast.Table
import dev.brikk.house.sql.ast.toIdentifier
import dev.brikk.house.sql.dialects.Dialects
import dev.brikk.house.sql.optimizer.Scope
import dev.brikk.house.sql.optimizer.traverseScope

/** Bind only lake-qualified relations; quoted names, literals and table aliases retain their meaning. */
internal object TrinoReplayBindings {
    fun referencesLake(sql: String, alias: String): Boolean =
        Dialects.TRINO.parseOne(sql).findAll(Table::class).filterIsInstance<Table>().any { table ->
            table.thisArg is Identifier &&
                (sameIdentifier(table.catalog, alias) || table.catalog.isEmpty() && sameIdentifier(table.db, alias))
        }

    fun rewrite(sql: String, alias: String, catalog: String): String {
        val tree = Dialects.TRINO.parseOne(sql)
        for (scope in traverseScope(tree)) {
            if (shadowsCatalogAlias(scope, alias)) continue
            for (column in scope.columns) rewriteColumn(column, alias, catalog)
        }
        for (table in tree.findAll(Table::class).filterIsInstance<Table>()) {
            if (table.thisArg !is Identifier) continue
            when {
                sameIdentifier(table.catalog, alias) -> table.set("catalog", toIdentifier(catalog))
                table.catalog.isEmpty() && sameIdentifier(table.db, alias) -> {
                    table.set("catalog", toIdentifier(catalog))
                    table.set("db", toIdentifier("main"))
                }
            }
        }
        return Dialects.TRINO.generate(tree)
    }

    private fun rewriteColumn(column: Column, alias: String, catalog: String) {
        when {
            sameIdentifier(column.catalog, alias) -> column.set("catalog", toIdentifier(catalog))
            column.catalog.isEmpty() && sameIdentifier(column.db, alias) -> {
                column.set("catalog", toIdentifier(catalog))
                column.set("db", toIdentifier("main"))
            }
        }
    }

    private fun shadowsCatalogAlias(scope: Scope, alias: String): Boolean {
        var current: Scope? = scope
        while (current != null) {
            if (current.selectedSources.keys.any { sameIdentifier(it, alias) }) return true
            current = current.parent
        }
        return false
    }

    private fun sameIdentifier(left: String, right: String): Boolean = left.length == right.length &&
        left.indices.all { asciiFold(left[it]) == asciiFold(right[it]) }

    private fun asciiFold(value: Char): Char = if (value in 'A'..'Z') value.lowercaseChar() else value
}
