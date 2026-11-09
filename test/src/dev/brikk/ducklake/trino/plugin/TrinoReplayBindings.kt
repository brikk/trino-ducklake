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
import dev.brikk.house.sql.ast.Create
import dev.brikk.house.sql.ast.Drop
import dev.brikk.house.sql.ast.Expression
import dev.brikk.house.sql.ast.Identifier
import dev.brikk.house.sql.ast.Schema
import dev.brikk.house.sql.ast.Table
import dev.brikk.house.sql.ast.TemporaryProperty
import dev.brikk.house.sql.ast.Use
import dev.brikk.house.sql.ast.toIdentifier
import dev.brikk.house.sql.dialects.Dialects
import dev.brikk.house.sql.optimizer.Scope
import dev.brikk.house.sql.optimizer.traverseScope
import dev.brikk.house.sql.parser.ParseError
import dev.brikk.house.sql.parser.TokenType
import java.util.Collections
import java.util.IdentityHashMap

/** Bind only lake-qualified relations; quoted names, literals and table aliases retain their meaning. */
internal object TrinoReplayBindings {
    /**
     * Unqualified names in a query whose oracle connection has `USE`d the lake. DuckDB resolves a
     * bare name to a temp relation first, then to a relation of [schema], and otherwise to a
     * replacement scan (a file); `x.t` names lake schema `x` unless `x` is a database alias. Only
     * names [isLakeRelation] finds in the lake's current snapshot are lake reads.
     */
    class LakeDefault(
        val schema: String,
        val tempRelations: Set<String>,
        val isLakeRelation: (schema: String, name: String) -> Boolean,
    )

    /**
     * One oracle connection's DuckDB name-resolution defaults. A fresh connection starts in the
     * in-memory database; [catalog] is null after a `USE` that could not be interpreted.
     */
    class OracleNames {
        var catalog: String? = "memory"
        var schema: String = "main"
        val tempRelations: MutableSet<String> = mutableSetOf()

        fun lakeDefault(alias: String, isLakeRelation: (String, String) -> Boolean): LakeDefault? =
            catalog?.takeIf { sameIdentifier(it, alias) }?.let { LakeDefault(schema, tempRelations.toSet(), isLakeRelation) }
    }

    private val NAME_SCOPE_TOKENS = setOf(TokenType.USE, TokenType.CREATE, TokenType.DROP)

    /**
     * Applies the `USE`, temp-relation `CREATE` and `DROP` statements in oracle [sql] to [names].
     * [isLakeSchema] answers whether a catalog alias is an inspectable lake holding a schema, which
     * decides DuckDB's `USE x` ambiguity: a schema of the current database wins over a database.
     */
    fun observe(sql: String, names: OracleNames, isLakeSchema: (catalog: String, schema: String) -> Boolean) {
        // A statement DuckDB cannot tokenize fails in the oracle too and changes nothing there.
        val tokens = runCatching { Dialects.DUCKDB.tokenize(sql) }.getOrNull() ?: return
        if (tokens.none { it.tokenType in NAME_SCOPE_TOKENS }) return
        val statements = try {
            Dialects.DUCKDB.parse(sql).filterNotNull()
        } catch (_: ParseError) {
            if (tokens.any { it.tokenType == TokenType.USE }) names.catalog = null
            return
        }
        for (statement in statements) {
            when (statement) {
                is Use -> use(statement, names, isLakeSchema)
                is Create -> createdTemp(statement)?.let { names.tempRelations += it }
                is Drop -> (statement.args["tables"] as? List<*>).orEmpty().filterIsInstance<Table>()
                    .filter { it.catalog.isEmpty() && it.db.isEmpty() }
                    .forEach { dropped -> names.tempRelations.removeIf { sameIdentifier(it, dropped.name) } }
            }
        }
    }

    private fun use(statement: Use, names: OracleNames, isLakeSchema: (String, String) -> Boolean) {
        val target = statement.thisArg as? Table
        val current = names.catalog
        when {
            target == null || target.catalog.isNotEmpty() -> names.catalog = null
            target.db.isNotEmpty() -> {
                names.catalog = target.db
                names.schema = target.name
            }
            current != null && isLakeSchema(current, target.name) -> names.schema = target.name
            else -> {
                names.catalog = target.name
                names.schema = "main"
            }
        }
    }

    private fun createdTemp(statement: Create): String? {
        val kind = statement.text("kind")
        if (!kind.equals("TABLE", ignoreCase = true) && !kind.equals("VIEW", ignoreCase = true)) return null
        val table = when (val target = statement.thisArg) {
            is Schema -> target.thisArg as? Table
            is Table -> target
            else -> null
        } ?: return null
        val temporary = statement.find(TemporaryProperty::class) != null ||
            sameIdentifier(table.catalog, "temp") || table.catalog.isEmpty() && sameIdentifier(table.db, "temp")
        return table.name.takeIf { temporary }
    }

    fun referencesLake(sql: String, alias: String, default: LakeDefault? = null): Boolean {
        val tree = Dialects.TRINO.parseOne(sql)
        val sourceTables = sourceTables(tree, default)
        return tree.findAll(Table::class).filterIsInstance<Table>().any { lakeSchema(it, alias, default, sourceTables) != null }
    }

    fun rewrite(sql: String, alias: String, catalog: String, default: LakeDefault? = null): String {
        val tree = Dialects.TRINO.parseOne(sql)
        val sourceTables = sourceTables(tree, default)
        for (scope in traverseScope(tree)) {
            if (shadowsCatalogAlias(scope, alias)) continue
            for (column in scope.columns) rewriteColumn(column, alias, catalog)
        }
        for (table in tree.findAll(Table::class).filterIsInstance<Table>()) {
            val schema = lakeSchema(table, alias, default, sourceTables) ?: continue
            val keepsSchema = table.catalog.isNotEmpty() || table.db.isNotEmpty() && !sameIdentifier(table.db, alias)
            table.set("catalog", toIdentifier(catalog))
            if (!keepsSchema) table.set("db", toIdentifier(schema))
        }
        return Dialects.TRINO.generate(tree)
    }

    /** The lake schema [table] reads, or null when it is not a lake relation. */
    private fun lakeSchema(table: Table, alias: String, default: LakeDefault?, sourceTables: Set<Table>): String? = when {
        table.thisArg !is Identifier -> null
        sameIdentifier(table.catalog, alias) -> table.db
        table.catalog.isNotEmpty() -> null
        sameIdentifier(table.db, alias) -> "main"
        default == null -> null
        table.db.isNotEmpty() -> table.db.takeIf { default.isLakeRelation(it, table.name) }
        table !in sourceTables || default.tempRelations.any { sameIdentifier(it, table.name) } -> null
        else -> default.schema.takeIf { default.isLakeRelation(it, table.name) }
    }

    /** Tables that scopes resolve as relations rather than as references to a CTE. */
    private fun sourceTables(tree: Expression, default: LakeDefault?): Set<Table> {
        val tables = Collections.newSetFromMap(IdentityHashMap<Table, Boolean>())
        if (default == null) return tables
        for (scope in traverseScope(tree)) tables += scope.sources.values.filterIsInstance<Table>()
        return tables
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

    fun sameIdentifier(left: String, right: String): Boolean = left.length == right.length &&
        left.indices.all { asciiFold(left[it]) == asciiFold(right[it]) }

    private fun asciiFold(value: Char): Char = if (value in 'A'..'Z') value.lowercaseChar() else value
}
