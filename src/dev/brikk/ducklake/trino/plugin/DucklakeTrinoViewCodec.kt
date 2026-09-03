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

import dev.brikk.ducklake.catalog.DucklakeQuotedList
import dev.brikk.ducklake.catalog.DucklakeView
import io.trino.spi.TrinoException
import io.trino.spi.StandardErrorCode.INVALID_VIEW
import io.trino.spi.connector.CatalogSchemaName
import io.trino.spi.connector.ConnectorViewDefinition
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.type.TypeId
import java.util.Optional

/**
 * Maps a Trino [ConnectorViewDefinition] onto the DuckLake spec's view storage — and back —
 * using only slots upstream DuckDB understands:
 *
 * | Trino field                     | DuckLake slot                                              |
 * |---------------------------------|------------------------------------------------------------|
 * | `originalSql`                   | `ducklake_view.sql`                                        |
 * | —                               | `ducklake_view.dialect = 'trino'`                          |
 * | `columns[].name`                | `ducklake_view.column_aliases` (spec quoted list)          |
 * | `comment`                       | tag `comment` (interoperable with DuckDB `COMMENT ON VIEW`) |
 * | `columns[].type`                | tag `trino.column_types` — quoted list aligned with aliases |
 * | `columns[].comment`             | tag `trino.column_comments` — aligned; `""` means none     |
 * | `catalog` / `schema` / `owner`  | tags `trino.catalog` / `trino.schema` / `trino.owner`      |
 * | `runAsInvoker`                  | tag `trino.run_as_invoker` = `true` / `false`              |
 * | `path`                          | tag `trino.path` — quoted list `cat1,schema1,cat2,schema2`  |
 *
 * `ducklake_view.column_aliases` MUST be a spec quoted list — upstream parses it at catalog
 * load and refuses the whole catalog otherwise. Engine metadata therefore rides on
 * `ducklake_tag` rows (`object_id = view_id`), which upstream loads opaquely. Tag values are
 * plain text; nothing here is JSON.
 *
 * **Other Trino connectors.** `dialect = 'trino'` only says the SQL is Trino SQL. A different
 * DuckLake connector for Trino may write that dialect with different (or no) tags. We serve a
 * `trino` view only when the tags we need are present and consistent; otherwise [decode] throws
 * an [INVALID_VIEW] error naming the view, the reason, and the remedy (recreate it from this
 * connector). Such views are still *listed*, so operators can find and drop them. Dialects of
 * the form `trino/<variant>` are treated the same way (a Trino-SQL view written in a
 * variant format we do not read).
 */
internal object DucklakeTrinoViewCodec {
    /** The dialect this connector writes. Plain — no variant suffix. */
    const val DIALECT: String = "trino"

    const val TAG_COLUMN_TYPES: String = "trino.column_types"
    const val TAG_COLUMN_COMMENTS: String = "trino.column_comments"
    const val TAG_CATALOG: String = "trino.catalog"
    const val TAG_SCHEMA: String = "trino.schema"
    const val TAG_OWNER: String = "trino.owner"
    const val TAG_RUN_AS_INVOKER: String = "trino.run_as_invoker"
    const val TAG_PATH: String = "trino.path"

    /** Column names + tags to persist for [definition]. */
    data class Encoded(val columnAliases: List<String>, val tags: Map<String, String?>)

    /**
     * True when the view's SQL is Trino SQL — dialect `trino` or `trino/<variant>`. Such views
     * are listed by this connector; whether they can be *served* is decided by [decode].
     */
    fun isTrinoFamily(view: DucklakeView): Boolean =
        view.dialect.substringBefore('/').equals(DIALECT, ignoreCase = true)

    /** True when [decode] would succeed — exactly our dialect with consistent tags. */
    fun isServable(view: DucklakeView): Boolean = incompatibilityReason(view) == null

    fun encode(definition: ConnectorViewDefinition): Encoded {
        val columns = definition.columns
        val tags = LinkedHashMap<String, String?>()
        tags[DucklakeView.COMMENT_TAG_KEY] = definition.comment.orElse(null)
        tags[TAG_COLUMN_TYPES] = DucklakeQuotedList.encode(columns.map { it.type.id })
        if (columns.any { it.comment.isPresent }) {
            tags[TAG_COLUMN_COMMENTS] = DucklakeQuotedList.encode(columns.map { it.comment.orElse("") })
        }
        tags[TAG_CATALOG] = definition.catalog.orElse(null)
        tags[TAG_SCHEMA] = definition.schema.orElse(null)
        tags[TAG_OWNER] = definition.owner.orElse(null)
        tags[TAG_RUN_AS_INVOKER] = definition.isRunAsInvoker.toString()
        if (definition.path.isNotEmpty()) {
            tags[TAG_PATH] = DucklakeQuotedList.encode(definition.path.flatMap { listOf(it.catalogName, it.schemaName) })
        }
        return Encoded(columns.map { it.name }, tags)
    }

    /**
     * Rebuilds the definition, or throws [TrinoException] ([INVALID_VIEW]) with a precise
     * reason when [view] was not written in this connector's format.
     */
    fun decode(view: DucklakeView, viewName: SchemaTableName): ConnectorViewDefinition {
        incompatibilityReason(view)?.let { reason ->
            throw TrinoException(
                INVALID_VIEW,
                "View $viewName cannot be executed by this connector: $reason. " +
                    "It was written by a different writer (dialect '${view.dialect}'). " +
                    "Recreate it from this connector with CREATE OR REPLACE VIEW, or DROP VIEW to remove it.",
            )
        }
        val tags = view.tags
        val types = DucklakeQuotedList.parse(tags.getValue(TAG_COLUMN_TYPES))
        val comments = tags[TAG_COLUMN_COMMENTS]?.let { DucklakeQuotedList.parse(it) }
        val columns = view.columnAliases.mapIndexed { i, name ->
            val comment = comments?.getOrNull(i)?.takeIf { it.isNotEmpty() }
            ViewColumn(name, TypeId.of(types[i]), Optional.ofNullable(comment))
        }
        val path = tags[TAG_PATH]?.let { DucklakeQuotedList.parse(it) }
            ?.chunked(2) { CatalogSchemaName(it[0], it[1]) }
            ?: emptyList()
        return ConnectorViewDefinition(
            view.sql,
            Optional.ofNullable(tags[TAG_CATALOG]),
            Optional.ofNullable(tags[TAG_SCHEMA]),
            columns,
            Optional.ofNullable(view.comment),
            Optional.ofNullable(tags[TAG_OWNER]),
            tags[TAG_RUN_AS_INVOKER]?.toBooleanStrictOrNull() ?: false,
            path,
        )
    }

    /**
     * Null when [view] is decodable; otherwise a human-readable reason. Checks are ordered
     * from "wrong writer" to "corrupt tags" so the message points at the most likely cause.
     */
    fun incompatibilityReason(view: DucklakeView): String? {
        if (!view.dialect.equals(DIALECT, ignoreCase = true)) {
            return if (isTrinoFamily(view)) {
                "its dialect '${view.dialect}' is a Trino variant this connector does not read"
            }
            else {
                "its dialect is '${view.dialect}', not '$DIALECT'"
            }
        }
        view.malformedColumnAliases?.let {
            return "ducklake_view.column_aliases is not a spec quoted list (upstream DuckDB would refuse " +
                "to load this catalog): ${abbreviate(it)}"
        }
        val arity = view.columnAliases.size
        val rawTypes = view.tags[TAG_COLUMN_TYPES]
            ?: return "the '$TAG_COLUMN_TYPES' tag (Trino column types, one per column alias) is missing"
        alignedListProblem(TAG_COLUMN_TYPES, rawTypes, arity)?.let { return it }
        if (arity == 0) {
            return "it declares no output columns"
        }
        view.tags[TAG_COLUMN_COMMENTS]?.let { raw -> alignedListProblem(TAG_COLUMN_COMMENTS, raw, arity)?.let { return it } }
        view.tags[TAG_PATH]?.let { raw ->
            val path = parseOrNull(raw)
                ?: return "the '$TAG_PATH' tag is not a spec quoted list: ${abbreviate(raw)}"
            if (path.size % 2 != 0) {
                return "the '$TAG_PATH' tag must hold catalog/schema pairs but has ${path.size} entries"
            }
        }
        view.tags[TAG_RUN_AS_INVOKER]?.let { raw ->
            if (raw.toBooleanStrictOrNull() == null) {
                return "the '$TAG_RUN_AS_INVOKER' tag must be 'true' or 'false' but is '${abbreviate(raw)}'"
            }
        }
        return null
    }

    /** Problem with a tag that must be a quoted list with exactly [arity] entries, or null. */
    private fun alignedListProblem(tagKey: String, raw: String, arity: Int): String? {
        val values = parseOrNull(raw)
            ?: return "the '$tagKey' tag is not a spec quoted list: ${abbreviate(raw)}"
        if (values.size != arity) {
            return "the '$tagKey' tag has ${values.size} entries but column_aliases has $arity"
        }
        return null
    }

    private fun parseOrNull(raw: String): List<String>? =
        try {
            DucklakeQuotedList.parse(raw)
        }
        catch (_: IllegalArgumentException) {
            null
        }

    private fun abbreviate(s: String): String = if (s.length > MAX_QUOTED) s.take(MAX_QUOTED) + "…" else s

    private const val MAX_QUOTED = 80
}
