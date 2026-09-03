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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.Optional

/**
 * Pins the Trino view ↔ DuckLake (aliases + tags) mapping and, above all, the
 * fail-nicely contract for `dialect = 'trino'` views written by some OTHER Trino connector
 * in a format we don't read: listed, but refused with an INVALID_VIEW error that names the
 * view, the reason, and the remedy — never a silently wrong or empty definition.
 */
class TestDucklakeTrinoViewCodec {
    private val name = SchemaTableName("s", "v")

    private fun view(
        dialect: String = DucklakeTrinoViewCodec.DIALECT,
        aliases: List<String> = listOf("a", "b"),
        tags: Map<String, String> = mapOf(DucklakeTrinoViewCodec.TAG_COLUMN_TYPES to "\"bigint\",\"varchar\""),
        malformed: String? = null,
    ) = DucklakeView(7L, "uuid", 2L, "v", "SELECT 1 AS a, 'x' AS b", dialect, aliases, tags, 10L, null, malformed)

    @Test
    fun roundTripFullDefinition() {
        val definition = ConnectorViewDefinition(
            "SELECT id, \"we,ird\" FROM t",
            Optional.of("ducklake"),
            Optional.of("test_schema"),
            listOf(
                ViewColumn("id", TypeId.of("bigint"), Optional.of("the id")),
                ViewColumn("we,ird", TypeId.of("row(x integer, y varchar)"), Optional.empty()),
            ),
            Optional.of("view \"comment\""),
            Optional.of("alice"),
            false,
            listOf(CatalogSchemaName("c1", "s1"), CatalogSchemaName("c.2", "s,2")),
        )

        val encoded = DucklakeTrinoViewCodec.encode(definition)
        assertThat(encoded.columnAliases).containsExactly("id", "we,ird")
        assertThat(encoded.tags).containsEntry(DucklakeView.COMMENT_TAG_KEY, "view \"comment\"")
        assertThat(encoded.tags).containsEntry(DucklakeTrinoViewCodec.TAG_COLUMN_TYPES, "\"bigint\",\"row(x integer, y varchar)\"")
        assertThat(encoded.tags).containsEntry(DucklakeTrinoViewCodec.TAG_COLUMN_COMMENTS, "\"the id\",\"\"")
        assertThat(encoded.tags).containsEntry(DucklakeTrinoViewCodec.TAG_CATALOG, "ducklake")
        assertThat(encoded.tags).containsEntry(DucklakeTrinoViewCodec.TAG_SCHEMA, "test_schema")
        assertThat(encoded.tags).containsEntry(DucklakeTrinoViewCodec.TAG_OWNER, "alice")
        assertThat(encoded.tags).containsEntry(DucklakeTrinoViewCodec.TAG_RUN_AS_INVOKER, "false")
        assertThat(encoded.tags).containsEntry(DucklakeTrinoViewCodec.TAG_PATH, "\"c1\",\"s1\",\"c.2\",\"s,2\"")
        // No JSON anywhere, and every list is upstream-parseable.
        encoded.tags.values.filterNotNull().forEach { assertThat(it).doesNotStartWith("{") }
        assertThat(DucklakeQuotedList.parse(encoded.tags[DucklakeTrinoViewCodec.TAG_COLUMN_TYPES])).hasSize(2)

        val stored = view(
            aliases = encoded.columnAliases,
            tags = encoded.tags.filterValues { it != null }.mapValues { it.value!! },
        ).copy(sql = definition.originalSql)
        val decoded = DucklakeTrinoViewCodec.decode(stored, name)

        assertThat(decoded.originalSql).isEqualTo(definition.originalSql)
        assertThat(decoded.catalog).isEqualTo(definition.catalog)
        assertThat(decoded.schema).isEqualTo(definition.schema)
        assertThat(decoded.comment).isEqualTo(definition.comment)
        assertThat(decoded.owner).isEqualTo(definition.owner)
        assertThat(decoded.isRunAsInvoker).isFalse()
        assertThat(decoded.path).isEqualTo(definition.path)
        assertThat(decoded.columns.map { it.name }).containsExactly("id", "we,ird")
        assertThat(decoded.columns.map { it.type.id }).containsExactly("bigint", "row(x integer, y varchar)")
        assertThat(decoded.columns.map { it.comment }).containsExactly(Optional.of("the id"), Optional.empty())
    }

    @Test
    fun runAsInvokerRoundTrips() {
        val definition = ConnectorViewDefinition(
            "SELECT 1 AS a", Optional.empty(), Optional.empty(),
            listOf(ViewColumn("a", TypeId.of("integer"), Optional.empty())),
            Optional.empty(), Optional.empty(), true, emptyList(),
        )
        val encoded = DucklakeTrinoViewCodec.encode(definition)
        assertThat(encoded.tags).containsEntry(DucklakeTrinoViewCodec.TAG_RUN_AS_INVOKER, "true")
        val decoded = DucklakeTrinoViewCodec.decode(
            view(aliases = listOf("a"), tags = encoded.tags.filterValues { it != null }.mapValues { it.value!! }),
            name,
        )
        assertThat(decoded.isRunAsInvoker).isTrue()
        assertThat(decoded.owner).isEmpty()
    }

    @Test
    fun minimalDefinitionOmitsOptionalTags() {
        val definition = ConnectorViewDefinition(
            "SELECT 1 AS a", Optional.empty(), Optional.empty(),
            listOf(ViewColumn("a", TypeId.of("integer"), Optional.empty())),
            Optional.empty(), Optional.empty(), false, emptyList(),
        )
        val encoded = DucklakeTrinoViewCodec.encode(definition)
        assertThat(encoded.tags.filterValues { it != null }.keys)
            .containsExactlyInAnyOrder(DucklakeTrinoViewCodec.TAG_COLUMN_TYPES, DucklakeTrinoViewCodec.TAG_RUN_AS_INVOKER)
        val decoded = DucklakeTrinoViewCodec.decode(
            view(aliases = listOf("a"), tags = mapOf(DucklakeTrinoViewCodec.TAG_COLUMN_TYPES to "\"integer\"")),
            name,
        )
        assertThat(decoded.catalog).isEmpty()
        assertThat(decoded.comment).isEmpty()
        assertThat(decoded.isRunAsInvoker).isFalse()
        assertThat(decoded.path).isEmpty()
    }

    @Test
    fun ourDialectIsPlainTrino() {
        assertThat(DucklakeTrinoViewCodec.DIALECT).isEqualTo("trino")
        assertThat(DucklakeTrinoViewCodec.isTrinoFamily(view(dialect = "trino"))).isTrue()
        assertThat(DucklakeTrinoViewCodec.isTrinoFamily(view(dialect = "TRINO"))).isTrue()
        assertThat(DucklakeTrinoViewCodec.isTrinoFamily(view(dialect = "trino/someone-else"))).isTrue()
        assertThat(DucklakeTrinoViewCodec.isTrinoFamily(view(dialect = "duckdb"))).isFalse()
        assertThat(DucklakeTrinoViewCodec.isTrinoFamily(view(dialect = "trinoish"))).isFalse()
    }

    @Test
    fun foreignTrinoConnectorViewsAreRefusedWithReason() {
        // Another Trino connector: right dialect, no tags at all.
        assertRefused(view(tags = emptyMap()), "trino.column_types", "missing")
        // Another Trino connector: right dialect, its own idea of a tag payload.
        assertRefused(
            view(tags = mapOf(DucklakeTrinoViewCodec.TAG_COLUMN_TYPES to "{\"types\":[\"bigint\"]}")),
            "trino.column_types", "not a spec quoted list",
        )
        // Types don't line up with the aliases.
        assertRefused(
            view(tags = mapOf(DucklakeTrinoViewCodec.TAG_COLUMN_TYPES to "\"bigint\"")),
            "1 entries", "column_aliases has 2",
        )
        // A Trino-family variant dialect (e.g. a fork's suffix) — we don't read it.
        assertRefused(view(dialect = "trino/other"), "Trino variant")
        // Non-Trino dialect never reaches decode in practice (hidden by listing), but is refused too.
        assertRefused(view(dialect = "duckdb"), "not 'trino'")
        // The legacy shape: JSON stuffed into column_aliases. Flagged by the catalog layer.
        assertRefused(view(aliases = emptyList(), malformed = "{\"originalSql\":\"SELECT 1\"}"), "column_aliases", "quoted list")
        // Inconsistent optional tags are refused rather than half-applied.
        assertRefused(
            view(tags = mapOf(DucklakeTrinoViewCodec.TAG_COLUMN_TYPES to "\"bigint\",\"varchar\"", DucklakeTrinoViewCodec.TAG_PATH to "\"only-one\"")),
            "trino.path", "pairs",
        )
        assertRefused(
            view(tags = mapOf(DucklakeTrinoViewCodec.TAG_COLUMN_TYPES to "\"bigint\",\"varchar\"", DucklakeTrinoViewCodec.TAG_RUN_AS_INVOKER to "maybe")),
            "trino.run_as_invoker",
        )
        assertRefused(
            view(tags = mapOf(DucklakeTrinoViewCodec.TAG_COLUMN_TYPES to "\"bigint\",\"varchar\"", DucklakeTrinoViewCodec.TAG_COLUMN_COMMENTS to "\"x\"")),
            "trino.column_comments", "1 entries",
        )
        // Our own writes are always servable.
        assertThat(DucklakeTrinoViewCodec.isServable(view())).isTrue()
    }

    private fun assertRefused(view: DucklakeView, vararg messageParts: String) {
        assertThat(DucklakeTrinoViewCodec.isServable(view)).isFalse()
        val thrown = assertThatThrownBy { DucklakeTrinoViewCodec.decode(view, name) }
            .isInstanceOf(TrinoException::class.java)
            .hasMessageContaining("View s.v cannot be executed by this connector")
            .hasMessageContaining("CREATE OR REPLACE VIEW")
        for (part in messageParts) {
            thrown.hasMessageContaining(part)
        }
        thrown.extracting { (it as TrinoException).errorCode }.isEqualTo(INVALID_VIEW.toErrorCode())
    }
}
