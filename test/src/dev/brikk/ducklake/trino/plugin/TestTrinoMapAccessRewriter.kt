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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TestTrinoMapAccessRewriter {
    private val columns = mapOf("s" to "map(varchar, integer)", "r" to "row(i integer)")

    @Test
    fun mapAccessUsesElementAtButRowAccessStaysADereference() {
        val result = TrinoMapAccessRewriter.rewrite("SELECT s.i, r.i FROM lake.main.t WHERE s.i = 1") { columns }
        assertThat(result).contains("ELEMENT_AT(", "'i'").doesNotContain("s\".\"i")
        assertThat(result).contains("\"r\".\"i\"")
    }

    @Test
    fun mapKeyCaseAndQuotedPunctuationRemainLiteral() {
        val result = TrinoMapAccessRewriter.rewrite("SELECT s.Mixed, s.\"a'b\", 's.i' FROM lake.main.t") { columns }
        assertThat(result).contains("'Mixed'", "'a''b'", "'s.i'")
    }

    @Test
    fun tableAliasTakesPrecedenceOverAnUnqualifiedMapColumnName() {
        val sql = "SELECT s.i FROM lake.main.t AS s"
        val result = TrinoMapAccessRewriter.rewrite(sql) { columns + ("i" to "integer") }
        assertThat(result).isEqualTo(sql)
    }

    @Test
    fun ctesAndNestedMapsCarryValueTypesAcrossRewrites() {
        val probes = mutableListOf<String>()
        val result = TrinoMapAccessRewriter.rewrite(
                "WITH q AS (SELECT s.outer_key AS m FROM lake.main.t) SELECT q.m.inner_key FROM q") { table ->
            probes += table
            mapOf("s" to "map(varchar, map(varchar, integer))")
        }
        assertThat(probes).containsExactly("lake.main.t")
        assertThat(result).contains("'outer_key'", "'inner_key'")
        assertThat(Regex("ELEMENT_AT").findAll(result).count()).isEqualTo(2)
    }

    @Test
    fun joinsResolveSameNamedMapAndRowColumnsByAlias() {
        val result = TrinoMapAccessRewriter.rewrite(
                "SELECT a.s.i, b.s.i FROM lake.main.maps a CROSS JOIN lake.main.rows b") { table ->
            mapOf("s" to if (table.endsWith("maps")) "map(varchar, integer)" else "row(i integer)")
        }
        assertThat(Regex("ELEMENT_AT").findAll(result).count()).isEqualTo(1)
        assertThat(result).contains("\"b\".\"s\".\"i\"")
    }

    @Test
    fun nestedMapAndRowAccessesStayTyped() {
        val result = TrinoMapAccessRewriter.rewrite("SELECT r.Mixed.Key, m.Key.Mixed FROM lake.main.t") {
            mapOf("r" to "row(Mixed map(varchar, integer))", "m" to "map(varchar, row(Mixed integer))")
        }
        assertThat(Regex("ELEMENT_AT").findAll(result).count()).isEqualTo(2)
        assertThat(result).contains("'Key'")
    }

    @Test
    fun mixedTableQualificationUsesTheCurrentSessionNamespace() {
        val probes = mutableListOf<String>()
        val result = TrinoMapAccessRewriter.rewrite(
                "SELECT a.s.i, b.r.i FROM lake.main.t a CROSS JOIN t b", "lake", "main") { table ->
            probes += table
            columns
        }
        assertThat(probes).containsExactly("lake.main.t")
        assertThat(Regex("ELEMENT_AT").findAll(result).count()).isEqualTo(1)
    }

    @Test
    fun schemaProbesRetainAnExplicitSnapshot() {
        val probes = mutableListOf<String>()
        val result = TrinoMapAccessRewriter.rewrite(
                "SELECT s.i FROM lake.main.t FOR VERSION AS OF 7") { table ->
            probes += table
            columns
        }
        assertThat(probes.single()).contains("FOR VERSION AS OF 7")
        assertThat(result).contains("ELEMENT_AT", "FOR VERSION AS OF 7")
    }

    @Test
    fun conflictingHistoricalSchemasAreNotCollapsedIntoOneType() {
        val sql = "SELECT a.s.i FROM lake.main.t FOR VERSION AS OF 1 a " +
                "CROSS JOIN lake.main.t FOR VERSION AS OF 2 b"
        val result = TrinoMapAccessRewriter.rewrite(sql) { table ->
            mapOf("s" to if (table.contains("AS OF 1")) "row(i integer)" else "map(varchar, integer)")
        }
        assertThat(result).isEqualTo(sql)
    }

    @Test
    fun historicalColumnOrderDifferencesAreNotCollapsed() {
        val sql = "SELECT a.r.i FROM lake.main.t FOR VERSION AS OF 1 a(s, r) " +
                "CROSS JOIN lake.main.t FOR VERSION AS OF 2 b(s, r)"
        val first = linkedMapOf("m" to "map(varchar, integer)", "r" to "row(i integer)")
        val second = linkedMapOf("r" to "row(i integer)", "m" to "map(varchar, integer)")
        assertThat(TrinoMapAccessRewriter.rewrite(sql) { if (it.contains("AS OF 1")) first else second }).isEqualTo(sql)
    }

    @Test
    fun nonTextMapKeysAreNotGivenGuessedImplicitCasts() {
        val sql = "SELECT s.\"1\" FROM lake.main.t"
        assertThat(TrinoMapAccessRewriter.rewrite(sql) { mapOf("s" to "map(integer, integer)") }).isEqualTo(sql)
    }

    @Test
    fun queriesWithoutAnAccessNeedNoMetadataProbe() {
        val sql = "SELECT * FROM lake.main.t"
        assertThat(TrinoMapAccessRewriter.rewrite(sql) { error("metadata not needed") }).isEqualTo(sql)
    }
}
