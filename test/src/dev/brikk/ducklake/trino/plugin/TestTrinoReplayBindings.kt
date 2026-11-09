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
import dev.brikk.house.sql.ast.Literal
import dev.brikk.house.sql.ast.Table
import dev.brikk.house.sql.dialects.Dialects
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TestTrinoReplayBindings {
    @Test
    fun quotedSchemaAndTableNamesRemainSingleExactIdentifiers() {
        val result = TrinoReplayBindings.rewrite("SELECT i FROM dl.\"a.b\".\"t\"\"q\"", "dl", "corpus_1")
        val table = Dialects.TRINO.parseOne(result).findAll(Table::class).filterIsInstance<Table>().single()
        assertThat(table.parts.map { it.name }).containsExactly("corpus_1", "a.b", "t\"q")
    }

    @Test
    fun fullyQualifiedColumnsFollowTheirTableIncludingQuotedSchemaNames() {
        val result = TrinoReplayBindings.rewrite("""SELECT dl."a.b".t.i FROM dl."a.b".t""", "dl", "corpus_1")
        val tree = Dialects.TRINO.parseOne(result)
        assertThat(tree.findAll(Table::class).filterIsInstance<Table>().single().parts.map { it.name }).containsExactly("corpus_1", "a.b", "t")
        assertThat(tree.findAll(Column::class).filterIsInstance<Column>().single().parts.map { it.name }).containsExactly("corpus_1", "a.b", "t", "i")
    }

    @Test
    fun implicitMainIsAddedToTablesAndQualifiedColumns() {
        val result = TrinoReplayBindings.rewrite("SELECT dl.t.i FROM dl.t", "dl", "corpus_1")
        val tree = Dialects.TRINO.parseOne(result)
        assertThat(tree.findAll(Table::class).filterIsInstance<Table>().single().parts.map { it.name }).containsExactly("corpus_1", "main", "t")
        assertThat(tree.findAll(Column::class).filterIsInstance<Column>().single().parts.map { it.name }).containsExactly("corpus_1", "main", "t", "i")
    }

    @Test
    fun tableAliasesAndRowDereferencesDoNotBecomeCatalogNames() {
        val result = TrinoReplayBindings.rewrite("SELECT dl.r.i FROM dl.t AS dl", "dl", "corpus_1")
        val tree = Dialects.TRINO.parseOne(result)
        assertThat(tree.findAll(Table::class).filterIsInstance<Table>().single().parts.map { it.name }).containsExactly("corpus_1", "main", "t")
        assertThat(tree.findAll(Column::class).filterIsInstance<Column>().single().parts.map { it.name }).containsExactly("dl", "r", "i")
    }

    @Test
    fun anUnreferencedCteDoesNotShadowAnExplicitCatalogReference() {
        val result = TrinoReplayBindings.rewrite("WITH dl AS (SELECT 1 i) SELECT dl.t.i FROM dl.t", "dl", "corpus_1")
        val column = Dialects.TRINO.parseOne(result).findAll(Column::class).filterIsInstance<Column>().single()
        assertThat(column.parts.map { it.name }).containsExactly("corpus_1", "main", "t", "i")
    }

    @Test
    fun textAndUnrelatedCatalogsAreNotRewritten() {
        val result = TrinoReplayBindings.rewrite("SELECT 'dl.a.b', x.i FROM other.s.t x JOIN dl.t y ON x.i=y.i", "dl", "corpus_1")
        val tree = Dialects.TRINO.parseOne(result)
        assertThat(tree.findAll(Literal::class).single().name).isEqualTo("dl.a.b")
        assertThat(tree.findAll(Table::class).filterIsInstance<Table>().map { it.parts.map { p -> p.name } }.toList())
            .containsExactly(listOf("other", "s", "t"), listOf("corpus_1", "main", "t"))
    }

    @Test
    fun useOfTheLakeMakesItTheDefaultCatalogWithMainSchema() {
        val names = observed("USE dl")
        assertThat(names.catalog).isEqualTo("dl")
        assertThat(names.schema).isEqualTo("main")
        val default = names.lakeDefault("dl") { _, _ -> true }
        assertThat(TrinoReplayBindings.referencesLake("SELECT * FROM Stock ORDER BY item_id", "dl", default)).isTrue()
        assertThat(tables(TrinoReplayBindings.rewrite("SELECT * FROM Stock ORDER BY item_id", "dl", "corpus_1", default)))
            .containsExactly(listOf("corpus_1", "main", "Stock"))
    }

    @Test
    fun useOfCatalogAndSchemaQualifiesBareNamesWithThatSchema() {
        val names = observed("USE dl.s1;")
        assertThat(names.catalog).isEqualTo("dl")
        assertThat(names.schema).isEqualTo("s1")
        assertThat(tables(TrinoReplayBindings.rewrite("SELECT i FROM t", "dl", "corpus_1", names.lakeDefault("dl") { _, _ -> true })))
            .containsExactly(listOf("corpus_1", "s1", "t"))
    }

    @Test
    fun useOfASchemaOfTheCurrentLakeKeepsTheLakeAsCatalog() {
        val names = observed("USE dl", "USE s1") { catalog, schema -> catalog == "dl" && schema == "s1" }
        assertThat(names.catalog).isEqualTo("dl")
        assertThat(names.schema).isEqualTo("s1")
    }

    @Test
    fun schemaQualifiedNamesBindOnlyToExistingLakeSchemas() {
        val default = observed("USE dl").lakeDefault("dl") { schema, _ -> schema == "s1" }
        val result = TrinoReplayBindings.rewrite("SELECT * FROM s1.t JOIN memory.u ON t.i = u.i", "dl", "corpus_1", default)
        assertThat(tables(result)).containsExactlyInAnyOrder(listOf("corpus_1", "s1", "t"), listOf("memory", "u"))
        assertThat(TrinoReplayBindings.referencesLake("SELECT * FROM memory.u", "dl", default)).isFalse()
    }

    @Test
    fun cteNamesShadowBareLakeNames() {
        val default = observed("USE dl").lakeDefault("dl") { _, _ -> true }
        assertThat(TrinoReplayBindings.referencesLake("WITH t AS (SELECT 1 i) SELECT i FROM t", "dl", default)).isFalse()
        val result = TrinoReplayBindings.rewrite("WITH t AS (SELECT i FROM t) SELECT i FROM t", "dl", "corpus_1", default)
        assertThat(tables(result)).containsExactlyInAnyOrder(listOf("corpus_1", "main", "t"), listOf("t"))
    }

    @Test
    fun subqueryAliasesAreNotRelations() {
        val default = observed("USE dl").lakeDefault("dl") { _, _ -> true }
        assertThat(TrinoReplayBindings.referencesLake("SELECT s.x FROM (SELECT 1 x) s", "dl", default)).isFalse()
        assertThat(tables(TrinoReplayBindings.rewrite("SELECT s.x FROM (SELECT x FROM t) s", "dl", "corpus_1", default)))
            .containsExactly(listOf("corpus_1", "main", "t"))
    }

    @Test
    fun duckdbTempRelationsAreNotRewritten() {
        val names = observed(
            "USE dl",
            "CREATE TEMP TABLE src AS SELECT 1 i",
            "CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 2 i",
            "CREATE TABLE temp.main.w(i INT)",
            "CREATE TABLE lake_t(i INT)",
        )
        assertThat(names.tempRelations).containsExactlyInAnyOrder("src", "v", "w")
        val default = names.lakeDefault("dl") { _, _ -> true }
        assertThat(TrinoReplayBindings.referencesLake("SELECT * FROM SRC", "dl", default)).isFalse()
        assertThat(tables(TrinoReplayBindings.rewrite("SELECT * FROM lake_t JOIN src USING (i)", "dl", "corpus_1", default)))
            .containsExactlyInAnyOrder(listOf("corpus_1", "main", "lake_t"), listOf("src"))
        TrinoReplayBindings.observe("DROP TABLE src", names) { _, _ -> false }
        assertThat(names.tempRelations).containsExactlyInAnyOrder("v", "w")
    }

    @Test
    fun bareNamesAbsentFromTheLakeAreReplacementScansNotLakeReads() {
        val default = observed("USE dl").lakeDefault("dl") { schema, name -> schema == "main" && name == "t" }
        assertThat(TrinoReplayBindings.referencesLake("SELECT * FROM \"/tmp/x/*-delete.parquet\"", "dl", default)).isFalse()
        assertThat(tables(TrinoReplayBindings.rewrite("SELECT * FROM t, \"data.csv\"", "dl", "corpus_1", default)))
            .containsExactlyInAnyOrder(listOf("corpus_1", "main", "t"), listOf("data.csv"))
    }

    @Test
    fun useOfANonLakeDatabaseLeavesBareNamesUnbound() {
        assertThat(TrinoReplayBindings.OracleNames().lakeDefault("dl") { _, _ -> false }).isNull()
        val names = observed("USE dl", "USE memory")
        assertThat(names.catalog).isEqualTo("memory")
        assertThat(names.lakeDefault("dl") { _, _ -> true }).isNull()
        assertThat(TrinoReplayBindings.referencesLake("SELECT * FROM t", "dl", names.lakeDefault("dl") { _, _ -> true })).isFalse()
        assertThat(TrinoReplayBindings.rewrite("SELECT * FROM t", "dl", "corpus_1", null)).isEqualTo("SELECT * FROM t")
    }

    @Test
    fun aLaterUseInTheSameStatementWins() {
        assertThat(observed("USE dl; USE memory").catalog).isEqualTo("memory")
        assertThat(observed("USE memory; SELECT 1; USE dl").catalog).isEqualTo("dl")
    }

    @Test
    fun statementsWithoutNameScopeChangesAndUnparseableUsesAreHandled() {
        val names = observed("USE dl", "SELECT * FROM t", "INSERT INTO t VALUES (1)", "CREATE TABLE x(i INT)")
        assertThat(names.catalog).isEqualTo("dl")
        TrinoReplayBindings.observe("USE dl WITH (", names) { _, _ -> false }
        assertThat(names.catalog).isNull()
        assertThat(names.lakeDefault("dl") { _, _ -> true }).isNull()
    }

    private fun observed(vararg statements: String, isLakeSchema: (String, String) -> Boolean = { _, _ -> false }) =
        TrinoReplayBindings.OracleNames().also { names ->
            statements.forEach { TrinoReplayBindings.observe(it, names, isLakeSchema) }
        }

    private fun tables(sql: String): List<List<String>> =
        Dialects.TRINO.parseOne(sql).findAll(Table::class).filterIsInstance<Table>().map { it.parts.map { p -> p.name } }.toList()

    @Test
    fun cteNamesAndTableFunctionsDoNotPretendToBeLakeRelations() {
        assertThat(TrinoReplayBindings.referencesLake("WITH dl AS (SELECT 1 i) SELECT dl.i FROM dl", "dl")).isFalse()
        assertThat(TrinoReplayBindings.referencesLake("SELECT * FROM dl.snapshots()", "dl")).isFalse()
        assertThat(TrinoReplayBindings.referencesLake("""SELECT i FROM "DL"."雪.a".t""", "dl")).isTrue()
        assertThat(TrinoReplayBindings.referencesLake("SELECT i FROM İ.t", "i")).isFalse()
    }
}
