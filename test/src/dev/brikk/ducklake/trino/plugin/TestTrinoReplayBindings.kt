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
    fun cteNamesAndTableFunctionsDoNotPretendToBeLakeRelations() {
        assertThat(TrinoReplayBindings.referencesLake("WITH dl AS (SELECT 1 i) SELECT dl.i FROM dl", "dl")).isFalse()
        assertThat(TrinoReplayBindings.referencesLake("SELECT * FROM dl.snapshots()", "dl")).isFalse()
        assertThat(TrinoReplayBindings.referencesLake("""SELECT i FROM "DL"."雪.a".t""", "dl")).isTrue()
        assertThat(TrinoReplayBindings.referencesLake("SELECT i FROM İ.t", "i")).isFalse()
    }
}
