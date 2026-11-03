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
import dev.brikk.house.sql.ast.Table
import dev.brikk.house.sql.dialects.Dialects
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TestTrinoReplayMetadataBindings {
    private val mapping = mapOf("__ducklake_metadata_dl" to "public")

    @Test
    fun nativeIdentifierCaseDoesNotHideMetadataBindings() {
        val bound = TrinoReplayMetadataBindings.rewrite("SELECT * FROM __DUCKLAKE_METADATA_DL.MAIN.t", mapping)
        val table = Dialects.DUCKDB.parseOne(bound).findAll(Table::class).filterIsInstance<Table>().single()
        assertThat(table.db).isEqualTo("public")
    }

    @Test
    fun updateAndSelectBindOnlyTheActualMetadataSchema() {
        for (sql in listOf(
            "UPDATE __ducklake_metadata_dl.main.ducklake_view SET column_aliases=NULL WHERE view_name='v'",
            "SELECT view_name FROM __ducklake_metadata_dl.main.ducklake_view")) {
            val bound = TrinoReplayMetadataBindings.rewrite(sql, mapping)
            val table = Dialects.DUCKDB.parseOne(bound).findAll(Table::class).filterIsInstance<Table>().single()
            assertThat(table.parts.map { it.name }).containsExactly("__ducklake_metadata_dl", "public", "ducklake_view")
        }
    }

    @Test
    fun quotedIdentifiersAndQualifiedColumnsKeepTheirIdentity() {
        val sql = """SELECT "meta.name".main.t.i FROM "meta.name".main.t"""
        val bound = TrinoReplayMetadataBindings.rewrite(sql, mapOf("meta.name" to "schema.with.dot"))
        val tree = Dialects.DUCKDB.parseOne(bound)
        assertThat(tree.findAll(Table::class).filterIsInstance<Table>().single().parts.map { it.name })
            .containsExactly("meta.name", "schema.with.dot", "t")
        assertThat(tree.findAll(Column::class).filterIsInstance<Column>().single().parts.map { it.name })
            .containsExactly("meta.name", "schema.with.dot", "t", "i")
    }

    @Test
    fun logicalSchemasExplicitOtherSchemasAndLiteralsAreNotChanged() {
        for (sql in listOf(
            "SELECT '__ducklake_metadata_dl.main.ducklake_view' FROM dl.main.t",
            "SELECT * FROM __ducklake_metadata_dl.custom.ducklake_view",
            "SELECT * FROM unrelated.main.ducklake_view",
            "CALL dl.set_option('x','main')")) {
            assertThat(TrinoReplayMetadataBindings.rewrite(sql, mapping)).isEqualTo(sql)
        }
        assertThat(TrinoReplayMetadataBindings.rewrite("SELECT * FROM __ducklake_metadata_dl.main.t", emptyMap()))
            .isEqualTo("SELECT * FROM __ducklake_metadata_dl.main.t")
    }
}
