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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/** Cross-engine identity resolution: Parquet field_id wins over every name coincidence. */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeFieldIdResolution : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "field-id-resolution"

    @Test
    fun topLevelRenameSwapReadsColumnsByFieldId() {
        val table = "test_schema.field_swap"
        try {
            computeActual("CREATE TABLE $table (a INTEGER, b INTEGER)")
            computeActual("INSERT INTO $table VALUES (10, 20)")
            computeActual("ALTER TABLE $table RENAME COLUMN a TO tmp")
            computeActual("ALTER TABLE $table RENAME COLUMN b TO a")
            computeActual("ALTER TABLE $table RENAME COLUMN tmp TO b")

            assertThat(computeActual("SELECT a, b FROM $table").materializedRows.single().fields)
                    .containsExactly(20, 10)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun predicateOnReaddedNameDoesNotPruneItsNullOldFileValues() {
        val table = "test_schema.field_readd_predicate"
        try {
            computeActual("CREATE TABLE $table (a INTEGER)")
            computeActual("INSERT INTO $table VALUES (7)")
            computeActual("ALTER TABLE $table RENAME COLUMN a TO old_a")
            computeActual("ALTER TABLE $table ADD COLUMN a INTEGER")

            assertThat(computeActual("SELECT old_a, a FROM $table WHERE a IS NULL").materializedRows.single().fields)
                    .containsExactly(7, null)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun nestedRenameReadsOldFileChildByFieldId() {
        val table = "test_schema.nested_rename_id"
        try {
            createDuckdbFileTable(
                    "nested_rename_id",
                    "id INTEGER, s STRUCT(old_name VARCHAR)",
                    "(1, {'old_name': 'value'})")
            withCatalog { catalog ->
                val snapshot = catalog.currentSnapshotId
                val stored = catalog.getTable("test_schema", "nested_rename_id", snapshot)!!
                val child = catalog.getAllColumnsWithParentage(stored.tableId, snapshot)
                        .single { it.columnName == "old_name" }
                catalog.renameColumn(stored.tableId, child.columnId, "new_name")
            }

            assertThat(computeActual("SELECT s.new_name FROM $table").onlyValue).isEqualTo("value")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun nestedDropAndReaddDoesNotResurrectOldChildBytes() {
        val table = "test_schema.nested_readd_id"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(x VARCHAR, y INTEGER))")
            computeActual("INSERT INTO $table VALUES (1, ROW('old', 9))")
            computeActual("ALTER TABLE $table DROP COLUMN s.x")
            computeActual("ALTER TABLE $table ADD COLUMN s.x VARCHAR")

            assertThat(computeActual("SELECT s.x, s.y FROM $table").materializedRows.single().fields)
                    .containsExactly(null, 9)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun mixedCaseStructFieldsPreserveCatalogNames() {
        val table = "test_schema.mixed_case_struct"
        try {
            createDuckdbFileTable(
                    "mixed_case_struct",
                    "id INTEGER, s STRUCT(\"DisplayName\" VARCHAR, \"Value\" INTEGER, \"a:b,c\" VARCHAR)",
                    "(1, {'DisplayName': 'Alice', 'Value': 9, 'a:b,c': 'punctuation'})")

            assertThat(computeActual(
                    "SELECT s.\"DisplayName\", s.\"Value\", s.\"a:b,c\" FROM $table").materializedRows.single().fields)
                    .containsExactly("Alice", 9, "punctuation")
            assertThat(computeActual("DESCRIBE $table").materializedRows[1].getField(1).toString())
                    .contains("DisplayName").contains("Value").contains("a:b,c")
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun createDuckdbFileTable(bare: String, columns: String, row: String) {
        createDuckdbConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE ducklake_db.test_schema.$bare ($columns)")
                statement.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 0, " +
                        "schema => 'test_schema', table_name => '$bare')")
                statement.execute("INSERT INTO ducklake_db.test_schema.$bare VALUES $row")
            }
        }
    }

    private fun withCatalog(action: (DucklakeCatalog) -> Unit) {
        val isolated = getIsolatedCatalog()
        JdbcDucklakeCatalog(DucklakeConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl)
                .setCatalogDatabaseUser(isolated.user)
                .setCatalogDatabasePassword(isolated.password)
                .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                .toCatalogConfig()).use(action)
    }
}
