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
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Cross-engine validation of the `add_files` surface: DuckDB writes a raw
 * parquet file outside the table's data directory, Trino registers it with
 * `CALL ducklake.system.add_files(...)`, and DuckDB reads the resulting
 * DuckLake table back. Closes the loop on the `ducklake_name_mapping` +
 * `ducklake_column_mapping` rows our catalog writer produces — DuckDB's
 * reader consults those tables, so an end-to-end DuckDB read proves they're
 * well-formed (cross-engine compatibility, not a Trino-only happy path).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeAddFilesCrossEngine : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String {
        return "cross-engine-add-files"
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbWritesParquetTrinoAddFilesDuckdbReads() {
        // 1. DuckDB writes a parquet file outside any DuckLake-managed directory. The
        // file's column order intentionally differs from the table's column order
        // (parquet has name first, then id) to prove the name_map round-trip.
        val duckdbOutputDir = getIsolatedCatalog().dataDir.parent.resolve("add_files_xengine")
        java.nio.file.Files.createDirectories(duckdbOutputDir)
        val parquetPath = duckdbOutputDir.resolve("rows.parquet").toAbsolutePath()

        createDuckdbConnection().use { duckdb ->
            duckdb.createStatement().use { stmt ->
                stmt.execute(String.format(
                        "COPY (SELECT 'alice' AS name, 1 AS id UNION ALL SELECT 'bob' AS name, 2 AS id) "
                                + "TO '%s' (FORMAT PARQUET)",
                        parquetPath))
            }
        }

        // 2. Trino creates the destination DuckLake table and registers the file.
        computeActual("CREATE TABLE test_schema.xengine_add_files_dst (id INTEGER, name VARCHAR)")
        try {
            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'xengine_add_files_dst', "
                            + "files => ARRAY['%s'])",
                    parquetPath))

            // 3. Trino reads — sanity check the connector's own side
            val trinoResult = computeActual(
                    "SELECT id, name FROM test_schema.xengine_add_files_dst ORDER BY id")
            assertThat(trinoResult.rowCount).isEqualTo(2)
            assertThat(trinoResult.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(trinoResult.materializedRows[0].getField(1)).isEqualTo("alice")
            assertThat(trinoResult.materializedRows[1].getField(0)).isEqualTo(2)
            assertThat(trinoResult.materializedRows[1].getField(1)).isEqualTo("bob")

            // 4. DuckDB reads the catalog-registered table — proves name_map is consumed
            // by upstream DuckLake.
            createDuckdbConnection().use { duckdb ->
                duckdb.createStatement().use { stmt ->
                    stmt.executeQuery(
                            "SELECT id, name FROM ducklake_db.test_schema.xengine_add_files_dst ORDER BY id").use { rs ->
                        assertThat(rs.next()).`as`("DuckDB should find row 1").isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(1)
                        assertThat(rs.getString("name")).isEqualTo("alice")
                        assertThat(rs.next()).`as`("DuckDB should find row 2").isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(2)
                        assertThat(rs.getString("name")).isEqualTo("bob")
                        assertThat(rs.next()).`as`("DuckDB should not find more rows").isFalse()
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_add_files_dst")
        }
    }

    @Test
    fun nestedNameMapSurvivesTargetFieldRenames() {
        val outputDir = getIsolatedCatalog().dataDir.parent.resolve("add_files_nested_xengine")
        java.nio.file.Files.createDirectories(outputDir)
        val parquetPath = outputDir.resolve("nested.parquet").toAbsolutePath()
        createDuckdbConnection().use { duckdb ->
            duckdb.createStatement().use { statement ->
                statement.execute(
                        "COPY (SELECT {'DisplayName': 'Alice', 'Details': {'ScoreValue': 42::INTEGER}} " +
                                "AS \"Payload\") TO '$parquetPath' (FORMAT PARQUET)")
            }
        }

        val table = "test_schema.xengine_nested_add_files"
        computeActual("CREATE TABLE $table (payload ROW(displayname VARCHAR, details ROW(scorevalue INTEGER)))")
        try {
            computeActual(
                    "CALL ducklake.system.add_files(schema_name => 'test_schema', " +
                            "table_name => 'xengine_nested_add_files', files => ARRAY['$parquetPath'])")

            withCatalog { catalog ->
                val snapshot = catalog.currentSnapshotId
                val stored = catalog.getTable("test_schema", "xengine_nested_add_files", snapshot)!!
                val columns = catalog.getAllColumnsWithParentage(stored.tableId, snapshot)
                val displayName = columns.single { it.columnName == "displayname" }
                val scoreValue = columns.single { it.columnName == "scorevalue" }
                val mappingId = catalog.getDataFiles(stored.tableId, snapshot).single().mappingId!!
                assertThat(catalog.getNameMaps(setOf(mappingId)).getValue(mappingId))
                        .`as`("0.8.0 exposes top-level and nested target ids")
                        .containsEntry(displayName.columnId, "displayname")
                        .containsEntry(scoreValue.columnId, "scorevalue")
                catalog.renameColumn(stored.tableId, displayName.columnId, "label")
                catalog.renameColumn(stored.tableId, scoreValue.columnId, "score")
            }

            assertThat(computeActual("SELECT payload.label, payload.details.score FROM $table")
                    .materializedRows.single().fields)
                    .containsExactly("Alice", 42)
            createDuckdbConnection().use { duckdb ->
                duckdb.createStatement().use { statement ->
                    statement.executeQuery(
                            "SELECT payload.label, payload.details.score FROM ducklake_db.$table").use { rows ->
                        assertThat(rows.next()).isTrue()
                        assertThat(rows.getString(1)).isEqualTo("Alice")
                        assertThat(rows.getInt(2)).isEqualTo(42)
                        assertThat(rows.next()).isFalse()
                    }
                }
            }
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun addFilesPreservesDecimalPhysicalEncodingAndTimestampUnits() {
        val outputDir = getIsolatedCatalog().dataDir.parent.resolve("add_files_stats_xengine")
        java.nio.file.Files.createDirectories(outputDir)
        val parquetPath = outputDir.resolve("stats.parquet").toAbsolutePath()
        createDuckdbConnection().use { duckdb ->
            duckdb.createStatement().use { statement ->
                statement.execute(
                        "COPY (SELECT 1 AS id, 123.45::DECIMAL(10,2) AS amount, " +
                                "CAST('2024-01-15 10:30:00.123' AS TIMESTAMP_MS) AS ts_ms, " +
                                "CAST('2024-01-15 10:30:00.123456789' AS TIMESTAMP_NS) AS ts_ns " +
                                "UNION ALL SELECT 2, 987.65::DECIMAL(10,2), " +
                                "CAST('2024-06-30 23:59:59.987' AS TIMESTAMP_MS), " +
                                "CAST('2024-06-30 23:59:59.987654321' AS TIMESTAMP_NS)) " +
                                "TO '$parquetPath' (FORMAT PARQUET)")
            }
        }

        val table = "test_schema.xengine_add_files_stats"
        computeActual("CREATE TABLE $table (id INTEGER, amount DECIMAL(10,2), " +
                "ts_ms TIMESTAMP(3), ts_ns TIMESTAMP(9))")
        try {
            computeActual(
                    "CALL ducklake.system.add_files(schema_name => 'test_schema', " +
                            "table_name => 'xengine_add_files_stats', files => ARRAY['$parquetPath'])")

            assertThat(fileColumnBounds("xengine_add_files_stats", "amount"))
                    .containsExactly("123.45", "987.65")
            assertThat(fileColumnBounds("xengine_add_files_stats", "ts_ms"))
                    .containsExactly("2024-01-15 10:30:00.123", "2024-06-30 23:59:59.987")
            assertThat(fileColumnBounds("xengine_add_files_stats", "ts_ns"))
                    .containsExactly(
                            "2024-01-15 10:30:00.123456789",
                            "2024-06-30 23:59:59.987654321")

            createDuckdbConnection().use { duckdb ->
                duckdb.createStatement().use { statement ->
                    statement.executeQuery(
                            "SELECT id FROM ducklake_db.$table WHERE amount=123.45::DECIMAL(10,2) " +
                                    "AND ts_ms=TIMESTAMP '2024-01-15 10:30:00.123' " +
                                    "AND ts_ns=CAST('2024-01-15 10:30:00.123456789' AS TIMESTAMP_NS)").use { rows ->
                        assertThat(rows.next()).`as`("DuckDB did not mis-prune imported file").isTrue()
                        assertThat(rows.getInt(1)).isEqualTo(1)
                        assertThat(rows.next()).isFalse()
                    }
                }
            }
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun fileColumnBounds(tableName: String, columnName: String): List<String> {
        val isolated = getIsolatedCatalog()
        java.sql.DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { connection ->
            connection.prepareStatement(
                    "SELECT s.min_value, s.max_value FROM ducklake_file_column_stats s " +
                            "JOIN ducklake_column c ON c.table_id=s.table_id AND c.column_id=s.column_id " +
                            "JOIN ducklake_table t ON t.table_id=s.table_id " +
                            "WHERE t.table_name=? AND c.column_name=? AND t.end_snapshot IS NULL " +
                            "AND c.end_snapshot IS NULL").use { statement ->
                statement.setString(1, tableName)
                statement.setString(2, columnName)
                statement.executeQuery().use { rows ->
                    check(rows.next()) { "no stats for $tableName.$columnName" }
                    return listOf(rows.getString(1), rows.getString(2))
                }
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
