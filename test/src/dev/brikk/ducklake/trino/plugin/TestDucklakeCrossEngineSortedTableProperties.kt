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
import io.trino.spi.connector.ConnectorTableProperties
import io.trino.testing.connector.TestingConnectorSession.SESSION
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.util.UUID

/**
 * End-to-end check that DuckDB-written per-file sort metadata is NOT advertised to Trino's
 * planner as a globally sorted scan stream. DuckDB does the writing
 * (Trino-side `ALTER TABLE ... SET SORTED BY` isn't implemented and
 * doesn't need to be for the read-awareness story to work); Trino sees the
 * same catalog through [DucklakeMetadata.getTableProperties]. The connector still reads the sort
 * spec for physical write ordering; it simply cannot claim multiple scan splits are one ordered
 * stream.
 *
 *
 * We assert the SPI shape directly rather than relying on planner output formatting.
 *
 * SAME_THREAD (like every sibling cross-engine test): methods write to ONE shared per-class
 * catalog via cross-engine DuckDB CREATE/ALTER; serialize so concurrent commits can't race the
 * snapshot lineage under the suite's default concurrent execution.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCrossEngineSortedTableProperties
        : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String {
        return "cross-engine-sorted-table-properties"
    }

    @Test
    fun testDuckdbSortedTableDoesNotClaimGloballySortedScan() {
        val tableName = "xengine_sorted_" + UUID.randomUUID().toString().substring(0, 8)
        val fullDuckdb = "ducklake_db.test_schema.$tableName"

        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, name VARCHAR, price DOUBLE)")
                    // Two-key sort with mixed direction + explicit null ordering — exercises
                    // both branches of the SortOrder mapping plus multi-key ordering.
                    stmt.execute("ALTER TABLE $fullDuckdb"
                            + " SET SORTED BY (name ASC NULLS LAST, price DESC NULLS FIRST)")
                }
            }

            val properties = readTableProperties("test_schema", tableName)

            assertThat(properties.localProperties)
                    .`as`("DuckLake ordering is per-file, not a scan-stream guarantee")
                    .isEmpty()
        }
        finally {
            tryDropDuckdbTable(fullDuckdb)
        }
    }

    @Test
    fun testUnsortedTableYieldsEmptyLocalProperties() {
        val tableName = "xengine_unsorted_" + UUID.randomUUID().toString().substring(0, 8)
        val fullDuckdb = "ducklake_db.test_schema.$tableName"

        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, name VARCHAR)")
                }
            }

            val properties = readTableProperties("test_schema", tableName)

            assertThat(properties.localProperties).isEmpty()
        }
        finally {
            tryDropDuckdbTable(fullDuckdb)
        }
    }

    @Test
    fun testSortResetClearsLocalProperties() {
        // RESET SORTED BY end-snapshots the sort_info row. The next snapshot read should
        // see no sort spec — confirming our active-snapshot filter actually filters.
        val tableName = "xengine_reset_sort_" + UUID.randomUUID().toString().substring(0, 8)
        val fullDuckdb = "ducklake_db.test_schema.$tableName"

        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, name VARCHAR)")
                    stmt.execute("ALTER TABLE $fullDuckdb SET SORTED BY (name ASC NULLS LAST)")
                    stmt.execute("ALTER TABLE $fullDuckdb RESET SORTED BY")
                }
            }

            val properties = readTableProperties("test_schema", tableName)

            assertThat(properties.localProperties).isEmpty()
        }
        finally {
            tryDropDuckdbTable(fullDuckdb)
        }
    }

    private fun readTableProperties(schemaName: String, tableName: String): ConnectorTableProperties {
        val isolated = getIsolatedCatalog()
        val config = DucklakeConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl)
                .setCatalogDatabaseUser(isolated.user)
                .setCatalogDatabasePassword(isolated.password)
                .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                .setMaxCatalogConnections(5)
        val catalog: DucklakeCatalog = JdbcDucklakeCatalog(config.toCatalogConfig())
        try {
            val snapshotId = catalog.currentSnapshotId
            val schema = catalog.listSchemas(snapshotId).stream()
                    .filter { s -> s.schemaName == schemaName }
                    .findFirst()
                    .orElseThrow { AssertionError("Missing schema: $schemaName") }
            val table = catalog.listTables(schema.schemaId, snapshotId).stream()
                    .filter { t -> t.tableName == tableName }
                    .findFirst()
                    .orElseThrow { AssertionError("Missing table: $schemaName.$tableName") }

            val typeConverter = DucklakeTypeConverter(
                    io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
            val metadata = DucklakeMetadata(catalog, typeConverter)
            return metadata.getTableProperties(
                    SESSION,
                    DucklakeTableHandle(schemaName, tableName, table.tableId, snapshotId))
        }
        finally {
            catalog.close()
        }
    }

    private fun tryDropDuckdbTable(fullName: String) {
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS $fullName")
                }
            }
        }
        catch (ignored: Exception) {
        }
    }

}
