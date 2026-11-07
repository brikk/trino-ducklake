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
import io.trino.filesystem.cache.NoopSplitAffinityProvider
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilterSnapshot
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.testing.connector.TestingConnectorSession.SESSION
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Catalog-stats file pruning through real SQL. Every case asserts the row count AND the number of
 * data-file splits the scan actually ran, so a pruning path that is silently never reached cannot
 * pass: each table holds one data file per INSERT, and a selective predicate must scan fewer splits
 * than files. Boundary predicates sit exactly on a file's stored min/max, where an inexact
 * normalization in the dropping direction would lose rows. Types the connector cannot write are
 * written by DuckDB, which also covers DuckDB's own stats spellings.
 *
 * Range bounds reach the catalog as inclusive, so `c < 11` keeps a file whose min is 11.
 */
class TestDucklakeStatsFilePruning : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "stats-file-pruning"

    @Test
    fun integerTypesPrune() {
        for (type in listOf("TINYINT", "SMALLINT", "INTEGER", "BIGINT")) {
            withFiles("prune_${type.lowercase()}", type, "1, 2", "11, 12", "21, 22") { t ->
                assertScan("SELECT count(*) FROM $t WHERE c = $type '12'", rows = 1, splits = 1)
                assertScan("SELECT count(*) FROM $t WHERE c >= $type '12'", rows = 3, splits = 2)
                assertScan("SELECT count(*) FROM $t WHERE c < $type '10'", rows = 2, splits = 1)
                assertScan("SELECT count(*) FROM $t WHERE c > $type '22'", rows = 0, splits = 1)
                assertScan("SELECT count(*) FROM $t WHERE c > $type '25'", rows = 0, splits = 0)
            }
        }
    }

    @Test
    fun doublePrunes() {
        withFiles("prune_double", "DOUBLE", "0.1, 0.2", "1.1, 1.2", "2.1, 2.2") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = 1.2e0", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c >= 1.2e0", rows = 3, splits = 2)
            assertScan("SELECT count(*) FROM $t WHERE c <= 1.1e0", rows = 3, splits = 2)
            assertScan("SELECT count(*) FROM $t WHERE c BETWEEN 0.3e0 AND 1.0e0", rows = 0, splits = 0)
        }
    }

    @Test
    fun realPrunesOnItsFloatValue() {
        // 0.1f is 0.100000001490116..., so the Trino-native float bits must be rendered as the float,
        // not as the bits and not as a double that would fall outside the file's [min, max].
        withFiles("prune_real", "REAL", "REAL '0.1', REAL '0.2'", "REAL '1.1', REAL '1.2'", "REAL '2.1', REAL '2.2'") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = REAL '1.1'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c >= REAL '1.2'", rows = 3, splits = 2)
            assertScan("SELECT count(*) FROM $t WHERE c <= REAL '0.2'", rows = 2, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c > REAL '2.25'", rows = 0, splits = 0)
        }
    }

    @Test
    fun decimalPrunesOnScaledValue() {
        withFiles("prune_short_decimal", "DECIMAL(10, 2)", "1.25, 2.50", "11.25, 12.50", "21.25, 22.50") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = DECIMAL '12.50'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c >= DECIMAL '12.5'", rows = 3, splits = 2)
            assertScan("SELECT count(*) FROM $t WHERE c BETWEEN DECIMAL '2.51' AND DECIMAL '11.24'", rows = 0, splits = 0)
        }
        withFiles(
                "prune_long_decimal",
                "DECIMAL(30, 4)",
                "DECIMAL '-12345678901234567890.0001', DECIMAL '-1.0000'",
                "DECIMAL '1.0000', DECIMAL '12345678901234567890.0001'",
                "DECIMAL '12345678901234567890.0002', DECIMAL '99999999999999999999.9999'") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = DECIMAL '12345678901234567890.0001'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c >= DECIMAL '12345678901234567890.0002'", rows = 2, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c < DECIMAL '0'", rows = 2, splits = 1)
        }
    }

    @Test
    fun datePrunes() {
        withFiles(
                "prune_date",
                "DATE",
                "DATE '0100-01-01', DATE '1969-12-31'",
                "DATE '2024-01-01', DATE '2024-01-31'",
                "DATE '2024-02-01', DATE '9999-12-31'") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = DATE '2024-01-31'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c >= DATE '2024-01-31'", rows = 3, splits = 2)
            assertScan("SELECT count(*) FROM $t WHERE c < DATE '1970-01-01'", rows = 2, splits = 1)
        }
    }

    @Test
    fun timestampPrunesAtTrinoWritablePrecisions() {
        withFiles(
                "prune_ts3",
                "TIMESTAMP(3)",
                "TIMESTAMP '2024-01-01 00:00:00.000', TIMESTAMP '2024-01-01 10:00:00.122'",
                "TIMESTAMP '2024-01-01 10:00:00.123', TIMESTAMP '2024-01-01 10:00:00.124'",
                "TIMESTAMP '2024-01-01 10:00:00.125', TIMESTAMP '2024-01-02 00:00:00.000'") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = TIMESTAMP '2024-01-01 10:00:00.124'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c <= TIMESTAMP '2024-01-01 10:00:00.123'", rows = 3, splits = 2)
        }
        withFiles(
                "prune_ts6",
                "TIMESTAMP(6)",
                "TIMESTAMP '1960-06-01 12:00:00.000001', TIMESTAMP '2024-01-01 10:00:00.123455'",
                "TIMESTAMP '2024-01-01 10:00:00.123456', TIMESTAMP '2024-01-01 10:00:00.123457'",
                "TIMESTAMP '2024-01-01 10:00:00.123458', TIMESTAMP '2025-01-01 00:00:00.000000'") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = TIMESTAMP '2024-01-01 10:00:00.123457'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c >= TIMESTAMP '2024-01-01 10:00:00.123457'", rows = 3, splits = 2)
            assertScan("SELECT count(*) FROM $t WHERE c < TIMESTAMP '1970-01-01 00:00:00'", rows = 1, splits = 1)
        }
        withFiles(
                "prune_ts9",
                "TIMESTAMP(9)",
                "TIMESTAMP '2024-01-01 00:00:00.000000000', TIMESTAMP '2024-01-01 10:00:00.123456788'",
                "TIMESTAMP '2024-01-01 10:00:00.123456789', TIMESTAMP '2024-01-01 10:00:00.123456790'",
                "TIMESTAMP '2024-01-01 10:00:00.123456791', TIMESTAMP '2024-01-02 00:00:00.000000000'") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = TIMESTAMP '2024-01-01 10:00:00.123456790'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c <= TIMESTAMP '2024-01-01 10:00:00.123456789'", rows = 3, splits = 2)
        }
    }

    @Test
    fun varcharPrunesInCodePointOrder() {
        // U+FB00 sorts after the surrogate pair of U+1F600 in UTF-16 but before it by code point,
        // which is the order of both Trino and the catalog.
        withFiles("prune_varchar", "VARCHAR", "'apple', 'apricot'", "'banana', 'ﬀ'", "'😀', '😃'") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = 'banana'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c = 'ﬀ'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c >= '😀'", rows = 2, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE c < 'b'", rows = 2, splits = 1)
        }
    }

    @Test
    fun booleanPrunes() {
        withFiles("prune_boolean", "BOOLEAN", "false, false", "true, true", "false, true") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c", rows = 3, splits = 2)
            assertScan("SELECT count(*) FROM $t WHERE c = false", rows = 3, splits = 2)
            assertScan("SELECT count(*) FROM $t WHERE NOT c", rows = 3, splits = AtLeast(2))
        }
    }

    @Test
    fun predicateAdmittingNullKeepsFilesWithNulls() {
        withFiles("prune_nulls", "BIGINT", "1, 2", "11, 12", "NULL, 30") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c IS NULL OR c = 12", rows = 2, splits = 3)
            assertScan("SELECT count(*) FROM $t WHERE c = 30", rows = 1, splits = 1)
        }
    }

    @Test
    fun floatPredicateThatAdmitsNaNKeepsTheNaNFile() {
        withFiles("prune_nan", "DOUBLE", "1.0e0, 2.0e0", "10.0e0, nan()", "20.0e0, 30.0e0") { t ->
            assertScan("SELECT count(*) FROM $t WHERE NOT (c >= 5)", rows = 3, splits = AtLeast(2))
            assertScan("SELECT count(*) FROM $t WHERE NOT (c <= 15)", rows = 3, splits = AtLeast(2))
            // contains_nan invalidates the [10, 10] file's max, so it is kept for a lower bound.
            assertScan("SELECT count(*) FROM $t WHERE c > 15", rows = 2, splits = 2)
        }
    }

    @Test
    fun duckdbWrittenStatsPruneForEverySupportedType() {
        val columns = mapOf(
                "i" to "BIGINT",
                "f" to "FLOAT",
                "d" to "DOUBLE",
                "dec" to "DECIMAL(18, 3)",
                "dt" to "DATE",
                "ts" to "TIMESTAMP",
                "ts_s" to "TIMESTAMP_S",
                "ts_ms" to "TIMESTAMP_MS",
                "ts_ns" to "TIMESTAMP_NS",
                "tz" to "TIMESTAMPTZ",
                "s" to "VARCHAR",
                "b" to "BOOLEAN")
        val files = listOf(
                "(1, 0.1, 0.1, 1.001, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00.000001', TIMESTAMP_S '2024-01-01 00:00:00', " +
                        "TIMESTAMP_MS '2024-01-01 00:00:00.001', '2024-01-01 00:00:00.000000001'::TIMESTAMP_NS, " +
                        "TIMESTAMPTZ '2024-01-01 02:00:00.000001+02', 'apple', false)",
                "(2, 1.1, 1.1, 2.002, DATE '2024-01-02', TIMESTAMP '2024-01-02 00:00:00.000002', TIMESTAMP_S '2024-01-02 00:00:01', " +
                        "TIMESTAMP_MS '2024-01-02 00:00:00.002', '2024-01-02 00:00:00.000000002'::TIMESTAMP_NS, " +
                        "TIMESTAMPTZ '2024-01-02 03:00:00.000002+03', 'banana', true)",
                "(3, 2.1, 2.1, 3.003, DATE '2024-01-03', TIMESTAMP '2024-01-03 00:00:00.000003', TIMESTAMP_S '2024-01-03 00:00:02', " +
                        "TIMESTAMP_MS '2024-01-03 00:00:00.003', '2024-01-03 00:00:00.000000003'::TIMESTAMP_NS, " +
                        "TIMESTAMPTZ '2024-01-03 00:00:00.000003+00', 'cherry', true)")
        withDuckdbFiles("duck_types", columns.entries.joinToString(", ") { "${it.key} ${it.value}" }, files) { t ->
            val secondFile = mapOf(
                    "i" to "BIGINT '2'",
                    "f" to "REAL '1.1'",
                    "d" to "1.1e0",
                    "dec" to "DECIMAL '2.002'",
                    "dt" to "DATE '2024-01-02'",
                    "ts" to "TIMESTAMP '2024-01-02 00:00:00.000002'",
                    "ts_s" to "TIMESTAMP '2024-01-02 00:00:01'",
                    "ts_ms" to "TIMESTAMP '2024-01-02 00:00:00.002'",
                    "ts_ns" to "TIMESTAMP '2024-01-02 00:00:00.000000002'",
                    "tz" to "TIMESTAMP '2024-01-02 09:00:00.000002 Asia/Tokyo'",
                    "s" to "'banana'")
            for ((column, literal) in secondFile) {
                assertScan("SELECT count(*) FROM $t WHERE $column = $literal", rows = 1, splits = 1)
            }
            assertScan("SELECT count(*) FROM $t WHERE b = false", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE tz > TIMESTAMP '2024-01-02 00:00:00.000002 UTC'", rows = 1, splits = 2)
            assertScan("SELECT count(*) FROM $t WHERE tz >= TIMESTAMP '2024-01-02 00:00:00.000003 UTC'", rows = 1, splits = 1)
            assertScan("SELECT count(*) FROM $t WHERE ts_ns <= TIMESTAMP '2024-01-02 00:00:00.000000001'", rows = 1, splits = 1)
        }
    }

    @Test
    fun unsupportedTypesRetainAllFiles() {
        withDuckdbFiles(
                "duck_uuid",
                "u UUID",
                listOf(
                        "(UUID '00000000-0000-0000-0000-000000000001')",
                        "(UUID '00000000-0000-0000-0000-000000000002')",
                        "(UUID '00000000-0000-0000-0000-000000000003')")) { t ->
            assertScan("SELECT count(*) FROM $t WHERE u = UUID '00000000-0000-0000-0000-000000000002'", rows = 1, splits = 3)
        }
        withDuckdbFiles("duck_uint", "u UTINYINT", listOf("(1)", "(100)", "(200)")) { t ->
            assertScan("SELECT count(*) FROM $t WHERE u = SMALLINT '100'", rows = 1, splits = 3)
        }
        withFiles("prune_varbinary", "VARBINARY", "X'01'", "X'02'", "X'03'") { t ->
            assertScan("SELECT count(*) FROM $t WHERE c = X'02'", rows = 1, splits = 3)
        }
    }

    @Test
    fun filesFromBeforeATypePromotionAreKept() {
        // The catalog keeps the REAL-era stats text ("0.1") across REAL -> DOUBLE; read as a double
        // it is below the widened row value 0.10000000149011612, so pruning on it would drop the row.
        // Driven through the split manager because the row-group predicate on the promoted REAL
        // file fails the scan itself (see dev-docs/TODO.md).
        val table = "test_schema.prune_promoted"
        try {
            computeActual("CREATE TABLE $table (c REAL)")
            computeActual("INSERT INTO $table VALUES (REAL '0.1')")
            computeActual("INSERT INTO $table VALUES (REAL '5.5')")
            computeActual("ALTER TABLE $table ALTER COLUMN c SET DATA TYPE DOUBLE")
            computeActual("INSERT INTO $table VALUES (10.5e0)")
            computeActual("INSERT INTO $table VALUES (20.5e0)")
            assertFileCount(table, 4)

            assertThat(splitPaths("prune_promoted", Domain.singleValue(DOUBLE, 0.1f.toDouble())))
                    .`as`("both REAL-era files kept, the DOUBLE-era files pruned")
                    .hasSize(2)
                    .allSatisfy { assertThat(it).isIn(splitPaths("prune_promoted", Domain.singleValue(DOUBLE, 5.5))) }
            assertThat(splitPaths("prune_promoted", Domain.singleValue(DOUBLE, 20.5))).hasSize(3)
        }
        finally {
            tryDropTable(table)
        }
    }

    /** Data-file paths the split manager emits for `c` constrained to [domain], as Trino would request them. */
    private fun splitPaths(tableName: String, domain: Domain): List<String> {
        val isolated = getIsolatedCatalog()
        val config = DucklakeConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl)
                .setCatalogDatabaseUser(isolated.user)
                .setCatalogDatabasePassword(isolated.password)
                .setDataPath(isolated.dataDir.toAbsolutePath().toString())
        JdbcDucklakeCatalog(config.toCatalogConfig()).use { catalog: DucklakeCatalog ->
            val snapshotId = catalog.currentSnapshotId
            val table = catalog.getTable("test_schema", tableName, snapshotId)!!
            val column = catalog.getTableColumns(table.tableId, snapshotId).single { it.columnName == "c" }
            val handle = DucklakeColumnHandle(column.columnId, "c", domain.type, true)
            val tableHandle = DucklakeTableHandle(
                    "test_schema", tableName, table.tableId, snapshotId,
                    TupleDomain.withColumnDomains(mapOf(handle to domain)), TupleDomain.all())
            val splitManager = DucklakeSplitManager(catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider())
            splitManager.getSplits(null, SESSION, tableHandle, mutableSetOf(), Constraint.alwaysTrue()).use { source ->
                val paths = mutableListOf<String>()
                while (!source.isFinished) {
                    source.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).get().forEach { paths += (it as DucklakeSplit).dataFilePath }
                }
                return paths
            }
        }
    }

    private fun withDuckdbFiles(name: String, columns: String, fileRows: List<String>, body: (String) -> Unit) {
        val table = "test_schema.$name"
        try {
            createDuckdbConnection().use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE ducklake_db.$table ($columns)")
                    statement.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 0, " +
                            "schema => 'test_schema', table_name => '$name')")
                    for (row in fileRows) {
                        statement.execute("INSERT INTO ducklake_db.$table VALUES $row")
                    }
                }
            }
            assertFileCount(table, fileRows.size.toLong())
            assertScan("SELECT count(*) FROM $table", rows = fileRows.size.toLong(), splits = fileRows.size.toLong())
            body(table)
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun withFiles(name: String, type: String, vararg fileRows: String, body: (String) -> Unit) {
        val table = "test_schema.$name"
        try {
            computeActual("CREATE TABLE $table (c $type)")
            for (rows in fileRows) {
                computeActual("INSERT INTO $table VALUES " + rows.splitTopLevel().joinToString(", ") { "($it)" })
            }
            assertFileCount(table, fileRows.size.toLong())
            assertScan("SELECT count(*) FROM $table", rows = fileRows.sumOf { it.splitTopLevel().size }.toLong(), splits = fileRows.size.toLong())
            body(table)
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun assertFileCount(table: String, expected: Long) {
        val (schema, name) = table.split('.')
        assertThat(computeScalar("SELECT count(*) FROM $schema.\"$name\$files\""))
                .`as`("data files of %s", table)
                .isEqualTo(expected)
    }

    private sealed interface Splits
    private data class Exactly(val count: Long) : Splits
    private data class AtLeast(val count: Long) : Splits

    private fun assertScan(sql: String, rows: Long, splits: Long) = assertScan(sql, rows, Exactly(splits))

    private fun assertScan(sql: String, rows: Long, splits: Splits) {
        val runner = distributedQueryRunner
        val result = runner.executeWithPlan(session, sql)
        assertThat(result.result().onlyColumnAsSet).`as`("rows of %s", sql).containsExactly(rows)
        val scans = runner.coordinator.queryManager.getFullQueryInfo(result.queryId())
                .queryStats
                .operatorSummaries
                .filter { it.operatorType == "ScanFilterAndProjectOperator" || it.operatorType == "TableScanOperator" }
        val drivers = scans.sumOf { it.totalDrivers.toLong() }
        // With no data-file splits Trino still schedules one empty split, which reads no bytes (a
        // data-file split always reads at least the Parquet footer, even when no row group survives).
        val bytesRead = scans.sumOf { it.physicalInputDataSize.toBytes() }
        val scanned = if (drivers == 1L && bytesRead == 0L) 0L else drivers
        when (splits) {
            is Exactly -> assertThat(scanned).`as`("splits scanned by %s", sql).isEqualTo(splits.count)
            is AtLeast -> assertThat(scanned).`as`("splits scanned by %s", sql).isGreaterThanOrEqualTo(splits.count)
        }
    }

    /** Splits a VALUES row list on commas outside quotes and parentheses. */
    private fun String.splitTopLevel(): List<String> {
        val parts = mutableListOf<String>()
        var depth = 0
        var quoted = false
        var start = 0
        forEachIndexed { i, ch ->
            when {
                ch == '\'' -> quoted = !quoted
                quoted -> Unit
                ch == '(' -> depth++
                ch == ')' -> depth--
                ch == ',' && depth == 0 -> {
                    parts += substring(start, i).trim()
                    start = i + 1
                }
            }
        }
        parts += substring(start).trim()
        return parts
    }
}
