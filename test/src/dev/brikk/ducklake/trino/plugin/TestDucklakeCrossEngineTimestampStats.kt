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
import java.sql.DriverManager

/** DuckDB must be able to trust catalog timestamp bounds written by Trino. */
class TestDucklakeCrossEngineTimestampStats : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "timestamp-stats"

    @Test
    fun trinoWrittenTimestampUnitsAndBoundsAreDuckdbCompatible()
    {
        val table = "test_schema.timestamp_stat_units"
        try {
            computeActual(
                    "CREATE TABLE $table (id INTEGER, ts_ms TIMESTAMP(3), " +
                            "ts_us TIMESTAMP(6), ts_ns TIMESTAMP(9))")
            computeActual(
                    "INSERT INTO $table VALUES " +
                            "(1, TIMESTAMP '2024-01-15 10:30:00.123', " +
                            "TIMESTAMP '2024-01-15 10:30:00.123456', TIMESTAMP '2024-01-15 10:30:00.123456789'), " +
                            "(2, TIMESTAMP '2024-06-30 23:59:59.987', " +
                            "TIMESTAMP '2024-06-30 23:59:59.987654', TIMESTAMP '2024-06-30 23:59:59.987654321')")

            val stats = catalogBounds("timestamp_stat_units")
            assertThat(stats.getValue("ts_ms")).containsExactly(
                    "2024-01-15 10:30:00.123", "2024-06-30 23:59:59.987")
            assertThat(stats.getValue("ts_us")).containsExactly(
                    "2024-01-15 10:30:00.123456", "2024-06-30 23:59:59.987654")
            assertThat(stats.getValue("ts_ns")).containsExactly(
                    "2024-01-15 10:30:00.123456789", "2024-06-30 23:59:59.987654321")

            assertDuckdbPredicatesRetainFile(table)
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun assertDuckdbPredicatesRetainFile(table: String) {
        createDuckdbConnection().use { duck ->
            duck.createStatement().use { statement ->
                for ((column, predicateValue) in mapOf(
                        "ts_ms" to "TIMESTAMP '2024-01-15 10:30:00.123'",
                        "ts_us" to "TIMESTAMP '2024-01-15 10:30:00.123456'",
                        "ts_ns" to "CAST('2024-01-15 10:30:00.123456789' AS TIMESTAMP_NS)")) {
                    statement.executeQuery(
                            "SELECT id FROM ducklake_db.$table WHERE $column = $predicateValue").use { rows ->
                        assertThat(rows.next()).`as`("DuckDB retained file for $column predicate").isTrue()
                        assertThat(rows.getInt(1)).isEqualTo(1)
                        assertThat(rows.next()).isFalse()
                    }
                }
            }
        }
    }

    private fun catalogBounds(tableName: String): Map<String, List<String>> {
        val isolated = getIsolatedCatalog()
        DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { connection ->
            connection.prepareStatement(
                    "SELECT c.column_name, s.min_value, s.max_value FROM ducklake_file_column_stats s " +
                            "JOIN ducklake_column c ON c.table_id=s.table_id AND c.column_id=s.column_id " +
                            "JOIN ducklake_table t ON t.table_id=s.table_id " +
                            "WHERE t.table_name=? AND t.end_snapshot IS NULL AND c.end_snapshot IS NULL").use { statement ->
                statement.setString(1, tableName)
                statement.executeQuery().use { rows ->
                    val result = linkedMapOf<String, List<String>>()
                    while (rows.next()) {
                        result[rows.getString(1)] = listOf(rows.getString(2), rows.getString(3))
                    }
                    return result
                }
            }
        }
    }
}
