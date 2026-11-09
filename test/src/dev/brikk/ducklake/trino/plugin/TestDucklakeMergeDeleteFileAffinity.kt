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

/**
 * Row changes from one source data file must reach one merge sink. Multiple active delete files
 * make upstream's plain LEFT JOIN return the data file twice, duplicating survivors filtered by
 * different partial delete sets.
 */
class TestDucklakeMergeDeleteFileAffinity : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "merge-delete-affinity"

    @Test
    fun mergeWritesAtMostOneActiveDeleteFilePerDataFile()
    {
        val table = "test_schema.merge_file_affinity"
        try {
            computeActual("CREATE TABLE $table AS SELECT i AS id FROM UNNEST(sequence(1, 1000)) AS t(i)")
            // Several matches spread through one source file: without update-layout affinity this
            // reliably reaches more than one local writer sink in DistributedQueryRunner.
            computeActual(
                    "MERGE INTO $table t USING (VALUES 1, 203, 407, 611, 815, 999) s(id) " +
                            "ON t.id = s.id WHEN MATCHED THEN DELETE")

            val isolated = getIsolatedCatalog()
            DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery(
                            "SELECT max(n) FROM (" +
                                    "SELECT count(*) n FROM ducklake_delete_file d " +
                                    "JOIN ducklake_table t ON t.table_id = d.table_id " +
                                    "WHERE t.table_name = 'merge_file_affinity' AND t.end_snapshot IS NULL " +
                                    "AND d.end_snapshot IS NULL GROUP BY d.data_file_id) x").use { rows ->
                        assertThat(rows.next()).isTrue()
                        assertThat(rows.getLong(1))
                                .`as`("one active delete file per source data file")
                                .isEqualTo(1L)
                    }
                }
            }

            createDuckdbConnection().use { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery(
                            "SELECT count(*), count(DISTINCT id), min(id), max(id) " +
                                    "FROM ducklake_db.$table").use { rows ->
                        assertThat(rows.next()).isTrue()
                        assertThat(rows.getLong(1)).`as`("DuckDB survivor rows").isEqualTo(994L)
                        assertThat(rows.getLong(2)).`as`("no duplicate survivors").isEqualTo(994L)
                        assertThat(rows.getInt(3)).isEqualTo(2)
                        assertThat(rows.getInt(4)).isEqualTo(1000)
                    }
                }
            }
        }
        finally {
            tryDropTable(table)
        }
    }
}
