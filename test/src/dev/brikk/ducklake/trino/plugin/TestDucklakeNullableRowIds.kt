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

import dev.brikk.ducklake.catalog.testing.CatalogQueries
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import io.trino.Session
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager

/** Real native rewrites followed by worker-exchanged Trino mutations and native observations. */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeNullableRowIds : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "nullable-row-ids"

    @Test
    fun nullableStartsSupportReadsDeletesUpdatesAndMerge() = exerciseMutations("ci02_null", null)

    @Test
    fun embeddedLineageWinsEvenWithOverlappingNonNullStarts() = exerciseMutations("ci02_present", 777L)

    private fun exerciseMutations(name: String, overrideStart: Long?) {
        val table = "test_schema.$name"
        val nativeTable = "ducklake_db.$table"
        try {
            computeActual("CREATE TABLE $table (id BIGINT, val BIGINT, bucket BIGINT)")
            createDuckdbConnection().use { connection ->
                connection.createStatement().use { sql ->
                    sql.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 0)")
                    // Distinct partitions prevent the native rewrite from merging the two tiny
                    // inputs into one file: mutation routing must handle two NULL-start sources.
                    sql.execute("ALTER TABLE $nativeTable SET PARTITIONED BY (bucket)")
                    sql.execute("INSERT INTO $nativeTable SELECT i, i, 0 FROM range(8) t(i)")
                    sql.execute("INSERT INTO $nativeTable SELECT i, i, 1 FROM range(8, 16) t(i)")
                    sql.execute("DELETE FROM $nativeTable WHERE id IN (1, 3, 9, 11)")
                    sql.execute("CALL ducklake_flush_inlined_data('ducklake_db')")
                    sql.execute("CALL ducklake_rewrite_data_files('ducklake_db', '$name', schema => 'test_schema', delete_threshold => 0)")
                }
            }
            // Prove the native control actually produced two files with absent starts and ID gaps.
            assertThat(computeScalar("SELECT count(*) FROM test_schema.\"$name\$files\" WHERE row_id_start IS NULL") as Long)
                    .isEqualTo(2L)
            val original = nativeRows("SELECT id, val, rowid FROM $nativeTable ORDER BY id")
            assertThat(original.map { it[0] }).containsExactly(0L, 2L, 4L, 5L, 6L, 7L, 8L, 10L, 12L, 13L, 14L, 15L)
            val originalIds = original.associate { it[0] to it[2] }
            if (overrideStart != null) setStarts(name, overrideStart.toString())
            assertThat(trinoRows("SELECT id, val, \"\$row_id\" FROM $table ORDER BY id")).isEqualTo(original)
            val beforeDelete = snapshot()

            computeActual("DELETE FROM $table WHERE id IN (2, 10)")
            val deletionIds = trinoRows("SELECT id, rowid FROM TABLE(ducklake.system.table_deletions(" +
                    "'test_schema', '$name', ${beforeDelete + 1}, ${snapshot()})) ORDER BY id")
            assertThat(deletionIds).containsExactly(listOf(2L, originalIds.getValue(2)), listOf(10L, originalIds.getValue(10)))
            // A pruned/filtered batch must not renumber the physical positions or historical rows.
            assertThat(trinoRows("SELECT id, val, \"\$row_id\" FROM $table FOR VERSION AS OF $beforeDelete ORDER BY id"))
                    .isEqualTo(original)

            val lineageSession = Session.builder(session)
                    .setCatalogSessionProperty("ducklake", DucklakeSessionProperties.WRITE_ROW_LINEAGE, "true").build()
            val beforeUpdate = snapshot()
            computeActual(lineageSession, "UPDATE $table SET val = val + 100 WHERE id IN (4, 12)")
            val afterUpdate = snapshot()
            val changes = computeActual("SELECT change_type, id, rowid FROM TABLE(ducklake.system.table_changes(" +
                    "'test_schema', '$name', ${beforeUpdate + 1}, $afterUpdate)) ORDER BY change_type, id").materializedRows
            assertThat(changes.map { it.getField(0) }).containsExactly(
                    "update_postimage", "update_postimage", "update_preimage", "update_preimage")
            changes.forEach { assertThat(it.getField(2)).isEqualTo(originalIds.getValue(it.getField(1) as Long)) }

            computeActual(lineageSession, "MERGE INTO $table t USING (VALUES BIGINT '5', BIGINT '13', BIGINT '100') s(id) " +
                    "ON t.id = s.id WHEN MATCHED THEN UPDATE SET val = t.val + 200 " +
                    "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, 999)")
            val result = trinoRows("SELECT id, val, \"\$row_id\" FROM $table ORDER BY id")
            assertThat(result).isEqualTo(nativeRows("SELECT id, val, rowid FROM $nativeTable ORDER BY id"))
            assertThat(result.map { it[0] }).containsExactly(0L, 4L, 5L, 6L, 7L, 8L, 12L, 13L, 14L, 15L, 100L)
            for ((id, value, rowId) in result) {
                if (id == 100L) {
                    assertThat(value).isEqualTo(999L)
                    assertThat(rowId).isGreaterThan(originalIds.values.max())
                } else {
                    assertThat(rowId).isEqualTo(originalIds.getValue(id))
                    assertThat(value).isEqualTo(id + when (id) { 4L, 12L -> 100L; 5L, 13L -> 200L; else -> 0L })
                }
            }
        } finally {
            tryDropTable(table)
        }
    }

    @Test
    fun absentStartWithoutLineageAllowsDataAndPositionReadsButNotLogicalIds() {
        val name = "ci02_missing_identity"
        val table = "test_schema.$name"
        try {
            computeActual("CREATE TABLE $table (id BIGINT)")
            computeActual("INSERT INTO $table VALUES 10, 20, 30")
            setStarts(name, "NULL")
            assertThat(trinoRows("SELECT id, \"\$file_row_number\" FROM $table WHERE id >= 20 ORDER BY id"))
                    .containsExactly(listOf(20L, 1L), listOf(30L, 2L))
            assertThatThrownBy { computeActual("SELECT \"\$row_id\" FROM $table") }
                    .hasMessageContaining("neither is available")
            // DELETE only needs physical identity; it must not force a fabricated logical ID.
            computeActual("DELETE FROM $table WHERE id = 20")
            assertThat(trinoRows("SELECT id, \"\$file_row_number\" FROM $table ORDER BY id"))
                    .containsExactly(listOf(10L, 0L), listOf(30L, 2L))
        } finally {
            tryDropTable(table)
        }
    }

    private fun trinoRows(sql: String): List<List<Long>> = computeActual(sql).materializedRows.map { row ->
        (0 until row.fieldCount).map { row.getField(it) as Long }
    }

    private fun nativeRows(sql: String): List<List<Long>> = createDuckdbConnection().use { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { rows ->
                buildList { while (rows.next()) add((1..rows.metaData.columnCount).map { rows.getLong(it) }) }
            }
        }
    }

    private fun snapshot(): Long {
        val catalog = getIsolatedCatalog()
        return DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use {
            CatalogQueries.latestSnapshotId(CatalogTestSupport.dsl(it))
        }
    }

    private fun setStarts(name: String, value: String) {
        val catalog = getIsolatedCatalog()
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { connection ->
            connection.createStatement().use { statement ->
                statement.executeUpdate("UPDATE ducklake_data_file SET row_id_start = $value WHERE end_snapshot IS NULL " +
                        "AND table_id = (SELECT table_id FROM ducklake_table WHERE table_name = '$name' AND end_snapshot IS NULL)")
            }
        }
    }
}
