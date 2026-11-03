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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager

@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeNativeSnapshotReads : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "native-snapshot-reads"

    @Test
    fun bothRewriteModesAndRecompactionKeepRowOriginsAndHistoricalVisibility() {
        for (partial in listOf(false, true)) exerciseRewrite(partial)
    }

    private fun exerciseRewrite(partial: Boolean) {
        val name = "rewrite_snapshots_${if (partial) "partial" else "ordinary"}"
        val table = "test_schema.$name"
        try {
            native("CALL ducklake_db.set_option('data_inlining_row_limit',0); CREATE TABLE ducklake_db.$table(id BIGINT); " +
                "INSERT INTO ducklake_db.$table VALUES (1)")
            val first = snapshot()
            native("INSERT INTO ducklake_db.$table VALUES (2)")
            val expected = mutableListOf(listOf(1L, first), listOf(2L, snapshot()))
            rewrite(name, partial)
            assertSnapshotRows(table, expected)
            native("INSERT INTO ducklake_db.$table VALUES (3)")
            expected.add(listOf(3L, snapshot()))
            rewrite(name, false)
            assertSnapshotRows(table, expected)
            native("INSERT INTO ducklake_db.$table VALUES (4)")
            expected.add(listOf(4L, snapshot()))
            rewrite(name, true)
            assertSnapshotRows(table, expected)
            assertThat(trinoRows("SELECT id,$INSERT_SNAPSHOT FROM $table FOR VERSION AS OF $first"))
                .containsExactly(listOf(1L, first))
        } finally { tryDropTable(table) }
    }

    private fun rewrite(name: String, partial: Boolean) {
        computeActual("CALL ducklake.system.rewrite_data_files(schema_name=>'test_schema',table_name=>'$name',reclaim_sources_immediately=>$partial)")
    }

    private fun assertSnapshotRows(table: String, expected: List<List<Long>>) {
        assertThat(trinoRows("SELECT id,$INSERT_SNAPSHOT FROM $table ORDER BY id")).isEqualTo(expected)
        assertThat(nativeRows("SELECT id,snapshot_id FROM ducklake_db.$table ORDER BY id")).isEqualTo(expected)
    }

    @Test
    fun nativeSnapshotIdsSurviveInlineFlushHistoryAndPositionDeletes() {
        val table = "test_schema.row_snapshots"
        try {
            native("CALL ducklake_db.set_option('data_inlining_row_limit',100); CREATE TABLE ducklake_db.$table(id BIGINT); " +
                "INSERT INTO ducklake_db.$table VALUES (1),(2)")
            val first = snapshot()
            native("INSERT INTO ducklake_db.$table VALUES (3)")
            val second = snapshot()
            val expected = listOf(listOf(1L, first), listOf(2L, first), listOf(3L, second))
            assertThat(trinoRows("SELECT id,$INSERT_SNAPSHOT FROM $table ORDER BY id")).isEqualTo(expected)
            native("CALL ducklake_flush_inlined_data('ducklake_db',schema_name=>'test_schema',table_name=>'row_snapshots')")
            assertThat(nativeRows("SELECT id,snapshot_id FROM ducklake_db.$table ORDER BY id")).isEqualTo(expected)
            assertThat(trinoRows("SELECT id,$INSERT_SNAPSHOT FROM $table ORDER BY id")).isEqualTo(expected)
            assertThat(trinoRows("SELECT id,$INSERT_SNAPSHOT FROM $table FOR VERSION AS OF $first ORDER BY id"))
                .containsExactly(listOf(1L, first), listOf(2L, first))
            assertThat(trinoRows("SELECT id FROM $table WHERE $INSERT_SNAPSHOT=$second")).containsExactly(listOf(3L))
            assertThat(trinoRows("SELECT id,\"\$snapshot_id\" FROM $table ORDER BY id"))
                .containsExactly(listOf(1L, first), listOf(2L, first), listOf(3L, first))
            assertThat(computeActual("SELECT * FROM $table").types).hasSize(1)
            computeActual("DELETE FROM $table WHERE id=2")
            assertThat(trinoRows("SELECT id,$INSERT_SNAPSHOT FROM $table ORDER BY id"))
                .isEqualTo(nativeRows("SELECT id,snapshot_id FROM ducklake_db.$table ORDER BY id"))
        } finally { tryDropTable(table) }
    }

    @Test
    fun realSnapshotIdColumnTakesPrecedenceAndFreshFilesUseTheirOrigin() {
        val physical = "test_schema.physical_snapshot"
        val fresh = "test_schema.fresh_snapshot"
        try {
            computeActual("CREATE TABLE $physical(id BIGINT)")
            computeActual("INSERT INTO $physical VALUES 1")
            computeActual("ALTER TABLE $physical ADD COLUMN snapshot_id VARCHAR")
            computeActual("UPDATE $physical SET snapshot_id='physical'")
            assertThat(computeActual("SELECT snapshot_id FROM $physical").onlyValue).isEqualTo("physical")
            assertThat(computeActual("SELECT * FROM $physical").types).hasSize(2)
            computeActual("CREATE TABLE $fresh AS SELECT BIGINT '42' id")
            assertThat(trinoRows("SELECT id,$INSERT_SNAPSHOT FROM $fresh"))
                .isEqualTo(nativeRows("SELECT id,snapshot_id FROM ducklake_db.$fresh"))
            assertThat(computeActual("SELECT $INSERT_SNAPSHOT FROM $fresh").onlyValue)
                .isEqualTo(nativeRows("SELECT snapshot_id FROM ducklake_db.$fresh").single().single())
        } finally { tryDropTable(physical); tryDropTable(fresh) }
    }

    private fun native(sql: String) = createDuckdbConnection().use { c -> c.createStatement().use { it.execute(sql) }; Unit }

    private fun nativeRows(sql: String): List<List<Long>> = createDuckdbConnection().use { c ->
        c.createStatement().use { s -> s.executeQuery(sql).use { r ->
            buildList { while (r.next()) add((1..r.metaData.columnCount).map { r.getLong(it) }) }
        } }
    }

    private fun trinoRows(sql: String): List<List<Long>> = computeActual(sql).materializedRows.map { row ->
        (0 until row.fieldCount).map { (row.getField(it) as Number).toLong() }
    }

    private fun snapshot(): Long {
        val catalog = getIsolatedCatalog()
        return DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { c ->
            c.createStatement().use { s -> s.executeQuery("SELECT max(snapshot_id) FROM ducklake_snapshot").use { r ->
                check(r.next()); r.getLong(1)
            } }
        }
    }

    private companion object {
        const val INSERT_SNAPSHOT = "\"\$insert_snapshot_id\""
    }
}
