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

import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Change-feed correctness over a CONSOLIDATED ("partial") DELETE file.
 * DuckLake's delete consolidation (flush_inlined_data) folds
 * deletions recorded at several snapshots into ONE 3-column delete file (`file_path, pos,
 * _ducklake_internal_snapshot_id`) whose `begin_snapshot` = MIN(deletion snapshots) and
 * `partial_max` may be MAX or SQL NULL. The change feed must inspect the physical schema, report
 * each deletion at its OWN snapshot, and window accordingly — not attribute the whole batch to
 * `begin_snapshot` or infer the shape from `partial_max`.
 *
 * SAME_THREAD: single shared local catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeChangeFeedPartialDelete : AbstractTestQueryFramework() {

    private lateinit var rootDir: Path
    private var delSnapA: Long = -1 // first deletion (id=1) — begin_snapshot of the consolidated file
    private var delSnapB: Long = -1 // second deletion (id=5) — partial_max

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        rootDir = Files.createTempDirectory("ducklake-cf-pdel-")
        val lakeDb = rootDir.resolve("lake.db").toAbsolutePath().toString()
        val dataDir = rootDir.resolve("data").toAbsolutePath().toString()

        DucklakeTestCatalogEnvironment.createDuckdbConnection("ducklake:$lakeDb", "$dataDir/").use { conn ->
            conn.createStatement().use { st ->
                st.execute("USE lake")
                st.execute("CREATE SCHEMA test_schema")
                st.execute("CREATE TABLE test_schema.t AS SELECT i::INTEGER AS id FROM range(10) t(i)")
                st.execute("CALL ducklake_flush_inlined_data('lake')")   // materialize to a data file
                st.execute("DELETE FROM test_schema.t WHERE id = 1")      // deletion @ snapshot A
                st.execute("DELETE FROM test_schema.t WHERE id = 5")      // deletion @ snapshot B
                st.execute("CALL ducklake_flush_inlined_data('lake')")   // consolidate into one partial delete file
            }
        }
        DriverManager.getConnection("jdbc:duckdb:$lakeDb").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT begin_snapshot, partial_max FROM ducklake_delete_file "
                        + "WHERE partial_max IS NOT NULL ORDER BY delete_file_id LIMIT 1").use { rs ->
                    check(rs.next()) { "delete consolidation did not produce a partial delete file" }
                    delSnapA = rs.getLong(1)
                    delSnapB = rs.getLong(2)
                }
                // Upstream flush-written files use embedded snapshots even when partial_max is NULL.
                st.executeUpdate("UPDATE ducklake_delete_file SET partial_max = NULL WHERE partial_max IS NOT NULL")
            }
        }
        check(delSnapB > delSnapA) { "expected partial_max ($delSnapB) > begin ($delSnapA)" }

        return DucklakeQueryRunner.builder()
            .useCatalog(DucklakeConfig().setCatalogDatabaseUrl("jdbc:duckdb:$lakeDb").setDataPath("$dataDir/"))
            .build()
    }

    @AfterAll
    fun cleanup() {
        if (::rootDir.isInitialized && Files.exists(rootDir)) {
            Files.walk(rootDir).use { w -> w.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }

    private fun deletions(from: Long, to: Long): List<Pair<Long, Int>> =
            computeActual("SELECT snapshot_id, id FROM " +
                    "TABLE(ducklake.system.table_deletions('test_schema', 't', $from, $to)) ORDER BY id")
                    .materializedRows
                    .map { (it.getField(0) as Long) to (it.getField(1) as Int) }

    @Test
    fun eachDeletionAttributedToItsOwnSnapshot() {
        val all = deletions(1, delSnapB)
        assertThat(all).containsExactlyInAnyOrder(delSnapA to 1, delSnapB to 5)
    }

    @Test
    fun windowExcludingFirstDeletionDropsIt() {
        // Window [A+1, B]: only id=5's deletion (snapshot B) qualifies; id=1's deletion (snapshot A
        // < start) must be excluded even though it lives in the same consolidated file whose
        // begin_snapshot == A.
        val rows = deletions(delSnapA + 1, delSnapB)
        assertThat(rows).containsExactly(delSnapB to 5)
    }

    @Test
    fun windowOnlyFirstDeletionExcludesLater() {
        val rows = deletions(delSnapA, delSnapA)
        assertThat(rows).containsExactly(delSnapA to 1)
    }
}
