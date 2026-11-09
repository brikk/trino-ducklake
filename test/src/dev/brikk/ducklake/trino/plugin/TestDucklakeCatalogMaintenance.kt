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

import dev.brikk.ducklake.catalog.ExpireSnapshotsResult
import dev.brikk.ducklake.catalog.DucklakeExpiryCleanupIncompleteException
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.testing.DistributedQueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.UUID

/** Published catalog maintenance contracts, exercised through the connector and real metadata. */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCatalogMaintenance {
    @TempDir
    lateinit var data: Path
    private lateinit var config: DucklakeConfig

    @Test
    fun budgetedExpiryResumesByTableAndCollectsDroppedTableMetadata() = withLake { runner, catalog ->
        runner.execute("CREATE TABLE a AS SELECT 1 id")
        runner.execute("INSERT INTO a VALUES 2")
        runner.execute("TRUNCATE TABLE a")
        runner.execute("CREATE TABLE b AS SELECT 4 id")
        runner.execute("INSERT INTO b VALUES 5")
        val a = scalar("SELECT table_id FROM ducklake_table WHERE table_name='a' AND end_snapshot IS NULL")
        val b = scalar("SELECT table_id FROM ducklake_table WHERE table_name='b' AND end_snapshot IS NULL")
        runner.execute("DROP TABLE b")
        runner.execute("INSERT INTO a VALUES 3")
        val ids = catalog.listExpirableSnapshots(null, null).toSet()

        val expired = catalog.expireSnapshots(ids, 0)
        assertThat(expired.expiredSnapshotCount).isEqualTo(ids.size)
        assertThat(expired.scheduledFileCount).isZero()
        assertThat(expired.cleanupComplete).isFalse()
        assertThat(catalog.listTablesWithExpiredFiles(null, 1)).containsExactly(a)
        assertThat(catalog.listTablesWithExpiredFiles(a, 1)).containsExactly(b)
        assertThat(catalog.listTablesWithExpiredFiles(b, 1)).isEmpty()

        assertThat(drain(catalog, a)).isEqualTo(2)
        assertThat(scalar("SELECT count(*) FROM ducklake_data_file WHERE table_id=$b")).isEqualTo(2)
        assertThat(catalog.listTablesWithExpiredFiles(null, 1)).containsExactly(b)
        assertThat(drain(catalog, b)).isEqualTo(2)
        assertThat(scalar("SELECT count(*) FROM ducklake_table WHERE table_id=$b")).isZero()
        assertThat(catalog.listTablesWithExpiredFiles(null, 1)).isEmpty()
        assertThat(catalog.cleanUpExpiredObjects(4).cleanupComplete).isTrue()
        assertThat(catalog.cleanUpExpiredObjects(4).scheduledFileCount).isZero()
        assertThat(runner.execute("SELECT id FROM a").onlyValue).isEqualTo(3)
        assertThat(ExpireSnapshotsResult(0, 0).cleanupComplete).isTrue()
    }

    @Test
    fun procedureCleanupFailureLeavesSnapshotsExpiredAndExplicitCleanupResumes() = withLake { runner, catalog ->
        runner.execute("CREATE TABLE t AS SELECT 1 id")
        runner.execute("INSERT INTO t VALUES 2")
        runner.execute("TRUNCATE TABLE t")
        runner.execute("INSERT INTO t VALUES 3")
        val table = scalar("SELECT table_id FROM ducklake_table WHERE table_name='t' AND end_snapshot IS NULL")
        val ids = catalog.listExpirableSnapshots(null, null).toSet()
        sql("CREATE FUNCTION trial_fail_cleanup() RETURNS trigger LANGUAGE plpgsql AS " +
            "'BEGIN RAISE EXCEPTION ''trial cleanup failure''; END;'")
        // Fail after queue insertion, so the whole page rolls back and must not inflate reported counts.
        sql("CREATE TRIGGER trial_fail_cleanup BEFORE DELETE ON ducklake_data_file " +
            "FOR EACH ROW EXECUTE FUNCTION trial_fail_cleanup()")
        try {
            val failure = catchThrowable {
                DucklakeExpireSnapshotsProcedure(catalog, config).expireSnapshots(SESSION, null, ids.toList(), false)
            }
            assertThat(failure).isInstanceOf(DucklakeExpiryCleanupIncompleteException::class.java)
                .hasMessageContaining("expiry is committed").hasMessageContaining("cleanUpExpiredObjects")
            val incomplete = failure as DucklakeExpiryCleanupIncompleteException
            assertThat(incomplete.expiredSnapshotCount).isEqualTo(ids.size)
            assertThat(incomplete.scheduledFileCount).isZero()
            assertThat(incomplete.scheduledFileCount.toLong())
                .isEqualTo(scalar("SELECT count(*) FROM ducklake_files_scheduled_for_deletion"))
            assertThat(incomplete.cause).hasMessageContaining("trial cleanup failure")
            assertThat(scalar("SELECT count(*) FROM ducklake_snapshot WHERE snapshot_id IN (${ids.joinToString(",")})")).isZero()
            assertThat(scalar("SELECT count(*) FROM ducklake_data_file WHERE table_id=$table AND end_snapshot IS NOT NULL"))
                .isEqualTo(2)
            assertThat(scalar("SELECT count(*) FROM ducklake_files_scheduled_for_deletion")).isZero()
            assertThat(runner.execute("SELECT id FROM t").onlyValue).isEqualTo(3)
        } finally {
            sql("DROP TRIGGER trial_fail_cleanup ON ducklake_data_file")
            sql("DROP FUNCTION trial_fail_cleanup()")
        }
        assertThat(drain(catalog, table)).isEqualTo(2)
        assertThat(catalog.cleanUpExpiredObjects(4).cleanupComplete).isTrue()
        runner.execute("CALL system.cleanup_old_files(retention_threshold => '0s')")
        assertThat(scalar("SELECT count(*) FROM ducklake_files_scheduled_for_deletion")).isZero()
        assertThat(runner.execute("SELECT id FROM t").onlyValue).isEqualTo(3)
    }

    @Test
    fun dataFileDeleteFanoutObeysTheHardBudgetAndMakesProgress() = withLake { runner, catalog ->
        runner.execute("CREATE TABLE t AS SELECT * FROM (VALUES 1,2,3,4) AS u(id)")
        runner.execute("DELETE FROM t WHERE id=1")
        runner.execute("DELETE FROM t WHERE id=2")
        val table = scalar("SELECT table_id FROM ducklake_table WHERE table_name='t' AND end_snapshot IS NULL")
        runner.execute("TRUNCATE TABLE t")
        runner.execute("INSERT INTO t VALUES 9")
        val deletes = scalar("SELECT count(*) FROM ducklake_delete_file WHERE table_id=$table")
        assertThat(deletes).isGreaterThanOrEqualTo(2)
        val expected = 1 + deletes.toInt()
        val first = catalog.expireSnapshots(catalog.listExpirableSnapshots(null, null).toSet(), 1)
        assertThat(first.scheduledFileCount).isEqualTo(1)
        assertThat(first.cleanupComplete).isFalse()
        assertThat(scalar("SELECT count(*) FROM ducklake_data_file WHERE table_id=$table AND end_snapshot IS NOT NULL"))
            .isEqualTo(1)
        var scheduled = first.scheduledFileCount
        var complete = false
        repeat(8) {
            if (!complete) {
                val next = catalog.cleanUpExpiredObjects(table, 1)
                assertThat(next.scheduledFileCount).isBetween(0, 1)
                scheduled += next.scheduledFileCount
                complete = next.cleanupComplete
            }
        }
        assertThat(complete).isTrue()
        assertThat(scheduled).isEqualTo(expected)
        assertThat(catalog.cleanUpExpiredObjects(1).scheduledFileCount).isZero()
        assertThat(runner.execute("SELECT id FROM t").onlyValue).isEqualTo(9)
    }

    @Test
    fun legacyResultCopyAndKotlinDefaultBridgeStillLink() {
        val type = ExpireSnapshotsResult::class.java
        val result = ExpireSnapshotsResult(2, 3, false)
        val copy = type.getMethod("copy", Int::class.javaPrimitiveType, Int::class.javaPrimitiveType)
        assertThat(copy.invoke(result, 4, 5)).isEqualTo(ExpireSnapshotsResult(4, 5, false))
        val defaults = type.getMethod("copy\$default", type, Int::class.javaPrimitiveType,
            Int::class.javaPrimitiveType, Int::class.javaPrimitiveType, Any::class.java)
        assertThat(defaults.invoke(null, result, 0, 6, 1, null)).isEqualTo(ExpireSnapshotsResult(2, 6, false))
        assertThat(defaults.invoke(null, result, 7, 0, 2, null)).isEqualTo(ExpireSnapshotsResult(7, 3, false))
        assertThat(defaults.invoke(null, result, 0, 0, 3, null)).isEqualTo(result)
    }

    @Test
    fun duplicateDeletionRequestsForAnAlreadyMissingFileAreAcknowledged() = withLake { runner, catalog ->
        runner.execute("CREATE TABLE t AS SELECT 1 id")
        val oldPath = runner.execute("SELECT \"\$path\" FROM t").onlyValue as String
        runner.execute("TRUNCATE TABLE t")
        runner.execute("INSERT INTO t VALUES 2")
        val result = catalog.expireSnapshots(catalog.listExpirableSnapshots(null, null).toSet())
        assertThat(result.cleanupComplete).isTrue()
        assertThat(result.scheduledFileCount).isEqualTo(1)
        sql("INSERT INTO ducklake_files_scheduled_for_deletion SELECT * FROM ducklake_files_scheduled_for_deletion")
        assertThat(scalar("SELECT count(*) FROM ducklake_files_scheduled_for_deletion")).isEqualTo(2)
        val physical = if (oldPath.startsWith("file:")) Path.of(URI.create(oldPath)) else Path.of(oldPath)
        assertThat(physical.startsWith(data)).isTrue()
        Files.delete(physical)

        runner.execute("CALL system.cleanup_old_files(retention_threshold => '0s')")
        assertThat(scalar("SELECT count(*) FROM ducklake_files_scheduled_for_deletion")).isZero()
        assertThat(runner.execute("SELECT id FROM t").onlyValue).isEqualTo(2)
    }

    private fun drain(catalog: JdbcDucklakeCatalog, table: Long): Int {
        var scheduled = 0
        repeat(6) {
            val result = catalog.cleanUpExpiredObjects(table, 1)
            assertThat(result.expiredSnapshotCount).isZero()
            assertThat(result.scheduledFileCount).isBetween(0, 1)
            scheduled += result.scheduledFileCount
            if (result.cleanupComplete) return scheduled
        }
        error("Small table cleanup did not complete within six single-file calls")
    }

    private fun withLake(action: (DistributedQueryRunner, JdbcDucklakeCatalog) -> Unit) {
        val server = DucklakeTestCatalogEnvironment.getServer()
        val database = "trial_" + UUID.randomUUID().toString().replace("-", "")
        config = DucklakeTestCatalogEnvironment.createPostgresCatalog(database, "$data/").setDataPath(data.toString())
        DucklakeTestCatalogEnvironment.createDuckdbConnection(server.getDuckDbAttachUri(database), "$data/").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CALL lake.set_option('data_inlining_row_limit',0)")
            }
        }
        DucklakeQueryRunner.builder().useCatalog(config).amendSession { it.setSchema("main") }.setWorkerCount(2)
            .addConnectorProperty("ducklake.maintenance.min-retention", "0s")
            .addCoordinatorProperty("node-scheduler.include-coordinator", "false").build().use { runner ->
                JdbcDucklakeCatalog(config.toCatalogConfig()).use { catalog -> action(runner, catalog) }
            }
    }

    private fun sql(query: String) = connection { it.createStatement().use { statement -> statement.execute(query) }; Unit }

    private fun scalar(query: String): Long = connection { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(query).use { result -> check(result.next()); result.getLong(1) }
        }
    }

    private fun <T> connection(action: (java.sql.Connection) -> T): T =
        DriverManager.getConnection(config.getCatalogDatabaseUrl(), config.getCatalogDatabaseUser(),
            config.getCatalogDatabasePassword()).use(action)
}
