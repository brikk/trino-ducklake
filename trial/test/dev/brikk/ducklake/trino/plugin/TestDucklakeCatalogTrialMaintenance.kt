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
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.UUID

/** Opt-in published 0.15 trial only; the normal build still compiles against 0.14. */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCatalogTrialMaintenance {
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
        sql("CREATE TRIGGER trial_fail_cleanup BEFORE INSERT ON ducklake_files_scheduled_for_deletion " +
            "FOR EACH ROW EXECUTE FUNCTION trial_fail_cleanup()")
        try {
            assertThatThrownBy {
                runner.execute("CALL system.expire_snapshots(snapshot_ids => ARRAY[${ids.joinToString(",")}])")
            }.hasMessageContaining("trial cleanup failure")
            assertThat(scalar("SELECT count(*) FROM ducklake_snapshot WHERE snapshot_id IN (${ids.joinToString(",")})")).isZero()
            assertThat(scalar("SELECT count(*) FROM ducklake_data_file WHERE table_id=$table AND end_snapshot IS NOT NULL"))
                .isEqualTo(2)
            assertThat(scalar("SELECT count(*) FROM ducklake_files_scheduled_for_deletion")).isZero()
            assertThat(runner.execute("SELECT id FROM t").onlyValue).isEqualTo(3)
        } finally {
            sql("DROP TRIGGER trial_fail_cleanup ON ducklake_files_scheduled_for_deletion")
            sql("DROP FUNCTION trial_fail_cleanup()")
        }
        assertThat(drain(catalog, table)).isEqualTo(2)
        assertThat(catalog.cleanUpExpiredObjects(4).cleanupComplete).isTrue()
        runner.execute("CALL system.cleanup_old_files(retention_threshold => '0s')")
        assertThat(scalar("SELECT count(*) FROM ducklake_files_scheduled_for_deletion")).isZero()
        assertThat(runner.execute("SELECT id FROM t").onlyValue).isEqualTo(3)
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
        server.createDatabase(database)
        config = DucklakeConfig().setCatalogDatabaseUrl(server.getJdbcUrl(database))
            .setCatalogDatabaseUser(server.getUser()).setCatalogDatabasePassword(server.getPassword())
            .setDataPath(data.toString())
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres")
                statement.execute("ATTACH '${server.getDuckDbAttachUri(database)}' AS lake " +
                    "(DATA_PATH '$data/', DUCKLAKE_VERSION '1.0', AUTOMATIC_MIGRATION FALSE)")
                statement.execute("CALL lake.set_option('data_inlining_row_limit',0)")
            }
        }
        val session = testSessionBuilder().setCatalog("ducklake").setSchema("main").build()
        DistributedQueryRunner.builder(session).setWorkerCount(2)
            .addCoordinatorProperty("node-scheduler.include-coordinator", "false").build().use { runner ->
                runner.installPlugin(DucklakePlugin())
                runner.createCatalog("ducklake", "ducklake", mapOf(
                    "ducklake.catalog.database-url" to server.getJdbcUrl(database),
                    "ducklake.catalog.database-user" to server.getUser(),
                    "ducklake.catalog.database-password" to server.getPassword(),
                    "ducklake.data-path" to data.toString(), "ducklake.maintenance.min-retention" to "0s",
                    "fs.hadoop.enabled" to "true"))
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
