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

import dev.brikk.ducklake.catalog.DucklakeInvalidOperationException
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Path
import java.sql.DriverManager
import java.util.UUID

/** Consumer bounds/write-guard regressions; production planning deliberately retains getColumnStats. */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCatalogBounds {
    @TempDir
    lateinit var data: Path
    private lateinit var config: DucklakeConfig

    @Test
    fun currentBoundsRequireReadSessionAndNeverReplaceExactSqlAggregates() = withLake(
        "CREATE TABLE lake.t(i INTEGER, d DOUBLE); " +
            "INSERT INTO lake.t VALUES (5,1.5),(10,NULL); INSERT INTO lake.t VALUES (-3,'nan'::DOUBLE)",
    ) { runner, catalog ->
        val table = catalog.getTable("main", "t", catalog.currentSnapshotId)!!
        assertThatThrownBy { catalog.getCurrentColumnBounds(table.tableId) }
            .isInstanceOf(DucklakeInvalidOperationException::class.java).hasMessageContaining("readSession")
        catalog.readSession {
            val snapshot = catalog.currentSnapshotId
            val columns = catalog.getTableColumns(table.tableId, snapshot).associate { it.columnName to it.columnId }
            val bounds = catalog.getCurrentColumnBounds(table.tableId).associateBy { it.columnId }
            val stats = catalog.getColumnStats(table.tableId, snapshot).associateBy { it.columnId }
            val integer = bounds.getValue(columns.getValue("i"))
            assertThat(integer.minValue to integer.maxValue).isEqualTo("-3" to "10")
            assertThat(integer.minValue to integer.maxValue)
                .isEqualTo(stats.getValue(integer.columnId).let { it.minValue to it.maxValue })
            assertThat(bounds.getValue(columns.getValue("d")).containsNull).isTrue()
            assertThat(bounds.getValue(columns.getValue("d")).containsNan).isTrue()
        }
        runner.execute("DELETE FROM t WHERE i=10")
        val exact = runner.execute("SELECT min(i),max(i),count(*) FROM t").materializedRows.single()
        assertThat((0..2).map { (exact.getField(it) as Number).toLong() }).containsExactly(-3L, 5L, 2L)
    }

    @Test
    fun omittedBoundsStillHaveFileStatsAndHistoricalQueries() = withLake(
        "CREATE TABLE lake.t(i INTEGER, j INTEGER); INSERT INTO lake.t VALUES (1,2),(3,4)",
    ) { runner, catalog ->
        val oldSnapshot = catalog.currentSnapshotId
        val table = catalog.getTable("main", "t", oldSnapshot)!!
        val columns = catalog.getTableColumns(table.tableId, oldSnapshot).associate { it.columnName to it.columnId }
        runner.execute("INSERT INTO t VALUES (100,200)")
        sql("DELETE FROM ducklake_table_column_stats WHERE table_id=${table.tableId} AND column_id=${columns.getValue("j")}")
        catalog.readSession {
            assertThat(catalog.getCurrentColumnBounds(table.tableId).map { it.columnId })
                .containsExactly(columns.getValue("i"))
            assertThat(catalog.getColumnStats(table.tableId, oldSnapshot).single { it.columnId == columns.getValue("j") }.maxValue)
                .isEqualTo("4")
        }
        assertThat(runner.execute("SELECT max(j) FROM t FOR VERSION AS OF $oldSnapshot").onlyValue).isEqualTo(4)
        assertThat(runner.execute("SELECT max(j) FROM t").onlyValue).isEqualTo(200)
    }

    @Test
    fun combinedWriteGuardsStillEnforceCommitMessagePolicyWithoutPublishing() = withLake(
        "CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES (1)",
    ) { runner, catalog ->
        val before = catalog.currentSnapshotId
        sql("DELETE FROM ducklake_metadata WHERE key='require_commit_message' AND scope IS NULL")
        sql("INSERT INTO ducklake_metadata(key,value) VALUES ('require_commit_message','true')")
        assertThatThrownBy { runner.execute("INSERT INTO t VALUES 2") }.hasMessageContaining("require_commit_message")
        assertThatThrownBy { runner.execute("CREATE SCHEMA refused_schema") }.hasMessageContaining("require_commit_message")
        assertThat(catalog.currentSnapshotId).isEqualTo(before)
        assertThat(runner.execute("SELECT i FROM t").onlyValue).isEqualTo(1)
        sql("UPDATE ducklake_metadata SET value='false' WHERE key='require_commit_message' AND scope IS NULL")
        runner.execute("INSERT INTO t VALUES 2")
        assertThat(runner.execute("SELECT i FROM t ORDER BY i").materializedRows.map { it.getField(0) }).containsExactly(1, 2)
    }

    private fun withLake(seed: String, action: (DistributedQueryRunner, JdbcDucklakeCatalog) -> Unit) {
        val server = DucklakeTestCatalogEnvironment.getServer()
        val database = "trial017_" + UUID.randomUUID().toString().replace("-", "")
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
                statement.execute(seed)
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
                    "ducklake.data-path" to data.toString(), "fs.hadoop.enabled" to "true"))
                JdbcDucklakeCatalog(config.toCatalogConfig()).use { catalog -> action(runner, catalog) }
            }
    }

    private fun sql(query: String) {
        DriverManager.getConnection(config.getCatalogDatabaseUrl(), config.getCatalogDatabaseUser(),
            config.getCatalogDatabasePassword()).use { connection ->
                connection.createStatement().use { it.execute(query) }
            }
    }
}
