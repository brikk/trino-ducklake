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

import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Properties
import java.util.function.Supplier

/** Release-driver qualification, separate from the historical R07 producer pins. */
class TestDucklakeReleasedQuack {
    @TempDir
    lateinit var data: Path

    @Test
    fun final11AdmissionFailsBeforeOpeningAnyConnection() {
        val config = DucklakeConfig().setCatalogDatabaseUrl("jdbc:unsupported:no-connection-should-be-opened")
            .setEnableFinal11Reads(true)
        assertThatThrownBy { DucklakeCatalogProvider(config).get() }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("explicitly selected catalog candidate build")
    }

    @Test
    fun releasedDriversReadEvolvedInlineAndParquetAndPublishTrinoMutations() {
        TestingDucklakeDuckDbQuackCatalogServer().use { server ->
            native(server, "CREATE TABLE lake.main.t(i INTEGER); INSERT INTO lake.main.t VALUES (1), (2); " +
                "CALL ducklake_flush_inlined_data('lake'); INSERT INTO lake.main.t VALUES (3); " +
                "ALTER TABLE lake.main.t ADD COLUMN label VARCHAR DEFAULT 'seed'")
            val url = "jdbc:quack://${server.getHost()}:${server.getMappedPort()}"
            DriverManager.getConnection(url, Properties().apply { setProperty("token", server.getToken()) }).use { connection ->
                assertThat(connection.metaData.driverVersion).isEqualTo("0.13.0")
            }
            val config = DucklakeConfig().setCatalogDatabaseUrl(url).setCatalogDatabasePassword(server.getToken())
                .setDataPath(data.toString()).setMaxCatalogConnections(1)
            verifyReadSessions(config.toCatalogConfig())
            val session = testSessionBuilder().setCatalog("lake").setSchema("main").build()
            DistributedQueryRunner.builder(session).setWorkerCount(2)
                .addCoordinatorProperty("node-scheduler.include-coordinator", "false").build().use { runner ->
                    runner.installPlugin(DucklakePlugin())
                    runner.createCatalog("lake", "ducklake", mapOf(
                        "ducklake.catalog.database-url" to url,
                        "ducklake.catalog.database-password" to server.getToken(),
                        "ducklake.data-path" to data.toString(), "fs.hadoop.enabled" to "true"))
                    verifyWorkerMutations(runner, server)
                }
        }
    }

    private fun native(server: TestingDucklakeDuckDbQuackCatalogServer, sql: String) {
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            assertThat(connection.javaClass.protectionDomain.codeSource.location.path).endsWith("duckdb_jdbc-2.0.0.1.jar")
            connection.createStatement().use { statement ->
                statement.execute("INSTALL ducklake; LOAD ducklake; INSTALL quack; LOAD quack")
                statement.execute("CREATE SECRET (TYPE quack, TOKEN '${server.getToken()}')")
                statement.execute("ATTACH '${server.getDucklakeAttachUri()}' AS lake " +
                    "(DATA_PATH '$data', METADATA_CATALOG 'release_meta', DUCKLAKE_VERSION '1.0', AUTOMATIC_MIGRATION FALSE)")
                statement.execute(sql)
            }
        }
    }

    private fun verifyReadSessions(config: DucklakeCatalogConfig) {
        JdbcDucklakeCatalog(config).use { catalog ->
            assertThat(catalog.getSpecVersion()).isEqualTo("1.0")
            // Repeated owned sessions exercise early-ending metadata results on one connection.
            repeat(20) {
                catalog.readSession(Supplier {
                    val snapshot = catalog.currentSnapshotId
                    val table = catalog.getTable("main", "t", snapshot)!!
                    assertThat(catalog.getTableColumns(table.tableId, snapshot).map { it.columnName }).containsExactly("i", "label")
                    assertThat(catalog.getDataFiles(table.tableId, snapshot)).isNotEmpty()
                    assertThat(catalog.getInlinedDataInfos(table.tableId, snapshot)).isNotEmpty()
                })
            }
        }
    }

    private fun verifyWorkerMutations(runner: DistributedQueryRunner, server: TestingDucklakeDuckDbQuackCatalogServer) {
        fun rows() = runner.execute("SELECT i, label FROM t ORDER BY i").materializedRows.map { it.fields }
        assertThat(rows()).containsExactly(listOf(1, "seed"), listOf(2, "seed"), listOf(3, "seed"))
        runner.execute("INSERT INTO t VALUES (4, 'trino')")
        assertThatThrownBy { runner.execute("UPDATE t SET label = 'updated' WHERE i = 4") }.hasMessageContaining("has inlined rows")
        native(server, "CALL ducklake_flush_inlined_data('lake')")
        runner.execute("UPDATE t SET label = 'updated' WHERE i = 4")
        runner.execute("DELETE FROM t WHERE i = 1")
        assertThat(rows()).containsExactly(listOf(2, "seed"), listOf(3, "seed"), listOf(4, "updated"))
        native(server, "CREATE TABLE lake.main.native_check AS SELECT * FROM lake.main.t")
        assertThat(runner.execute("SELECT i, label FROM native_check ORDER BY i").materializedRows.map { it.fields })
            .containsExactly(listOf(2, "seed"), listOf(3, "seed"), listOf(4, "updated"))
    }
}
