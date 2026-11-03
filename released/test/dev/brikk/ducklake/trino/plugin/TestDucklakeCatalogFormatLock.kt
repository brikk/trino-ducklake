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

@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCatalogFormatLock {
    @TempDir
    lateinit var data: Path

    @Test
    fun format10CatalogRefusesBothKindsOf11TableBeforeReadingOrWriting() = withLockedCatalog("1.1") { _, runner ->
        for (sql in listOf("SELECT i FROM parquet_t", "SELECT i FROM inline_t", "INSERT INTO parquet_t VALUES 43")) {
            assertThatThrownBy { runner.execute(sql) }.hasMessageContaining("1.1").hasMessageContaining("1.0")
        }
    }

    @Test
    fun existingCatalogRechecksItsFormatAfterAnotherEngineMigratesIt() = withLockedCatalog("1.0") { uri, runner ->
        assertThat(runner.execute("SELECT i FROM parquet_t").onlyValue).isEqualTo(42)
        native(uri, "1.1", true, "SELECT 1")
        for (sql in listOf("SELECT i FROM parquet_t", "SELECT i FROM inline_t", "INSERT INTO parquet_t VALUES 43")) {
            assertThatThrownBy { runner.execute(sql) }.hasMessageContaining("1.1").hasMessageContaining("1.0")
        }
    }

    private fun withLockedCatalog(format: String, action: (String, DistributedQueryRunner) -> Unit) {
        val server = DucklakeTestCatalogEnvironment.getServer()
        val database = "format_" + UUID.randomUUID().toString().replace("-", "")
        server.createDatabase(database)
        val uri = server.getDuckDbAttachUri(database)
        native(uri, format, false, "CALL lake.set_option('data_inlining_row_limit',0); " +
            "CREATE TABLE lake.parquet_t AS SELECT 42::INTEGER i; " +
            "CALL lake.set_option('data_inlining_row_limit',100); CREATE TABLE lake.inline_t AS SELECT 42::INTEGER i")
        val session = testSessionBuilder().setCatalog("locked").setSchema("main").build()
        DistributedQueryRunner.builder(session).setWorkerCount(2)
            .addCoordinatorProperty("node-scheduler.include-coordinator", "false").build().use { runner ->
                runner.installPlugin(DucklakePlugin())
                runner.createCatalog("locked", "ducklake", mapOf(
                    "ducklake.catalog.database-url" to server.getJdbcUrl(database),
                    "ducklake.catalog.database-user" to server.getUser(),
                    "ducklake.catalog.database-password" to server.getPassword(),
                    "ducklake.catalog.format-version" to "1.0", "ducklake.data-path" to data.toString(),
                    "fs.hadoop.enabled" to "true"))
                action(uri, runner)
            }
    }

    private fun native(uri: String, format: String, migrate: Boolean, sql: String) {
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres")
                statement.execute("ATTACH '$uri' AS lake (DATA_PATH '$data', DUCKLAKE_VERSION '$format', AUTOMATIC_MIGRATION $migrate)")
                statement.execute(sql)
            }
        }
    }
}
