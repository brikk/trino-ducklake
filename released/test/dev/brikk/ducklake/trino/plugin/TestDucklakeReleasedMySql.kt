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

import dev.brikk.ducklake.catalog.TestingDucklakeMySqlCatalogServer
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.sql.DriverManager

/** Real Connector/J read/write coverage; ongoing native MySQL interoperability is a separate probe. */
class TestDucklakeReleasedMySql {
    @TempDir
    lateinit var data: Path

    @Test
    fun workersReadAndWriteTheReleasedCatalogOnMySql() {
        TestingDucklakeMySqlCatalogServer().use { server ->
            DriverManager.getConnection("jdbc:duckdb:").use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("INSTALL ducklake; LOAD ducklake; INSTALL mysql; LOAD mysql")
                    statement.execute("ATTACH '${server.getDuckDbAttachUri()}' AS lake " +
                        "(DATA_PATH '$data', DUCKLAKE_VERSION '1.0', AUTOMATIC_MIGRATION FALSE)")
                }
            }
            val session = testSessionBuilder().setCatalog("lake").setSchema("main").build()
            DistributedQueryRunner.builder(session).setWorkerCount(2)
                .addCoordinatorProperty("node-scheduler.include-coordinator", "false").build().use { runner ->
                    runner.installPlugin(DucklakePlugin())
                    runner.createCatalog("lake", "ducklake", mapOf(
                        "ducklake.catalog.database-url" to server.getJdbcUrl(),
                        "ducklake.catalog.database-user" to server.getUser(),
                        "ducklake.catalog.database-password" to server.getPassword(),
                        "ducklake.data-path" to data.toString(), "fs.hadoop.enabled" to "true"))
                    runner.execute("CREATE TABLE t AS SELECT 1 AS i, 'native' AS label")
                    runner.execute("INSERT INTO t VALUES (2, 'trino')")
                    runner.execute("UPDATE t SET label = 'updated' WHERE i = 1")
                    runner.execute("DELETE FROM t WHERE i = 2")
                    assertThat(runner.execute("SELECT i, label FROM t").materializedRows.single().fields)
                        .containsExactly(1, "updated")
                    runner.execute("ANALYZE t")
                }
        }
    }
}
