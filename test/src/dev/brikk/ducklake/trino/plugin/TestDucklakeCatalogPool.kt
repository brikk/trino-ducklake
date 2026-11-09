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
import io.airlift.configuration.ConfigurationFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.sql.DriverManager
import java.util.UUID

class TestDucklakeCatalogPool {
    @TempDir
    lateinit var data: Path

    @Test
    fun boundPoolSettingsAndJdbcUrlParametersReachTheMetadataDriver() {
        val server = DucklakeTestCatalogEnvironment.getServer()
        val database = "pool_" + UUID.randomUUID().toString().replace("-", "")
        DucklakeTestCatalogEnvironment.createPostgresCatalog(database, "$data/")
        val baseUrl = server.getJdbcUrl(database)
        val application = "trino-pool-" + UUID.randomUUID().toString()
        val url = baseUrl + (if (baseUrl.contains('?')) "&" else "?") + "prepareThreshold=0&ApplicationName=$application"
        ConfigurationFactory(mapOf("ducklake.catalog.database-url" to url,
            "ducklake.catalog.database-user" to server.getUser(), "ducklake.catalog.database-password" to server.getPassword(),
            "ducklake.data-path" to data.toString(), "ducklake.catalog.max-connections" to "2",
            "ducklake.catalog.min-idle-connections" to "1", "ducklake.catalog.connection-idle-timeout" to "10s",
            "ducklake.catalog.connection-max-lifetime" to "60s", "ducklake.catalog.connection-timeout" to "2s")).use { factory ->
                val config = factory.build(DucklakeConfig::class.java).toCatalogConfig()
                assertThat(config.catalogDatabaseUrl).isEqualTo(url)
                JdbcDucklakeCatalog(config).use { catalog ->
                    repeat(8) {
                        catalog.readSession {
                            assertThat(catalog.listSchemas(catalog.currentSnapshotId).map { it.schemaName }).contains("main")
                        }
                    }
                    assertThat(applicationConnectionCount(baseUrl, server.getUser(), server.getPassword(), database, application))
                        .isGreaterThanOrEqualTo(1)
                }
            }
    }

    private fun applicationConnectionCount(url: String, user: String, password: String, database: String, application: String): Long =
        DriverManager.getConnection(url, user, password).use { connection ->
            connection.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE datname = ? AND application_name = ?").use { query ->
                query.setString(1, database)
                query.setString(2, application)
                query.executeQuery().use { result ->
                    check(result.next())
                    result.getLong(1)
                }
            }
        }
}
