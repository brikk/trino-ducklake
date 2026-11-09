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

import dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer
import dev.brikk.ducklake.catalog.TestingDucklakeLocalDuckDbCatalogFixture
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import org.junit.jupiter.api.Assumptions
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Manages the shared catalog-backend fixtures used by Trino plugin tests. Holds
 * one lazily-created [TestingDucklakePostgreSqlCatalogServer] for the
 * default (PG) backend and one [TestingDucklakeDuckDbQuackCatalogServer]
 * for the Quack-backed DuckDB backend, with selection driven by
 * [DucklakeTestCatalogBackend.fromSystemProperty].
 *
 *
 * Backend selection is read once per JVM and pinned for the life of the
 * test process — flipping the system property mid-suite isn't supported.
 */
object DucklakeTestCatalogEnvironment {
    fun createLocalCatalog(database: Path, dataDirectory: Path): DucklakeConfig {
        Files.createDirectories(dataDirectory)
        createDuckdbConnection("ducklake:${database.toAbsolutePath()}", "$dataDirectory/").use { }
        return DucklakeConfig().setCatalogDatabaseUrl("jdbc:duckdb:${database.toAbsolutePath()}")
    }

    fun createPostgresCatalog(databaseName: String, dataPath: String): DucklakeConfig {
        val server = getServer()
        server.createDatabase(databaseName)
        createDuckdbConnection(server.getDuckDbAttachUri(databaseName), dataPath).use { }
        return DucklakeConfig().setCatalogDatabaseUrl(server.getJdbcUrl(databaseName))
            .setCatalogDatabaseUser(server.getUser()).setCatalogDatabasePassword(server.getPassword())
            .setDataPath(dataPath)
    }

    fun createDuckdbConnection(extensions: List<String> = emptyList()): Connection {
        val connection = DriverManager.getConnection("jdbc:duckdb:")
        try {
            connection.createStatement().use { statement ->
                for (extension in extensions) {
                    statement.execute("INSTALL $extension")
                    statement.execute("LOAD $extension")
                }
            }
            return connection
        }
        catch (failure: Throwable) {
            io.airlift.testing.Closeables.closeAllSuppress(failure, connection)
            throw failure
        }
    }

    fun createDuckdbConnection(attachUri: String, dataPath: String, alias: String = "lake"): Connection {
        val extensions = listOf("ducklake") + if (attachUri.startsWith("ducklake:postgres:")) listOf("postgres") else emptyList()
        val connection = createDuckdbConnection(extensions)
        try {
            attachDucklake(connection, attachUri, dataPath, alias)
            return connection
        }
        catch (failure: Throwable) {
            io.airlift.testing.Closeables.closeAllSuppress(failure, connection)
            throw failure
        }
    }

    fun attachDucklake(connection: Connection, attachUri: String, dataPath: String, alias: String = "lake") {
        connection.createStatement().use { statement ->
            statement.execute(ducklakeAttachSql(attachUri, dataPath, alias))
        }
    }

    /** Native fixtures must not silently migrate into a format the connector cannot write. */
    fun ducklakeAttachSql(attachUri: String, dataPath: String, alias: String = "lake", metadataCatalog: String? = null): String =
        "ATTACH '${attachUri.replace("'", "''")}' AS \"${alias.replace("\"", "\"\"")}\" " +
            "(DATA_PATH '${dataPath.replace("'", "''")}', DUCKLAKE_VERSION '1.0', AUTOMATIC_MIGRATION FALSE" +
            (metadataCatalog?.let { ", METADATA_CATALOG '${it.replace("'", "''")}'" } ?: "") + ")"

    fun connectorProperties(config: DucklakeConfig): Map<String, String> = buildMap {
        put("ducklake.catalog.database-url", requireNotNull(config.getCatalogDatabaseUrl()))
        put("ducklake.data-path", requireNotNull(config.getDataPath()))
        config.getCatalogDatabaseUser()?.let { put("ducklake.catalog.database-user", it) }
        config.getCatalogDatabasePassword()?.let { put("ducklake.catalog.database-password", it) }
    }

    private val LOCK = Any()

    @Volatile
    private var pgServer: TestingDucklakePostgreSqlCatalogServer? = null

    @Volatile
    private var quackServer: TestingDucklakeDuckDbQuackCatalogServer? = null

    @Volatile
    private var localDuckDbFixture: TestingDucklakeLocalDuckDbCatalogFixture? = null

    @Volatile
    private var serverUnavailable: RuntimeException? = null

    @Volatile
    private var pgCatalogGenerated: Boolean = false

    @JvmStatic
    fun selectedBackend(): DucklakeTestCatalogBackend {
        return DucklakeTestCatalogBackend.fromSystemProperty()
    }

    /**
     * Returns the PG server. Only valid when the selected backend is POSTGRES
     * — callers that need backend-pluggable behaviour should branch on
     * [selectedBackend] first.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun getServer(): TestingDucklakePostgreSqlCatalogServer {
        return ensurePostgresServer()
    }

    @JvmStatic
    @Throws(Exception::class)
    fun getQuackServer(): TestingDucklakeDuckDbQuackCatalogServer {
        return ensureQuackServer()
    }

    @JvmStatic
    fun getLocalDuckDbFixture(): TestingDucklakeLocalDuckDbCatalogFixture {
        return ensureLocalDuckDbFixture()
    }

    @JvmStatic
    @Throws(Exception::class)
    fun createDucklakeConfig(): DucklakeConfig {
        // Only the shared (non-isolated) PG catalog flow runs through here today;
        // it's the path used by tests that don't call useIsolatedCatalog and
        // expect the pre-bootstrapped 17-table fixture. The Quack backend doesn't
        // ship that bootstrap yet (see DucklakeCatalogGenerator) so any test
        // reaching this code path under DUCKDB_QUACK will need migration to the
        // isolated-catalog form.
        if (selectedBackend() != DucklakeTestCatalogBackend.POSTGRES) {
            throw IllegalStateException(
                "createDucklakeConfig (shared catalog) is only supported under POSTGRES backend; "
                        + "selected backend is " + selectedBackend()
                        + ". Use useIsolatedCatalog(...) on the query-runner builder instead.")
        }
        val server = ensurePostgresServer()
        ensureCatalogGenerated(server)

        return DucklakeConfig()
            .setMaxCatalogConnections(5)
            .setCatalogDatabaseUrl(server.getJdbcUrl())
            .setCatalogDatabaseUser(server.getUser())
            .setCatalogDatabasePassword(server.getPassword())
            .setDataPath(DucklakeCatalogGenerator.getPostgreSqlCatalogDirectory().resolve("data").toAbsolutePath().toString())
    }

    @JvmStatic
    @Throws(Exception::class)
    fun getConnectorProperties(): Map<String, String> {
        return connectorProperties(createDucklakeConfig())
    }

    @Throws(Exception::class)
    private fun ensurePostgresServer(): TestingDucklakePostgreSqlCatalogServer {
        val unavailable = serverUnavailable
        if (unavailable != null) {
            skipTests(unavailable)
        }

        var result = pgServer
        if (result == null) {
            synchronized(LOCK) {
                result = pgServer
                if (result == null) {
                    try {
                        result = TestingDucklakePostgreSqlCatalogServer()
                        pgServer = result
                        Runtime.getRuntime().addShutdownHook(Thread { result!!.close() })
                    }
                    catch (e: RuntimeException) {
                        if (isDockerUnavailable(e)) {
                            serverUnavailable = e
                            skipTests(e)
                        }
                        throw e
                    }
                }
            }
        }
        return result!!
    }

    @Throws(Exception::class)
    private fun ensureQuackServer(): TestingDucklakeDuckDbQuackCatalogServer {
        val unavailable = serverUnavailable
        if (unavailable != null) {
            skipTests(unavailable)
        }

        var result = quackServer
        if (result == null) {
            synchronized(LOCK) {
                result = quackServer
                if (result == null) {
                    try {
                        result = TestingDucklakeDuckDbQuackCatalogServer()
                        quackServer = result
                        Runtime.getRuntime().addShutdownHook(Thread { result!!.close() })
                    }
                    catch (e: RuntimeException) {
                        if (isDockerUnavailable(e)) {
                            serverUnavailable = e
                            skipTests(e)
                        }
                        throw e
                    }
                }
            }
        }
        return result!!
    }

    private fun ensureLocalDuckDbFixture(): TestingDucklakeLocalDuckDbCatalogFixture {
        var result = localDuckDbFixture
        if (result == null) {
            synchronized(LOCK) {
                result = localDuckDbFixture
                if (result == null) {
                    result = TestingDucklakeLocalDuckDbCatalogFixture()
                    localDuckDbFixture = result
                    Runtime.getRuntime().addShutdownHook(Thread { result!!.close() })
                }
            }
        }
        return result!!
    }

    @Throws(Exception::class)
    private fun ensureCatalogGenerated(server: TestingDucklakePostgreSqlCatalogServer) {
        if (!pgCatalogGenerated) {
            synchronized(LOCK) {
                if (!pgCatalogGenerated) {
                    DucklakeCatalogGenerator.generatePostgreSqlCatalog(server)
                    pgCatalogGenerated = true
                }
            }
        }
    }

    @JvmStatic
    internal fun isDockerUnavailable(throwable: Throwable): Boolean {
        var current: Throwable? = throwable
        while (current != null) {
            val message = current.message
            if (message != null && (message.contains("Could not find a valid Docker environment")
                        || message.contains("Previous attempts to find a Docker environment failed"))) {
                return true
            }
            current = current.cause
        }
        return false
    }

    private fun skipTests(cause: Throwable) {
        Assumptions.assumeTrue(
            false
        ) { "Ducklake tests require a working Docker environment: " + cause.message }
    }
}
