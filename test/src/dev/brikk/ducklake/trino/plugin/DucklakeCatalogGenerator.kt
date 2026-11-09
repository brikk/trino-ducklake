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
import java.io.IOException
import java.io.UncheckedIOException
import java.nio.file.Files
import java.nio.file.Path

/**
 * Utility to generate a test Ducklake catalog using DuckDB's embedded JDBC driver.
 * Creates PostgreSQL-backed Ducklake catalog metadata and Parquet data files
 * by writing through DuckDB + Ducklake extension.
 */
object DucklakeCatalogGenerator {
    private val TARGET_DIR: Path = Path.of("build", "test-data")
    private val POSTGRESQL_CATALOG_DIR: Path = TARGET_DIR.resolve("test-catalog-postgresql")

    @JvmStatic
    fun getPostgreSqlCatalogDirectory(): Path = POSTGRESQL_CATALOG_DIR

    /**
     * Generate the shared PostgreSQL test catalog with all standard test tables and views.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun generatePostgreSqlCatalog(server: TestingDucklakePostgreSqlCatalogServer) {
        generateTestCatalog(
            "postgresql",
            POSTGRESQL_CATALOG_DIR,
            server.getDuckDbAttachUri(),
            listOf("postgres"),
            "catalog database: " + server.getJdbcUrl(),
        )
    }

    /**
     * Generate an isolated PostgreSQL test catalog in a unique database.
     * Each call creates a new database in the shared container, generates all test tables/views,
     * and returns the JDBC URL + data directory for the new catalog.
     * Use this for write tests that need their own catalog to avoid cross-test interference.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun generateIsolatedPostgreSqlCatalog(
        server: TestingDucklakePostgreSqlCatalogServer,
        testName: String,
    ): IsolatedCatalog {
        val databaseName = "ducklake_" + testName.replace('-', '_')
        server.createDatabase(databaseName)

        val catalogDir = TARGET_DIR.resolve("test-catalog-isolated-$testName")
        generateTestCatalog(
            "postgresql",
            catalogDir,
            server.getDuckDbAttachUri(databaseName),
            listOf("postgres"),
            "isolated catalog for $testName",
        )

        return IsolatedCatalog(
            server.getJdbcUrl(databaseName),
            server.getUser(),
            server.getPassword(),
            catalogDir.resolve("data"),
            server.getDuckDbAttachUri(databaseName),
        )
    }

    @JvmRecord
    data class IsolatedCatalog(
        val jdbcUrl: String,
        val user: String?,
        val password: String?,
        val dataDir: Path,
        val duckDbAttachUri: String,
    )

    /**
     * Generate an isolated local-DuckDB test catalog. Each call creates a fresh
     * `lake.db` file under the fixture's root and bootstraps the full
     * test-data fixture by ATTACHing it as a DuckLake catalog through an
     * in-process DuckDB JDBC connection. The connector then talks to the same
     * `.db` file directly via `jdbc:duckdb:/path/to/lake.db` —
     * no DuckLake-on-X wrapper at runtime, so `JdbcDucklakeCatalog`'s
     * UPDATE/DELETE on `ducklake_*` metadata rows hits regular base tables
     * and is unaffected by the read-only-sibling restriction that DuckLake-on-Quack
     * imposes today.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun generateIsolatedLocalDuckDbCatalog(
        fixture: TestingDucklakeLocalDuckDbCatalogFixture,
        testName: String,
    ): IsolatedCatalog {
        val catalogDir = fixture.catalogDirectory(testName)
        val catalogFile = catalogDir.resolve("lake.db")
        val attachUri = "ducklake:" + catalogFile.toAbsolutePath()

        generateTestCatalog(
            "duckdb-local",
            catalogDir,
            attachUri,
            listOf(),
            "isolated local DuckDB catalog for $testName",
        )

        return IsolatedCatalog(
            "jdbc:duckdb:" + catalogFile.toAbsolutePath(),
            /* user */ null,
            /* password */ null,
            catalogDir.resolve("data"),
            attachUri,
        )
    }

    /**
     * Generate an isolated Quack-backed test catalog. Each call picks a unique
     * METADATA_CATALOG name inside the shared container's remote DuckDB, so two
     * isolated catalogs in the same JVM don't see each other's metadata. The
     * returned `jdbcUrl` is the synthetic
     * `jdbc:duckdb:quack://host:port?metadata_catalog=name` URL that the
     * connector's `JdbcDucklakeCatalog` knows how to interpret.
     *
     * Unlike the PG path, this does not pre-create the 17-table test fixture
     * data. Tests that need pre-bootstrapped tables must create their own data under
     * `DUCKDB_QUACK`.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun generateIsolatedDuckDbQuackCatalog(
        server: TestingDucklakeDuckDbQuackCatalogServer,
        testName: String,
    ): IsolatedCatalog {
        val metadataCatalog = toQuackMetadataCatalogName(testName)
        val catalogDir = TARGET_DIR.resolve("test-catalog-quack-$testName")
        val dataDir = catalogDir.resolve("data")
        if (Files.exists(catalogDir)) {
            deleteDirectory(catalogDir)
        }
        Files.createDirectories(dataDir)

        // Pre-touch the catalog so its metadata schema exists before the connector's
        // JdbcDucklakeCatalog issues its first SELECT. Doing it here keeps the
        // connector-side init script idempotent and front-loads any extension-
        // download cost into test setup (visible) rather than the first query.
        DucklakeTestCatalogEnvironment.createDuckdbConnection(listOf("quack", "ducklake")).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE OR REPLACE SECRET (TYPE quack, TOKEN '" + escapeSql(server.getToken()) + "')")
                stmt.execute(DucklakeTestCatalogEnvironment.ducklakeAttachSql(
                    server.getDucklakeAttachUri(), dataDir.toAbsolutePath().toString(), metadataCatalog = metadataCatalog))
                stmt.execute("USE lake")
                stmt.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
            }
        }

        val syntheticUrl = "jdbc:duckdb:quack://" + server.getHost() + ":" + server.getMappedPort() +
            "?metadata_catalog=" + metadataCatalog
        return IsolatedCatalog(
            syntheticUrl,
            /* user */ null,
            /* password */ server.getToken(),
            dataDir,
            server.getDucklakeAttachUri(),
        )
    }

    private fun toQuackMetadataCatalogName(testName: String): String {
        // METADATA_CATALOG must match [A-Za-z0-9_]+ — QuackBackedDuckDbCatalogUrl
        // validates this on the parsing side. Normalise the test name to fit.
        val normalised = testName.replace(Regex("[^A-Za-z0-9_]"), "_")
        return "qmeta_$normalised"
    }

    @JvmStatic
    @Throws(Exception::class)
    internal fun generateTestCatalog(
        backendName: String,
        catalogDirectory: Path,
        ducklakeAttachUri: String,
        extraExtensions: List<String>,
        catalogDescription: String,
    ) {
        println("==========================================")
        println("Ducklake Test Catalog Generator")
        println("==========================================")
        println("Backend: $backendName")
        println()

        Files.createDirectories(TARGET_DIR)

        if (Files.exists(catalogDirectory)) {
            println("Removing existing test catalog data...")
            deleteDirectory(catalogDirectory)
        }

        println("Creating DuckLake 1.0 test catalog with DuckDB + Ducklake extension...")
        println()

        Files.createDirectories(catalogDirectory)
        val dataDir = catalogDirectory.resolve("data")
        Files.createDirectories(dataDir)

        DucklakeTestCatalogEnvironment.createDuckdbConnection(listOf("ducklake") + extraExtensions).use { conn ->
            conn.createStatement().use { stmt ->
                println("Attaching Ducklake catalog...")
                val attachSql = DucklakeTestCatalogEnvironment.ducklakeAttachSql(
                    ducklakeAttachUri, dataDir.toAbsolutePath().toString(), "ducklake_db")
                stmt.execute(attachSql)

                println("Creating test schema...")
                stmt.execute("CREATE SCHEMA ducklake_db.test_schema")

                DucklakeTestCatalogTables.createPrimitiveTables(stmt)
                DucklakeTestCatalogTables.createIdentityPartitionedTable(stmt)
                DucklakeTestCatalogTables.createTemporalPartitionedTable(stmt)
                DucklakeTestCatalogTables.createDailyPartitionedTable(stmt)
                DucklakeTestCatalogTables.createTimestampPartitionedTable(stmt)
                DucklakeTestCatalogTables.createNestedTable(stmt)
                DucklakeTestCatalogTables.createWideTypesTable(stmt)
                DucklakeTestCatalogTables.createNullableAndEmptyTables(stmt)
                DucklakeTestCatalogTables.createSchemaEvolutionTable(stmt)
                DucklakeTestCatalogTables.createAggregationTable(stmt)
                DucklakeTestCatalogTables.createDeletedRowsTable(stmt)
                DucklakeTestCatalogTables.createComplexNullsTable(stmt)
                DucklakeTestCatalogTables.createMultiFileTable(stmt)

                println("Forcing checkpoint to write Parquet files...")
                stmt.execute("CHECKPOINT ducklake_db")

                println("Detaching Ducklake catalog...")
                stmt.execute("DETACH ducklake_db")

                // Re-attach to create inlined tables AFTER checkpoint/detach.
                println("Re-attaching catalog for inlined table creation...")
                stmt.execute(attachSql)

                DucklakeTestCatalogTables.createInlinedTables(stmt)
                DucklakeTestCatalogTables.createMixedInlineTable(stmt)
                DucklakeTestCatalogTables.createViews(stmt)

                // Detach WITHOUT checkpoint — inlined data stays in metadata catalog
                println("Detaching catalog (no checkpoint — inlined data preserved)...")
                stmt.execute("DETACH ducklake_db")

                println()
                println("Test catalog created successfully!")
                println()
                println("Catalog location: " + catalogDirectory.toAbsolutePath())
                println(catalogDescription)
                println("Data directory: " + dataDir.toAbsolutePath())
            }
        }
    }

    @Throws(Exception::class)
    private fun deleteDirectory(directory: Path) {
        if (Files.exists(directory)) {
            Files.walk(directory)
                .sorted { a, b -> -a.compareTo(b) }
                .forEach { path ->
                    try {
                        Files.delete(path)
                    } catch (e: IOException) {
                        throw UncheckedIOException("Failed to delete: $path", e)
                    }
                }
        }
    }

    private fun escapeSql(value: String): String = value.replace("'", "''")
}
