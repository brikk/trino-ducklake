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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.TableColumnSpec
import dev.brikk.ducklake.catalog.TableLocationSpec
import io.trino.filesystem.FileEntry
import io.trino.filesystem.FileIterator
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.local.LocalFileSystemFactory
import io.trino.filesystem.manager.FileSystemConfig
import io.trino.spi.security.ConnectorIdentity
import io.trino.testing.connector.TestingConnectorSession.SESSION
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.sql.DriverManager
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Optional

/** Real catalog and native-local filesystem with an isolated, non-/ root per test. */
class TestDucklakeOrphanFilePathIdentity {
    @TempDir
    lateinit var root: Path
    private lateinit var catalog: DucklakeCatalog
    private lateinit var config: DucklakeConfig
    private lateinit var fileSystem: TrinoFileSystem
    private lateinit var warehouse: Path
    private val tableLocation = Location.of("local:///warehouse/")

    @BeforeEach
    fun setUp() {
        warehouse = Files.createDirectories(root.resolve("warehouse"))
        val database = root.resolve("catalog.db")
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("INSTALL ducklake")
                statement.execute("LOAD ducklake")
                statement.execute("ATTACH 'ducklake:$database' AS lake (DATA_PATH '$warehouse/')")
            }
        }
        config = DucklakeConfig().setCatalogDatabaseUrl("jdbc:duckdb:$database")
        catalog = JdbcDucklakeCatalog(config.toCatalogConfig())
        catalog.createSchema("test_schema")
        catalog.createTable("test_schema", "t", listOf(TableColumnSpec.leaf("id", "int32", true)),
                null, TableLocationSpec(tableLocation.toString(), false))
        fileSystem = LocalFileSystemFactory(root).create(SESSION)
    }

    @AfterEach
    fun tearDown() {
        if (::catalog.isInitialized) catalog.close()
    }

    @Test
    fun nativeLocalAliasesProtectRegisteredFilesWithoutRewritingPaths() {
        val paths = listOf("file:/warehouse/ducklake-live%20name.parquet", "file:///warehouse/ducklake-long.parquet")
        val tableId = catalog.getTable("test_schema", "t", catalog.currentSnapshotId)!!.tableId
        for (path in paths) {
            val physical = warehouse.resolve(path.substringAfterLast('/'))
            DriverManager.getConnection("jdbc:duckdb:").use { connection ->
                connection.createStatement().use { it.execute("COPY (SELECT 42 AS id) TO '$physical' (FORMAT PARQUET)") }
            }
            age(physical)
            catalog.commitInsert(tableId, listOf(DucklakeWriteFragment(path, false, "parquet",
                    Files.size(physical), 0L, 1L, emptyList(), emptyMap(), null, null)))
        }
        val orphan = plantOrphan("ducklake-live name.parquet")
        val deleted = mutableListOf<Location>()
        val observing = object : TrinoFileSystem by fileSystem {
            override fun deleteFiles(locations: Collection<Location>) {
                deleted += locations
                fileSystem.deleteFiles(locations)
            }
        }
        procedure(observing).removeOrphanFiles(SESSION, "test_schema", "t", "7d", false)
        paths.forEach { assertThat(warehouse.resolve(it.substringAfterLast('/'))).exists() }
        assertThat(orphan).doesNotExist()
        assertThat(deleted).containsExactly(Location.of("local:///warehouse/ducklake-live name.parquet"))
        assertThat(catalog.getDataFiles(tableId, catalog.currentSnapshotId).map { it.path })
                .containsExactlyInAnyOrderElementsOf(paths)

        catalog.dropTable("test_schema", "t")
        catalog.createTable("test_schema", "t", listOf(TableColumnSpec.leaf("id", "int32", true)),
                null, TableLocationSpec(tableLocation.toString(), false))
        plantOrphan("ducklake-second-orphan.parquet")
        procedure(fileSystem).removeOrphanFiles(SESSION, "test_schema", "t", "7d", false)
        paths.forEach { assertThat(warehouse.resolve(it.substringAfterLast('/'))).exists() }
        assertThat(warehouse.resolve("ducklake-second-orphan.parquet")).doesNotExist()
    }

    @Test
    fun nativeLocalAliasesProtectScheduledFiles() {
        val scheduled = plantOrphan("ducklake-scheduled.parquet")
        val orphan = plantOrphan("ducklake-orphan.parquet")
        DriverManager.getConnection(config.getCatalogDatabaseUrl()).use { connection ->
            connection.prepareStatement("INSERT INTO ducklake_files_scheduled_for_deletion " +
                    "(data_file_id, path, path_is_relative, schedule_start) VALUES (?, ?, false, CURRENT_TIMESTAMP)").use { statement ->
                statement.setLong(1, 9_000_001L)
                statement.setString(2, "file:/warehouse/ducklake-scheduled.parquet")
                statement.executeUpdate()
            }
        }
        procedure(fileSystem).removeOrphanFiles(SESSION, "test_schema", "t", "7d", false)
        assertThat(scheduled).exists()
        assertThat(orphan).doesNotExist()
    }

    @Test
    fun keepsFileCreatedAfterEmptyDirectoryListing() {
        val partition = Files.createDirectories(warehouse.resolve("partition"))
        val partitionLocation = tableLocation.appendPath("partition")
        val orphan = plantOrphan("partition/ducklake-orphan-aged.parquet")
        val newFile = partition.resolve("ducklake-new.parquet")
        val content = byteArrayOf(9, 8, 7)
        val recursiveDeletes = mutableListOf<Location>()
        var emptyListing: FileIterator? = null
        val interleaved = object : TrinoFileSystem by fileSystem {
            override fun deleteFiles(locations: Collection<Location>) {
                assertThat(locations).containsExactly(Location.of("local:///warehouse/partition/ducklake-orphan-aged.parquet"))
                fileSystem.deleteFiles(locations)
                // Capture the empty listing, then interleave a writer before it is consumed.
                emptyListing = fileSystem.listFiles(partitionLocation)
                assertThat(checkNotNull(emptyListing).hasNext()).isFalse()
                Files.write(newFile, content)
            }

            override fun listFiles(location: Location): FileIterator =
                    if (location == partitionLocation && emptyListing != null) checkNotNull(emptyListing)
                    else fileSystem.listFiles(location)

            override fun deleteDirectory(location: Location) {
                recursiveDeletes += location
                fileSystem.deleteDirectory(location)
            }
        }
        procedure(interleaved).removeOrphanFiles(SESSION, "test_schema", "t", "7d", false)
        assertThat(orphan).doesNotExist()
        assertThat(newFile).exists()
        assertThat(Files.readAllBytes(newFile)).containsExactly(*content)
        assertThat(recursiveDeletes).isEmpty()
    }

    @Test
    fun invalidListingAfterAnOrphanAbortsBeforeDeletion() {
        val orphan = plantOrphan("ducklake-orphan.parquet")
        val entries = listOf(
                FileEntry(Location.of("local:///warehouse/ducklake-orphan.parquet"), Files.size(orphan),
                        Files.getLastModifiedTime(orphan).toInstant(), Optional.empty()),
                FileEntry(Location.of("file:///warehouse/../ducklake-ambiguous.parquet"), 1L,
                        Instant.now(), Optional.empty())).iterator()
        val invalidListing = object : TrinoFileSystem by fileSystem {
            override fun listFiles(location: Location): FileIterator = object : FileIterator {
                override fun hasNext(): Boolean = entries.hasNext()
                override fun next(): FileEntry = entries.next()
            }

            override fun deleteFiles(locations: Collection<Location>) {
                throw AssertionError("No deletion may occur after an invalid listing")
            }
        }
        assertThatThrownBy { procedure(invalidListing).removeOrphanFiles(SESSION, "test_schema", "t", "7d", false) }
                .hasMessageContaining("noncanonical local/Hadoop paths")
        assertThat(orphan).exists()
    }

    private fun procedure(delegate: TrinoFileSystem): DucklakeRemoveOrphanFilesProcedure {
        val factory = object : DucklakeFileSystemFactory {
            override fun create(identity: ConnectorIdentity): TrinoFileSystem = delegate
        }
        return DucklakeRemoveOrphanFilesProcedure(catalog, factory, DucklakePathResolver(catalog, config), config,
                DucklakeFilePathIdentity(FileSystemConfig().setLocalEnabled(true)))
    }

    private fun plantOrphan(relative: String): Path {
        val file = warehouse.resolve(relative)
        Files.write(file, byteArrayOf(1, 2, 3))
        age(file)
        return file
    }

    private fun age(file: Path) {
        Files.setLastModifiedTime(file, FileTime.from(Instant.now().minus(8, ChronoUnit.DAYS)))
    }
}
