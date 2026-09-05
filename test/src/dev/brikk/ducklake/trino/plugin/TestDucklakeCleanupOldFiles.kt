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
import dev.brikk.ducklake.catalog.DucklakeFileKind
import dev.brikk.ducklake.catalog.DucklakeFileKind.DATA
import dev.brikk.ducklake.catalog.DucklakeFileKind.DELETE
import dev.brikk.ducklake.catalog.DucklakeReferencedFiles
import dev.brikk.ducklake.catalog.DucklakeRetainedFileReference
import dev.brikk.ducklake.catalog.DucklakeScheduledFile
import dev.brikk.ducklake.catalog.DucklakeScheduledFileEntry
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.TableColumnSpec
import dev.brikk.ducklake.catalog.TableLocationSpec
import io.airlift.units.Duration
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.local.LocalFileSystemFactory
import io.trino.filesystem.manager.FileSystemConfig
import io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.security.ConnectorIdentity
import io.trino.testing.connector.TestingConnectorSession.SESSION
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.time.Instant
import java.time.OffsetDateTime
import java.util.function.Supplier

/** Real metadata and native-local storage, isolated under a non-/ root for every test. */
class TestDucklakeCleanupOldFiles {
    @TempDir
    lateinit var root: Path
    private lateinit var warehouse: Path
    private lateinit var config: DucklakeConfig
    private lateinit var rawCatalog: DucklakeCatalog
    private lateinit var catalog: DucklakeCatalog
    private lateinit var fileSystem: TrinoFileSystem
    private var tableId = 0L
    private var inReadSession = false
    private var readSessions = 0
    private val pageCalls = mutableListOf<PageCall>()
    private val batchLimits = mutableListOf<Int>()
    private val removals = mutableListOf<List<DucklakeScheduledFileEntry>>()
    private val deletions = mutableListOf<Location>()
    private var failDeleteAt: Location? = null
    private var beforeRemove: ((Collection<DucklakeScheduledFileEntry>) -> Unit)? = null
    private val oldTime = OffsetDateTime.parse("2020-01-01T00:00:00.123456Z")

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
                .setMaintenanceMinRetention(Duration.valueOf("0s"))
                .setCleanupBatchSize(2)
                .setCleanupMaxReferenceFiles(32)
        // Bootstrap with a physical path, then use the native filesystem's rooted namespace.
        sql("UPDATE ducklake_metadata SET value = 'local:///warehouse/' WHERE key = 'data_path'")
        rawCatalog = JdbcDucklakeCatalog(config.toCatalogConfig())
        rawCatalog.createSchema("test_schema")
        tableId = createTable("t")
        catalog = object : DucklakeCatalog by rawCatalog {
            override fun getDataPath(): String? = throw AssertionError("cleanup must bound its root-path fetch")

            override fun getCleanupDataPath(): String? {
                assertThat(inReadSession).isTrue()
                return rawCatalog.getCleanupDataPath()
            }

            override fun listAllReferencedFiles(): DucklakeReferencedFiles =
                    throw AssertionError("cleanup must not materialize all references")

            override fun listFilesScheduledForDeletion(olderThan: Instant): List<DucklakeScheduledFile> =
                    throw AssertionError("cleanup must select a bounded batch")

            override fun removeScheduledFileRows(dataFileIds: Collection<Long>) {
                throw AssertionError("cleanup must acknowledge exact queue tuples, not IDs")
            }

            override fun <T> readSession(action: Supplier<T>): T = rawCatalog.readSession {
                assertThat(inReadSession).isFalse()
                inReadSession = true
                readSessions++
                try {
                    action.get()
                }
                finally {
                    inReadSession = false
                }
            }

            override fun listScheduledFileBatch(olderThan: Instant, limit: Int): List<DucklakeScheduledFileEntry> {
                assertThat(inReadSession).isTrue()
                batchLimits += limit
                assertThat(limit).isEqualTo(2)
                return rawCatalog.listScheduledFileBatch(olderThan, limit)
            }

            override fun listRetainedFileReferencePage(
                    kind: DucklakeFileKind, afterFileId: Long?, limit: Int): List<DucklakeRetainedFileReference> {
                assertThat(inReadSession).isTrue()
                assertThat(limit).isBetween(1, 2)
                pageCalls += PageCall(kind, afterFileId, limit)
                return rawCatalog.listRetainedFileReferencePage(kind, afterFileId, limit)
            }

            override fun removeSelectedScheduledFiles(entries: Collection<DucklakeScheduledFileEntry>) {
                assertThat(inReadSession).isFalse()
                removals += entries.toList()
                if (entries.isNotEmpty()) {
                    val hook = beforeRemove
                    beforeRemove = null
                    hook?.invoke(entries)
                }
                rawCatalog.removeSelectedScheduledFiles(entries)
            }
        }
        val native = LocalFileSystemFactory(root).create(SESSION)
        fileSystem = object : TrinoFileSystem by native {
            override fun deleteFile(location: Location) {
                assertThat(inReadSession).isFalse()
                deletions += location
                if (location == failDeleteAt) throw IOException("injected delete failure")
                native.deleteFile(location)
            }

            override fun deleteDirectory(location: Location) {
                assertThat(inReadSession).isFalse()
                deletions += location
                native.deleteDirectory(location)
            }
        }
    }

    @AfterEach
    fun tearDown() {
        if (::rawCatalog.isInitialized) rawCatalog.close()
    }

    @Test
    fun lastReferencePageProtectsSharedFileAndCancelsObsoleteRequest() {
        val source = createTable("source")
        val sourceFile = ownParquet("shared.parquet", owner = source)
        ownParquet("filler-1.parquet")
        val second = ownParquet("filler-2.parquet")
        ownParquet("shared.parquet")
        rawCatalog.dropTable("test_schema", "source")
        rawCatalog.expireSnapshots(rawCatalog.listExpirableSnapshots(null, null).toSet())
        val obsolete = queued().single()
        assertThat(obsolete.dataFileId).isEqualTo(sourceFile)
        val orphan = plant("orphan.parquet")
        schedule(9001, "orphan.parquet", relative = true)
        val before = queued()

        cleanup()

        assertThat(warehouse.resolve("shared.parquet")).isRegularFile()
        assertThat(orphan).doesNotExist()
        assertThat(queued()).isEmpty()
        assertThat(removals.filter { it.isNotEmpty() }).containsExactly(listOf(obsolete), before.filter { it != obsolete })
        assertThat(deletions).containsExactly(Location.of("local:///warehouse/orphan.parquet"))
        assertThat(pageCalls).containsExactly(PageCall(DATA, null, 2), PageCall(DATA, second, 2), PageCall(DELETE, null, 2))
        assertThat(readSessions).isEqualTo(1)
    }

    @Test
    fun droppedButUnexpiredTableStillProtectsItsRetiredFile() {
        ownParquet("filler-1.parquet")
        val second = ownParquet("filler-2.parquet")
        val retained = ownParquet("retained.parquet")
        val snapshot = rawCatalog.currentSnapshotId
        rawCatalog.dropTable("test_schema", "t")
        assertThat(rawCatalog.getDataFiles(tableId, rawCatalog.currentSnapshotId)).isEmpty()
        assertThat(rawCatalog.getDataFiles(tableId, snapshot).map { it.dataFileId }).contains(retained)
        schedule(9001, "retained.parquet", relative = true)
        val request = queued().single()

        cleanup()

        assertThat(warehouse.resolve("retained.parquet")).isRegularFile()
        assertThat(queued()).isEmpty()
        assertThat(removals.flatten()).containsExactly(request)
        assertThat(deletions).isEmpty()
        assertThat(pageCalls).containsExactly(PageCall(DATA, null, 2), PageCall(DATA, second, 2), PageCall(DELETE, null, 2))
    }

    @Test
    fun deleteReferencesUsePagesAndShareTheDataReferenceBudget() {
        val dataIds = (1..3).map { ownParquet("data-$it.parquet") }
        for (index in 1..3) {
            val file = parquet("delete-$index.parquet", "SELECT 'data-$index.parquet' AS file_path, 0::BIGINT AS pos")
            sql("INSERT INTO ducklake_delete_file " +
                    "(delete_file_id, table_id, begin_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size) " +
                    "VALUES (?, ?, ?, ?, ?, false, 'parquet', 1, ?, 0)",
                    100L + index, tableId, rawCatalog.currentSnapshotId, dataIds[index - 1],
                    "file:/warehouse/delete-$index.parquet", Files.size(file))
        }
        rawCatalog.dropTable("test_schema", "t")
        schedule(9001, "delete-3.parquet", relative = true)
        val orphan = plant("orphan.parquet")
        schedule(9002, "orphan.parquet", relative = true)
        val before = queued()
        config.setCleanupMaxReferenceFiles(5)

        assertThatThrownBy { cleanup() }.isInstanceOf(TrinoException::class.java)
                .hasMessageContaining("max-reference-files=5")
        assertThat(deletions).isEmpty()
        assertThat(removals).isEmpty()
        assertThat(queued()).containsExactlyElementsOf(before)
        assertThat(orphan).exists()
        assertThat(pageCalls).containsExactly(PageCall(DATA, null, 2), PageCall(DATA, dataIds[1], 2),
                PageCall(DELETE, null, 2), PageCall(DELETE, 102, 1))

        pageCalls.clear()
        config.setCleanupMaxReferenceFiles(6)
        cleanup()

        assertThat(warehouse.resolve("delete-3.parquet")).isRegularFile()
        assertThat(orphan).doesNotExist()
        assertThat(queued()).isEmpty()
        assertThat(pageCalls).containsExactly(PageCall(DATA, null, 2), PageCall(DATA, dataIds[1], 2),
                PageCall(DELETE, null, 2), PageCall(DELETE, 102, 2))
    }

    @Test
    fun oversizedRootAbortsEvenWhenAllSelectedPathsAreAbsolute() {
        val orphan = plant("orphan.parquet")
        schedule(9001, "local:///warehouse/orphan.parquet")
        val before = queued()
        sql("UPDATE ducklake_metadata SET value = ? WHERE key = 'data_path'", "x".repeat(16385))

        assertThatThrownBy { cleanup() }.hasMessageContaining("Cleanup path exceeds")

        assertThat(orphan).exists()
        assertThat(queued()).containsExactlyElementsOf(before)
        assertThat(removals).isEmpty()
        assertThat(deletions).isEmpty()
    }

    @Test
    fun whitespaceScheduledFilenameDoesNotBecomeTheWarehouseDirectory() {
        val selected = plant(" ")
        val younger = plant("younger.parquet")
        schedule(9001, " ", relative = true)
        schedule(9002, "younger.parquet", relative = true, time = OffsetDateTime.now().plusDays(1))
        val futureRequest = queued().last()

        cleanup()

        assertThat(selected).doesNotExist()
        assertThat(younger).exists()
        assertThat(warehouse).isDirectory()
        assertThat(queued()).containsExactly(futureRequest)
        assertThat(deletions).containsExactly(Location.of("local:///warehouse/ "))
    }

    @Test
    fun whitespaceRetainedFilenameIsAnExactOwnerNotADirectory() {
        ownParquet("test_schema/t/ ", storedPath = " ", relative = true)
        schedule(9001, "test_schema/t/ ", relative = true)

        cleanup()

        assertThat(warehouse.resolve("test_schema/t/ ")).isRegularFile()
        assertThat(queued()).isEmpty()
        assertThat(deletions).isEmpty()
    }

    @Test
    fun eachCallProcessesOnlyOneBatchAndRepeatedCallsDrainTheQueue() {
        val files = (1..5).map { index ->
            plant("orphan-$index.parquet").also {
                schedule(9000L + index, "orphan-$index.parquet", relative = true)
            }
        }
        val initial = queued()
        for (remaining in listOf(3, 1, 0)) {
            val previouslyDeleted = deletions.size
            cleanup()
            assertThat(deletions.size - previouslyDeleted).isBetween(1, 2)
            assertThat(queued()).containsExactlyElementsOf(initial.takeLast(remaining))
            files.take(5 - remaining).forEach { assertThat(it).doesNotExist() }
            files.takeLast(remaining).forEach { assertThat(it).exists() }
        }
        cleanup()
        assertThat(deletions).hasSize(5)
        assertThat(batchLimits).containsExactly(2, 2, 2, 2)
        assertThat(readSessions).isEqualTo(4)
        assertThat(removals.flatten()).containsExactlyElementsOf(initial)
        assertThat(pageCalls).containsExactlyElementsOf(List(3) { listOf(PageCall(DATA, null, 2), PageCall(DELETE, null, 2)) }.flatten())
    }

    @Test
    fun budgetFailureAfterFindingAnOwnerDoesNotDeleteOrCancelAnything() {
        ownParquet("owned.parquet")
        val second = ownParquet("filler-1.parquet")
        ownParquet("filler-2.parquet")
        val orphan = plant("orphan.parquet")
        schedule(9001, "owned.parquet", relative = true)
        schedule(9002, "orphan.parquet", relative = true)
        val before = queued()
        config.setCleanupMaxReferenceFiles(2)

        assertThatThrownBy { cleanup() }.isInstanceOfSatisfying(TrinoException::class.java) { failure ->
            assertThat(failure.errorCode).isEqualTo(GENERIC_USER_ERROR.toErrorCode())
            assertThat(failure.message).contains("max-reference-files=2", "no files or queue entries were changed")
        }

        assertThat(pageCalls).containsExactly(PageCall(DATA, null, 2), PageCall(DATA, second, 1))
        assertThat(warehouse.resolve("owned.parquet")).isRegularFile()
        assertThat(orphan).exists()
        assertThat(queued()).containsExactlyElementsOf(before)
        assertThat(deletions).isEmpty()
        assertThat(removals).isEmpty()
    }

    @Test
    fun exactReferenceBudgetSucceedsAfterEmptyLookaheadInBothKinds() {
        ownParquet("filler.parquet")
        val last = ownParquet("owned.parquet")
        val orphan = plant("orphan.parquet")
        schedule(9001, "owned.parquet", relative = true)
        schedule(9002, "orphan.parquet", relative = true)
        config.setCleanupMaxReferenceFiles(2)

        cleanup()

        assertThat(pageCalls).containsExactly(PageCall(DATA, null, 2), PageCall(DATA, last, 1), PageCall(DELETE, null, 1))
        assertThat(warehouse.resolve("owned.parquet")).isRegularFile()
        assertThat(orphan).doesNotExist()
        assertThat(queued()).isEmpty()
    }

    @Test
    fun dryRunScansAllOwnersAndEnforcesBudgetWithoutMutating() {
        ownParquet("filler-1.parquet")
        val second = ownParquet("filler-2.parquet")
        ownParquet("owned.parquet")
        val orphan = plant("orphan.parquet")
        schedule(9001, "owned.parquet", relative = true)
        schedule(9002, "orphan.parquet", relative = true)
        val before = queued()
        config.setCleanupMaxReferenceFiles(3)

        cleanup(dryRun = true)

        assertThat(pageCalls).containsExactly(PageCall(DATA, null, 2), PageCall(DATA, second, 2), PageCall(DELETE, null, 1))
        assertThat(queued()).containsExactlyElementsOf(before)
        assertThat(orphan).exists()
        assertThat(warehouse.resolve("owned.parquet")).isRegularFile()
        assertThat(deletions).isEmpty()
        assertThat(removals).isEmpty()

        config.setCleanupMaxReferenceFiles(2)
        assertThatThrownBy { cleanup(dryRun = true) }.hasMessageContaining("max-reference-files=2")
        assertThat(queued()).containsExactlyElementsOf(before)
        assertThat(orphan).exists()
        assertThat(deletions).isEmpty()
        assertThat(removals).isEmpty()
    }

    @Test
    fun cancellationPreservesUnselectedSameIdPathsAndTimestamps() {
        ownParquet("owned.parquet")
        val other = plant("other.parquet")
        val orphan = plant("orphan.parquet")
        schedule(9001, "owned.parquet", relative = true)
        schedule(9002, "orphan.parquet", relative = true)
        val before = queued()
        val selected = before.first()
        // A metadata interleaving at acknowledgment, not a claim of concurrent ownership locking.
        beforeRemove = { entries ->
            assertThat(entries).containsExactly(selected)
            assertThat(deletions).isEmpty()
            schedule(selected.dataFileId, "other.parquet", relative = true, time = selected.scheduleStart)
            schedule(selected.dataFileId, selected.path, relative = true, time = selected.scheduleStart.plusSeconds(1))
        }

        cleanup()

        assertThat(queued()).containsExactly(selected.copy(path = "other.parquet"),
                selected.copy(scheduleStart = selected.scheduleStart.plusSeconds(1)))
        assertThat(removals.flatten()).containsExactlyElementsOf(before)
        assertThat(warehouse.resolve("owned.parquet")).isRegularFile()
        assertThat(other).exists()
        assertThat(orphan).doesNotExist()
    }

    @Test
    fun failedDeleteKeepsItsExactRequestWhileAnotherSelectedDeleteSucceeds() {
        val failed = plant("failed.parquet")
        val successful = plant("successful.parquet")
        // The same ID also exercises exact acknowledgment on the successful deletion path.
        schedule(9001, "failed.parquet", relative = true)
        schedule(9001, "successful.parquet", relative = true)
        val before = queued()
        failDeleteAt = Location.of("local:///warehouse/failed.parquet")

        cleanup()

        assertThat(failed).exists()
        assertThat(successful).doesNotExist()
        assertThat(queued()).containsExactly(before.first())
        assertThat(removals.flatten()).containsExactly(before.last())
        assertThat(deletions).containsExactly(failDeleteAt, Location.of("local:///warehouse/successful.parquet"))
    }

    @Test
    fun nativeAliasesAreLiteralAndScheduledPathsAreCatalogRootRelative() {
        ownParquet("live%20name.parquet", storedPath = "file:/warehouse/live%20name.parquet")
        ownParquet("test_schema/t/nested.parquet", storedPath = "nested.parquet", relative = true)
        val literalSpace = plant("live name.parquet")
        val rootOrphan = plant("root-only.parquet")
        val tableLookalike = plant("test_schema/t/root-only.parquet")
        schedule(9001, "file:///warehouse/live%20name.parquet")
        schedule(9002, "test_schema/t/nested.parquet", relative = true)

        cleanup()

        assertThat(queued()).isEmpty()
        assertThat(deletions).isEmpty()
        assertThat(warehouse.resolve("live%20name.parquet")).isRegularFile()
        assertThat(warehouse.resolve("test_schema/t/nested.parquet")).isRegularFile()
        schedule(9003, "live name.parquet", relative = true)
        schedule(9004, "root-only.parquet", relative = true)

        cleanup()

        assertThat(literalSpace).doesNotExist()
        assertThat(rootOrphan).doesNotExist()
        assertThat(tableLookalike).exists()
        assertThat(queued()).isEmpty()
        assertThat(deletions).containsExactly(Location.of("local:///warehouse/live name.parquet"),
                Location.of("local:///warehouse/root-only.parquet"))
    }

    @Test
    fun ambiguousLastReferenceAbortsBeforeCancellationOrDeletion() {
        ownParquet("owned.parquet")
        val second = ownParquet("filler.parquet")
        ownParquet("ambiguous.parquet", storedPath = "file:///warehouse/../ambiguous.parquet")
        val orphan = plant("orphan.parquet")
        schedule(9001, "owned.parquet", relative = true)
        schedule(9002, "orphan.parquet", relative = true)
        val before = queued()

        assertThatThrownBy { cleanup() }.isInstanceOf(TrinoException::class.java)
                .hasMessageContaining("noncanonical local/Hadoop paths")

        assertThat(pageCalls).containsExactly(PageCall(DATA, null, 2), PageCall(DATA, second, 2))
        assertThat(queued()).containsExactlyElementsOf(before)
        assertThat(warehouse.resolve("owned.parquet")).exists()
        assertThat(orphan).exists()
        assertThat(removals).isEmpty()
        assertThat(deletions).isEmpty()
    }

    @Test
    fun ambiguousScheduledPathAbortsBeforeAnyMutation() {
        val orphan = plant("orphan.parquet")
        schedule(9001, "orphan.parquet", relative = true)
        schedule(9002, "file:///warehouse/../ambiguous.parquet")
        val before = queued()

        assertThatThrownBy { cleanup() }.isInstanceOf(TrinoException::class.java)
                .hasMessageContaining("noncanonical local/Hadoop paths")

        assertThat(orphan).exists()
        assertThat(queued()).containsExactlyElementsOf(before)
        assertThat(pageCalls).isEmpty()
        assertThat(removals).isEmpty()
        assertThat(deletions).isEmpty()
    }

    @Test
    fun exactDirectoryOwnersCancelRequestsWithOrWithoutTrailingSlash() {
        val first = plant("first.lance/data/part.lance")
        val second = plant("second.lance/data/part.lance")
        seedLanceReference(101, "file:/warehouse/first.lance/")
        seedLanceReference(102, "file:///warehouse/second.lance")
        schedule(9001, "first.lance", relative = true)
        schedule(9002, "second.lance/", relative = true)
        val before = queued()

        cleanup()

        assertThat(first).exists()
        assertThat(second).exists()
        assertThat(warehouse.resolve("first.lance")).isDirectory()
        assertThat(warehouse.resolve("second.lance")).isDirectory()
        assertThat(queued()).isEmpty()
        assertThat(removals.flatten()).containsExactlyElementsOf(before)
        assertThat(deletions).isEmpty()
    }

    @Test
    fun prefixOnlyParentAndChildOverlapsRemainScheduledRatherThanCancelled() {
        val child = plant("parent/child.lance/data/part.lance")
        val descendant = plant("owned.lance/data/part.lance")
        seedLanceReference(101, "local:///warehouse/parent/child.lance/")
        seedLanceReference(102, "local:///warehouse/owned.lance/")
        schedule(9001, "parent", relative = true)
        schedule(9002, "owned.lance/data/part.lance", relative = true)
        val before = queued()

        repeat(2) { cleanup() }

        assertThat(child).exists()
        assertThat(descendant).exists()
        // Retiring a child cannot later requeue/reclaim the whole parent directory.
        assertThat(queued()).containsExactlyElementsOf(before)
        assertThat(removals.flatten()).isEmpty()
        assertThat(deletions).isEmpty()
    }

    @Test
    fun similarlyNamedSiblingDirectoryIsNotAnOverlapAndCanBeDeleted() {
        val owned = plant("dataset.lance/data/part.lance")
        val sibling = plant("dataset.lance-backup/data/part.lance")
        seedLanceReference(101, "local:///warehouse/dataset.lance")
        schedule(9001, "dataset.lance-backup", relative = true)

        cleanup()

        assertThat(owned).exists()
        assertThat(sibling).doesNotExist()
        assertThat(warehouse.resolve("dataset.lance-backup")).doesNotExist()
        assertThat(queued()).isEmpty()
        assertThat(deletions).containsExactly(Location.of("local:///warehouse/dataset.lance-backup"))
    }

    private fun cleanup(dryRun: Boolean = false) {
        val factory = object : DucklakeFileSystemFactory {
            override fun create(identity: ConnectorIdentity): TrinoFileSystem = fileSystem
        }
        DucklakeCleanupOldFilesProcedure(catalog, factory, config,
                DucklakeFilePathIdentity(FileSystemConfig().setLocalEnabled(true)))
                .cleanupOldFiles(SESSION, "0s", dryRun)
    }

    private fun createTable(name: String): Long {
        rawCatalog.createTable("test_schema", name, listOf(TableColumnSpec.leaf("id", "int32", true)),
                null, TableLocationSpec("$name/", true))
        return rawCatalog.getTable("test_schema", name, rawCatalog.currentSnapshotId)!!.tableId
    }

    private fun ownParquet(
            name: String, storedPath: String = "local:///warehouse/$name", relative: Boolean = false, owner: Long = tableId): Long {
        val file = warehouse.resolve(name)
        if (!Files.exists(file)) parquet(name)
        rawCatalog.commitInsert(owner, listOf(DucklakeWriteFragment(storedPath, relative, "parquet",
                Files.size(file), 0L, 1L, emptyList(), emptyMap(), null, null)))
        return rawCatalog.getDataFiles(owner, rawCatalog.currentSnapshotId).single { it.path == storedPath }.dataFileId
    }

    private fun parquet(name: String, query: String = "SELECT 42 AS id"): Path {
        val file = warehouse.resolve(name)
        Files.createDirectories(file.parent)
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { it.execute("COPY ($query) TO '$file' (FORMAT PARQUET)") }
        }
        return file
    }

    private fun plant(name: String): Path {
        val file = warehouse.resolve(name)
        Files.createDirectories(file.parent)
        return Files.write(file, byteArrayOf(1, 2, 3))
    }

    private fun seedLanceReference(id: Long, path: String) {
        // Legacy metadata only: cleanup never reads dataset contents. Do not mislabel it parquet.
        sql("INSERT INTO ducklake_data_file " +
                "(data_file_id, table_id, begin_snapshot, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start) " +
                "VALUES (?, ?, ?, ?, false, 'lance', 1, 3, 0, 0)", id, tableId, rawCatalog.currentSnapshotId, path)
    }

    private fun schedule(id: Long, path: String, relative: Boolean = false, time: OffsetDateTime = oldTime) {
        sql("INSERT INTO ducklake_files_scheduled_for_deletion " +
                "(data_file_id, path, path_is_relative, schedule_start) VALUES (?, ?, ?, CAST(? AS TIMESTAMPTZ))",
                id, path, relative, time.toString())
    }

    private fun queued(): List<DucklakeScheduledFileEntry> =
            rawCatalog.listScheduledFileBatch(Instant.parse("2100-01-01T00:00:00Z"), 1024)

    private fun sql(query: String, vararg arguments: Any) {
        DriverManager.getConnection(config.getCatalogDatabaseUrl()).use { connection ->
            connection.prepareStatement(query).use { statement ->
                arguments.forEachIndexed { index, value -> statement.setObject(index + 1, value) }
                statement.executeUpdate()
            }
        }
    }

    private data class PageCall(val kind: DucklakeFileKind, val after: Long?, val limit: Int)
}
