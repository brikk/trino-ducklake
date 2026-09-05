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

import com.google.common.collect.ImmutableList
import com.google.inject.Inject
import com.google.inject.Provider
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeCleanupLimits
import dev.brikk.ducklake.catalog.DucklakeFileKind
import dev.brikk.ducklake.catalog.DucklakeScheduledFileEntry
import dev.brikk.ducklake.catalog.DucklakeTableFilePathRef
import dev.brikk.ducklake.trino.plugin.DucklakeFilePathIdentity.Key
import io.airlift.log.Logger
import io.airlift.slice.Slices.utf8Slice
import io.airlift.units.Duration
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.procedure.Procedure
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.VarcharType.VARCHAR
import java.io.IOException
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.time.Instant
import java.util.TreeMap

/**
 * Implements `CALL <catalog>.system.cleanup_old_files(retention_threshold => '7d', dry_run =>
 * false)` — the second half of the two-phase deletion model (see dev-docs/DESIGN-maintenance.md):
 * physically deletes files that `expire_snapshots` (or a cross-engine DuckLake op) previously
 * **scheduled** into `ducklake_files_scheduled_for_deletion`, once they are older than
 * `retention_threshold` (the grace period that protects in-flight, possibly cross-engine, readers).
 * Catalog-scoped, processing one bounded batch per call. Repeat the call to drain further batches.
 *
 * `retention_threshold` defaults to 7d and is floored by `ducklake.maintenance.min-retention`.
 * Retained owners are scanned in bounded pages before any mutation. An exact remaining owner
 * cancels the obsolete queued deletion without removing storage; its eventual retirement can
 * enqueue the object again. Prefix-only overlaps are deferred for manual reconciliation.
 * `dry_run => true` performs the same checks but changes neither storage nor the queue.
 *
 * Scheduled paths written by this connector are absolute; relative ones (written by DuckLake) are
 * resolved against the catalog `data_path` ROOT, not a per-table directory.
 * This does not coordinate concurrent imports or independent engine cleanup.
 */
class DucklakeCleanupOldFilesProcedure @Inject constructor(
        private val catalog: DucklakeCatalog,
        private val fileSystemFactory: DucklakeFileSystemFactory,
        config: DucklakeConfig,
        private val pathIdentity: DucklakeFilePathIdentity,
) : Provider<Procedure> {
    private val minRetention: Duration = config.getMaintenanceMinRetention()
    private val configuredDataPath: String? = config.getDataPath()
    private val batchSize = config.getCleanupBatchSize().also {
        require(it in 1..DucklakeCleanupLimits.MAX_BATCH_SIZE) { "Invalid cleanup batch size" }
    }
    private val maxReferenceFiles = config.getCleanupMaxReferenceFiles().also {
        require(it in 1..10_000_000L) { "Invalid cleanup reference-file limit" }
    }

    override fun get(): Procedure =
        Procedure(
                "system",
                "cleanup_old_files",
                ImmutableList.of(
                        Procedure.Argument("RETENTION_THRESHOLD", VARCHAR, false, utf8Slice("7d")),
                        Procedure.Argument("DRY_RUN", BOOLEAN, false, false)),
                CLEANUP_OLD_FILES.bindTo(this),
                true)

    @Suppress("unused") // invoked via MethodHandle
    fun cleanupOldFiles(
            session: ConnectorSession,
            retentionThreshold: String?,
            dryRun: Boolean,
    ) {
        val retention = parseRetention(retentionThreshold)
        val cutoff: Instant = Instant.now().minusMillis(retention.toMillis())
        val fileSystem = fileSystemFactory.create(session)
        val plan = try {
            planCleanup(fileSystem, cutoff)
        }
        catch (e: IOException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Could not plan cleanup_old_files; no files or queue entries were changed", e)
        }
        if (plan.candidates.isEmpty()) {
            log.info("cleanup_old_files: nothing scheduled past the %s grace period", retention)
            return
        }
        val protected = plan.candidates.filter { it.owned }.map { it.entry }
        val deferred = plan.candidates.count { !it.owned && it.overlaps }
        val deletable = plan.candidates.filter { !it.owned && !it.overlaps }
        if (dryRun) {
            log.info("cleanup_old_files (dry_run): %d files deletable, %d obsolete requests cancellable, %d overlaps deferred; batch_full=%s",
                    deletable.size, protected.size, deferred, plan.batchFull)
            return
        }

        // Do not leave a known-obsolete deletion request for another engine to consume later.
        catalog.removeSelectedScheduledFiles(protected)
        val deleted = mutableListOf<DucklakeScheduledFileEntry>()
        for (candidate in deletable) {
            try {
                if (candidate.directory) fileSystem.deleteDirectory(candidate.location)
                else fileSystem.deleteFile(candidate.location)
                deleted += candidate.entry
            }
            catch (e: IOException) {
                log.warn(e, "cleanup_old_files: failed to delete %s — leaving its selected request scheduled", candidate.location)
            }
        }
        catalog.removeSelectedScheduledFiles(deleted)
        log.info("cleanup_old_files: deleted %d, cancelled %d obsolete requests, deferred %d overlaps, failed %d; batch_full=%s",
                deleted.size, protected.size, deferred, deletable.size - deleted.size, plan.batchFull)
        if (deferred > 0) {
            log.warn("cleanup_old_files: overlapping directory requests remain scheduled and require manual reconciliation before another engine cleans them")
        }
        if (plan.batchFull) log.info("cleanup_old_files: batch limit reached; repeat the call to process further eligible requests")
    }

    private fun planCleanup(fileSystem: TrinoFileSystem, cutoff: Instant): CleanupPlan = catalog.readSession {
        val entries = catalog.listScheduledFileBatch(cutoff, batchSize)
        if (entries.isEmpty()) return@readSession CleanupPlan(emptyList(), false)
        val root = catalog.getCleanupDataPath() ?: configuredDataPath
        if (root != null && root.length > DucklakeCleanupLimits.MAX_PATH_LENGTH) {
            throw TrinoException(GENERIC_USER_ERROR, "cleanup_old_files data_path exceeds the path-length limit; no files or queue entries were changed")
        }
        val candidates = entries.distinct().map { entry ->
            val path = resolveAbsolute(entry.path, entry.pathIsRelative, root)
            val key = cleanupKey(path)
            val location = Location.of(checkNotNull(path))
            CleanupCandidate(entry, location, key, fileSystem.directoryExists(location).orElse(false))
        }
        pathIdentity.validateNamespaces(candidates.map { it.key })
        classifyRetainedOwners(candidates, root)
        CleanupPlan(candidates, entries.size == batchSize)
    }

    private fun classifyRetainedOwners(candidates: List<CleanupCandidate>, root: String?) {
        val byKey = TreeMap<Key, List<CleanupCandidate>>(compareBy({ it.namespace }, { it.path }))
        byKey.putAll(candidates.groupBy { it.key })
        val directories = candidates.filter { it.directory }
        val namespace = candidates.first().key
        var scanned = 0L
        for (kind in DucklakeFileKind.entries) {
            var afterId: Long? = null
            while (true) {
                val remaining = maxReferenceFiles - scanned
                val limit = minOf(batchSize.toLong(), remaining + 1).toInt()
                val page = catalog.listRetainedFileReferencePage(kind, afterId, limit)
                if (page.size > remaining) {
                    throw TrinoException(GENERIC_USER_ERROR, "cleanup_old_files exceeded ducklake.cleanup.max-reference-files=$maxReferenceFiles; " +
                            "no files or queue entries were changed")
                }
                for (reference in page) {
                    val key = cleanupKey(resolveReference(reference.pathRef, root))
                    // One representative suffices: opaque namespaces cannot mix with any other.
                    pathIdentity.validateNamespaces(listOf(namespace, key))
                    markOwner(key, byKey, directories)
                }
                scanned += page.size
                if (page.size < limit) break
                afterId = page.last().fileId
            }
        }
    }

    private fun markOwner(key: Key, byKey: TreeMap<Key, List<CleanupCandidate>>, directories: List<CleanupCandidate>) {
        byKey[key]?.forEach { it.owned = true }
        val prefix = key.directoryPrefix()
        // A retained dataset also owns candidate members beneath it, but not similarly named siblings.
        byKey.tailMap(prefix).entries.asSequence().takeWhile { it.key.isUnder(prefix) }
                .forEach { (_, values) -> values.forEach { it.overlaps = true } }
        for (directory in directories) {
            if (directory.prefix == prefix) directory.owned = true
            else if (key.isUnder(directory.prefix)) directory.overlaps = true
        }
    }

    private fun resolveReference(ref: DucklakeTableFilePathRef, root: String?): String? {
        val schema = resolveScope(ref.schemaPath, ref.schemaPathIsRelative, root)
        val table = resolveScope(ref.tablePath, ref.tablePathIsRelative, schema)
        return resolveAbsolute(ref.path, ref.pathIsRelative, table)
    }

    private fun resolveScope(path: String?, relative: Boolean?, parent: String?): String? =
            if (path.isNullOrBlank()) parent else resolveAbsolute(path, relative == true, parent)

    private fun resolveAbsolute(path: String, relative: Boolean, parent: String?): String? {
        if (!relative) return path
        if (parent.isNullOrBlank()) return null
        return if (parent.endsWith('/')) "$parent$path" else "$parent/$path"
    }

    private fun cleanupKey(path: String?): Key {
        if (path == null || path.length > DucklakeCleanupLimits.MAX_PATH_LENGTH) {
            throw TrinoException(GENERIC_USER_ERROR, "cleanup_old_files cannot resolve a bounded file path; no files or queue entries were changed")
        }
        return pathIdentity.key(path)
    }

    private data class CleanupPlan(val candidates: List<CleanupCandidate>, val batchFull: Boolean)

    private data class CleanupCandidate(
            val entry: DucklakeScheduledFileEntry,
            val location: Location,
            val key: Key,
            val directory: Boolean,
            var owned: Boolean = false,
            var overlaps: Boolean = false,
    ) {
        val prefix: Key = key.directoryPrefix()
    }

    private fun parseRetention(value: String?): Duration {
        val raw = if (value.isNullOrBlank()) "7d" else value
        val retention = try {
            Duration.valueOf(raw)
        }
        catch (e: IllegalArgumentException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid retention_threshold '$raw': ${e.message}", e)
        }
        if (retention.compareTo(minRetention) < 0) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT,
                    "retention_threshold $retention is below the minimum $minRetention "
                            + "(set by ducklake.maintenance.min-retention); refusing to delete recently-scheduled files")
        }
        return retention
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeCleanupOldFilesProcedure::class.java)

        private val CLEANUP_OLD_FILES: MethodHandle = MethodHandles.lookup().findVirtual(
                DucklakeCleanupOldFilesProcedure::class.java,
                "cleanupOldFiles",
                MethodType.methodType(
                        Void.TYPE,
                        ConnectorSession::class.java,
                        String::class.java,
                        java.lang.Boolean.TYPE))
    }
}
