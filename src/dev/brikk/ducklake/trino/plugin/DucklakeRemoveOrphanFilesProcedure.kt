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

import com.google.inject.Inject
import com.google.inject.Provider
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeReferencedFiles
import dev.brikk.ducklake.catalog.DucklakeTableFilePathRef
import dev.brikk.ducklake.trino.plugin.DucklakeFilePathIdentity.Key
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.DEFAULT_RETENTION
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.parseRetention
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.requireSchemaForTable
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.resolveSchema
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.resolveTable
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.systemProcedure
import io.airlift.log.Logger
import io.airlift.slice.Slices.utf8Slice
import io.airlift.units.Duration
import io.trino.filesystem.FileEntry
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.procedure.Procedure
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.VarcharType.VARCHAR
import java.io.IOException
import java.time.Instant

/**
 * Implements `CALL <catalog>.system.remove_orphan_files(schema_name => ..., table_name => ...,
 * retention_threshold => '7d', dry_run => false)` — deletes files under the target data path(s)
 * that **no catalog row references** and that are **older than the retention threshold**. Orphans
 * are the residue of failed/aborted commits (a data file written, then the catalog commit lost).
 *
 * Scope by argument presence (the more you supply, the narrower):
 *  - `schema_name` + `table_name` → one table's data path.
 *  - `schema_name` only → every table in the schema; scans the schema's data directory.
 *  - neither → the whole catalog; scans the warehouse root. The known set is the UNION of every
 *      catalog-referenced file, so residue from a **failed CREATE TABLE** (no catalog
 *      row, so no per-table sweep could name it) is reclaimed, and one table's live files are never
 *      mistaken for another's orphans. (Tables with a custom absolute `location` outside the
 *      scanned root are covered by a table-scoped call — the wide scan only walks the root tree.)
 *
 * Safety (see dev-docs/DESIGN-maintenance.md):
 *  - Orphans have no catalog row, so this touches storage ONLY — no snapshot, no catalog mutation,
 *    no conflict matrix. The global "known set" is every path the catalog references at ANY
 *    snapshot (including dropped-but-unexpired tables/schemas) plus files already scheduled for
 *    deletion ([DucklakeCatalog.listAllReferencedFiles]). A listed file not in that set and not
 *    inside a referenced directory is a candidate orphan.
 *  - **Type-scoped, not a blind diff**: only recognizably DuckLake-written files are deleted (see
 *    [isDucklakeManagedResidue]); foreign files a user parked under the data path are never
 *    touched. Upstream DuckLake's `ducklake_delete_orphaned_files` (1.5.5) sweeps
 *    `.parquet`/`.puffin` only.
 *  - The retention threshold is the grace period that keeps the op safe without a global lock: a
 *    file young enough to still be referenced by an in-flight (possibly cross-engine) writer is
 *    never deleted. The argument is floored by `ducklake.remove-orphan-files.min-retention`
 *    (default 7d); a call below the floor is rejected so the op can't be turned into a foot-gun.
 *  - At most [MAX_ORPHANS_PER_CALL] orphans are deleted per call; repeat the call to continue.
 *  - `dry_run => true` logs what would be deleted and removes nothing.
 *  - Directory shells are retained: recursively deleting a directory after an emptiness check
 *    can delete a concurrent writer's new files, bypassing ownership and retention checks.
 *  - Comparison uses verified native-local/S3 aliases, not raw path spelling. Unresolved mixed
 *    namespaces and noncanonical fallback paths abort the entire sweep before deletion.
 *
 * `.db` is deliberately NOT eligible: DuckDB supports placing the metadata catalog under
 * `data_path`, and a `ducklake-*.db` catalog must never be mistaken for an orphan data file.
 */
class DucklakeRemoveOrphanFilesProcedure @Inject constructor(
        private val catalog: DucklakeCatalog,
        private val fileSystemFactory: DucklakeFileSystemFactory,
        private val pathResolver: DucklakePathResolver,
        config: DucklakeConfig,
        private val pathIdentity: DucklakeFilePathIdentity,
) : Provider<Procedure> {
    private val minRetention: Duration = config.getRemoveOrphanFilesMinRetention()

    override fun get(): Procedure =
        systemProcedure(
                "remove_orphan_files",
                listOf(
                        // schema_name / table_name are OPTIONAL: the more you supply, the narrower
                        // the scope — table (both) → schema (schema only) → catalog-wide (neither).
                        Procedure.Argument("SCHEMA_NAME", VARCHAR, false, null),
                        Procedure.Argument("TABLE_NAME", VARCHAR, false, null),
                        Procedure.Argument("RETENTION_THRESHOLD", VARCHAR, false, utf8Slice(DEFAULT_RETENTION)),
                        Procedure.Argument("DRY_RUN", BOOLEAN, false, false)),
                this,
                "removeOrphanFiles",
                String::class.java,
                String::class.java,
                String::class.java,
                java.lang.Boolean.TYPE)

    @Suppress("unused") // invoked via MethodHandle
    fun removeOrphanFiles(
            session: ConnectorSession,
            schemaName: String?,
            tableName: String?,
            retentionThreshold: String?,
            dryRun: Boolean,
    ) {
        requireSchemaForTable(schemaName, tableName)
        val retention = parseRetention(retentionThreshold, minRetention, "ducklake.remove-orphan-files.min-retention",
                "refusing to delete recently-written files")
        val snapshotId = catalog.currentSnapshotId
        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
        val cutoff: Instant = Instant.now().minusMillis(retention.toMillis())

        // Scan scope is narrowable, but the KNOWN set is always global: after DROP, a retained
        // table/schema row can still own files for time travel even though active-object listing
        // no longer finds it.
        val (scanRoot, scopeLabel) = when {
            !schemaName.isNullOrEmpty() && !tableName.isNullOrEmpty() -> {
                val (schema, table) = resolveTable(catalog, schemaName, tableName, snapshotId)
                pathResolver.resolveTableDataPath(schema, table) to "table $schemaName.$tableName"
            }
            !schemaName.isNullOrEmpty() ->
                pathResolver.resolveSchemaDataPath(resolveSchema(catalog, schemaName, snapshotId)) to "schema $schemaName"
            else -> pathResolver.rootDataPath() to "catalog"
        }

        val knownPaths = resolveKnownPaths(catalog.listAllReferencedFiles())
        pathIdentity.validateNamespaces(knownPaths + pathIdentity.key(scanRoot))

        sweep(fileSystem, scanRoot, knownPaths, cutoff, dryRun, scopeLabel)
    }

    private fun sweep(
            fileSystem: TrinoFileSystem,
            scanRoot: String,
            knownPaths: Set<Key>,
            cutoff: Instant,
            dryRun: Boolean,
            scopeLabel: String,
    ) {
        val orphans = findOrphans(fileSystem, scanRoot, knownPaths, cutoff)
        val more = if (orphans.size == MAX_ORPHANS_PER_CALL) "; limit reached, repeat the call to continue" else ""
        if (orphans.isEmpty()) {
            log.info("remove_orphan_files: no orphans for %s", scopeLabel)
            return
        }
        if (dryRun) {
            log.info("remove_orphan_files (dry_run): %d orphan file(s) for %s would be deleted%s: %s",
                    orphans.size, scopeLabel, more, orphans)
            return
        }
        try {
            fileSystem.deleteFiles(orphans)
        }
        catch (e: IOException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Failed to delete orphan files for $scopeLabel: ${e.message}", e)
        }
        log.info("remove_orphan_files: deleted %d orphan file(s) for %s%s", orphans.size, scopeLabel, more)
    }

    /**
     * Lists [scanRoot] and keeps up to [MAX_ORPHANS_PER_CALL] deletable orphans. Every listed
     * entry examined is namespace-checked before any deletion happens.
     */
    private fun findOrphans(
            fileSystem: TrinoFileSystem,
            scanRoot: String,
            knownPaths: Set<Key>,
            cutoff: Instant,
    ): List<Location> {
        val rootKey = pathIdentity.key(scanRoot)
        val orphans = mutableListOf<Location>()
        try {
            val files = fileSystem.listFiles(Location.of(scanRoot))
            while (files.hasNext() && orphans.size < MAX_ORPHANS_PER_CALL) {
                val entry = files.next()
                val key = pathIdentity.key(entry.location().toString())
                pathIdentity.validateListing(rootKey, key)
                if (isDeletableOrphan(entry, key, knownPaths, cutoff)) {
                    orphans.add(entry.location())
                }
            }
        }
        catch (e: IOException) {
            // A missing data path (table never written, or already cleaned) means no orphans.
            log.info("remove_orphan_files: could not list %s (%s) — treating as no orphans", scanRoot, e.message)
        }
        return orphans
    }

    /**
     * An entry is a deletable orphan iff it is (a) not a known catalog path, (b) not inside a
     * referenced directory (a dataset registered as a directory owns its member files),
     * (c) recognizably DuckLake-managed residue, and (d) older than the cutoff.
     */
    private fun isDeletableOrphan(entry: FileEntry, key: Key, knownPaths: Set<Key>, cutoff: Instant): Boolean =
        key !in knownPaths &&
                !isInsideKnownDirectory(key, knownPaths) &&
                isDucklakeManagedResidue(entry.location().toString()) &&
                entry.lastModified().isBefore(cutoff)

    /**
     * Whether some known path is a directory ancestor of [key], i.e. `key.isUnder(known.directoryPrefix())`,
     * found by probing each ancestor of [key] (with and without its trailing slash) in [knownPaths].
     */
    private fun isInsideKnownDirectory(key: Key, knownPaths: Set<Key>): Boolean {
        if (Key(key.namespace, "") in knownPaths) {
            return true
        }
        val path = key.path
        var slash = path.indexOf('/')
        while (slash >= 0) {
            if (Key(key.namespace, path.substring(0, slash)) in knownPaths ||
                    Key(key.namespace, path.substring(0, slash + 1)) in knownPaths) {
                return true
            }
            slash = path.indexOf('/', slash + 1)
        }
        return false
    }

    /**
     * Whether [path] is a file a DuckLake writer could have produced as data or delete-file
     * residue — the only files `remove_orphan_files` may delete. Two shapes:
     *
     *  - A **single data/delete file**: basename starts with the `ducklake-` prefix every DuckLake
     *    writer uses (`ducklake-<uuid>.parquet`; delete files `ducklake-delete-<uuid>.<ext>` from
     *    this connector, `ducklake-<uuid>-delete.<ext>` from DuckDB) and ends with a managed
     *    extension. `.vortex` is listed because earlier releases of this connector wrote it.
     *  - A **Lance dataset member**: earlier releases also wrote Lance datasets as directories
     *    (`ducklake-<uuid>.lance/`) whose member files do not carry the prefix, so they are
     *    recognized by the enclosing dataset directory anywhere in the path.
     */
    private fun isDucklakeManagedResidue(path: String): Boolean {
        val basename = path.substringAfterLast('/')
        if (basename.startsWith(DUCKLAKE_FILE_PREFIX) &&
                MANAGED_FILE_EXTENSIONS.any { basename.endsWith(it, ignoreCase = true) }) {
            return true
        }
        return path.split('/').any { segment ->
            segment.startsWith(DUCKLAKE_FILE_PREFIX) && segment.endsWith(LANCE_DATASET_SUFFIX, ignoreCase = true)
        }
    }

    /** Resolve all three spec path scopes without losing dropped-but-retained ownership. */
    private fun resolveKnownPaths(refs: DucklakeReferencedFiles): Set<Key> {
        val root = pathResolver.rootDataPath()
        val tableFiles = refs.tableFiles.map { resolveKnownTableFile(it, root) }
        val scheduledFiles = refs.scheduledFiles.map { ref ->
            pathIdentity.key(pathResolver.resolveFilePath(ref.path, ref.pathIsRelative, root))
        }
        return (tableFiles + scheduledFiles).toSet()
    }

    private fun resolveKnownTableFile(ref: DucklakeTableFilePathRef, root: String): Key {
        val schemaPath = pathResolver.resolveScopedPath(ref.schemaPath, ref.schemaPathIsRelative, root)
        val tablePath = pathResolver.resolveScopedPath(ref.tablePath, ref.tablePathIsRelative, schemaPath)
        return pathIdentity.key(pathResolver.resolveFilePath(ref.path, ref.pathIsRelative, tablePath))
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeRemoveOrphanFilesProcedure::class.java)

        /** Bounds the in-memory orphan list and the deletion work of one call. */
        internal const val MAX_ORPHANS_PER_CALL: Int = 10_000

        private const val DUCKLAKE_FILE_PREFIX: String = "ducklake-"
        private const val LANCE_DATASET_SUFFIX: String = ".lance"
        private val MANAGED_FILE_EXTENSIONS: List<String> = listOf(".parquet", ".puffin", ".vortex")
    }
}
