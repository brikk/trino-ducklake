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
import dev.brikk.ducklake.catalog.DucklakeChangeFeedDeletion
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.TrinoFileSystemFactory
import io.trino.parquet.ParquetReaderOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND
import io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.Type
import java.io.IOException
import java.util.Optional

/** Plans a change-feed scan ([ChangeFeedTableHandle]) into [ChangeFeedUnit]s over data files and inlined rows. */
internal class DucklakeChangeFeedPlanner(
        private val catalog: DucklakeCatalog,
        private val pathResolver: DucklakePathResolver,
        private val fileSystemFactory: TrinoFileSystemFactory,
        private val parquetReaderOptions: ParquetReaderOptions,
        private val fileFormatDataSourceStats: FileFormatDataSourceStats,
        private val readStrategy: DucklakeReadStrategy,
        private val inlinedRows: DucklakeInlinedRowCodec.Selector,
        private val parquetPageSources: DucklakeParquetPageSourceFactory,
        private val readWorkLimits: DucklakeReadWorkLimits) {
    /**
     * Page source for a change-feed scan ([ChangeFeedTableHandle]). Builds the read plan from
     * the catalog: the data files inserted in the window (insert side) and, for the delete side,
     * the newly-deleted positions per snapshot (current delete file minus its predecessor, read
     * here through the delete-file readers). Update pairing = the `rowid`s that are BOTH deleted
     * and re-inserted in the same snapshot. Each unit reads its data file through the ordinary
     * data-file pipeline ([DucklakeParquetPageSourceFactory.create]) as of the END-snapshot schema, so
     * schema evolution works unchanged. See [ChangeFeedPageSource].
     */
    fun create(
            session: ConnectorSession,
            table: ChangeFeedTableHandle,
            columns: List<ColumnHandle>): ConnectorPageSource
    {
        val requestedColumns: List<DucklakeColumnHandle> = columns.map { it as DucklakeColumnHandle }
        val requestedDataColumns: List<DucklakeColumnHandle> = requestedColumns.filter { it.columnId >= 0 }

        val schema: DucklakeSchema = catalog.getSchema(table.schemaName, table.endSnapshot)
                ?: throw TrinoException(SCHEMA_NOT_FOUND, "Schema not found for change feed: ${table.schemaName}")
        val tableMetadata: DucklakeTable = catalog.getTableById(table.tableId, table.endSnapshot)
                ?: throw TrinoException(TABLE_NOT_FOUND, "Table not found for change feed: ${table.tableName}")
        val tableDataPath: String = pathResolver.resolveTableDataPath(schema, tableMetadata)
        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)

        val files = DucklakeChangeFeedFiles(fileSystem, parquetReaderOptions, fileFormatDataSourceStats,
                { rows -> readStrategy.chargeRows { rows } })

        val insertFiles: List<DucklakeDataFile> = if (table.feedType != ChangeFeedType.DELETIONS)
            catalog.getDataFilesAddedBetween(table.tableId, table.startSnapshot, table.endSnapshot)
        else
            emptyList()
        val deletions: List<DucklakeChangeFeedDeletion> = if (readStrategy.readsDeletions(table.feedType))
            catalog.getDeletionsBetween(table.tableId, table.startSnapshot, table.endSnapshot)
        else
            emptyList()
        readWorkLimits.files(insertFiles)
        readWorkLimits.descriptors(insertFiles.size.toLong() + deletions.size)
        val physicalCandidates = readStrategy.changeFeedPhysicalWork(insertFiles, deletions)

        // Read DuckLake's embedded row-lineage column (parquet field-id 2147483540) for each file
        // that carries it (UPDATE/compaction output); otherwise a present start supplies identity.
        // This is what lets an UPDATE's delete + re-insert land on the SAME rowid and pair.
        val insertLineage: Map<Long, LongArray?> = readInsertLineage(files, tableDataPath, insertFiles)

        // Physical origin fields are authoritative even when catalog partial_max is absent.
        val insertOriginSnapshots: Map<Long, LongArray?> = readInsertOriginSnapshots(files, tableDataPath, insertFiles)

        val deletedRowidsBySnapshot = mutableMapOf<Long, DucklakeLongSet>()
        val transientPositions = mutableMapOf<String, DucklakeLongSet>()
        val resolvedDeletions: List<ResolvedChangeFeedDeletion> =
                resolveChangeFeedDeletions(table, fileSystem, files, tableDataPath, deletions, deletedRowidsBySnapshot, transientPositions)

        // Inlined data: small writes DuckDB keeps in ducklake_inlined_* tables. Inlined-row
        // insert/delete events (case 1/2) + inline file-position deletes (case 3), all folded into
        // the same rowid space so pairing spans file + inlined sources. See ChangeFeedPageSource.
        val inlined = InlinedChangeCollector(physicalCandidates)
        collectInlinedDataEvents(table, requestedDataColumns, inlined, deletedRowidsBySnapshot)
        val inlineFileDeleteGroups: List<InlineFileDeleteGroup> =
                collectInlineFileDeleteGroups(files, tableDataPath, table, deletedRowidsBySnapshot, transientPositions)

        val updatedBySnapshot = computeUpdatedRowids(
                deletedRowidsBySnapshot, insertFiles, insertLineage, insertOriginSnapshots,
                inlined.insertedRowidsBySnapshot)

        val units: MutableList<ChangeFeedUnit> = mutableListOf()
        for (file in insertFiles) {
            val excluded = transientPositions[pathResolver.resolveFilePath(file.path, file.pathIsRelative, tableDataPath)] ?: DucklakeLongSet()
            units.addAll(insertUnitsForFile(session, files, table, tableDataPath, requestedDataColumns, file,
                    insertLineage[file.dataFileId], insertOriginSnapshots[file.dataFileId], updatedBySnapshot).map { excluding(it, excluded) })
        }
        for (resolved in resolvedDeletions.filter { table.feedType != ChangeFeedType.INSERTIONS }) {
            units.add(deleteUnit(session, files, table, requestedDataColumns, resolved, updatedBySnapshot))
        }
        for (group in inlineFileDeleteGroups.filter { table.feedType != ChangeFeedType.INSERTIONS }) {
            units.add(inlineFileDeleteUnit(session, files, table, tableDataPath, requestedDataColumns, group, updatedBySnapshot))
        }
        for ((snapshotId, events) in inlined.insertEventsBySnapshot) {
            units.add(inlinedDataUnit(snapshotId, events, requestedDataColumns, isDelete = false, updatedBySnapshot))
        }
        for ((snapshotId, events) in inlined.deleteEventsBySnapshot) {
            units.add(inlinedDataUnit(snapshotId, events, requestedDataColumns, isDelete = true, updatedBySnapshot))
        }

        readWorkLimits.events(units.size.toLong())
        readStrategy.chargeRows { units.fold(physicalCandidates) { n, unit -> Math.addExact(n, unit.physicalRows) } }
        return ChangeFeedPageSource(units, requestedColumns, requestedDataColumns, table.feedType.hasChangeType,
                files.getRetainedSizeInBytes(), files::clear)
    }

    private fun excluding(unit: ChangeFeedUnit, positions: DucklakeLongSet): ChangeFeedUnit =
        ChangeFeedUnit(unit.baseSource, unit.snapshotId, unit.rowIdStart, unit.lineageRowIds, unit.keepPositions,
                unit.updatedRowids, unit.isDelete, unit.physicalRows, positions)

    /** One inlined change row for the in-memory base source: its rowid + projected data values. */
    private class InlinedEvent(val rowId: Long, val values: List<Any?>)

    /** Accumulates inlined-data change events by snapshot + the inlined inserted rowids (pairing). */
    private class InlinedChangeCollector(var candidateCount: Long = 0) {
        val insertEventsBySnapshot: MutableMap<Long, MutableList<InlinedEvent>> = mutableMapOf()
        val deleteEventsBySnapshot: MutableMap<Long, MutableList<InlinedEvent>> = mutableMapOf()
        val insertedRowidsBySnapshot = mutableMapOf<Long, DucklakeLongSet>()
    }

    /** One data file's inline-deleted positions retired at one snapshot (case 3). */
    private class InlineFileDeleteGroup(
            val dataFile: DucklakeDataFile,
            val snapshotId: Long,
            val positions: DucklakeLongSet,
            val lineage: LongArray?)

    /** Inlined-data insert/delete events (cases 1 & 2) per snapshot, folding rowids into pairing. */
    private fun collectInlinedDataEvents(
            table: ChangeFeedTableHandle,
            dataColumns: List<DucklakeColumnHandle>,
            collector: InlinedChangeCollector,
            deletedRowidsBySnapshot: MutableMap<Long, DucklakeLongSet>) {
        val wantInserts = table.feedType != ChangeFeedType.DELETIONS
        val wantDeletes = table.feedType != ChangeFeedType.INSERTIONS
        val window: LongRange = table.startSnapshot..table.endSnapshot
        val columnIds: List<Long> = dataColumns.map { it.columnId }
        for (info in catalog.getInlinedDataInfos(table.tableId, table.endSnapshot)) {
            val rows = inlinedRows.codec().changesBetween(
                    table.tableId, info.schemaVersion, table.startSnapshot, table.endSnapshot, columnIds)
            collector.candidateCount = Math.addExact(collector.candidateCount, Math.multiplyExact(rows.size.toLong(), 2L))
            readWorkLimits.events(collector.candidateCount)
            for (row in rows) {
                // A zero-lifetime version never existed in a committed state. Do not turn
                // it into a spurious paired update (even for a deletion-only projection).
                if (row.endSnapshot == row.beginSnapshot) continue
                if (wantInserts && row.beginSnapshot in window) {
                    collector.insertEventsBySnapshot.getOrPut(row.beginSnapshot) { mutableListOf() }
                            .add(InlinedEvent(row.rowId, row.values))
                    collector.insertedRowidsBySnapshot.getOrPut(row.beginSnapshot) { DucklakeLongSet() }.addLong(row.rowId)
                }
                val end: Long? = row.endSnapshot
                if (wantDeletes && end != null && end in window) {
                    collector.deleteEventsBySnapshot.getOrPut(end) { mutableListOf() }
                            .add(InlinedEvent(row.rowId, row.values))
                    deletedRowidsBySnapshot.getOrPut(end) { DucklakeLongSet() }.addLong(row.rowId)
                }
            }
        }
    }

    /** Inline file-position deletes (case 3): group by (data file, snapshot), read lineage, fold rowids. */
    private fun collectInlineFileDeleteGroups(
            files: DucklakeChangeFeedFiles,
            tableDataPath: String,
            table: ChangeFeedTableHandle,
            deletedRowidsBySnapshot: MutableMap<Long, DucklakeLongSet>,
            transientPositions: MutableMap<String, DucklakeLongSet>): List<InlineFileDeleteGroup> {
        if (!readStrategy.readsDeletions(table.feedType)) {
            return emptyList()
        }
        val fileDeletes = catalog.getInlinedFileDeletesBetween(table.tableId, table.startSnapshot, table.endSnapshot)
        if (fileDeletes.isEmpty()) {
            return emptyList()
        }
        val dataFilesById: Map<Long, DucklakeDataFile> = catalog.getDataFilesByIds(
                table.tableId, fileDeletes.map { it.dataFileId }.toSet()).associateBy { it.dataFileId }
        val groups: MutableList<InlineFileDeleteGroup> = mutableListOf()
        for ((key, dels) in fileDeletes.groupBy { it.dataFileId to it.snapshotId }) {
            val dataFile = dataFilesById[key.first] ?: throw TrinoException(GENERIC_INTERNAL_ERROR,
                    "Missing required retained CDC data file ${key.first}")
            if (dels.any { it.position < 0 || it.position >= dataFile.recordCount })
                throw TrinoException(GENERIC_INTERNAL_ERROR, "Inline CDC delete position outside physical file")
            val resolvedPath = pathResolver.resolveFilePath(dataFile.path, dataFile.pathIsRelative, tableDataPath)
            val info = files.get(resolvedPath, dataFile.footerSize, dataFile.fileFormat)
            val origins = info.origins
            val positions = committedPositions(dels.mapTo(DucklakeLongSet()) { it.position }, key.second,
                    dataFile.beginSnapshot, origins, resolvedPath, transientPositions)
            val lineage = info.lineage
            positions.forEachLong { deletedRowidsBySnapshot.getOrPut(key.second) { DucklakeLongSet() }
                    .addLong(rowIdForPosition(lineage, dataFile.rowIdStart, it)) }
            groups.add(InlineFileDeleteGroup(dataFile, key.second, positions, lineage))
        }
        return groups
    }

    private fun inlineFileDeleteUnit(
            session: ConnectorSession,
            files: DucklakeChangeFeedFiles,
            table: ChangeFeedTableHandle,
            tableDataPath: String,
            dataColumns: List<DucklakeColumnHandle>,
            group: InlineFileDeleteGroup,
            updatedBySnapshot: Map<Long, DucklakeLongSet>): ChangeFeedUnit {
        val df = group.dataFile
        val resolvedPath = pathResolver.resolveFilePath(df.path, df.pathIsRelative, tableDataPath)
        return ChangeFeedUnit(
                baseSource = {
                    openChangeFeedFile(session, files, resolvedPath, df.fileFormat, df.footerSize,
                            df.fileSizeBytes, df.rowIdStart, df.recordCount, df.beginSnapshot, table, dataColumns)
                },
                snapshotId = group.snapshotId,
                rowIdStart = df.rowIdStart,
                lineageRowIds = group.lineage,
                keepPositions = group.positions,
                updatedRowids = updatedBySnapshot[group.snapshotId] ?: emptySet(),
                isDelete = true,
                physicalRows = df.recordCount)
    }

    /** A change-feed unit backed by IN-MEMORY inlined rows (no data file): the base source yields
     * the group's data values + a `$row_id` column = the inlined `row_id` (so rowid = row_id). */
    private fun inlinedDataUnit(
            snapshotId: Long,
            events: List<InlinedEvent>,
            dataColumns: List<DucklakeColumnHandle>,
            isDelete: Boolean,
            updatedBySnapshot: Map<Long, DucklakeLongSet>): ChangeFeedUnit =
        ChangeFeedUnit(
                baseSource = { inlinedRecordPageSource(events, dataColumns) },
                snapshotId = snapshotId,
                rowIdStart = 0L,
                lineageRowIds = null,
                keepPositions = null,
                updatedRowids = updatedBySnapshot[snapshotId] ?: emptySet(),
                isDelete = isDelete,
                physicalRows = events.size.toLong())

    private fun inlinedRecordPageSource(events: List<InlinedEvent>, dataColumns: List<DucklakeColumnHandle>): ConnectorPageSource = catalog.readSession {
        val dataTypes: List<Type> = dataColumns.map { it.columnType }
        val types: List<Type> = dataTypes + BIGINT
        val codec = inlinedRows.codec()
        val rows: List<List<Any?>> = events.map { event ->
            dataTypes.indices.map { i -> codec.convert(event.values[i], dataTypes[i]) } + event.rowId
        }
        DucklakeNativeRowsPageSource(types, rows)
    }

    private class ResolvedChangeFeedDeletion(
            val deletion: DucklakeChangeFeedDeletion,
            // The snapshot to REPORT these deletions at. Usually the delete file's begin_snapshot
            // (== deletion.snapshotId), but for a CONSOLIDATED ("partial") delete file it is the
            // per-row `_ducklake_internal_snapshot_id`, so one physical file yields several
            // resolutions — one per in-window deletion snapshot.
            val snapshotId: Long,
            val resolvedPath: String,
            val deletedPositions: DucklakeLongSet,
            val lineage: LongArray?)

    /** Per insert-file embedded row-lineage array (keyed by data_file_id), null when absent. */
    private fun readInsertLineage(
            files: DucklakeChangeFeedFiles,
            tableDataPath: String,
            insertFiles: List<DucklakeDataFile>): Map<Long, LongArray?> {
        val lineage: MutableMap<Long, LongArray?> = mutableMapOf()
        for (file in insertFiles) {
            val resolvedPath: String = pathResolver.resolveFilePath(file.path, file.pathIsRelative, tableDataPath)
            lineage[file.dataFileId] = files.get(resolvedPath, file.footerSize, file.fileFormat).lineage
        }
        return lineage
    }

    /**
     * A file is a cross-snapshot compacted ("partial") file when its `partial_max` (MAX origin
     * snapshot) exceeds its `begin_snapshot` (MIN origin) — i.e. it merges rows from more than one
     * source snapshot. Ordinary files have `partial_max == null` (or `== begin_snapshot`).
     */
    private fun isPartialFile(file: DucklakeDataFile): Boolean {
        val max: Long = file.partialMax ?: return false
        return max > file.beginSnapshot
    }

    /**
     * Per insert-file embedded `_ducklake_internal_snapshot_id` array (keyed by data_file_id) — the
     * ORIGIN snapshot of each row, in file order. Physical presence is authoritative;
     * an ordinary file without the field maps to null and uses its `begin_snapshot`.
     */
    private fun readInsertOriginSnapshots(
            files: DucklakeChangeFeedFiles,
            tableDataPath: String,
            insertFiles: List<DucklakeDataFile>): Map<Long, LongArray?> {
        val origins: MutableMap<Long, LongArray?> = mutableMapOf()
        for (file in insertFiles) {
            val resolvedPath: String = pathResolver.resolveFilePath(file.path, file.pathIsRelative, tableDataPath)
            origins[file.dataFileId] = try {
                files.get(resolvedPath, file.footerSize, file.fileFormat).origins.also {
                    if (isPartialFile(file) && it == null) throw TrinoException(GENERIC_INTERNAL_ERROR,
                            "Partial data file is missing its origin snapshot column: $resolvedPath")
                }
            }
            catch (e: IOException) {
                throw TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read partial-file origin snapshots: $resolvedPath", e)
            }
        }
        return origins
    }

    /** The DuckLake rowid of a file-local [position]: the embedded lineage value when present. */
    private fun rowIdForPosition(lineage: LongArray?, rowIdStart: Long?, position: Long): Long =
            DucklakeRowIdentity.rowId(lineage, rowIdStart, position)

    /** Resolves each deletion to its newly-deleted positions + lineage, dropping no-op events and
     * recording the deleted rowids by snapshot (for update pairing).
     *
     * A CONSOLIDATED ("partial") current delete file — 3-column `(file_path, pos,
     * _ducklake_internal_snapshot_id)` produced by delete consolidation (`flush_inlined_data`),
     * whose `begin_snapshot` is only the MIN of the folded deletion snapshots and `partial_max` the
     * MAX — is split into one resolution PER in-window deletion snapshot, each reporting that
     * snapshot's positions. Otherwise the whole batch would be mis-reported at `begin_snapshot` and
     * mis-windowed. */
    private fun resolveChangeFeedDeletions(
            table: ChangeFeedTableHandle,
            fileSystem: TrinoFileSystem,
            files: DucklakeChangeFeedFiles,
            tableDataPath: String,
            deletions: List<DucklakeChangeFeedDeletion>,
            deletedRowidsBySnapshot: MutableMap<Long, DucklakeLongSet>,
            transientPositions: MutableMap<String, DucklakeLongSet>): List<ResolvedChangeFeedDeletion> {
        val resolved: MutableList<ResolvedChangeFeedDeletion> = mutableListOf()
        for (deletion in deletions) {
            val resolvedPath: String = pathResolver.resolveFilePath(
                    deletion.dataFilePath, deletion.dataFilePathIsRelative, tableDataPath)
            val info = files.get(resolvedPath, deletion.dataFileFooterSize, deletion.dataFileFormat)
            val lineage = info.lineage
            val origins = info.origins

            for ((eventSnapshot, candidates) in newlyDeletedPositionsBySnapshot(fileSystem, tableDataPath, table, deletion)) {
                val positions = committedPositions(candidates, eventSnapshot, deletion.dataFileBeginSnapshot,
                        origins, resolvedPath, transientPositions)
                if (positions.isEmpty()) {
                    continue
                }
                val deletedRowids = deletedRowidsBySnapshot.getOrPut(eventSnapshot) { DucklakeLongSet() }
                positions.forEachLong { deletedRowids.addLong(rowIdForPosition(lineage, deletion.rowIdStart, it)) }
                resolved.add(ResolvedChangeFeedDeletion(deletion, eventSnapshot, resolvedPath, positions, lineage))
            }
        }
        return resolved
    }

    private fun committedPositions(
            positions: DucklakeLongSet, deletedAt: Long, begin: Long, origins: LongArray?, path: String,
            transients: MutableMap<String, DucklakeLongSet>): DucklakeLongSet = positions.filterLong { position ->
        if (position < 0 || (origins != null && position >= origins.size))
            throw TrinoException(GENERIC_INTERNAL_ERROR, "CDC delete position outside physical origin array")
        val origin = origins?.get(Math.toIntExact(position)) ?: begin
        if (origin > deletedAt) throw TrinoException(GENERIC_INTERNAL_ERROR, "CDC deletion precedes physical row origin")
        if (origin == deletedAt) {
            transients.getOrPut(path) { DucklakeLongSet() }.addLong(position)
            false
        } else true
    }

    /**
     * Newly-deleted file positions grouped by the snapshot to REPORT them at.
     *
     * Ordinary (2-column) or full-file delete: one group at `deletion.snapshotId` (the incremental
     * `current − previous` diff). Consolidated 3-column current file: the per-row
     * `_ducklake_internal_snapshot_id` gives each position's true deletion snapshot — group by it and
     * keep only groups inside the feed window `[startSnapshot, endSnapshot]`. `previous` is not
     * subtracted in the consolidated case: consolidation folds prior deletions INTO this file with
     * their own snapshots, so the per-row snapshot already windows them correctly.
     */
    private fun newlyDeletedPositionsBySnapshot(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            table: ChangeFeedTableHandle,
            deletion: DucklakeChangeFeedDeletion): Map<Long, DucklakeLongSet> {
        val currentDeletePath = deletion.currentDeletePath
        puffinDeletionSnapshots(fileSystem, tableDataPath, deletion)?.let { snapshots ->
            return snapshots.filterKeys { it in table.startSnapshot..table.endSnapshot }
        }
        if (!deletion.fullFileDelete && currentDeletePath != null &&
                deletion.currentDeleteFormat.equals("parquet", ignoreCase = true)) {
            val currentPath: String = pathResolver.resolveFilePath(
                    currentDeletePath, deletion.currentDeletePathIsRelative ?: false, tableDataPath)
            // Multi-snapshot shape is a property of the physical file, not catalog partial_max.
            // Upstream flush writes the marker column while leaving partial_max NULL.
            val posToSnapshot = DucklakeDeleteFileReader.readPositionsWithSnapshotsIfPresent(
                    fileSystem,
                    currentPath,
                    deletion.currentDeleteFooterSize ?: 0L,
                    parquetReaderOptions,
                    fileFormatDataSourceStats)
            if (posToSnapshot != null) {
                val window: LongRange = table.startSnapshot..table.endSnapshot
                val bySnapshot = mutableMapOf<Long, DucklakeLongSet>()
                for ((position, snapshot) in posToSnapshot) {
                    if (snapshot in window) {
                        bySnapshot.getOrPut(snapshot) { DucklakeLongSet() }.addLong(position)
                    }
                }
                return bySnapshot
            }
        }
        return mapOf(deletion.snapshotId to newlyDeletedPositions(fileSystem, tableDataPath, deletion))
    }

    private fun puffinDeletionSnapshots(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            deletion: DucklakeChangeFeedDeletion): Map<Long, DucklakeLongSet>? {
        if (deletion.fullFileDelete) return null
        val currentPath = deletion.currentDeletePath ?: return null
        if (!deletion.currentDeleteFormat.equals("puffin", ignoreCase = true) && !DucklakeReadSupport.isPuffinPath(currentPath)) return null
        val path = pathResolver.resolveFilePath(currentPath, deletion.currentDeletePathIsRelative ?: false, tableDataPath)
        return DucklakePuffinDeleteReader.readDeletionGroupsIfPresent(fileSystem.newInputFile(DucklakeReadSupport.toLocation(path)))
    }

    /** rowids that were BOTH deleted and inserted in the same snapshot (update pairing). Inserted
     * rowids come from file lineage values / the `[rowIdStart, +count)` range, plus inlined-insert
     * rowids. For a cross-snapshot compacted ("partial") file each row's inserted rowid is bucketed
     * by its PER-ROW origin snapshot ([insertOriginSnapshots]), not the file's `begin_snapshot`, so
     * an update's re-insert pairs against the delete in the snapshot that actually re-inserted it. */
    private fun computeUpdatedRowids(
            deletedRowidsBySnapshot: Map<Long, DucklakeLongSet>,
            insertFiles: List<DucklakeDataFile>,
            insertLineage: Map<Long, LongArray?>,
            insertOriginSnapshots: Map<Long, LongArray?>,
            inlinedInsertedRowidsBySnapshot: Map<Long, DucklakeLongSet>): Map<Long, DucklakeLongSet> {
        val rangesBySnapshot: MutableMap<Long, MutableList<LongRange>> = mutableMapOf()
        val lineageSetBySnapshot = mutableMapOf<Long, DucklakeLongSet>()
        for (file in insertFiles) {
            val lineage: LongArray? = insertLineage[file.dataFileId]
            val origins: LongArray? = insertOriginSnapshots[file.dataFileId]
            if (file.recordCount == 0L) continue
            if (origins != null && lineage != null) {
                // Partial file: attribute each row's rowid to its own origin snapshot. (Compaction
                // output always embeds BOTH the lineage rowid and the origin-snapshot columns, so
                // both arrays are present and index-aligned by file position.)
                val n: Int = minOf(origins.size, lineage.size)
                for (position in 0 until n) {
                    lineageSetBySnapshot.getOrPut(origins[position]) { DucklakeLongSet() }.addLong(lineage[position])
                }
            }
            else if (lineage != null) {
                val ids = lineageSetBySnapshot.getOrPut(file.beginSnapshot) { DucklakeLongSet() }
                lineage.forEach { ids.addLong(it) }
            }
            else {
                val start = DucklakeRowIdentity.rowId(null, file.rowIdStart, 0)
                rangesBySnapshot.getOrPut(file.beginSnapshot) { mutableListOf() }
                        .add(start until Math.addExact(start, file.recordCount))
            }
        }
        return deletedRowidsBySnapshot.mapNotNull { (snapshot, deletedRowids) ->
            val ranges: List<LongRange> = rangesBySnapshot[snapshot] ?: emptyList()
            val lineageSet = lineageSetBySnapshot[snapshot]
            val inlinedSet = inlinedInsertedRowidsBySnapshot[snapshot]
            val updated = deletedRowids.filterLong { rowId ->
                lineageSet?.containsLong(rowId) == true || inlinedSet?.containsLong(rowId) == true || ranges.any { rowId in it }
            }
            if (updated.isEmpty()) null else snapshot to updated
        }.toMap()
    }

    /**
     * The insert-side change-feed unit(s) for one data file.
     *
     * Ordinary file: ONE unit whose every row is attributed to the file's `begin_snapshot`
     * (`keepPositions = null`, whole file kept).
     *
     * Cross-snapshot compacted ("partial") file ([originSnapshots] non-null): the file merges rows
     * from several source snapshots, so attributing all rows to `begin_snapshot` is wrong. Emit one
     * unit PER distinct origin snapshot that falls in the feed window `[startSnapshot, endSnapshot]`,
     * each keeping only that snapshot's file positions and stamping that snapshot id. Rows whose
     * origin is outside the window are dropped (they belong to a different change-feed window).
     */
    private fun insertUnitsForFile(
            session: ConnectorSession,
            files: DucklakeChangeFeedFiles,
            table: ChangeFeedTableHandle,
            tableDataPath: String,
            dataColumns: List<DucklakeColumnHandle>,
            file: DucklakeDataFile,
            lineage: LongArray?,
            originSnapshots: LongArray?,
            updatedBySnapshot: Map<Long, DucklakeLongSet>): List<ChangeFeedUnit> {
        val resolvedPath: String = pathResolver.resolveFilePath(file.path, file.pathIsRelative, tableDataPath)
        val baseSource: () -> ConnectorPageSource = {
            openChangeFeedFile(session, files, resolvedPath, file.fileFormat, file.footerSize,
                    file.fileSizeBytes, file.rowIdStart, file.recordCount, file.beginSnapshot, table, dataColumns)
        }

        if (originSnapshots == null) {
            return listOf(ChangeFeedUnit(
                    baseSource = baseSource,
                    snapshotId = file.beginSnapshot,
                    rowIdStart = file.rowIdStart,
                    lineageRowIds = lineage,
                    keepPositions = null,
                    updatedRowids = updatedBySnapshot[file.beginSnapshot] ?: emptySet(),
                    isDelete = false,
                    physicalRows = file.recordCount))
        }

        val window: LongRange = table.startSnapshot..table.endSnapshot
        val positionsBySnapshot = mutableMapOf<Long, DucklakeLongSet>()
        for (position in originSnapshots.indices) {
            val origin: Long = originSnapshots[position]
            if (origin in window) {
                positionsBySnapshot.getOrPut(origin) { DucklakeLongSet() }.addLong(position.toLong())
            }
        }
        return positionsBySnapshot.entries
                .sortedBy { it.key }
                .map { (snapshotId, positions) ->
                    ChangeFeedUnit(
                            baseSource = baseSource,
                            snapshotId = snapshotId,
                            rowIdStart = file.rowIdStart,
                            lineageRowIds = lineage,
                            keepPositions = positions,
                            updatedRowids = updatedBySnapshot[snapshotId] ?: emptySet(),
                            isDelete = false,
                            physicalRows = file.recordCount)
                }
    }

    private fun deleteUnit(
            session: ConnectorSession,
            files: DucklakeChangeFeedFiles,
            table: ChangeFeedTableHandle,
            dataColumns: List<DucklakeColumnHandle>,
            resolved: ResolvedChangeFeedDeletion,
            updatedBySnapshot: Map<Long, DucklakeLongSet>): ChangeFeedUnit {
        val deletion: DucklakeChangeFeedDeletion = resolved.deletion
        return ChangeFeedUnit(
                baseSource = {
                    openChangeFeedFile(session, files, resolved.resolvedPath, deletion.dataFileFormat, deletion.dataFileFooterSize,
                            deletion.dataFileSizeBytes, deletion.rowIdStart, deletion.recordCount,
                            deletion.dataFileBeginSnapshot, table, dataColumns)
                },
                snapshotId = resolved.snapshotId,
                rowIdStart = deletion.rowIdStart,
                lineageRowIds = resolved.lineage,
                keepPositions = resolved.deletedPositions,
                updatedRowids = updatedBySnapshot[resolved.snapshotId] ?: emptySet(),
                isDelete = true,
                physicalRows = deletion.recordCount)
    }

    /** Opens one change-feed data file through the ordinary read pipeline, projecting the data
     * columns followed by `$file_row_number` (raw file positions — lineage/rowid resolution
     * happens in ChangeFeedPageSource), reading as of [table]'s END-snapshot schema. */
    private fun openChangeFeedFile(
            session: ConnectorSession,
            files: DucklakeChangeFeedFiles,
            resolvedPath: String,
            fileFormat: String,
            footerSize: Long,
            fileSizeBytes: Long,
            rowIdStart: Long?,
            recordCount: Long,
            beginSnapshot: Long,
            table: ChangeFeedTableHandle,
            dataColumns: List<DucklakeColumnHandle>): ConnectorPageSource
    {
        val split = DucklakeSplit(
                resolvedPath,
                emptyList(),
                rowIdStart,
                recordCount,
                fileSizeBytes,
                fileFormat,
                TupleDomain.all(),
                footerSize,
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptySet(),
                Optional.empty(),
                beginSnapshot,
                null,
                emptyMap())
        val syntheticHandle = DucklakeTableHandle(table.schemaName, table.tableName, table.tableId, table.endSnapshot)
        // $file_row_number, NOT $row_id: the feed needs the raw FILE POSITION to
        // match keepPositions and index lineage arrays; $row_id is lineage-first, not positional.
        val readColumns: List<DucklakeColumnHandle> = dataColumns + VirtualKind.FILE_ROW_NUMBER.columnHandle()
        return try {
            val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
            catalog.queryReadSession(session.queryId) {
                parquetPageSources.create(split, syntheticHandle, readColumns, TupleDomain.all(), fileSystem,
                        files.get(resolvedPath, footerSize, fileFormat).metadata)
            }
        }
        catch (e: IOException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open change-feed data file: $resolvedPath", e)
        }
    }

    /** The FILE-LOCAL positions newly deleted at [deletion]'s snapshot: current delete positions
     * minus the predecessor's. (Mapped to rowids by the caller, lineage-aware.) */
    private fun newlyDeletedPositions(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            deletion: DucklakeChangeFeedDeletion): DucklakeLongSet
    {
        val current = if (deletion.fullFileDelete) {
            DucklakeLongSet().apply { addRange(0, deletion.recordCount) }
        }
        else {
            readDeletePositions(fileSystem, tableDataPath, deletion.currentDeletePath, deletion.currentDeletePathIsRelative,
                    deletion.currentDeleteFormat, deletion.currentDeleteFooterSize, deletion.rowIdStart)
        }
        val previous = readDeletePositions(fileSystem, tableDataPath, deletion.previousDeletePath,
                deletion.previousDeletePathIsRelative, deletion.previousDeleteFormat, deletion.previousDeleteFooterSize, deletion.rowIdStart)
        return current.subtract(previous)
    }

    /** Reads a delete file's FILE-LOCAL positions (parquet or puffin); empty when [path] is null. */
    private fun readDeletePositions(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            path: String?,
            pathIsRelative: Boolean?,
            format: String?,
            footerSize: Long?,
            rowIdStart: Long?): DucklakeLongSet
    {
        if (path == null) {
            return DucklakeLongSet()
        }
        val resolved: String = pathResolver.resolveFilePath(path, pathIsRelative ?: false, tableDataPath)
        if (DucklakeReadSupport.isPuffinPath(resolved) || "puffin".equals(format, ignoreCase = true)) {
            return DucklakePuffinDeleteReader.readDeletedPositions(fileSystem.newInputFile(DucklakeReadSupport.toLocation(resolved)))
        }
        val positions = DucklakeDeleteFileReader.readPositions(
                fileSystem, resolved, footerSize ?: 0L, parquetReaderOptions, fileFormatDataSourceStats)
        // Legacy Trino delete files store GLOBAL row ids (rowIdStart + pos); normalize to file-local.
        return if (positions.global) {
            val start = DucklakeRowIdentity.requireStart(rowIdStart)
            DucklakeLongSet().also { local -> positions.values.forEachLong { local.addLong(Math.subtractExact(it, start)) } }
        } else positions.values
    }
}
