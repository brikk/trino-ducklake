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

import io.airlift.log.Logger
import io.trino.filesystem.TrinoFileSystem
import io.trino.parquet.ParquetReaderOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.TransformConnectorPageSource
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.SourcePage
import java.util.function.Function

/** Merge-on-read deletes of one data-file split: delete files, inlined deletes and the partial-file snapshot filter. */
internal class DucklakeSplitDeleteFilter(
        private val parquetReaderOptions: ParquetReaderOptions,
        private val fileFormatDataSourceStats: FileFormatDataSourceStats) {
    fun apply(fileSystem: TrinoFileSystem, split: DucklakeSplit, dataSource: ConnectorPageSource): ConnectorPageSource
    {
        val hasDeleteFiles: Boolean = split.deleteFilePaths.isNotEmpty()
        val hasInlinedDeletes: Boolean = split.inlinedDeletedRowPositions.isNotEmpty()
        val hasSnapshotFilter: Boolean = split.snapshotFilterMax != null
        if (!hasDeleteFiles && !hasInlinedDeletes && !hasSnapshotFilter) {
            return dataSource
        }

        // Two delete vocabularies, kept apart: GLOBAL row ids (Trino-written `row_id` delete
        // files, value = rowIdStart + file position) and FILE-LOCAL offsets (DuckLake-spec
        // `(file_path, pos)` delete files, puffin deletion vectors, and inlined deletes from
        // ducklake_inlined_delete_<tableId>). Each value must be matched under exactly ONE
        // interpretation: merging into a single both-ways-checked set phantom-deletes rows
        // whenever the file's rowIdStart < recordCount, because global ids then numerically
        // alias local offsets (caught by AbstractDucklakeRowLevelFormatTest's spread-delete).
        val localOffsets = DucklakeLongSet.copyOf(split.inlinedDeletedRowPositions)
        val globalRowIds = DucklakeLongSet()
        // Partial (cross-snapshot compacted) data file: drop file-local positions whose
        // _ducklake_internal_snapshot_id exceeds the read snapshot — same file-local-offset
        // vocabulary as DuckLake-spec deletes, so they merge into localOffsets.
        split.snapshotFilterMax?.let { filterMax ->
            localOffsets.addAll(DucklakeDeleteFileReader.readSnapshotDropPositions(
                    fileSystem, split.dataFilePath, split.footerSize, filterMax,
                    parquetReaderOptions, fileFormatDataSourceStats))
        }
        for (deleteFilePath in split.deleteFilePaths) {
            if (DucklakeReadSupport.isPuffinPath(deleteFilePath)) {
                val inputFile = fileSystem.newInputFile(DucklakeReadSupport.toLocation(deleteFilePath))
                // Consolidated ("partial") puffin file holding deletions newer than this read →
                // apply only the per-blob deletion vectors whose ducklake-snapshot-id <= the read
                // snapshot. A non-partial file (no snapshot filter) reads the full union.
                val snapshotFilter: Long? = split.deleteFileSnapshotFilters[deleteFilePath]
                localOffsets.addAll(
                        if (snapshotFilter != null) {
                            DucklakePuffinDeleteReader.readDeletedPositions(inputFile, snapshotFilter)
                        }
                        else {
                            DucklakePuffinDeleteReader.readDeletedPositions(inputFile)
                        })
            }
            else {
                val positions = readDeletedRowsFromFile(fileSystem, deleteFilePath, split)
                if (positions.global) {
                    globalRowIds.addAll(positions.values)
                }
                else {
                    localOffsets.addAll(positions.values)
                }
            }
        }

        if (globalRowIds.isEmpty() && localOffsets.isEmpty()) {
            return dataSource
        }

        log.debug("Applying deletions to data file %s: %d parquet delete file(s), %d inlined deletes, %d global + %d local deleted rows",
                split.dataFilePath,
                split.deleteFilePaths.size,
                split.inlinedDeletedRowPositions.size,
                globalRowIds.size,
                localOffsets.size)
        val transform = DeleteRowFilterTransform(globalRowIds, localOffsets, split.rowIdStart)
        return DucklakeRetainedPageSource(TransformConnectorPageSource.create(dataSource, transform), transform.retainedSizeInBytes)
    }

    private fun readDeletedRowsFromFile(fileSystem: TrinoFileSystem, deleteFilePath: String, split: DucklakeSplit): DucklakeDeleteFileReader.DeletePositions
    {
        // Delete files carry their own footer_size in ducklake_delete_file.
        val deleteFooterHint: Long = split.deleteFileFooterSizes.getOrDefault(deleteFilePath, 0L)
        // Consolidated ("partial") delete file: apply only the deletions recorded at or before the
        // read snapshot (filter by _ducklake_internal_snapshot_id).
        val snapshotFilter: Long? = split.deleteFileSnapshotFilters[deleteFilePath]
        if (snapshotFilter != null) {
            return DucklakeDeleteFileReader.readPositionsWithSnapshotFilter(
                    fileSystem,
                    deleteFilePath,
                    deleteFooterHint,
                    snapshotFilter,
                    parquetReaderOptions,
                    fileFormatDataSourceStats)
        }
        return DucklakeDeleteFileReader.readPositions(
                fileSystem,
                deleteFilePath,
                deleteFooterHint,
                parquetReaderOptions,
                fileFormatDataSourceStats)
    }

    private companion object {
        val log: Logger = Logger.get(DucklakeSplitDeleteFilter::class.java)
    }
}

/**
 * Drops tombstoned positions from each page. Two delete vocabularies apply to a split and
 * each value is matched ONLY under its own interpretation: [globalRowIds] holds global row
 * ids (`rowIdStart + file position`; Trino-written `row_id` delete files) and [localOffsets]
 * holds file-local row offsets (DuckLake-spec `pos` delete files, puffin deletion vectors,
 * inlined deletes). Cross-matching one vocabulary against the other phantom-deletes rows
 * whenever `rowIdStart < recordCount`, because the two numeric ranges overlap.
 */
internal class DeleteRowFilterTransform(
        globalRowIds: Set<Long>,
        localOffsets: Set<Long>,
        private val rowIdStart: Long?) : Function<SourcePage, SourcePage>
{
    private val globalRowIds = DucklakeLongSet.copyOf(globalRowIds)
    private val localOffsets = DucklakeLongSet.copyOf(localOffsets)
    val retainedSizeInBytes: Long get() = 64L + globalRowIds.getRetainedSizeInBytes() + localOffsets.getRetainedSizeInBytes()
    private var nextRowOffset: Long = 0

    override fun apply(page: SourcePage): SourcePage
    {
        val positionCount: Int = page.positionCount
        val retainedPositions = IntArray(positionCount)
        var retainedCount = 0
        val hasGlobal = globalRowIds.isNotEmpty()
        val start = if (hasGlobal) DucklakeRowIdentity.requireStart(rowIdStart) else 0L

        for (position in 0 until positionCount) {
            val rowOffset: Long = nextRowOffset + position
            val globallyDeleted = hasGlobal && globalRowIds.containsLong(Math.addExact(start, rowOffset))
            if (!globallyDeleted && !localOffsets.containsLong(rowOffset)) {
                retainedPositions[retainedCount] = position
                retainedCount++
            }
        }
        nextRowOffset += positionCount

        if (retainedCount == positionCount) {
            return page
        }
        page.selectPositions(retainedPositions, 0, retainedCount)
        return page
    }
}
