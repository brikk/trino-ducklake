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

import io.airlift.slice.Slices
import io.trino.spi.Page
import io.trino.spi.block.Block
import io.trino.spi.block.RunLengthEncodedBlock
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.SourcePage
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TypeUtils.writeNativeValue
import io.trino.spi.type.VarcharType.VARCHAR
import java.io.IOException

/**
 * One data file to read for the change feed. [baseSource] lazily opens the connector's normal
 * data-file read pipeline projecting exactly [ChangeFeedPageSource]'s requested data columns
 * followed by `$file_row_number` (or the actual row ID for an inlined source), reading as of the
 * feed's END-snapshot schema.
 *
 * - [snapshotId] is the snapshot to report for every row of this unit (the file's `begin_snapshot`
 *   for an insert unit; the deletion's snapshot for a delete unit).
 * - [keepPositions] = null keeps every row (insert unit); otherwise only rows at the given FILE
 *   positions are emitted (delete unit — the positions newly deleted at [snapshotId]).
 * - The emitted `rowid` is [lineageRowIds]`[position]` when the file carries DuckLake's embedded
 *   row-lineage column (UPDATE/compaction output — see [DucklakeDeleteFileReader.readInternalRowIds]),
 *   else `rowIdStart + position`. Using the preserved rowid is what lets an UPDATE's delete and
 *   re-insert land on the SAME rowid in one snapshot and pair into update pre/post-image.
 * - [updatedRowids] are the `rowid`s at [snapshotId] that are BOTH deleted and re-inserted — i.e.
 *   updates. Their `change_type` becomes [ChangeFeedColumns.CHANGE_UPDATE_POSTIMAGE] on the insert
 *   side and [ChangeFeedColumns.CHANGE_UPDATE_PREIMAGE] on the delete side; all others get the
 *   plain [insert] / [delete] change type. (Trino-written UPDATEs pair too when the write ran with
 *   the `write_row_lineage` session property — the merge sink then embeds the original rowid; see
 *   DucklakeMergeSink. Without it they surface as delete+insert, a faithful description of Trino's
 *   delete-then-insert UPDATE under fresh rowids.)
 */
class ChangeFeedUnit @JvmOverloads constructor(
        val baseSource: () -> ConnectorPageSource,
        val snapshotId: Long,
        val rowIdStart: Long?,
        val lineageRowIds: LongArray?,
        keepPositions: Set<Long>?,
        updatedRowids: Set<Long>,
        val isDelete: Boolean,
        /** Whole-source rows visited by this unit, including rows rejected by keepPositions. */
        val physicalRows: Long = 0,
        /** Physical versions born and deleted in one snapshot, not other versions of that row ID. */
        excludedPositions: Set<Long> = emptySet()) {
    val keepPositions: DucklakeLongSet? = keepPositions?.let { if (it is DucklakeLongSet) it else DucklakeLongSet.copyOf(it) }
    val updatedRowids: DucklakeLongSet = if (updatedRowids is DucklakeLongSet) updatedRowids else DucklakeLongSet.copyOf(updatedRowids)
    val excludedPositions: DucklakeLongSet = if (excludedPositions is DucklakeLongSet) excludedPositions else DucklakeLongSet.copyOf(excludedPositions)
}

/**
 * Streams the DuckLake change feed as `snapshot_id, rowid[, change_type]` + table columns.
 * Reads each [ChangeFeedUnit]'s data file through the connector's ordinary read pipeline, then
 * per page: derives each row's FILE position from
 * `$file_row_number`, filters to the deleted positions (delete units), computes the true
 * `rowid` (embedded lineage column when present, else positional), stamps `snapshot_id`, and
 * classifies `change_type` (pairing an UPDATE's delete + re-insert of the same rowid in one
 * snapshot into update pre/post-image).
 *
 * The data columns are read in [dataColumns] order followed by the physical-position column at
 * channel `dataColumns.size`; output blocks are projected in [requestedColumns] order.
 */
class ChangeFeedPageSource(
        units: List<ChangeFeedUnit>,
        private val requestedColumns: List<DucklakeColumnHandle>,
        private val dataColumns: List<DucklakeColumnHandle>,
        private val emitChangeType: Boolean,
        private val retainedFileBytes: Long = 0,
        private val releaseFiles: () -> Unit = {}) : ConnectorPageSource {
    private val units = units.toMutableList()
    private val rowIdChannel: Int = dataColumns.size
    private val dataChannelByColumnId: Map<Long, Int> = dataColumns.withIndex()
            .associate { (i, col) -> col.columnId to i }

    private var unitIndex: Int = 0
    private var currentUnit: ChangeFeedUnit? = null
    private var currentSource: ConnectorPageSource? = null
    private var finished: Boolean = false
    private var completedBytes: Long = 0
    private var readTimeNanos: Long = 0
    private val retainedUnitBytes = retainedUnitBytes()
    private var closed = false

    override fun getCompletedBytes(): Long = completedBytes

    override fun getReadTimeNanos(): Long = readTimeNanos

    override fun isFinished(): Boolean = finished

    override fun getMemoryUsage(): Long = if (closed) 0 else retainedFileBytes + retainedUnitBytes + (currentSource?.memoryUsage ?: 0L)

    private fun retainedUnitBytes(): Long {
        val seen = java.util.Collections.newSetFromMap(java.util.IdentityHashMap<Any, Boolean>())
        return units.sumOf { unit ->
            128L + listOfNotNull(unit.keepPositions, unit.updatedRowids, unit.excludedPositions, unit.lineageRowIds).sumOf { value ->
                if (!seen.add(value)) 0L else when (value) {
                    is DucklakeLongSet -> value.getRetainedSizeInBytes()
                    is LongArray -> 16L + 8L * value.size
                    else -> 0L
                }
            }
        }
    }

    override fun getNextSourcePage(): SourcePage? {
        while (!finished) {
            val source: ConnectorPageSource = currentSource ?: run {
                if (unitIndex >= units.size) {
                    finished = true
                    close()
                    return null
                }
                currentUnit = units[unitIndex]
                units[unitIndex].baseSource().also { currentSource = it }
            }

            val page: SourcePage? = source.nextSourcePage
            if (page == null) {
                completedBytes += source.completedBytes
                readTimeNanos += source.readTimeNanos
                source.close()
                currentSource = null
                currentUnit = null
                unitIndex++
                continue
            }
            val transformed: SourcePage? = transform(currentUnit!!, page)
            if (transformed != null) {
                return transformed
            }
        }
        return null
    }

    private fun transform(unit: ChangeFeedUnit, page: SourcePage): SourcePage? {
        val positionCount: Int = page.positionCount
        // The trailing read column is $file_row_number: the raw FILE POSITION
        // (delete-survivor rows keep their original positions).
        val filePositionBlock: Block = page.getBlock(rowIdChannel)

        val keptIdx = IntArray(positionCount)
        val keptRowIds = LongArray(positionCount)
        val keptCount = fillKeptRows(unit, filePositionBlock, keptIdx, keptRowIds)
        if (keptCount == 0) {
            return null
        }
        val selectAll: Boolean = unit.keepPositions == null && keptCount == positionCount

        val blocks: Array<Block?> = arrayOfNulls(requestedColumns.size)
        for ((outIndex, column) in requestedColumns.withIndex()) {
            blocks[outIndex] = when (column.columnId) {
                ChangeFeedColumns.SNAPSHOT_ID.columnId ->
                    RunLengthEncodedBlock.create(writeNativeValue(BIGINT, unit.snapshotId), keptCount)
                ChangeFeedColumns.ROWID.columnId -> rowIdBlock(keptRowIds, keptCount)
                ChangeFeedColumns.CHANGE_TYPE.columnId -> changeTypeBlock(unit, keptRowIds, keptCount)
                else -> {
                    val channel: Int = dataChannelByColumnId[column.columnId]
                            ?: throw IllegalStateException("Change-feed column not in read plan: ${column.columnName}")
                    val block: Block = page.getBlock(channel)
                    if (selectAll) block else block.getPositions(keptIdx, 0, keptCount)
                }
            }
        }
        @Suppress("UNCHECKED_CAST")
        return SourcePage.create(Page(keptCount, *(blocks as Array<Block>)))
    }

    private fun keep(unit: ChangeFeedUnit, position: Long): Boolean =
        !unit.excludedPositions.containsLong(position) && (unit.keepPositions == null || unit.keepPositions.containsLong(position))

    private fun fillKeptRows(unit: ChangeFeedUnit, positions: Block, indexes: IntArray, rowIds: LongArray): Int {
        var count = 0
        for (index in indexes.indices) {
            if (keep(unit, BIGINT.getLong(positions, index))) indexes[count++] = index
        }
        if (count == 0) return 0
        val start = if (unit.lineageRowIds == null) DucklakeRowIdentity.requireStart(unit.rowIdStart) else 0L
        for (index in 0 until count) {
            val position = BIGINT.getLong(positions, indexes[index])
            rowIds[index] = if (unit.lineageRowIds == null) Math.addExact(start, position)
                else DucklakeRowIdentity.rowId(unit.lineageRowIds, null, position)
        }
        return count
    }

    private fun rowIdBlock(keptRowIds: LongArray, keptCount: Int): Block {
        val builder = BIGINT.createBlockBuilder(null, keptCount)
        for (j in 0 until keptCount) {
            BIGINT.writeLong(builder, keptRowIds[j])
        }
        return builder.build()
    }

    private fun changeTypeBlock(unit: ChangeFeedUnit, keptRowIds: LongArray, keptCount: Int): Block {
        val updatedLabel = if (unit.isDelete) UPDATE_PREIMAGE else UPDATE_POSTIMAGE
        val plainLabel = if (unit.isDelete) DELETE else INSERT
        val builder = VARCHAR.createBlockBuilder(null, keptCount)
        for (j in 0 until keptCount) {
            val label = if (unit.updatedRowids.containsLong(keptRowIds[j])) updatedLabel else plainLabel
            VARCHAR.writeSlice(builder, label)
        }
        return builder.build()
    }

    init {
        require(emitChangeType || requestedColumns.none { it.columnId == ChangeFeedColumns.CHANGE_TYPE.columnId }) {
            "change_type requested but this feed does not emit it"
        }
    }

    @Throws(IOException::class)
    override fun close() {
        try { currentSource?.close() }
        finally {
            currentSource = null
            currentUnit = null
            units.clear()
            releaseFiles()
            finished = true
            closed = true
        }
    }

    companion object {
        private val INSERT = Slices.utf8Slice(ChangeFeedColumns.CHANGE_INSERT)
        private val DELETE = Slices.utf8Slice(ChangeFeedColumns.CHANGE_DELETE)
        private val UPDATE_PREIMAGE = Slices.utf8Slice(ChangeFeedColumns.CHANGE_UPDATE_PREIMAGE)
        private val UPDATE_POSTIMAGE = Slices.utf8Slice(ChangeFeedColumns.CHANGE_UPDATE_POSTIMAGE)
    }
}
