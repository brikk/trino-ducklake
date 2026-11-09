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
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.block.BlockBuilder
import io.trino.spi.block.RunLengthEncodedBlock
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.SourcePage
import io.trino.spi.metrics.Metrics
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TypeUtils.writeNativeValue
import io.trino.spi.type.VarcharType.VARCHAR
import java.io.IOException
import java.util.OptionalLong
import java.util.concurrent.CompletableFuture

/** Injection of the hidden virtual columns into data-file page sources. */
internal object DucklakeVirtualColumns {
    /**
     * Single-position constant block for a virtual column on a file-backed split
     * (parquet): $path = the data file path, $snapshot_id = the file's
     * begin_snapshot. The injecting page source RLE-expands it to each page.
     */
    fun dataFileVirtualBlock(kind: VirtualKind, split: DucklakeSplit): Block = when (kind) {
        VirtualKind.PATH -> writeNativeValue(VARCHAR, Slices.utf8Slice(split.dataFilePath))
        VirtualKind.SNAPSHOT_ID -> writeNativeValue(BIGINT, split.beginSnapshot)
        VirtualKind.FILE_SIZE_BYTES -> writeNativeValue(BIGINT, split.fileSizeBytes)
        else -> throw TrinoException(NOT_SUPPORTED, "Virtual column not yet supported on the read path: ${kind.columnName}")
    }

    /**
     * Wrap [delegate] so the virtual columns selected by [wrapKind] are injected as constant
     * RLE blocks at their requested output positions. Only those kinds are wrapped here; any
     * other virtuals in [requestedColumns] are assumed to be produced by [delegate] (e.g. the
     * positional virtuals injected in-pipeline on file-backed splits) and consume a delegate
     * channel. The relative order of delegate-produced columns matches their order in
     * [requestedColumns]. Returns the delegate unchanged when nothing needs wrapping.
     */
    fun injectConstantVirtuals(
            delegate: ConnectorPageSource,
            requestedColumns: List<DucklakeColumnHandle>,
            wrapKind: (VirtualKind) -> Boolean,
            blockForKind: (VirtualKind) -> Block): ConnectorPageSource
    {
        val constantBlocks: Map<Int, Block> = requestedColumns.withIndex()
                .mapNotNull { (i, col) ->
                    val kind: VirtualKind? = col.virtualKind()
                    if (kind != null && wrapKind(kind)) i to blockForKind(kind) else null
                }
                .toMap()
        if (constantBlocks.isEmpty()) {
            return delegate
        }
        return VirtualColumnInjectingPageSource(delegate, requestedColumns.size, constantBlocks)
    }

    /**
     * Classify a requested column as a positional injection, or not.
     * Returns true  -> row identity: the MERGE locator (-100) or logical $row_id (-104).
     * Returns false -> $file_row_number (value = file position, 0-based): the -103 virtual.
     * Returns null  -> not positional (real column, or a constant virtual).
     */
    fun positionalColumnKind(column: DucklakeColumnHandle): Boolean? {
        if (column.isRowIdColumn()) {
            return true
        }
        return when (column.virtualKind()) {
            VirtualKind.ROW_ID -> true
            VirtualKind.FILE_ROW_NUMBER -> false
            else -> null
        }
    }
}

/** One positional column to inject and its physical/logical identity semantics. */
internal class PositionalInjection(
    val outputPosition: Int,
    /**
     * True only for the queryable `$row_id` virtual (-104): when the file
     * carries the embedded lineage column, the injected value is the row's
     * PRESERVED rowid instead of `rowIdStart + position`.
     */
    val resolveLineage: Boolean = false,
    /** The MERGE channel (-100) is a ROW(data_file_id, physical position). */
    val mutationLocator: Boolean = false)

/** Pass-through of everything but the next page to a delegate page source. */
internal abstract class DelegatingDucklakePageSource(protected val delegate: ConnectorPageSource) : ConnectorPageSource
{
    override fun getCompletedBytes(): Long = delegate.completedBytes

    override fun getCompletedPositions(): OptionalLong = delegate.completedPositions

    override fun getReadTimeNanos(): Long = delegate.readTimeNanos

    override fun isFinished(): Boolean = delegate.isFinished

    override fun getMemoryUsage(): Long = delegate.memoryUsage

    override fun getMetrics(): Metrics = delegate.metrics

    override fun isBlocked(): CompletableFuture<*> = delegate.isBlocked()

    @Throws(IOException::class)
    override fun close() = delegate.close()

    /** Places [injected] blocks at their channels and the source page's blocks, in order, elsewhere. */
    protected fun assemble(sourcePage: SourcePage, totalChannels: Int, injected: (Int) -> Block?): SourcePage
    {
        val blocks = arrayOfNulls<Block>(totalChannels)
        var srcChannel = 0
        for (i in 0 until totalChannels) {
            blocks[i] = injected(i) ?: sourcePage.getBlock(srcChannel++)
        }
        @Suppress("UNCHECKED_CAST")
        return SourcePage.create(Page(sourcePage.positionCount, *(blocks as Array<Block>)))
    }
}

/**
 * Injects the physical MERGE locator, logical $row_id, and $file_row_number. The logical
 * identity uses embedded lineage first; only absent lineage uses a present catalog start.
 * All share the same per-page running offset. Must be applied BEFORE delete-file filtering
 * so the values match original file positions.
 */
internal class PositionalVirtualInjectingPageSource(
    delegate: ConnectorPageSource,
    private val totalChannels: Int,
    injections: List<PositionalInjection>,
    private val rowIdStart: Long?,
    /** Embedded lineage values by file position (null = file carries none). */
    private val lineage: LongArray? = null,
    private val dataFileId: Long? = null) : DelegatingDucklakePageSource(delegate)
{
    private val injections: List<PositionalInjection> = injections.toList()
    private var nextRowOffset: Long = 0

    override fun getNextSourcePage(): SourcePage?
    {
        val sourcePage: SourcePage = delegate.nextSourcePage ?: return null
        val positionCount: Int = sourcePage.positionCount

        val injectedBlocks: HashMap<Int, Block> = HashMap(injections.size * 2)
        for (injection in injections) {
            val blockBuilder: BlockBuilder = BIGINT.createBlockBuilder(null, positionCount)
            if (injection.resolveLineage) {
                for (i in 0 until positionCount) {
                    BIGINT.writeLong(blockBuilder, DucklakeRowIdentity.rowId(lineage, rowIdStart, nextRowOffset + i))
                }
            }
            else {
                for (i in 0 until positionCount) {
                    BIGINT.writeLong(blockBuilder, nextRowOffset + i)
                }
            }
            val positions = blockBuilder.build()
            injectedBlocks[injection.outputPosition] = if (injection.mutationLocator) {
                DucklakeMergeRowId.block(checkNotNull(dataFileId) { "Missing data file ID for DuckLake mutation scan" }, positions)
            } else positions
        }
        nextRowOffset += positionCount

        return assemble(sourcePage, totalChannels) { injectedBlocks[it] }
    }
}

/**
 * Wraps a ConnectorPageSource and injects constant (per-split) virtual-column blocks at
 * fixed output positions, RLE-expanded to each page's position count. The delegate supplies
 * the non-virtual columns in order; [constantBlocks] maps an output channel index to its
 * single-position constant block. Used for the constant virtuals ($path, $snapshot_id). The
 * row-varying virtuals on file-backed splits are handled in-pipeline by
 * [PositionalVirtualInjectingPageSource] instead.
 */
internal class VirtualColumnInjectingPageSource(
    delegate: ConnectorPageSource,
    private val totalChannels: Int,
    private val constantBlocks: Map<Int, Block>) : DelegatingDucklakePageSource(delegate)
{
    override fun getNextSourcePage(): SourcePage?
    {
        val sourcePage: SourcePage = delegate.nextSourcePage ?: return null
        val positionCount: Int = sourcePage.positionCount
        return assemble(sourcePage, totalChannels) { channel ->
            constantBlocks[channel]?.let { RunLengthEncodedBlock.create(it, positionCount) }
        }
    }
}
