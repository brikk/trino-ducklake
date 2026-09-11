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
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.trino.plugin.DucklakeMergeTableHandle.DataFileRange
import io.airlift.json.JsonCodec
import io.airlift.log.Logger
import io.airlift.slice.Slice
import io.airlift.slice.Slices
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.parquet.ParquetReaderOptions
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.spi.Page
import io.trino.spi.block.Block
import io.trino.spi.connector.ConnectorMergeSink
import io.trino.spi.connector.ConnectorPageSink
import io.trino.spi.connector.MergePage
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType.VARCHAR
import org.apache.parquet.format.CompressionCodec
import java.io.IOException
import java.io.OutputStream
import java.io.UncheckedIOException
import java.util.TreeSet
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture

/**
 * Merge sink for DuckLake DELETE/UPDATE/MERGE operations.
 * Collects deleted row IDs grouped by data file, writes DuckLake-spec parquet delete files —
 * `(file_path, pos)` with FILE-LOCAL positions, the shape DuckDB's reader requires — and
 * delegates inserts to the standard DucklakePageSink.
 */
open class DucklakeMergeSink(
        private val mergeHandle: DucklakeMergeTableHandle,
        private val fileSystem: TrinoFileSystem,
        private val deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment>,
        private val writerOptions: ParquetWriterOptions,
        private val parquetReaderOptions: ParquetReaderOptions,
        private val fileFormatDataSourceStats: FileFormatDataSourceStats,
        private val trinoVersion: String,
        // When true, tombstones are written as DuckLake puffin deletion-vector (.puffin) files
        // instead of parquet (file_path, pos) delete files (session write_deletion_vectors).
        private val writeDeletionVectors: Boolean,
        // Insert sink for handling UPDATE inserts
        private val insertSink: ConnectorPageSink,
        /**
         * Lineage-preserving mode (session `write_row_lineage`, parquet-only):
         * UPDATE-rewritten rows are routed to this second sink with one extra
         * trailing BIGINT channel carrying each row's ORIGINAL rowid, which the
         * sink embeds as `_ducklake_internal_row_id` (field-id 2147483540).
         * MERGE-inserted (not-matched) rows keep going to [insertSink] — the
         * lineage column must be non-null for EVERY row of a carrying file
         * (readers reject malformed lineage), so the two row kinds
         * cannot share a file. Null = lineage disabled by the legacy opt-out.
         */
        private val lineageInsertSink: ConnectorPageSink? = null)
    : ConnectorMergeSink
{
    private val dataColumnCount: Int = mergeHandle.insertHandle.columns.size

    // Accumulated physical delete positions grouped by data file ID.
    private val deletesByDataFile: MutableMap<Long, MutableList<Long>> = mutableMapOf()

    // Logical row IDs can be non-contiguous or overlap across versions. Mutation ownership is
    // physical and resolves directly by data_file_id, without synthesizing catalog row-ID ranges.
    private val filesById: Map<Long, DataFileRange> = mergeHandle.dataFileRanges.associateBy { it.dataFileId }

    override fun storeMergedRows(page: Page) {
        val mergePage: MergePage = MergePage.createDeleteAndInsertPages(page, dataColumnCount)

        mergePage.deletionsPage.ifPresent { p -> this.processDeletes(p) }
        if (lineageInsertSink == null) {
            mergePage.insertionsPage.ifPresent { insertPage -> insertSink.appendPage(insertPage) }
            return
        }
        routeInsertsWithLineage(page, lineageInsertSink)
    }

    /**
     * Splits insert rows by kind on the RAW merge page (data channels…, TINYINT
     * operation, INT case number, rowId last): Trino's merge processor emits an
     * UPDATE as delete + insert rows. Trino 483 copies the source row ID onto BOTH rows
     * (`DeleteAndInsertMergeProcessor.addInsertRow`), precisely so connector sinks can preserve
     * lineage after an update-layout exchange reorders/splits pages. Read it from each
     * UPDATE_INSERT directly; never rely on adjacency. Plain not-matched inserts carry a null row
     * ID and go to the ordinary insert sink.
     */
    private fun routeInsertsWithLineage(page: Page, lineageSink: ConnectorPageSink) {
        val opBlock: Block = page.getBlock(dataColumnCount)
        val rowIdBlock: Block = page.getBlock(page.channelCount - 1)
        val plainPositions = mutableListOf<Int>()
        val updatePositions = mutableListOf<Int>()
        val lineage = mutableListOf<Long>()
        for (position in 0 until page.positionCount) {
            when (TINYINT.getByte(opBlock, position).toInt()) {
                ConnectorMergeSink.INSERT_OPERATION_NUMBER -> plainPositions.add(position)
                ConnectorMergeSink.UPDATE_INSERT_OPERATION_NUMBER -> {
                    check(!rowIdBlock.isNull(position)) { "update-insert row has no source row ID" }
                    val locator = DucklakeMergeRowId.read(rowIdBlock, position)
                    updatePositions.add(position)
                    // Resolve the logical identity separately so chained updates preserve it.
                    lineage.add(resolveTrueRowId(locator))
                }
                else -> Unit // DELETE / UPDATE_DELETE rows: tombstones only, handled via processDeletes
            }
        }
        if (plainPositions.isNotEmpty()) {
            insertSink.appendPage(dataOnlyPage(page, plainPositions))
        }
        if (updatePositions.isNotEmpty()) {
            lineageSink.appendPage(withLineageChannel(dataOnlyPage(page, updatePositions), lineage))
        }
    }

    /** Per-source-file embedded lineage (null = none), loaded lazily for chained-update resolution. */
    private val lineageBySourceFile = mutableMapOf<Long, LongArray?>()

    /**
     * Resolves the physical mutation locator to the row's logical DuckLake id:
     * when the source data file carries the embedded lineage column (written by a
     * prior lineage UPDATE or compaction), the value at that file position wins —
     * so chained updates keep the ORIGINAL id forever, matching DuckDB.
     */
    private fun resolveTrueRowId(locator: DucklakeMergeRowId.Locator): Long {
        val file = sourceFile(locator)
        // Cache absence too: nullable getOrPut would reopen a file with no lineage for every row.
        if (!lineageBySourceFile.containsKey(file.dataFileId)) {
            lineageBySourceFile[file.dataFileId] = try {
                DucklakeDeleteFileReader.readInternalRowIds(
                        fileSystem, file.dataFilePath, 0L, parquetReaderOptions, fileFormatDataSourceStats)
            }
            catch (e: IOException) {
                throw UncheckedIOException("Failed to read row lineage from ${file.dataFilePath}", e)
            }
        }
        return DucklakeRowIdentity.rowId(lineageBySourceFile[file.dataFileId], file.rowIdStart, locator.position)
    }

    private fun dataOnlyPage(page: Page, positions: List<Int>): Page {
        val positionArray = positions.toIntArray()
        val blocks = Array(dataColumnCount) { channel ->
            page.getBlock(channel).getPositions(positionArray, 0, positionArray.size)
        }
        return Page(positionArray.size, *blocks)
    }

    private fun withLineageChannel(dataPage: Page, lineage: List<Long>): Page {
        val builder = BIGINT.createBlockBuilder(null, lineage.size)
        lineage.forEach { BIGINT.writeLong(builder, it) }
        val lineageBlock = builder.build()
        val blocks = Array(dataPage.channelCount + 1) { channel ->
            if (channel < dataPage.channelCount) dataPage.getBlock(channel) else lineageBlock
        }
        return Page(dataPage.positionCount, *blocks)
    }

    private fun processDeletes(deletePage: Page) {
        // Delete page has data columns followed by row ID column (last column)
        val rowIdChannel = deletePage.channelCount - 1
        val rowIdBlock: Block = deletePage.getBlock(rowIdChannel)

        var position = 0
        while (position < deletePage.positionCount) {
            val locator = DucklakeMergeRowId.read(rowIdBlock, position)
            sourceFile(locator) // Reject unknown files or out-of-file positions before writing.
            deletesByDataFile.getOrPut(locator.dataFileId) { mutableListOf() }.add(locator.position)
            position++
        }
    }

    private fun sourceFile(locator: DucklakeMergeRowId.Locator): DataFileRange {
        val file = filesById[locator.dataFileId]
                ?: throw IllegalStateException("Unknown DuckLake mutation data file: ${locator.dataFileId}")
        if (locator.position >= file.recordCount) {
            throw IllegalStateException("DuckLake mutation position outside data file: $locator")
        }
        return file
    }

    override fun finish(): CompletableFuture<Collection<Slice>> {
        val fragments: MutableList<Slice> = mutableListOf()

        // Write delete Parquet files
        for (entry in deletesByDataFile.entries) {
            val dataFileId = entry.key
            val rowIds: List<Long> = entry.value

            if (rowIds.isEmpty()) {
                continue
            }

            try {
                val fragment = writeDeleteFile(dataFileId, rowIds)
                fragments.add(Slices.wrappedBuffer(*deleteFragmentCodec.toJsonBytes(fragment)))
            }
            catch (e: IOException) {
                throw UncheckedIOException("Failed to write delete file for data file $dataFileId", e)
            }
        }

        // Collect insert fragments. They are already DucklakeWriteFragment JSON — the insert
        // DucklakePageSink serialized them with that codec — and finishMerge distinguishes
        // delete from insert fragments structurally (delete-fragment trial-parse plus a
        // "ducklake-delete-" path-prefix check), not by any tag. So they pass through
        // unchanged; the prior deserialize-then-reserialize was a byte-for-byte no-op.
        try {
            val insertFragments: Collection<Slice> = insertSink.finish().get()
            fragments.addAll(insertFragments)
            if (lineageInsertSink != null) {
                fragments.addAll(lineageInsertSink.finish().get())
            }
        }
        catch (e: Exception) {
            throw RuntimeException("Failed to finish insert sink", e)
        }

        val asCollection: Collection<Slice> = fragments
        return completedFuture(asCollection)
    }

    @Throws(IOException::class)
    private fun writeDeleteFile(dataFileId: Long, rowIds: List<Long>): DucklakeDeleteFragment {
        // B3a: read any prior-active delete files for this data_file_id (paths captured on the
        // merge handle at beginMerge time) and UNION their positions with this commit's new
        // deletes. The catalog end-snapshots the prior files in the same transaction we insert
        // this new one (JdbcDucklakeCatalog.applyDeleteFragments), so the new file must carry
        // every still-deleted position or rows resurrect. record_count is decremented by
        // newDeleteCount (positions truly new this commit), since the prior file's positions
        // were already deducted when it was first committed.
        //
        // Everything is normalized to FILE-LOCAL positions: this sink writes DuckLake-spec
        // `(file_path, pos)` delete files — the only shape DuckDB's reader accepts (it rejects
        // the connector's legacy single-column `row_id` files outright, which made every
        // Trino-deleted table unreadable cross-engine until TestDucklakeCrossEngineTrinoDeleteRead
        // caught it). Legacy `row_id` files carry GLOBAL ids and are rebased by rowIdStart.
        val range: DataFileRange = checkNotNull(findDataFileRange(dataFileId)) {
            "No data file range for data file $dataFileId"
        }
        // Upstream's delete writer uses std::set, and its reader rejects positions that are not
        // strictly increasing. A LinkedHashSet preserved "prior file, then new arrival order", so
        // deleting a lower position in a later statement produced e.g. [4, 1] and made DuckDB
        // refuse the table. TreeSet also deduplicates pathological input while enforcing the wire
        // invariant for both Parquet and Puffin writers.
        val unionPositions: TreeSet<Long> = TreeSet()
        for (existingPath in range.existingDeleteFilePaths) {
            unionPositions.addAll(readPriorPositions(existingPath, range))
        }
        // rowIds are disjoint from prior positions by engine invariant: the SELECT phase of
        // DELETE/UPDATE/MERGE only sees rows that aren't already tombstoned, so the engine
        // never issues a delete for a position that's in any prior active delete file. We
        // still go through a set to defend against pathological input rather than relying on
        // engine semantics for correctness of the size math.
        val preNewSize: Long = unionPositions.size.toLong()
        unionPositions.addAll(rowIds)
        val totalPositions: Long = unionPositions.size.toLong()
        val newDeleteCount: Long = totalPositions - preNewSize

        return if (writeDeletionVectors) {
            writePuffinDeleteFile(dataFileId, unionPositions, totalPositions, newDeleteCount)
        }
        else {
            writeParquetDeleteFile(dataFileId, range, unionPositions, totalPositions, newDeleteCount)
        }
    }

    /** Read a prior active delete file's positions as FILE-LOCAL offsets (handles parquet + puffin). */
    @Throws(IOException::class)
    private fun readPriorPositions(existingPath: String, range: DataFileRange): Set<Long> {
        if (existingPath.endsWith(".puffin", ignoreCase = true)) {
            return DucklakePuffinDeleteReader.readDeletedPositions(fileSystem.newInputFile(toLocation(existingPath)))
        }
        val prior = DucklakeDeleteFileReader.readPositions(
                fileSystem, existingPath, 0L, parquetReaderOptions, fileFormatDataSourceStats)
        // Legacy `row_id` files carry GLOBAL ids → rebase to file-local; spec/puffin are already local.
        return if (prior.global) {
            val start = DucklakeRowIdentity.requireStart(range.rowIdStart)
            prior.values.mapTo(TreeSet()) { it - start }
        } else prior.values
    }

    /** Write the union positions as a DuckLake puffin deletion-vector (bare blob) delete file. */
    @Throws(IOException::class)
    private fun writePuffinDeleteFile(
            dataFileId: Long,
            unionPositions: Set<Long>,
            totalPositions: Long,
            newDeleteCount: Long): DucklakeDeleteFragment {
        val fileName = "ducklake-delete-" + UUID.randomUUID() + ".puffin"
        val filePath: Location = Location.of(mergeHandle.insertHandle.tableDataPath).appendPath(fileName)
        val blob: ByteArray = DucklakePuffinDeleteWriter.encodeBlob(unionPositions)
        fileSystem.newOutputFile(filePath).create().use { out -> out.write(blob) }
        log.debug("Wrote puffin delete file %s with %d positions (%d new) for data file %d",
                filePath, totalPositions, newDeleteCount, dataFileId)
        // footer_size = 0: a puffin file has no parquet footer (the reader reads the whole file);
        // the catalog maps 0 to SQL NULL.
        return DucklakeDeleteFragment(dataFileId, fileName, totalPositions, blob.size.toLong(), 0L, newDeleteCount, "puffin")
    }

    @Throws(IOException::class)
    private fun writeParquetDeleteFile(
            dataFileId: Long,
            range: DataFileRange,
            unionPositions: Set<Long>,
            totalPositions: Long,
            newDeleteCount: Long): DucklakeDeleteFragment {
        val fileName = "ducklake-delete-" + UUID.randomUUID() + ".parquet"
        val tableDataPath: String = mergeHandle.insertHandle.tableDataPath
        val filePath: Location = Location.of(tableDataPath).appendPath(fileName)

        val outputFile = fileSystem.newOutputFile(filePath)
        val outputStream: OutputStream = outputFile.create()

        val columnNames: List<String> = ImmutableList.of(
                DucklakeDeleteFileReader.SPEC_FILE_PATH_COLUMN,
                DucklakeDeleteFileReader.SPEC_POSITION_COLUMN)
        val columnTypes: List<Type> = ImmutableList.of(VARCHAR, BIGINT)

        val schemaConverter = ParquetSchemaConverter(
                columnTypes, columnNames, false, false)

        val parquetWriter = ParquetWriter(
                outputStream,
                schemaConverter.messageType,
                schemaConverter.primitiveTypes,
                writerOptions,
                CompressionCodec.ZSTD,
                trinoVersion,
                java.util.Optional.empty(),
                java.util.Optional.empty())

        // Write all positions (prior ∪ new) as a single page. close() must be called even on
        // write failure or both the writer and the wrapped output stream leak.
        val fileMetaData: org.apache.parquet.format.FileMetaData
        val fileSize: Long
        try {
            val dataFilePathSlice: Slice = Slices.utf8Slice(range.dataFilePath)
            val pathBuilder: io.trino.spi.block.BlockBuilder = VARCHAR.createBlockBuilder(null, totalPositions.toInt())
            val posBuilder: io.trino.spi.block.BlockBuilder = BIGINT.createBlockBuilder(null, totalPositions.toInt())
            for (position in unionPositions) {
                VARCHAR.writeSlice(pathBuilder, dataFilePathSlice)
                BIGINT.writeLong(posBuilder, position)
            }
            val pathBlock: Block = pathBuilder.build()
            val posBlock: Block = posBuilder.build()
            parquetWriter.write(Page(pathBlock.positionCount, pathBlock, posBlock))
            parquetWriter.close()
            fileMetaData = parquetWriter.getFileMetaData()
            fileSize = parquetWriter.estimatedWrittenBytes
        }
        catch (t: Throwable) {
            try { parquetWriter.close() } catch (_: Exception) {}
            throw t
        }

        log.debug("Wrote parquet delete file %s with %d total positions (%d new this commit) for data file %d",
                filePath, totalPositions, newDeleteCount, dataFileId)

        return DucklakeDeleteFragment(
                dataFileId,
                fileName,
                totalPositions,
                fileSize,
                footerSizeOf(fileMetaData),
                newDeleteCount,
                "parquet")
    }

    private fun footerSizeOf(fileMetaData: org.apache.parquet.format.FileMetaData): Long =
        try {
            val footerOutput = io.airlift.slice.DynamicSliceOutput(40)
            org.apache.parquet.format.Util.writeFileMetaData(fileMetaData, footerOutput)
            footerOutput.size().toLong()
        }
        catch (e: IOException) {
            0
        }

    /** Resolve a delete-file path to a Location, wrapping a bare local path as a file URI.
     * Prefix file:// on the RAW path (not Path.toUri(), which percent-encodes and would
     * double-encode paths already carrying literal %XX escapes); Trino's Location does not
     * percent-decode. */
    private fun toLocation(path: String): Location {
        val location = Location.of(path)
        return if (location.scheme().isPresent) location else Location.of("file://" + path)
    }

    private fun findDataFileRange(dataFileId: Long): DataFileRange? =
            filesById[dataFileId]

    override fun abort() {
        lineageInsertSink?.abort()
        insertSink.abort()
        // Delete files written during this operation will become orphans —
        // cleaned up by DuckLake's maintenance procedures
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeMergeSink::class.java)
    }
}
