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

import com.google.common.collect.ImmutableMap
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import io.airlift.log.Logger
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.TrinoInputFile
import io.trino.memory.context.AggregatedMemoryContext
import io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext
import io.trino.parquet.Column
import io.trino.parquet.ParquetDataSource
import io.trino.parquet.ParquetDataSourceId
import io.trino.parquet.ParquetReaderOptions
import io.trino.parquet.ParquetTypeUtils.getColumnIO
import io.trino.parquet.ParquetTypeUtils.getDescriptors
import io.trino.parquet.metadata.ParquetMetadata
import io.trino.parquet.predicate.PredicateUtils.buildPredicate
import io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups
import io.trino.parquet.predicate.TupleDomainParquetPredicate
import io.trino.parquet.reader.MetadataReader
import io.trino.parquet.reader.ParquetReader
import io.trino.parquet.reader.RowGroupInfo
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.TransformConnectorPageSource
import io.trino.plugin.hive.parquet.ParquetPageSource
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.BigintType.BIGINT
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.ColumnIO
import org.apache.parquet.io.MessageColumnIO
import org.apache.parquet.io.PrimitiveColumnIO
import org.apache.parquet.schema.MessageType
import org.joda.time.DateTimeZone.UTC
import java.io.IOException
import java.util.Optional
import java.util.OptionalLong

/**
 * Plans the read of one Parquet data file: field resolution by identity, row-group pruning,
 * missing-column projection, positional virtuals and merge-on-read deletes.
 */
internal class DucklakeParquetPageSourceFactory(
        private val catalog: DucklakeCatalog,
        private val typeConverter: DucklakeTypeConverter,
        private val columnTrees: DucklakeColumnTreeCache,
        private val readStrategy: DucklakeReadStrategy,
        private val parquetReaderOptions: ParquetReaderOptions,
        private val fileFormatDataSourceStats: FileFormatDataSourceStats) {
    private val deleteFilter = DucklakeSplitDeleteFilter(parquetReaderOptions, fileFormatDataSourceStats)

    /**
     * The data-file page source WITHOUT the constant-virtual wrapper. Positional virtuals
     * ($row_id / $file_row_number) requested in [sourceColumns] are injected in-pipeline and the
     * split's own deletes are applied. Non-parquet data-file formats are rejected.
     */
    fun create(
            ducklakeSplit: DucklakeSplit,
            table: ConnectorTableHandle,
            sourceColumns: List<DucklakeColumnHandle>,
            effectivePredicate: TupleDomain<DucklakeColumnHandle>,
            fileSystem: TrinoFileSystem,
            cachedMetadata: ParquetMetadata? = null): ConnectorPageSource
    = catalog.readSession {
        readStrategy.pinReadSession(catalog)
        val format = ducklakeSplit.fileFormat
        if (!DucklakeSessionProperties.FORMAT_PARQUET.equals(format, ignoreCase = true)) {
            throw TrinoException(NOT_SUPPORTED,
                    "Data file '${ducklakeSplit.dataFilePath}' has format '$format', but this connector reads only " +
                            "parquet data files.")
        }
        val inputFile: TrinoInputFile = fileSystem.newInputFile(DucklakeReadSupport.toLocation(ducklakeSplit.dataFilePath))
        val currentTree = if (table is DucklakeTableHandle)
            readStrategy.fileColumnTree(catalog, columnTrees, table.tableId, table.snapshotId)
        else emptyList()
        val defaults = readStrategy.fileDefaults(catalog, table, currentTree, typeConverter)
        readStrategy.rejectUnreadableColumns(sourceColumns, currentTree)
        val eraTree = if (table is DucklakeTableHandle && ducklakeSplit.beginSnapshot > 0L)
            columnTrees.get(table.tableId, ducklakeSplit.beginSnapshot)
        else
            emptyList()
        val target = FileTarget(ducklakeSplit, currentTree, eraTree, defaults)
        createParquetPageSource(inputFile, sourceColumns, target, effectivePredicate, fileSystem, cachedMetadata)
    }

    /** What a file is projected onto: the split, the current and file-era column trees, and qualified defaults. */
    private class FileTarget(
            val split: DucklakeSplit,
            val currentTree: List<DucklakeColumn>,
            val eraTree: List<DucklakeColumn>,
            val defaults: DucklakeFileDefaults?)

    private fun createParquetPageSource(
            inputFile: TrinoInputFile,
            columns: List<DucklakeColumnHandle>,
            target: FileTarget,
            effectivePredicate: TupleDomain<DucklakeColumnHandle>,
            fileSystem: TrinoFileSystem,
            cachedMetadata: ParquetMetadata?): ConnectorPageSource
    {
        val split = target.split
        val memoryContext: AggregatedMemoryContext = newSimpleAggregatedMemoryContext()
        var dataSource: ParquetDataSource? = null
        try {
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.of(split.fileSizeBytes),
                    parquetReaderOptions,
                    memoryContext,
                    fileFormatDataSourceStats)

            // Feed DuckLake's footer_size hint to Trino's Parquet reader. For typical
            // footers (<48 KB), this trims the blind tail read down to the exact bytes;
            // for oversized footers, it replaces the fallback two-round-trip path with a
            // single-shot read. See FooterPrefetchingParquetDataSource.
            if (cachedMetadata == null) {
                dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(
                        dataSource, split.footerSize, parquetReaderOptions.maxFooterReadSize.toBytes())
            }

            val parquetMetadata: ParquetMetadata = cachedMetadata ?: MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions,
                    Optional.empty(),
                    Optional.empty())
            readStrategy.chargeRows { parquetMetadata.blocks.fold(0L) { n, block -> Math.addExact(n, block.rowCount) } }
            val file = FileSchema(parquetMetadata.fileMetaData.schema)
            val resolvedColumnIO = resolveColumns(file, columns, target, effectivePredicate)
            val rowGroups = filterRowGroups(file, dataSource, parquetMetadata, columns, split, resolvedColumnIO, effectivePredicate)

            val positionalInjections: List<PositionalInjection> = positionalInjections(columns)
            val fileColumns: List<DucklakeColumnHandle> = columns.filter { DucklakeVirtualColumns.positionalColumnKind(it) == null }

            val projection = FileColumnProjection(target, resolvedColumnIO, file.hasNameMap(split))
            fileColumns.forEach(projection::add)

            val dataSourceId: ParquetDataSourceId = dataSource.id
            val parquetReader = ParquetReader(
                    Optional.ofNullable(parquetMetadata.fileMetaData.createdBy),
                    projection.parquetColumns.toList(),
                    false, // appendRowNumberColumn
                    rowGroups.groups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    { exception -> handleParquetException(dataSourceId, exception) },
                    if (rowGroups.tupleDomain.isAll) Optional.empty() else Optional.of(rowGroups.predicate),
                    Optional.empty(), // bloomFilterStore
                    Optional.empty()) // rowFilter

            var pageSource: ConnectorPageSource = projection.transforms.build(ParquetPageSource(parquetReader))
            pageSource = injectPositionalColumns(pageSource, inputFile, fileSystem, split, fileColumns.size, positionalInjections)
            pageSource = deleteFilter.apply(fileSystem, split, pageSource)

            log.debug("Created Parquet page source for %d columns from file: %s",
                    columns.size, split.dataFilePath)

            return pageSource
        }
        catch (e: IOException) {
            throw closeAfterFailure(dataSource, split.dataFilePath, e)
        }
        catch (e: RuntimeException) {
            throw closeAfterFailure(dataSource, split.dataFilePath, e)
        }
    }

    /**
     * Separate out positional columns — the MERGE $row_id (-100, used for
     * DELETE/UPDATE/MERGE) plus the queryable positional virtuals $row_id and
     * $file_row_number — from file-resident columns. They are injected from cumulative
     * file position after the reader returns rows, BEFORE delete filtering, so positions
     * reflect original file positions.
     */
    private fun positionalInjections(columns: List<DucklakeColumnHandle>): List<PositionalInjection> =
        columns.withIndex()
                .filter { (_, column) -> DucklakeVirtualColumns.positionalColumnKind(column) != null }
                .map { (i, column) ->
                    PositionalInjection(
                            i,
                            resolveLineage = column.virtualKind() == VirtualKind.ROW_ID,
                            mutationLocator = column.isRowIdColumn())
                }

    /** The physical schema of one file, indexed for identity- and name-based field resolution. */
    private class FileSchema(val schema: MessageType) {
        val descriptorsByPath: Map<List<String>, ColumnDescriptor> = getDescriptors(schema, schema)
        val messageColumnIO: MessageColumnIO = getColumnIO(schema, schema)
        val fieldIdToColumnIO: Map<Int, ColumnIO> = buildMap {
            for (field in schema.fields) {
                if (field.id != null) {
                    messageColumnIO.getChild(field.name)?.let { child -> put(field.id.intValue(), child) }
                }
            }
        }

        fun hasNameMap(split: DucklakeSplit): Boolean = split.fieldIdToParquetSourceName.isNotEmpty()
    }

    private fun resolveColumns(
            file: FileSchema,
            columns: List<DucklakeColumnHandle>,
            target: FileTarget,
            effectivePredicate: TupleDomain<DucklakeColumnHandle>): Map<DucklakeColumnHandle, ColumnIO?>
    {
        val hasNameMap = file.hasNameMap(target.split)
        val predicateColumns = effectivePredicate.domains.orElse(emptyMap()).keys
        return (columns + predicateColumns).distinct()
                .filterNot { DucklakeVirtualColumns.positionalColumnKind(it) != null }
                .associateWith { column ->
                    if (column.virtualKind() == VirtualKind.ROW_SNAPSHOT_ID) DucklakeNativeSnapshotColumn.resolve(file.messageColumnIO)
                    else resolveColumnIO(
                            column,
                            hasNameMap,
                            file.messageColumnIO,
                            file.fieldIdToColumnIO,
                            target.split,
                            target.eraTree,
                            target.defaults != null)
                }
    }

    private class RowGroups(
            val tupleDomain: TupleDomain<ColumnDescriptor>,
            val predicate: TupleDomainParquetPredicate,
            val groups: List<RowGroupInfo>)

    private fun filterRowGroups(
            file: FileSchema,
            dataSource: ParquetDataSource,
            parquetMetadata: ParquetMetadata,
            columns: List<DucklakeColumnHandle>,
            split: DucklakeSplit,
            resolvedColumnIO: Map<DucklakeColumnHandle, ColumnIO?>,
            effectivePredicate: TupleDomain<DucklakeColumnHandle>): RowGroups
    {
        // Delete matching and positional columns derive file positions from cumulative
        // page sizes. They require whole-file splits streaming contiguously from row 0:
        // row-group/page pruning would create gaps and identify the wrong rows. Trino
        // still applies the residual predicate above this source. Finer split granularity
        // requires authoritative file positions rather than a zero-seeded page counter.
        val requiresContiguousPositions: Boolean =
                splitHasActiveDeletes(split) || columns.any { DucklakeVirtualColumns.positionalColumnKind(it) != null }
        val parquetTupleDomain: TupleDomain<ColumnDescriptor> =
                if (requiresContiguousPositions) TupleDomain.all()
                else toParquetTupleDomain(resolvedColumnIO, effectivePredicate)
        val parquetPredicate: TupleDomainParquetPredicate = buildPredicate(file.schema, parquetTupleDomain, file.descriptorsByPath, UTC)
        val rowGroups: List<RowGroupInfo> = getFilteredRowGroups(
                0,
                split.fileSizeBytes,
                dataSource,
                parquetMetadata,
                listOf(parquetTupleDomain),
                listOf(parquetPredicate),
                file.descriptorsByPath,
                UTC,
                Domain.DEFAULT_COMPACTION_THRESHOLD,
                parquetReaderOptions)
        return RowGroups(parquetTupleDomain, parquetPredicate, rowGroups)
    }

    /** Builds the reader's column list and the transforms that place file and missing columns. */
    private inner class FileColumnProjection(
            private val target: FileTarget,
            private val resolvedColumnIO: Map<DucklakeColumnHandle, ColumnIO?>,
            private val hasNameMap: Boolean) {
        val parquetColumns: MutableList<Column> = mutableListOf()
        val transforms: TransformConnectorPageSource.Builder = TransformConnectorPageSource.builder()
        private var parquetColumnOrdinal = 0

        fun add(column: DucklakeColumnHandle) {
            if (column.virtualKind() == VirtualKind.ROW_SNAPSHOT_ID) {
                addNativeSnapshot(resolvedColumnIO[column])
                return
            }
            val split = target.split
            val defaults = target.defaults
            defaults?.validate(column.columnId, column.columnType)
            val columnIO: ColumnIO? = resolvedColumnIO[column]

            if (columnIO == null) {
                // Missing column in file. Two cases:
                //   (1) hive-style external file: parquet body omits the partition column,
                //       but the catalog has a value for it via ducklake_file_partition_value.
                //       Project that value as a constant block.
                //   (2) schema evolution: use the qualified initial default (possibly NULL).
                transforms.constantValue(
                    if (defaults != null && column.columnId !in split.partitionValuesByColumnId) defaults.missingBlock(column)
                    else readStrategy.missingColumnBlock(column, split))
                return
            }

            val field = DucklakeParquetTypeUtils.constructField(
                    column.columnType,
                    columnIO,
                    column.columnId,
                    target.currentTree,
                    target.eraTree,
                    split.fieldIdToParquetSourceName,
                    hasNameMap,
                    defaults?.let { it::missing })
            if (field.isEmpty) {
                readStrategy.rejectUnsupportedPhysicalField(column.columnName)
                transforms.constantValue(readStrategy.missingColumnBlock(column, split))
                return
            }

            parquetColumns.add(Column(column.columnName, field.get()))
            if (defaults != null && defaults.needsRewrite(column.columnId))
                transforms.transform(parquetColumnOrdinal) { block -> defaults.rewrite(column, block) }
            else transforms.column(parquetColumnOrdinal)
            parquetColumnOrdinal++
        }

        private fun addNativeSnapshot(columnIO: ColumnIO?) {
            if (columnIO == null) {
                transforms.constantValue(DucklakeNativeSnapshotColumn.fallback(target.split.beginSnapshot))
                return
            }
            val field = DucklakeParquetTypeUtils.constructField(BIGINT, columnIO).orElseThrow {
                TrinoException(GENERIC_INTERNAL_ERROR, "Native snapshot_id field cannot be read as BIGINT")
            }
            parquetColumns.add(Column(columnIO.name, field))
            transforms.transform(parquetColumnOrdinal, DucklakeNativeSnapshotColumn::validate)
            parquetColumnOrdinal++
        }
    }

    /**
     * Inject positional virtuals before delete filtering so they reflect original file
     * positions. The queryable $row_id resolves the file's embedded lineage column when
     * present (UPDATE/compaction-preserved rowids) — one footer pass, only when requested.
     */
    private fun injectPositionalColumns(
            pageSource: ConnectorPageSource,
            inputFile: TrinoInputFile,
            fileSystem: TrinoFileSystem,
            split: DucklakeSplit,
            fileColumnCount: Int,
            positionalInjections: List<PositionalInjection>): ConnectorPageSource
    {
        if (positionalInjections.isEmpty()) {
            return pageSource
        }
        val lineage: LongArray? =
                if (positionalInjections.any { it.resolveLineage }) {
                    readLineage(fileSystem, inputFile.location().toString(), split.fileFormat, split.footerSize)
                } else {
                    null
                }
        return PositionalVirtualInjectingPageSource(
                pageSource, fileColumnCount + positionalInjections.size, positionalInjections,
                split.rowIdStart, lineage, split.dataFileId)
    }

    /** The embedded row-lineage array for a data file (parquet only), or null when it isn't carried. */
    private fun readLineage(fileSystem: TrinoFileSystem, resolvedPath: String, fileFormat: String, footerSize: Long): LongArray? {
        if (!DucklakeSessionProperties.FORMAT_PARQUET.equals(fileFormat, ignoreCase = true)) {
            return null
        }
        return try {
            DucklakeDeleteFileReader.readInternalRowIds(
                    fileSystem, resolvedPath, footerSize, parquetReaderOptions, fileFormatDataSourceStats,
                    { rows -> readStrategy.chargeRows { rows } })
        }
        catch (e: IOException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read row lineage: $resolvedPath", e)
        }
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeParquetPageSourceFactory::class.java)

        /**
         * Whether the split carries any active position deletes — either external parquet/puffin
         * delete files or inlined deletes from `ducklake_inlined_delete_<tableId>`.
         */
        fun splitHasActiveDeletes(split: DucklakeSplit): Boolean =
            split.deleteFilePaths.isNotEmpty() || split.inlinedDeletedRowPositions.isNotEmpty() ||
                    // A partial-file snapshot filter drops rows by file-local position too, so the
                    // reader must stream contiguously (no row-group pruning) for positions to align.
                    split.snapshotFilterMax != null

        private fun closeAfterFailure(dataSource: ParquetDataSource?, path: String, failure: Exception): TrinoException
        {
            if (dataSource != null) {
                try {
                    dataSource.close()
                }
                catch (closeFailure: IOException) {
                    if (failure != closeFailure) {
                        failure.addSuppressed(closeFailure)
                    }
                }
            }
            return failure as? TrinoException
                ?: TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create Parquet page source for file: $path", failure)
        }

        private fun toParquetTupleDomain(
                resolvedColumnIO: Map<DucklakeColumnHandle, ColumnIO?>,
                effectivePredicate: TupleDomain<DucklakeColumnHandle>): TupleDomain<ColumnDescriptor>
        {
            if (effectivePredicate.isNone) {
                return TupleDomain.none()
            }
            if (effectivePredicate.isAll) {
                return TupleDomain.all()
            }
            val domains: Map<DucklakeColumnHandle, Domain> = effectivePredicate.domains.orElse(null)
                    ?: return TupleDomain.all()
            val predicate: ImmutableMap.Builder<ColumnDescriptor, Domain> = ImmutableMap.builder()
            for ((columnHandle, domain) in domains) {
                (resolvedColumnIO[columnHandle] as? PrimitiveColumnIO)?.columnDescriptor?.let { predicate.put(it, domain) }
            }
            val parquetDomains: Map<ColumnDescriptor, Domain> = predicate.buildOrThrow()
            if (parquetDomains.isEmpty()) {
                return TupleDomain.all()
            }
            return TupleDomain.withColumnDomains(parquetDomains)
        }

        private fun handleParquetException(dataSourceId: ParquetDataSourceId, exception: Exception): RuntimeException =
            exception as? TrinoException
                ?: TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Error reading Parquet file: $dataSourceId",
                    exception)

        /**
         * Resolves the parquet [ColumnIO] for a projected column. A file with a
         * name map ([hasNameMap], set for add_files-registered files) is
         * AUTHORITATIVE: resolve ONLY through target_field_id → source_name (a
         * miss = column absent when the file was written → NULL/default), never a
         * bare-name coincidence — otherwise a dropped-then-re-added column would
         * resurrect the dead identity's physical data. Unmapped (INSERT-written /
         * legacy) files: field-id → era-name → current-name (rename fallbacks), with name
         * match gated on era-aware column existence (see below).
         */
        private fun resolveColumnIO(
                column: DucklakeColumnHandle,
                hasNameMap: Boolean,
                messageColumnIO: MessageColumnIO,
                fieldIdToColumnIO: Map<Int, ColumnIO>,
                split: DucklakeSplit,
                eraColumns: List<DucklakeColumn>,
                authoritativeIds: Boolean = false): ColumnIO?
        {
            if (hasNameMap) {
                return split.fieldIdToParquetSourceName[column.columnId]
                        ?.let { messageColumnIO.getChild(it) }
            }
            // Field identity is authoritative whenever present. Name-first binding swaps values
            // after rename cycles and lets a re-added name bind the dropped field's bytes.
            if (column.columnId > 0 || authoritativeIds) {
                if (column.columnId in 0..Int.MAX_VALUE.toLong()) fieldIdToColumnIO[column.columnId.toInt()]?.let { return it }
                if (authoritativeIds && fieldIdToColumnIO.isNotEmpty()) {
                    requireCompleteRootIds(messageColumnIO)
                    return null
                }
                val eraName: String? = eraColumns.firstOrNull { it.columnId == column.columnId }?.columnName
                if (eraName != null && !eraName.equals(column.columnName, ignoreCase = true)) {
                    messageColumnIO.getChild(eraName)?.let { return it }
                }
            }
            // Name fallback is for legacy files without field ids, but only when this identity
            // existed in the file's era. Otherwise a dropped/re-added name belongs to another id.
            if (eraColumns.isEmpty() || eraColumns.any { it.columnId == column.columnId }) {
                return messageColumnIO.getChild(column.columnName)
            }
            return null
        }

        private fun requireCompleteRootIds(message: MessageColumnIO) {
            if ((0 until message.childrenCount).any { message.getChild(it).type.id == null })
                throw TrinoException(NOT_SUPPORTED, "Cannot prove absent final-1.1 field in a partially ID-annotated Parquet file")
        }
    }
}
