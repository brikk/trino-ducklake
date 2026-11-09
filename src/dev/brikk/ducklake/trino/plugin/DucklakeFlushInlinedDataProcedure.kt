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

import dev.brikk.ducklake.trino.plugin.DucklakeReadSupport.toLocation

import com.google.inject.Inject
import com.google.inject.Provider
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakeInlinedChangeRow
import dev.brikk.ducklake.catalog.DucklakeInlinedDataInfo
import dev.brikk.ducklake.catalog.DucklakeInlinedFileDelete
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.FlushedInlinedFile
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.requireSchemaForTable
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.resolveSchema
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.resolveTable
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.systemProcedure
import dev.brikk.ducklake.trino.plugin.DucklakeProcedureSupport.translatingConflicts
import io.airlift.log.Logger
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.parquet.ParquetReaderOptions
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.spi.NodeVersion
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.procedure.Procedure
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType.VARCHAR
import org.apache.parquet.format.CompressionCodec
import java.util.Optional
import java.util.TreeMap
import java.util.UUID

/**
 * Implements `CALL <catalog>.system.flush_inlined_data(schema_name, table_name)` — materializes
 * a table's INLINED rows (rows DuckLake stores directly in `ducklake_inlined_data_<tableId>_<sv>`
 * metadata tables, written cross-engine by DuckDB under `data_inlining_row_limit`) into a real
 * Parquet data file, then end-snapshots the inlined rows — atomically. Row count and values are
 * preserved; the rows just move from catalog-resident to file-resident.
 *
 * Why it matters: Trino's merge sink can only tombstone file-resident rows, so DELETE/UPDATE/MERGE
 * are gated while a table has inlined rows ([DucklakeMetadata.beginMerge]). Flushing first makes
 * those operations work. Mirrors upstream DuckLake's `flush_inlined_data`.
 *
 * Writes one Parquet file per inlined schema version using that version's historical schema.
 * Partitioned inlined data is unsupported because its rows carry no partition assignment.
 * The whole read-then-write is conflict-checked at commit
 * (`ConflictMatrix.checkFlushedInlinedData`), so a concurrent change to the table's inlined data
 * aborts rather than duplicating or dropping rows.
 */
class DucklakeFlushInlinedDataProcedure @Inject constructor(
        private val catalog: DucklakeCatalog,
        private val fileSystemFactory: DucklakeFileSystemFactory,
        private val typeConverter: DucklakeTypeConverter,
        private val pathResolver: DucklakePathResolver,
        parquetReaderConfig: ParquetReaderConfig,
        private val fileFormatDataSourceStats: FileFormatDataSourceStats,
        nodeVersion: NodeVersion,
        config: DucklakeConfig,
) : Provider<Procedure> {
    private val maxLiveRows: Long = config.getReadMaxRows()
    private val trinoVersion: String = nodeVersion.toString()
    private val writerOptions: ParquetWriterOptions = ParquetWriterOptions.builder().build()
    private val parquetReaderOptions: ParquetReaderOptions = parquetReaderConfig.toParquetReaderOptions()

    override fun get(): Procedure =
        systemProcedure(
                "flush_inlined_data",
                listOf(
                        // Optional: both → one table; schema only → the schema; neither → every
                        // table with inlined rows across the catalog.
                        Procedure.Argument("SCHEMA_NAME", VARCHAR, false, null),
                        Procedure.Argument("TABLE_NAME", VARCHAR, false, null)),
                this,
                "flushInlinedData",
                String::class.java,
                String::class.java)

    @Suppress("unused") // invoked via MethodHandle
    fun flushInlinedData(session: ConnectorSession, schemaName: String?, tableName: String?) {
        requireSchemaForTable(schemaName, tableName)
        val snapshotId = catalog.currentSnapshotId
        val explicitSingle = !schemaName.isNullOrEmpty() && !tableName.isNullOrEmpty()
        val targets: List<Pair<DucklakeSchema, DucklakeTable>> = when {
            explicitSingle -> listOf(resolveTable(catalog, schemaName, tableName, snapshotId))
            !schemaName.isNullOrEmpty() -> {
                val schema = resolveSchema(catalog, schemaName, snapshotId)
                catalog.listTables(schema.schemaId, snapshotId).map { schema to it }
            }
            else -> catalog.listSchemas(snapshotId).flatMap { schema ->
                catalog.listTables(schema.schemaId, snapshotId).map { schema to it }
            }
        }

        var flushed = 0
        for ((schema, table) in targets) {
            if (flushOneTable(session, schema, table, snapshotId, failOnPartitioned = explicitSingle)) {
                flushed++
            }
        }
        if (!explicitSingle) {
            log.info("flush_inlined_data: flushed %d table(s) with inlined rows", flushed)
        }
    }

    /**
     * Flushes one table's inlined rows to a Parquet file. Returns true if a file was written.
     * Partitioned tables are unsupported (their inlined rows have no partition assignment): when
     * [failOnPartitioned] (an explicit single-table call) throws a clear error; otherwise (a
     * schema-/catalog-wide sweep) logs and skips so one partitioned table can't abort the batch.
     */
    private fun flushOneTable(
            session: ConnectorSession,
            schema: DucklakeSchema,
            table: DucklakeTable,
            snapshotId: Long,
            failOnPartitioned: Boolean,
    ): Boolean {
        val tableId = table.tableId
        val infos = catalog.getInlinedDataInfos(tableId, snapshotId)
        checkLiveRowBudget(schema, table, infos, snapshotId)
        val versions = infos.mapNotNull { loadVersionRows(tableId, snapshotId, it) }
        val inlinedFileDeletes = catalog.getInlinedFileDeletesBetween(tableId, 0L, snapshotId)
        if (versions.isEmpty() && inlinedFileDeletes.isEmpty()) {
            return false
        }

        // Inlined DATA rows carry no partition assignment to write into a hive-style file. File
        // deletion rows already target partitioned files and can be flushed independently.
        if (versions.isNotEmpty() && catalog.getPartitionSpecs(tableId, snapshotId).isNotEmpty()) {
            if (failOnPartitioned) {
                throw TrinoException(NOT_SUPPORTED,
                        "flush_inlined_data does not support partitioned tables yet: ${schema.schemaName}.${table.tableName}")
            }
            log.info("flush_inlined_data: skipping partitioned table %s.%s (unsupported)",
                    schema.schemaName, table.tableName)
            return false
        }

        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
        val tableDataPath: String = pathResolver.resolveTableDataPath(schema, table)
        val deleteWriter = DucklakeFlushDeleteFileWriter(fileSystem, writerOptions, trinoVersion)
        val files = versions.map { version -> materializeVersion(fileSystem, deleteWriter, tableDataPath, version) }
        val existingFileDeletes = materializeExistingFileDeletes(
                fileSystem, deleteWriter, tableDataPath, tableId, snapshotId, inlinedFileDeletes)

        translatingConflicts { catalog.flushInlinedDataWithSnapshots(tableId, files, existingFileDeletes, snapshotId) }
        return true
    }

    /**
     * The catalog returns a version's inlined rows as one list (it has no paged read), so refuse
     * before loading when the live rows exceed `ducklake.read.max-physical-rows`. Deleted rows that
     * were never flushed are loaded too but cannot be counted up front; see CRUTCHES-AND-SHORTCUTS.
     */
    private fun checkLiveRowBudget(
            schema: DucklakeSchema,
            table: DucklakeTable,
            infos: List<DucklakeInlinedDataInfo>,
            snapshotId: Long) {
        val liveRows = infos.filter { it.hasLiveRows }.fold(0L) { total, info ->
            Math.addExact(total, catalog.countInlinedRows(table.tableId, info.schemaVersion, snapshotId))
        }
        if (liveRows > maxLiveRows) {
            throw TrinoException(GENERIC_USER_ERROR,
                    "flush_inlined_data: ${schema.schemaName}.${table.tableName} has $liveRows live inlined rows, " +
                            "above ducklake.read.max-physical-rows=$maxLiveRows; nothing was written")
        }
    }

    private data class VersionRows(
            val columns: List<DucklakeColumnHandle>,
            val allColumns: List<DucklakeColumn>,
            val changes: List<DucklakeInlinedChangeRow>)

    /** One physical inlined schema version -> one Parquet file with that historical schema. */
    private fun loadVersionRows(
            tableId: Long,
            readSnapshotId: Long,
            info: DucklakeInlinedDataInfo): VersionRows? {
        val schemaSnapshot = catalog.resolveSchemaVersionSnapshot(tableId, info.schemaVersion, readSnapshotId)
            ?: throw TrinoException(GENERIC_INTERNAL_ERROR,
                    "Cannot resolve schema version ${info.schemaVersion} for inlined table $tableId")
        val sourceColumns = catalog.getTableColumns(tableId, schemaSnapshot)
                .filter { it.parentColumn == null }
        val allColumns = catalog.getAllColumnsWithParentage(tableId, schemaSnapshot)
        val handles = sourceColumns.map { column ->
            DucklakeColumnHandle(
                    column.columnId,
                    column.columnName,
                    typeConverter.toTrinoType(column, allColumns),
                    column.nullsAllowed)
        }
        val changes = catalog.getInlinedChangesBetween(
                tableId,
                info.schemaVersion,
                0L,
                readSnapshotId,
                sourceColumns.map { it.columnId })
        if (changes.isEmpty()) {
            return null
        }
        return VersionRows(handles, allColumns, changes)
    }

    private fun materializeVersion(
            fileSystem: TrinoFileSystem,
            deleteWriter: DucklakeFlushDeleteFileWriter,
            tableDataPath: String,
            version: VersionRows): FlushedInlinedFile {
        val types = version.columns.map { it.columnType }
        val rows = version.changes.map { change ->
            change.values.indices.map { index ->
                DucklakeInlinedValueConverter.convertJdbcValue(change.values[index], types[index])
            }
        }
        val rowIds = version.changes.map { it.rowId }
        val beginSnapshots = version.changes.map { it.beginSnapshot }
        val fragment = writeParquetFile(
                fileSystem,
                tableDataPath,
                version.columns,
                version.allColumns,
                types,
                rows,
                rowIds,
                beginSnapshots)
        val deleted = version.changes.mapIndexedNotNull { position, change ->
            change.endSnapshot?.let { DucklakeFlushDeleteFileWriter.DeletedPosition(position.toLong(), it) }
        }
        val resolvedDataPath = pathResolver.resolveFilePath(fragment.path, fragment.pathIsRelative, tableDataPath)
        val deleteFragment = if (deleted.isEmpty()) null else
            deleteWriter.write(tableDataPath, resolvedDataPath, 0L, deleted)
        return FlushedInlinedFile(
                fragment,
                beginSnapshots.min(),
                beginSnapshots.max(),
                rowIds.min(),
                deleteFragment)
    }

    /**
     * Fold metadata-resident deletes of existing data files into snapshot-tagged replacements.
     * Existing cumulative delete positions retain their original snapshots; newly inlined
     * positions already carry the exact snapshot in [DucklakeInlinedFileDelete].
     */
    private fun materializeExistingFileDeletes(
            fileSystem: TrinoFileSystem,
            writer: DucklakeFlushDeleteFileWriter,
            tableDataPath: String,
            tableId: Long,
            readSnapshotId: Long,
            inlinedDeletes: List<DucklakeInlinedFileDelete>): List<DucklakeDeleteFragment> {
        if (inlinedDeletes.isEmpty()) {
            return emptyList()
        }
        val filesById = catalog.getDataFiles(tableId, readSnapshotId).groupBy { it.dataFileId }
        val deleteBeginByPath = catalog.getDeletionsBetween(tableId, 0L, readSnapshotId)
                .mapNotNull { event -> event.currentDeletePath?.let { it to event.snapshotId } }
                .groupBy({ it.first }, { it.second })
                .mapValues { (_, snapshots) -> snapshots.min() }
        return inlinedDeletes.groupBy { it.dataFileId }.map { (dataFileId, newDeletes) ->
            val joinedRows = filesById[dataFileId]
                ?: throw TrinoException(GENERIC_INTERNAL_ERROR, "Inlined deletion references inactive data_file_id $dataFileId")
            val primary = joinedRows.first()
            val activeDeleteRows = joinedRows.mapNotNull { row -> row.deleteFilePath?.let { path -> row to path } }
                    .distinctBy { (_, path) -> path }
            check(activeDeleteRows.size <= 1) {
                "data_file_id $dataFileId has ${activeDeleteRows.size} active delete files"
            }
            val merged = TreeMap<Long, Long>()
            activeDeleteRows.singleOrNull()?.let { (dataFile, deleteFilePath) ->
                val beginSnapshot = deleteBeginByPath[deleteFilePath]
                    ?: throw TrinoException(GENERIC_INTERNAL_ERROR,
                            "Cannot resolve begin snapshot for delete file $deleteFilePath")
                readExistingDeletePositions(fileSystem, tableDataPath, dataFile, deleteFilePath, beginSnapshot)
                        .forEach { (position, snapshot) -> merged.merge(position, snapshot, ::minOf) }
            }
            newDeletes.forEach { deletion -> merged.merge(deletion.position, deletion.snapshotId, ::minOf) }
            val resolvedDataPath = pathResolver.resolveFilePath(primary.path, primary.pathIsRelative, tableDataPath)
            writer.write(
                    tableDataPath,
                    resolvedDataPath,
                    dataFileId,
                    merged.map { (position, snapshot) ->
                        DucklakeFlushDeleteFileWriter.DeletedPosition(position, snapshot)
                    })
        }
    }

    private fun readExistingDeletePositions(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            file: dev.brikk.ducklake.catalog.DucklakeDataFile,
            deleteFilePath: String,
            beginSnapshot: Long): Map<Long, Long> {
        val deletePath = pathResolver.resolveFilePath(
                deleteFilePath,
                file.deleteFilePathIsRelative ?: false,
                tableDataPath)
        if (file.deleteFileFormat.equals("puffin", ignoreCase = true)) {
            return DucklakePuffinDeleteReader.readDeletedPositions(fileSystem.newInputFile(toLocation(deletePath)))
                    .associateWith { beginSnapshot }
        }
        val withSnapshots = DucklakeDeleteFileReader.readPositionsWithSnapshotsIfPresent(
                fileSystem,
                deletePath,
                file.deleteFileFooterSize ?: 0L,
                parquetReaderOptions,
                fileFormatDataSourceStats)
        if (withSnapshots != null) {
            return withSnapshots
        }
        val positions = DucklakeDeleteFileReader.readPositions(
                fileSystem,
                deletePath,
                file.deleteFileFooterSize ?: 0L,
                parquetReaderOptions,
                fileFormatDataSourceStats)
        return positions.values.associate { value ->
            (if (positions.global) value - DucklakeRowIdentity.requireStart(file.rowIdStart) else value) to beginSnapshot
        }
    }

    /**
     * Materialize [rows] into one Parquet data file and return its registration fragment. The
     * file carries trailing `_ducklake_internal_row_id` / `_ducklake_internal_snapshot_id` columns
     * holding each row's original [rowIds] / [beginSnapshots]. Both follow data columns, keeping
     * catalog-derived leaf-stat indices valid and excluding internal columns from stats.
     */
    private fun writeParquetFile(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            columnHandles: List<DucklakeColumnHandle>,
            allCatalogColumns: List<DucklakeColumn>,
            columnTypes: List<Type>,
            rows: List<List<Any?>>,
            rowIds: List<Long>,
            beginSnapshots: List<Long>): DucklakeWriteFragment {
        val columnNames: List<String> = columnHandles.map { it.columnName }
        // Physical write layout = catalog columns + trailing BIGINT lineage/snapshot columns.
        // JSON columns are physically UTF-8 VARCHAR in parquet (catalog type stays 'json').
        val writeNames: List<String> = columnNames + listOf(
                DucklakePageSink.LINEAGE_COLUMN_NAME,
                DucklakeDeleteFileReader.INTERNAL_SNAPSHOT_ID_COLUMN)
        val writeTypes: List<Type> = columnTypes + listOf(BIGINT, BIGINT)
        val schemaConverter = ParquetSchemaConverter(
                writeTypes.map { DucklakeJsonSupport.toParquetWriteType(it) }, writeNames, false, false)
        val messageType = DucklakeParquetSchemaBuilder.buildMessageType(
                columnHandles, allCatalogColumns, schemaConverter.messageType,
                mapOf(
                        DucklakePageSink.LINEAGE_COLUMN_NAME
                                to DucklakeDeleteFileReader.ROW_ID_PARQUET_FIELD_ID.toLong(),
                        DucklakeDeleteFileReader.INTERNAL_SNAPSHOT_ID_COLUMN
                                to DucklakeDeleteFileReader.SNAPSHOT_ID_PARQUET_FIELD_ID.toLong()))

        val fileName = "ducklake-${UUID.randomUUID()}.parquet"
        val filePath: Location = Location.of(tableDataPath).appendPath(fileName)
        val outputStream = fileSystem.newOutputFile(filePath).create()

        val parquetWriter = ParquetWriter(
                outputStream,
                messageType,
                schemaConverter.primitiveTypes,
                writerOptions,
                CompressionCodec.ZSTD,
                trinoVersion,
                Optional.empty(),
                Optional.empty())
        val writer = ParquetFileWriter(parquetWriter, outputStream, fileSystem, filePath, fileName,
                emptyMap(), null, columnHandles, allCatalogColumns, messageType)

        val rowsWithLineage: List<List<Any?>> = rows.mapIndexed { i, row ->
            row + rowIds[i] + beginSnapshots[i]
        }

        var fragment: DucklakeWriteFragment? = null
        try {
            // Use the same native-row → bounded-page path as inlined reads (including 484 JSON).
            DucklakeNativeRowsPageSource(writeTypes, rowsWithLineage).use { source ->
                while (!source.isFinished) {
                    val sourcePage = source.nextSourcePage ?: continue
                    writer.write(sourcePage.page)
                }
            }
            fragment = writer.finishAndBuildFragment()
        }
        finally {
            if (fragment == null) {
                writer.abort()
            }
        }
        return fragment
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeFlushInlinedDataProcedure::class.java)
    }
}
