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

import io.airlift.json.JsonCodec
import io.airlift.slice.Slice
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableList.toImmutableList
import com.google.common.collect.ImmutableMap
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.TableColumnSpec
import dev.brikk.ducklake.trino.plugin.DucklakeCatalogExceptions.translateCatalogExceptions
import dev.brikk.ducklake.trino.plugin.DucklakeMetadataTables.MetadataTableName
import dev.brikk.ducklake.trino.plugin.DucklakeMetadataTables.getMetadataColumns
import dev.brikk.ducklake.trino.plugin.DucklakeMetadataTables.parseMetadataTableName
import dev.brikk.ducklake.trino.plugin.DucklakeMetadataTables.toColumnHandles
import dev.brikk.ducklake.trino.plugin.DucklakeTableVersions.getSnapshotIdFromVersion
import dev.brikk.ducklake.trino.plugin.DucklakeTableVersions.getSnapshotTimestampFromVersion
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ColumnMetadata
import io.trino.spi.connector.ColumnPosition
import io.trino.spi.connector.ConnectorAnalyzeMetadata
import io.trino.spi.connector.ConnectorInsertTableHandle
import io.trino.spi.connector.ConnectorMergeTableHandle
import io.trino.spi.connector.ConnectorOutputMetadata
import io.trino.spi.connector.ConnectorOutputTableHandle
import io.trino.spi.connector.ConnectorPartitioningHandle
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.ConnectorTableLayout
import io.trino.spi.connector.ConnectorTableMetadata
import io.trino.spi.connector.ConnectorTableProperties
import io.trino.spi.connector.ConnectorTableVersion
import io.trino.spi.connector.PointerType
import io.trino.spi.connector.ConnectorViewDefinition
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.ConstraintApplicationResult
import io.trino.spi.connector.RetryMode
import io.trino.spi.connector.RowChangeParadigm
import io.trino.spi.connector.SaveMode
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.connector.SchemaTablePrefix
import io.trino.spi.connector.TableFunctionApplicationResult
import io.trino.spi.function.table.ConnectorTableFunctionHandle
import io.trino.spi.security.TrinoPrincipal
import io.trino.spi.statistics.ComputedStatistics
import io.trino.spi.statistics.TableStatistics
import io.trino.spi.type.Type

import java.time.Instant
import java.util.Optional

import io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED

/**
 * The active partition spec is the most-recently-added one (highest version), or empty if the
 * table has none. Shared by the metadata write paths and the add_files procedure.
 */
internal fun activePartitionSpecOf(specs: List<DucklakePartitionSpec>): Optional<DucklakePartitionSpec> =
        if (specs.isEmpty()) Optional.empty() else Optional.of(specs.last())

/**
 * Metadata implementation for Ducklake connector: tables, columns and DDL here; views,
 * statistics, filter pushdown and the write paths in their own classes.
 */
class DucklakeMetadata(
        private val catalog: DucklakeCatalog,
        private val typeConverter: DucklakeTypeConverter,
        private val snapshotResolver: DucklakeSnapshotResolver,
        fragmentCodec: JsonCodec<DucklakeWriteFragment>,
        deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment>,
        pathResolver: DucklakePathResolver,
        temporalPartitionEncoding: DucklakeTemporalPartitionEncoding)
        : DucklakeMetadataSpi()
{
    private val views = DucklakeViewMetadata(catalog, snapshotResolver)
    private val statistics = DucklakeTableStatistics(catalog)
    private val filterPushdown = DucklakeFilterPushdown(catalog)
    private val writes = DucklakeWriteMetadata(
            catalog, typeConverter, fragmentCodec, deleteFragmentCodec, pathResolver, temporalPartitionEncoding)

    override fun listSchemaNames(session: ConnectorSession): List<String>
    = catalog.queryReadSession(session.queryId) {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        catalog.listSchemas(snapshotId).map(DucklakeSchema::schemaName)
    }

    override fun getTableHandle(
            session: ConnectorSession,
            tableName: SchemaTableName,
            startVersion: Optional<ConnectorTableVersion>,
            endVersion: Optional<ConnectorTableVersion>): ConnectorTableHandle?
    = catalog.queryReadSession(session.queryId) {
        if (startVersion.isPresent && endVersion.isPresent) {
            throw TrinoException(NOT_SUPPORTED, "DuckLake does not support version ranges; provide only one table version bound")
        }

        val queryVersion: Optional<ConnectorTableVersion> = if (endVersion.isPresent) endVersion else startVersion
        var querySnapshotId: Long? = null
        var querySnapshotTimestamp: Instant? = null
        if (queryVersion.isPresent) {
            val version = queryVersion.get()
            when (version.pointerType) {
                PointerType.TARGET_ID -> querySnapshotId = getSnapshotIdFromVersion(version)
                PointerType.TEMPORAL -> querySnapshotTimestamp = getSnapshotTimestampFromVersion(session, version)
                else -> {}
            }
        }

        val metadataTable: MetadataTableName? = parseMetadataTableName(tableName)
        val baseTableName: SchemaTableName =
                if (metadataTable != null) SchemaTableName(tableName.schemaName, metadataTable.baseTableName)
                else tableName

        val snapshotId = snapshotResolver.resolveSnapshotId(session, querySnapshotId, querySnapshotTimestamp)

        val table: DucklakeTable = catalog.getTable(baseTableName.schemaName, baseTableName.tableName, snapshotId)
            ?: return@queryReadSession null

        // The catalog library refuses writes into encrypted lakes because it cannot produce the
        // required per-file encryption keys. Refuse normal table reads just as early: otherwise
        // encrypted Parquet reaches Trino's reader and surfaces as an opaque "not a parquet file"
        // error (or, worse, an inlined split returns only part of the table). Metadata tables remain
        // available so an operator can diagnose the catalog.
        if (metadataTable == null && catalog.isEncrypted()) {
            throw TrinoException(
                    NOT_SUPPORTED,
                    "DuckLake catalog is encrypted; this connector cannot read encrypted data files")
        }

        validateDataFileFormatIsParquet(catalog.getTableDataFileFormat(table.tableId), baseTableName)

        if (metadataTable != null) {
            val parsed = metadataTable
            return@queryReadSession DucklakeMetadataTableHandle(
                    tableName.schemaName,
                    tableName.tableName,
                    parsed.baseTableName,
                    table.tableId,
                    snapshotId,
                    parsed.metadataTableType)
        }

        DucklakeTableHandle(
                tableName.schemaName,
                tableName.tableName,
                table.tableId,
                snapshotId)
    }

    override fun getTableMetadata(session: ConnectorSession, tableHandle: ConnectorTableHandle): ConnectorTableMetadata
    {
        if (tableHandle is DucklakeMetadataTableHandle) {
            return ConnectorTableMetadata(
                    tableHandle.getSchemaTableName(),
                    getMetadataColumns(tableHandle.metadataTableType))
        }

        if (tableHandle is ChangeFeedTableHandle) {
            // PTF-scan handle (applyTableFunction). Reached by EXPLAIN / plan printing; the
            // synthetic name is the change-feed function, columns are the function output.
            return ConnectorTableMetadata(
                    SchemaTableName("system", tableHandle.feedType.functionName),
                    tableHandle.outputColumns().map { column ->
                        ColumnMetadata.builder()
                                .setName(column.columnName)
                                .setType(column.columnType)
                                .setNullable(column.nullable)
                                .build()
                    })
        }

        val ducklakeTableHandle = tableHandle as DucklakeTableHandle

        val columns: List<DucklakeColumn> = catalog.getTableColumns(
                ducklakeTableHandle.tableId,
                ducklakeTableHandle.snapshotId)
        val allColumns = catalog.getAllColumnsWithParentage(
                ducklakeTableHandle.tableId,
                ducklakeTableHandle.snapshotId)
        val columnComments: Map<Long, String> = catalog.getColumnComments(
                ducklakeTableHandle.tableId,
                ducklakeTableHandle.snapshotId)

        val columnMetadata: ImmutableList.Builder<ColumnMetadata> = ImmutableList.builder()
        for (column in columns) {
            columnMetadata.add(ColumnMetadata.builder()
                    .setName(column.columnName)
                    .setType(typeConverter.toTrinoType(column, allColumns))
                    .setNullable(column.nullsAllowed)
                    .setComment(Optional.ofNullable(columnComments[column.columnId]))
                    .build())
        }
        // Hidden virtual columns must be present here (with hidden=true) to be resolvable by
        // name — the hidden flag is what keeps them out of SELECT * / DESCRIBE, not their
        // absence. Keep this list in lockstep with getColumnHandles.
        for (kind in EXPOSED_VIRTUAL_COLUMNS) {
            columnMetadata.add(ColumnMetadata.builder()
                    .setName(kind.columnName)
                    .setType(kind.columnType)
                    .setNullable(true)
                    .setHidden(true)
                    .build())
        }

        return ConnectorTableMetadata(
                ducklakeTableHandle.getSchemaTableName(),
                columnMetadata.build(),
                mapOf(),
                Optional.ofNullable(catalog.getTableComment(
                        ducklakeTableHandle.tableId,
                        ducklakeTableHandle.snapshotId)))
    }

    override fun getTableProperties(session: ConnectorSession, table: ConnectorTableHandle): ConnectorTableProperties
    {
        // DuckLake sort metadata describes ordering WITHIN each data file. ConnectorTableProperties
        // localProperties describes the stream emitted by a scan driver; multiple files/splits are
        // not globally ordered, and partitioned/MERGE writes are not consistently sorted either.
        // Advertising the per-file spec here lets Trino omit a required Sort and can produce wrong
        // window/streaming results. Keep sort metadata for write-side per-file ordering only.
        return ConnectorTableProperties()
    }

    override fun listTables(session: ConnectorSession, schemaName: Optional<String>): List<SchemaTableName>
    = catalog.queryReadSession(session.queryId) {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val relations: ImmutableList.Builder<SchemaTableName> = ImmutableList.builder()

        if (schemaName.isPresent) {
            val schema: DucklakeSchema = catalog.getSchema(schemaName.get(), snapshotId)
                ?: return@queryReadSession ImmutableList.of()
            for (table in catalog.listTables(schema.schemaId, snapshotId)) {
                relations.add(SchemaTableName(schemaName.get(), table.tableName))
            }
            for (view in catalog.listViews(schema.schemaId, snapshotId)) {
                if (views.isViewAccessible(view)) {
                    relations.add(SchemaTableName(schemaName.get(), view.viewName))
                }
            }
            return@queryReadSession relations.build()
        }

        for (schema in catalog.listSchemas(snapshotId)) {
            for (table in catalog.listTables(schema.schemaId, snapshotId)) {
                relations.add(SchemaTableName(schema.schemaName, table.tableName))
            }
            for (view in catalog.listViews(schema.schemaId, snapshotId)) {
                if (views.isViewAccessible(view)) {
                    relations.add(SchemaTableName(schema.schemaName, view.viewName))
                }
            }
        }
        relations.build()
    }

    override fun getColumnHandles(session: ConnectorSession, tableHandle: ConnectorTableHandle): Map<String, ColumnHandle>
    {
        if (tableHandle is DucklakeMetadataTableHandle) {
            return toColumnHandles(getMetadataColumns(tableHandle.metadataTableType))
        }

        if (tableHandle is ChangeFeedTableHandle) {
            return tableHandle.outputColumns().associateBy { it.columnName }
        }

        val ducklakeTableHandle = tableHandle as DucklakeTableHandle

        val columns: List<DucklakeColumn> = catalog.getTableColumns(
                ducklakeTableHandle.tableId,
                ducklakeTableHandle.snapshotId)
        val allColumns = catalog.getAllColumnsWithParentage(
                ducklakeTableHandle.tableId,
                ducklakeTableHandle.snapshotId)

        val columnHandles: ImmutableMap.Builder<String, ColumnHandle> = ImmutableMap.builder()
        for (column in columns) {
            columnHandles.put(
                    column.columnName,
                    DucklakeColumnHandle(
                            column.columnId,
                            column.columnName,
                            typeConverter.toTrinoType(column, allColumns),
                            column.nullsAllowed,
                            // Rows predating an ADD COLUMN ... DEFAULT must project the
                            // initial default, not NULL (upstream issue 1135 semantics).
                            column.initialDefault))
        }
        for (kind in EXPOSED_VIRTUAL_COLUMNS) {
            columnHandles.put(kind.columnName, kind.columnHandle())
        }
        return columnHandles.buildOrThrow()
    }

    override fun getColumnMetadata(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            columnHandle: ColumnHandle): ColumnMetadata
    {
        val ducklakeColumnHandle = columnHandle as DucklakeColumnHandle

        return ColumnMetadata.builder()
                .setName(ducklakeColumnHandle.columnName)
                .setType(ducklakeColumnHandle.columnType)
                .setNullable(ducklakeColumnHandle.nullable)
                .setHidden(ducklakeColumnHandle.isVirtual())
                .build()
    }

    override fun getTableStatistics(session: ConnectorSession, tableHandle: ConnectorTableHandle): TableStatistics =
            statistics.getTableStatistics(tableHandle) { getColumnHandles(session, tableHandle) }

    override fun getStatisticsCollectionMetadata(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            analyzeProperties: Map<String, Any>): ConnectorAnalyzeMetadata =
            statistics.getStatisticsCollectionMetadata(tableHandle)

    override fun beginStatisticsCollection(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle): ConnectorTableHandle =
            // No scan-side preparation needed — the refresh runs entirely against the catalog in
            // finishStatisticsCollection. Pass the handle straight through.
            tableHandle

    override fun finishStatisticsCollection(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            computedStatistics: Collection<ComputedStatistics>) =
            statistics.finishStatisticsCollection(tableHandle)

    /**
     * Map the change-feed table function to a [ChangeFeedTableHandle] *scan*, so the engine's
     * `RewriteTableFunctionToTableScan` plans an ordinary TableScanNode over the function's output
     * and [applyFilter] composes on it.
     */
    override fun applyTableFunction(
            session: ConnectorSession,
            handle: ConnectorTableFunctionHandle): Optional<TableFunctionApplicationResult<ConnectorTableHandle>>
    {
        if (handle is ChangeFeedHandle) {
            return Optional.of(TableFunctionApplicationResult(
                    ChangeFeedTableHandle(
                            handle.feedType,
                            handle.schemaName,
                            handle.tableName,
                            handle.tableId,
                            handle.startSnapshot,
                            handle.endSnapshot,
                            handle.dataColumns),
                    handle.outputColumns()))
        }
        return Optional.empty()
    }

    override fun applyFilter(
            session: ConnectorSession,
            handle: ConnectorTableHandle,
            constraint: Constraint): Optional<ConstraintApplicationResult<ConnectorTableHandle>> =
            filterPushdown.applyFilter(handle, constraint)

    override fun listTableColumns(
            session: ConnectorSession,
            prefix: SchemaTablePrefix): Map<SchemaTableName, List<ColumnMetadata>>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val columns: ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> = ImmutableMap.builder()

        val tables: List<SchemaTableName> = prefix.table
                .map { listOf(prefix.toSchemaTableName()) }
                .orElseGet { listTables(session, prefix.schema) }

        for (tableName in tables) {
            val metadataTable: MetadataTableName? = parseMetadataTableName(tableName)
            if (metadataTable != null) {
                val baseTable = SchemaTableName(tableName.schemaName, metadataTable.baseTableName)
                if (catalog.getTable(baseTable.schemaName, baseTable.tableName, snapshotId) != null) {
                    columns.put(tableName, getMetadataColumns(metadataTable.metadataTableType))
                }
                continue
            }

            val table: DucklakeTable? = catalog.getTable(tableName.schemaName, tableName.tableName, snapshotId)
            if (table != null) {
                val tableColumns: List<DucklakeColumn> = catalog.getTableColumns(table.tableId, snapshotId)
                val allColumns = catalog.getAllColumnsWithParentage(table.tableId, snapshotId)
                columns.put(
                        tableName,
                        tableColumns.stream()
                                .map { column ->
                                    ColumnMetadata.builder()
                                            .setName(column.columnName)
                                            .setType(typeConverter.toTrinoType(column, allColumns))
                                            .setNullable(column.nullsAllowed)
                                            .build()
                                }
                                .collect(toImmutableList()))
            }
        }

        return columns.buildOrThrow()
    }

    override fun createSchema(session: ConnectorSession, schemaName: String, properties: Map<String, Any?>, owner: TrinoPrincipal)
    {
        translateCatalogExceptions { catalog.createSchema(schemaName) }
    }

    override fun dropSchema(session: ConnectorSession, schemaName: String, cascade: Boolean)
    {
        translateCatalogExceptions { catalog.dropSchema(schemaName) }
    }

    override fun createTable(session: ConnectorSession, tableMetadata: ConnectorTableMetadata, saveMode: SaveMode)
    {
        if (saveMode == SaveMode.REPLACE) {
            throw TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables")
        }
        writes.createTable(tableMetadata)
    }

    override fun dropTable(session: ConnectorSession, tableHandle: ConnectorTableHandle)
    {
        val handle = tableHandle as DucklakeTableHandle
        translateCatalogExceptions { catalog.dropTable(handle.schemaName, handle.tableName) }
    }

    override fun truncateTable(session: ConnectorSession, tableHandle: ConnectorTableHandle)
    {
        val handle = tableHandle as DucklakeTableHandle
        translateCatalogExceptions { catalog.truncateTable(handle.schemaName, handle.tableName) }
    }

    override fun renameTable(session: ConnectorSession, tableHandle: ConnectorTableHandle, newTableName: SchemaTableName)
    {
        val handle = tableHandle as DucklakeTableHandle
        if (!newTableName.schemaName.equals(handle.schemaName, ignoreCase = true)) {
            // Table data paths are schema-relative in DuckLake, so a cross-schema move would
            // leave the table's data files unreachable. Upstream has no cross-schema rename.
            throw TrinoException(NOT_SUPPORTED,
                    "Renaming a table across schemas is not supported: table data paths are schema-relative")
        }
        translateCatalogExceptions {
            catalog.renameTable(handle.tableId, newTableName.schemaName, newTableName.tableName)
        }
    }

    override fun renameSchema(session: ConnectorSession, source: String, target: String)
    {
        translateCatalogExceptions { catalog.renameSchema(source, target) }
    }

    override fun setTableComment(session: ConnectorSession, tableHandle: ConnectorTableHandle, comment: Optional<String>)
    {
        val handle = tableHandle as DucklakeTableHandle
        translateCatalogExceptions { catalog.setTableComment(handle.tableId, comment.orElse(null)) }
    }

    override fun setColumnComment(session: ConnectorSession, tableHandle: ConnectorTableHandle, column: ColumnHandle, comment: Optional<String>)
    {
        val handle = tableHandle as DucklakeTableHandle
        val columnHandle = column as DucklakeColumnHandle
        if (columnHandle.isVirtual() || columnHandle.isRowIdColumn()) {
            throw TrinoException(NOT_SUPPORTED, "Cannot comment on virtual column: ${columnHandle.columnName}")
        }
        translateCatalogExceptions {
            catalog.setColumnComment(handle.tableId, columnHandle.columnId, comment.orElse(null))
        }
    }

    override fun addColumn(session: ConnectorSession, tableHandle: ConnectorTableHandle, column: ColumnMetadata, position: ColumnPosition)
    {
        val handle = tableHandle as DucklakeTableHandle
        val columnSpec: TableColumnSpec = writes.toColumnSpec(column.name, column.type, column.isNullable)
        translateCatalogExceptions { catalog.addColumn(handle.tableId, columnSpec) }
    }

    override fun dropColumn(session: ConnectorSession, tableHandle: ConnectorTableHandle, column: ColumnHandle)
    {
        val handle = tableHandle as DucklakeTableHandle
        val ducklakeColumn = column as DucklakeColumnHandle
        translateCatalogExceptions { catalog.dropColumn(handle.tableId, ducklakeColumn.columnId) }
    }

    override fun renameColumn(session: ConnectorSession, tableHandle: ConnectorTableHandle, source: ColumnHandle, target: String)
    {
        val handle = tableHandle as DucklakeTableHandle
        val ducklakeColumn = source as DucklakeColumnHandle
        translateCatalogExceptions { catalog.renameColumn(handle.tableId, ducklakeColumn.columnId, target) }
    }

    // Widening preserves reads of existing physical files; narrowing would corrupt existing rows.
    override fun setColumnType(session: ConnectorSession, tableHandle: ConnectorTableHandle, column: ColumnHandle, type: Type)
    {
        val handle = tableHandle as DucklakeTableHandle
        val ducklakeColumn = column as DucklakeColumnHandle
        val sourceType: Type = ducklakeColumn.columnType
        if (sourceType == type) {
            return
        }
        if (!DucklakeTypePromotion.isWidening(sourceType, type)) {
            throw TrinoException(NOT_SUPPORTED, String.format(
                    "ALTER COLUMN SET DATA TYPE supports only widening type promotions; " +
                            "%s -> %s is not allowed for column \"%s\"",
                    sourceType.displayName, type.displayName, ducklakeColumn.columnName))
        }
        // toDucklakeType rejects any target with no DuckLake representation (e.g. an unsupported
        // timestamp precision) with its own NOT_SUPPORTED before we touch the catalog.
        val newDucklakeType: String = typeConverter.toDucklakeType(type)
        translateCatalogExceptions { catalog.setColumnType(handle.tableId, ducklakeColumn.columnId, newDucklakeType) }
    }

    override fun setFieldType(session: ConnectorSession, tableHandle: ConnectorTableHandle, fieldPath: List<String>, type: Type)
    {
        val handle = tableHandle as DucklakeTableHandle
        val sourceType: Type = resolveFieldTrinoType(handle.tableId, handle.snapshotId, fieldPath)
        if (sourceType == type) {
            return
        }
        if (!DucklakeTypePromotion.isWidening(sourceType, type)) {
            throw TrinoException(NOT_SUPPORTED, String.format(
                    "ALTER COLUMN SET DATA TYPE supports only widening type promotions; " +
                            "%s -> %s is not allowed for field \"%s\"",
                    sourceType.displayName, type.displayName, fieldPath.joinToString(".")))
        }
        val newDucklakeType: String = typeConverter.toDucklakeType(type)
        translateCatalogExceptions { catalog.setFieldType(handle.tableId, fieldPath, newDucklakeType) }
    }

    private fun resolveFieldTrinoType(tableId: Long, snapshotId: Long, fieldPath: List<String>): Type {
        val columns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(tableId, snapshotId)
        val field: DucklakeColumn? = fieldPath.fold(null as DucklakeColumn?) { parent, name ->
            columns.firstOrNull { it.columnName == name && it.parentColumn == parent?.columnId }
                    ?: throw TrinoException(COLUMN_NOT_FOUND, "Field not found: ${fieldPath.joinToString(".")} (no '$name')")
        }
        return typeConverter.toTrinoType(checkNotNull(field) { "Empty field path" }, columns)
    }

    // Nested struct field DDL. `addField`'s parentPath includes the top-level column name (there is no
    // separate ColumnHandle); `dropField` supplies the column separately, so we prepend its name.
    override fun addField(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            parentPath: List<String>,
            fieldName: String,
            type: Type,
            ignoreExisting: Boolean)
    {
        val handle = tableHandle as DucklakeTableHandle
        val fieldSpec: TableColumnSpec = writes.toColumnSpec(fieldName, type, true)
        translateCatalogExceptions { catalog.addField(handle.tableId, parentPath, fieldSpec, ignoreExisting) }
    }

    override fun dropField(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            column: ColumnHandle,
            fieldPath: List<String>)
    {
        val handle = tableHandle as DucklakeTableHandle
        val ducklakeColumn = column as DucklakeColumnHandle
        val fullPath: List<String> = listOf(ducklakeColumn.columnName) + fieldPath
        translateCatalogExceptions { catalog.dropField(handle.tableId, fullPath) }
    }

    override fun beginInsert(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            columns: List<ColumnHandle>,
            retryMode: RetryMode): ConnectorInsertTableHandle
    {
        val handle = tableHandle as DucklakeTableHandle

        rejectVirtualColumnWrites(columns)

        return writes.beginInsert(handle, columns.map { it as DucklakeColumnHandle })
    }

    override fun finishInsert(
            session: ConnectorSession,
            insertHandle: ConnectorInsertTableHandle,
            sourceTableHandles: List<ConnectorTableHandle>,
            fragments: Collection<Slice>,
            computedStatistics: Collection<ComputedStatistics>): Optional<ConnectorOutputMetadata>
    {
        writes.finishInsert(insertHandle as DucklakeWritableTableHandle, fragments)
        return Optional.empty()
    }

    override fun beginCreateTable(
            session: ConnectorSession,
            tableMetadata: ConnectorTableMetadata,
            layout: Optional<ConnectorTableLayout>,
            retryMode: RetryMode,
            replace: Boolean): ConnectorOutputTableHandle
    {
        if (replace) {
            throw TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables")
        }
        return writes.beginCreateTable(tableMetadata)
    }

    override fun finishCreateTable(
            session: ConnectorSession,
            tableHandle: ConnectorOutputTableHandle,
            fragments: Collection<Slice>,
            computedStatistics: Collection<ComputedStatistics>): Optional<ConnectorOutputMetadata>
    {
        writes.finishCreateTable(tableHandle as DucklakeWritableTableHandle, fragments)
        return Optional.empty()
    }

    /** Transaction rollback: drop the tables a CTAS created but never published, see [DucklakeWriteMetadata]. */
    internal fun dropUnpublishedCreatedTables() = writes.dropUnpublishedCreatedTables()

    /**
     * Reject unsupported table-scoped formats before reading data. An unset format inherits parquet.
     */
    private fun validateDataFileFormatIsParquet(dataFileFormat: String?, tableName: SchemaTableName) {
        if (dataFileFormat != null
                && !DucklakeSessionProperties.FORMAT_PARQUET.equals(dataFileFormat, ignoreCase = true)) {
            throw TrinoException(NOT_SUPPORTED, String.format(
                    "Table %s.%s declares data_file_format '%s', but this connector reads only parquet data " +
                            "files. The duckdb/vortex/lance data-file formats were removed (see brikk/duckbridge). " +
                            "Recreate the table as parquet to read it here.",
                    tableName.schemaName, tableName.tableName, dataFileFormat))
        }
    }

    override fun getRowChangeParadigm(session: ConnectorSession, tableHandle: ConnectorTableHandle): RowChangeParadigm =
            RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW

    override fun getMergeRowIdColumnHandle(session: ConnectorSession, tableHandle: ConnectorTableHandle): ColumnHandle =
            DucklakeColumnHandle.rowIdColumnHandle()

    /**
     * Route every source data file's row changes to one writer. Without this, Trino distributes
     * merge rows across writer sinks and several sinks can each emit a delete file for the same
     * data_file_id; upstream's file-list LEFT JOIN then scans surviving rows once per delete file.
     */
    override fun getUpdateLayout(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle): Optional<ConnectorPartitioningHandle> {
        val table = tableHandle as DucklakeTableHandle
        val ranges = catalog.getDataFiles(table.tableId, table.snapshotId)
                .associateBy { it.dataFileId } // catalog join can repeat a file for legacy duplicate deletes
                .values
                .filter { it.recordCount > 0 }
                .sortedBy { it.dataFileId }
                .map { file ->
                    DucklakeUpdatePartitioningHandle.FilePositions(
                            file.dataFileId,
                            file.recordCount)
                }
        return Optional.of(DucklakeUpdatePartitioningHandle(ranges))
    }

    override fun beginMerge(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            updateCaseColumns: Map<Int, Collection<ColumnHandle>>,
            retryMode: RetryMode): ConnectorMergeTableHandle
    {
        val handle = tableHandle as DucklakeTableHandle

        updateCaseColumns.values.forEach { rejectVirtualColumnWrites(it) }

        return writes.beginMerge(handle)
    }

    override fun finishDucklakeMerge(
            mergeTableHandle: ConnectorMergeTableHandle,
            fragments: Collection<Slice>) =
            writes.finishMerge(mergeTableHandle as DucklakeMergeTableHandle, fragments)

    override fun listViews(session: ConnectorSession, schemaName: Optional<String>): List<SchemaTableName> =
            views.listViews(session, schemaName)

    override fun isView(session: ConnectorSession, viewName: SchemaTableName): Boolean = views.isView(session, viewName)

    override fun getView(session: ConnectorSession, viewName: SchemaTableName): Optional<ConnectorViewDefinition> =
            views.getView(session, viewName)

    override fun getViews(session: ConnectorSession, schemaName: Optional<String>): Map<SchemaTableName, ConnectorViewDefinition> =
            views.getViews(session, schemaName)

    override fun createView(
            session: ConnectorSession,
            viewName: SchemaTableName,
            definition: ConnectorViewDefinition,
            viewProperties: Map<String, Any?>,
            replace: Boolean) =
            views.createView(viewName, definition, replace)

    override fun renameView(session: ConnectorSession, source: SchemaTableName, target: SchemaTableName) =
            views.renameView(session, source, target)

    override fun setViewComment(session: ConnectorSession, viewName: SchemaTableName, comment: Optional<String>) =
            views.setViewComment(session, viewName, comment)

    override fun setViewColumnComment(session: ConnectorSession, viewName: SchemaTableName, columnName: String, comment: Optional<String>) =
            views.setViewColumnComment(session, viewName, columnName, comment)

    override fun dropView(session: ConnectorSession, viewName: SchemaTableName) = views.dropView(session, viewName)

    companion object {
        private val EXPOSED_VIRTUAL_COLUMNS: List<VirtualKind> = VirtualKind.values().toList()

        /**
         * Reject any virtual column handle in a write column list with NOT_SUPPORTED.
         * Routed through DucklakeColumnHandle.isVirtual() (the single sentinel check) so
         * the guard can never drift from VirtualKind. The MERGE row-id handle (-100) is
         * NOT virtual, so this never rejects it.
         */
        internal fun rejectVirtualColumnWrites(columns: Iterable<ColumnHandle>)
        {
            for (column in columns) {
                if (column is DucklakeColumnHandle && column.isVirtual()) {
                    throw TrinoException(NOT_SUPPORTED, "Virtual column cannot be written: ${column.columnName}")
                }
            }
        }
    }
}
