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
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeCommitOutcomeUnknownException
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakeException
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.DucklakePostCommitException
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeSortKey
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.PartitionFieldSpec
import dev.brikk.ducklake.catalog.TableColumnSpec
import dev.brikk.ducklake.catalog.TableLocationSpec
import dev.brikk.ducklake.trino.plugin.DucklakeCatalogExceptions.translateCatalogException
import dev.brikk.ducklake.trino.plugin.DucklakeCatalogExceptions.translateCatalogExceptions
import io.airlift.json.JsonCodec
import io.airlift.log.Logger
import io.airlift.slice.Slice
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND
import io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnMetadata
import io.trino.spi.connector.ConnectorTableMetadata
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.type.ArrayType
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.Type
import java.util.Locale
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

/** Table creation, writable/merge handles and the commit of INSERT, CTAS and MERGE fragments. */
internal class DucklakeWriteMetadata(
        private val catalog: DucklakeCatalog,
        private val typeConverter: DucklakeTypeConverter,
        private val fragmentCodec: JsonCodec<DucklakeWriteFragment>,
        private val deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment>,
        private val pathResolver: DucklakePathResolver,
        private val temporalPartitionEncoding: DucklakeTemporalPartitionEncoding)
{
    /**
     * Tables created by [beginCreateTable] in this transaction whose CTAS data is not known to be
     * published, keyed by table id. The catalog commits `createTable` and the data `commitInsert`
     * as separate snapshots, so a failed CTAS would otherwise leave an empty table behind.
     */
    private val unpublishedCreatedTables: MutableMap<Long, SchemaTableName> = ConcurrentHashMap()

    fun createTable(tableMetadata: ConnectorTableMetadata)
    {
        val tableName: SchemaTableName = tableMetadata.table

        val columnSpecs: List<TableColumnSpec> = tableMetadata.columns
                .map { column -> toColumnSpec(column.name, column.type, column.isNullable) }

        val partitionFields: List<PartitionFieldSpec> = DucklakeTableProperties.getPartitionFields(tableMetadata.properties)
        validateIdentityPartitionTypes(tableMetadata.columns, partitionFields)
        val partitionSpec: List<PartitionFieldSpec>? = partitionFields.ifEmpty { null }

        val location: TableLocationSpec? = DucklakeTableProperties.getLocation(tableMetadata.properties).orElse(null)

        val declaredFormat: String? = DucklakeTableProperties.getDataFileFormat(tableMetadata.properties)

        translateCatalogExceptions {
            catalog.createTable(
                tableName.schemaName,
                tableName.tableName,
                columnSpecs,
                partitionSpec,
                location,
                declaredFormat
            )
        }
    }

    /**
     * Reject an IDENTITY partition over a type the connector can't encode to a partition-value
     * string, at CREATE TABLE time — otherwise the table is created and its first INSERT fails
     * (DucklakePartitionComputer.computeIdentityValue would throw). Only identity fields are
     * checked here; bucket/temporal transforms have their own column-type expectations.
     */
    private fun validateIdentityPartitionTypes(
            columns: List<ColumnMetadata>,
            partitionFields: List<PartitionFieldSpec>) {
        if (partitionFields.isEmpty()) {
            return
        }
        val typeByName: Map<String, Type> = columns.associate { it.name.lowercase(Locale.ROOT) to it.type }
        for (field in partitionFields.filter { it.transform == DucklakePartitionTransform.IDENTITY }) {
            // Unknown column: leave it to the catalog's own validation to report.
            val type: Type = typeByName[field.columnName.lowercase(Locale.ROOT)] ?: continue
            if (!DucklakePartitionComputer.isIdentityPartitionable(type)) {
                throw TrinoException(INVALID_TABLE_PROPERTY,
                        "Identity partitioning is not supported for column '${field.columnName}' of type $type")
            }
        }
    }

    fun toColumnSpec(name: String, trinoType: Type, nullable: Boolean): TableColumnSpec
    {
        val ducklakeType: String = typeConverter.toDucklakeType(trinoType)

        if (trinoType is ArrayType) {
            val children: List<TableColumnSpec> = listOf(
                    toColumnSpec("element", trinoType.elementType, true))
            return TableColumnSpec(name, ducklakeType, nullable, children)
        }
        if (trinoType is RowType) {
            val children: List<TableColumnSpec> = trinoType.fields
                    .map { field -> toColumnSpec(
                            field.name.orElseThrow { TrinoException(NOT_SUPPORTED, "Anonymous row fields not supported") },
                            field.type,
                            true) }
            return TableColumnSpec(name, ducklakeType, nullable, children)
        }
        if (trinoType is MapType) {
            val children: List<TableColumnSpec> = listOf(
                    toColumnSpec("key", trinoType.keyType, false),
                    toColumnSpec("value", trinoType.valueType, true))
            return TableColumnSpec(name, ducklakeType, nullable, children)
        }

        return TableColumnSpec.leaf(name, ducklakeType, nullable)
    }

    fun beginInsert(handle: DucklakeTableHandle, ducklakeColumns: List<DucklakeColumnHandle>): DucklakeWritableTableHandle
    {
        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(handle.tableId, handle.snapshotId)

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(handle.tableId, handle.snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = activePartitionSpecOf(partitionSpecs)

        val tableDataPath: String = resolveTableDataPath(handle.schemaName, handle.tableName, handle.snapshotId)

        val fileFormat: String = DucklakeSessionProperties.FORMAT_PARQUET

        return DucklakeWritableTableHandle(
                handle.schemaName,
                handle.tableName,
                handle.tableId,
                ducklakeColumns,
                allCatalogColumns,
                tableDataPath,
                activePartitionSpec,
                temporalPartitionEncoding,
                fileFormat,
                resolveWriteSortColumns(
                        handle.tableId, handle.snapshotId, ducklakeColumns, activePartitionSpec, fileFormat))
    }

    /**
     * Resolve the honored write-side sort prefix for a page sink, or empty when sorting must
     * stay off. This is the ONE place the sorted-write gate is enforced: we return a non-empty
     * list ONLY when
     *
     *   1. the table is UNPARTITIONED (partitioned + sorted falls back to unsorted — the sink
     *      routes rows per partition and a global sort across partitions is out of scope), and
     *   2. the data file format is parquet, and
     *   3. the table's catalog sort spec resolves to at least one leading column via the exact
     *      prefix rules ([DucklakeSortPropertyMapper.resolveHonoredPrefix]).
     *
     * Any failing condition yields [emptyList], disabling sort buffering.
     */
    private fun resolveWriteSortColumns(
            tableId: Long,
            snapshotId: Long,
            columns: List<DucklakeColumnHandle>,
            activePartitionSpec: Optional<DucklakePartitionSpec>,
            fileFormat: String): List<DucklakeSortColumn>
    {
        if (activePartitionSpec.isPresent) {
            return emptyList()
        }
        if (!DucklakeSessionProperties.FORMAT_PARQUET.equals(fileFormat, ignoreCase = true)) {
            return emptyList()
        }
        val sortKeys: List<DucklakeSortKey> = catalog.getSortKeys(tableId, snapshotId)
        if (sortKeys.isEmpty()) {
            return emptyList()
        }
        // Channel index = position of the column in the sink's block layout (handle.columns).
        // First occurrence wins if a name somehow repeats; lowercased to match the mapper.
        val channelByLowercaseName: MutableMap<String, Int> = HashMap()
        for ((index, column) in columns.withIndex()) {
            channelByLowercaseName.putIfAbsent(column.columnName.lowercase(Locale.ROOT), index)
        }
        val resolved: List<ResolvedSortColumn> =
                DucklakeSortPropertyMapper.resolveHonoredPrefix(sortKeys, channelByLowercaseName.keys)
        return resolved.map { column ->
            DucklakeSortColumn(channelByLowercaseName.getValue(column.lowercaseColumnName), column.order)
        }
    }

    fun finishInsert(handle: DucklakeWritableTableHandle, fragments: Collection<Slice>)
    {
        val writeFragments: List<DucklakeWriteFragment> = deserializeFragments(fragments)

        if (!writeFragments.isEmpty()) {
            translateCatalogExceptions { catalog.commitInsert(handle.tableId, writeFragments) }
        }
    }

    fun beginCreateTable(tableMetadata: ConnectorTableMetadata): DucklakeWritableTableHandle
    {
        val tableName: SchemaTableName = tableMetadata.table

        createTable(tableMetadata)

        val snapshotId: Long = catalog.currentSnapshotId
        val table: DucklakeTable = catalog.getTable(tableName.schemaName, tableName.tableName, snapshotId)
            ?: throw TrinoException(GENERIC_INTERNAL_ERROR, "Table was not created: $tableName")
        unpublishedCreatedTables[table.tableId] = tableName

        // For the WRITE handle, a column's type must match the blocks the engine will stream
        // from the CTAS source — not the catalog round-trip. The two agree for every type
        // EXCEPT timestamptz: DuckLake stores it micros-only, so the round-trip always reports
        // precision 6 (a long / Fixed12 tstz), but a CTAS streams the source's actual precision
        // (e.g. the default precision-3 short / LongArray tstz). Use the originally-declared type so
        // the writer's isShort branch matches the block; the stored value still widens to micros
        // and reads back as precision 6.
        val declaredTstzByName: Map<String, Type> = tableMetadata.columns
                .filter { it.type is TimestampWithTimeZoneType }
                .associate { it.name to it.type }
        val catalogColumns: List<DucklakeColumn> = catalog.getTableColumns(table.tableId, snapshotId)
        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(table.tableId, snapshotId)
        val columnHandles: List<DucklakeColumnHandle> = catalogColumns
                .filter { col -> col.parentColumn == null }
                .map { col -> DucklakeColumnHandle(
                        col.columnId,
                        col.columnName,
                        declaredTstzByName[col.columnName] ?: typeConverter.toTrinoType(col, allCatalogColumns),
                        col.nullsAllowed) }

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(table.tableId, snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = activePartitionSpecOf(partitionSpecs)

        val tableDataPath: String = resolveTableDataPath(tableName.schemaName, tableName.tableName, snapshotId)

        val fileFormat: String = DucklakeSessionProperties.FORMAT_PARQUET

        return DucklakeWritableTableHandle(
                tableName.schemaName,
                tableName.tableName,
                table.tableId,
                columnHandles,
                allCatalogColumns,
                tableDataPath,
                activePartitionSpec,
                temporalPartitionEncoding,
                fileFormat,
                // A freshly-created table generally has no sort spec yet (Trino has no
                // `sorted_by` property; DuckDB's SET SORTED BY runs post-create), so this is
                // normally empty — but resolving here keeps CTAS consistent with INSERT for the
                // rare case a sort spec is already visible at this snapshot.
                resolveWriteSortColumns(
                        table.tableId, snapshotId, columnHandles, activePartitionSpec, fileFormat))
    }

    fun finishCreateTable(handle: DucklakeWritableTableHandle, fragments: Collection<Slice>)
    {
        val writeFragments: List<DucklakeWriteFragment> = deserializeFragments(fragments)

        // Only a catalog failure that proves the commit rolled back keeps the table droppable.
        // An unknown or acknowledged COMMIT, or any non-catalog failure, may have published the
        // data, and published output must be retained for the operator to reconcile.
        var mayHavePublished = true
        try {
            if (writeFragments.isNotEmpty()) {
                catalog.commitInsert(handle.tableId, writeFragments)
            }
        }
        catch (e: DucklakeException) {
            mayHavePublished = e is DucklakeCommitOutcomeUnknownException || e is DucklakePostCommitException
            throw translateCatalogException(e)
        }
        finally {
            if (mayHavePublished) {
                unpublishedCreatedTables.remove(handle.tableId)
            }
        }
    }

    /**
     * Rollback of a transaction whose CTAS never published its data: drop each table
     * [beginCreateTable] created. A table is left in place, with a warning, when it is no longer
     * the one this transaction created or another writer has committed rows into it since.
     * Every table is attempted; the first failure is rethrown with the others suppressed.
     */
    fun dropUnpublishedCreatedTables()
    {
        val failures: MutableList<RuntimeException> = mutableListOf()
        for ((tableId, tableName) in unpublishedCreatedTables) {
            try {
                dropUnpublishedCreatedTable(tableId, tableName)
            }
            catch (e: RuntimeException) {
                failures += e
            }
        }
        unpublishedCreatedTables.clear()
        failures.firstOrNull()?.let { first ->
            failures.drop(1).forEach(first::addSuppressed)
            throw first
        }
    }

    private fun dropUnpublishedCreatedTable(tableId: Long, tableName: SchemaTableName)
    {
        val snapshotId: Long = catalog.currentSnapshotId
        val table: DucklakeTable? = catalog.getTable(tableName.schemaName, tableName.tableName, snapshotId)
        if (table == null || table.tableId != tableId) {
            log.warn("CTAS rollback: %s (table id %d) is no longer the table this query created; not dropping it",
                    tableName, tableId)
            return
        }
        val hasRows = catalog.getDataFiles(tableId, snapshotId).isNotEmpty()
                || catalog.getInlinedDataInfos(tableId, snapshotId).any { it.hasLiveRows }
        if (hasRows) {
            log.warn("CTAS rollback: %s holds rows committed by another writer; not dropping it", tableName)
            return
        }
        translateCatalogExceptions { catalog.dropTable(tableName.schemaName, tableName.tableName) }
        log.info("CTAS rollback: dropped %s, created by the failed query", tableName)
    }

    private fun deserializeFragments(fragments: Collection<Slice>): List<DucklakeWriteFragment>
    {
        return fragments.map { fragment -> fragmentCodec.fromJson(fragment.bytes) }
    }

    private fun resolveTableDataPath(schemaName: String, tableName: String, snapshotId: Long): String
    {
        val schema: DucklakeSchema = catalog.getSchema(schemaName, snapshotId)
            ?: throw TrinoException(SCHEMA_NOT_FOUND, "Cannot resolve data path: schema not found: $schemaName")
        val table: DucklakeTable = catalog.getTable(schemaName, tableName, snapshotId)
            ?: throw TrinoException(TABLE_NOT_FOUND, "Cannot resolve data path: table not found: $schemaName.$tableName")

        return pathResolver.resolveTableDataPath(schema, table)
    }

    fun beginMerge(handle: DucklakeTableHandle): DucklakeMergeTableHandle
    {
        // Gate DELETE/UPDATE/MERGE on tables that still hold live inlined rows. The merge scan
        // requests the $row_id of every scanned row, but inlined rows have no data file / file
        // position, and this connector's merge sink only writes positional deletes
        // keyed by (data_file_id, position) — it has no way to tombstone an inlined row. Rather
        // than fail mid-scan with an opaque "Column not found: $row_id" from the inlined page
        // source, reject early with guidance. Flushing the inlined data to files (DuckLake's
        // flush_inlined_data, or data_inlining_row_limit = 0 then a rewrite) makes the rows
        // file-resident and DELETE/UPDATE/MERGE work normally.
        if (catalog.getInlinedDataInfos(handle.tableId, handle.snapshotId).any { it.hasLiveRows }) {
            throw TrinoException(NOT_SUPPORTED,
                    "DELETE/UPDATE/MERGE is not supported while ${handle.schemaName}.${handle.tableName} has " +
                            "inlined rows (rows written below DuckLake's data_inlining_row_limit). Flush the " +
                            "inlined data to files first (e.g. DuckLake's flush_inlined_data procedure, or set " +
                            "data_inlining_row_limit = 0).")
        }

        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(handle.tableId, handle.snapshotId)
        val ducklakeColumns: List<DucklakeColumnHandle> = catalog.getTableColumns(handle.tableId, handle.snapshotId)
                .filter { col -> col.parentColumn == null }
                .map { col -> DucklakeColumnHandle(
                        col.columnId,
                        col.columnName,
                        typeConverter.toTrinoType(col, allCatalogColumns),
                        col.nullsAllowed) }

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(handle.tableId, handle.snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = activePartitionSpecOf(partitionSpecs)

        val tableDataPath: String = resolveTableDataPath(handle.schemaName, handle.tableName, handle.snapshotId)

        val insertHandle = DucklakeWritableTableHandle(
                handle.schemaName,
                handle.tableName,
                handle.tableId,
                ducklakeColumns,
                allCatalogColumns,
                tableDataPath,
                activePartitionSpec,
                temporalPartitionEncoding,
                DucklakeSessionProperties.FORMAT_PARQUET)

        return DucklakeMergeTableHandle(handle, insertHandle, dataFileRanges(handle, tableDataPath))
    }

    /**
     * Source file bounds, optional logical starts, and paths. getDataFiles LEFT-JOINs
     * ducklake_delete_file, returning one row per (data_file, active delete_file) pair —
     * group by data_file_id to deduplicate and collect resolved delete-file paths. The
     * merge sink uses these paths to read prior-active positions and union them with
     * this commit's new deletes; superseding files must preserve prior deletions or rows resurrect.
     */
    private fun dataFileRanges(handle: DucklakeTableHandle, tableDataPath: String): List<DucklakeMergeTableHandle.DataFileRange>
    {
        val dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(handle.tableId, handle.snapshotId)
        val primaryByFileId: LinkedHashMap<Long, DucklakeDataFile> = linkedMapOf()
        val deletePathsByFileId: LinkedHashMap<Long, MutableList<String>> = linkedMapOf()
        for (df in dataFiles) {
            primaryByFileId.putIfAbsent(df.dataFileId, df)
            df.deleteFilePath?.let { deleteFilePath ->
                val resolved: String = pathResolver.resolveFilePath(
                        deleteFilePath,
                        df.deleteFilePathIsRelative ?: false,
                        tableDataPath)
                deletePathsByFileId
                        .computeIfAbsent(df.dataFileId) { _ -> mutableListOf() }
                        .add(resolved)
            }
        }
        return primaryByFileId.values
                .map { df -> DucklakeMergeTableHandle.DataFileRange(
                        df.dataFileId,
                        df.rowIdStart,
                        df.recordCount,
                        deletePathsByFileId.getOrDefault(df.dataFileId, emptyList()),
                        pathResolver.resolveFilePath(df.path, df.pathIsRelative, tableDataPath)) }
    }

    fun finishMerge(mergeHandle: DucklakeMergeTableHandle, fragments: Collection<Slice>)
    {
        val tableHandle: DucklakeTableHandle = mergeHandle.tableHandle

        val deleteFragments: MutableList<DucklakeDeleteFragment> = mutableListOf()
        val insertFragments: MutableList<DucklakeWriteFragment> = mutableListOf()

        for (fragment in fragments) {
            try {
                val tagged = DucklakeMergeFragment.fromSlice(fragment)
                when (tagged.kind) {
                    DucklakeMergeFragment.Kind.DELETE -> deleteFragments.add(deleteFragmentCodec.fromJson(tagged.payload))
                    DucklakeMergeFragment.Kind.INSERT -> insertFragments.add(fragmentCodec.fromJson(tagged.payload))
                }
            }
            catch (e: RuntimeException) {
                throw e as? TrinoException
                    ?: TrinoException(GENERIC_INTERNAL_ERROR, "Failed to deserialize merge fragment", e)
            }
        }

        // Commit atomically in a single snapshot — critical for UPDATE (delete+insert must be atomic)
        if (!deleteFragments.isEmpty() && !insertFragments.isEmpty()) {
            // The planning snapshot lets the catalog abort (non-retryably) if another writer
            // committed a delete file for one of our data files after we planned — otherwise our
            // replacement delete file would supersede theirs without its positions.
            translateCatalogExceptions { catalog.commitMerge(tableHandle.tableId, deleteFragments, insertFragments, tableHandle.snapshotId) }
        }
        else if (!deleteFragments.isEmpty()) {
            translateCatalogExceptions { catalog.commitDelete(tableHandle.tableId, deleteFragments, tableHandle.snapshotId) }
        }
        else if (!insertFragments.isEmpty()) {
            translateCatalogExceptions { catalog.commitInsert(tableHandle.tableId, insertFragments) }
        }
    }

    private companion object {
        val log: Logger = Logger.get(DucklakeWriteMetadata::class.java)
    }
}
