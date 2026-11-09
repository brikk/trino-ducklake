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
import io.airlift.log.Logger
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.type.Type

/**
 * Page sources for inlined-data splits. Rows are materialized in memory, so virtual columns are
 * woven in per row here (each row needs its own begin_snapshot for `$snapshot_id`).
 */
internal class DucklakeInlinedPageSourceFactory(
        private val catalog: DucklakeCatalog,
        private val typeConverter: DucklakeTypeConverter,
        private val readStrategy: DucklakeReadStrategy,
        private val inlinedRows: DucklakeInlinedRowCodec.Selector) {
    fun create(inlinedSplit: DucklakeInlinedSplit, ducklakeColumns: List<DucklakeColumnHandle>): ConnectorPageSource
    {
        inlinedRows.controlledReads()?.let { return createPaged(inlinedSplit, ducklakeColumns, it) }

        // Real (catalog-resident) columns are read from the inlined table; virtuals are woven in
        // per row below. Inlined data has no backing file, so $path / $file_row_number / $row_id
        // are NULL; $snapshot_id is each row's own begin_snapshot (inlined rows are versioned
        // catalog rows, each with its own begin_snapshot — so this is genuinely per-row).
        val realColumns: List<DucklakeColumnHandle> = ducklakeColumns.filterNot { it.isVirtual() }
        val needSnapshot: Boolean = ducklakeColumns.any { it.virtualKind() in INLINE_SNAPSHOT_KINDS }
        val inlineCount: Long? = readStrategy.inlinedRowCount(catalog, inlinedSplit)

        val beginSnapshots: List<Long>? = if (needSnapshot)
                catalog.readInlinedBeginSnapshots(inlinedSplit.tableId, inlinedSplit.schemaVersion, inlinedSplit.snapshotId)
            else null

        // Read the real-column values (if any) and establish the row count.
        val tableColumns: List<DucklakeColumn> = catalog.getTableColumns(inlinedSplit.tableId, inlinedSplit.snapshotId)
        val columnById: Map<Long, DucklakeColumn> = tableColumns.associateBy { it.columnId }
        val realQueryColumns: List<DucklakeColumn> = realColumns.map { handle ->
                columnById[handle.columnId]
                        ?: throw IllegalStateException("Column not found in table metadata: ${handle.columnName}")
        }

        val convertedRealRows: List<List<Any?>>
        val rowCount: Int
        if (realQueryColumns.isNotEmpty()) {
            val codec = inlinedRows.codec()
            val rawRows: List<List<Any?>> = codec.readRows(inlinedSplit, realQueryColumns)
            convertedRealRows = codec.convertRows(inlinedSplit, realColumns, realColumns.map { it.columnType }, rawRows)
            rowCount = convertedRealRows.size
        }
        else if (beginSnapshots != null) {
            // No real columns, but $snapshot_id was requested → its row count + values suffice.
            convertedRealRows = emptyList()
            rowCount = beginSnapshots.size
        }
        else if (inlineCount != null) {
            // A true inline-row count, never a gross file/table estimate or a payload projection.
            convertedRealRows = emptyList()
            rowCount = Math.toIntExact(inlineCount)
        }
        else {
            // Only NULL virtuals (e.g. SELECT $path) or pure COUNT(*): query one column for the count.
            val countRows: List<List<Any?>> = catalog.readInlinedData(
                    inlinedSplit.tableId, inlinedSplit.schemaVersion, inlinedSplit.snapshotId, listOf(tableColumns.first()))
            convertedRealRows = emptyList()
            rowCount = countRows.size
        }

        // Assemble output rows in the requested column order, weaving virtuals per row.
        val outputTypes: List<Type> = ducklakeColumns.map { it.columnType }
        val outputRows: List<List<Any?>> = (0 until rowCount).map { r ->
            var realIdx = 0
            ducklakeColumns.map { col ->
                when (col.virtualKind()) {
                    null -> convertedRealRows[r][realIdx++]
                    VirtualKind.SNAPSHOT_ID, VirtualKind.ROW_SNAPSHOT_ID ->
                        checkNotNull(beginSnapshots) { "Inlined begin snapshots were not read" }[r]
                    else -> null  // PATH, FILE_ROW_NUMBER, ROW_ID are NULL on inlined data
                }
            }
        }

        log.debug("Created inlined page source with %d rows for tableId=%d", rowCount, inlinedSplit.tableId)
        return DucklakeNativeRowsPageSource(outputTypes, outputRows)
    }

    private fun createPaged(
            split: DucklakeInlinedSplit,
            columns: List<DucklakeColumnHandle>,
            controlled: DucklakeControlledReads): ConnectorPageSource {
        val real = columns.filterNot { it.isVirtual() }
        val target = catalog.getTableColumns(split.tableId, split.snapshotId).associateBy { it.columnId }
        real.forEach { column ->
            val fresh = target[column.columnId] ?: throw TrinoException(GENERIC_INTERNAL_ERROR, "Missing inline target field ${column.columnId}")
            if (column.columnType != typeConverter.toTrinoType(fresh.columnType)) throw TrinoException(GENERIC_INTERNAL_ERROR,
                    "Inline projection type differs from fresh target metadata for field ${column.columnId}")
        }
        // One physical version stream supplies both values and virtual snapshots. Empty IDs
        // mean identity-only, not an arbitrary unqualified payload or a separate live count.
        val rows = controlled.inlineRows(split.tableId, split.schemaVersion, split.snapshotId, real.map { it.columnId })
        val converted = inlinedRows.codec().convertRows(split, real, real.map { it.columnType }, rows.map { it.values })
        val output = rows.mapIndexed { index, row ->
            var channel = 0
            columns.map { column -> when (column.virtualKind()) {
                null -> converted[index][channel++]
                VirtualKind.SNAPSHOT_ID, VirtualKind.ROW_SNAPSHOT_ID -> row.beginSnapshot
                else -> null
            } }
        }
        return DucklakeNativeRowsPageSource(columns.map { it.columnType }, output)
    }

    private companion object {
        val log: Logger = Logger.get(DucklakeInlinedPageSourceFactory::class.java)
        val INLINE_SNAPSHOT_KINDS = setOf(VirtualKind.SNAPSHOT_ID, VirtualKind.ROW_SNAPSHOT_ID)
    }
}
