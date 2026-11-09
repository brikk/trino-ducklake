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
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeDataFile
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.type.TypeUtils.writeNativeValue

/**
 * Read-path behaviour that differs between the default DuckLake format-1.0 reads and the
 * experimental final-1.1 reads, selected once from [DucklakeConfig.isEnableFinal11Reads].
 */
internal sealed class DucklakeReadStrategy {
    /** Forces format/layout validation and the first catalog read in the owned read view. */
    abstract fun pinReadSession(catalog: DucklakeCatalog)

    /** Charges physical rows against the read budget; [count] is only evaluated when bounded. */
    abstract fun chargeRows(count: () -> Long)

    /** The table's column tree at [snapshotId] used to project a data file. */
    abstract fun fileColumnTree(catalog: DucklakeCatalog, trees: DucklakeColumnTreeCache, tableId: Long, snapshotId: Long): List<DucklakeColumn>

    abstract fun fileDefaults(
            catalog: DucklakeCatalog,
            table: ConnectorTableHandle,
            tree: List<DucklakeColumn>,
            typeConverter: DucklakeTypeConverter): DucklakeFileDefaults?

    abstract fun rejectUnreadableColumns(columns: List<DucklakeColumnHandle>, tree: List<DucklakeColumn>)

    /** A bounded inline-row count for count-only projections, or null to count by reading a column. */
    abstract fun inlinedRowCount(catalog: DucklakeCatalog, split: DucklakeInlinedSplit): Long?

    /** Whether a change feed of [feedType] reads the catalog's deletion events. */
    abstract fun readsDeletions(feedType: ChangeFeedType): Boolean

    /** Charges and returns the change feed's physical candidate rows (0 when unbounded). */
    abstract fun changeFeedPhysicalWork(files: List<DucklakeDataFile>, deletions: List<DucklakeChangeFeedDeletion>): Long

    /** The constant block for a column the data file does not carry. */
    abstract fun missingColumnBlock(column: DucklakeColumnHandle, split: DucklakeSplit): Block

    abstract fun rejectUnsupportedPhysicalField(columnName: String)

    /** Whether a table with no data files but an inlined-data table gets one (empty) inlined split. */
    abstract fun emitsEmptyInlinedSplit(tableHasNoDataFiles: Boolean): Boolean

    /** Whether catalog-stats file pruning is disabled in this read mode. */
    abstract val skipsCatalogStatsPruning: Boolean

    /** The released format-1.0 read path. */
    object Format10 : DucklakeReadStrategy() {
        override fun pinReadSession(catalog: DucklakeCatalog) {
            // Only final-1.1 reads pin the owned read view up front.
        }

        override fun chargeRows(count: () -> Long) {
            // Format-1.0 reads are not budgeted.
        }

        override fun fileColumnTree(catalog: DucklakeCatalog, trees: DucklakeColumnTreeCache, tableId: Long, snapshotId: Long) =
            trees.get(tableId, snapshotId)

        override fun fileDefaults(
                catalog: DucklakeCatalog,
                table: ConnectorTableHandle,
                tree: List<DucklakeColumn>,
                typeConverter: DucklakeTypeConverter): DucklakeFileDefaults? = null

        override fun rejectUnreadableColumns(columns: List<DucklakeColumnHandle>, tree: List<DucklakeColumn>) {
            // A file-resident VARIANT payload is rejected when its field is constructed (DucklakeParquetTypeUtils).
        }

        override fun inlinedRowCount(catalog: DucklakeCatalog, split: DucklakeInlinedSplit): Long? = null

        override fun readsDeletions(feedType: ChangeFeedType): Boolean = feedType != ChangeFeedType.INSERTIONS

        override fun changeFeedPhysicalWork(files: List<DucklakeDataFile>, deletions: List<DucklakeChangeFeedDeletion>): Long = 0L

        override fun missingColumnBlock(column: DucklakeColumnHandle, split: DucklakeSplit): Block
        {
            // Imported identity partitions override defaults for rows predating the column.
            val storedValue: String? = split.partitionValuesByColumnId[column.columnId]
                    ?: column.initialDefault
            if (storedValue == null) {
                return column.columnType.createNullBlock()
            }
            val origin = if (column.columnId in split.partitionValuesByColumnId) "partition value" else "initial_default"
            val nativeValue = DucklakeReadSupport.parseProjectedValue(column, storedValue, origin)
            return writeNativeValue(column.columnType, nativeValue)
        }

        override fun rejectUnsupportedPhysicalField(columnName: String) {
            // The caller projects NULL, or the partition constant, instead.
        }

        override fun emitsEmptyInlinedSplit(tableHasNoDataFiles: Boolean): Boolean = tableHasNoDataFiles

        override val skipsCatalogStatsPruning: Boolean = false
    }

    /** The opt-in final-1.1 read path: bounded work, qualified defaults, no NULL substitution. */
    class Final11(private val limits: DucklakeReadWorkLimits) : DucklakeReadStrategy() {
        override fun pinReadSession(catalog: DucklakeCatalog) {
            catalog.currentSnapshotId
        }

        override fun chargeRows(count: () -> Long) = limits.rows(count())

        override fun fileColumnTree(catalog: DucklakeCatalog, trees: DucklakeColumnTreeCache, tableId: Long, snapshotId: Long) =
            catalog.getAllColumnsWithParentage(tableId, snapshotId)

        override fun fileDefaults(
                catalog: DucklakeCatalog,
                table: ConnectorTableHandle,
                tree: List<DucklakeColumn>,
                typeConverter: DucklakeTypeConverter): DucklakeFileDefaults? =
            if (table is DucklakeTableHandle)
                DucklakeFileDefaults(catalog, table, tree, typeConverter) else null

        override fun rejectUnreadableColumns(columns: List<DucklakeColumnHandle>, tree: List<DucklakeColumn>) {
            val requested = columns.map { it.columnId }.toSet()
            val byId = tree.associateBy { it.columnId }
            for (variant in tree.filter { it.columnType.equals("variant", ignoreCase = true) }) {
                var node = variant
                val seen = mutableSetOf<Long>()
                while (seen.add(node.columnId)) {
                    if (node.columnId in requested) throw TrinoException(NOT_SUPPORTED, "Unsupported final-1.1 VARIANT Parquet payload")
                    val parent = node.parentColumn ?: break
                    node = byId[parent] ?: throw TrinoException(GENERIC_INTERNAL_ERROR, "Missing parent in final-1.1 Parquet column tree")
                }
                check(node.parentColumn == null || node.columnId !in seen) { "Cycle in final-1.1 Parquet column tree" }
            }
        }

        override fun inlinedRowCount(catalog: DucklakeCatalog, split: DucklakeInlinedSplit): Long =
            catalog.countInlinedRows(split.tableId, split.schemaVersion, split.snapshotId).also { limits.rows(it) }

        override fun readsDeletions(feedType: ChangeFeedType): Boolean = true

        override fun changeFeedPhysicalWork(files: List<DucklakeDataFile>, deletions: List<DucklakeChangeFeedDeletion>): Long {
            val deleteRows = deletions.fold(0L) { n, deletion -> Math.addExact(n, deletion.recordCount) }
            val insertRows = files.distinctBy { it.dataFileId }.fold(0L) { n, file -> Math.addExact(n, file.recordCount) }
            val total = Math.addExact(deleteRows, insertRows)
            limits.rows(total)
            limits.events(total)
            return total
        }

        override fun missingColumnBlock(column: DucklakeColumnHandle, split: DucklakeSplit): Block {
            if (column.initialDefault != null && column.columnId !in split.partitionValuesByColumnId) {
                throw TrinoException(NOT_SUPPORTED,
                    "Unqualified final-1.1 missing-file-column initial default: ${column.columnName}; " +
                        "requires decoded catalog default projection")
            }
            val stored = split.partitionValuesByColumnId[column.columnId] ?: return column.columnType.createNullBlock()
            return writeNativeValue(column.columnType, DucklakeReadSupport.parseProjectedValue(column, stored, "partition value"))
        }

        override fun rejectUnsupportedPhysicalField(columnName: String) {
            throw TrinoException(NOT_SUPPORTED,
                "Unsupported physical Parquet field for final-1.1 column $columnName; cannot substitute NULL")
        }

        // Registry entries can describe absent physical tables: keep zero splits rather than
        // inventing an empty payload table to open.
        override fun emitsEmptyInlinedSplit(tableHasNoDataFiles: Boolean): Boolean = false

        // The legacy stats DTO has no endpoint exactness: the public catalog range API
        // deliberately retains EVERY active ID. Reuse the already paged file enumeration rather
        // than transferring that same set through its old list SQL.
        override val skipsCatalogStatsPruning: Boolean = true
    }

    companion object {
        fun of(config: DucklakeConfig): DucklakeReadStrategy =
            if (config.isEnableFinal11Reads()) Final11(DucklakeReadWorkLimits(config)) else Format10
    }
}
