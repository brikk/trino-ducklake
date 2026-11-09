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

import com.google.common.collect.ImmutableMap.toImmutableMap
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumnStats
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.trino.plugin.DucklakeCatalogExceptions.translateCatalogExceptions
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorAnalyzeMetadata
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.statistics.ColumnStatistics
import io.trino.spi.statistics.DoubleRange
import io.trino.spi.statistics.Estimate
import io.trino.spi.statistics.TableStatisticType
import io.trino.spi.statistics.TableStatistics
import io.trino.spi.statistics.TableStatisticsMetadata
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import java.time.LocalDate

/** Optimizer statistics from DuckLake's catalog stats, and ANALYZE as a catalog stats refresh. */
internal class DucklakeTableStatistics(private val catalog: DucklakeCatalog)
{
    fun getTableStatistics(tableHandle: ConnectorTableHandle, columnHandles: () -> Map<String, ColumnHandle>): TableStatistics
    {
        // The legacy column-stat aggregate transfers file-column rows before aggregation.
        // Optional optimizer estimates must not bypass the candidate's bounded read tier.
        if (catalog.requiresBoundedStatistics()) return TableStatistics.empty()
        if (tableHandle is DucklakeMetadataTableHandle
                || tableHandle is ChangeFeedTableHandle) {
            return TableStatistics.empty()
        }

        val table = tableHandle as DucklakeTableHandle
        val dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(table.tableId, table.snapshotId)
        if (hasUnknownStatistics(table, dataFiles)) {
            return TableStatistics.empty()
        }

        val hasLiveInlinedRows = hasLiveInlinedRows(table)

        // ducklake_table_stats.record_count is the GROSS row count (upstream never decrements it on
        // DELETE; DuckDB uses record_count == live as the "no row was ever deleted" proof). The live
        // count at this snapshot is derived from the catalog the way upstream's
        // GetNetDataFileRowCount + GetNetInlinedRowCount do.
        val recordCount: Long = catalog.getLiveRowCount(table.tableId, table.snapshotId)

        val stats: TableStatistics.Builder = TableStatistics.builder()
                .setRowCount(Estimate.of(recordCount.toDouble()))

        if (recordCount == 0L) {
            return stats.build()
        }

        val handles: Map<String, ColumnHandle> = columnHandles()

        if (hasLiveInlinedRows) {
            // Conservative mode: when mixed inlined rows are present, file-level column statistics
            // cover only the Parquet portion and can become misleading. Keep only row-count stats.
            return stats.build()
        }

        val activeDataFileRowCount: Long = dataFiles.distinctBy { it.dataFileId }.sumOf { it.recordCount }
        val statsById: Map<Long, DucklakeColumnStats> = catalog.getColumnStats(table.tableId, table.snapshotId).stream()
                .collect(toImmutableMap(DucklakeColumnStats::columnId) { s -> s })

        for (handle in handles.values) {
            val column = handle as DucklakeColumnHandle
            columnStatistics(column, statsById[column.columnId], activeDataFileRowCount)
                    ?.let { stats.setColumnStatistics(column, it) }
        }

        return stats.build()
    }

    /** Whether row-level statistics at the handle's snapshot cannot be stated without a scan. */
    private fun hasUnknownStatistics(table: DucklakeTableHandle, dataFiles: List<DucklakeDataFile>): Boolean =
        // Cross-snapshot native merges can put later rows in a historical physical file.
        // Its gross record_count is neither an exact historical count nor a safe bound.
        // Do not invoke the opt-in live-count API's intentional refusal, or replace it with
        // a gross/zero estimate. The query must scan; unknown statistics are sufficient.
        (dataFiles.isNotEmpty() && table.snapshotId < catalog.currentSnapshotId)
            // Conservative mode: delete-file snapshots can make row-level table stats stale across engines.
            // Prefer unknown over wrong.
            || dataFiles.any { dataFile -> dataFile.deleteFilePath != null }
            // Same conservative policy for inlined deletes: file-level stats predate the
            // deletions, so any column min/max/null fractions can be wrong relative to the
            // surviving rows. Prefer unknown over wrong.
            || catalog.hasInlinedDeletes(table.tableId, table.snapshotId)

    private fun columnStatistics(column: DucklakeColumnHandle, colStats: DucklakeColumnStats?, activeDataFileRowCount: Long): ColumnStatistics?
    {
        if (colStats == null) {
            return null
        }
        // Null counts also mean coverage cannot be established. Omit the
        // entire column entry, just as for known incomplete coverage after schema evolution.
        val totalValueCount = colStats.totalValueCount ?: return null
        val totalNullCount = colStats.totalNullCount ?: return null
        val totalCount = totalValueCount + totalNullCount
        if (activeDataFileRowCount > 0 && totalCount != activeDataFileRowCount) {
            return null
        }

        val colBuilder: ColumnStatistics.Builder = ColumnStatistics.builder()
        if (totalCount > 0) {
            colBuilder.setNullsFraction(Estimate.of(totalNullCount.toDouble() / totalCount.toDouble()))
        }

        if (colStats.totalSizeBytes > 0) {
            colBuilder.setDataSize(Estimate.of(colStats.totalSizeBytes.toDouble()))
        }

        toDoubleRange(column.columnType, colStats)?.let { colBuilder.setRange(it) }
        return colBuilder.build()
    }

    private fun hasLiveInlinedRows(table: DucklakeTableHandle): Boolean =
            catalog.getInlinedDataInfos(table.tableId, table.snapshotId).any { it.hasLiveRows }

    // ANALYZE — refresh DuckLake's cached table-level statistics. The catalog recomputes
    // ducklake_table_stats (record_count = GROSS rows of the active files + live inlined rows, as
    // upstream defines it) and rebuilds the per-column aggregates from the authoritative per-file
    // stats (DucklakeCatalog.analyzeTable), which also tightens any min/max that incremental
    // maintenance left stale after a delete. The engine's scanned ROW_COUNT is not what
    // record_count means, so it is not written; we still declare it so ANALYZE has a plan shape.
    // We deliberately do NOT declare column statistics here: doing so would force the engine to
    // hand back typed min/max blocks we'd have to re-encode into DuckLake's string form,
    // duplicating logic the per-file aggregation already gets right.
    fun getStatisticsCollectionMetadata(tableHandle: ConnectorTableHandle): ConnectorAnalyzeMetadata
    {
        if (tableHandle is DucklakeMetadataTableHandle
                || tableHandle is ChangeFeedTableHandle) {
            throw TrinoException(NOT_SUPPORTED, "ANALYZE is not supported for this table")
        }
        val statisticsMetadata = TableStatisticsMetadata(
                emptySet(),
                setOf(TableStatisticType.ROW_COUNT),
                emptyList())
        return ConnectorAnalyzeMetadata(tableHandle, statisticsMetadata)
    }

    fun finishStatisticsCollection(tableHandle: ConnectorTableHandle)
    {
        val table = tableHandle as DucklakeTableHandle
        translateCatalogExceptions { catalog.analyzeTable(table.tableId) }
    }

    private companion object {
        val RANGE_NUMERIC_TYPES: Set<Type> = setOf(BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL)

        fun toDoubleRange(type: Type, stats: DucklakeColumnStats): DoubleRange?
        {
            val minStr: String = stats.minValue ?: return null
            val maxStr: String = stats.maxValue ?: return null

            try {
                if (type in RANGE_NUMERIC_TYPES) {
                    return DoubleRange(minStr.toDouble(), maxStr.toDouble())
                }
                if (type == DATE) {
                    val minDays: Long = LocalDate.parse(minStr).toEpochDay()
                    val maxDays: Long = LocalDate.parse(maxStr).toEpochDay()
                    return DoubleRange(minDays.toDouble(), maxDays.toDouble())
                }
            }
            catch (_: RuntimeException) {
            }
            return null
        }
    }
}
