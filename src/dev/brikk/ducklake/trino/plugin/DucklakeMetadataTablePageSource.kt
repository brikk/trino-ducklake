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
import dev.brikk.ducklake.catalog.DucklakeSnapshot
import dev.brikk.ducklake.catalog.DucklakeSnapshotChange
import io.airlift.slice.Slices
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.InMemoryRecordSet
import io.trino.spi.connector.RecordPageSource
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.spi.type.Type
import java.time.Instant

/** Page sources for the `$files`, `$snapshots`, `$current_snapshot` and `$snapshot_changes` tables. */
internal class DucklakeMetadataTablePageSource(private val catalog: DucklakeCatalog) {
    fun create(metadataSplit: DucklakeMetadataSplit, projectedColumns: List<DucklakeColumnHandle>): ConnectorPageSource
    {
        val projectedTypes: List<Type> = projectedColumns.map { it.columnType }
        val rows: List<Map<String, Any?>> = when (metadataSplit.metadataTableType) {
            DucklakeMetadataTableType.FILES -> buildFilesRows(metadataSplit)
            DucklakeMetadataTableType.SNAPSHOTS -> buildSnapshotRows(catalog.listSnapshots())
            DucklakeMetadataTableType.CURRENT_SNAPSHOT -> catalog.getSnapshot(metadataSplit.snapshotId)
                    ?.let { snapshot -> buildSnapshotRows(listOf(snapshot)) }
                    ?: emptyList()
            DucklakeMetadataTableType.SNAPSHOT_CHANGES -> buildSnapshotChangeRows(catalog.listSnapshotChanges())
        }
        val projectedRows: List<List<Any?>> = rows.map { row -> projectMetadataRow(row, projectedColumns, projectedTypes) }
        return RecordPageSource(InMemoryRecordSet(projectedTypes, projectedRows))
    }

    private fun buildFilesRows(metadataSplit: DucklakeMetadataSplit): List<Map<String, Any?>> =
        catalog.getDataFiles(metadataSplit.baseTableId, metadataSplit.snapshotId).map { file ->
            linkedMapOf(
                "data_file_id" to file.dataFileId,
                "path" to file.path,
                "file_format" to file.fileFormat,
                "record_count" to file.recordCount,
                "file_size_bytes" to file.fileSizeBytes,
                "row_id_start" to file.rowIdStart,
                "partition_id" to file.partitionId,
                "delete_file_path" to file.deleteFilePath)
        }

    private companion object {
        fun buildSnapshotRows(snapshots: List<DucklakeSnapshot>): List<Map<String, Any?>> =
            snapshots.map { snapshot ->
                linkedMapOf(
                    "snapshot_id" to snapshot.snapshotId,
                    "snapshot_time" to snapshot.snapshotTime,
                    "schema_version" to snapshot.schemaVersion,
                    "next_catalog_id" to snapshot.nextCatalogId,
                    "next_file_id" to snapshot.nextFileId)
            }

        fun buildSnapshotChangeRows(changes: List<DucklakeSnapshotChange>): List<Map<String, Any?>> =
            changes.map { change ->
                linkedMapOf(
                    "snapshot_id" to change.snapshotId,
                    "changes_made" to change.changesMade,
                    "author" to change.author,
                    "commit_message" to change.commitMessage,
                    "commit_extra_info" to change.commitExtraInfo)
            }

        fun projectMetadataRow(row: Map<String, Any?>, columns: List<DucklakeColumnHandle>, types: List<Type>): List<Any?> =
            columns.indices.map { index -> toNativeMetadataValue(row[columns[index].columnName], types[index]) }

        fun toNativeMetadataValue(value: Any?, type: Type): Any?
        {
            if (value == null) {
                return null
            }
            if (type == TIMESTAMP_TZ_MILLIS) {
                val instant: Instant = value as Instant
                return packDateTimeWithZone(instant.toEpochMilli(), UTC_KEY)
            }
            if (type == BIGINT || type == INTEGER) {
                return value
            }
            return Slices.utf8Slice(value.toString())
        }
    }
}
