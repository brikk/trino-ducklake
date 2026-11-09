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

import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ColumnMetadata
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType.VARCHAR

/** Names and columns of the `<table>$<suffix>` metadata tables. */
internal object DucklakeMetadataTables
{
    private const val METADATA_TABLE_SEPARATOR: String = "$"

    data class MetadataTableName(val baseTableName: String, val metadataTableType: DucklakeMetadataTableType)

    fun parseMetadataTableName(tableName: SchemaTableName): MetadataTableName?
    {
        val rawTableName: String = tableName.tableName
        val separator: Int = rawTableName.lastIndexOf(METADATA_TABLE_SEPARATOR)
        if (separator <= 0 || separator == rawTableName.length - 1) {
            return null
        }

        val baseName: String = rawTableName.substring(0, separator)
        val suffix: String = rawTableName.substring(separator + 1)
        return DucklakeMetadataTableType.fromSuffix(suffix)
                ?.let { type -> MetadataTableName(baseName, type) }
    }

    fun toColumnHandles(metadataColumns: List<ColumnMetadata>): Map<String, ColumnHandle> =
        metadataColumns.withIndex().associate { (index, column) ->
            column.name to DucklakeColumnHandle(-(index + 1L), column.name, column.type, column.isNullable)
        }

    fun getMetadataColumns(metadataTableType: DucklakeMetadataTableType): List<ColumnMetadata> =
        when (metadataTableType) {
            DucklakeMetadataTableType.FILES -> listOf(
                    column("data_file_id", BIGINT, false),
                    column("path", VARCHAR, false),
                    column("file_format", VARCHAR, false),
                    column("record_count", BIGINT, false),
                    column("file_size_bytes", BIGINT, false),
                    column("row_id_start", BIGINT, true),
                    column("partition_id", BIGINT, true),
                    column("delete_file_path", VARCHAR, true))
            DucklakeMetadataTableType.SNAPSHOTS -> snapshotColumns()
            DucklakeMetadataTableType.CURRENT_SNAPSHOT -> snapshotColumns()
            DucklakeMetadataTableType.SNAPSHOT_CHANGES -> listOf(
                    column("snapshot_id", BIGINT, false),
                    column("changes_made", VARCHAR, true),
                    column("author", VARCHAR, true),
                    column("commit_message", VARCHAR, true),
                    column("commit_extra_info", VARCHAR, true))
        }

    private fun snapshotColumns(): List<ColumnMetadata> =
            listOf(
                    column("snapshot_id", BIGINT, false),
                    column("snapshot_time", TIMESTAMP_TZ_MILLIS, false),
                    column("schema_version", BIGINT, false),
                    column("next_catalog_id", BIGINT, false),
                    column("next_file_id", BIGINT, false))

    private fun column(name: String, type: Type, nullable: Boolean): ColumnMetadata =
            ColumnMetadata.builder()
                    .setName(name)
                    .setType(type)
                    .setNullable(nullable)
                    .build()
}
