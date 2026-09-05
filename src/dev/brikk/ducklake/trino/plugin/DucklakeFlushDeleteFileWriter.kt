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

import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import io.airlift.slice.DynamicSliceOutput
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.spi.connector.InMemoryRecordSet
import io.trino.spi.connector.RecordPageSource
import io.trino.spi.type.Type
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.format.Util
import java.util.Optional
import java.util.UUID

import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.VarcharType.VARCHAR

/** Writes an upstream 3-column positional delete file with one snapshot id per position. */
internal class DucklakeFlushDeleteFileWriter(
        private val fileSystem: TrinoFileSystem,
        private val writerOptions: ParquetWriterOptions,
        private val trinoVersion: String)
{
    data class DeletedPosition(val position: Long, val snapshotId: Long)

    fun write(
            tableDataPath: String,
            dataFilePath: String,
            dataFileId: Long,
            deletions: List<DeletedPosition>): DucklakeDeleteFragment {
        require(deletions.isNotEmpty()) { "deletions is empty" }
        require(deletions.zipWithNext().all { (a, b) -> a.position < b.position }) {
            "flush delete positions must be strictly increasing"
        }
        val columnTypes = listOf<Type>(VARCHAR, BIGINT, BIGINT)
        val schemaConverter = deleteSchemaConverter(columnTypes)
        val fileName = "ducklake-delete-${UUID.randomUUID()}.parquet"
        val outputStream = fileSystem.newOutputFile(Location.of(tableDataPath).appendPath(fileName)).create()
        val parquetWriter = ParquetWriter(
                outputStream,
                annotatedDeleteSchema(schemaConverter),
                schemaConverter.primitiveTypes,
                writerOptions,
                CompressionCodec.ZSTD,
                trinoVersion,
                Optional.empty(),
                Optional.empty())
        var closed = false
        try {
            val rows = deletions.map { listOf<Any?>(dataFilePath, it.position, it.snapshotId) }
            RecordPageSource(InMemoryRecordSet(columnTypes, rows)).use { source ->
                while (!source.isFinished) {
                    parquetWriter.write((source.nextSourcePage ?: continue).page)
                }
            }
            parquetWriter.close()
            closed = true
            return fragment(dataFileId, fileName, deletions, parquetWriter)
        }
        finally {
            if (!closed) {
                runCatching { parquetWriter.close() }
            }
        }
    }

    private fun fragment(
            dataFileId: Long,
            fileName: String,
            deletions: List<DeletedPosition>,
            writer: ParquetWriter): DucklakeDeleteFragment {
        val footer = DynamicSliceOutput(40)
        Util.writeFileMetaData(writer.getFileMetaData(), footer)
        return DucklakeDeleteFragment(
                dataFileId,
                fileName,
                deletions.size.toLong(),
                writer.estimatedWrittenBytes,
                footer.size().toLong(),
                deletions.size.toLong(),
                "parquet",
                deletions.minOf { it.snapshotId },
                deletions.maxOf { it.snapshotId })
    }

    companion object {
        private const val FILE_PATH_FIELD_ID: Long = 2_147_483_646L
        private const val POSITION_FIELD_ID: Long = 2_147_483_645L

        private fun deleteSchemaConverter(types: List<Type>): ParquetSchemaConverter =
            ParquetSchemaConverter(
                    types,
                    listOf(
                            DucklakeDeleteFileReader.SPEC_FILE_PATH_COLUMN,
                            DucklakeDeleteFileReader.SPEC_POSITION_COLUMN,
                            DucklakeDeleteFileReader.INTERNAL_SNAPSHOT_ID_COLUMN),
                    false,
                    false)

        private fun annotatedDeleteSchema(converter: ParquetSchemaConverter) =
            DucklakeParquetSchemaBuilder.buildMessageType(
                    emptyList(),
                    emptyList(),
                    converter.messageType,
                    mapOf(
                            DucklakeDeleteFileReader.SPEC_FILE_PATH_COLUMN to FILE_PATH_FIELD_ID,
                            DucklakeDeleteFileReader.SPEC_POSITION_COLUMN to POSITION_FIELD_ID,
                            DucklakeDeleteFileReader.INTERNAL_SNAPSHOT_ID_COLUMN
                                    to DucklakeDeleteFileReader.SNAPSHOT_ID_PARQUET_FIELD_ID.toLong()))
    }
}
