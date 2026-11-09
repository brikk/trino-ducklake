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

import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.memory.MemoryFileSystem
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.spi.Page
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TypeUtils.writeNativeValue
import org.apache.parquet.format.CompressionCodec
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional

class TestParquetFileWriterAbort {
    private val location = Location.of("memory:///table/ducklake-partial.parquet")

    @Test
    fun abortDeletesThePartialFile() {
        val fileSystem = MemoryFileSystem()
        writerWithOneRow(fileSystem).abort()
        assertThat(fileSystem.newInputFile(location).exists()).isFalse()
    }

    @Test
    fun abortSurvivesAFailedDelete() {
        val memory = MemoryFileSystem()
        val failingDelete = object : TrinoFileSystem by memory {
            override fun deleteFile(location: Location) = throw IOException("storage unavailable")
        }
        val writer = writerWithOneRow(failingDelete)
        assertThatCode { writer.abort() }.doesNotThrowAnyException()
    }

    private fun writerWithOneRow(fileSystem: TrinoFileSystem): ParquetFileWriter {
        val column = DucklakeColumnHandle(1L, "id", BIGINT, true)
        val schema = ParquetSchemaConverter(listOf(BIGINT), listOf("id"), false, false)
        val output = fileSystem.newOutputFile(location).create()
        val parquetWriter = ParquetWriter(output, schema.messageType, schema.primitiveTypes,
                ParquetWriterOptions.builder().build(), CompressionCodec.UNCOMPRESSED, "test", Optional.empty(), Optional.empty())
        val writer = ParquetFileWriter(parquetWriter, output, fileSystem, location, "ducklake-partial.parquet",
                emptyMap(), null, listOf(column), emptyList(), schema.messageType)
        val block = BIGINT.createBlockBuilder(null, 1).also { writeNativeValue(BIGINT, it, 7L) }.build()
        writer.write(Page(block))
        return writer
    }
}
