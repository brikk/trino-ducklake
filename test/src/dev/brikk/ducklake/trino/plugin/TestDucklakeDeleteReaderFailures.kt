/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import io.airlift.units.DataSize
import io.trino.filesystem.Location
import io.trino.parquet.ParquetReaderOptions
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TypeUtils.writeNativeValue
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.io.ParquetDecodingException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional

class TestDucklakeDeleteReaderFailures {
    private val path = "memory:///delete.parquet"
    private val options = ParquetReaderOptions.builder().withSmallFileThreshold(DataSize.ofBytes(0)).build()
    private val stats = FileFormatDataSourceStats()

    private enum class Entry(val preservesTrino: Boolean = false) {
        POSITIONS, INFO(true), ROW_IDS, DROP_POSITIONS(true), ORIGINS(true), FILTERED_POSITIONS, SNAPSHOTS(true)
    }

    @Test
    fun everyEntryClosesOnIoAndReaderFailuresWithoutLosingTheCause() {
        for (entry in Entry.entries) {
            for (failure in listOf(IOException("storage unavailable"), IllegalArgumentException("invalid schema"),
                IllegalStateException("invalid reader state"), ArithmeticException("row count overflow"),
                ParquetDecodingException("invalid encoding"))) {
                val fs = fileSystem().apply { readFailure = failure }
                assertThatThrownBy { read(entry, fs) }.`as`("$entry / ${failure.javaClass.simpleName}")
                    .hasMessageContaining("Failed to read delete file").hasCause(failure)
                fs.assertInputsClosed()
            }
        }
    }

    @Test
    fun everyEntryKeepsItsTrinoErrorClassification() {
        for (entry in Entry.entries) {
            val failure = TrinoException(NOT_SUPPORTED, "physical field unsupported")
            val fs = fileSystem().apply { readFailure = failure }
            val assertion = assertThatThrownBy { read(entry, fs) }.`as`(entry.name)
            if (entry.preservesTrino) assertion.isSameAs(failure)
            else assertion.hasMessageContaining("Failed to read delete file").hasCause(failure)
            assertThat(failure.errorCode).isEqualTo(NOT_SUPPORTED.toErrorCode())
            fs.assertInputsClosed()
        }
    }

    @Test
    fun everyEntryClosesEvenWhenAnErrorMustPropagateUnchanged() {
        for (entry in Entry.entries) {
            val failure = AssertionError("reader invariant")
            val fs = fileSystem().apply { readFailure = failure }
            assertThatThrownBy { read(entry, fs) }.isSameAs(failure)
            fs.assertInputsClosed()
        }
    }

    @Test
    fun closeFailureIsSuppressedInsteadOfReplacingTheReadFailure() {
        for (entry in Entry.entries) {
            val failure = IOException("read failed")
            val closeFailure = IOException("close failed")
            val fs = fileSystem().apply { readFailure = failure; inputCloseFailure = closeFailure }
            assertThatThrownBy { read(entry, fs) }.hasCause(failure)
            assertThat(failure.suppressed).contains(closeFailure)
            fs.assertInputsClosed()
        }
    }

    private fun read(entry: Entry, fs: FailureTrackingFileSystem): Any? = when (entry) {
        Entry.POSITIONS -> DucklakeDeleteFileReader.readPositions(fs, path, 0, options, stats)
        Entry.INFO -> DucklakeDeleteFileReader.readDataFileInfo(fs, path, 0, options, stats) {}
        Entry.ROW_IDS -> DucklakeDeleteFileReader.readInternalRowIds(fs, path, 0, options, stats)
        Entry.DROP_POSITIONS -> DucklakeDeleteFileReader.readSnapshotDropPositions(fs, path, 0, 1, options, stats)
        Entry.ORIGINS -> DucklakeDeleteFileReader.readInternalSnapshotIdsIfPresent(fs, path, 0, options, stats)
        Entry.FILTERED_POSITIONS -> DucklakeDeleteFileReader.readPositionsWithSnapshotFilter(fs, path, 0, 1, options, stats)
        Entry.SNAPSHOTS -> DucklakeDeleteFileReader.readPositionsWithSnapshotsIfPresent(fs, path, 0, options, stats)
    }

    private fun fileSystem(): FailureTrackingFileSystem = FailureTrackingFileSystem().apply {
        val schema = ParquetSchemaConverter(listOf(BIGINT), listOf("pos"), false, false)
        ParquetWriter(newOutputFile(Location.of(path)).create(), schema.messageType, schema.primitiveTypes,
            ParquetWriterOptions.builder().build(), CompressionCodec.UNCOMPRESSED, "test", Optional.empty(), Optional.empty()).use {
            it.write(Page(writeNativeValue(BIGINT, 1L)))
        }
    }
}
