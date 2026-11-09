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

import com.sun.management.ThreadMXBean
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeChangeFeedDeletion
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystemFactory
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.TrinoInput
import io.trino.filesystem.TrinoInputFile
import io.trino.filesystem.memory.MemoryFileSystem
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.spi.Page
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.connector.SourcePage
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.schema.MessageType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.roaringbitmap.RoaringBitmap
import java.lang.management.ManagementFactory
import java.lang.reflect.Proxy
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import java.util.zip.CRC32

/** Synchronous connector reads: caller-thread allocation excludes fixture creation and warmup. */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeReadPathPerformance {
    @Test
    fun changeFeedReadsOneFooterAcrossAllUnitsAndReleasesRetainedMemory() {
        val fixture = Fixture(countFooters = true)
        val source = fixture.changes()
        assertThat(source.memoryUsage).isGreaterThan(16_000_000L)
        source.use { while (!it.isFinished) it.nextSourcePage }
        assertThat(source.memoryUsage).isZero()
        assertThat(fixture.footerReads).isEqualTo(1)
        fixture.changes().use { while (!it.isFinished) it.nextSourcePage }
        assertThat(fixture.footerReads).isEqualTo(2)
    }

    @Test
    fun millionRowsWithThirtyPercentDeletesAndThreeOriginSnapshots() {
        val fixture = Fixture()
        measure("decode-filter") {
            val deleted = DucklakePuffinDeleteReader.decodeBlob(fixture.blob)
            val filter = DucklakePageSourceProvider.DeleteRowFilterTransform(emptySet(), deleted, 0L)
            var kept = 0L
            repeat(1000) { kept += filter.apply(SourcePage.create(1000)).positionCount }
            assertThat(kept).isEqualTo(700_000L)
        }
        measure("parquet-delete-scan") {
            fixture.scan().use { source ->
                var rows = 0L
                var sum = 0L
                while (!source.isFinished) {
                    val page = source.nextSourcePage ?: continue
                    val block = page.getBlock(0)
                    rows += page.positionCount
                    for (i in 0 until page.positionCount) sum += BIGINT.getLong(block, i)
                }
                assertThat(rows).isEqualTo(700_000L)
                assertThat(sum).isEqualTo(350_000_700_000L)
            }
        }
        measure("change-feed") {
            fixture.changes().use { source ->
                val counts = mutableMapOf<Long, Long>()
                var deletes = 0L
                while (!source.isFinished) {
                    val page = source.nextSourcePage ?: continue
                    for (i in 0 until page.positionCount) {
                        counts.merge(BIGINT.getLong(page.getBlock(0), i), 1L, Long::plus)
                        if (VARCHAR.getSlice(page.getBlock(1), i).toStringUtf8() == "delete") deletes++
                    }
                }
                assertThat(counts).isEqualTo(mapOf(2L to 333_334L, 3L to 333_333L, 4L to 333_333L, 5L to 300_000L))
                assertThat(deletes).isEqualTo(300_000L)
            }
        }
    }

    private fun measure(label: String, action: () -> Unit) {
        repeat(3) { action() }
        val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean
        bean.isThreadAllocatedMemoryEnabled = true
        val thread = Thread.currentThread().threadId()
        val samples = (1..5).map {
            val allocated = bean.getThreadAllocatedBytes(thread)
            val start = System.nanoTime()
            action()
            (System.nanoTime() - start) / 1_000_000.0 to (bean.getThreadAllocatedBytes(thread) - allocated)
        }
        println("READ_PATH $label median_ms=${samples.map { it.first }.sorted()[2]} " +
                "median_allocated_bytes=${samples.map { it.second }.sorted()[2]} samples=$samples")
    }

    private class Fixture(countFooters: Boolean = false) {
        val fs = MemoryFileSystem()
        var footerReads = 0
        val path = Location.of("memory:///data.parquet")
        val deletePath = Location.of("memory:///delete.puffin")
        val column = DucklakeColumnHandle(1, "id", BIGINT, true)
        val blob = deletionBlob()
        val file: DucklakeDataFile
        val provider: DucklakePageSourceProvider

        init {
            val schema = ParquetSchemaConverter(listOf(BIGINT, BIGINT, BIGINT),
                    listOf("id", "lineage", DucklakeDeleteFileReader.INTERNAL_SNAPSHOT_ID_COLUMN), false, false)
            val message = MessageType("fixture", schema.messageType.fields.mapIndexed { index, field ->
                field.withId(when (index) {
                    0 -> 1
                    1 -> DucklakeDeleteFileReader.ROW_ID_PARQUET_FIELD_ID
                    else -> DucklakeDeleteFileReader.SNAPSHOT_ID_PARQUET_FIELD_ID
                })
            })
            ParquetWriter(fs.newOutputFile(path).create(), message, schema.primitiveTypes,
                    ParquetWriterOptions.builder().build(), CompressionCodec.UNCOMPRESSED, "test", Optional.empty(), Optional.empty()).use { writer ->
                repeat(1000) { batch ->
                    val ids = BIGINT.createBlockBuilder(null, 1000)
                    val origins = BIGINT.createBlockBuilder(null, 1000)
                    repeat(1000) { index ->
                        val id = batch * 1000L + index
                        BIGINT.writeLong(ids, id)
                        BIGINT.writeLong(origins, 2 + id % 3)
                    }
                    val block = ids.build()
                    writer.write(Page(block, block, origins.build()))
                }
            }
            fs.newOutputFile(deletePath).create().use { it.write(blob) }
            val footer = if (countFooters) fs.newInputFile(path).newInput().use {
                ByteBuffer.wrap(it.readTail(8).bytes).order(ByteOrder.LITTLE_ENDIAN).int.toLong()
            } else 0L
            file = DucklakeDataFile(1, 1, 2, null, 0, path.toString(), false, "parquet", 1_000_000,
                    fs.newInputFile(path).length(), footer, 0, null, null, null, null, null, null, partialMax = 4)
            val deletion = DucklakeChangeFeedDeletion(5, 2, file.path, false, "parquet", footer, file.fileSizeBytes,
                    0, file.recordCount, false, deletePath.toString(), false, "puffin", 0, null, null, null, null, null, null)
            val catalog = Proxy.newProxyInstance(DucklakeCatalog::class.java.classLoader, arrayOf(DucklakeCatalog::class.java)) { _, method, args ->
                when (method.name) {
                    "readSession" -> (args[0] as Supplier<*>).get()
                    "getSpecVersion" -> "1.0"
                    "getDataPath" -> "memory:///"
                    "getSchema" -> DucklakeSchema(1, UUID(0, 1), 1, null, "main", "memory:///", false)
                    "getTableById" -> DucklakeTable(1, UUID(0, 2), 1, null, 1, "t", "memory:///", false)
                    "getDataFilesAddedBetween" -> listOf(file)
                    "getDeletionsBetween" -> listOf(deletion)
                    "getInlinedDataInfos", "getInlinedFileDeletesBetween" -> emptyList<Any>()
                    "getAllColumnsWithParentage" -> listOf(DucklakeColumn(1, 1, null, 1, 0, "id", "int64", true, null))
                    else -> error("Unexpected catalog call ${method.name}")
                }
            } as DucklakeCatalog
            val reads = if (countFooters) countingFileSystem() else fs
            provider = DucklakePageSourceProvider(TrinoFileSystemFactory { reads }, FileFormatDataSourceStats(),
                    ParquetReaderConfig().toParquetReaderOptions(), catalog, DucklakeConfig().setDataPath("memory:///"),
                    DucklakeTypeConverter(TESTING_TYPE_MANAGER))
        }

        private fun countingFileSystem(): TrinoFileSystem = object : TrinoFileSystem by fs {
            private fun wrap(input: TrinoInputFile): TrinoInputFile = object : TrinoInputFile by input {
                override fun newInput(): TrinoInput {
                    val delegate = input.newInput()
                    return object : TrinoInput by delegate {
                        override fun readTail(buffer: ByteArray, offset: Int, length: Int): Int {
                            if (input.location() == path) footerReads++
                            return delegate.readTail(buffer, offset, length)
                        }
                        override fun readTail(length: Int): io.airlift.slice.Slice {
                            if (input.location() == path) footerReads++
                            return delegate.readTail(length)
                        }
                    }
                }
            }
            override fun newInputFile(location: Location): TrinoInputFile = wrap(fs.newInputFile(location))
            override fun newInputFile(location: Location, length: Long): TrinoInputFile = wrap(fs.newInputFile(location, length))
        }

        fun scan(): ConnectorPageSource = provider.createPageSource(null, SESSION,
                testingDucklakeSplit(file.path, listOf(deletePath.toString()), 0, file.recordCount, file.fileSizeBytes, "parquet", TupleDomain.all()),
                DucklakeTableHandle("main", "t", 1, 5), Optional.empty(), listOf(column), DynamicFilter.EMPTY)

        fun changes(): ConnectorPageSource = provider.createPageSource(null, SESSION, ChangeFeedSplit(1),
                ChangeFeedTableHandle(ChangeFeedType.CHANGES, "main", "t", 1, 2, 5, listOf(column)), Optional.empty(),
                listOf(ChangeFeedColumns.SNAPSHOT_ID, ChangeFeedColumns.CHANGE_TYPE, column), DynamicFilter.EMPTY)
    }

    companion object {
        private fun deletionBlob(): ByteArray {
            val bitmap = RoaringBitmap()
            for (position in 0 until 1_000_000) if (position % 10 < 3) bitmap.add(position)
            val checkedSize = 4 + 8 + 4 + bitmap.serializedSizeInBytes()
            val buffer = ByteBuffer.allocate(checkedSize + 8)
            buffer.putInt(checkedSize).put(byteArrayOf(0xD1.toByte(), 0xD3.toByte(), 0x39, 0x64))
            buffer.order(ByteOrder.LITTLE_ENDIAN).putLong(1).putInt(0)
            bitmap.serialize(buffer)
            val crc = CRC32().apply { update(buffer.array(), 4, checkedSize) }
            buffer.position(4 + checkedSize)
            buffer.order(ByteOrder.BIG_ENDIAN).putInt(crc.value.toInt())
            return buffer.array()
        }
    }
}
