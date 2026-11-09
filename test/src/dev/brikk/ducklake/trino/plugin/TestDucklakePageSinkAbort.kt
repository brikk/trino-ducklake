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

import dev.brikk.ducklake.catalog.DucklakePartitionField
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import io.airlift.json.JsonCodec
import io.trino.plugin.hive.parquet.ParquetWriterConfig
import io.trino.spi.Page
import io.trino.spi.PageIndexer
import io.trino.spi.PageIndexerFactory
import io.trino.spi.PageSorter
import io.trino.spi.type.BigintType.BIGINT
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.Test
import java.lang.reflect.Proxy
import java.util.Optional

class TestDucklakePageSinkAbort {
    @Test
    fun unexpectedWriterAbortFailuresDoNotPreventOtherClosesOrOutputDeletion() {
        val fs = FailureTrackingFileSystem()
        val column = DucklakeColumnHandle(1, "id", BIGINT, true)
        val spec = DucklakePartitionSpec(1, 1, listOf(DucklakePartitionField(0, 1, DucklakePartitionTransform.IDENTITY)))
        val handle = DucklakeWritableTableHandle("main", "t", 1, listOf(column), emptyList(), "memory:///table",
            Optional.of(spec), DucklakeTemporalPartitionEncoding.CALENDAR, "parquet")
        val indexer = PageIndexerFactory { object : PageIndexer {
            override fun indexPage(page: Page): IntArray = IntArray(page.positionCount) { it }
            override fun getMaxIndex(): Int = 1
        } }
        val sorter = Proxy.newProxyInstance(PageSorter::class.java.classLoader, arrayOf(PageSorter::class.java)) { _, _, _ ->
            error("unpartitioned sorter is not used")
        } as PageSorter
        val sink = DucklakePageSink(handle, fs, JsonCodec.jsonCodec(DucklakeWriteFragment::class.java),
            ParquetWriterConfig(), "test", indexer, sorter)
        val block = BIGINT.createBlockBuilder(null, 2).also { BIGINT.writeLong(it, 1); BIGINT.writeLong(it, 2) }.build()
        sink.appendPage(Page(block))
        assertThat(fs.openedOutputs).hasSize(2)
        fs.outputCloseFailure = object : RuntimeException("unexpected storage close failure") {}

        assertThatCode { sink.abort() }.doesNotThrowAnyException()

        assertThat(fs.closedOutputs).containsAll(fs.openedOutputs)
        assertThat(fs.deleted).containsAll(fs.openedOutputs)
        assertThat(fs.openedOutputs).allSatisfy { assertThat(fs.newInputFile(it).exists()).isFalse() }
        assertThat(sink.memoryUsage).isZero()
    }
}
