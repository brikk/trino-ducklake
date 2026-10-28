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

import io.airlift.slice.Slice
import io.trino.spi.PageBuilder
import io.trino.spi.block.Block
import io.trino.spi.block.SqlMap
import io.trino.spi.block.SqlRow
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.SourcePage
import io.trino.spi.type.Type
import io.trino.spi.type.TypeUtils.writeNativeValue

/** Streams already-decoded native rows into bounded pages, including 484's native JSON values.
 * InMemoryRecordSet's hard-coded Java-class size estimator does not accept native JSON objects.
 * No extra whole-result copy or text re-interpretation is needed at this boundary.
 */
internal class DucklakeNativeRowsPageSource(
    private val types: List<Type>,
    rows: List<List<Any?>>,
) : ConnectorPageSource {
    private var rows: Iterator<List<Any?>> = rows.iterator()
    private var retainedBytes = 128L + rows.sumOf { row ->
        require(row.size == types.size) { "Native row width differs from projected types" }
        64L + row.indices.sumOf { channel -> retainedValueBytes(types[channel], row[channel]) }
    }
    private var closed = false
    private var completedBytes = 0L

    override fun getCompletedBytes(): Long = completedBytes
    override fun getReadTimeNanos(): Long = 0
    override fun isFinished(): Boolean = closed
    override fun getMemoryUsage(): Long = retainedBytes

    override fun getNextSourcePage(): SourcePage? {
        if (closed) return null
        val builder = PageBuilder(types)
        while (!builder.isFull && rows.hasNext()) {
            val row = rows.next()
            require(row.size == types.size) { "Native row width differs from projected types" }
            builder.declarePosition()
            types.forEachIndexed { channel, type -> writeNativeValue(type, builder.getBlockBuilder(channel), row[channel]) }
        }
        if (!rows.hasNext()) close()
        if (builder.isEmpty) return null
        val page = builder.build()
        completedBytes += page.sizeInBytes
        return SourcePage.create(page)
    }

    override fun close() {
        closed = true
        rows = emptyList<List<Any?>>().iterator()
        retainedBytes = 0
    }

    private fun retainedValueBytes(type: Type, value: Any?): Long = when (value) {
        null -> 8L
        is Slice -> 16L + value.retainedSize
        is Block -> 16L + value.retainedSizeInBytes
        is SqlMap -> 16L + value.retainedSizeInBytes
        is SqlRow -> 16L + value.retainedSizeInBytes
        is String -> 64L + value.length * 2L
        is ByteArray -> 32L + value.size
        // Inline native JSON values wrap text; count a block's backing bytes and object allowance.
        else -> if (DucklakeJsonSupport.isJson(type)) {
            256L + writeNativeValue(type, value).retainedSizeInBytes
        } else 128L // Conservative allowance for scalar native values (boxed primitives/Int128/time).
    }
}
