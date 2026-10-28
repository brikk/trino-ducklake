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

import io.trino.spi.type.BigintType.BIGINT
import io.trino.type.JsonType.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TestDucklakeNativeRowsPageSource {
    @Test
    fun nativeJsonAndNullRowsStreamAcrossBoundedPages() {
        // Exceed the standard page byte budget without relying on a particular row-count limit.
        val text = """{"a":"${"x".repeat(512)}"}"""
        val json = DucklakeInlinedValueConverter.convertJdbcValue(text, JSON)
        val rows = (0L until 10_000L).map { listOf(it, if (it % 2L == 0L) json else null) }
        val source = DucklakeNativeRowsPageSource(listOf(BIGINT, JSON), rows)
        assertThat(source.memoryUsage).isPositive()
        var count = 0L
        var pages = 0
        while (!source.isFinished) {
            val page = source.nextSourcePage ?: continue
            pages++
            for (position in 0 until page.positionCount) {
                assertThat(BIGINT.getLong(page.getBlock(0), position)).isEqualTo(count)
                if (count % 2L == 0L) assertThat(JSON.getSlice(page.getBlock(1), position).toStringUtf8()).isEqualTo(text)
                else assertThat(page.getBlock(1).isNull(position)).isTrue()
                count++
            }
        }
        assertThat(count).isEqualTo(10_000L)
        assertThat(pages).isGreaterThan(1)
        assertThat(source.completedBytes).isPositive()
        assertThat(source.memoryUsage).isZero()
        assertThat(source.nextSourcePage).isNull()
    }

    @Test
    fun closeReleasesRowsAndEmptyProjectionPreservesCount() {
        val source = DucklakeNativeRowsPageSource(emptyList(), List(3) { emptyList() })
        assertThat(source.nextSourcePage!!.positionCount).isEqualTo(3)
        source.close()
        assertThat(source.isFinished).isTrue()
        assertThat(source.memoryUsage).isZero()
    }
}
