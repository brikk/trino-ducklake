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

import io.airlift.slice.Slices.utf8Slice
import io.trino.spi.Page
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TypeUtils
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.type.JsonType.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/** The same connector boundary must work with 483 Slice JSON and 484 native JSON blocks. */
class TestDucklakeJsonSupport {
    private val document = """{"z":1,"a":[true,null,"λ"]}"""

    @Test
    fun inlineJsonUsesTheTargetsNativeValue() {
        val native = DucklakeInlinedValueConverter.convertJdbcValue(document, JSON)
        val block = TypeUtils.writeNativeValue(JSON, native)
        assertThat(JSON.getSlice(block, 0).toStringUtf8()).isEqualTo(document)
        assertThat(DucklakeInlinedValueConverter.convertJdbcValue(null, JSON)).isNull()
    }

    @Test
    fun parquetJsonIsTextWithNullsAndOtherChannelsPreserved() {
        val json = JSON.createBlockBuilder(null, 2)
        JSON.writeSlice(json, utf8Slice(document))
        json.appendNull()
        val id = BIGINT.createBlockBuilder(null, 2)
        BIGINT.writeLong(id, 7)
        BIGINT.writeLong(id, 8)
        val page = Page(id.build(), json.build())

        val physical = DucklakeJsonSupport.toParquetPage(page, listOf(BIGINT, JSON))
        assertThat(physical.getBlock(0)).isSameAs(page.getBlock(0))
        assertThat(VARCHAR.getSlice(physical.getBlock(1), 0).toStringUtf8()).isEqualTo(document)
        assertThat(physical.getBlock(1).isNull(1)).isTrue()
        assertThat(JSON.getSlice(page.getBlock(1), 0).toStringUtf8()).isEqualTo(document)
        assertThat(DucklakeJsonSupport.toParquetWriteType(JSON)).isEqualTo(VARCHAR)
    }

    @Test
    fun nonJsonPagesNeedNoCopy() {
        val page = Page(TypeUtils.writeNativeValue(BIGINT, 42L))
        assertThat(DucklakeJsonSupport.toParquetPage(page, listOf(BIGINT))).isSameAs(page)
    }
}
