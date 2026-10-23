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

import dev.brikk.ducklake.catalog.DucklakeInterval
import io.airlift.slice.Slice
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.RowType
import io.trino.spi.type.TimeType.TIME_MICROS
import io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MICROS
import io.trino.spi.type.VarbinaryType.VARBINARY
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.LocalTime
import java.time.OffsetTime

class TestDucklakeDecodedValueConverter {
    @Test
    fun decodedContainersKeepNullLiteralNullAndOpaqueBytes() {
        val type = RowType.from(listOf(RowType.field("s", VARCHAR), RowType.field("b", VARBINARY), RowType.field("a", ArrayType(BIGINT))))
        val bytes = byteArrayOf(0, 0x5c, 0xff.toByte())
        val row = DucklakeDecodedValueConverter.convert(mapOf("s" to "NULL", "b" to bytes, "a" to listOf(4L, null)), type)
        val builder = type.createBlockBuilder(null, 1)
        type.writeObject(builder, row)
        val actual = type.getObjectValue(builder.build(), 0) as List<*>
        assertThat(actual[0]).isEqualTo("NULL")
        assertThat((actual[1] as io.trino.spi.type.SqlVarbinary).bytes).containsExactly(*bytes)
        assertThat(actual[2]).isEqualTo(listOf(4L, null))
        assertThat(DucklakeDecodedValueConverter.convert(null, type)).isNull()
        assertThatThrownBy { DucklakeDecodedValueConverter.convert(mapOf("old" to "NULL"), type) }
            .hasMessageContaining("target schema")
    }

    @Test
    fun intervalTextKeepsIndependentComponents() {
        val value = DucklakeDecodedValueConverter.convert(DucklakeInterval(2, -3, 360000123456L), VARCHAR) as Slice
        assertThat(value.toStringUtf8()).isEqualTo("2 months -3 days 360000123456 microseconds")
    }

    @Test
    fun timeIsPicosecondsAndUnrepresentableTimetzRejects() {
        assertThat(DucklakeDecodedValueConverter.convert(LocalTime.of(0, 0, 0, 123456000), TIME_MICROS))
            .isEqualTo(123456000000L)
        assertThatThrownBy { DucklakeDecodedValueConverter.convert(OffsetTime.parse("01:02:03+05:30:15"), TIME_TZ_MICROS) }
            .hasMessageContaining("cannot be represented losslessly")
    }
}
