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

import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import java.time.LocalDateTime
import java.time.OffsetDateTime

class TestDucklakeTimestampValues {
    @Test
    fun catalogTextMatchesNativeDuckDbAndLegacyJdbcPolicyRemainsExplicit() {
        val localType = TimestampType.createTimestampType(6)
        val zonedType = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(6)
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                for (offset in listOf("+02", "+0200", "+02:00", "-02", "Z", "")) {
                    val text = "1969-12-31 23:59:59.123456$offset"
                    statement.executeQuery("SELECT epoch_us(TIMESTAMP '$text'), epoch_us(TIMESTAMPTZ '$text')").use { result ->
                        check(result.next())
                        val local = DucklakePartitionValueParser.parseIdentity(localType, text)
                        assertThat(local).isEqualTo(result.getLong(1))
                        assertThat(DucklakeDecodedValueConverter.convert(text, localType)).isEqualTo(local)
                        val zoned = DucklakePartitionValueParser.parseIdentity(zonedType, text) as LongTimestampWithTimeZone
                        assertThat(zoned.epochMillis * 1000 + zoned.picosOfMilli / 1_000_000).isEqualTo(result.getLong(2))
                        assertThat(DucklakeInlinedValueConverter.convertJdbcValue(text, zonedType)).isEqualTo(zoned)
                        assertThat(DucklakeDecodedValueConverter.convert(text, zonedType)).isEqualTo(zoned)
                        assertThat(DucklakeInlinedValueConverter.convertJdbcValue(text, localType)).isEqualTo(result.getLong(2))
                    }
                }
            }
        }
    }

    @Test
    fun nanosecondsNegativeEpochsTypedValuesAndNumericMicrosUseOneEncoding() {
        val type = TimestampType.createTimestampType(9)
        val zonedType = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(9)
        val local = LocalDateTime.parse("1969-12-31T23:59:59.999999123")
        val zoned = OffsetDateTime.parse("1969-12-31T23:59:59.999999123Z")
        assertThat(DucklakeTimestampValues.timestamp(local, type)).isEqualTo(LongTimestamp(-1, 123_000))
        assertThat(DucklakeDecodedValueConverter.convert(local, type)).isEqualTo(LongTimestamp(-1, 123_000))
        val expected = LongTimestampWithTimeZone.fromEpochMillisAndFraction(-1, 999_123_000, io.trino.spi.type.TimeZoneKey.UTC_KEY)
        assertThat(DucklakeTimestampValues.timestampWithTimeZone(zoned, zonedType)).isEqualTo(expected)
        assertThat(DucklakeTimestampValues.timestampWithTimeZone(-1L, zonedType))
                .isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(-1, 999_000_000, io.trino.spi.type.TimeZoneKey.UTC_KEY))
        val shortType = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)
        assertThat(unpackMillisUtc(DucklakeTimestampValues.timestampWithTimeZone(-1L, shortType) as Long)).isEqualTo(-1L)
        assertThatThrownBy { DucklakeTimestampValues.timestamp("bad", type) }.isInstanceOf(IllegalArgumentException::class.java)
    }
}
