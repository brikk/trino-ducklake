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

import dev.brikk.ducklake.catalog.DucklakeStatTypes
import io.airlift.slice.Slices
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType.createDecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimeType.TIME_MICROS
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimeZoneKey.getTimeZoneKey
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.TimestampType.TIMESTAMP_NANOS
import io.trino.spi.type.TimestampType.createTimestampType
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType.UUID
import io.trino.spi.type.VarbinaryType.VARBINARY
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger
import java.time.LocalDate
import kotlin.random.Random

/**
 * The normalizer is only correct if the catalog parses its text to exactly the key it parses the
 * writer's stats text to. These tests compare through the catalog's own `DucklakeStatTypes.parseStat`.
 */
class TestDucklakeStatsPredicateNormalizer {
    @Test
    fun supportsOnlyTheConnectorsOwnTypeProjection() {
        assertThat(DucklakeStatsPredicateNormalizer.supports(BIGINT, "int64")).isTrue()
        assertThat(DucklakeStatsPredicateNormalizer.supports(createDecimalType(10, 2), "decimal(10, 2)")).isTrue()
        assertThat(DucklakeStatsPredicateNormalizer.supports(TIMESTAMP_NANOS, "timestamp_ns")).isTrue()
        assertThat(DucklakeStatsPredicateNormalizer.supports(TIMESTAMP_TZ_MICROS, "TIMESTAMPTZ")).isTrue()

        // Unsigned and hugeint promotions, VARCHAR degradations, and mismatched precision.
        assertThat(DucklakeStatsPredicateNormalizer.supports(SMALLINT, "uint8")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(BIGINT, "uint32")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(createDecimalType(20, 0), "uint64")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(createDecimalType(38, 0), "int128")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(VARCHAR, "uint128")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(VARCHAR, "interval")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(VARCHAR, "variant")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(createDecimalType(10, 2), "decimal(10,3)")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(TIMESTAMP_MICROS, "timestamp_ms")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(DOUBLE, "float32")).isFalse()

        // Types whose comparison is not provably exact.
        assertThat(DucklakeStatsPredicateNormalizer.supports(UUID, "uuid")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(VARBINARY, "blob")).isFalse()
        assertThat(DucklakeStatsPredicateNormalizer.supports(TIME_MICROS, "time")).isFalse()
    }

    @Test
    fun integersAndBooleans() {
        for (type in listOf(TINYINT, SMALLINT, INTEGER, BIGINT)) {
            assertSameKey(type, "int64", -5L, "-5")
        }
        assertSameKey(BIGINT, "int64", Long.MIN_VALUE, Long.MIN_VALUE.toString())
        assertSameKey(BOOLEAN, "boolean", true, "true")
        assertSameKey(BOOLEAN, "boolean", false, "false")
    }

    @Test
    fun realKeysAreTheFloatValueForRandomBits() {
        val random = Random(20261107)
        repeat(20_000) {
            val f = java.lang.Float.intBitsToFloat(random.nextInt())
            if (!f.isFinite()) return@repeat
            val bits = f.toRawBits().toLong()
            // Two writer spellings of the same float: shortest text and the exact binary expansion.
            assertSameKey(REAL, "float32", bits, f.toString())
            assertSameKey(REAL, "float32", bits, BigDecimal(f.toDouble()).toPlainString())
        }
        assertThat(DucklakeStatsPredicateNormalizer.toStatText(REAL, java.lang.Float.NaN.toRawBits().toLong())).isNull()
        assertThat(DucklakeStatsPredicateNormalizer.toStatText(REAL, java.lang.Float.POSITIVE_INFINITY.toRawBits().toLong())).isNull()
    }

    @Test
    fun realKeysPreserveOrderOfAdjacentFloats() {
        var f = 0.1f
        repeat(1_000) {
            val next = Math.nextUp(f)
            assertThat(compareKeys(REAL, "float32", f.toRawBits().toLong(), next.toRawBits().toLong())).isNegative()
            f = next
        }
    }

    @Test
    fun doubleKeysAreTheDoubleValueForRandomBits() {
        val random = Random(20261108)
        repeat(20_000) {
            val d = java.lang.Double.longBitsToDouble(random.nextLong())
            if (!d.isFinite()) return@repeat
            assertSameKey(DOUBLE, "float64", d, d.toString())
            assertSameKey(DOUBLE, "float64", d, BigDecimal(d).toPlainString())
        }
        assertThat(DucklakeStatsPredicateNormalizer.toStatText(DOUBLE, Double.NaN)).isNull()
    }

    @Test
    fun decimalsAreScaled() {
        assertSameKey(createDecimalType(10, 2), "decimal(10,2)", 1250L, "12.50")
        assertSameKey(createDecimalType(10, 2), "decimal(10,2)", -1L, "-0.01")
        val unscaled = BigInteger("-123456789012345678901234567890")
        assertSameKey(createDecimalType(38, 5), "decimal(38,5)", Int128.valueOf(unscaled), "-1234567890123456789012345.67890")
    }

    @Test
    fun datesBeforeAndAfterTheEpoch() {
        assertSameKey(DATE, "date", LocalDate.parse("2024-02-29").toEpochDay(), "2024-02-29")
        assertSameKey(DATE, "date", LocalDate.parse("0001-01-01").toEpochDay(), "0001-01-01")
        assertThat(DucklakeStatsPredicateNormalizer.toStatText(DATE, LocalDate.parse("+10000-01-01").toEpochDay())).isNull()
        assertThat(DucklakeStatsPredicateNormalizer.toStatText(DATE, LocalDate.parse("-0001-01-01").toEpochDay())).isNull()
    }

    @Test
    fun timestampsMatchTheDuckDbSpelling() {
        val micros = -1L // 1969-12-31 23:59:59.999999
        assertSameKey(TIMESTAMP_MICROS, "timestamp", micros, "1969-12-31 23:59:59.999999")
        assertSameKey(TIMESTAMP_MICROS, "timestamp", 1_704_103_200_123_456L, "2024-01-01 10:00:00.123456")
        assertSameKey(createTimestampType(0), "timestamp_s", 1_704_103_200_000_000L, "2024-01-01 10:00:00")
        assertSameKey(createTimestampType(3), "timestamp_ms", 1_704_103_200_123_000L, "2024-01-01 10:00:00.123")
        assertSameKey(TIMESTAMP_NANOS, "timestamp_ns", LongTimestamp(1_704_103_200_123_456L, 789_000), "2024-01-01 10:00:00.123456789")
        assertSameKey(TIMESTAMP_NANOS, "timestamp_ns", LongTimestamp(-1L, 999_000), "1969-12-31 23:59:59.999999999")
    }

    @Test
    fun timestampsWithTimeZoneCompareAsInstants() {
        val zone = getTimeZoneKey("+02:00")
        assertSameKey(
                TIMESTAMP_TZ_MICROS,
                "timestamptz",
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_704_103_200_123L, 456_000_000, zone),
                "2024-01-01 12:00:00.123456+02")
        assertSameKey(
                TIMESTAMP_TZ_MICROS,
                "timestamptz",
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_704_103_200_123L, 456_000_000, UTC_KEY),
                "2024-01-01 10:00:00.123456+00")
        assertSameKey(TIMESTAMP_TZ_MILLIS, "timestamptz", packDateTimeWithZone(-1L, zone), "1969-12-31 23:59:59.999+00")
    }

    @Test
    fun varcharIsTheUtf8Text() {
        assertSameKey(VARCHAR, "varchar", Slices.utf8Slice("😀 ﬀ"), "😀 ﬀ")
        assertThat(DucklakeStatsPredicateNormalizer.toStatText(VARCHAR, Slices.wrappedBuffer(0xC3.toByte()))).isNull()
    }

    private fun assertSameKey(type: Type, ducklakeType: String, value: Any, writerText: String) {
        val text = DucklakeStatsPredicateNormalizer.toStatText(type, value)
        assertThat(text).`as`("%s %s", type, value).isNotNull()
        val ours = DucklakeStatTypes.parseStat(ducklakeType, text)
        val theirs = DucklakeStatTypes.parseStat(ducklakeType, writerText)
        assertThat(ours).`as`("%s parsed from %s", type, text).isNotNull()
        @Suppress("UNCHECKED_CAST")
        assertThat((ours as Comparable<Any>).compareTo(theirs!!))
                .`as`("%s: normalized %s vs writer %s", type, text, writerText)
                .isZero()
    }

    private fun compareKeys(type: Type, ducklakeType: String, left: Any, right: Any): Int {
        val l = DucklakeStatTypes.parseStat(ducklakeType, DucklakeStatsPredicateNormalizer.toStatText(type, left))
        val r = DucklakeStatTypes.parseStat(ducklakeType, DucklakeStatsPredicateNormalizer.toStatText(type, right))
        @Suppress("UNCHECKED_CAST")
        return (l as Comparable<Any>).compareTo(r!!)
    }
}
