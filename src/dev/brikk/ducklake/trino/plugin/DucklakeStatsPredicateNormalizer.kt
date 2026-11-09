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
import io.airlift.slice.Slices
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimeType.TIME_MICROS
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType.UUID
import io.trino.spi.type.UuidType.trinoUuidToJavaUuid
import io.trino.spi.type.VarcharType
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale

/**
 * Renders a Trino predicate value as the `min_value`/`max_value` text that the catalog's
 * `DucklakeStatTypes.parseStat` (ducklake-catalog) reads back as exactly the same value for the
 * column's stored DuckLake type. `findDataFileIdsInRange` compares our text and each file's stats
 * through that one parser, so the text only has to parse to the identical comparison key:
 *  - integers and decimals parse as `BigDecimal`, so plain decimal text is exact;
 *  - `float32`/`float64` parse as `BigDecimal` and are then rounded to the declared binary type,
 *    so the shortest round-trip text of the float/double yields the same key as any spelling of it
 *    in the file stats;
 *  - temporal types parse to an epoch-nanosecond key (ISO and DuckDB spellings, explicit offsets
 *    applied), so ISO text with nanosecond fraction, and `Z` for `timestamptz`, is exact; `time`
 *    parses to nanoseconds since midnight;
 *  - `varchar` compares by code point, which is Trino's UTF-8 byte order;
 *  - `uuid` compares by code point, and lowercase hyphenated text, the spelling of DuckDB's and
 *    this connector's file stats, orders like the unsigned big-endian bytes Trino compares.
 *
 * Pruning is supported only when the Trino type is the one the connector itself maps the stored
 * DuckLake type to *and* back ([supports]); unsigned/hugeint promotions, VARCHAR degradations
 * (`uint128`, `variant`, `interval`), binary, time with time zone and nested types are not, and
 * their columns never exclude a file.
 */
internal object DucklakeStatsPredicateNormalizer {
    private val DECIMAL_TYPE = Regex("decimal\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)")
    private val ISO_TIME_NANOS: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS", Locale.ROOT)
    private val ISO_NANOS: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS", Locale.ROOT)
    private const val MAX_FOUR_DIGIT_YEAR = 9999
    private val SCALAR_STORED_TYPES: Map<Type, String> = mapOf(
            BOOLEAN to "boolean",
            TINYINT to "int8",
            SMALLINT to "int16",
            INTEGER to "int32",
            BIGINT to "int64",
            REAL to "float32",
            DOUBLE to "float64",
            DATE to "date")
    private val TIMESTAMP_STORED_TYPES: Map<Int, String> = mapOf(
            0 to "timestamp_s",
            3 to "timestamp_ms",
            6 to "timestamp",
            9 to "timestamp_ns")

    /** True when a predicate on a Trino [trinoType] column stored as [ducklakeType] can be normalized exactly. */
    fun supports(trinoType: Type, ducklakeType: String): Boolean {
        val stored = ducklakeType.trim().lowercase(Locale.ROOT)
        return when (trinoType) {
            is DecimalType -> DECIMAL_TYPE.matchEntire(stored)
                    ?.let { it.groupValues[1].toInt() == trinoType.precision && it.groupValues[2].toInt() == trinoType.scale }
                    ?: false
            is TimestampType -> stored == TIMESTAMP_STORED_TYPES[trinoType.precision]
            is TimestampWithTimeZoneType -> stored == "timestamptz"
            is VarcharType -> stored == "varchar"
            TIME_MICROS -> stored == "time"
            UUID -> stored == "uuid"
            else -> stored == SCALAR_STORED_TYPES[trinoType]
        }
    }

    /**
     * The stat text for a native predicate [value] of a [supports]-accepted [type], or null when this
     * value has no exact text (non-finite floats, years outside 1..9999, invalid UTF-8). A null bound
     * leaves that side of the range open, which can only retain more files.
     */
    fun toStatText(type: Type, value: Any): String? = when (type) {
        BOOLEAN -> (value as Boolean).toString()
        TINYINT, SMALLINT, INTEGER, BIGINT -> (value as Long).toString()
        REAL -> java.lang.Float.intBitsToFloat((value as Long).toInt()).takeIf { it.isFinite() }?.toString()
        DOUBLE -> (value as Double).takeIf { it.isFinite() }?.toString()
        DATE -> LocalDate.ofEpochDay(value as Long).takeIf { it.year in 1..MAX_FOUR_DIGIT_YEAR }?.toString()
        is DecimalType -> decimalText(type, value)
        is TimestampType -> timestampText(type, value)
        is TimestampWithTimeZoneType -> timestampWithTimeZoneText(type, value)
        is VarcharType -> (value as Slice).toStringUtf8().takeIf { Slices.utf8Slice(it) == value }
        TIME_MICROS -> ISO_TIME_NANOS.format(LocalTime.ofNanoOfDay((value as Long) / PICOS_PER_NANO))
        UUID -> trinoUuidToJavaUuid(value as Slice).toString()
        else -> null
    }

    private fun decimalText(type: DecimalType, value: Any): String {
        val unscaled = if (type.isShort) BigDecimal.valueOf(value as Long) else BigDecimal((value as Int128).toBigInteger())
        return unscaled.movePointLeft(type.scale).toPlainString()
    }

    private fun timestampText(type: TimestampType, value: Any): String? {
        val (epochMicros, nanosOfMicro) = if (type.isShort) {
            (value as Long) to 0
        }
        else {
            val long = value as LongTimestamp
            long.epochMicros to long.picosOfMicro / PICOS_PER_NANO
        }
        val instant = Instant.ofEpochSecond(
                Math.floorDiv(epochMicros, MICROS_PER_SECOND),
                Math.floorMod(epochMicros, MICROS_PER_SECOND) * NANOS_PER_MICRO + nanosOfMicro)
        return isoText(instant)
    }

    private fun timestampWithTimeZoneText(type: TimestampWithTimeZoneType, value: Any): String? {
        val (epochMillis, nanosOfMilli) = if (type.isShort) {
            unpackMillisUtc(value as Long) to 0
        }
        else {
            val long = value as LongTimestampWithTimeZone
            long.epochMillis to long.picosOfMilli / PICOS_PER_NANO
        }
        val instant = Instant.ofEpochSecond(
                Math.floorDiv(epochMillis, MILLIS_PER_SECOND),
                Math.floorMod(epochMillis, MILLIS_PER_SECOND) * NANOS_PER_MILLI + nanosOfMilli)
        return isoText(instant)?.let { "${it}Z" }
    }

    // parseTemporal recognises only four-digit years; anything else must not reach the catalog.
    private fun isoText(instant: Instant): String? {
        val local = LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
        return if (local.year in 1..MAX_FOUR_DIGIT_YEAR) ISO_NANOS.format(local) else null
    }

    private const val PICOS_PER_NANO = 1_000
    private const val MICROS_PER_SECOND = 1_000_000L
    private const val NANOS_PER_MICRO = 1_000L
    private const val MILLIS_PER_SECOND = 1_000L
    private const val NANOS_PER_MILLI = 1_000_000L
}
