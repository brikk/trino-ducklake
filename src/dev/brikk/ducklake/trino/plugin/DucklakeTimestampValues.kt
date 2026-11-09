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

import io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeParseException

internal object DucklakeTimestampValues {
    private val HOUR_OFFSET = Regex(".*[+-][0-9]{2}$")
    private val COMPACT_OFFSET = Regex(".*[+-][0-9]{4}$")

    fun timestamp(value: Any, type: TimestampType, jdbcOffsetAsInstant: Boolean = false): Any {
        val local = if (value is Number) null else local(value, jdbcOffsetAsInstant)
        val micros = if (local == null) (value as Number).toLong() else epochMicros(local.toEpochSecond(ZoneOffset.UTC), local.nano)
        return if (type.isShort) micros else LongTimestamp(micros, (local?.nano ?: 0) % 1_000 * 1_000)
    }

    fun timestampWithTimeZone(value: Any, type: TimestampWithTimeZoneType): Any {
        val offset = if (value is Number) null else offset(value)
        val micros = if (offset == null) (value as Number).toLong() else epochMicros(offset.toEpochSecond(), offset.nano)
        val millis = Math.floorDiv(micros, 1_000)
        val picos = Math.floorMod(micros, 1_000) * 1_000_000 + (offset?.nano ?: 0) % 1_000 * 1_000
        return if (type.isShort) packDateTimeWithZone(millis, UTC_KEY)
            else LongTimestampWithTimeZone.fromEpochMillisAndFraction(millis, picos, UTC_KEY)
    }

    private fun epochMicros(seconds: Long, nanos: Int): Long =
        Math.addExact(Math.multiplyExact(seconds, 1_000_000), nanos / 1_000L)

    private fun local(value: Any, jdbcOffsetAsInstant: Boolean): LocalDateTime {
        if (value is LocalDateTime) return value
        val text = normalize(DucklakeInlinedValueConverter.toStringValue(value))
        return try { LocalDateTime.parse(text) }
        catch (_: DateTimeParseException) {
            val offset = parseOffset(text, value)
            // Catalog/DuckDB TIMESTAMP ignores offsets; legacy JDBC text treats them as instants.
            if (jdbcOffsetAsInstant) offset.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime() else offset.toLocalDateTime()
        }
    }

    private fun offset(value: Any): OffsetDateTime {
        if (value is OffsetDateTime) return value
        if (value is LocalDateTime) return value.atOffset(ZoneOffset.UTC)
        val text = normalize(DucklakeInlinedValueConverter.toStringValue(value))
        return try { OffsetDateTime.parse(text) }
        catch (e: DateTimeParseException) {
            try { LocalDateTime.parse(text).atOffset(ZoneOffset.UTC) }
            catch (_: DateTimeParseException) { throw IllegalArgumentException("Invalid timestamp with time zone value: $value", e) }
        }
    }

    private fun parseOffset(text: String, value: Any): OffsetDateTime = try { OffsetDateTime.parse(text) }
        catch (e: DateTimeParseException) { throw IllegalArgumentException("Invalid timestamp value: $value", e) }

    private fun normalize(value: String): String {
        val text = value.trim().replace(' ', 'T')
        return when {
            text.matches(HOUR_OFFSET) -> "$text:00"
            text.matches(COMPACT_OFFSET) -> text.dropLast(2) + ":" + text.takeLast(2)
            else -> text
        }
    }
}
