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

import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTableVersion
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND
import io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import java.lang.Math.floorDiv
import java.lang.Math.floorMod
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

/** `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` bounds converted to a snapshot id or instant. */
internal object DucklakeTableVersions
{
    private val SNAPSHOT_ID_TYPES: List<Type> = listOf(SMALLINT, TINYINT, INTEGER, BIGINT)

    fun getSnapshotIdFromVersion(version: ConnectorTableVersion): Long
    {
        val versionType: Type = version.versionType
        if (SNAPSHOT_ID_TYPES.any { it === versionType }) {
            return (version.version as Number).toLong()
        }

        throw TrinoException(NOT_SUPPORTED, "Unsupported type for table version: ${versionType.displayName}")
    }

    fun getSnapshotTimestampFromVersion(session: ConnectorSession, version: ConnectorTableVersion): Instant
    {
        val versionType: Type = version.versionType
        if (versionType == DATE) {
            return LocalDate.ofEpochDay(version.version as Long)
                    .atStartOfDay()
                    .atZone(session.timeZoneKey.zoneId)
                    .toInstant()
        }
        if (versionType is TimestampType) {
            val epochMicrosUtc: Long = if (versionType.isShort)
                version.version as Long
            else
                (version.version as LongTimestamp).epochMicros
            val epochSecondUtc: Long = floorDiv(epochMicrosUtc, MICROSECONDS_PER_SECOND)
            val nanosOfSecond: Int = floorMod(epochMicrosUtc, MICROSECONDS_PER_SECOND).toInt() * NANOSECONDS_PER_MICROSECOND
            return LocalDateTime.ofEpochSecond(epochSecondUtc, nanosOfSecond, ZoneOffset.UTC)
                    .atZone(session.timeZoneKey.zoneId)
                    .toInstant()
        }
        if (versionType is TimestampWithTimeZoneType) {
            val epochMillis: Long = if (versionType.isShort)
                unpackMillisUtc(version.version as Long)
            else
                (version.version as LongTimestampWithTimeZone).epochMillis
            return Instant.ofEpochMilli(epochMillis)
        }

        throw TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: ${versionType.displayName}")
    }
}
