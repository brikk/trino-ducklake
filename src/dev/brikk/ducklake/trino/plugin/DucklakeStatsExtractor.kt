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

import dev.brikk.ducklake.catalog.DucklakeFileColumnStats
import dev.brikk.ducklake.catalog.DucklakeStatTypes
import io.trino.spi.type.BigintType
import io.trino.spi.type.BooleanType
import io.trino.spi.type.DateType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.RealType
import io.trino.spi.type.SmallintType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType
import io.trino.spi.type.VarcharType
import org.apache.parquet.format.FileMetaData
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Locale

/**
 * Aggregates per-row-group parquet statistics into one [DucklakeFileColumnStats]
 * row per parquet leaf, keyed by the DuckLake catalog field_id at that leaf
 * (top-level column_id for flat columns; child column_id for nested struct /
 * array / map leaves).
 *
 * Caller is responsible for projecting the leaf list — see
 * [DucklakeStatsLeafProjector] (write path) and
 * [DucklakeAddFilesNameMapper] (add_files path). Both produce a
 * [LeafStatsTarget] list in the same order parquet writes leaves into
 * `RowGroup.columns`.
 */
object DucklakeStatsExtractor {
    fun extractStats(
            fileMetaData: FileMetaData,
            leafTargets: List<LeafStatsTarget>,
            parquetSchema: MessageType? = null): List<DucklakeFileColumnStats> {
        val physicalLeaves = parquetSchema?.let { primitiveLeaves(it) }.orEmpty()
        return leafTargets.map { target ->
            val accumulator = StatsAccumulator(target, physicalLeaves.getOrNull(target.parquetColumnIndex))
            fileMetaData.getRow_groups().forEach(accumulator::add)
            accumulator.result()
        }
    }

    private class StatsAccumulator(
            private val target: LeafStatsTarget,
            private val physicalLeaf: PrimitiveType?) {
        private var totalCompressedSize = 0L
        private var totalValueCount = 0L
        private var totalNullCount = 0L
        private var countsKnown = true
        private var minValue: String? = null
        private var maxValue: String? = null
        private var hasBounds = false
        private var boundsKnown = true

        fun add(rowGroup: org.apache.parquet.format.RowGroup) {
            val columnMeta = rowGroup.columns.getOrNull(target.parquetColumnIndex)?.meta_data
            if (columnMeta == null) {
                countsKnown = false
                boundsKnown = false
                return
            }
            totalCompressedSize += columnMeta.total_compressed_size
            val statistics = columnMeta.statistics
            val nullCount = if (columnMeta.isSetStatistics && statistics.isSetNull_count)
                statistics.null_count
            else
                null
            if (nullCount == null) {
                countsKnown = false
                boundsKnown = false
                return
            }
            totalNullCount += nullCount
            val valueCount = columnMeta.num_values - nullCount
            totalValueCount += valueCount
            if (!hasMinMax(columnMeta)) {
                if (valueCount > 0) {
                    boundsKnown = false
                }
                return
            }
            mergeBounds(columnMeta, statistics.getMin_value(), statistics.getMax_value())
        }

        private fun mergeBounds(
                columnMeta: org.apache.parquet.format.ColumnMetaData,
                minBytes: ByteArray,
                maxBytes: ByteArray) {
            val physicalType = physicalType(physicalLeaf, columnMeta)
            val timeUnit = timestampUnit(physicalLeaf, target.leafType)
            val groupMin = convertStatValue(minBytes, target.leafType, physicalType, timeUnit)
            val groupMax = convertStatValue(maxBytes, target.leafType, physicalType, timeUnit)
            if (groupMin == null || groupMax == null) {
                boundsKnown = false
                return
            }
            val numeric = isNumericTrinoType(target.leafType)
            minValue = minValue?.let { DucklakeStatTypes.min(it, groupMin, numeric) } ?: groupMin
            maxValue = maxValue?.let { DucklakeStatTypes.max(it, groupMax, numeric) } ?: groupMax
            hasBounds = true
        }

        fun result(): DucklakeFileColumnStats = DucklakeFileColumnStats(
                target.fieldId,
                totalCompressedSize,
                if (countsKnown) totalValueCount else null,
                if (countsKnown) totalNullCount else null,
                if (boundsKnown && hasBounds) minValue else null,
                if (boundsKnown && hasBounds) maxValue else null,
                null) // Parquet footer has no NaN flag; writers that inspect values overwrite it.
    }

    internal fun convertStatValue(value: ByteArray?, type: Type): String? {
        return convertStatValue(value, type, null, null)
    }

    internal fun convertStatValue(value: ByteArray?, type: Type, physicalType: org.apache.parquet.format.Type?): String? {
        return convertStatValue(value, type, physicalType, null)
    }

    private fun convertStatValue(
            value: ByteArray?,
            type: Type,
            physicalType: org.apache.parquet.format.Type?,
            timestampUnit: TimeUnit?): String? {
        if (value == null || value.isEmpty()) {
            return null
        }
        return try {
            when (type) {
                is DateType -> LocalDate.ofEpochDay(littleEndianInt(value).toLong()).toString()
                is TimestampType -> convertTimestamp(value, type, physicalType, timestampUnit)
                is TimestampWithTimeZoneType -> convertTimestampTz(value, physicalType, timestampUnit)
                is DecimalType -> BigDecimal(decodeDecimalUnscaled(value, physicalType), type.scale).toPlainString()
                else -> convertBasic(value, type)
            }
        }
        catch (e: RuntimeException) {
            null
        }
    }

    private fun convertBasic(value: ByteArray, type: Type): String? = when {
        type is BooleanType -> if (value[0].toInt() != 0) "true" else "false"
        type is TinyintType || type is SmallintType || type is IntegerType -> littleEndianInt(value).toString()
        type is BigintType -> littleEndianLong(value).toString()
        type is RealType -> littleEndianFloat(value).takeUnless { it.isNaN() }?.toString()
        type is DoubleType -> littleEndianDouble(value).takeUnless { it.isNaN() }?.toString()
        type is VarcharType -> String(value, Charsets.UTF_8)
        type is VarbinaryType || type is UuidType -> null
        else -> null
    }

    private fun convertTimestamp(
            value: ByteArray,
            type: TimestampType,
            physicalType: org.apache.parquet.format.Type?,
            unit: TimeUnit?): String? {
        if (physicalType == org.apache.parquet.format.Type.INT96) {
            return null
        }
        val instant = timestampInstant(littleEndianLong(value), unit ?: inferredTimestampUnit(type))
        return formatTimestamp(LocalDateTime.ofInstant(instant, ZoneOffset.UTC))
    }

    private fun convertTimestampTz(
            value: ByteArray,
            physicalType: org.apache.parquet.format.Type?,
            unit: TimeUnit?): String? {
        if (physicalType == org.apache.parquet.format.Type.INT96) {
            return null
        }
        val instant = timestampInstant(littleEndianLong(value), unit ?: TimeUnit.MICROS)
        return formatTimestamp(LocalDateTime.ofInstant(instant, ZoneOffset.UTC)) + "+00"
    }

    private fun littleEndianInt(value: ByteArray): Int =
        ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt()

    private fun littleEndianLong(value: ByteArray): Long =
        ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong()

    private fun littleEndianFloat(value: ByteArray): Float =
        ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getFloat()

    private fun littleEndianDouble(value: ByteArray): Double =
        ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getDouble()

    /**
     * Parquet stores DECIMAL statistics in different byte orders depending on the
     * physical type: INT32/INT64-backed decimals are little-endian two's-complement
     * (matching the primitive layout), while FIXED_LEN_BYTE_ARRAY / BINARY decimals
     * are big-endian two's-complement. Decoding the short INT32/INT64 forms as
     * big-endian (`new BigInteger(byte[])`) silently corrupts min/max for
     * low-precision decimals.
     */
    private fun decodeDecimalUnscaled(value: ByteArray, physicalType: org.apache.parquet.format.Type?): BigInteger {
        if (physicalType == org.apache.parquet.format.Type.INT32) {
            return BigInteger.valueOf(ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt().toLong())
        }
        if (physicalType == org.apache.parquet.format.Type.INT64) {
            return BigInteger.valueOf(ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong())
        }
        // FIXED_LEN_BYTE_ARRAY / BINARY (and the unknown/test path): big-endian two's complement.
        return BigInteger(value)
    }

    private fun primitiveLeaves(schema: MessageType): List<PrimitiveType> {
        val leaves = mutableListOf<PrimitiveType>()
        fun visit(type: org.apache.parquet.schema.Type) {
            if (type.isPrimitive) {
                leaves += type.asPrimitiveType()
            }
            else {
                type.asGroupType().fields.forEach(::visit)
            }
        }
        schema.fields.forEach(::visit)
        return leaves
    }

    private fun physicalType(
            leaf: PrimitiveType?,
            columnMeta: org.apache.parquet.format.ColumnMetaData?): org.apache.parquet.format.Type? {
        if (leaf != null) {
            return when (leaf.primitiveTypeName) {
                PrimitiveType.PrimitiveTypeName.BINARY -> org.apache.parquet.format.Type.BYTE_ARRAY
                else -> runCatching { org.apache.parquet.format.Type.valueOf(leaf.primitiveTypeName.name) }.getOrNull()
            }
        }
        return if (columnMeta?.isSetType == true) columnMeta.getType() else null
    }

    private fun hasMinMax(columnMeta: org.apache.parquet.format.ColumnMetaData): Boolean =
        columnMeta.isSetStatistics &&
                columnMeta.statistics.isSetMin_value &&
                columnMeta.statistics.isSetMax_value

    private fun timestampUnit(leaf: PrimitiveType?, type: Type): TimeUnit? =
        (leaf?.logicalTypeAnnotation as? TimestampLogicalTypeAnnotation)?.unit
            ?: if (type is TimestampType) inferredTimestampUnit(type) else null

    private fun inferredTimestampUnit(type: TimestampType): TimeUnit = when {
        type.precision <= 3 -> TimeUnit.MILLIS
        type.precision <= 6 -> TimeUnit.MICROS
        else -> TimeUnit.NANOS
    }

    private fun timestampInstant(value: Long, unit: TimeUnit): Instant {
        val unitsPerSecond = when (unit) {
            TimeUnit.MILLIS -> 1_000L
            TimeUnit.MICROS -> 1_000_000L
            TimeUnit.NANOS -> 1_000_000_000L
        }
        val nanosPerUnit = 1_000_000_000L / unitsPerSecond
        return Instant.ofEpochSecond(
                Math.floorDiv(value, unitsPerSecond),
                Math.floorMod(value, unitsPerSecond) * nanosPerUnit)
    }

    /** DuckDB `Timestamp::ToString`: space separator, mandatory seconds, trimmed fraction. */
    private fun formatTimestamp(value: LocalDateTime): String {
        val base = String.format(
                Locale.ROOT,
                "%04d-%02d-%02d %02d:%02d:%02d",
                value.year, value.monthValue, value.dayOfMonth,
                value.hour, value.minute, value.second)
        if (value.nano == 0) {
            return base
        }
        return "$base.${value.nano.toString().padStart(9, '0').trimEnd('0')}"
    }

    private fun isNumericTrinoType(type: Type): Boolean {
        return type is TinyintType
                || type is SmallintType
                || type is IntegerType
                || type is BigintType
                || type is RealType
                || type is DoubleType
                || type is DecimalType
    }
}
