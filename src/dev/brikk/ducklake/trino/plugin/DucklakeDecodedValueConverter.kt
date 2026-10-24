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
import io.airlift.slice.Slices
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.type.ArrayType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.TimeType
import io.trino.spi.type.TimeWithTimeZoneType
import io.trino.spi.type.Type
import io.trino.spi.type.TypeUtils.writeNativeValue
import java.time.LocalTime
import java.time.OffsetTime

/** Consumes the catalog's validated target-schema values without another text serialization. */
internal object DucklakeDecodedValueConverter {
    fun convert(value: Any?, type: Type): Any? = when {
        value == null -> null
        type is RowType -> row(value as Map<*, *>, type)
        type is MapType -> map(value as Map<*, *>, type)
        type is ArrayType -> {
            val values = value as List<*>
            val builder = type.elementType.createBlockBuilder(null, values.size)
            values.forEach { writeNativeValue(type.elementType, builder, convert(it, type.elementType)) }
            builder.build()
        }
        type is TimeType -> {
            val time = value as LocalTime
            require(time.nano % 1_000 == 0) { "TIME precision exceeds the connector's microsecond mapping" }
            time.toNanoOfDay() * 1_000L
        }
        type is TimeWithTimeZoneType -> offsetTime(value as OffsetTime)
        type is DecimalType -> {
            val decimal = java.math.BigDecimal(value.toString()).setScale(type.scale, java.math.RoundingMode.UNNECESSARY)
            require(decimal.precision() <= type.precision) { "Decoded decimal exceeds Trino precision ${type.precision}; cannot narrow the value" }
            DucklakeInlinedValueConverter.convertJdbcValue(decimal, type)
        }
        value is DucklakeInterval -> Slices.utf8Slice("${value.months} months ${value.days} days ${value.micros} microseconds")
        else -> DucklakeInlinedValueConverter.convertJdbcValue(value, type)
    }

    private fun row(value: Map<*, *>, type: RowType): Any {
        val names = type.fields.map { it.name.orElseThrow { IllegalArgumentException("Unnamed decoded STRUCT field") } }
        require(value.keys == names.toSet()) { "Decoded STRUCT fields differ from the target schema" }
        val builder = type.createBlockBuilder(null, 1) as io.trino.spi.block.RowBlockBuilder
        builder.buildEntry<RuntimeException> { fields ->
            type.fields.forEachIndexed { index, field ->
                writeNativeValue(field.type, fields[index], convert(value[names[index]], field.type))
            }
        }
        return type.getObject(builder.build(), 0)
    }

    private fun map(value: Map<*, *>, type: MapType): Any {
        val builder = type.createBlockBuilder(null, 1) as io.trino.spi.block.MapBlockBuilder
        builder.buildEntry<RuntimeException> { keys, values ->
            value.forEach { (key, item) ->
                writeNativeValue(type.keyType, keys, convert(key, type.keyType))
                writeNativeValue(type.valueType, values, convert(item, type.valueType))
            }
        }
        return type.getObject(builder.build(), 0)
    }

    private fun offsetTime(value: OffsetTime): Long {
        if (value.offset.totalSeconds % 60 != 0 || value.nano % 1_000 != 0) {
            throw TrinoException(NOT_SUPPORTED, "TIMETZ seconds offsets or sub-microsecond values cannot be represented losslessly")
        }
        return packTimeWithTimeZone(value.toLocalTime().toNanoOfDay(), value.offset.totalSeconds / 60)
    }
}
