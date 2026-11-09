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

import com.google.common.collect.ImmutableList
import com.google.inject.Inject
import dev.brikk.ducklake.catalog.DucklakeColumn
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType
import io.trino.spi.type.BooleanType
import io.trino.spi.type.DateType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.MapType
import io.trino.spi.type.RealType
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType
import io.trino.spi.type.StandardTypes
import io.trino.spi.type.TimeType
import io.trino.spi.type.TimeWithTimeZoneType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType
import io.trino.spi.type.Type
import io.trino.spi.type.TypeManager
import io.trino.spi.type.TypeParameter
import io.trino.spi.type.TypeDescriptor
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType
import io.trino.spi.type.VarcharType
import java.util.Locale
import java.util.Optional
import java.util.regex.Pattern

/**
 * Converts Ducklake type strings to Trino types.
 * Handles all Ducklake primitive, nested, and special types.
 */
open class DucklakeTypeConverter @Inject constructor(private val typeManager: TypeManager) {

    /**
     * Convert a catalog column using the identity-bearing parent/child rows rather than parsing a
     * rendered type string. Struct names retain exact case and punctuation, and nested shape stays
     * tied to column_id. Primitive leaves still use [toTrinoType].
     */
    open fun toTrinoType(column: DucklakeColumn, allColumns: List<DucklakeColumn>): Type {
        val childrenByParent = allColumns
                .mapNotNull { column -> column.parentColumn?.let { parent -> parent to column } }
                .groupBy({ it.first }, { it.second })
                .mapValues { (_, children) -> children.sortedBy { it.columnOrder } }

        fun convert(node: DucklakeColumn): Type {
            val base = node.columnType.trim().substringBefore('<').lowercase(Locale.ROOT)
            val children = childrenByParent[node.columnId].orEmpty()
            return when (base) {
                "struct" -> RowType.from(children.map { child ->
                    RowType.Field(Optional.of(child.columnName), convert(child))
                })
                "list" -> {
                    require(children.size == 1) {
                        "DuckLake list column ${node.columnId} has ${children.size} children"
                    }
                    ArrayType(convert(children.single()))
                }
                "map" -> {
                    val key = children.singleOrNull { it.columnName.equals("key", ignoreCase = true) }
                        ?: throw IllegalArgumentException("DuckLake map column ${node.columnId} has no key child")
                    val value = children.singleOrNull { it.columnName.equals("value", ignoreCase = true) }
                        ?: throw IllegalArgumentException("DuckLake map column ${node.columnId} has no value child")
                    typeManager.getParameterizedType(
                            StandardTypes.MAP,
                            ImmutableList.of(
                                    TypeParameter.typeParameter(convert(key).typeDescriptor),
                                    TypeParameter.typeParameter(convert(value).typeDescriptor)))
                }
                else -> toTrinoType(node.columnType)
            }
        }
        return convert(column)
    }

    /**
     * Convert Ducklake type string to Trino Type. Nested `struct`/`list`/`map` signatures are parsed
     * by the catalog's grammar ([catalogCompositeType]); the rest are scalar type names.
     */
    open fun toTrinoType(ducklakeType: String): Type {
        catalogCompositeType(ducklakeType, typeManager, ::toTrinoType)?.let { return it }
        val normalizedType: String = ducklakeType.trim().lowercase(Locale.ROOT)

        // Native Trino JSON type (io.trino.type.JsonType, resolved via TypeManager since it is
        // not on the trino-spi compile classpath). Physically a UTF-8 string in parquet — the
        // same shape DuckDB writes — so reads/writes reuse the VARCHAR value paths.
        if (normalizedType == "json") {
            return typeManager.getType(TypeDescriptor(StandardTypes.JSON))
        }
        SCALAR_TRINO_TYPES[normalizedType]?.let { return it }

        val decimalMatcher = DECIMAL_PATTERN.matcher(normalizedType)
        if (decimalMatcher.matches()) {
            val precision = decimalMatcher.group(1).toInt()
            val scale = decimalMatcher.group(2).toInt()
            return DecimalType.createDecimalType(precision, scale)
        }
        throw TrinoException(NOT_SUPPORTED, "Unsupported Ducklake type: $ducklakeType")
    }

    /**
     * Convert Trino type to Ducklake type string (for write operations)
     */
    open fun toDucklakeType(trinoType: Type): String = when (trinoType) {
        is DecimalType -> "decimal(${trinoType.precision},${trinoType.scale})"
        is TimestampType -> timestampDucklakeType(trinoType.precision)
        // DuckLake timestamptz is microsecond-only (DuckDB has a single TIMESTAMPTZ = micros;
        // no second/milli/nano variants like plain TIMESTAMP). The stored type is always
        // micros regardless of the declared precision; a column declared at another precision
        // silently widens to micros on read — tolerated here (unlike TIMESTAMP/TIME) because
        // there is no narrower tstz storage. The CTAS short-block mismatch this used to cause
        // is fixed in beginCreateTable, not by rejecting the type.
        is TimestampWithTimeZoneType -> "timestamptz"
        // DuckLake time / timetz are microsecond-only. Reject any other declared precision rather
        // than silently coercing to micros (which would read back as TIME(6) / a changed precision).
        is TimeType -> microsecondTimeType("time", "time", trinoType.precision)
        is TimeWithTimeZoneType -> microsecondTimeType("timetz", "time with time zone", trinoType.precision)
        is VarcharType -> "varchar"
        // Nested types: DuckLake stores these as parent column type strings;
        // child columns are separate rows linked via parent_column.
        is ArrayType -> "list"
        is RowType -> "struct"
        is MapType -> "map"
        else -> SCALAR_DUCKLAKE_TYPES[trinoType] ?: jsonDucklakeType(trinoType)
    }

    private fun timestampDucklakeType(precision: Int): String = when (precision) {
        0 -> "timestamp_s"
        3 -> "timestamp_ms"
        6 -> "timestamp"
        9 -> "timestamp_ns"
        // DuckLake only encodes second/milli/micro/nano timestamps; any other Trino
        // precision (1, 2, 4, 5, 7, 8) has no representable DuckLake type. Fail loudly at
        // DDL time instead of silently collapsing to micros and reading back as TIMESTAMP(6).
        else -> throw TrinoException(NOT_SUPPORTED,
                "Unsupported timestamp precision $precision for DuckLake write; supported precisions are 0, 3, 6, 9")
    }

    private fun microsecondTimeType(ducklakeType: String, typeName: String, precision: Int): String {
        if (precision != 6) {
            throw TrinoException(NOT_SUPPORTED,
                    "Unsupported $typeName precision $precision for DuckLake write; only precision 6 (microseconds) is supported")
        }
        return ducklakeType
    }

    private fun jsonDucklakeType(trinoType: Type): String {
        if (DucklakeJsonSupport.isJson(trinoType)) {
            return "json"
        }
        throw TrinoException(NOT_SUPPORTED, "Unsupported Trino type: $trinoType")
    }

    companion object {
        private val DECIMAL_PATTERN: Pattern = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)")

        private val SCALAR_TRINO_TYPES: Map<String, Type> = buildMap {
            put("boolean", BooleanType.BOOLEAN)

            // Signed integers
            put("int8", TinyintType.TINYINT)
            put("int16", SmallintType.SMALLINT)
            put("int32", IntegerType.INTEGER)
            put("int64", BigintType.BIGINT)

            // Unsigned integers map to the next larger signed type; writes are range-checked by
            // DucklakeUnsignedRangeChecker.
            put("uint8", SmallintType.SMALLINT)  // 0..255 -> -32768..32767
            put("uint16", IntegerType.INTEGER)    // 0..65535 -> -2^31..2^31-1
            put("uint32", BigintType.BIGINT)      // 0..2^32-1 -> -2^63..2^63-1
            put("uint64", DecimalType.createDecimalType(20, 0)) // 0..2^64-1 -> decimal(20,0)

            // 128-bit integers. DuckLake stores these as "int128" / "uint128" (see upstream
            // ducklake_types.cpp: int128 <-> HUGEINT, uint128 <-> UHUGEINT). int128 fits in
            // DECIMAL(38, 0) for all but the extreme edges of its ±1.7e38 range; uint128's
            // 0..3.4e38 range exceeds any Trino decimal, so we degrade it to VARCHAR (data
            // preserved as text, no numeric ops). See dev-docs/archive/COMPARE-pg_ducklake.md B3.
            put("int128", DecimalType.createDecimalType(38, 0))
            put("uint128", VarcharType.VARCHAR)

            // Floating point
            put("float32", RealType.REAL)
            put("float64", DoubleType.DOUBLE)

            // Temporal types
            put("date", DateType.DATE)
            put("time", TimeType.TIME_MICROS)  // Ducklake uses microsecond precision
            put("timetz", TimeWithTimeZoneType.TIME_TZ_MICROS)
            put("timestamp", TimestampType.TIMESTAMP_MICROS)
            put("timestamptz", TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS)
            put("timestamp_s", TimestampType.createTimestampType(0))   // second precision
            put("timestamp_ms", TimestampType.createTimestampType(3))  // millisecond precision
            put("timestamp_ns", TimestampType.createTimestampType(9))  // nanosecond precision

            // String types
            put("varchar", VarcharType.VARCHAR)
            put("blob", VarbinaryType.VARBINARY)
            put("uuid", UuidType.UUID)

            // ⚠️ DEGRADED SEMANTICS - MVP PLACEHOLDERS ONLY
            // These mappings allow basic data access but DO NOT provide full type support:
            // - No type-specific operators or functions
            // - No validation or type safety
            // - Data is accessible but with reduced functionality
            put("variant", VarcharType.VARCHAR)  // DEGRADED: No variant support, needs shredding implementation
            put("interval", VarcharType.VARCHAR)  // DEGRADED: Interval not supported in Trino, stored as string

            // DEGRADED: Geometry types stored as raw bytes, no spatial functions.
            // DuckLake 1.0 renamed "linestring z" to "linestring_z"; we accept both
            // so catalogs written against either spec version read correctly.
            for (geometry in listOf("geometry", "point", "linestring", "polygon",
                    "multipoint", "multilinestring", "multipolygon",
                    "linestring_z", "linestring z", "geometrycollection")) {
                put(geometry, VarbinaryType.VARBINARY)
            }
        }

        private val SCALAR_DUCKLAKE_TYPES: Map<Type, String> = mapOf(
                BooleanType.BOOLEAN to "boolean",
                TinyintType.TINYINT to "int8",
                SmallintType.SMALLINT to "int16",
                IntegerType.INTEGER to "int32",
                BigintType.BIGINT to "int64",
                RealType.REAL to "float32",
                DoubleType.DOUBLE to "float64",
                DateType.DATE to "date",
                VarbinaryType.VARBINARY to "blob",
                UuidType.UUID to "uuid")
    }
}
