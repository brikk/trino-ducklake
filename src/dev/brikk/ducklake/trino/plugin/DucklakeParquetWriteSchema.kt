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

import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.type.ArrayType
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.TimeType
import io.trino.spi.type.TimeType.TIME_MICROS
import io.trino.spi.type.TimeWithTimeZoneType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Type.Repetition.OPTIONAL
import org.apache.parquet.schema.Type.Repetition.REQUIRED
import org.apache.parquet.schema.Types
import org.apache.parquet.schema.Type as ParquetType

/**
 * The Parquet write schema (without field IDs; see [DucklakeParquetSchemaBuilder]) and the Trino
 * type of every leaf, as trino-parquet's `ParquetWriter` consumes them.
 *
 * trino-parquet's [ParquetSchemaConverter] covers only the Hive type set, so `time(6)`,
 * `timestamp(6) with time zone` and `uuid` leaves are annotated here, exactly as DuckDB writes the
 * DuckLake `time`, `timestamptz` and `uuid` types; `ParquetWriters` then selects its
 * `TimeMicros`/`TimestampTzMicros`/`Uuid` value writers from the leaf's Trino type. Every other
 * leaf is delegated to [ParquetSchemaConverter], and groups use its LIST/MAP/struct layout. Types
 * DuckLake stores in a form no Trino value writer produces are refused before any file is opened.
 */
class DucklakeParquetWriteSchema(types: List<Type>, names: List<String>) {
    val messageType: MessageType
    val primitiveTypes: Map<List<String>, Type>

    init {
        require(types.size == names.size) { "types size ${types.size} != names size ${names.size}" }
        val leaves = linkedMapOf<List<String>, Type>()
        val builder = Types.buildMessage()
        types.zip(names).forEach { (type, name) ->
            builder.addField(convert(DucklakeJsonSupport.toParquetWriteType(type), name, emptyList(), OPTIONAL, leaves))
        }
        messageType = builder.named("trino_schema")
        primitiveTypes = leaves
    }

    private fun convert(
            type: Type,
            name: String,
            parent: List<String>,
            repetition: Repetition,
            leaves: MutableMap<List<String>, Type>): ParquetType {
        val path = parent + name
        return when (type) {
            is RowType -> Types.buildGroup(repetition)
                    .apply {
                        type.fields.forEach { field ->
                            val fieldName = field.name.orElseThrow { TrinoException(NOT_SUPPORTED, "Anonymous row fields not supported") }
                            addField(convert(field.type, fieldName, path, OPTIONAL, leaves))
                        }
                    }
                    .named(name)
            is ArrayType -> Types.list(repetition)
                    .element(convert(type.elementType, "element", path + "list", OPTIONAL, leaves))
                    .named(name)
            is MapType -> Types.map(repetition)
                    .key(convert(type.keyType, "key", path + "key_value", REQUIRED, leaves))
                    .value(convert(type.valueType, "value", path + "key_value", OPTIONAL, leaves))
                    .named(name)
            else -> {
                leaves[path] = type
                primitive(type, name, repetition)
            }
        }
    }

    private fun primitive(type: Type, name: String, repetition: Repetition): PrimitiveType = when {
        type == TIME_MICROS -> Types.primitive(INT64, repetition)
                .`as`(LogicalTypeAnnotation.timeType(false, MICROS))
                .named(name)
        type == TIMESTAMP_TZ_MICROS -> Types.primitive(INT64, repetition)
                .`as`(LogicalTypeAnnotation.timestampType(true, MICROS))
                .named(name)
        type is UuidType -> Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
                .length(UUID_BYTES)
                .`as`(LogicalTypeAnnotation.uuidType())
                .named(name)
        type is TimeWithTimeZoneType -> throw TrinoException(NOT_SUPPORTED,
                "Writing $type to Parquet is not supported: Parquet has no time-with-time-zone logical type " +
                        "(DuckDB stores timetz as TIME(MICROS, isAdjustedToUTC=true) normalized to UTC, without the offset), " +
                        "and Trino's Parquet reader cannot decode that column as $type")
        // DuckLake time and timestamptz are microseconds only; DDL maps nothing else to them.
        type is TimeType || type is TimestampWithTimeZoneType -> throw TrinoException(NOT_SUPPORTED,
                "Writing $type to Parquet is not supported: DuckLake stores this type with microsecond precision")
        type is TimestampType && type.precision == 0 -> throw TrinoException(NOT_SUPPORTED,
                "Writing $type to Parquet is not supported yet: DuckDB stores DuckLake timestamp_s as " +
                        "INT64 TIMESTAMP(MICROS), and trino-parquet has no value writer for $type")
        else -> withRepetition(
                ParquetSchemaConverter(listOf(type), listOf(name), false, false).messageType.getType(0).asPrimitiveType(),
                repetition)
    }

    private fun withRepetition(source: PrimitiveType, repetition: Repetition): PrimitiveType {
        if (source.repetition == repetition) {
            return source
        }
        var builder = Types.primitive(source.primitiveTypeName, repetition)
        source.logicalTypeAnnotation?.let { builder = builder.`as`(it) }
        if (source.primitiveTypeName == FIXED_LEN_BYTE_ARRAY) {
            builder = builder.length(source.typeLength)
        }
        return builder.named(source.name)
    }

    private companion object {
        const val UUID_BYTES = 16
    }
}
