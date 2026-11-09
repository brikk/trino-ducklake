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
import io.trino.spi.TrinoException
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType.createDecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.MapType
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimeType.TIME_MICROS
import io.trino.spi.type.TimeType.TIME_MILLIS
import io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MICROS
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS
import io.trino.spi.type.TimestampType.TIMESTAMP_NANOS
import io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.TypeOperators
import io.trino.spi.type.UuidType.UUID
import io.trino.spi.type.VarbinaryType.VARBINARY
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.spi.type.VarcharType.createVarcharType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.Repetition.OPTIONAL
import org.apache.parquet.schema.Type.Repetition.REQUIRED
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.Optional

class TestDucklakeParquetWriteSchema {
    private val typeOperators = TypeOperators()

    @Test
    fun hiveTypesKeepTrinoParquetsSchemaAndLeafTypes() {
        val types = listOf(
                BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, DATE,
                createDecimalType(9, 2), createDecimalType(18, 4), createDecimalType(38, 10),
                TIMESTAMP_MILLIS, TIMESTAMP_MICROS, TIMESTAMP_NANOS,
                VARCHAR, createVarcharType(10), VARBINARY,
                ArrayType(createDecimalType(38, 0)),
                MapType(VARCHAR, ArrayType(BIGINT), typeOperators),
                MapType(createDecimalType(20, 2), DATE, typeOperators),
                row("a" to INTEGER, "b" to row("c" to TIMESTAMP_MICROS, "d" to ArrayType(VARBINARY))))
        val names = types.indices.map { "c$it" }

        val ours = DucklakeParquetWriteSchema(types, names)
        val trino = ParquetSchemaConverter(types, names, false, false)

        assertThat(ours.messageType).isEqualTo(trino.messageType)
        assertThat(ours.primitiveTypes).isEqualTo(trino.primitiveTypes)
    }

    @Test
    fun ducklakeTypesUseDuckdbsAnnotations() {
        val schema = DucklakeParquetWriteSchema(listOf(TIME_MICROS, TIMESTAMP_TZ_MICROS, UUID), listOf("t", "ts", "u"))

        assertPrimitive(schema.messageType, listOf("t"), INT64, LogicalTypeAnnotation.timeType(false, MICROS), OPTIONAL)
        assertPrimitive(schema.messageType, listOf("ts"), INT64, LogicalTypeAnnotation.timestampType(true, MICROS), OPTIONAL)
        assertPrimitive(schema.messageType, listOf("u"), FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType(), OPTIONAL)
        assertThat(schema.messageType.getType("u").asPrimitiveType().typeLength).isEqualTo(16)
        assertThat(schema.primitiveTypes).isEqualTo(mapOf(
                listOf("t") to TIME_MICROS,
                listOf("ts") to TIMESTAMP_TZ_MICROS,
                listOf("u") to UUID))
    }

    @Test
    fun nestedDucklakeTypesKeepTheirAnnotationsAndRepetition() {
        val types = listOf(
                row("a" to TIME_MICROS, "b" to UUID, "c" to TIMESTAMP_TZ_MICROS),
                ArrayType(UUID),
                MapType(UUID, TIMESTAMP_TZ_MICROS, typeOperators),
                MapType(TIME_MICROS, ArrayType(row("x" to UUID)), typeOperators))
        val schema = DucklakeParquetWriteSchema(types, listOf("n", "l", "m", "deep")).messageType

        assertPrimitive(schema, listOf("n", "a"), INT64, LogicalTypeAnnotation.timeType(false, MICROS), OPTIONAL)
        assertPrimitive(schema, listOf("n", "b"), FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType(), OPTIONAL)
        assertPrimitive(schema, listOf("n", "c"), INT64, LogicalTypeAnnotation.timestampType(true, MICROS), OPTIONAL)
        assertPrimitive(schema, listOf("l", "list", "element"), FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType(), OPTIONAL)
        assertPrimitive(schema, listOf("m", "key_value", "key"), FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType(), REQUIRED)
        assertPrimitive(schema, listOf("m", "key_value", "value"), INT64, LogicalTypeAnnotation.timestampType(true, MICROS), OPTIONAL)
        assertPrimitive(schema, listOf("deep", "key_value", "key"), INT64, LogicalTypeAnnotation.timeType(false, MICROS), REQUIRED)
        assertPrimitive(schema, listOf("deep", "key_value", "value", "list", "element", "x"),
                FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType(), OPTIONAL)
    }

    @Test
    fun mapKeysDelegatedToTrinoParquetStayRequired() {
        val schema = DucklakeParquetWriteSchema(listOf(MapType(createDecimalType(30, 2), VARCHAR, typeOperators)), listOf("m"))
        val key = schema.messageType.getType("m").asGroupType().getType("key_value").asGroupType().getType("key").asPrimitiveType()

        assertThat(key.repetition).isEqualTo(REQUIRED)
        assertThat(key.primitiveTypeName).isEqualTo(FIXED_LEN_BYTE_ARRAY)
        assertThat(key.typeLength).isEqualTo(13)
        assertThat(key.logicalTypeAnnotation).isEqualTo(LogicalTypeAnnotation.decimalType(2, 30))
    }

    @Test
    fun typesWithoutAnExactParquetEncodingAreRefused() {
        assertRefused(TIME_TZ_MICROS, "Parquet has no time-with-time-zone logical type")
        assertRefused(ArrayType(TIME_TZ_MICROS), "Parquet has no time-with-time-zone logical type")
        assertRefused(TIMESTAMP_SECONDS, "trino-parquet has no value writer for timestamp(0)")
        assertRefused(row("x" to TIMESTAMP_SECONDS), "trino-parquet has no value writer for timestamp(0)")
        assertRefused(TIMESTAMP_TZ_MILLIS, "DuckLake stores this type with microsecond precision")
        assertRefused(MapType(VARCHAR, TIMESTAMP_TZ_NANOS, typeOperators), "DuckLake stores this type with microsecond precision")
        assertRefused(TIME_MILLIS, "DuckLake stores this type with microsecond precision")
    }

    private fun assertRefused(type: Type, message: String) {
        assertThatThrownBy { DucklakeParquetWriteSchema(listOf(type), listOf("c")) }
                .isInstanceOf(TrinoException::class.java)
                .hasMessageContaining(message)
    }

    private fun assertPrimitive(
            schema: MessageType,
            path: List<String>,
            physical: PrimitiveType.PrimitiveTypeName,
            annotation: LogicalTypeAnnotation,
            repetition: org.apache.parquet.schema.Type.Repetition) {
        val leaf = schema.getType(*path.toTypedArray()).asPrimitiveType()
        assertThat(leaf.primitiveTypeName).`as`("physical type of %s", path).isEqualTo(physical)
        assertThat(leaf.logicalTypeAnnotation).`as`("annotation of %s", path).isEqualTo(annotation)
        assertThat(leaf.repetition).`as`("repetition of %s", path).isEqualTo(repetition)
    }

    private fun row(vararg fields: Pair<String, Type>): RowType =
            RowType.from(fields.map { (name, type) -> RowType.Field(Optional.of(name), type) })
}
