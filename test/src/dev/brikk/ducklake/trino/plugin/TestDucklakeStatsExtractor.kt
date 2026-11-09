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
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.TimeType.TIME_MICROS
import io.trino.spi.type.TimeType.TIME_MILLIS
import io.trino.spi.type.TimeType.TIME_NANOS
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.spi.type.UuidType.UUID
import io.trino.spi.type.VarcharType.VARCHAR
import org.apache.parquet.format.ColumnChunk
import org.apache.parquet.format.ColumnMetaData
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.format.Encoding
import org.apache.parquet.format.FileMetaData
import org.apache.parquet.format.RowGroup
import org.apache.parquet.format.SchemaElement
import org.apache.parquet.format.Statistics
import org.apache.parquet.format.Type
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder

internal class TestDucklakeStatsExtractor {
    @Test
    fun testConvertIntegerStats() {
        val bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(42).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, INTEGER)).isEqualTo("42")
    }

    @Test
    fun timestampStatsUsePhysicalUnitsAndDuckdbText() {
        assertThat(DucklakeStatsExtractor.convertStatValue(
                longBytes(0L), TimestampType.createTimestampType(0), Type.INT64))
                .isEqualTo("1970-01-01 00:00:00")
        assertThat(DucklakeStatsExtractor.convertStatValue(
                longBytes(123L), TimestampType.createTimestampType(3), Type.INT64))
                .isEqualTo("1970-01-01 00:00:00.123")
        assertThat(DucklakeStatsExtractor.convertStatValue(
                longBytes(123_456L), TimestampType.createTimestampType(6), Type.INT64))
                .isEqualTo("1970-01-01 00:00:00.123456")
        assertThat(DucklakeStatsExtractor.convertStatValue(
                longBytes(123_456_789L), TimestampType.createTimestampType(9), Type.INT64))
                .isEqualTo("1970-01-01 00:00:00.123456789")
        assertThat(DucklakeStatsExtractor.convertStatValue(
                longBytes(123_456L), TIMESTAMP_TZ_MICROS, Type.INT64))
                .isEqualTo("1970-01-01 00:00:00.123456+00")
    }

    @Test
    fun timeStatsUseThePhysicalUnitAndDuckdbText() {
        val schema = Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64).`as`(LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS)).named("micros"))
                .addField(Types.optional(PrimitiveTypeName.INT32).`as`(LogicalTypeAnnotation.timeType(false, TimeUnit.MILLIS)).named("millis"))
                .addField(Types.optional(PrimitiveTypeName.INT64).`as`(LogicalTypeAnnotation.timeType(false, TimeUnit.NANOS)).named("nanos"))
                .named("root")
        val file = fileMetaData(rowGroup(2,
                column(Type.INT64, longBytes(45_296_000_000L), longBytes(86_399_999_999L), 0),
                column(Type.INT32, intBytes(0), intBytes(45_296_500), 0),
                column(Type.INT64, longBytes(1L), longBytes(45_296_123_456_789L), 0)))

        val stats = DucklakeStatsExtractor.extractStats(
                file,
                listOf(LeafStatsTarget(1L, TIME_MICROS, 0), LeafStatsTarget(2L, TIME_MILLIS, 1), LeafStatsTarget(3L, TIME_NANOS, 2)),
                schema)

        assertThat(stats.map { it.minValue to it.maxValue }).containsExactly(
                "12:34:56" to "23:59:59.999999",
                "00:00:00" to "12:34:56.5",
                "00:00:00.000000001" to "12:34:56.123456789")
        assertThat(DucklakeStatsExtractor.convertStatValue(longBytes(0L), TIME_MICROS, Type.INT64))
                .`as`("a TIME leaf without its annotation has no known unit")
                .isNull()
    }

    @Test
    fun uuidStatsAreDuckdbsLowercaseText() {
        for (text in listOf("550e8400-e29b-41d4-a716-446655440000", "ffffffff-0000-0000-8000-00000000000a")) {
            val uuid = java.util.UUID.fromString(text)
            val bytes = ByteBuffer.allocate(16).putLong(uuid.mostSignificantBits).putLong(uuid.leastSignificantBits).array()
            assertThat(DucklakeStatsExtractor.convertStatValue(bytes, UUID, Type.FIXED_LEN_BYTE_ARRAY)).isEqualTo(text)
        }
        assertThat(DucklakeStatsExtractor.convertStatValue(ByteArray(15), UUID, Type.FIXED_LEN_BYTE_ARRAY)).isNull()
    }

    @Test
    fun testConvertNegativeInteger() {
        val bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(-17).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, INTEGER)).isEqualTo("-17")
    }

    @Test
    fun testConvertBigintStats() {
        val bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(9_000_000_000L).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, BIGINT)).isEqualTo("9000000000")
    }

    @Test
    fun testConvertDoubleStats() {
        val bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(3.14).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, DOUBLE)).isEqualTo("3.14")
    }

    @Test
    fun testConvertDoubleNan() {
        val bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(Double.NaN).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, DOUBLE)).isNull()
    }

    @Test
    fun testConvertRealStats() {
        val bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(2.5f).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, REAL)).isEqualTo("2.5")
    }

    @Test
    fun testConvertVarcharStats() {
        val bytes = "hello world".toByteArray(java.nio.charset.StandardCharsets.UTF_8)
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, VARCHAR)).isEqualTo("hello world")
    }

    @Test
    fun testConvertBooleanTrue() {
        val bytes = byteArrayOf(1)
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, BOOLEAN)).isEqualTo("true")
    }

    @Test
    fun testConvertBooleanFalse() {
        val bytes = byteArrayOf(0)
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, BOOLEAN)).isEqualTo("false")
    }

    @Test
    fun testConvertDateStats() {
        val epochDay = java.time.LocalDate.of(2024, 1, 15).toEpochDay().toInt()
        val bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(epochDay).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, DATE)).isEqualTo("2024-01-15")
    }

    @Test
    fun testConvertShortDecimalInt32IsLittleEndian() {
        // decimal(5,2) value 123.45 -> unscaled 12345, stored by parquet as a
        // little-endian INT32. Decoding it big-endian yields garbage.
        val type = DecimalType.createDecimalType(5, 2)
        val bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(12345).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, type, org.apache.parquet.format.Type.INT32))
                .isEqualTo("123.45")
    }

    @Test
    fun testShortDecimalInt32NegativeIsLittleEndian() {
        val type = DecimalType.createDecimalType(5, 2)
        val bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(-12345).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, type, org.apache.parquet.format.Type.INT32))
                .isEqualTo("-123.45")
    }

    @Test
    fun testShortDecimalInt64IsLittleEndian() {
        // decimal(18,4) value 12345.6789 -> unscaled 123456789, stored as little-endian INT64.
        val type = DecimalType.createDecimalType(18, 4)
        val bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(123_456_789L).array()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, type, org.apache.parquet.format.Type.INT64))
                .isEqualTo("12345.6789")
    }

    @Test
    fun testLongDecimalFixedLenByteArrayIsBigEndian() {
        // High-precision decimals are FIXED_LEN_BYTE_ARRAY / BINARY: big-endian two's complement.
        val type = DecimalType.createDecimalType(38, 0)
        val bytes = BigInteger.valueOf(1_000_000L).toByteArray()
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, type, org.apache.parquet.format.Type.FIXED_LEN_BYTE_ARRAY))
                .isEqualTo("1000000")
    }

    @Test
    fun testConvertEmptyValue() {
        assertThat(DucklakeStatsExtractor.convertStatValue(ByteArray(0), INTEGER)).isNull()
    }

    @Test
    fun testConvertNullValue() {
        assertThat(DucklakeStatsExtractor.convertStatValue(null, INTEGER)).isNull()
    }

    @Test
    fun extractStatsKeysOutputByLeafFieldId() {
        val file = fileMetaData(
                rowGroup(100L,
                        column(INT_TYPE, intBytes(1), intBytes(5), 0L),
                        column(BYTE_ARRAY_TYPE, utf8Bytes("alpha"), utf8Bytes("delta"), 1L)))

        val stats = DucklakeStatsExtractor.extractStats(
                file,
                listOf(
                        LeafStatsTarget(42L, INTEGER, 0),
                        LeafStatsTarget(43L, VARCHAR, 1)))

        assertThat(stats).hasSize(2)
        assertThat(stats[0].columnId).isEqualTo(42L)
        assertThat(stats[0].minValue).contains("1")
        assertThat(stats[0].maxValue).contains("5")
        assertThat(stats[0].nullCount).isEqualTo(0L)
        assertThat(stats[1].columnId).isEqualTo(43L)
        assertThat(stats[1].minValue).contains("alpha")
        assertThat(stats[1].maxValue).contains("delta")
        assertThat(stats[1].nullCount).isEqualTo(1L)
    }

    @Test
    fun extractStatsForStructLeavesUsesChildFieldIds() {
        // A table-shaped (id INTEGER, data ROW(a INTEGER, b VARCHAR)). Three parquet
        // leaves: id at index 0, data.a at index 1, data.b at index 2. Targets carry
        // the leaf-level field_ids (catalog children of the ROW).
        val file = fileMetaData(
                rowGroup(50L,
                        column(INT_TYPE, intBytes(1), intBytes(3), 0L),
                        column(INT_TYPE, intBytes(7), intBytes(9), 2L),
                        column(BYTE_ARRAY_TYPE, utf8Bytes("aa"), utf8Bytes("zz"), 5L)))

        val stats = DucklakeStatsExtractor.extractStats(
                file,
                listOf(
                        LeafStatsTarget(10L, INTEGER, 0),
                        LeafStatsTarget(21L, INTEGER, 1),
                        LeafStatsTarget(22L, VARCHAR, 2)))

        assertThat(stats).extracting(java.util.function.Function<DucklakeFileColumnStats, Long> { it.columnId }).containsExactly(10L, 21L, 22L)
        assertThat(stats[1].minValue).contains("7")
        assertThat(stats[1].maxValue).contains("9")
        assertThat(stats[1].nullCount).isEqualTo(2L)
        assertThat(stats[2].minValue).contains("aa")
        assertThat(stats[2].maxValue).contains("zz")
        assertThat(stats[2].nullCount).isEqualTo(5L)
    }

    @Test
    fun extractStatsRespectsNonContiguousParquetColumnIndices() {
        // Simulates an add_files case where a parquet column at index 1 has been
        // skipped (ignore_extra_columns) so the mapped leaves are at indices 0 and 2.
        // The extractor must look up the correct column chunk by the explicit index.
        val file = fileMetaData(
                rowGroup(20L,
                        column(INT_TYPE, intBytes(1), intBytes(1), 0L),
                        column(INT_TYPE, intBytes(99), intBytes(99), 0L), // skipped
                        column(BYTE_ARRAY_TYPE, utf8Bytes("x"), utf8Bytes("y"), 0L)))

        val stats = DucklakeStatsExtractor.extractStats(
                file,
                listOf(
                        LeafStatsTarget(100L, INTEGER, 0),
                        LeafStatsTarget(101L, VARCHAR, 2)))

        assertThat(stats).extracting(java.util.function.Function<DucklakeFileColumnStats, Long> { it.columnId }).containsExactly(100L, 101L)
        assertThat(stats[0].maxValue).contains("1")
        // If parquetColumnIndex were positional in the leaf list, the second target
        // would have picked up the skipped column's INT bytes and decoded "99" instead
        // of the VARCHAR-decoded "x"/"y" — pin against that regression.
        assertThat(stats[1].minValue!!).doesNotContain("99")
        assertThat(stats[1].minValue).contains("x")
        assertThat(stats[1].maxValue).contains("y")
    }

    @Test
    fun extractStatsMergesStatisticsAcrossRowGroups() {
        val file = fileMetaData(
                rowGroup(10L, column(INT_TYPE, intBytes(3), intBytes(8), 1L)),
                rowGroup(10L, column(INT_TYPE, intBytes(1), intBytes(12), 2L)))

        val stats = DucklakeStatsExtractor.extractStats(
                file, listOf(LeafStatsTarget(7L, INTEGER, 0)))

        assertThat(stats).hasSize(1)
        // value_count is the NON-NULL count: each row group has num_values=10, with 1 and 2 nulls
        // respectively, so (10-1) + (10-2) = 17. null counts accumulate to 3.
        assertThat(stats[0].valueCount).isEqualTo(17L)
        assertThat(stats[0].nullCount).isEqualTo(3L)
        // Stats stored as text must merge numerically: lexically "8" > "12".
        assertThat(stats[0].minValue).contains("1")
        assertThat(stats[0].maxValue).contains("12")
    }

    @Test
    fun extractStatsValueCountExcludesNulls() {
        // A single row group of 100 values with 30 nulls must report value_count=70, not 100. The invariant the
        // DucklakeMetadata consumer relies on is value_count + null_count == row count, matching
        // the DuckDB writer path (COUNT(col) for value_count) and the DuckLake spec
        // (value_count = num_values - null_count). Emitting num_values (100) would inflate the
        // consumer's totalCount past the file row count and suppress the column's stats.
        val file = fileMetaData(
                rowGroup(100L, column(INT_TYPE, intBytes(1), intBytes(99), 30L)))

        val stats = DucklakeStatsExtractor.extractStats(
                file, listOf(LeafStatsTarget(7L, INTEGER, 0)))

        assertThat(stats).hasSize(1)
        assertThat(stats[0].valueCount).isEqualTo(70L)
        assertThat(stats[0].nullCount).isEqualTo(30L)
        assertThat(stats[0].valueCount!! + stats[0].nullCount!!).isEqualTo(100L)
    }

    @Test
    fun missingBoundsInOneNonNullRowGroupMakesFileBoundsUnknown() {
        val file = fileMetaData(
                rowGroup(10L, column(INT_TYPE, intBytes(1), intBytes(10), 0L)),
                rowGroup(10L, columnWithoutBounds(INT_TYPE, 0L)))

        val stats = DucklakeStatsExtractor.extractStats(
                file, listOf(LeafStatsTarget(7L, INTEGER, 0))).single()

        assertThat(stats.valueCount).isEqualTo(20L)
        assertThat(stats.nullCount).isZero()
        assertThat(stats.minValue).isNull()
        assertThat(stats.maxValue).isNull()
    }

    @Test
    fun missingNullCountMakesFileCountsAndBoundsUnknown() {
        val file = fileMetaData(rowGroup(10L, columnWithoutBounds(INT_TYPE, null)))
        val stats = DucklakeStatsExtractor.extractStats(
                file, listOf(LeafStatsTarget(7L, INTEGER, 0))).single()

        assertThat(stats.valueCount).isNull()
        assertThat(stats.nullCount).isNull()
        assertThat(stats.minValue).isNull()
        assertThat(stats.maxValue).isNull()
    }

    companion object {
        private val INT_TYPE: Type = Type.INT32
        private val BYTE_ARRAY_TYPE: Type = Type.BYTE_ARRAY

        private fun fileMetaData(vararg rowGroups: RowGroup): FileMetaData {
            val fm = FileMetaData()
            fm.setVersion(1)
            fm.setSchema(listOf(SchemaElement("root")))
            fm.setNum_rows(0L)
            fm.setRow_groups(listOf(*rowGroups))
            return fm
        }

        private fun rowGroup(valueCountPerColumn: Long, vararg columns: ColumnChunk): RowGroup {
            val rg = RowGroup()
            rg.setColumns(listOf(*columns))
            // num_rows must be set on a valid thrift RowGroup, but extractStats reads
            // num_values from each column chunk, not row count.
            rg.setNum_rows(valueCountPerColumn)
            rg.setTotal_byte_size(0L)
            for (chunk in columns) {
                chunk.getMeta_data().setNum_values(valueCountPerColumn)
            }
            return rg
        }

        private fun column(type: Type, minValue: ByteArray, maxValue: ByteArray, nullCount: Long): ColumnChunk {
            val meta = ColumnMetaData()
            meta.setType(type)
            meta.setEncodings(listOf(Encoding.PLAIN))
            meta.setPath_in_schema(listOf("leaf"))
            meta.setCodec(CompressionCodec.UNCOMPRESSED)
            meta.setNum_values(0L) // overwritten per row group
            meta.setTotal_uncompressed_size(0L)
            meta.setTotal_compressed_size(0L)
            meta.setData_page_offset(0L)

            val stats = Statistics()
            stats.setMin_value(minValue)
            stats.setMax_value(maxValue)
            stats.setNull_count(nullCount)
            meta.setStatistics(stats)

            val chunk = ColumnChunk()
            chunk.setFile_offset(0L)
            chunk.setMeta_data(meta)
            return chunk
        }

        private fun columnWithoutBounds(type: Type, nullCount: Long?): ColumnChunk {
            val meta = ColumnMetaData()
            meta.setType(type)
            meta.setEncodings(listOf(Encoding.PLAIN))
            meta.setPath_in_schema(listOf("leaf"))
            meta.setCodec(CompressionCodec.UNCOMPRESSED)
            meta.setNum_values(0L)
            meta.setTotal_uncompressed_size(0L)
            meta.setTotal_compressed_size(0L)
            meta.setData_page_offset(0L)
            if (nullCount != null) {
                meta.setStatistics(Statistics().setNull_count(nullCount))
            }
            val chunk = ColumnChunk()
            chunk.setFile_offset(0L)
            chunk.setMeta_data(meta)
            return chunk
        }

        private fun intBytes(v: Int): ByteArray {
            return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()
        }

        private fun longBytes(v: Long): ByteArray =
            ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

        private fun utf8Bytes(s: String): ByteArray {
            return s.toByteArray(java.nio.charset.StandardCharsets.UTF_8)
        }
    }
}
