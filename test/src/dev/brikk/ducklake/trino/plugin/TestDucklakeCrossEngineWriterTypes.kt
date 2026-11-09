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

import io.trino.spi.type.TimeType.TIME_MICROS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType.UUID
import io.trino.spi.type.UuidType.javaUuidToTrinoUuid
import io.trino.testing.MaterializedRow
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager
import java.time.LocalTime
import java.time.OffsetDateTime

/**
 * `time(6)`, `timestamp(6) with time zone` and `uuid` written by Trino and by DuckDB for the same
 * rows must be the same files to each other engine: identical Parquet leaf annotations, catalog
 * statistics, partition values and values read back in both directions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCrossEngineWriterTypes : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "cross-engine-writer-types"

    private val trinoColumns = "id INTEGER, t TIME(6), ts TIMESTAMP(6) WITH TIME ZONE, u UUID, " +
            "n ROW(a TIME(6), b UUID, c TIMESTAMP(6) WITH TIME ZONE), l ARRAY(UUID), m MAP(UUID, TIMESTAMP(6) WITH TIME ZONE)"
    private val duckdbColumns = "id INTEGER, t TIME, ts TIMESTAMPTZ, u UUID, " +
            "n STRUCT(a TIME, b UUID, c TIMESTAMPTZ), l UUID[], m MAP(UUID, TIMESTAMPTZ)"
    private val nestedType = "ROW(a TIME(6), b UUID, c TIMESTAMP(6) WITH TIME ZONE)"

    private val trinoRows = listOf(
            "(1, TIME '00:00:00', TIMESTAMP '1970-01-01 00:00:00 UTC', UUID '00000000-0000-0000-0000-000000000000', " +
                    "CAST(ROW(TIME '00:00:00.000001', UUID '00000000-0000-0000-0000-000000000001', " +
                    "TIMESTAMP '1969-12-31 23:59:59.999999 UTC') AS $nestedType), " +
                    "ARRAY[UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff', NULL], " +
                    "MAP(ARRAY[UUID '7fffffff-ffff-ffff-ffff-ffffffffffff'], ARRAY[TIMESTAMP '2024-02-29 12:00:00.5 UTC']))",
            "(2, TIME '23:59:59.999999', TIMESTAMP '2024-06-15 08:30:00.123456 America/New_York', " +
                    "UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff', CAST(ROW(NULL, NULL, NULL) AS $nestedType), ARRAY[], MAP())",
            "(3, TIME '12:34:56.5', TIMESTAMP '1900-01-01 00:00:00.000001 UTC', UUID '550e8400-e29b-41d4-a716-446655440000', " +
                    "NULL, NULL, NULL)",
            "(4, NULL, NULL, NULL, NULL, NULL, NULL)")
    private val duckdbRows = listOf(
            "(1, TIME '00:00:00', TIMESTAMPTZ '1970-01-01 00:00:00+00', UUID '00000000-0000-0000-0000-000000000000', " +
                    "{'a': TIME '00:00:00.000001', 'b': UUID '00000000-0000-0000-0000-000000000001', " +
                    "'c': TIMESTAMPTZ '1969-12-31 23:59:59.999999+00'}, " +
                    "[UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff', NULL], " +
                    "MAP {UUID '7fffffff-ffff-ffff-ffff-ffffffffffff': TIMESTAMPTZ '2024-02-29 12:00:00.5+00'})",
            "(2, TIME '23:59:59.999999', TIMESTAMPTZ '2024-06-15 12:30:00.123456+00', " +
                    "UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff', {'a': NULL, 'b': NULL, 'c': NULL}, [], MAP {})",
            "(3, TIME '12:34:56.5', TIMESTAMPTZ '1900-01-01 00:00:00.000001+00', UUID '550e8400-e29b-41d4-a716-446655440000', " +
                    "NULL, NULL, NULL)",
            "(4, NULL, NULL, NULL, NULL, NULL, NULL)")

    private val fixture: Pair<String, String> by lazy {
        computeActual("CREATE TABLE test_schema.xw_trino ($trinoColumns)")
        computeActual("INSERT INTO test_schema.xw_trino VALUES ${trinoRows.joinToString(", ")}")
        assertRowsWrittenToParquet("xw_trino")
        duckdb(
                "CREATE TABLE ducklake_db.test_schema.xw_duck ($duckdbColumns)",
                "CALL ducklake_db.set_option('data_inlining_row_limit', 0, schema => 'test_schema', table_name => 'xw_duck')",
                "INSERT INTO ducklake_db.test_schema.xw_duck VALUES ${duckdbRows.joinToString(", ")}")
        assertRowsWrittenToParquet("xw_duck")
        "xw_trino" to "xw_duck"
    }

    @Test
    fun parquetLeavesCarryDuckdbsAnnotations() {
        val (trino, duck) = fixture
        val leaves = { table: String ->
            duckdbRows("SELECT name, type, type_length, converted_type, logical_type, repetition_type " +
                    "FROM parquet_schema(${dataFileList(table)}) WHERE type IS NOT NULL AND name <> 'id'")
        }
        assertThat(leaves(trino)).hasSize(9).isEqualTo(leaves(duck))
    }

    @Test
    fun duckdbReadsTrinoFilesAsItsOwn() {
        val (trino, duck) = fixture
        assertThat(duckdbRows("SELECT count(*) FROM (SELECT * FROM ducklake_db.test_schema.$trino " +
                "EXCEPT SELECT * FROM ducklake_db.test_schema.$duck)")).containsExactly("0")
        assertThat(duckdbRows("SELECT count(*) FROM (SELECT * FROM ducklake_db.test_schema.$duck " +
                "EXCEPT SELECT * FROM ducklake_db.test_schema.$trino)")).containsExactly("0")
        val text = { table: String -> duckdbRows("SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ducklake_db.test_schema.$table ORDER BY id") }
        assertThat(text(trino)).isEqualTo(text(duck))
        assertThat(duckdbRows("SELECT id, t::VARCHAR, ts::VARCHAR, u::VARCHAR FROM ducklake_db.test_schema.$trino ORDER BY id")).containsExactly(
                "1|00:00:00|1970-01-01 00:00:00+00|00000000-0000-0000-0000-000000000000",
                "2|23:59:59.999999|2024-06-15 12:30:00.123456+00|ffffffff-ffff-ffff-ffff-ffffffffffff",
                "3|12:34:56.5|1900-01-01 00:00:00.000001+00|550e8400-e29b-41d4-a716-446655440000",
                "4|null|null|null")
    }

    @Test
    fun trinoReadsDuckdbFilesAsItsOwn() {
        val (trino, duck) = fixture
        val rows = { table: String -> computeActual("SELECT * FROM test_schema.$table ORDER BY id").materializedRows }
        assertThat(rows(duck)).hasSize(4).isEqualTo(rows(trino))
        assertThat(computeActual("SELECT count(*) FROM (SELECT * FROM test_schema.$duck EXCEPT SELECT * FROM test_schema.$trino)")
                .onlyValue).isEqualTo(0L)
        assertThat(computeActual("SELECT id, t, ts, u FROM test_schema.$duck WHERE id < 4 ORDER BY id").materializedRows
                .map(MaterializedRow::getFields))
                .containsExactly(
                        listOf(1, LocalTime.MIDNIGHT, utc("1970-01-01T00:00:00Z"), java.util.UUID.fromString("00000000-0000-0000-0000-000000000000")),
                        listOf(2, LocalTime.parse("23:59:59.999999"), utc("2024-06-15T12:30:00.123456Z"),
                                java.util.UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")),
                        listOf(3, LocalTime.parse("12:34:56.5"), utc("1900-01-01T00:00:00.000001Z"),
                                java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000")))
    }

    @Test
    fun catalogStatisticsEqualDuckdbs() {
        val (trino, duck) = fixture
        val fileStats = { table: String ->
            catalogRows("SELECT c.column_name, s.min_value, s.max_value, s.null_count, s.value_count " +
                    "FROM ducklake_file_column_stats s " +
                    "JOIN ducklake_data_file f ON f.data_file_id = s.data_file_id AND f.end_snapshot IS NULL " +
                    "JOIN ducklake_column c ON c.column_id = s.column_id AND c.table_id = s.table_id AND c.end_snapshot IS NULL " +
                    "JOIN ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL " +
                    "WHERE t.table_name = '$table' ORDER BY c.column_id")
        }
        val tableStats = { table: String ->
            catalogRows("SELECT c.column_name, s.contains_null, s.min_value, s.max_value " +
                    "FROM ducklake_table_column_stats s " +
                    "JOIN ducklake_column c ON c.column_id = s.column_id AND c.table_id = s.table_id AND c.end_snapshot IS NULL " +
                    "JOIN ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL " +
                    "WHERE t.table_name = '$table' ORDER BY c.column_id")
        }
        assertThat(fileStats(trino)).isEqualTo(fileStats(duck))
        assertThat(fileStats(trino)).contains(
                "t|00:00:00|23:59:59.999999|1|3",
                "ts|1900-01-01 00:00:00.000001+00|2024-06-15 12:30:00.123456+00|1|3",
                "u|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff|1|3")
        assertThat(tableStats(trino)).isEqualTo(tableStats(duck))
    }

    @Test
    fun partitionValuesAndBucketsEqualDuckdbs() {
        val values = listOf(
                Triple("TIMESTAMP '2024-01-01 12:00:00.123456 UTC'", "TIMESTAMPTZ '2024-01-01 12:00:00.123456+00'",
                        Pair("UUID 'ffffffff-0000-0000-0000-000000000001'", "TIME '12:00:00.5'")),
                Triple("TIMESTAMP '1969-07-20 20:17:40 UTC'", "TIMESTAMPTZ '1969-07-20 20:17:40+00'",
                        Pair("UUID '00000000-0000-0000-0000-000000000001'", "TIME '00:00:00'")),
                Triple("TIMESTAMP '2024-03-10 01:30:00.5 America/Los_Angeles'", "TIMESTAMPTZ '2024-03-10 09:30:00.5+00'",
                        Pair("UUID '550e8400-e29b-41d4-a716-446655440000'", "TIME '23:59:59.999999'")))
        try {
            computeActual("CREATE TABLE test_schema.xp_trino (id INTEGER, ts TIMESTAMP(6) WITH TIME ZONE, u UUID, t TIME(6)) " +
                    "WITH (partitioned_by = ARRAY['ts', 'bucket(8, u)', 'bucket(8, t)'])")
            duckdb("CREATE TABLE ducklake_db.test_schema.xp_duck (id INTEGER, ts TIMESTAMPTZ, u UUID, t TIME)",
                    "CALL ducklake_db.set_option('data_inlining_row_limit', 0, schema => 'test_schema', table_name => 'xp_duck')",
                    "ALTER TABLE ducklake_db.test_schema.xp_duck SET PARTITIONED BY (ts, bucket(8, u), bucket(8, t))")
            values.forEachIndexed { i, (trinoTs, duckTs, ut) ->
                computeActual("INSERT INTO test_schema.xp_trino VALUES (${i + 1}, $trinoTs, ${ut.first}, ${ut.second})")
                duckdb("INSERT INTO ducklake_db.test_schema.xp_duck VALUES (${i + 1}, $duckTs, ${ut.first}, ${ut.second})")
            }
            val partitionValues = { table: String ->
                catalogRows("SELECT v.partition_key_index, v.partition_value FROM ducklake_file_partition_value v " +
                        "JOIN ducklake_data_file f ON f.data_file_id = v.data_file_id AND f.end_snapshot IS NULL " +
                        "JOIN ducklake_table t ON t.table_id = v.table_id AND t.end_snapshot IS NULL " +
                        "WHERE t.table_name = '$table' ORDER BY f.data_file_id, v.partition_key_index")
            }
            assertThat(partitionValues("xp_trino")).hasSize(9).isEqualTo(partitionValues("xp_duck"))
            assertThat(partitionValues("xp_trino")).contains("0|2024-01-01 12:00:00.123456+00", "0|1969-07-20 20:17:40+00")

            values.forEachIndexed { i, (trinoTs, duckTs, ut) ->
                for (table in listOf("xp_trino", "xp_duck")) {
                    for (predicate in listOf("ts = $duckTs", "u = ${ut.first}", "t = ${ut.second}")) {
                        assertThat(duckdbRows("SELECT id FROM ducklake_db.test_schema.$table WHERE $predicate"))
                                .`as`("DuckDB %s WHERE %s", table, predicate)
                                .containsExactly("${i + 1}")
                    }
                    for (predicate in listOf("ts = $trinoTs", "u = ${ut.first}", "t = ${ut.second}")) {
                        assertThat(computeActual("SELECT id FROM test_schema.$table WHERE $predicate").onlyColumnAsSet)
                                .`as`("Trino %s WHERE %s", table, predicate)
                                .containsExactly(i + 1)
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.xp_trino")
            tryDropTable("test_schema.xp_duck")
        }
    }

    @Test
    fun bucketHashesEqualDuckLakeMurmur3() {
        val uuids = listOf("ffffffff-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000000", "550e8400-e29b-41d4-a716-446655440000")
        val times = listOf("00:00:00", "12:00:00.5", "23:59:59.999999")
        val instants = listOf("2024-01-01T12:00:00.123456Z", "1969-12-31T23:59:59.999999Z", "1900-01-01T00:00:00Z")
        val checks = uuids.map { Triple("UUID '$it'", UUID as Type, javaUuidToTrinoUuid(java.util.UUID.fromString(it)) as Any) } +
                times.map { Triple("TIME '$it'", TIME_MICROS, LocalTime.parse(it).toNanoOfDay() * 1_000L) } +
                instants.map { instant ->
                    val value = OffsetDateTime.parse(instant)
                    Triple("TIMESTAMPTZ '${value.toLocalDateTime()}+00'", TIMESTAMP_TZ_MICROS,
                            DucklakeTimestampValues.timestampWithTimeZone(value, TIMESTAMP_TZ_MICROS))
                }
        for ((literal, type, value) in checks) {
            val ducklake = duckdbRows("SELECT murmur3_32($literal) & 2147483647").single().toInt()
            val builder = type.createBlockBuilder(null, 1)
            when (value) {
                is Long -> type.writeLong(builder, value)
                is io.airlift.slice.Slice -> type.writeSlice(builder, value)
                else -> type.writeObject(builder, value)
            }
            assertThat(DucklakePartitionComputer.computeBucket(type, builder.build(), 0, Int.MAX_VALUE))
                    .`as`("bucket hash of %s", literal)
                    .isEqualTo(ducklake % Int.MAX_VALUE)
        }
    }

    @Test
    fun updateDeleteAndMergeRewriteTheseColumns() {
        val table = "test_schema.xw_dml"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, t TIME(6), ts TIMESTAMP(6) WITH TIME ZONE, u UUID, n ROW(a TIME(6), b UUID))")
            computeActual("INSERT INTO $table VALUES " +
                    "(1, TIME '01:00:00', TIMESTAMP '2024-01-01 00:00:00.000001 UTC', UUID '00000000-0000-0000-0000-000000000001', " +
                    "CAST(ROW(TIME '01:00:00', UUID '00000000-0000-0000-0000-00000000000a') AS ROW(a TIME(6), b UUID))), " +
                    "(2, TIME '02:00:00', TIMESTAMP '2024-01-02 00:00:00 UTC', UUID '00000000-0000-0000-0000-000000000002', NULL)")
            computeActual("INSERT INTO $table VALUES (3, TIME '03:00:00', TIMESTAMP '2024-01-03 00:00:00 UTC', " +
                    "UUID '00000000-0000-0000-0000-000000000003', NULL)")

            computeActual("UPDATE $table SET t = TIME '11:11:11.111111', ts = ts + INTERVAL '1' HOUR, " +
                    "u = UUID 'ffffffff-ffff-ffff-ffff-fffffffffff1' WHERE id = 1")
            computeActual("DELETE FROM $table WHERE u = UUID '00000000-0000-0000-0000-000000000002'")
            computeActual("MERGE INTO $table d USING (VALUES " +
                    "(3, TIME '13:00:00.5', UUID '33333333-3333-3333-3333-333333333333'), " +
                    "(4, TIME '14:00:00', UUID '44444444-4444-4444-4444-444444444444')) s(id, t, u) ON d.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET t = s.t, n = CAST(ROW(s.t, s.u) AS ROW(a TIME(6), b UUID)) " +
                    "WHEN NOT MATCHED THEN INSERT (id, t, ts, u) VALUES (s.id, s.t, TIMESTAMP '2024-01-04 23:59:59.999999 Asia/Tokyo', s.u)")

            val expected = listOf(
                    "1|11:11:11.111111|2024-01-01 01:00:00.000001+00|ffffffff-ffff-ffff-ffff-fffffffffff1|" +
                            "{'a': '01:00:00', 'b': 00000000-0000-0000-0000-00000000000a}",
                    "3|13:00:00.5|2024-01-03 00:00:00+00|00000000-0000-0000-0000-000000000003|" +
                            "{'a': '13:00:00.5', 'b': 33333333-3333-3333-3333-333333333333}",
                    "4|14:00:00|2024-01-04 14:59:59.999999+00|44444444-4444-4444-4444-444444444444|null")
            assertThat(duckdbRows("SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ducklake_db.$table ORDER BY id")).isEqualTo(expected)
            assertThat(computeActual("SELECT id, CAST(t AS VARCHAR), CAST(ts AS VARCHAR), CAST(u AS VARCHAR) FROM $table ORDER BY id")
                    .materializedRows.map { it.fields.joinToString("|") })
                    .containsExactly(
                            "1|11:11:11.111111|2024-01-01 01:00:00.000001 UTC|ffffffff-ffff-ffff-ffff-fffffffffff1",
                            "3|13:00:00.500000|2024-01-03 00:00:00.000000 UTC|00000000-0000-0000-0000-000000000003",
                            "4|14:00:00.000000|2024-01-04 14:59:59.999999 UTC|44444444-4444-4444-4444-444444444444")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun shorterTimestampWithTimeZoneCtasIsStoredAsMicros() {
        val table = "test_schema.xw_ctas"
        try {
            computeActual("CREATE TABLE $table AS SELECT TIMESTAMP '2024-01-01 12:00:00.123 Europe/Berlin' AS ts3, " +
                    "TIMESTAMP '1969-12-31 23:59:59 UTC' AS ts0, " +
                    "CAST(ROW(TIMESTAMP '2024-01-01 12:00:00.123 UTC') AS ROW(x TIMESTAMP(3) WITH TIME ZONE)) AS r, " +
                    "ARRAY[TIMESTAMP '2024-01-01 12:00:00.1 UTC'] AS a")
            assertThat(computeActual("SELECT typeof(ts3), typeof(ts0), typeof(r), typeof(a) FROM $table").materializedRows.single().fields)
                    .containsExactly("timestamp(6) with time zone", "timestamp(6) with time zone",
                            "row(\"x\" timestamp(6) with time zone)", "array(timestamp(6) with time zone)")
            assertThat(duckdbRows("SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ducklake_db.$table")).containsExactly(
                    "2024-01-01 11:00:00.123+00|1969-12-31 23:59:59+00|{'x': '2024-01-01 12:00:00.123+00'}|['2024-01-01 12:00:00.1+00']")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun typesWithoutAnExactEncodingAreRefusedBeforeWriting() {
        try {
            computeActual("CREATE TABLE test_schema.xw_timetz (c TIME(6) WITH TIME ZONE)")
            assertThatThrownBy { computeActual("INSERT INTO test_schema.xw_timetz VALUES (TIME '12:00:00.000000 +02:00')") }
                    .hasMessageContaining("Parquet has no time-with-time-zone logical type")
            computeActual("CREATE TABLE test_schema.xw_ts0 (c TIMESTAMP(0))")
            assertThatThrownBy { computeActual("INSERT INTO test_schema.xw_ts0 VALUES (TIMESTAMP '2024-01-01 00:00:01')") }
                    .hasMessageContaining("trino-parquet has no value writer for timestamp(0)")
            assertThat(computeScalar("SELECT count(*) FROM test_schema.\"xw_timetz\$files\"")).isEqualTo(0L)
            assertThat(computeScalar("SELECT count(*) FROM test_schema.\"xw_ts0\$files\"")).isEqualTo(0L)

            for (select in listOf(
                    "SELECT TIMESTAMP '2024-01-01 00:00:00.123456789 UTC' AS c",
                    "SELECT CAST(ROW(TIMESTAMP '2024-01-01 00:00:00.123456789 UTC') AS ROW(x TIMESTAMP(9) WITH TIME ZONE)) AS c")) {
                assertThatThrownBy { computeActual("CREATE TABLE test_schema.xw_ts9 AS $select") }
                        .hasMessageContaining("DuckLake stores this type with microsecond precision")
                assertThat(computeActual("SHOW TABLES FROM test_schema LIKE 'xw_ts9'").rowCount).isZero()
            }
            for ((name, type) in listOf("u" to "UUID", "t" to "TIME(6)")) {
                assertThatThrownBy {
                    computeActual("CREATE TABLE test_schema.xw_part_bad ($name $type) WITH (partitioned_by = ARRAY['$name'])")
                }.hasMessageContaining("Identity partitioning is not supported")
            }
        }
        finally {
            tryDropTable("test_schema.xw_timetz")
            tryDropTable("test_schema.xw_ts0")
            tryDropTable("test_schema.xw_ts9")
            tryDropTable("test_schema.xw_part_bad")
        }
    }

    @Test
    fun flushAndRewriteWriteTheseColumns() {
        try {
            // Inlined TIME values cannot be read yet (RV-18i), so the flushed table has no TIME column.
            duckdb("CREATE TABLE ducklake_db.test_schema.xw_flush " +
                            "(id INTEGER, ts TIMESTAMPTZ, u UUID, n STRUCT(b UUID, c TIMESTAMPTZ), l UUID[], m MAP(UUID, TIMESTAMPTZ))",
                    "CALL ducklake_db.set_option('data_inlining_row_limit', 1000, schema => 'test_schema', table_name => 'xw_flush')",
                    "INSERT INTO ducklake_db.test_schema.xw_flush VALUES " +
                            "(1, TIMESTAMPTZ '1969-12-31 23:59:59.999999+00', UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff', " +
                            "{'b': UUID '00000000-0000-0000-0000-000000000001', 'c': TIMESTAMPTZ '2024-02-29 12:00:00.5+00'}, " +
                            "[UUID '7fffffff-ffff-ffff-ffff-ffffffffffff', NULL], " +
                            "MAP {UUID '550e8400-e29b-41d4-a716-446655440000': TIMESTAMPTZ '1900-01-01 00:00:00.000001+00'}), " +
                            "(2, NULL, NULL, NULL, NULL, NULL)")
            assertRowsStayedInlined("xw_flush", 2L)
            val before = duckdbRows("SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ducklake_db.test_schema.xw_flush ORDER BY id")
            computeActual("CALL ducklake.system.flush_inlined_data(schema_name => 'test_schema', table_name => 'xw_flush')")
            assertRowsWrittenToParquet("xw_flush")
            assertThat(duckdbRows("SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ducklake_db.test_schema.xw_flush ORDER BY id")).isEqualTo(before)

            computeActual("CREATE TABLE test_schema.xw_rewrite ($trinoColumns)")
            for (row in trinoRows) {
                computeActual("INSERT INTO test_schema.xw_rewrite VALUES $row")
            }
            val rewriteBefore = duckdbRows("SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ducklake_db.test_schema.xw_rewrite ORDER BY id")
            assertThat(rewriteBefore).hasSize(4)
            computeActual("CALL ducklake.system.rewrite_data_files(schema_name => 'test_schema', table_name => 'xw_rewrite')")
            assertThat(computeScalar("SELECT count(*) FROM test_schema.\"xw_rewrite\$files\"")).isEqualTo(1L)
            assertThat(duckdbRows("SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ducklake_db.test_schema.xw_rewrite ORDER BY id"))
                    .isEqualTo(rewriteBefore)
        }
        finally {
            tryDropTable("test_schema.xw_flush")
            tryDropTable("test_schema.xw_rewrite")
        }
    }

    private fun utc(instant: String) = java.time.ZonedDateTime.parse(instant).withZoneSameInstant(java.time.ZoneId.of("UTC"))

    private fun duckdb(vararg statements: String) {
        createDuckdbConnection().use { connection ->
            connection.createStatement().use { statement ->
                statements.forEach(statement::execute)
            }
        }
    }

    /** Rows as `|`-joined cell text, in a UTC DuckDB session. */
    private fun duckdbRows(sql: String): List<String> =
            createDuckdbConnection().use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("SET TimeZone = 'UTC'")
                    statement.executeQuery(sql).use { rs ->
                        buildList {
                            while (rs.next()) {
                                add((1..rs.metaData.columnCount).joinToString("|") { rs.getString(it) ?: "null" })
                            }
                        }
                    }
                }
            }

    private fun catalogRows(sql: String): List<String> {
        val catalog = getIsolatedCatalog()
        return DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { rs ->
                    buildList {
                        while (rs.next()) {
                            add((1..rs.metaData.columnCount).joinToString("|") { rs.getString(it) ?: "null" })
                        }
                    }
                }
            }
        }
    }

    private fun dataFileList(table: String): String =
            duckdbRows("SELECT data_file FROM ducklake_list_files('ducklake_db', '$table', schema => 'test_schema')")
                    .also { assertThat(it).`as`("data files of %s", table).hasSize(1) }
                    .joinToString(", ", "[", "]") { "'${it.replace("'", "''")}'" }
}
