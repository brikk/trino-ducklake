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

import java.sql.Statement

/** The tables and views of the shared test catalog, created through DuckDB by [DucklakeCatalogGenerator]. */
internal object DucklakeTestCatalogTables {
    /** simple_table and array_table: primitives, with and without an array column. */
    fun createPrimitiveTables(stmt: Statement) {
        println("Creating simple_table (primitives only)...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.simple_table (
                id INTEGER,
                name VARCHAR,
                price DOUBLE,
                active BOOLEAN,
                created_date DATE
            )
            """.trimIndent(),
        )

        println("Inserting data into simple_table...")
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.simple_table VALUES
                (1, 'Product A', 19.99, true, '2024-01-15'),
                (2, 'Product B', 29.99, true, '2024-02-20'),
                (3, 'Product C', 39.99, false, '2024-03-10'),
                (4, 'Product D', 49.99, true, '2024-01-05'),
                (5, 'Product E', 59.99, false, '2024-02-28')
            """.trimIndent(),
        )

        println("Creating array_table (primitives + array)...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.array_table (
                id INTEGER,
                product_name VARCHAR,
                tags VARCHAR[],
                quantity INTEGER
            )
            """.trimIndent(),
        )

        println("Inserting data into array_table...")
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.array_table VALUES
                (1, 'Widget', ['electronics', 'gadgets', 'sale'], 100),
                (2, 'Gizmo', ['tools', 'hardware'], 50),
                (3, 'Doohickey', ['accessories', 'premium', 'new'], 25),
                (4, 'Thingamajig', ['clearance'], 200),
                (5, 'Whatchamacallit', ['featured', 'bestseller', 'trending'], 75)
            """.trimIndent(),
        )
    }

    /** partitioned_table: identity partition by region. */
    fun createIdentityPartitionedTable(stmt: Statement) {
        println("Creating partitioned_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.partitioned_table (
                id INTEGER,
                name VARCHAR,
                region VARCHAR,
                amount DOUBLE
            )
            """.trimIndent(),
        )

        println("Setting partition by region...")
        stmt.execute("ALTER TABLE ducklake_db.test_schema.partitioned_table SET PARTITIONED BY (region)")

        println("Inserting partitioned data...")
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                (1, 'Alice', 'US', 100.0),
                (2, 'Bob', 'US', 200.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                (3, 'Charlie', 'EU', 150.0),
                (4, 'Diana', 'EU', 250.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                (5, 'Emi', 'APAC', 300.0)
            """.trimIndent(),
        )
    }

    /** temporal_partitioned_table: year/month partitions of a DATE. */
    fun createTemporalPartitionedTable(stmt: Statement) {
        println("Creating temporal_partitioned_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.temporal_partitioned_table (
                id INTEGER,
                event_name VARCHAR,
                event_date DATE,
                amount DOUBLE
            )
            """.trimIndent(),
        )

        println("Setting temporal partition by year(event_date), month(event_date)...")
        stmt.execute(
            "ALTER TABLE ducklake_db.test_schema.temporal_partitioned_table SET PARTITIONED BY (year(event_date), month(event_date))",
        )

        println("Inserting temporally partitioned data...")
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.temporal_partitioned_table VALUES
                (1, 'Jan Event', '2023-01-15', 100.0),
                (2, 'Jan Meeting', '2023-01-20', 150.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.temporal_partitioned_table VALUES
                (3, 'Jun Event', '2023-06-10', 200.0),
                (4, 'Jun Meeting', '2023-06-25', 250.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.temporal_partitioned_table VALUES
                (5, 'Next Year', '2024-03-05', 300.0),
                (6, 'Next Year Too', '2024-03-20', 350.0)
            """.trimIndent(),
        )
    }

    /** daily_partitioned_table: year/month/day partitions of a DATE. */
    fun createDailyPartitionedTable(stmt: Statement) {
        println("Creating daily_partitioned_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.daily_partitioned_table (
                id INTEGER,
                event_name VARCHAR,
                event_date DATE,
                amount DOUBLE
            )
            """.trimIndent(),
        )

        println("Setting partition by year/month/day...")
        stmt.execute(
            "ALTER TABLE ducklake_db.test_schema.daily_partitioned_table SET PARTITIONED BY (year(event_date), month(event_date), day(event_date))",
        )

        println("Inserting daily partitioned data...")
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                (1, 'Morning standup', '2023-06-15', 10.0),
                (2, 'Afternoon review', '2023-06-15', 20.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                (3, 'Sprint planning', '2023-06-20', 30.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                (4, 'July kickoff', '2023-07-01', 40.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                (5, 'New year event', '2024-01-10', 50.0)
            """.trimIndent(),
        )
    }

    /** timestamp_partitioned_table: year/month/day partitions of a TIMESTAMPTZ. */
    fun createTimestampPartitionedTable(stmt: Statement) {
        println("Creating timestamp_partitioned_table (TIMESTAMPTZ partitioned by day)...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.timestamp_partitioned_table (
                id INTEGER,
                event_name VARCHAR,
                inserted_at TIMESTAMPTZ,
                amount DOUBLE
            )
            """.trimIndent(),
        )

        println("Setting partition by year/month/day on inserted_at...")
        stmt.execute(
            "ALTER TABLE ducklake_db.test_schema.timestamp_partitioned_table SET PARTITIONED BY (year(inserted_at), month(inserted_at), day(inserted_at))",
        )

        println("Inserting timestamp partitioned data...")
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.timestamp_partitioned_table VALUES
                (1, 'Morning event', '2026-03-07 08:00:00+00', 100.0),
                (2, 'Afternoon event', '2026-03-07 14:30:00+00', 150.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.timestamp_partitioned_table VALUES
                (3, 'Next day event', '2026-03-08 09:00:00+00', 200.0),
                (4, 'Next day late', '2026-03-08 22:00:00+00', 250.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.timestamp_partitioned_table VALUES
                (5, 'Summer event', '2026-06-15 10:00:00+00', 300.0)
            """.trimIndent(),
        )
    }

    /** nested_table: struct, map and nested arrays. */
    fun createNestedTable(stmt: Statement) {
        println("Creating nested_table (struct, map, nested arrays)...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.nested_table (
                id INTEGER,
                metadata STRUCT(key VARCHAR, value VARCHAR),
                tags MAP(VARCHAR, INTEGER),
                nested_list INTEGER[][],
                complex_struct STRUCT(name VARCHAR, scores INTEGER[], attrs MAP(VARCHAR, VARCHAR))
            )
            """.trimIndent(),
        )

        println("Inserting data into nested_table...")
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.nested_table VALUES
                (1,
                 {'key': 'color', 'value': 'red'},
                 MAP {'priority': 1, 'severity': 3},
                 [[1, 2], [3, 4]],
                 {'name': 'Alice', 'scores': [90, 85, 92], 'attrs': MAP {'dept': 'eng', 'level': 'senior'}}
                ),
                (2,
                 {'key': 'size', 'value': 'large'},
                 MAP {'priority': 2, 'severity': 1},
                 [[10, 20, 30], [40]],
                 {'name': 'Bob', 'scores': [75, 88], 'attrs': MAP {'dept': 'sales', 'level': 'junior'}}
                ),
                (3,
                 {'key': 'shape', 'value': 'round'},
                 MAP {'priority': 3},
                 [[100]],
                 {'name': 'Carol', 'scores': [95, 97, 99, 100], 'attrs': MAP {'dept': 'eng', 'level': 'lead'}}
                )
            """.trimIndent(),
        )
    }

    /** wide_types_table. */
    fun createWideTypesTable(stmt: Statement) {
        println("Creating wide_types_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.wide_types_table (
                col_tinyint TINYINT,
                col_smallint SMALLINT,
                col_integer INTEGER,
                col_bigint BIGINT,
                col_float FLOAT,
                col_double DOUBLE,
                col_decimal DECIMAL(10, 2),
                col_boolean BOOLEAN,
                col_varchar VARCHAR,
                col_date DATE,
                col_timestamp TIMESTAMP,
                col_blob BLOB
            )
            """.trimIndent(),
        )

        println("Inserting data into wide_types_table...")
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.wide_types_table VALUES
                (1, 100, 10000, 1000000000, 1.5, 2.5, 123.45, true, 'hello', '2024-01-01', '2024-01-01 12:00:00', '\x48454C4C4F'),
                (2, 200, 20000, 2000000000, 2.5, 3.5, 678.90, false, 'world', '2024-06-15', '2024-06-15 18:30:00', '\x574F524C44'),
                (-1, -100, -10000, -1000000000, -1.5, -2.5, -123.45, true, '', '1970-01-01', '1970-01-01 00:00:00', '\x00')
            """.trimIndent(),
        )
    }

    /** nullable_table (with NULLs) and empty_table. */
    fun createNullableAndEmptyTables(stmt: Statement) {
        println("Creating nullable_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.nullable_table (
                id INTEGER,
                name VARCHAR,
                price DOUBLE,
                active BOOLEAN,
                created_date DATE,
                tags VARCHAR[],
                metadata STRUCT(key VARCHAR, value VARCHAR)
            )
            """.trimIndent(),
        )

        println("Inserting data into nullable_table (with NULLs)...")
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.nullable_table VALUES
                (1, 'Present', 10.0, true, '2024-01-01', ['a', 'b'], {'key': 'k1', 'value': 'v1'}),
                (2, NULL, NULL, NULL, NULL, NULL, NULL),
                (NULL, 'NoId', 20.0, false, '2024-06-01', ['c'], {'key': 'k2', 'value': 'v2'}),
                (4, NULL, NULL, true, NULL, NULL, {'key': 'k3', 'value': NULL})
            """.trimIndent(),
        )

        println("Creating empty_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.empty_table (
                id INTEGER,
                name VARCHAR,
                value DOUBLE
            )
            """.trimIndent(),
        )
    }

    /** schema_evolution_table: a column added between two checkpointed inserts. */
    fun createSchemaEvolutionTable(stmt: Statement) {
        println("Creating schema_evolution_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.schema_evolution_table (
                id INTEGER,
                original_col VARCHAR
            )
            """.trimIndent(),
        )

        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.schema_evolution_table VALUES
                (1, 'row1'),
                (2, 'row2')
            """.trimIndent(),
        )

        stmt.execute("CHECKPOINT ducklake_db")

        println("Adding column to schema_evolution_table...")
        stmt.execute("ALTER TABLE ducklake_db.test_schema.schema_evolution_table ADD COLUMN added_col INTEGER")

        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.schema_evolution_table VALUES
                (3, 'row3', 300),
                (4, 'row4', 400)
            """.trimIndent(),
        )
    }

    /** aggregation_table: 30 rows over three categories. */
    fun createAggregationTable(stmt: Statement) {
        println("Creating aggregation_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.aggregation_table (
                id INTEGER,
                category VARCHAR,
                amount DOUBLE,
                quantity INTEGER
            )
            """.trimIndent(),
        )

        val insertSql = StringBuilder(
            "INSERT INTO ducklake_db.test_schema.aggregation_table VALUES ",
        )
        val categories = arrayOf("A", "B", "C")
        for (i in 1..30) {
            if (i > 1) {
                insertSql.append(", ")
            }
            val category = categories[(i - 1) % 3]
            val amount = 10.0 * i + (i % 7)
            val quantity = i * 5
            insertSql.append(String.format("(%d, '%s', %.1f, %d)", i, category, amount, quantity))
        }
        stmt.execute(insertSql.toString())
    }

    /** deleted_rows_table: flushed to Parquet, then half its rows deleted. */
    fun createDeletedRowsTable(stmt: Statement) {
        println("Creating deleted_rows_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.deleted_rows_table (
                id INTEGER,
                name VARCHAR,
                value DOUBLE
            )
            """.trimIndent(),
        )

        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.deleted_rows_table VALUES
                (1, 'keep', 10.0),
                (2, 'delete', 20.0),
                (3, 'keep', 30.0),
                (4, 'delete', 40.0),
                (5, 'keep', 50.0),
                (6, 'delete', 60.0)
            """.trimIndent(),
        )

        stmt.execute("CALL ducklake_flush_inlined_data('ducklake_db')")
        println("Deleting rows from deleted_rows_table...")
        stmt.execute("DELETE FROM ducklake_db.test_schema.deleted_rows_table WHERE name = 'delete'")
    }

    /** complex_nulls_table: NULL structs, arrays and elements. */
    fun createComplexNullsTable(stmt: Statement) {
        println("Creating complex_nulls_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.complex_nulls_table (
                id INTEGER,
                pair STRUCT(a INTEGER, b INTEGER),
                items INTEGER[]
            )
            """.trimIndent(),
        )

        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.complex_nulls_table VALUES
                (1, {'a': 10, 'b': 20}, [1, 2, 3]),
                (2, NULL, [NULL]),
                (3, {'a': NULL, 'b': 40}, NULL),
                (4, NULL, [4, 5]),
                (5, {'a': 50, 'b': 60}, [])
            """.trimIndent(),
        )
    }

    /** multi_file_table: three inserts with inlining off, one Parquet file each. */
    fun createMultiFileTable(stmt: Statement) {
        println("Creating multi_file_table...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.multi_file_table (
                id INTEGER,
                value VARCHAR
            )
            """.trimIndent(),
        )

        stmt.execute(
            "CALL ducklake_db.set_option('data_inlining_row_limit', 0, schema => 'test_schema', table_name => 'multi_file_table')",
        )

        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.multi_file_table VALUES
                (1, 'file1_row1'),
                (2, 'file1_row2')
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.multi_file_table VALUES
                (3, 'file2_row1'),
                (NULL, NULL)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.multi_file_table VALUES
                (5, 'file3_row1')
            """.trimIndent(),
        )
    }

    /** inlined_table and inlined_nullable_table: data stays inlined in the metadata catalog. */
    fun createInlinedTables(stmt: Statement) {
        println("Creating inlined_table (data stays inlined in metadata catalog)...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.inlined_table (
                id INTEGER,
                name VARCHAR,
                value DOUBLE
            )
            """.trimIndent(),
        )

        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.inlined_table VALUES
                (1, 'alpha', 10.5),
                (2, 'beta', 20.0),
                (3, 'gamma', 30.75)
            """.trimIndent(),
        )

        println("Creating inlined_nullable_table (inlined data with NULLs)...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.inlined_nullable_table (
                id INTEGER,
                name VARCHAR,
                value DOUBLE
            )
            """.trimIndent(),
        )

        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.inlined_nullable_table VALUES
                (1, 'present', 10.0),
                (NULL, NULL, NULL),
                (3, NULL, 30.0)
            """.trimIndent(),
        )
    }

    /** mixed_inline_table: inlined and Parquet rows in the same snapshot. */
    fun createMixedInlineTable(stmt: Statement) {
        println("Creating mixed_inline_table (inlined + Parquet in same snapshot)...")
        stmt.execute(
            """
            CREATE TABLE ducklake_db.test_schema.mixed_inline_table (
                id INTEGER,
                source VARCHAR,
                value DOUBLE
            )
            """.trimIndent(),
        )
        stmt.execute(
            "CALL ducklake_db.set_option('data_inlining_row_limit', 3, schema => 'test_schema', table_name => 'mixed_inline_table')",
        )

        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.mixed_inline_table VALUES
                (1, 'inlined', 11.0),
                (2, 'inlined', 22.0)
            """.trimIndent(),
        )
        stmt.execute(
            """
            INSERT INTO ducklake_db.test_schema.mixed_inline_table VALUES
                (100, 'parquet', 100.0),
                (101, 'parquet', 101.0),
                (102, 'parquet', 102.0),
                (103, 'parquet', 103.0),
                (104, 'parquet', 104.0)
            """.trimIndent(),
        )
    }

    /** simple_view, aliased_view and duckdb_specific_view. */
    fun createViews(stmt: Statement) {
        println("Creating simple_view (compatible DuckDB view)...")
        stmt.execute(
            """
            CREATE VIEW ducklake_db.test_schema.simple_view AS
                SELECT id, name, price FROM ducklake_db.test_schema.simple_table WHERE active = true
            """.trimIndent(),
        )

        println("Creating aliased_view (column aliases)...")
        stmt.execute(
            """
            CREATE VIEW ducklake_db.test_schema.aliased_view (product_id, product_name, product_price) AS
                SELECT id, name, price FROM ducklake_db.test_schema.simple_table
            """.trimIndent(),
        )

        println("Creating duckdb_specific_view (DuckDB-only syntax)...")
        stmt.execute(
            """
            CREATE VIEW ducklake_db.test_schema.duckdb_specific_view AS
                SELECT id, product_name, list_sort(tags) as sorted_tags FROM ducklake_db.test_schema.array_table
            """.trimIndent(),
        )
    }
}
