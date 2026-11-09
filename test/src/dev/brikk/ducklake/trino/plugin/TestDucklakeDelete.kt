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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

@Execution(ExecutionMode.SAME_THREAD)
open class TestDucklakeDelete : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "delete-integration"
    }

    @Test
    fun testDeleteAllRows() {
        try {
            computeActual("CREATE TABLE test_schema.delete_all (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO test_schema.delete_all VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_all")).isEqualTo(3L)

            computeActual("DELETE FROM test_schema.delete_all")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_all")).isEqualTo(0L)
        }
        finally {
            tryDropTable("test_schema.delete_all")
        }
    }

    @Test
    fun testDeleteWithWhereClause() {
        try {
            computeActual("CREATE TABLE test_schema.delete_where (id INTEGER, name VARCHAR, amount DOUBLE)")
            computeActual("INSERT INTO test_schema.delete_where VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 150.0), (4, 'Diana', 300.0), (5, 'Eve', 50.0)")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_where")).isEqualTo(5L)

            computeActual("DELETE FROM test_schema.delete_where WHERE amount > 150.0")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_where")).isEqualTo(3L)

            val result = computeActual("SELECT id, name FROM test_schema.delete_where ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("Alice")
            assertThat(result.materializedRows[1].getField(1)).isEqualTo("Charlie")
            assertThat(result.materializedRows[2].getField(1)).isEqualTo("Eve")
        }
        finally {
            tryDropTable("test_schema.delete_where")
        }
    }

    @Test
    fun testDeleteSingleRow() {
        try {
            computeActual("CREATE TABLE test_schema.delete_single (id INTEGER, value VARCHAR)")
            computeActual("INSERT INTO test_schema.delete_single VALUES (1, 'keep'), (2, 'remove'), (3, 'keep')")

            computeActual("DELETE FROM test_schema.delete_single WHERE id = 2")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_single")).isEqualTo(2L)

            val result = computeActual("SELECT value FROM test_schema.delete_single ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo("keep")
            assertThat(result.materializedRows[1].getField(0)).isEqualTo("keep")
        }
        finally {
            tryDropTable("test_schema.delete_single")
        }
    }

    @Test
    fun testDeleteThenInsert() {
        try {
            computeActual("CREATE TABLE test_schema.delete_insert (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO test_schema.delete_insert VALUES (1, 'Alice'), (2, 'Bob')")

            computeActual("DELETE FROM test_schema.delete_insert WHERE id = 2")
            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_insert")).isEqualTo(1L)

            computeActual("INSERT INTO test_schema.delete_insert VALUES (3, 'Charlie'), (4, 'Diana')")
            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_insert")).isEqualTo(3L)

            val result = computeActual("SELECT name FROM test_schema.delete_insert ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo("Alice")
            assertThat(result.materializedRows[1].getField(0)).isEqualTo("Charlie")
            assertThat(result.materializedRows[2].getField(0)).isEqualTo("Diana")
        }
        finally {
            tryDropTable("test_schema.delete_insert")
        }
    }

    @Test
    fun testDeleteWithNullValues() {
        try {
            computeActual("CREATE TABLE test_schema.delete_nulls (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO test_schema.delete_nulls VALUES (1, 'Alice'), (2, NULL), (3, 'Charlie'), (4, NULL)")

            computeActual("DELETE FROM test_schema.delete_nulls WHERE name IS NULL")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_nulls")).isEqualTo(2L)

            val result = computeActual("SELECT name FROM test_schema.delete_nulls ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo("Alice")
            assertThat(result.materializedRows[1].getField(0)).isEqualTo("Charlie")
        }
        finally {
            tryDropTable("test_schema.delete_nulls")
        }
    }

    @Test
    fun testDeleteFromMultipleInserts() {
        try {
            computeActual("CREATE TABLE test_schema.delete_multi (id INTEGER, batch VARCHAR)")
            // Insert in separate batches — creates multiple data files
            computeActual("INSERT INTO test_schema.delete_multi VALUES (1, 'batch1'), (2, 'batch1')")
            computeActual("INSERT INTO test_schema.delete_multi VALUES (3, 'batch2'), (4, 'batch2')")
            computeActual("INSERT INTO test_schema.delete_multi VALUES (5, 'batch3'), (6, 'batch3')")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_multi")).isEqualTo(6L)

            computeActual("DELETE FROM test_schema.delete_multi WHERE id IN (2, 4, 6)")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_multi")).isEqualTo(3L)

            val result = computeActual("SELECT id FROM test_schema.delete_multi ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[1].getField(0)).isEqualTo(3)
            assertThat(result.materializedRows[2].getField(0)).isEqualTo(5)
        }
        finally {
            tryDropTable("test_schema.delete_multi")
        }
    }

    @Test
    fun testDeleteNoMatchingRows() {
        try {
            computeActual("CREATE TABLE test_schema.delete_nomatch (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO test_schema.delete_nomatch VALUES (1, 'Alice'), (2, 'Bob')")

            computeActual("DELETE FROM test_schema.delete_nomatch WHERE id > 100")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_nomatch")).isEqualTo(2L)
        }
        finally {
            tryDropTable("test_schema.delete_nomatch")
        }
    }

    @Test
    fun testDeleteThenAggregate() {
        try {
            computeActual("CREATE TABLE test_schema.delete_agg (category VARCHAR, amount DOUBLE)")
            computeActual("INSERT INTO test_schema.delete_agg VALUES " +
                    "('A', 10.0), ('A', 20.0), ('B', 30.0), ('B', 40.0), ('C', 50.0)")

            computeActual("DELETE FROM test_schema.delete_agg WHERE category = 'B'")

            assertThat(computeScalar("SELECT sum(amount) FROM test_schema.delete_agg")).isEqualTo(80.0)

            val result = computeActual(
                    "SELECT category, sum(amount) AS total FROM test_schema.delete_agg GROUP BY category ORDER BY category")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo("A")
            assertThat(result.materializedRows[0].getField(1)).isEqualTo(30.0)
            assertThat(result.materializedRows[1].getField(0)).isEqualTo("C")
            assertThat(result.materializedRows[1].getField(1)).isEqualTo(50.0)
        }
        finally {
            tryDropTable("test_schema.delete_agg")
        }
    }

    @Test
    fun testDeleteSnapshotTracking() {
        try {
            computeActual("CREATE TABLE test_schema.delete_snap (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO test_schema.delete_snap VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

            val snapshotBefore = computeScalar(
                    "SELECT max(snapshot_id) FROM \"delete_snap\$snapshots\"") as Long

            computeActual("DELETE FROM test_schema.delete_snap WHERE id = 2")

            val snapshotAfter = computeScalar(
                    "SELECT max(snapshot_id) FROM \"delete_snap\$snapshots\"") as Long
            assertThat(snapshotAfter).isGreaterThan(snapshotBefore)

            val changes = computeActual(
                    "SELECT changes_made FROM \"delete_snap\$snapshot_changes\" WHERE snapshot_id = " + snapshotAfter)
            assertThat(changes.rowCount).isGreaterThanOrEqualTo(1)
            val hasDeleteChange = changes.materializedRows.stream()
                    .anyMatch { row -> row.getField(0) != null && row.getField(0).toString().startsWith("deleted_from_table:") }
            assertThat(hasDeleteChange).isTrue()
        }
        finally {
            tryDropTable("test_schema.delete_snap")
        }
    }

    @Test
    fun testDeleteWithMultipleTypes() {
        try {
            computeActual("CREATE TABLE test_schema.delete_types (" +
                    "id INTEGER, name VARCHAR, amount DOUBLE, active BOOLEAN, event_date DATE)")
            computeActual("INSERT INTO test_schema.delete_types VALUES " +
                    "(1, 'Alice', 100.0, true, DATE '2024-01-15'), " +
                    "(2, 'Bob', 200.0, false, DATE '2024-02-20'), " +
                    "(3, 'Charlie', 150.0, true, DATE '2024-03-10')")

            computeActual("DELETE FROM test_schema.delete_types WHERE active = false")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_types")).isEqualTo(2L)

            val result = computeActual("SELECT * FROM test_schema.delete_types ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("Alice")
            assertThat(result.materializedRows[0].getField(2)).isEqualTo(100.0)
            assertThat(result.materializedRows[1].getField(0)).isEqualTo(3)
            assertThat(result.materializedRows[1].getField(1)).isEqualTo("Charlie")
        }
        finally {
            tryDropTable("test_schema.delete_types")
        }
    }

    @Test
    fun testMultipleDeletesOnSameTable() {
        try {
            computeActual("CREATE TABLE test_schema.multi_delete (id INTEGER, status VARCHAR)")
            computeActual("INSERT INTO test_schema.multi_delete VALUES " +
                    "(1, 'active'), (2, 'inactive'), (3, 'active'), (4, 'pending'), (5, 'inactive')")

            computeActual("DELETE FROM test_schema.multi_delete WHERE status = 'inactive'")
            assertThat(computeScalar("SELECT count(*) FROM test_schema.multi_delete")).isEqualTo(3L)

            computeActual("DELETE FROM test_schema.multi_delete WHERE status = 'pending'")
            assertThat(computeScalar("SELECT count(*) FROM test_schema.multi_delete")).isEqualTo(2L)

            val result = computeActual("SELECT id FROM test_schema.multi_delete ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[1].getField(0)).isEqualTo(3)
        }
        finally {
            tryDropTable("test_schema.multi_delete")
        }
    }

    @Test
    fun testDeleteFromPartitionedTable() {
        try {
            computeActual("CREATE TABLE test_schema.delete_part (id INTEGER, region VARCHAR, amount DOUBLE) " +
                    "WITH (partitioned_by = ARRAY['region'])")
            computeActual("INSERT INTO test_schema.delete_part VALUES " +
                    "(1, 'US', 100.0), (2, 'EU', 200.0), (3, 'US', 150.0), (4, 'EU', 250.0), (5, 'APAC', 300.0)")

            computeActual("DELETE FROM test_schema.delete_part WHERE region = 'EU'")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_part")).isEqualTo(3L)

            val result = computeActual("SELECT id, region FROM test_schema.delete_part ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("US")
            assertThat(result.materializedRows[1].getField(1)).isEqualTo("US")
            assertThat(result.materializedRows[2].getField(1)).isEqualTo("APAC")
        }
        finally {
            tryDropTable("test_schema.delete_part")
        }
    }

    @Test
    fun testDeleteWithSubquery() {
        try {
            computeActual("CREATE TABLE test_schema.delete_subq (id INTEGER, name VARCHAR, score INTEGER)")
            computeActual("INSERT INTO test_schema.delete_subq VALUES " +
                    "(1, 'Alice', 90), (2, 'Bob', 60), (3, 'Charlie', 85), (4, 'Diana', 45), (5, 'Eve', 75)")

            computeActual("DELETE FROM test_schema.delete_subq WHERE score < (SELECT avg(score) FROM test_schema.delete_subq)")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_subq")).isEqualTo(3L)

            val result = computeActual("SELECT name FROM test_schema.delete_subq ORDER BY name")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo("Alice")
            assertThat(result.materializedRows[1].getField(0)).isEqualTo("Charlie")
            assertThat(result.materializedRows[2].getField(0)).isEqualTo("Eve")
        }
        finally {
            tryDropTable("test_schema.delete_subq")
        }
    }

    @Test
    fun testDeletePreservesOtherTableData() {
        try {
            computeActual("CREATE TABLE test_schema.delete_t1 (id INTEGER, name VARCHAR)")
            computeActual("CREATE TABLE test_schema.delete_t2 (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO test_schema.delete_t1 VALUES (1, 'Alice'), (2, 'Bob')")
            computeActual("INSERT INTO test_schema.delete_t2 VALUES (1, 'Charlie'), (2, 'Diana')")

            computeActual("DELETE FROM test_schema.delete_t1 WHERE id = 1")

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_t1")).isEqualTo(1L)
            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_t2")).isEqualTo(2L)
        }
        finally {
            tryDropTable("test_schema.delete_t1")
            tryDropTable("test_schema.delete_t2")
        }
    }
}
