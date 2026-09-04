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

/**
 * Identity partition pruning is an optimization, not row-level enforcement. A file written before
 * the table became partitioned has no `ducklake_file_partition_value`; the split pruner correctly
 * keeps it because it cannot decide. If applyFilter drops the residual predicate, every row in that
 * file leaks through. Upstream always re-evaluates the filter on scanned rows.
 */
class TestDucklakeIdentityPartitionResidualFilter : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "identity-partition-residual"

    @Test
    fun rowsInPrePartitionFileAreStillFiltered()
    {
        val table = "test_schema.partition_evolved"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'US'), (2, 'EU')")

            // Change the partition spec through the reference implementation. Existing files keep
            // partition_id NULL; later files carry an identity partition value.
            createDuckdbConnection().use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("ALTER TABLE ducklake_db.$table SET PARTITIONED BY (region)")
                }
            }
            computeActual("INSERT INTO $table VALUES (3, 'US'), (4, 'EU')")

            assertThat(computeActual("SELECT id FROM $table WHERE region = 'US' ORDER BY id")
                    .materializedRows.map { (it.getField(0) as Number).toInt() })
                    .containsExactly(1, 3)
            assertThat(computeActual("SELECT id FROM $table WHERE region = 'EU' ORDER BY id")
                    .materializedRows.map { (it.getField(0) as Number).toInt() })
                    .containsExactly(2, 4)
        }
        finally {
            tryDropTable(table)
        }
    }
}
