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

import com.google.common.collect.ImmutableMap
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.airlift.slice.Slices
import io.trino.filesystem.cache.NoopSplitAffinityProvider
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.Constraint
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.Range
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.predicate.ValueSet
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.RowType
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.Optional

/** Identity partitions: applyFilter's enforced/unenforced split and partition-value split pruning. */
class TestDucklakePartitionPruning {

    companion object {
        private lateinit var catalog: DucklakeCatalog
        private lateinit var config: DucklakeConfig
        private var snapshotId: Long = 0
        private lateinit var partitionedTable: DucklakeTable

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            config = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            catalog = JdbcDucklakeCatalog(config.toCatalogConfig())
            snapshotId = catalog.currentSnapshotId
            partitionedTable = testSchemaTable(catalog, snapshotId, "partitioned_table")
        }

        private fun getSplits(splitManager: DucklakeSplitManager, tableHandle: DucklakeTableHandle): List<DucklakeSplit> =
            allSplits(splitManager, tableHandle)
    }
    @Test
    fun testApplyFilterClassifiesEnforcedPredicate() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = testingDucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val regionColumnId = catalog.getTableColumns(partitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "region" }
            .findFirst().orElseThrow().columnId
        val regionColumn = DucklakeColumnHandle(regionColumnId, "region", VARCHAR, true)

        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(regionColumn as ColumnHandle, Domain.singleValue(VARCHAR, Slices.utf8Slice("US")))
            )
        )

        val result: Optional<io.trino.spi.connector.ConstraintApplicationResult<ConnectorTableHandle>> =
            metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        // Identity partition values are used for exact file pruning, but the predicate remains a
        // residual for files without a usable value (pre-partition / retired-spec / inlined).
        assertThat(newHandle.enforcedPredicate.isAll).isFalse()
        assertThat(newHandle.enforcedPredicate.domains.orElseThrow()).containsKey(regionColumn)
        assertThat(newHandle.unenforcedPredicate.domains.orElseThrow()).containsKey(regionColumn)

        // Trino must still evaluate region row-by-row when file pruning cannot prove it.
        val remaining: TupleDomain<ColumnHandle> = result.get().remainingFilter
        assertThat(remaining.isAll).isFalse()
        assertThat(remaining.domains.orElseThrow()).containsKey(regionColumn)
    }

    @Test
    fun testApplyFilterNonPartitionColumnIsUnenforced() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = testingDucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val amountColumnId = catalog.getTableColumns(partitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "amount" }
            .findFirst().orElseThrow().columnId
        val amountColumn = DucklakeColumnHandle(amountColumnId, "amount", DOUBLE, true)

        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    amountColumn as ColumnHandle, Domain.create(
                        ValueSet.ofRanges(Range.greaterThan(DOUBLE, 100.0)),
                        false
                    )
                )
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        assertThat(newHandle.enforcedPredicate.isAll).isTrue()
        assertThat(newHandle.unenforcedPredicate.isAll).isFalse()

        assertThat(result.get().remainingFilter.isAll).isFalse()
    }

    @Test
    fun testApplyFilterAllConstraintReturnsEmpty() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = testingDucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val result = metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysTrue())

        assertThat(result).isEmpty
    }

    @Test
    fun testApplyFilterNoneConstraintProducesUnsatisfiableHandle() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = testingDucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val result = metadata.applyFilter(SESSION, tableHandle, Constraint(TupleDomain.none()))

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle
        assertThat(newHandle.enforcedPredicate.isNone).isTrue()
    }

    @Test
    fun testApplyFilterIsIdempotentForSamePredicate() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = testingDucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val regionColumnId = catalog.getTableColumns(partitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "region" }
            .findFirst().orElseThrow().columnId
        val regionColumn = DucklakeColumnHandle(regionColumnId, "region", VARCHAR, true)
        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(regionColumn as ColumnHandle, Domain.singleValue(VARCHAR, Slices.utf8Slice("US")))
            )
        )

        val filteredHandle = metadata.applyFilter(SESSION, tableHandle, constraint)
            .orElseThrow()
            .handle as DucklakeTableHandle

        val secondApply = metadata.applyFilter(SESSION, filteredHandle, constraint)
        assertThat(secondApply).isEmpty
    }

    @Test
    fun testApplyFilterIgnoresComplexTypePredicate() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = testingDucklakeMetadata(catalog, typeConverter)

        val nestedTable = catalog.listSchemas(snapshotId).stream()
            .filter { it.schemaName == "test_schema" }
            .findFirst()
            .flatMap { schema ->
                catalog.listTables(schema.schemaId, snapshotId).stream()
                    .filter { it.tableName == "nested_table" }
                    .findFirst()
            }
            .orElseThrow()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "nested_table", nestedTable.tableId, snapshotId
        )

        val metadataColumnId = catalog.getTableColumns(nestedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "metadata" }
            .findFirst().orElseThrow().columnId
        val metadataType = RowType.from(
            listOf(
                RowType.Field(Optional.of("key"), VARCHAR),
                RowType.Field(Optional.of("value"), VARCHAR)
            )
        )
        val metadataColumn = DucklakeColumnHandle(metadataColumnId, "metadata", metadataType, true)

        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(metadataColumn as ColumnHandle, Domain.onlyNull(metadataType))
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isEmpty
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerPrunesByPartitionValue() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val regionColumnId = catalog.getTableColumns(partitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "region" }
            .findFirst().orElseThrow().columnId
        val regionColumn = DucklakeColumnHandle(regionColumnId, "region", VARCHAR, true)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table",
            partitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    regionColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("EU"))
                )
            )
        )

        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        assertThat(prunedSplits.size).isLessThan(allSplits.size)
        assertThat(prunedSplits).isNotEmpty
    }

    @Test
    fun testNonPartitionedTableAllPredicatesUnenforced() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = testingDucklakeMetadata(catalog, typeConverter)

        val simpleTable = catalog.listSchemas(snapshotId).stream()
            .filter { it.schemaName == "test_schema" }
            .findFirst()
            .flatMap { schema ->
                catalog.listTables(schema.schemaId, snapshotId).stream()
                    .filter { it.tableName == "simple_table" }
                    .findFirst()
            }
            .orElseThrow()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "simple_table", simpleTable.tableId, snapshotId
        )

        val nameColumnId = catalog.getTableColumns(simpleTable.tableId, snapshotId).stream()
            .filter { it.columnName == "name" }
            .findFirst().orElseThrow().columnId
        val nameColumn = DucklakeColumnHandle(nameColumnId, "name", VARCHAR, true)

        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(nameColumn as ColumnHandle, Domain.singleValue(VARCHAR, Slices.utf8Slice("Product A")))
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        assertThat(newHandle.enforcedPredicate.isAll).isTrue()
        assertThat(newHandle.unenforcedPredicate.isAll).isFalse()
    }
}
