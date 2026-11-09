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
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.filesystem.cache.NoopSplitAffinityProvider
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.Constraint
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.Range
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.predicate.ValueSet
import io.trino.spi.type.DateType
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.LocalDate

/** Temporal (year/month/day) partitions of DATE and TIMESTAMPTZ columns: classification and pruning. */
class TestDucklakeTemporalPartitionPruning {

    companion object {
        private lateinit var catalog: DucklakeCatalog
        private lateinit var config: DucklakeConfig
        private var snapshotId: Long = 0
        private lateinit var temporalPartitionedTable: DucklakeTable
        private lateinit var dailyPartitionedTable: DucklakeTable
        private lateinit var timestampPartitionedTable: DucklakeTable

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            config = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            catalog = JdbcDucklakeCatalog(config.toCatalogConfig())
            snapshotId = catalog.currentSnapshotId
            temporalPartitionedTable = testSchemaTable(catalog, snapshotId, "temporal_partitioned_table")
            dailyPartitionedTable = testSchemaTable(catalog, snapshotId, "daily_partitioned_table")
            timestampPartitionedTable = testSchemaTable(catalog, snapshotId, "timestamp_partitioned_table")
        }

        private fun getSplits(splitManager: DucklakeSplitManager, tableHandle: DucklakeTableHandle): List<DucklakeSplit> =
            allSplits(splitManager, tableHandle)
    }
    @Test
    fun testTemporalPartitionSpecHasYearAndMonthTransforms() {
        val specs = catalog.getPartitionSpecs(temporalPartitionedTable.tableId, snapshotId)

        assertThat(specs).hasSize(1)
        val spec = specs.first()
        assertThat(spec.fields).hasSize(2)
        assertThat(spec.fields[0].transform).isEqualTo(DucklakePartitionTransform.YEAR)
        assertThat(spec.fields[1].transform).isEqualTo(DucklakePartitionTransform.MONTH)
    }

    @Test
    fun testTemporalPartitionFileValuesPresent() {
        val values = catalog.getFilePartitionValues(temporalPartitionedTable.tableId, snapshotId)

        assertThat(values).hasSize(3)
        for (fileValues in values.values) {
            assertThat(fileValues).hasSize(2)
        }
    }

    @Test
    fun testApplyFilterClassifiesTemporalPartitionAsEnforced() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = testingDucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "temporal_partitioned_table",
            temporalPartitionedTable.tableId, snapshotId
        )

        val eventDateColumnId = catalog.getTableColumns(temporalPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val daysSinceEpoch = LocalDate.of(2023, 6, 10).toEpochDay()
        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn as ColumnHandle,
                    Domain.singleValue(DateType.DATE, daysSinceEpoch)
                )
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        assertThat(newHandle.enforcedPredicate.isAll).isFalse()
        assertThat(newHandle.enforcedPredicate.domains.orElseThrow()).containsKey(eventDateColumn)

        // Temporal transform pruning is partial; engine must still verify the original predicate.
        assertThat(newHandle.unenforcedPredicate.isAll).isFalse()
        assertThat(result.get().remainingFilter.isAll).isFalse()
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerPrunesByTemporalPartitionValue() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(temporalPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val jan1 = LocalDate.of(2023, 1, 1).toEpochDay()
        val jan31 = LocalDate.of(2023, 1, 31).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "temporal_partitioned_table",
            temporalPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(DateType.DATE, jan1, true, jan31, true)
                        ),
                        false
                    )
                )
            )
        )

        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "temporal_partitioned_table",
                temporalPartitionedTable.tableId, snapshotId
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        assertThat(allSplits.size).isGreaterThan(prunedSplits.size)
        assertThat(prunedSplits).isNotEmpty
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerEpochStrictPrunesCalendarCatalogTooAggressively() {
        val epochStrictConfig = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            .setTemporalPartitionEncoding("epoch")
            .setTemporalPartitionEncodingReadLeniency(false)
        val splitManager = DucklakeSplitManager(
            catalog, epochStrictConfig, DucklakePathResolver(catalog, epochStrictConfig), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val june15 = LocalDate.of(2023, 6, 15).toEpochDay()
        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.singleValue(DateType.DATE, june15)
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)
        assertThat(prunedSplits).isEmpty()
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerEpochLenientReadsCalendarCatalogSafely() {
        val epochLenientConfig = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            .setTemporalPartitionEncoding("epoch")
            .setTemporalPartitionEncodingReadLeniency(true)
        val splitManager = DucklakeSplitManager(
            catalog, epochLenientConfig, DucklakePathResolver(catalog, epochLenientConfig), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val june15 = LocalDate.of(2023, 6, 15).toEpochDay()
        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.singleValue(DateType.DATE, june15)
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)
        assertThat(prunedSplits).hasSize(1)
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerCalendarStrictAndLenientMatchForCalendarCatalog() {
        val calendarStrictConfig = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            .setTemporalPartitionEncoding("calendar")
            .setTemporalPartitionEncodingReadLeniency(false)
        val calendarLenientConfig = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            .setTemporalPartitionEncoding("calendar")
            .setTemporalPartitionEncodingReadLeniency(true)

        val strictSplitManager = DucklakeSplitManager(
            catalog, calendarStrictConfig, DucklakePathResolver(catalog, calendarStrictConfig), NoopSplitAffinityProvider()
        )
        val lenientSplitManager = DucklakeSplitManager(
            catalog, calendarLenientConfig, DucklakePathResolver(catalog, calendarLenientConfig), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val june15 = LocalDate.of(2023, 6, 15).toEpochDay()
        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.singleValue(DateType.DATE, june15)
                )
            )
        )

        val strictSplits = getSplits(strictSplitManager, tableHandle)
        val lenientSplits = getSplits(lenientSplitManager, tableHandle)

        assertThat(strictSplits).hasSize(1)
        assertThat(lenientSplits).hasSize(1)
    }

    @Test
    @Throws(Exception::class)
    fun testTemporalPruningByYearOnly() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(temporalPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val year2024Start = LocalDate.of(2024, 1, 1).toEpochDay()
        val year2024End = LocalDate.of(2024, 12, 31).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "temporal_partitioned_table",
            temporalPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(DateType.DATE, year2024Start, true, year2024End, true)
                        ),
                        false
                    )
                )
            )
        )

        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "temporal_partitioned_table",
                temporalPartitionedTable.tableId, snapshotId
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        assertThat(allSplits.size).isGreaterThan(prunedSplits.size)
        assertThat(prunedSplits).isNotEmpty
    }

    @Test
    fun testDailyPartitionSpecHasThreeTransforms() {
        val specs = catalog.getPartitionSpecs(dailyPartitionedTable.tableId, snapshotId)

        assertThat(specs).hasSize(1)
        val spec = specs.first()
        assertThat(spec.fields).hasSize(3)
        assertThat(spec.fields[0].transform).isEqualTo(DucklakePartitionTransform.YEAR)
        assertThat(spec.fields[1].transform).isEqualTo(DucklakePartitionTransform.MONTH)
        assertThat(spec.fields[2].transform).isEqualTo(DucklakePartitionTransform.DAY)
    }

    @Test
    fun testDailyPartitionFileValuesPresent() {
        val values = catalog.getFilePartitionValues(dailyPartitionedTable.tableId, snapshotId)

        assertThat(values).hasSize(4)
        for (fileValues in values.values) {
            assertThat(fileValues).hasSize(3)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDailyPrunesToSingleDay() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val june15 = LocalDate.of(2023, 6, 15).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.singleValue(DateType.DATE, june15)
                )
            )
        )

        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "daily_partitioned_table",
                dailyPartitionedTable.tableId, snapshotId
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        assertThat(allSplits).hasSize(4)
        assertThat(prunedSplits).hasSize(1)
    }

    @Test
    @Throws(Exception::class)
    fun testDailyPrunesWithinSameMonth() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val june14 = LocalDate.of(2023, 6, 14).toEpochDay()
        val june16 = LocalDate.of(2023, 6, 16).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(DateType.DATE, june14, true, june16, true)
                        ),
                        false
                    )
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        assertThat(prunedSplits).hasSize(1)
    }

    @Test
    @Throws(Exception::class)
    fun testDailyPrunesByMonthAcrossDays() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val june1 = LocalDate.of(2023, 6, 1).toEpochDay()
        val june30 = LocalDate.of(2023, 6, 30).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(DateType.DATE, june1, true, june30, true)
                        ),
                        false
                    )
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        assertThat(prunedSplits).hasSize(2)
    }

    @Test
    fun testTimestampTzPartitionSpecHasThreeTransforms() {
        val specs = catalog.getPartitionSpecs(timestampPartitionedTable.tableId, snapshotId)

        assertThat(specs).hasSize(1)
        val spec = specs.first()
        assertThat(spec.fields).hasSize(3)
        assertThat(spec.fields[0].transform).isEqualTo(DucklakePartitionTransform.YEAR)
        assertThat(spec.fields[1].transform).isEqualTo(DucklakePartitionTransform.MONTH)
        assertThat(spec.fields[2].transform).isEqualTo(DucklakePartitionTransform.DAY)
    }

    @Test
    fun testTimestampTzFilePartitionValuesPresent() {
        val values = catalog.getFilePartitionValues(timestampPartitionedTable.tableId, snapshotId)

        assertThat(values).hasSize(3)
        for (fileValues in values.values) {
            assertThat(fileValues).hasSize(3) // year + month + day
        }
    }

    @Test
    fun testTimestampTzApplyFilterClassifiesAsEnforced() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = testingDucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "timestamp_partitioned_table",
            timestampPartitionedTable.tableId, snapshotId
        )

        val insertedAtColumnId = catalog.getTableColumns(timestampPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "inserted_at" }
            .findFirst().orElseThrow().columnId
        val insertedAtColumn = DucklakeColumnHandle(
            insertedAtColumnId, "inserted_at", TIMESTAMP_TZ_MICROS, true
        )

        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z) — typical day query with exclusive high
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L, 0, UTC_KEY
        )
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L + 86_400_000L, 0, UTC_KEY
        )

        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    insertedAtColumn as ColumnHandle,
                    Domain.create(
                        ValueSet.ofRanges(
                            Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)
                        ), false
                    )
                )
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        assertThat(newHandle.enforcedPredicate.isAll).isFalse()
        assertThat(newHandle.enforcedPredicate.domains.orElseThrow()).containsKey(insertedAtColumn)
        assertThat(newHandle.unenforcedPredicate.isAll).isFalse()
    }

    @Test
    @Throws(Exception::class)
    fun testTimestampTzExclusiveHighKeepsUnknownWriterZones() {
        // This is the exact real-world pattern: WHERE inserted_at >= TIMESTAMP '...' AND inserted_at < TIMESTAMP '...'
        // The < produces an exclusive high bound, but the writer's partition zone is unknown.
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val insertedAtColumnId = catalog.getTableColumns(timestampPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "inserted_at" }
            .findFirst().orElseThrow().columnId
        val insertedAtColumn = DucklakeColumnHandle(
            insertedAtColumnId, "inserted_at", TIMESTAMP_TZ_MICROS, true
        )

        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z) — exclusive high at midnight boundary
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L, 0, UTC_KEY
        )
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L + 86_400_000L, 0, UTC_KEY
        )

        val tableHandle = DucklakeTableHandle(
            "test_schema", "timestamp_partitioned_table",
            timestampPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    insertedAtColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)
                        ), false
                    )
                )
            )
        )

        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "timestamp_partitioned_table",
                timestampPartitionedTable.tableId, snapshotId
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        assertThat(allSplits).hasSize(3)
        // No per-file writer-zone provenance: UTC boundaries cannot exclude local partitions.
        assertThat(prunedSplits).hasSameSizeAs(allSplits)
    }

    @Test
    @Throws(Exception::class)
    fun testTimestampTzExclusiveHighNotAtBoundaryKeepsUnknownWriterZones() {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T12:00:00Z) — exclusive high at NOON, not a day boundary
        // Day 8 should NOT be pruned because there's data before noon
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val insertedAtColumnId = catalog.getTableColumns(timestampPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "inserted_at" }
            .findFirst().orElseThrow().columnId
        val insertedAtColumn = DucklakeColumnHandle(
            insertedAtColumnId, "inserted_at", TIMESTAMP_TZ_MICROS, true
        )

        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L, 0, UTC_KEY
        )
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L + 86_400_000L + 43_200_000L, 0, UTC_KEY
        ) // noon on March 8

        val tableHandle = DucklakeTableHandle(
            "test_schema", "timestamp_partitioned_table",
            timestampPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    insertedAtColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)
                        ), false
                    )
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        // The residual predicate handles exact exclusion; keep unknown-zone partition candidates.
        assertThat(prunedSplits).hasSize(3)
    }

    @Test
    @Throws(Exception::class)
    fun testTimestampTzInclusiveHighAtBoundaryKeepsUnknownWriterZones() {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z] — INCLUSIVE high at midnight
        // Day 8 should be kept because midnight of March 8 is explicitly included
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val insertedAtColumnId = catalog.getTableColumns(timestampPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "inserted_at" }
            .findFirst().orElseThrow().columnId
        val insertedAtColumn = DucklakeColumnHandle(
            insertedAtColumnId, "inserted_at", TIMESTAMP_TZ_MICROS, true
        )

        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L, 0, UTC_KEY
        )
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L + 86_400_000L, 0, UTC_KEY
        )

        val tableHandle = DucklakeTableHandle(
            "test_schema", "timestamp_partitioned_table",
            timestampPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    insertedAtColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(TIMESTAMP_TZ_MICROS, low, true, high, true)
                        ), false
                    )
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        // The residual predicate handles exact exclusion; keep unknown-zone partition candidates.
        assertThat(prunedSplits).hasSize(3)
    }
}
