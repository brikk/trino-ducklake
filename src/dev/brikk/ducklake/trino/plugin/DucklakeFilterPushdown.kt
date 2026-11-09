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
import dev.brikk.ducklake.catalog.DucklakePartitionField
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.ConstraintApplicationResult
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.spi.type.Type
import java.util.Optional

/** `applyFilter`: splits a constraint into partition-enforced and residual (unenforced) domains. */
internal class DucklakeFilterPushdown(private val catalog: DucklakeCatalog)
{
    fun applyFilter(handle: ConnectorTableHandle, constraint: Constraint): Optional<ConstraintApplicationResult<ConnectorTableHandle>>
    {
        if (isFilterExemptHandle(handle)) {
            return Optional.empty()
        }

        val table = handle as DucklakeTableHandle

        val summary: TupleDomain<ColumnHandle> = constraint.summary

        val newEnforced: TupleDomain<DucklakeColumnHandle>
        val newUnenforced: TupleDomain<DucklakeColumnHandle>
        if (summary.isAll) {
            // No TupleDomain pushdown to do — but we still need to check the
            // expression below for function-shape predicates.
            newEnforced = TupleDomain.all()
            newUnenforced = TupleDomain.all()
        }
        else {
            val newPredicate: TupleDomain<DucklakeColumnHandle> = extractDucklakePredicate(summary)

            // Classify predicates as enforced (partition-prunable) or unenforced (best-effort)
            val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(
                    table.tableId, table.snapshotId)

            val enforced: ImmutableMap.Builder<DucklakeColumnHandle, Domain> = ImmutableMap.builder()
            val unenforced: ImmutableMap.Builder<DucklakeColumnHandle, Domain> = ImmutableMap.builder()

            if (!newPredicate.isNone) {
                for (entry in newPredicate.getDomains().orElse(emptyMap()).entries) {
                    when (classifyColumnConstraint(partitionSpecs, entry.key)) {
                        ConstraintEnforcement.FULLY_ENFORCED -> enforced.put(entry.key, entry.value)
                        ConstraintEnforcement.PARTIALLY_ENFORCED -> {
                            // Keep in both predicates: connector can use partition transforms for pruning,
                            // but engine must still evaluate original predicate for correctness.
                            enforced.put(entry.key, entry.value)
                            unenforced.put(entry.key, entry.value)
                        }
                        ConstraintEnforcement.NOT_ENFORCED -> unenforced.put(entry.key, entry.value)
                    }
                }
            }

            newEnforced = if (newPredicate.isNone)
                TupleDomain.none()
            else
                toTupleDomain(enforced.buildOrThrow())
            newUnenforced = if (newPredicate.isNone)
                TupleDomain.all()
            else
                toTupleDomain(unenforced.buildOrThrow())
        }

        val combinedEnforced: TupleDomain<DucklakeColumnHandle> = table.enforcedPredicate.intersect(newEnforced)
        val combinedUnenforced: TupleDomain<DucklakeColumnHandle> = table.unenforcedPredicate.intersect(newUnenforced)

        if (combinedEnforced == table.enforcedPredicate
                && combinedUnenforced == table.unenforcedPredicate) {
            return Optional.empty()
        }

        val newHandle = DucklakeTableHandle(
                table.schemaName,
                table.tableName,
                table.tableId,
                table.snapshotId,
                combinedUnenforced,
                combinedEnforced)

        // Fully enforced predicates are omitted from remaining filter.
        // Partially enforced predicates (e.g. temporal transforms) remain so engine verifies exact semantics.
        val remainingFilter: TupleDomain<ColumnHandle> = newUnenforced.transformKeys(ColumnHandle::class.java::cast)

        return Optional.of(ConstraintApplicationResult(
                newHandle,
                remainingFilter,
                constraint.expression,
                false))
    }

    private enum class ConstraintEnforcement
    {
        FULLY_ENFORCED,
        PARTIALLY_ENFORCED,
        NOT_ENFORCED
    }

    private companion object {
        val TEMPORAL_PARTITION_TYPES: Set<Type> = setOf(DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS, TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MICROS)

        /** Table handles with no TupleDomain pushdown surface (predicates stay in the engine filter). */
        fun isFilterExemptHandle(handle: ConnectorTableHandle): Boolean =
                handle is DucklakeMetadataTableHandle || handle is ChangeFeedTableHandle

        fun classifyColumnConstraint(
                specs: List<DucklakePartitionSpec>,
                column: DucklakeColumnHandle): ConstraintEnforcement
        {
            if (specs.isEmpty()) {
                return ConstraintEnforcement.NOT_ENFORCED
            }

            var fullyEnforced = true
            // A predicate can only be enforced (fully or partially) if it is enforceable in EVERY active spec
            // (spec evolution means different files may have different partition schemes)
            for (spec in specs) {
                val field: DucklakePartitionField = spec.fields.firstOrNull { it.columnId == column.columnId }
                        ?: return ConstraintEnforcement.NOT_ENFORCED

                val fieldEnforcement: ConstraintEnforcement = classifyTransformEnforcement(field, column)
                if (fieldEnforcement == ConstraintEnforcement.NOT_ENFORCED) {
                    return ConstraintEnforcement.NOT_ENFORCED
                }
                if (fieldEnforcement == ConstraintEnforcement.PARTIALLY_ENFORCED) {
                    fullyEnforced = false
                }
            }
            return if (fullyEnforced) ConstraintEnforcement.FULLY_ENFORCED else ConstraintEnforcement.PARTIALLY_ENFORCED
        }

        fun classifyTransformEnforcement(field: DucklakePartitionField, column: DucklakeColumnHandle): ConstraintEnforcement
        {
            if (field.transform.isIdentity()) {
                // Identity values allow exact FILE pruning only when every split carries a
                // parseable value for the active spec. Files written before partitioning, under a
                // retired spec, imported without partition values, and inlined rows do not. Keep
                // the predicate as a residual so those rows are still filtered by Trino.
                return ConstraintEnforcement.PARTIALLY_ENFORCED
            }
            // Temporal transforms support safe partition pruning but do not fully enforce
            // original predicates (e.g. day equality with month transform).
            if (field.transform.isTemporal() && column.columnType in TEMPORAL_PARTITION_TYPES) {
                return ConstraintEnforcement.PARTIALLY_ENFORCED
            }
            return ConstraintEnforcement.NOT_ENFORCED
        }

        fun extractDucklakePredicate(summary: TupleDomain<ColumnHandle>): TupleDomain<DucklakeColumnHandle>
        {
            if (summary.isNone) {
                return TupleDomain.none()
            }

            val domains: Map<ColumnHandle, Domain> = summary.getDomains().orElse(null)
                    ?: return TupleDomain.all()

            val ducklakeDomains: ImmutableMap.Builder<DucklakeColumnHandle, Domain> = ImmutableMap.builder()
            for ((columnHandle, domain) in domains) {
                // Only push down primitive types (arrays/complex types can't be pruned)
                if (columnHandle is DucklakeColumnHandle && columnHandle.columnType.typeParameters.isEmpty()) {
                    ducklakeDomains.put(columnHandle, domain)
                }
            }

            val result: Map<DucklakeColumnHandle, Domain> = ducklakeDomains.buildOrThrow()
            if (result.isEmpty()) {
                return TupleDomain.all()
            }
            return TupleDomain.withColumnDomains(result)
        }

        fun toTupleDomain(domains: Map<DucklakeColumnHandle, Domain>): TupleDomain<DucklakeColumnHandle>
        {
            if (domains.isEmpty()) {
                return TupleDomain.all()
            }
            return TupleDomain.withColumnDomains(domains)
        }
    }
}
