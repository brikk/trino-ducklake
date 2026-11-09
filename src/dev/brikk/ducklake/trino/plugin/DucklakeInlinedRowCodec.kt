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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeInlinedChangeRow
import io.trino.spi.type.Type

/**
 * Inlined-row value encoding, which follows the catalog's metadata format: a format-1.1 catalog
 * returns decoded values already projected to the target stable-ID schema, a format-1.0 catalog
 * the JDBC text/primitive encoding of the row's own schema era.
 */
internal sealed class DucklakeInlinedRowCodec {
    abstract fun readRows(split: DucklakeInlinedSplit, columns: List<DucklakeColumn>): List<List<Any?>>

    abstract fun changesBetween(
            tableId: Long,
            schemaVersion: Long,
            startSnapshot: Long,
            endSnapshot: Long,
            columnIds: List<Long>): List<DucklakeInlinedChangeRow>

    abstract fun convert(value: Any?, type: Type): Any?

    /** Converts raw inlined rows of [split] to Trino-native values of [realColumns]. */
    abstract fun convertRows(
            split: DucklakeInlinedSplit,
            realColumns: List<DucklakeColumnHandle>,
            realTypes: List<Type>,
            rawRows: List<List<Any?>>): List<List<Any?>>

    private class Decoded(private val catalog: DucklakeCatalog) : DucklakeInlinedRowCodec() {
        override fun readRows(split: DucklakeInlinedSplit, columns: List<DucklakeColumn>) =
            catalog.readInlinedDataDecoded(split.tableId, split.schemaVersion, split.snapshotId, columns)

        override fun changesBetween(tableId: Long, schemaVersion: Long, startSnapshot: Long, endSnapshot: Long, columnIds: List<Long>) =
            catalog.getInlinedChangesBetweenDecoded(tableId, schemaVersion, startSnapshot, endSnapshot, columnIds)

        override fun convert(value: Any?, type: Type): Any? = DucklakeDecodedValueConverter.convert(value, type)

        // The opt-in catalog tier already validates and projects to the TARGET stable-ID
        // schema, including initial defaults and decoded nested/temporal values. Reapplying
        // the legacy source-era name/default adapter would project the same evolution twice.
        override fun convertRows(
                split: DucklakeInlinedSplit,
                realColumns: List<DucklakeColumnHandle>,
                realTypes: List<Type>,
                rawRows: List<List<Any?>>): List<List<Any?>> =
            rawRows.map { row -> row.mapIndexed { i, value -> convert(value, realTypes[i]) } }
    }

    private class Jdbc(private val catalog: DucklakeCatalog, private val columnTrees: DucklakeColumnTreeCache) : DucklakeInlinedRowCodec() {
        override fun readRows(split: DucklakeInlinedSplit, columns: List<DucklakeColumn>) =
            catalog.readInlinedData(split.tableId, split.schemaVersion, split.snapshotId, columns)

        override fun changesBetween(tableId: Long, schemaVersion: Long, startSnapshot: Long, endSnapshot: Long, columnIds: List<Long>) =
            catalog.getInlinedChangesBetween(tableId, schemaVersion, startSnapshot, endSnapshot, columnIds)

        override fun convert(value: Any?, type: Type): Any? = DucklakeInlinedValueConverter.convertJdbcValue(value, type)

        /**
         * Identity-mapped nested text parsing, and era-default substitution: rows inlined under an
         * era that PREDATES a column must project the column's `initial_default` (upstream issue
         * 1135 semantics, same rule as the parquet missing-column path). readInlinedData returns
         * NULL for era-absent columns.
         */
        override fun convertRows(
                split: DucklakeInlinedSplit,
                realColumns: List<DucklakeColumnHandle>,
                realTypes: List<Type>,
                rawRows: List<List<Any?>>): List<List<Any?>>
        {
            val nestedMappings: Map<Long, InlinedTextFieldMapping> = resolveNestedMappings(split, realColumns)
            val eraDefaults: Map<Int, Any?> = resolveEraDefaults(split, realColumns)
            return rawRows.map { row ->
                row.indices.map { i ->
                    if (eraDefaults.containsKey(i)) eraDefaults[i]
                    else DucklakeInlinedValueConverter.convertJdbcValue(row[i], realTypes[i], nestedMappings[realColumns[i].columnId])
                }
            }
        }

        /**
         * Identity-based nested-field mapping: rows in this split were inlined under schemaVersion's
         * era; struct text carries era field NAMES. Map era names onto current fields by column_id
         * (rename/reuse safety) — see [InlinedNestedFieldMapper]. Empty (name-fallback) when no
         * nested rows are projected or the era cannot be resolved.
         */
        private fun resolveNestedMappings(
                split: DucklakeInlinedSplit,
                realColumns: List<DucklakeColumnHandle>): Map<Long, InlinedTextFieldMapping>
        {
            if (realColumns.none { InlinedNestedFieldMapper.containsRow(it.columnType) }) {
                return emptyMap()
            }
            val eraSnapshot: Long = catalog.resolveSchemaVersionSnapshot(split.tableId, split.schemaVersion, split.snapshotId)
                    ?: return emptyMap()
            return InlinedNestedFieldMapper.build(
                    realColumns.map { it.columnId to it.columnType },
                    columnTrees.get(split.tableId, split.snapshotId),
                    columnTrees.get(split.tableId, eraSnapshot))
        }

        /**
         * For columns ABSENT at the split's schema-version era (added later via ALTER TABLE ADD
         * COLUMN), the projected value for every row: the parsed `initial_default` when the column
         * declares one, else null. Columns present at the era are not in the map.
         */
        private fun resolveEraDefaults(
                split: DucklakeInlinedSplit,
                realColumns: List<DucklakeColumnHandle>): Map<Int, Any?>
        {
            if (realColumns.none { it.initialDefault != null }) {
                return emptyMap()
            }
            val eraSnapshot: Long = catalog.resolveSchemaVersionSnapshot(split.tableId, split.schemaVersion, split.snapshotId)
                    ?: return emptyMap()
            val eraColumnIds: Set<Long> = columnTrees.get(split.tableId, eraSnapshot)
                    .filter { it.parentColumn == null }
                    .mapTo(mutableSetOf()) { it.columnId }
            val defaults = mutableMapOf<Int, Any?>()
            realColumns.forEachIndexed { index, handle ->
                val initialDefault = handle.initialDefault
                if (initialDefault != null && handle.columnId !in eraColumnIds) {
                    defaults[index] = DucklakeReadSupport.parseProjectedValue(handle, initialDefault, "inlined initial_default")
                }
            }
            return defaults
        }
    }

    /** Selects the codec from the catalog's metadata format at each read. */
    class Selector(private val catalog: DucklakeCatalog, columnTrees: DucklakeColumnTreeCache) {
        private val decoded: DucklakeInlinedRowCodec = Decoded(catalog)
        private val jdbc: DucklakeInlinedRowCodec = Jdbc(catalog, columnTrees)

        fun codec(): DucklakeInlinedRowCodec = if (catalog.getSpecVersion() == "1.1") decoded else jdbc

        /** The bounded, paged inline reader of a format-1.1 controlled catalog, if this is one. */
        fun controlledReads(): DucklakeControlledReads? =
            (catalog as? DucklakeControlledReads)?.takeIf { catalog.getSpecVersion() == "1.1" }
    }
}
