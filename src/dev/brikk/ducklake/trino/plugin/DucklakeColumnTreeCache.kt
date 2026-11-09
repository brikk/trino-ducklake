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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

/**
 * Full identity-bearing column tree per (tableId, snapshot), shared by Parquet field-id
 * resolution and inlined nested-field mapping. Bounded + cleared wholesale on overflow.
 */
internal class DucklakeColumnTreeCache(
        private val catalog: DucklakeCatalog,
        private val reportSharedMemory: (Long) -> Unit) {
    private val trees: ConcurrentMap<Key, List<DucklakeColumn>> = ConcurrentHashMap()
    private var retainedBytes = 0L

    private data class Key(val tableId: Long, val beginSnapshot: Long)

    @Synchronized
    fun get(tableId: Long, snapshotId: Long): List<DucklakeColumn> {
        val key = Key(tableId, snapshotId)
        trees[key]?.let { return it }
        if (trees.size >= MAX_ENTRIES) {
            trees.clear()
            retainedBytes = 0L
        }
        val tree: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(tableId, snapshotId)
        trees[key] = tree
        // Conservative retained-size reservation: map/key/list/record/boxed fields plus UTF-16
        // string storage (also covers compact strings). Serialized under the same lock as updates.
        retainedBytes += 256L + tree.sumOf { column ->
            256L + 2L * (column.columnName.length + column.columnType.length + (column.initialDefault?.length ?: 0))
        }
        reportSharedMemory(retainedBytes)
        return tree
    }

    private companion object {
        const val MAX_ENTRIES = 2048
    }
}
