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

import io.trino.spi.Page
import io.trino.spi.connector.BucketFunction
import io.trino.spi.connector.ConnectorNodePartitioningProvider
import io.trino.spi.connector.ConnectorPartitioningHandle
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.type.Type
import java.util.NavigableMap
import java.util.TreeMap

import io.trino.spi.type.BigintType.BIGINT

/** Node partitioning used only by row-level DELETE/UPDATE/MERGE. */
class DucklakeNodePartitioningProvider : ConnectorNodePartitioningProvider {
    override fun getBucketFunction(
            transactionHandle: ConnectorTransactionHandle,
            session: ConnectorSession,
            partitioningHandle: ConnectorPartitioningHandle,
            partitionChannelTypes: List<Type>,
            bucketCount: Int): BucketFunction {
        require(partitioningHandle is DucklakeUpdatePartitioningHandle) {
            "Unsupported DuckLake partitioning handle: ${partitioningHandle.javaClass.name}"
        }
        require(partitionChannelTypes == listOf(BIGINT)) {
            "DuckLake update partitioning requires one BIGINT row-id channel: $partitionChannelTypes"
        }
        return UpdateBucketFunction(partitioningHandle, bucketCount)
    }

    internal class UpdateBucketFunction(
            handle: DucklakeUpdatePartitioningHandle,
            private val bucketCount: Int) : BucketFunction {
        private val rangesByStart: NavigableMap<Long, DucklakeUpdatePartitioningHandle.RowIdRange> = TreeMap()

        init {
            require(bucketCount > 0) { "bucketCount must be positive" }
            handle.ranges.forEach { range -> rangesByStart[range.rowIdStart] = range }
        }

        override fun getBucket(page: Page, position: Int): Int {
            val rowIdBlock = page.getBlock(0)
            // NOT MATCHED INSERT rows have no source data file. They can share one writer; only
            // delete-bearing rows need file affinity.
            if (rowIdBlock.isNull(position)) {
                return 0
            }
            val rowId = BIGINT.getLong(rowIdBlock, position)
            val range = rangesByStart.floorEntry(rowId)?.value
                ?: throw IllegalArgumentException("No DuckLake data file owns merge row ID $rowId")
            if (rowId - range.rowIdStart >= range.recordCount) {
                throw IllegalArgumentException("No DuckLake data file owns merge row ID $rowId")
            }
            return (range.dataFileId.hashCode() and Int.MAX_VALUE) % bucketCount
        }
    }
}
