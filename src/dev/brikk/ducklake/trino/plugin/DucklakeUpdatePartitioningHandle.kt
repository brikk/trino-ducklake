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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import io.trino.spi.connector.ConnectorPartitioningHandle

/**
 * Update/MERGE distribution by the source data file owning a global DuckLake row ID.
 *
 * Every row in `[rowIdStart, rowIdStart + recordCount)` belongs to [dataFileId]. The engine
 * serializes this handle into the fragment plan; workers use it through
 * [DucklakeNodePartitioningProvider]. Routing all rows of one data file to one writer preserves
 * DuckLake's invariant of at most one active delete file per data file and snapshot.
 */
@JvmRecord
data class DucklakeUpdatePartitioningHandle @JsonCreator constructor(
        @param:JsonProperty("ranges") val ranges: List<RowIdRange>)
        : ConnectorPartitioningHandle
{
    init {
        var previousEnd = Long.MIN_VALUE
        for (range in ranges) {
            require(range.recordCount >= 0) { "recordCount is negative for data_file_id ${range.dataFileId}" }
            require(range.rowIdStart >= previousEnd) { "DuckLake row-id ranges overlap or are not ordered" }
            previousEnd = Math.addExact(range.rowIdStart, range.recordCount)
        }
    }

    @JvmRecord
    data class RowIdRange @JsonCreator constructor(
            @param:JsonProperty("dataFileId") val dataFileId: Long,
            @param:JsonProperty("rowIdStart") val rowIdStart: Long,
            @param:JsonProperty("recordCount") val recordCount: Long)
}
