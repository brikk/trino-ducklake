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
 * Update/MERGE distribution by the physical source data file.
 *
 * The engine serializes the file bounds into the fragment plan; workers use them through
 * [DucklakeNodePartitioningProvider]. Routing all rows of one data file to one writer preserves
 * DuckLake's invariant of at most one active delete file per data file and snapshot.
 */
@JvmRecord
data class DucklakeUpdatePartitioningHandle @JsonCreator constructor(
        @param:JsonProperty("files") val files: List<FilePositions>)
        : ConnectorPartitioningHandle
{
    init {
        require(files.map { it.dataFileId }.distinct().size == files.size) { "Duplicate DuckLake data file IDs" }
        for (file in files) {
            require(file.dataFileId >= 0 && file.recordCount >= 0) { "Invalid DuckLake mutation file bounds: $file" }
        }
    }

    @JvmRecord
    data class FilePositions @JsonCreator constructor(
            @param:JsonProperty("dataFileId") val dataFileId: Long,
            @param:JsonProperty("recordCount") val recordCount: Long)
}
