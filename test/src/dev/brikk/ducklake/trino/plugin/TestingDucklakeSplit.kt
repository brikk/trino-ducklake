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

import io.trino.spi.predicate.TupleDomain
import java.util.Optional

internal fun testingDucklakeSplit(
    dataFilePath: String,
    deleteFilePaths: List<String>,
    rowIdStart: Long?,
    recordCount: Long,
    fileSizeBytes: Long,
    fileFormat: String,
    fileStatisticsDomain: TupleDomain<DucklakeColumnHandle>,
    footerSize: Long = 0L,
    deleteFileFooterSizes: Map<String, Long> = emptyMap(),
    partitionValuesByColumnId: Map<Long, String> = emptyMap(),
    fieldIdToParquetSourceName: Map<Long, String> = emptyMap(),
    inlinedDeletedRowPositions: Set<Long> = emptySet(),
    affinityKey: Optional<String> = Optional.empty(),
    beginSnapshot: Long = 0L,
    snapshotFilterMax: Long? = null,
    deleteFileSnapshotFilters: Map<String, Long> = emptyMap(),
    dataFileId: Long? = null,
): DucklakeSplit = DucklakeSplit(
    dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat,
    fileStatisticsDomain, footerSize, deleteFileFooterSizes, partitionValuesByColumnId,
    fieldIdToParquetSourceName, inlinedDeletedRowPositions, affinityKey, beginSnapshot,
    snapshotFilterMax, deleteFileSnapshotFilters, dataFileId,
)
