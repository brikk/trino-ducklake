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

import io.trino.filesystem.TrinoFileSystem
import io.trino.parquet.ParquetReaderOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException

/** Query-local: lazy data scans and every event for a path share one footer/identity read. */
internal class DucklakeChangeFeedFiles(
        private val fileSystem: TrinoFileSystem,
        private val options: ParquetReaderOptions,
        private val stats: FileFormatDataSourceStats,
        private val checkRows: (Long) -> Unit) {
    private val files = mutableMapOf<String, DucklakeDeleteFileReader.DataFileInfo>()
    fun get(path: String, footer: Long, format: String): DucklakeDeleteFileReader.DataFileInfo {
        if (!format.equals("parquet", ignoreCase = true)) throw TrinoException(NOT_SUPPORTED,
                "Data file '$path' has format '$format', but this connector reads only parquet data files.")
        return files.getOrPut(path) {
            DucklakeDeleteFileReader.readDataFileInfo(fileSystem, path, footer, options, stats, checkRows)
        }
    }
    fun getRetainedSizeInBytes(): Long = files.entries.sumOf { 128L + 2L * it.key.length + it.value.getRetainedSizeInBytes() }
    fun clear() = files.clear()
}
