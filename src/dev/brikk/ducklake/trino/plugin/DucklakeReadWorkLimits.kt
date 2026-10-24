/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import dev.brikk.ducklake.catalog.DucklakeDataFile
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException

/** Consumer work bounds, NOT a claim that the library's legacy list APIs are SQL-paged. */
internal class DucklakeReadWorkLimits(private val config: DucklakeConfig) {
    fun files(files: List<DucklakeDataFile>) {
        if (!config.isEnableFinal11Reads()) return
        descriptors(files.size.toLong())
        val rows = files.distinctBy { it.dataFileId }.fold(0L) { n, file -> Math.addExact(n, file.recordCount) }
        rows(rows)
    }
    fun descriptors(count: Long) = check("file/split descriptors", count, config.getReadMaxFiles().toLong())
    fun rows(count: Long) = check("physical rows", count, config.getReadMaxRows())
    fun events(count: Long) = check("CDC events/units", count, config.getReadMaxCdcEvents())
    private fun check(kind: String, count: Long, maximum: Long) {
        if (config.isEnableFinal11Reads() && (count < 0 || count > maximum)) {
            throw TrinoException(NOT_SUPPORTED, "DuckLake read budget exceeded: $kind $count (limit $maximum); no partial result is permitted")
        }
        if (Thread.currentThread().isInterrupted) throw java.util.concurrent.CancellationException("DuckLake read cancelled")
    }
}
