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

import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException

/** Logical identity is independent of a row's physical mutation locator. */
object DucklakeRowIdentity {
    fun rowId(lineage: LongArray?, rowIdStart: Long?, position: Long): Long {
        require(position >= 0) { "Negative physical file position: $position" }
        if (lineage != null) {
            if (position >= lineage.size) {
                throw TrinoException(GENERIC_INTERNAL_ERROR, "Embedded DuckLake row-ID column is shorter than the data file")
            }
            return lineage[position.toInt()]
        }
        val start = rowIdStart ?: throw TrinoException(GENERIC_INTERNAL_ERROR,
                "DuckLake row identity requires an embedded row-ID column or a present row_id_start; neither is available")
        return Math.addExact(start, position)
    }

    fun requireStart(rowIdStart: Long?): Long = rowIdStart
            ?: throw TrinoException(GENERIC_INTERNAL_ERROR,
                    "Legacy positional row-ID arithmetic requires a present DuckLake row_id_start")
}
