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

import io.trino.filesystem.Location
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException
import java.time.DateTimeException

/** File-location and projected-value helpers shared by the read-path page sources. */
internal object DucklakeReadSupport {
    fun isPuffinPath(path: String): Boolean =
        // DuckLake's delete-file path always ends with .puffin when format='puffin'
        // (see ducklake/src/storage/ducklake_delete.cpp — the writer hardcodes
        // "ducklake-<uuid>-delete.puffin"). Catalog format='puffin' has already been
        // permitted by DucklakeSplitManager.validateDeleteFileFormats by the time we get
        // here; dispatching on extension keeps the split schema stable and matches the
        // pattern Trino's Iceberg connector uses for puffin DV files.
        path.regionMatches(path.length - ".puffin".length, ".puffin", 0, ".puffin".length, ignoreCase = true)

    fun toLocation(path: String): Location
    {
        val location: Location = Location.of(path)
        if (location.scheme().isPresent) {
            return location
        }
        // Local path with no scheme. Prefix file:// on the RAW path — routing through
        // Path.toUri() percent-encodes, which DOUBLE-encodes a path that already contains
        // literal %XX escapes: DuckDB writes hive partition directories URL-encoded on disk
        // (e.g. `category=home%20appliances`, `created_at=…%3A30%3A00`), so Path.toUri turns
        // the literal `%` into `%25` and the reader looks for a non-existent `%2520` path.
        // Trino's Location does not percent-decode, so the literal on-disk path is what
        // newInputFile needs. For paths without special characters this is identical to the
        // old toUri() form (`file://` + absolute path).
        return Location.of("file://" + path)
    }

    /** Parses a stored partition value or `initial_default` for projection; never projects NULL on failure. */
    fun parseProjectedValue(column: DucklakeColumnHandle, value: String, origin: String): Any =
        try {
            DucklakePartitionValueParser.parseIdentity(column.columnType, value)
        }
        catch (e: IllegalArgumentException) {
            throw invalidProjectedValue(column, origin, e)
        }
        catch (e: ArithmeticException) {
            throw invalidProjectedValue(column, origin, e)
        }
        catch (e: DateTimeException) {
            throw invalidProjectedValue(column, origin, e)
        }

    private fun invalidProjectedValue(column: DucklakeColumnHandle, origin: String, cause: RuntimeException): TrinoException =
        TrinoException(GENERIC_INTERNAL_ERROR,
            "Invalid $origin for column ${column.columnName} (${column.columnId}) of type ${column.columnType}", cause)
}
