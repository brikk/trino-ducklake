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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeInlinedChangeRow
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeUnsupportedCatalogVersionException
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import java.util.function.Supplier

/** Public API adapter boundary. The released build has no candidate class references. */
internal interface DucklakeControlledReads {
    val jdbc: JdbcDucklakeCatalog
    fun <T> queryRead(queryId: String?, action: Supplier<T>): T
    fun cancelQuery(queryId: String)
    fun inlineRows(tableId: Long, version: Long, snapshot: Long, ids: List<Long>): List<DucklakeInlinedChangeRow>
    fun fileDefault(tableId: Long, columnId: Long, snapshot: Long): Any?
}

internal fun <T> DucklakeCatalog.queryReadSession(queryId: String?, action: () -> T): T = try {
    if (this is DucklakeControlledReads) queryRead(queryId, Supplier(action)) else readSession(Supplier(action))
} catch (failure: DucklakeUnsupportedCatalogVersionException) {
    throw TrinoException(NOT_SUPPORTED, failure.message, failure)
}

internal fun DucklakeCatalog.cancelReadQuery(queryId: String) {
    if (this is DucklakeControlledReads) cancelQuery(queryId)
}

/** Final-1.1 controlled reads must not load the legacy unbounded statistics aggregate. */
internal fun DucklakeCatalog.requiresBoundedStatistics(): Boolean =
    this is DucklakeControlledReads && getSpecVersion() == "1.1"

internal fun DucklakeCatalog.jdbcCatalog(): JdbcDucklakeCatalog = when (this) {
    is DucklakeControlledReads -> jdbc
    is JdbcDucklakeCatalog -> this
    else -> error("Decoded file defaults require JdbcDucklakeCatalog")
}
