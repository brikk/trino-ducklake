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
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorMetadata
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy

/** One complete synchronous SPI metadata operation owns one read view, on its calling thread. */
internal object DucklakeReadOnlyMetadata {
    fun wrap(delegate: ConnectorMetadata, catalog: DucklakeCatalog, maxEntries: Int): ConnectorMetadata = Proxy.newProxyInstance(
        ConnectorMetadata::class.java.classLoader, arrayOf(ConnectorMetadata::class.java)) { proxy, method, arguments ->
        when (method.name) {
            "toString" -> "ReadOnlyDucklakeMetadata"
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === arguments?.firstOrNull()
            else -> {
                if (!isReadOperation(method.name)) throw TrinoException(NOT_SUPPORTED, "Experimental final-1.1 consumer is read-only: ${method.name}")
                // DucklakeMetadata inherits these no-SQL defaults. Opening JDBC here can make
                // cancellation/metrics collection wait behind the operation being cancelled.
                if (method.name in setOf("beginQuery", "cleanupQuery", "getMetrics")) {
                    return@newProxyInstance method.invoke(delegate, *(arguments ?: emptyArray()))
                }
                try {
                    catalog.readSession {
                        // Force in-session admission/layout validation even for a metadata cache hit.
                        catalog.currentSnapshotId
                        try { materialize(method.invoke(delegate, *(arguments ?: emptyArray())), maxEntries) }
                        catch (e: InvocationTargetException) { throw e.targetException }
                    }
                } catch (e: java.sql.SQLException) {
                    // SPI methods do not declare checked exceptions; retain the actual transport
                    // cause instead of leaking Proxy's opaque UndeclaredThrowableException.
                    throw TrinoException(GENERIC_INTERNAL_ERROR, "DuckLake metadata connection failed: ${e.message}", e)
                }
            }
        }
    } as ConnectorMetadata

    /** Never let a deferred catalog iterator escape its owning readSession. */
    private fun materialize(value: Any?, maxEntries: Int): Any? = when (value) {
        is java.util.stream.Stream<*> -> value.use { bounded(it.iterator(), maxEntries).stream() }
        is Iterator<*> -> {
            val closeable = value as? AutoCloseable
            val rows = if (closeable == null) bounded(value, maxEntries) else closeable.use { bounded(value, maxEntries) }
            rows.iterator()
        }
        else -> value
    }

    private fun bounded(values: Iterator<*>, maximum: Int): List<Any?> = buildList {
        while (values.hasNext()) {
            if (size >= maximum) throw TrinoException(NOT_SUPPORTED, "DuckLake metadata iteration budget exceeded; incomplete plan")
            add(values.next())
        }
    }

    private fun isReadOperation(name: String): Boolean =
        listOf("get", "list", "stream", "apply", "is", "supports").any { name.startsWith(it) } ||
            name in setOf("schemaExists", "tableExists", "validateScan", "beginQuery", "cleanupQuery", "redirectTable")
}
