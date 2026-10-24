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
import io.trino.spi.connector.ConnectorMetadata
import io.trino.spi.connector.SchemaTablePrefix
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.connector.TableColumnsMetadata
import io.trino.testing.connector.TestingConnectorSession.SESSION
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.lang.reflect.Proxy
import java.util.concurrent.CancellationException
import java.util.function.Supplier

class TestDucklakeReadOnlyMetadata {
    @Test
    fun noSqlLifecycleCallbacksNeverWaitForABorrowedConnection() {
        val fixture = Fixture()
        fixture.borrowFailure = java.sql.SQLException("blocked connection must not be borrowed")
        val wrapped = DucklakeReadOnlyMetadata.wrap(fixture.metadata, fixture.catalog, 10)
        wrapped.beginQuery(SESSION)
        wrapped.cleanupQuery(SESSION)
        assertThat(wrapped.getMetrics(SESSION)).isNotNull()
        assertThat(fixture.active).isFalse()
    }
    @Test
    fun lazyMetadataCannotEscapeTheOwnedViewAndBudgetsRecover() {
        val fixture = Fixture()
        val wrapped = DucklakeReadOnlyMetadata.wrap(fixture.metadata, fixture.catalog, 2)
        val rows = wrapped.streamTableColumns(SESSION, SchemaTablePrefix("main"))
        assertThat(fixture.active).isFalse()
        assertThat(rows.asSequence().toList()).hasSize(2)
        val bounded = DucklakeReadOnlyMetadata.wrap(fixture.metadata, fixture.catalog, 1)
        assertThatThrownBy { bounded.streamTableColumns(SESSION, SchemaTablePrefix("main")) }
            .hasMessageContaining("incomplete plan")
        assertThat(fixture.active).isFalse()
        assertThat(wrapped.listSchemaNames(SESSION)).containsExactly("main")
    }

    @Test
    fun writesNeverReachTheDelegateAndCancellationIsPreserved() {
        val fixture = Fixture()
        val wrapped = DucklakeReadOnlyMetadata.wrap(fixture.metadata, fixture.catalog, 10)
        assertThatThrownBy { wrapped.dropTable(SESSION, DucklakeTableHandle("main", "t", 1, 1)) }
            .hasMessageContaining("read-only")
        assertThat(fixture.writes).isZero()
        val cancelled = CancellationException("caller cancelled metadata")
        fixture.failure = cancelled
        assertThatThrownBy { wrapped.listSchemaNames(SESSION) }.isSameAs(cancelled)
        assertThat(fixture.active).isFalse()
        fixture.failure = null
        assertThat(wrapped.listSchemaNames(SESSION)).containsExactly("main")
    }

    @Test
    fun checkedConnectionFailureRetainsCauseNotUndeclaredThrowable() {
        val fixture = Fixture()
        val wrapped = DucklakeReadOnlyMetadata.wrap(fixture.metadata, fixture.catalog, 10)
        val sql = java.sql.SQLException("connection terminated", "08006")
        fixture.borrowFailure = sql
        assertThatThrownBy { wrapped.listSchemaNames(SESSION) }.hasMessageContaining("connection terminated").hasCause(sql)
        fixture.borrowFailure = null
        assertThat(wrapped.listSchemaNames(SESSION)).containsExactly("main")
    }

    private class Fixture {
        var active = false
        var writes = 0
        var failure: RuntimeException? = null
        var borrowFailure: java.sql.SQLException? = null
        private val base = Proxy.newProxyInstance(DucklakeCatalog::class.java.classLoader, arrayOf(DucklakeCatalog::class.java)) { _, method, _ ->
            when (method.name) {
                "getCurrentSnapshotId" -> { check(active); 3L }
                else -> error("Unexpected catalog call ${method.name}")
            }
        } as DucklakeCatalog
        val catalog: DucklakeCatalog = object : DucklakeCatalog by base {
            override fun <T> readSession(action: Supplier<T>): T {
                borrowFailure?.let { throw it }
                check(!active)
                active = true
                try { return action.get() } finally { active = false }
            }
        }
        val metadata = Proxy.newProxyInstance(ConnectorMetadata::class.java.classLoader, arrayOf(ConnectorMetadata::class.java)) { _, method, _ ->
            when (method.name) {
                "beginQuery", "cleanupQuery" -> { check(!active); null }
                "getMetrics" -> { check(!active); io.trino.spi.metrics.Metrics.EMPTY }
                "listSchemaNames" -> { check(active); failure?.let { throw it }; listOf("main") }
                "streamTableColumns" -> object : Iterator<TableColumnsMetadata> {
                    var index = 0
                    override fun hasNext(): Boolean { check(active); return index < 2 }
                    override fun next(): TableColumnsMetadata {
                        check(active)
                        if (!hasNext()) throw NoSuchElementException()
                        return TableColumnsMetadata.forTable(SchemaTableName("main", "t${index++}"), emptyList())
                    }
                }
                else -> { writes++; error("Write reached metadata") }
            }
        } as ConnectorMetadata
    }
}
