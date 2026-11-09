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

import io.airlift.units.Duration
import io.trino.spi.Plugin
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.connector.Connector
import io.trino.spi.connector.ConnectorContext
import io.trino.spi.connector.ConnectorFactory
import io.trino.spi.connector.ConnectorPageSink
import io.trino.spi.connector.ConnectorPageSinkProvider
import io.trino.testing.QueryRunner
import io.trino.testing.assertions.Assert.assertEventually
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.util.concurrent.TimeUnit.SECONDS

/** A CTAS whose page sink fails must not leave the table `beginCreateTable` created. */
class TestDucklakeCtasFailureCleanup : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "ctas_failure_cleanup"

    override fun createQueryRunner(): QueryRunner =
            DucklakeQueryRunner.builder()
                    .useIsolatedCatalog(isolatedCatalogName())
                    .usePlugin(object : Plugin {
                        override fun getConnectorFactories(): Iterable<ConnectorFactory> = listOf(failingSinkFactory())
                    })
                    .build()

    @Test
    fun ctasWhosePageSinkThrowsLeavesNoTable() {
        assertThatThrownBy { computeActual("CREATE TABLE test_schema.$FAILING_TABLE AS SELECT * FROM test_schema.simple_table") }
                .hasMessageContaining(INJECTED)

        // Trino aborts the transaction asynchronously after reporting the failure.
        assertEventually<RuntimeException>(Duration(30.0, SECONDS)) {
            assertThat(activeCatalogTables(FAILING_TABLE)).isZero()
        }
        assertThat(computeActual("SHOW TABLES FROM test_schema LIKE '$FAILING_TABLE'").rowCount).isZero()

        computeActual("CREATE TABLE test_schema.$FAILING_TABLE (id INTEGER)")
        tryDropTable("test_schema.$FAILING_TABLE")
    }

    @Test
    fun ctasIntoAnotherTableStillPublishes() {
        computeActual("CREATE TABLE test_schema.ctas_ok AS SELECT id FROM test_schema.simple_table")
        try {
            assertThat(computeScalar("SELECT count(*) FROM test_schema.ctas_ok")).isEqualTo(5L)
        }
        finally {
            tryDropTable("test_schema.ctas_ok")
        }
    }

    private fun activeCatalogTables(name: String): Int =
            openCatalogConnection().use { connection ->
                connection.prepareStatement(
                        "SELECT count(*) FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL").use { statement ->
                    statement.setString(1, name)
                    statement.executeQuery().use { result ->
                        result.next()
                        result.getInt(1)
                    }
                }
            }

    private fun failingSinkFactory(): ConnectorFactory = object : DucklakeConnectorFactory() {
        override fun create(catalogName: String, config: Map<String, String>, context: ConnectorContext): Connector =
                proxy(super.create(catalogName, config, context)) { method, _, result ->
                    if (method.name == "getPageSinkProvider") failingSinkProvider(result as ConnectorPageSinkProvider) else result
                }
    }

    private fun failingSinkProvider(delegate: ConnectorPageSinkProvider): ConnectorPageSinkProvider =
            proxy(delegate) { _, arguments, result ->
                val targetsFailingTable = arguments.any { it is DucklakeWritableTableHandle && it.tableName == FAILING_TABLE }
                if (result is ConnectorPageSink && targetsFailingTable) failingSink(result) else result
            }

    private fun failingSink(delegate: ConnectorPageSink): ConnectorPageSink =
            Proxy.newProxyInstance(ConnectorPageSink::class.java.classLoader, arrayOf(ConnectorPageSink::class.java)) {
                _, method, arguments ->
                if (method.name == "appendPage") throw TrinoException(GENERIC_INTERNAL_ERROR, INJECTED)
                invoke(delegate, method, arguments)
            } as ConnectorPageSink

    private inline fun <reified T : Any> proxy(delegate: T, crossinline after: (Method, Array<out Any?>, Any?) -> Any?): T =
            Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { _, method, arguments ->
                after(method, arguments ?: emptyArray(), invoke(delegate, method, arguments))
            } as T

    private fun invoke(target: Any, method: Method, arguments: Array<out Any?>?): Any? = try {
        method.invoke(target, *(arguments ?: emptyArray()))
    }
    catch (e: InvocationTargetException) {
        throw e.targetException
    }

    private companion object {
        const val FAILING_TABLE = "sink_fails_ctas"
        const val INJECTED = "injected CTAS page sink failure"
    }
}
