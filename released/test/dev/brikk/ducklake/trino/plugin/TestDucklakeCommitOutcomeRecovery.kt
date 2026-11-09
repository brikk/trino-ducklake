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

import dev.brikk.ducklake.catalog.DucklakeCommitOutcomeUnknownException
import dev.brikk.ducklake.catalog.DucklakeException
import dev.brikk.ducklake.catalog.DucklakePostCommitException
import io.trino.spi.Plugin
import io.trino.spi.connector.Connector
import io.trino.spi.connector.ConnectorContext
import io.trino.spi.connector.ConnectorMetadata
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.file.Path
import java.sql.DriverManager
import java.sql.SQLException
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/** Lost acknowledgement at the connector boundary AFTER real publication, not a JDBC fault simulator. */
class TestDucklakeCommitOutcomeRecovery {
    @TempDir
    lateinit var data: Path
    private val injected = AtomicReference<DucklakeException?>()
    private val finishCalls = AtomicInteger()

    @Test
    fun engineFailureDoesNotRetryOrRemovePublishedInsertCtasOrMergeOutput() {
        val server = DucklakeTestCatalogEnvironment.getServer()
        val database = "outcome_" + UUID.randomUUID().toString().replace("-", "")
        server.createDatabase(database)
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres")
                statement.execute("ATTACH '${server.getDuckDbAttachUri(database)}' AS lake " +
                    "(DATA_PATH '$data', DUCKLAKE_VERSION '1.0', AUTOMATIC_MIGRATION FALSE)")
            }
        }
        val session = testSessionBuilder().setCatalog("lake").setSchema("main").build()
        DistributedQueryRunner.builder(session).setWorkerCount(2)
            .addCoordinatorProperty("node-scheduler.include-coordinator", "false").build().use { runner ->
                runner.installPlugin(object : Plugin { override fun getConnectorFactories() = listOf(failureInjectingFactory()) })
                runner.createCatalog("lake", "ducklake", mapOf(
                    "ducklake.catalog.database-url" to server.getJdbcUrl(database),
                    "ducklake.catalog.database-user" to server.getUser(),
                    "ducklake.catalog.database-password" to server.getPassword(),
                    "ducklake.data-path" to data.toString(), "fs.hadoop.enabled" to "true"))
                runner.execute("CREATE TABLE t(i INTEGER)")
                for (acknowledged in listOf(false, true)) {
                    verifyOutcome(runner, acknowledged)
                }
            }
    }

    private fun verifyOutcome(runner: DistributedQueryRunner, acknowledged: Boolean) {
        val value = if (acknowledged) 20 else 10
        for ((sql, check) in listOf(
            "INSERT INTO t VALUES $value" to "SELECT count(*) FROM t WHERE i = $value",
            "CREATE TABLE c$value AS SELECT $value AS i" to "SELECT count(*) FROM c$value",
            "UPDATE t SET i = ${value + 1} WHERE i = $value" to "SELECT count(*) FROM t WHERE i = ${value + 1}",
            "MERGE INTO t USING (VALUES ${value + 2}) s(i) ON t.i = s.i " +
                "WHEN NOT MATCHED THEN INSERT (i) VALUES (s.i)" to "SELECT count(*) FROM t WHERE i = ${value + 2}",
            "DELETE FROM t WHERE i = ${value + 1}" to "SELECT count(*) FROM t WHERE i = ${value + 2}",
        )) {
            val cause = SQLException("lost acknowledgement after real publication", "08006")
            val failure = if (acknowledged) DucklakePostCommitException(sql, cause)
                else DucklakeCommitOutcomeUnknownException(sql, null, cause)
            val before = finishCalls.get()
            injected.set(failure)
            assertThatThrownBy { runner.execute(sql) }.hasMessageContaining(
                if (acknowledged) "COMMIT succeeded" else "COMMIT outcome is unknown")
            assertThat(injected.get()).isNull()
            assertThat(finishCalls.get()).isEqualTo(before + 1)
            // A scan after engine rollback must still open the published Parquet files.
            assertThat(runner.execute(check).onlyValue).isEqualTo(1L)
            assertThat(runner.execute("SELECT sum(i) FROM t").onlyValue).isNotNull()
        }
    }

    private fun failureInjectingFactory() = object : DucklakeConnectorFactory() {
        override fun create(catalogName: String, config: Map<String, String>, context: ConnectorContext): Connector =
            wrapConnector(super.create(catalogName, config, context))
    }

    private fun wrapConnector(delegate: Connector): Connector = Proxy.newProxyInstance(
        Connector::class.java.classLoader, arrayOf(Connector::class.java)) { _, method, arguments ->
        val result = invoke(delegate, method, arguments)
        if (method.name == "getMetadata") wrapMetadata(result as ConnectorMetadata) else result
    } as Connector

    private fun wrapMetadata(metadata: ConnectorMetadata): ConnectorMetadata = Proxy.newProxyInstance(
        ConnectorMetadata::class.java.classLoader, arrayOf(ConnectorMetadata::class.java)) { _, method, arguments ->
        val result = invoke(metadata, method, arguments)
        if (method.name in setOf("finishInsert", "finishCreateTable", "finishMerge")) {
            finishCalls.incrementAndGet()
            injected.getAndSet(null)?.let { failure -> throw DucklakeCatalogExceptions.translateCatalogException(failure) }
        }
        result
    } as ConnectorMetadata

    private fun invoke(target: Any, method: Method, arguments: Array<out Any?>?): Any? = try {
        method.invoke(target, *(arguments ?: emptyArray()))
    } catch (e: InvocationTargetException) {
        throw e.targetException
    }
}
