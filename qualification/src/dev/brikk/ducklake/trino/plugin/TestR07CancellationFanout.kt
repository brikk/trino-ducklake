package dev.brikk.ducklake.trino.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

/** Real adapter registration and catalog controls; the JDBC boundary injects cancel faults. */
@Timeout(30)
class TestR07CancellationFanout {
    @Test fun allRegisteredStatementsAreAttemptedAndFailuresRetainTheirDiagnostics() {
        FaultDriver().use { driver -> driver.catalog().use { jdbc ->
            val c = PagedDucklakeCatalog(jdbc, DucklakeConfig())
            val executor = Executors.newFixedThreadPool(3)
            val probes = mutableListOf<StatementProbe>()
            try {
                val reads = (1..3).map {
                    executor.submit<String?> { c.queryRead("query", Supplier { jdbc.getSpecVersion() }) }.also {
                        probes.add(driver.next())
                    }
                }
                val failures = listOf(SQLException("first driver", "57014"), IllegalStateException("second driver"))
                probes[0].cancelAction = { throw failures[0] }
                probes[1].cancelAction = { throw failures[1] }
                val failure = catchThrowable { c.cancelQuery("query") }
                assertThat(probes.map { it.cancels.get() }).containsExactly(1, 1, 1)
                assertThat(listOf(failure) + failure.suppressed).containsExactlyInAnyOrderElementsOf(failures)
                probes.forEach { it.release.countDown() }
                reads.forEach { read -> assertThatThrownBy { read.get(5, TimeUnit.SECONDS) }.isInstanceOf(ExecutionException::class.java) }
                c.cancelQuery("query")
                assertThat(probes.map { it.cancels.get() }).containsExactly(1, 1, 1)
                assertThat(c.queryRead("query", Supplier { 7 })).isEqualTo(7)
            } finally {
                probes.forEach { it.release.countDown() }
                executor.shutdownNow()
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue()
            }
        } }
    }

    @Test fun blockingDriverDoesNotHoldRegistrationOrFinalCleanupLocks() {
        FaultDriver().use { driver -> driver.catalog().use { jdbc ->
            val c = PagedDucklakeCatalog(jdbc, DucklakeConfig())
            val executor = Executors.newFixedThreadPool(3)
            val cancelEntered = CountDownLatch(1)
            val cancelRelease = CountDownLatch(1)
            val read = executor.submit<String?> { c.queryRead("query", Supplier { jdbc.getSpecVersion() }) }
            val probe = driver.next()
            probe.cancelAction = { cancelEntered.countDown(); check(cancelRelease.await(10, TimeUnit.SECONDS)) }
            try {
                val cancellation = executor.submit { c.cancelQuery("query") }
                check(cancelEntered.await(5, TimeUnit.SECONDS))
                // This must both register and deregister while the first driver's cancel is held.
                val other = executor.submit<Int> { c.queryRead("query", Supplier { 9 }) }
                assertThat(other.get(2, TimeUnit.SECONDS)).isEqualTo(9)
                probe.release.countDown()
                assertThatThrownBy { read.get(2, TimeUnit.SECONDS) }.isInstanceOf(ExecutionException::class.java)
                c.cancelQuery("query") // First read's final cleanup has already removed its control.
                assertThat(probe.cancels.get()).isEqualTo(1)
                cancelRelease.countDown()
                cancellation.get(5, TimeUnit.SECONDS)
                assertThat(c.queryRead("query", Supplier { 11 })).isEqualTo(11)
            } finally {
                cancelRelease.countDown()
                probe.release.countDown()
                executor.shutdownNow()
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue()
            }
        } }
    }

    private class StatementProbe {
        val release = CountDownLatch(1)
        val cancels = AtomicInteger()
        @Volatile var cancelAction: () -> Unit = {}
    }

    private class FaultDriver : Driver by org.duckdb.DuckDBDriver(), AutoCloseable {
        private val delegate = org.duckdb.DuckDBDriver()
        private val url = "jdbc:r07-cancel:${UUID.randomUUID()}"
        private val executing = LinkedBlockingQueue<StatementProbe>()
        init { DriverManager.registerDriver(this) }
        override fun acceptsURL(candidate: String?) = candidate == url
        override fun connect(candidate: String?, properties: Properties?): Connection? {
            if (!acceptsURL(candidate)) return null
            val connection = delegate.connect("jdbc:duckdb:", Properties())
            connection.createStatement().use {
                it.execute("CREATE TABLE ducklake_metadata(key VARCHAR, value VARCHAR, scope VARCHAR)")
                it.execute("INSERT INTO ducklake_metadata VALUES('version','1.0',NULL)")
            }
            return proxy(Connection::class.java) { method, args ->
                val result = invoke(connection, method, args)
                if (method.name == "prepareStatement" && (args!![0] as String).contains("ducklake_metadata")) {
                    val probe = StatementProbe()
                    proxy(PreparedStatement::class.java) { call, parameters ->
                        when (call.name) {
                            "execute", "executeQuery" -> {
                                executing.add(probe)
                                check(probe.release.await(15, TimeUnit.SECONDS))
                                invoke(result!!, call, parameters)
                            }
                            "cancel" -> { probe.cancels.incrementAndGet(); probe.cancelAction(); null }
                            else -> invoke(result!!, call, parameters)
                        }
                    }
                } else result
            }
        }
        fun next(): StatementProbe = checkNotNull(executing.poll(5, TimeUnit.SECONDS)) { "No registered executing statement" }
        fun catalog() = JdbcDucklakeCatalog(DucklakeCatalogConfig().apply {
            catalogDatabaseUrl = url
            maxCatalogConnections = 3
        })
        override fun close() { DriverManager.deregisterDriver(this) }
    }

    companion object {
        private fun invoke(target: Any, method: java.lang.reflect.Method, args: Array<out Any?>?): Any? =
            try { method.invoke(target, *(args ?: emptyArray())) } catch (e: InvocationTargetException) { throw e.targetException }
        private fun <T> proxy(type: Class<T>, action: (java.lang.reflect.Method, Array<out Any?>?) -> Any?): T =
            type.cast(Proxy.newProxyInstance(type.classLoader, arrayOf(type)) { _, method, args -> action(method, args) })
    }
}
