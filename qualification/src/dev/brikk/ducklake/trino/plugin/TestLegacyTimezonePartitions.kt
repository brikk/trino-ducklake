/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.Session
import io.trino.filesystem.cache.NoopSplitAffinityProvider
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilterSnapshot
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.TimeZoneKey
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import io.trino.testing.connector.TestingConnectorSession.SESSION
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.time.Instant
import java.util.function.Supplier

/** Native stored calendar values are writer-local in both old and migrated metadata. */
class TestLegacyTimezonePartitions {
    @Test
    fun realSqlRetainsLegacyAndUtcWritersWithExactResiduals(): Unit = Fixture().use { f ->
        f.prepare()
        DistributedQueryRunner.builder(testSessionBuilder().setCatalog("lenient").setSchema("main").build())
            .setWorkerCount(1).build().use { runner ->
                runner.installPlugin(DucklakePlugin())
                for (lenient in listOf(true, false)) {
                    val name = if (lenient) "lenient" else "strict"
                    runner.createCatalog(name, "ducklake", mapOf(
                        "ducklake.catalog.database-url" to "jdbc:duckdb:${f.db}", "ducklake.data-path" to f.data.toString(),
                        "ducklake.temporal-partition-encoding" to "CALENDAR",
                        "ducklake.temporal-partition-encoding-read-leniency" to lenient.toString(), "fs.hadoop.enabled" to "true"))
                    for (zone in listOf("UTC", "Pacific/Kiritimati", "America/Los_Angeles")) {
                        val session = Session.builder(runner.defaultSession).setCatalog(name).setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zone)).build()
                        assertThat(runner.execute(session, "SELECT id FROM t WHERE ts=TIMESTAMP '2021-01-01 05:00:00 UTC' ORDER BY id")
                            .materializedRows.map { it.getField(0) }).containsExactly(1, 2)
                        assertThat(runner.execute(session, "SELECT id FROM t WHERE ts>=TIMESTAMP '2021-01-01 05:00:00 UTC' " +
                            "AND ts<TIMESTAMP '2021-01-01 05:01:00 UTC' ORDER BY id").materializedRows.map { it.getField(0) })
                            .containsExactly(1, 2)
                    }
                }
            }
    }

    @Test
    fun actualSplitPlanningRetainsEveryUnknownTimezoneTransform(): Unit = Fixture().use { f ->
        f.prepare()
        f.assertSplits(final11 = false)
    }

    @Test
    fun nativeMigrationAndMixedNewWritesDoNotAttestLegacyUtcPartitions(): Unit = Fixture().use { f ->
        f.prepare()
        f.native("1.1", "SET TimeZone='Pacific/Kiritimati'; INSERT INTO lake.t VALUES(4,TIMESTAMPTZ '2021-01-01 05:00:00+00')", migrate = true)
        f.assertSplits(final11 = true)
    }

    private class Fixture : AutoCloseable {
        val root = Files.createTempDirectory(Path.of("/tmp/opencode"), "trino-tz-")
        val db = root.resolve("meta.db")
        val data = Files.createDirectories(root.resolve("data"))
        fun native(version: String, sql: String, migrate: Boolean = false) = DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                s.execute("INSTALL ducklake; LOAD ducklake")
                s.executeQuery("PRAGMA version").use { r -> check(r.next()); check(r.getString(2) == "d54cbbe3df") }
                s.executeQuery("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake'").use { r ->
                    check(r.next()); check(r.getString(1) == "417a9117f5")
                }
                s.execute("ATTACH 'ducklake:$db' AS lake(DATA_PATH '$data',DUCKLAKE_VERSION '$version',AUTOMATIC_MIGRATION $migrate,DATA_INLINING_ROW_LIMIT 0)")
                s.execute(sql)
                s.execute("DETACH lake")
            }
        }
        fun prepare() {
            native("1.0", "CREATE TABLE lake.t(id INTEGER,ts TIMESTAMPTZ); " +
                "ALTER TABLE lake.t SET PARTITIONED BY(year(ts),month(ts),day(ts),hour(ts)); " +
                "SET TimeZone='Etc/GMT+12'; INSERT INTO lake.t VALUES(1,TIMESTAMPTZ '2021-01-01 05:00:00+00'); " +
                "SET TimeZone='UTC'; INSERT INTO lake.t VALUES(2,TIMESTAMPTZ '2021-01-01 05:00:00+00'),(3,TIMESTAMPTZ '2021-01-01 05:30:00+00')")
        }
        fun assertSplits(final11: Boolean) {
            val config = DucklakeConfig().setCatalogDatabaseUrl("jdbc:duckdb:$db").setDataPath(data.toString())
            JdbcDucklakeCatalog(config.toCatalogConfig().apply { enableFinal11Reads = final11 }).use { catalog -> catalog.readSession(Supplier {
                val snapshot = catalog.currentSnapshotId
                val table = catalog.getTable("main", "t", snapshot)!!
                val ts = catalog.getTableColumns(table.tableId, snapshot).single { it.columnName == "ts" }
                val column = DucklakeColumnHandle(ts.columnId, "ts", TIMESTAMP_TZ_MICROS, true)
                val domain = Domain.singleValue(TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                    Instant.parse("2021-01-01T05:00:00Z").toEpochMilli(), 0, UTC_KEY))
                val predicate = TupleDomain.withColumnDomains(mapOf(column to domain))
                val handle = DucklakeTableHandle("main", "t", table.tableId, snapshot, predicate, predicate)
                val expected = catalog.getDataFiles(table.tableId, snapshot).map { it.dataFileId }.toSet()
                val parts = catalog.getFilePartitionValues(table.tableId, snapshot).values.flatten().map { it.partitionValue }
                assertThat(parts).contains("2020", "12", "31", "17", "2021", "1", "5")
                DucklakeSplitManager(catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider())
                    .getSplits(null, SESSION, handle, mutableSetOf(), Constraint.alwaysTrue()).use { source ->
                        val ids = mutableSetOf<Long>()
                        while (!source.isFinished) source.getNextBatch(128, DynamicFilterSnapshot.EMPTY).get().forEach { ids.add(checkNotNull((it as DucklakeSplit).dataFileId)) }
                        assertThat(ids).isEqualTo(expected)
                    }
            }) }
        }
        override fun close() { Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } } }
    }
}
