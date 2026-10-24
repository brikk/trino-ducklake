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

import dev.brikk.ducklake.catalog.DucklakeFileKind
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.catalog.TestingDucklakeMySqlCatalogServer
import dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer
import io.airlift.json.ObjectMapperProvider
import io.trino.filesystem.cache.NoopSplitAffinityProvider
import io.trino.filesystem.local.LocalFileSystemFactory
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.connector.DynamicFilterSnapshot
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.VarbinaryType.VARBINARY
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.TimestampType.TIMESTAMP_NANOS
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.DriverManager
import java.util.Optional
import java.util.function.Supplier

/** Candidate-only SPI qualification. This is NOT production flag/session wiring or SQL rollout. */
@Execution(ExecutionMode.SAME_THREAD)
class TestR07ConsumerTrial {
    @Test
    fun everyTrialCaseHasAJUnitDiscoverableVoidSignature() {
        val tests = javaClass.declaredMethods.filter { it.isAnnotationPresent(Test::class.java) }
        assertThat(tests.size).isGreaterThanOrEqualTo(15)
        assertThat(tests.map { it.returnType }).containsOnly(Void.TYPE)
    }
    @Test
    fun mixedNativeFileInlineScanAndMatchedPeerSplitWire(): Unit = mixedScan(Lane.LOCAL)
    @Test fun postgresMixedScan(): Unit = mixedScan(Lane.POSTGRES)
    @Test fun nativeQuackMixedScan(): Unit = mixedScan(Lane.NATIVE_QUACK)
    @Test fun javaQuackMixedScan(): Unit = mixedScan(Lane.JAVA_QUACK)
    @Test fun existingMySqlScalarMixedScan(): Unit = mixedScan(Lane.MYSQL)

    private fun mixedScan(lane: Lane): Unit = Fixture(lane).use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',2); CREATE TABLE lake.t(i INTEGER,b BLOB,s VARCHAR); " +
            "BEGIN; INSERT INTO lake.t VALUES(1,from_hex('005CFF'),NULL),(2,NULL,'NULL'); " +
            "INSERT INTO lake.t VALUES(3,NULL,''),(4,NULL,'four'),(5,NULL,'five'),(6,NULL,'six'),(7,NULL,'seven'); COMMIT")
        f.reader().use { coordinator -> f.reader().use { worker ->
            coordinator.readSession(Supplier {
                val profile = coordinator.readBackendProfile()
                assertThat(profile.qualifiedWrites).isFalse()
                if (lane in setOf(Lane.POSTGRES, Lane.MYSQL)) {
                    assertThat(profile.sqlEngineSourceId).isNull()
                    assertThat(profile.unsignedExtensionsAllowed).isNull()
                    assertThat(profile.ducklakeExtensionVersion).isNull()
                } else {
                    assertThat(profile.sqlEngineSourceId).isEqualTo("d54cbbe3df")
                    assertThat(profile.unsignedExtensionsAllowed).isFalse()
                }
                if (lane == Lane.JAVA_QUACK) assertThat(profile.jdbcDriverVersion).isEqualTo("0.8.0")
            })
            val plan = f.plan(coordinator)
            assertThat(plan.splits.filterIsInstance<DucklakeSplit>()).hasSize(1)
            assertThat(plan.splits.filterIsInstance<DucklakeInlinedSplit>()).hasSize(1)
            val columns = listOf(f.column(coordinator, plan, "i", INTEGER), f.column(coordinator, plan, "b", VARBINARY),
                f.column(coordinator, plan, "s", VARCHAR))
            val mapper = ObjectMapperProvider().get()
            val wire = plan.splits.map { split -> mapper.readValue(mapper.writeValueAsBytes(split), split.javaClass) as ConnectorSplit }
            val rows = f.scan(worker, plan.copy(splits = wire), columns).sortedBy { it[0] as Int }
            assertThat(rows.map { it[0] }).containsExactly(1, 2, 3, 4, 5, 6, 7)
            assertThat((rows[0][1] as io.trino.spi.type.SqlVarbinary).bytes).containsExactly(0, 0x5c, 0xff.toByte())
            assertThat(rows[0][2]).isNull()
            assertThat(rows[1][2]).isEqualTo("NULL")
            assertThat(rows[2][2]).isEqualTo("")
        } }
    }

    @Test
    fun realBarePuffinScan(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',0); CALL lake.set_option('write_deletion_vectors',true); " +
            "CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t FROM range(100); DELETE FROM lake.t WHERE i<10")
        f.reader().use { c ->
            val plan = f.plan(c)
            assertThat(plan.splits.filterIsInstance<DucklakeSplit>().single().deleteFilePaths.single()).endsWith(".puffin")
            assertThat(f.scan(c, plan, listOf(f.column(c, plan, "i", INTEGER))).map { it[0] as Int }.sorted())
                .containsExactlyElementsOf((10..99).toList())
        }
    }

    @Test
    fun nativeTaggedParquetNullPartialMaxAndEqualPathCdcWindows(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('write_deletion_vectors',false); CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t FROM range(10)")
        f.native("DELETE FROM lake.t WHERE i=1")
        val a = f.snapshot()
        f.native("DELETE FROM lake.t WHERE i=5")
        val b = f.snapshot()
        f.native("CALL ducklake_flush_inlined_data('lake')")
        f.reader().use { c ->
            val plan = f.plan(c, a)
            c.readSession(Supplier {
                val file = c.getDataFiles(plan.handle.tableId, a).single()
                assertThat(file.deleteFilePartialMax).isNull()
                val candidate = c.getDeletionsBetween(plan.handle.tableId, b, b).single { !it.fullFileDelete }
                assertThat(candidate.previousDeletePath).isEqualTo(candidate.currentDeletePath)
            })
            val split = plan.splits.filterIsInstance<DucklakeSplit>().single()
            assertThat(split.deleteFileSnapshotFilters.values).containsOnly(a)
            assertThat(f.scan(c, plan, listOf(f.column(c, plan, "i", INTEGER))).map { it[0] as Int }.sorted())
                .containsExactly(0, 2, 3, 4, 5, 6, 7, 8, 9)
            assertThat(f.deletions(c, b, b)).containsExactly(listOf<Any?>(b, 5))
            assertThat(f.deletions(c, a, b).sortedBy { it[0] as Long }).containsExactly(listOf<Any?>(a, 1), listOf<Any?>(b, 5))
        }
    }

    @Test
    fun nativeCumulativePuffinEqualPathsDoNotCancelCdc(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('write_deletion_vectors',true); CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(1),(2),(3),(4)")
        f.native("DELETE FROM lake.t WHERE i=2")
        val a = f.snapshot()
        f.native("DELETE FROM lake.t WHERE i=4")
        val b = f.snapshot()
        f.native("CALL ducklake_flush_inlined_data('lake',table_name=>'t')")
        f.reader().use { c ->
            val plan = f.plan(c, a)
            c.readSession(Supplier {
                val candidate = c.getDeletionsBetween(plan.handle.tableId, b, b).single { !it.fullFileDelete }
                assertThat(candidate.previousDeletePath).isEqualTo(candidate.currentDeletePath)
            })
            assertThat(f.scan(c, plan, listOf(f.column(c, plan, "i", INTEGER))).map { it[0] as Int }.sorted())
                .containsExactly(1, 3, 4)
            assertThat(f.deletions(c, b, b)).containsExactly(listOf<Any?>(b, 4))
            assertThat(f.deletions(c, a, b).sortedBy { it[0] as Long }).containsExactly(listOf<Any?>(a, 2), listOf<Any?>(b, 4))
        }
    }

    @Test
    fun nativeMergeEmbeddedAbsoluteIdsAndDataSnapshotsWinOverNullStart(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',2); CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(1),(2),(3)")
        val first = f.snapshot()
        f.native("INSERT INTO lake.t VALUES(4)")
        f.native("INSERT INTO lake.t VALUES(5),(6),(7)")
        f.native("CALL ducklake_merge_adjacent_files('lake','t')")
        f.reader().use { c ->
            val plan = f.plan(c, first)
            assertThat(plan.splits.filterIsInstance<DucklakeSplit>().single().rowIdStart).isNull()
            val cols = listOf(f.column(c, plan, "i", INTEGER), VirtualKind.ROW_ID.columnHandle())
            assertThat(f.scan(c, plan, cols).sortedBy { it[0] as Int })
                .containsExactly(listOf<Any?>(1, 0L), listOf<Any?>(2, 1L), listOf<Any?>(3, 2L))
            val latest = f.plan(c)
            assertThat(f.scan(c, latest, listOf(f.column(c, latest, "i", INTEGER))).map { it[0] as Int }.sorted())
                .containsExactly(1, 2, 3, 4, 5, 6, 7)
        }
        f.runner().use { runner ->
            assertThat(runner.execute("SELECT i,\"\$row_id\" FROM t FOR VERSION AS OF $first ORDER BY i").materializedRows.map { it.fields })
                .containsExactly(listOf<Any?>(1, 0L), listOf<Any?>(2, 1L), listOf<Any?>(3, 2L))
            assertThat(runner.execute("SELECT count(*) FROM t").onlyValue).isEqualTo(7L)
        }
    }

    @Test
    fun countRefusalAndOwnedSessionFailureRecoveryWithBoundedCompanionPages(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',0); CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(1)")
        val old = f.snapshot()
        f.native("INSERT INTO lake.t VALUES(2)")
        f.reader().use { c ->
            val abort = java.util.concurrent.CancellationException("consumer planning cancelled")
            assertThatThrownBy { c.readSession(Supplier { c.currentSnapshotId; throw abort }) }.isSameAs(abort)
            c.readSession(Supplier {
                val snapshot = c.currentSnapshotId
                val table = c.getTable("main", "t", snapshot)!!
                assertThatThrownBy { c.getLiveRowCount(table.tableId, old) }.hasMessageContaining("historical")
                val metadata = DucklakeMetadata(c, DucklakeTypeConverter(TESTING_TYPE_MANAGER))
                assertThat(metadata.getTableStatistics(SESSION, DucklakeTableHandle("main", "t", table.tableId, old)).rowCount.isUnknown).isTrue()
                assertThat(c.getLiveRowCount(table.tableId, snapshot)).isEqualTo(2)
                assertThat(c.getColumnStats(table.tableId, snapshot).map { it.minValue }).containsOnlyNulls()
                val h = c.readHierarchy()
                var cursor: Long? = null
                val paths = mutableSetOf<String>()
                var complete = false
                // Explicit total page/path budgets, including the terminal empty page.
                for (pageNumber in 1..4) {
                    val page = h.listRetainedFiles(DucklakeFileKind.DATA, cursor, 1)
                    if (page.isEmpty()) { complete = true; break }
                    check(paths.size < 3) { "path budget exhausted" }
                    val file = page.single()
                    check(cursor == null || file.fileId > cursor!!) { "non-advancing cursor" }
                    check(paths.add(file.resolvedPath)) { "duplicate path" }
                    cursor = file.fileId
                }
                check(complete) { "page budget exhausted: incomplete plan" }
                assertThat(paths).hasSize(2)
                assertThatThrownBy {
                    val onlyPage = h.listRetainedFiles(DucklakeFileKind.DATA, null, 1)
                    check(onlyPage.isEmpty()) { "page budget exhausted: incomplete plan" }
                }.hasMessageContaining("incomplete plan")
                assertThat(c.readBackendProfile().accessRoute).isEqualTo("local-duckdb")
            })
            val all = f.plan(c)
            val column = f.column(c, all, "i", INTEGER)
            val constraint = Constraint(io.trino.spi.predicate.TupleDomain.withColumnDomains(
                mapOf<io.trino.spi.connector.ColumnHandle, io.trino.spi.predicate.Domain>(column to
                    io.trino.spi.predicate.Domain.singleValue(INTEGER, 999L))))
            assertThat(f.plan(c, constraint = constraint).splits).hasSameSizeAs(all.splits)
        }
    }

    @Test
    fun rawTemporalTextAndTargetStableIdDefaultsAreConsumedOnlyOnce(): Unit = Fixture().use { f ->
        f.native("CREATE TABLE lake.t(i INTEGER,s VARCHAR,d DATE,ts TIMESTAMP_NS); " +
            "INSERT INTO lake.t VALUES(1,NULL,DATE '2024-02-29',TIMESTAMP_NS '2024-02-29 12:34:56.123456789')")
        f.native("ALTER TABLE lake.t ADD COLUMN added VARCHAR DEFAULT 'NULL'; ALTER TABLE lake.t RENAME s TO renamed")
        f.reader().use { c ->
            val plan = f.plan(c)
            val columns = listOf(f.column(c, plan, "i", INTEGER), f.column(c, plan, "renamed", VARCHAR),
                f.column(c, plan, "added", VARCHAR), f.column(c, plan, "d", DATE), f.column(c, plan, "ts", TIMESTAMP_NANOS))
            val row = f.scan(c, plan, columns).single()
            assertThat(row.take(3)).containsExactly(1, null, "NULL")
            assertThat(row[3].toString()).isEqualTo("2024-02-29")
            assertThat(row[4].toString()).isEqualTo("2024-02-29 12:34:56.123456789")
            c.readSession(Supplier {
                val tableColumns = c.getTableColumns(plan.handle.tableId, plan.handle.snapshotId)
                val info = c.getInlinedDataInfos(plan.handle.tableId, plan.handle.snapshotId).first()
                val raw = c.readInlinedData(plan.handle.tableId, info.schemaVersion, plan.handle.snapshotId, tableColumns).single()
                assertThat(raw[2]).isEqualTo("2024-02-29")
                assertThat(raw[3]).isEqualTo("2024-02-29 12:34:56.123456789")
                val decoded = c.readInlinedDataDecoded(plan.handle.tableId, info.schemaVersion, plan.handle.snapshotId, tableColumns).single()
                assertThat(decoded[2]).isEqualTo(java.time.LocalDate.of(2024, 2, 29))
                assertThat(decoded[3]).isEqualTo(java.time.LocalDateTime.parse("2024-02-29T12:34:56.123456789"))
            })
        }
        f.runner().use { runner ->
            assertThat(runner.execute("SELECT CAST(d AS VARCHAR),CAST(ts AS VARCHAR),added FROM t").materializedRows.single().fields)
                .containsExactly("2024-02-29", "2024-02-29 12:34:56.123456789", "NULL")
        }
    }

    @Test
    fun actualGeometryParquetIsWkbNotMerelyDiscoverableMetadata(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',0); CREATE TABLE lake.t(i INTEGER,g GEOMETRY); " +
            "INSERT INTO lake.t VALUES(1,'POINT(1 2)'::GEOMETRY),(2,NULL)")
        f.reader().use { c ->
            val plan = f.plan(c)
            val rows = f.scan(c, plan, listOf(f.column(c, plan, "i", INTEGER), f.column(c, plan, "g", VARBINARY))).sortedBy { it[0] as Int }
            val hex = (rows[0][1] as io.trino.spi.type.SqlVarbinary).bytes.joinToString("") { "%02x".format(it) }
            assertThat(hex).isEqualTo("0101000000000000000000f03f0000000000000040")
            assertThat(rows[1][1]).isNull()
        }
    }

    @Test
    fun nestedStableIdRenameWithBlobAndTemporalTextDoesNotReapplyOldNames(): Unit = Fixture().use { f ->
        f.native("CREATE TABLE lake.t(i INTEGER,s STRUCT(a INTEGER,b BLOB,d DATE)); " +
            "INSERT INTO lake.t VALUES(1,{'a':10,'b':from_hex('005CFF'),'d':DATE '2024-02-29'}),(2,NULL)")
        f.native("ALTER TABLE lake.t RENAME s.a TO renamed; ALTER TABLE lake.t ALTER s TYPE STRUCT(renamed BIGINT,b BLOB,d DATE)")
        f.reader().use { c ->
            val plan = f.plan(c)
            val type = io.trino.spi.type.RowType.from(listOf(
                io.trino.spi.type.RowType.field("renamed", BIGINT), io.trino.spi.type.RowType.field("b", VARBINARY),
                io.trino.spi.type.RowType.field("d", DATE)))
            val rows = f.scan(c, plan, listOf(f.column(c, plan, "i", INTEGER), f.column(c, plan, "s", type))).sortedBy { it[0] as Int }
            val struct = rows[0][1] as List<*>
            assertThat(struct[0]).isEqualTo(10L)
            assertThat((struct[1] as io.trino.spi.type.SqlVarbinary).bytes).containsExactly(0, 0x5c, 0xff.toByte())
            assertThat(struct[2].toString()).isEqualTo("2024-02-29")
            assertThat(rows[1][1]).isNull()
        }
    }

    @Test
    fun nativeInlineUpdatePairsImagesAndSuppressesSameTransactionTransientRows(): Unit = Fixture().use { f ->
        f.native("CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(1)")
        f.native("BEGIN; UPDATE lake.t SET i=2 WHERE i=1; INSERT INTO lake.t VALUES(99); DELETE FROM lake.t WHERE i=99; COMMIT")
        val updated = f.snapshot()
        f.reader().use { c ->
            val rows = f.changes(c, updated, updated)
            assertThat(rows.map { it[2] to it[3] }).containsExactlyInAnyOrder("update_preimage" to 1, "update_postimage" to 2)
            assertThat(rows.map { it[1] }).containsOnly(0L)
        }
    }

    @Test
    fun unsupportedVariantRejectsPayloadBeforeReturningRows(): Unit = Fixture().use { f ->
        f.native("CREATE TABLE lake.t(i INTEGER,v VARIANT); INSERT INTO lake.t VALUES(1,'literal'::VARIANT)")
        f.reader().use { c ->
            val plan = f.plan(c)
            assertThatThrownBy { f.scan(c, plan, listOf(f.column(c, plan, "v", VARCHAR))) }
                .hasMessageContaining("variant")
        }
    }

    @Test
    fun failedDescriptorQueryIsNotAnEmptyPlanAndReleasesTheOnlyConnection(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',0); CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(1)")
        f.metadata("ALTER TABLE ducklake_data_file RENAME file_format TO held_format")
        f.reader().use { c ->
            assertThatThrownBy { f.plan(c) }.hasMessageContaining("file_format")
            c.readSession(Supplier { assertThat(c.currentSnapshotId).isGreaterThan(0) })
        }
    }

    @Test
    fun unknownDeleteCountIsNotZeroOrAnExactOptimizerEstimate(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',0); CALL lake.set_option('write_deletion_vectors',true); " +
            "CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t FROM range(100); DELETE FROM lake.t WHERE i<10")
        f.metadata("UPDATE ducklake_delete_file SET delete_count=NULL")
        f.reader().use { c ->
            val plan = f.plan(c)
            c.readSession(Supplier {
                assertThatThrownBy { c.getLiveRowCount(plan.handle.tableId, plan.handle.snapshotId) }.hasMessageContaining("cannot assume zero")
                val metadata = DucklakeMetadata(c, DucklakeTypeConverter(TESTING_TYPE_MANAGER))
                assertThat(metadata.getTableStatistics(SESSION, plan.handle).rowCount.isUnknown).isTrue()
            })
            assertThat(f.scan(c, plan, listOf(f.column(c, plan, "i", INTEGER))).size).isEqualTo(90)
        }
    }

    @Test
    fun unmodifiedProductionSqlRouteKeepsFinal11ClosedOnMatchedNodes(): Unit = Fixture().use { f ->
        f.native("CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(1)")
        DistributedQueryRunner.builder(testSessionBuilder().setCatalog("ducklake").setSchema("main").build())
            .setWorkerCount(1).build().use { runner ->
                runner.installPlugin(DucklakePlugin())
                runner.createCatalog("ducklake", "ducklake", mapOf(
                    "ducklake.catalog.database-url" to "jdbc:duckdb:${f.db}",
                    "ducklake.data-path" to f.data.toString(), "fs.hadoop.enabled" to "true"))
                assertThatThrownBy { runner.execute("SELECT * FROM t") }.hasMessageContaining("1.1")
            }
    }

    @Test fun localPositiveDistributedSql(): Unit = distributedMixedScan(Lane.LOCAL)
    @Test fun postgresPositiveDistributedSql(): Unit = distributedMixedScan(Lane.POSTGRES)
    @Test fun javaQuackPositiveDistributedSql(): Unit = distributedMixedScan(Lane.JAVA_QUACK)
    @Test fun nativeQuackPositiveDistributedSql(): Unit = distributedMixedScan(Lane.NATIVE_QUACK)
    @Test fun mysqlPositiveDistributedSql(): Unit = distributedMixedScan(Lane.MYSQL)

    private fun distributedMixedScan(lane: Lane): Unit = Fixture(lane).use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',2); CREATE TABLE lake.t(i INTEGER,b BLOB,s VARCHAR); " +
            "BEGIN; INSERT INTO lake.t VALUES(1,from_hex('005CFF'),NULL),(2,NULL,'NULL'); " +
            "INSERT INTO lake.t VALUES(3,NULL,''),(4,NULL,'four'),(5,NULL,'five'),(6,NULL,'six'),(7,NULL,'seven'); COMMIT")
        f.runner().use { runner ->
            assertThat(runner.execute("SELECT i FROM t ORDER BY i").materializedRows.map { it.getField(0) })
                .containsExactly(1, 2, 3, 4, 5, 6, 7)
            assertThat(runner.execute("SELECT to_hex(b),s FROM t WHERE i=1").materializedRows.single().fields)
                .containsExactly("005CFF", null)
            assertThat(runner.execute("SELECT s FROM t WHERE i=2").onlyValue).isEqualTo("NULL")
            assertThat(runner.execute("SELECT count(*) FROM t").onlyValue).isEqualTo(7L)
            assertThat(runner.execute("SHOW TABLES").materializedRows.map { it.getField(0) }).contains("t")
            assertThat(runner.execute("DESCRIBE t").materializedRows.map { it.getField(0) }).containsExactly("i", "b", "s")
            assertThatThrownBy { runner.execute("CREATE TABLE forbidden(i INTEGER)") }.hasMessageContaining("read-only")
            assertThatThrownBy { runner.execute("INSERT INTO t VALUES(99,NULL,NULL)") }.hasMessageContaining("read-only")
            assertThat(runner.execute("SELECT count(*) FROM t").onlyValue).isEqualTo(7L)
        }
    }

    @Test
    fun distributedHistoricalPuffinAndLazyCdcScopes(): Unit = distributedHistoricalDeletes(true)
    @Test
    fun distributedHistoricalTaggedParquetAndLazyCdcScopes(): Unit = distributedHistoricalDeletes(false)

    private fun distributedHistoricalDeletes(puffin: Boolean): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('write_deletion_vectors',$puffin); CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(1),(2),(3),(4)")
        f.native("DELETE FROM lake.t WHERE i=2")
        val a = f.snapshot()
        f.native("DELETE FROM lake.t WHERE i=4")
        val b = f.snapshot()
        f.native("CALL ducklake_flush_inlined_data('lake',table_name=>'t')")
        f.runner().use { runner ->
            assertThat(runner.execute("SELECT i FROM t FOR VERSION AS OF $a ORDER BY i").materializedRows.map { it.getField(0) })
                .containsExactly(1, 3, 4)
            assertThat(runner.execute("SELECT count(*) FROM t FOR VERSION AS OF $a").onlyValue).isEqualTo(3L)
            assertThat(runner.execute("SELECT i FROM TABLE(ducklake.system.table_deletions('main','t',$b,$b))").onlyValue).isEqualTo(4)
            assertThat(runner.execute("SELECT i FROM t ORDER BY i").materializedRows.map { it.getField(0) }).containsExactly(1, 3)
        }
    }

    @Test
    fun distributedStableIdEvolutionHistoryAndDecodedCdc(): Unit = Fixture().use { f ->
        f.native("CREATE TABLE lake.t(i INTEGER,s STRUCT(a INTEGER,b BLOB,d DATE)); " +
            "INSERT INTO lake.t VALUES(1,{'a':10,'b':from_hex('005CFF'),'d':DATE '2024-02-29'})")
        val old = f.snapshot()
        f.native("ALTER TABLE lake.t RENAME s.a TO renamed; ALTER TABLE lake.t ALTER s TYPE STRUCT(renamed BIGINT,b BLOB,d DATE); " +
            "ALTER TABLE lake.t ADD COLUMN added VARCHAR DEFAULT 'NULL'")
        f.native("UPDATE lake.t SET s={'renamed':11,'b':from_hex('005CFF'),'d':DATE '2024-02-29'} WHERE i=1")
        val updated = f.snapshot()
        f.runner().use { runner ->
            assertThat(runner.execute("SELECT s.a FROM t FOR VERSION AS OF $old").onlyValue).isEqualTo(10)
            assertThat(runner.execute("SELECT s.renamed,to_hex(s.b),CAST(s.d AS VARCHAR),added FROM t").materializedRows.single().fields)
                .containsExactly(11L, "005CFF", "2024-02-29", "NULL")
            val changes = runner.execute("SELECT change_type,s.renamed,rowid FROM " +
                "TABLE(ducklake.system.table_changes('main','t',$updated,$updated)) ORDER BY change_type").materializedRows.map { it.fields }
            assertThat(changes).containsExactly(listOf<Any?>("update_postimage", 11L, 0L), listOf<Any?>("update_preimage", 10L, 0L))
        }
    }

    @Test
    fun unsupportedPhysicalVariantCannotSilentlyBecomeNull(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',0); CREATE TABLE lake.t(i INTEGER,v VARIANT); " +
            "INSERT INTO lake.t VALUES(1,'literal'::VARIANT)")
        f.runner().use { runner ->
            assertThatThrownBy { runner.execute("SELECT v FROM t") }.isInstanceOf(io.trino.testing.QueryFailedException::class.java)
        }
    }

    @Test
    fun unqualifiedFileInitialDefaultRefusesInsteadOfInventingValue(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',0); CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(1); " +
            "ALTER TABLE lake.t ADD COLUMN added VARCHAR DEFAULT 'NULL'")
        f.runner().use { runner ->
            assertThat(runner.execute("SELECT i FROM t").onlyValue).isEqualTo(1)
            assertThatThrownBy { runner.execute("SELECT added FROM t") }.hasMessageContaining("requires decoded catalog default projection")
        }
    }

    @Test
    fun distributedBudgetFailureDoesNotBecomePartialRowsAndNextPlanRecovers(): Unit = Fixture().use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',0); CREATE TABLE lake.t(i INTEGER); " +
            "INSERT INTO lake.t VALUES(1); INSERT INTO lake.t VALUES(2); " +
            "CREATE TABLE lake.small(i INTEGER); INSERT INTO lake.small VALUES(7)")
        f.runner(mapOf("ducklake.read.max-files" to "1")).use { runner ->
            assertThatThrownBy { runner.execute("SELECT * FROM t") }.hasMessageContaining("read budget exceeded")
            assertThat(runner.execute("SELECT i FROM small").onlyValue).isEqualTo(7)
        }
        // All coordinator/worker pools must close: local native RW cannot coexist with RO pools.
        f.native("INSERT INTO lake.small VALUES(8)")
    }

    @Test
    fun distributedInlineAndCdcLimitsFailLoud(): Unit = Fixture().use { f ->
        f.native("CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(1),(2),(3)")
        val head = f.snapshot()
        f.runner(mapOf("ducklake.read.max-physical-rows" to "2")).use { runner ->
            assertThatThrownBy { runner.execute("SELECT i FROM t") }.hasMessageContaining("physical rows")
        }
        f.runner(mapOf("ducklake.read.max-cdc-events" to "1")).use { runner ->
            assertThatThrownBy { runner.execute("SELECT i FROM TABLE(ducklake.system.table_insertions('main','t',$head,$head))") }
                .hasMessageContaining("CDC events")
            assertThat(runner.execute("SELECT i FROM t ORDER BY i").materializedRows.map { it.getField(0) }).containsExactly(1, 2, 3)
        }
    }

    @Test
    fun postgresQueryFailureAndTerminatedIdleConnectionsRecover(): Unit = Fixture(Lane.POSTGRES).use { f ->
        f.native("CALL lake.set_option('data_inlining_row_limit',0); CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(7)")
        f.runner().use { runner ->
            assertThat(runner.execute("SELECT i FROM t").onlyValue).isEqualTo(7)
            f.metadata("ALTER TABLE ducklake_data_file RENAME COLUMN file_format TO held_format")
            assertThatThrownBy { runner.execute("SELECT i FROM t") }.hasMessageContaining("file_format")
            f.metadata("ALTER TABLE ducklake_data_file RENAME COLUMN held_format TO file_format")
            assertThat(runner.execute("SELECT i FROM t").onlyValue).isEqualTo(7)
            f.metadata("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")
            // Hikari may detect an idle kill on borrow, or the next statement may fail once.
            // No connector retry is added: each attempt below is a separate user query.
            var recovered = false
            var failures = 0
            repeat(4) {
                if (!recovered) {
                    val result = runCatching { runner.execute("SELECT i FROM t").onlyValue }
                    if (result.isSuccess) { assertThat(result.getOrThrow()).isEqualTo(7); recovered = true }
                    else {
                        failures++
                        assertThat(result.exceptionOrNull()).isInstanceOf(io.trino.testing.QueryFailedException::class.java)
                            .hasMessageContaining("connection")
                    }
                }
            }
            assertThat(recovered).isTrue()
            println("R07 terminated-idle connection recovery: $failures failed user queries before recovery")
        }
    }

    @Test
    fun metadataTransactionPinsLogicalHeadAcrossNativePublication(): Unit = Fixture(Lane.POSTGRES).use { f ->
        f.native("CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(7)")
        f.config.setEnableFinal11Reads(true)
        f.reader().use { c ->
            val factory = DucklakeMetadataFactory(c, DucklakeTypeConverter(TESTING_TYPE_MANAGER), DucklakeSnapshotResolver(c, f.config),
                io.airlift.json.JsonCodec.jsonCodec(dev.brikk.ducklake.catalog.DucklakeWriteFragment::class.java),
                io.airlift.json.JsonCodec.jsonCodec(dev.brikk.ducklake.catalog.DucklakeDeleteFragment::class.java),
                DucklakePathResolver(c, f.config), f.config)
            val pinned = DucklakeReadOnlyMetadata.wrap(factory.create(), c, 100)
            val session = io.trino.testing.TestingConnectorSession.builder()
                .setPropertyMetadata(DucklakeSessionProperties().getSessionProperties()).build()
            // The logical head pins on the first actual metadata read, never inside beginTransaction.
            assertThat(pinned.listTables(session, Optional.of("main")).map { it.tableName }).containsExactly("t")
            f.native("CREATE TABLE lake.later(i INTEGER)")
            assertThat(pinned.listTables(session, Optional.of("main")).map { it.tableName }).containsExactly("t")
            val fresh = DucklakeReadOnlyMetadata.wrap(factory.create(), c, 100)
            assertThat(fresh.listTables(session, Optional.of("main")).map { it.tableName }).containsExactlyInAnyOrder("t", "later")
        }
    }

    @Test
    fun cancelledSqlWhileMetadataIsLockedReleasesAfterAbortAndRecovers(): Unit = Fixture(Lane.POSTGRES).use { f ->
        f.native("CREATE TABLE lake.t(i INTEGER); INSERT INTO lake.t VALUES(7)")
        f.runner().use { runner ->
            val executor = java.util.concurrent.Executors.newFixedThreadPool(2)
            try {
                DriverManager.getConnection(f.url, f.config.getCatalogDatabaseUser(), f.config.getCatalogDatabasePassword()).use { lock ->
                    lock.autoCommit = false
                    lock.createStatement().use { it.execute("LOCK TABLE ducklake_snapshot IN ACCESS EXCLUSIVE MODE") }
                    try {
                        val future = executor.submit<Any> { runner.execute("SELECT i AS r07_cancel_marker FROM t") }
                        var blocked = false
                        for (attempt in 1..100) {
                            if (f.blockedMetadataQueries() > 0) { blocked = true; break }
                            java.util.concurrent.locks.LockSupport.parkNanos(50_000_000L)
                        }
                        check(blocked) { "metadata query never reached the controlled lock" }
                        val query = runner.coordinator.dispatchManager.queries.single { it.query.contains("r07_cancel_marker") }
                        // Keep the fixture controller able to release the lock even if a future
                        // cancellation regression blocks the engine's own transaction monitor.
                        executor.submit { runner.coordinator.dispatchManager.cancelQuery(query.queryId) }
                            .get(10, java.util.concurrent.TimeUnit.SECONDS)
                        assertThatThrownBy { future.get(10, java.util.concurrent.TimeUnit.SECONDS) }
                            .hasCauseInstanceOf(io.trino.testing.QueryFailedException::class.java)
                        println("R07 cancelled SQL: JDBC metadata still blocked before lock release = ${f.blockedMetadataQueries() > 0}")
                    } finally { lock.rollback() }
                }
                assertThat(runner.execute("SELECT i FROM t").onlyValue).isEqualTo(7)
            } finally { executor.shutdownNow() }
        }
    }

    private data class Plan(val handle: DucklakeTableHandle, val splits: List<ConnectorSplit>)

    private enum class Lane { LOCAL, POSTGRES, NATIVE_QUACK, JAVA_QUACK, MYSQL }

    private class Fixture(val lane: Lane = Lane.LOCAL) : AutoCloseable {
        val root = Files.createTempDirectory(Path.of("/tmp/opencode"), "trino-r07-")
        val db = root.resolve("metadata.db")
        val data = Files.createDirectories(root.resolve("data"))
        val pg = if (lane == Lane.POSTGRES) TestingDucklakePostgreSqlCatalogServer() else null
        val mysql = if (lane == Lane.MYSQL) TestingDucklakeMySqlCatalogServer() else null
        val quack = if (lane in setOf(Lane.NATIVE_QUACK, Lane.JAVA_QUACK)) TestingDucklakeDuckDbQuackCatalogServer() else null
        val url = when (lane) {
            Lane.LOCAL -> "jdbc:duckdb:$db"
            Lane.POSTGRES -> pg!!.getJdbcUrl()
            Lane.MYSQL -> mysql!!.getJdbcUrl()
            Lane.JAVA_QUACK -> "jdbc:quack://${quack!!.getHost()}:${quack.getMappedPort()}"
            Lane.NATIVE_QUACK -> "jdbc:duckdb:quack://${quack!!.getHost()}:${quack.getMappedPort()}?metadata_catalog=trial_meta"
        }
        val config = DucklakeConfig().setCatalogDatabaseUrl(url).setDataPath(data.toString()).setMaxCatalogConnections(1)
            .setCatalogDatabaseUser(pg?.getUser() ?: mysql?.getUser())
            .setCatalogDatabasePassword(pg?.getPassword() ?: mysql?.getPassword() ?: quack?.getToken())
        private var initialized = false

        fun native(sql: String) = DriverManager.getConnection("jdbc:duckdb:").use { c -> c.createStatement().use { s ->
            val jar = Path.of(Class.forName("org.duckdb.DuckDBDriver").protectionDomain.codeSource.location.toURI())
            val hash = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(jar)).joinToString("") { "%02x".format(it) }
            check(hash == "5170daa90a43296705127ef3c80834ba1cd275ec9d765a9f7eee346ab0087186")
            s.execute("INSTALL ducklake; LOAD ducklake")
            s.executeQuery("PRAGMA version").use { r -> check(r.next()); check(r.getString(2) == "d54cbbe3df") }
            s.executeQuery("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake'").use { r ->
                check(r.next()); check(r.getString(1) == "417a9117f5")
            }
            val extension = when (lane) { Lane.POSTGRES -> "postgres"; Lane.MYSQL -> "mysql"; Lane.LOCAL -> null; else -> "quack" }
            if (extension != null) {
                s.execute("INSTALL $extension; LOAD $extension")
                val name = if (extension == "quack") "quack" else "${extension}_scanner"
                val expected = when (extension) { "postgres" -> "c0f5a3c0c4"; "mysql" -> "f7fc0d4271"; else -> "f96420ed46" }
                s.executeQuery("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='$name'").use { r ->
                    check(r.next()); check(r.getString(1) == expected)
                }
            }
            if (quack != null) s.execute("CREATE SECRET(TYPE quack,TOKEN '${quack.getToken()}')")
            s.executeQuery("SELECT current_setting('allow_unsigned_extensions')").use { r -> check(r.next()); check(!r.getBoolean(1)) }
            val target = pg?.getDuckDbAttachUri() ?: mysql?.getDuckDbAttachUri() ?: quack?.getDucklakeAttachUri() ?: "ducklake:$db"
            if (mysql != null && !initialized) {
                s.execute("ATTACH '$target' AS seed(DATA_PATH '$data',DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE)")
                s.execute("DETACH seed")
            }
            val extra = if (mysql != null) ",METADATA_SCHEMA 'ducklake'" else ",METADATA_CATALOG 'meta'"
            s.execute("ATTACH '$target' AS lake(DATA_PATH '$data',DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE$extra)")
            s.execute(sql)
            s.execute("DETACH lake")
            initialized = true
        } }

        fun snapshot(): Long = DriverManager.getConnection("jdbc:duckdb:$db").use { c -> c.createStatement().use { s ->
            s.executeQuery("SELECT max(snapshot_id) FROM ducklake_snapshot").use { r -> check(r.next()); r.getLong(1) }
        } }

        fun reader() = JdbcDucklakeCatalog(config.toCatalogConfig().apply { enableFinal11Reads = true })

        fun runner(extra: Map<String, String> = emptyMap()): DistributedQueryRunner {
            val runner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog("ducklake").setSchema("main").build())
                .setWorkerCount(2).addCoordinatorProperty("node-scheduler.include-coordinator", "false").build()
            try {
                runner.installPlugin(DucklakePlugin())
                val properties = mutableMapOf("ducklake.catalog.database-url" to url, "ducklake.data-path" to data.toString(),
                    "ducklake.catalog.max-connections" to "1", "ducklake.experimental.enable-final-1.1-reads" to "true", "fs.hadoop.enabled" to "true")
                config.getCatalogDatabaseUser()?.let { properties["ducklake.catalog.database-user"] = it }
                config.getCatalogDatabasePassword()?.let { properties["ducklake.catalog.database-password"] = it }
                runner.createCatalog("ducklake", "ducklake", properties + extra)
                return runner
            } catch (e: Throwable) { runner.close(); throw e }
        }

        fun metadata(sql: String) = DriverManager.getConnection(if (pg == null) "jdbc:duckdb:$db" else url,
            config.getCatalogDatabaseUser(), config.getCatalogDatabasePassword()).use { c ->
            c.createStatement().use { it.execute(sql) }
        }

        fun blockedMetadataQueries(): Long = DriverManager.getConnection(url,
            config.getCatalogDatabaseUser(), config.getCatalogDatabasePassword()).use { c -> c.createStatement().use { s ->
                s.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' " +
                    "AND query LIKE '%ducklake_snapshot%' AND pid<>pg_backend_pid()").use { r -> check(r.next()); r.getLong(1) }
            } }

        fun plan(c: JdbcDucklakeCatalog, snapshot: Long? = null, constraint: Constraint = Constraint.alwaysTrue()): Plan = c.readSession(Supplier {
            val id = snapshot ?: c.currentSnapshotId
            val table = c.getTable("main", "t", id)!!
            val handle = DucklakeTableHandle("main", "t", table.tableId, id)
            val manager = DucklakeSplitManager(c, config, DucklakePathResolver(c, config), NoopSplitAffinityProvider())
            val source = manager.getSplits(null, SESSION, handle, mutableSetOf(), constraint)
            source.use {
                val splits = mutableListOf<ConnectorSplit>()
                while (!source.isFinished) splits.addAll(source.getNextBatch(128, DynamicFilterSnapshot.EMPTY).get())
                Plan(handle, splits)
            }
        })

        fun column(c: JdbcDucklakeCatalog, plan: Plan, name: String, type: io.trino.spi.type.Type): DucklakeColumnHandle =
            c.readSession(Supplier {
                val col = c.getTableColumns(plan.handle.tableId, plan.handle.snapshotId).single { it.columnName == name }
                DucklakeColumnHandle(col.columnId, name, type, true)
            })

        fun provider(c: JdbcDucklakeCatalog) = DucklakePageSourceProvider(LocalFileSystemFactory(Path.of("/")),
            FileFormatDataSourceStats(), ParquetReaderConfig().toParquetReaderOptions(), c, config)

        fun scan(c: JdbcDucklakeCatalog, plan: Plan, columns: List<DucklakeColumnHandle>): List<List<Any?>> = c.readSession(Supplier {
            val provider = provider(c)
            plan.splits.flatMap { split -> drain(provider.createPageSource(null, SESSION, split, plan.handle,
                Optional.empty(), columns, DynamicFilter.EMPTY), columns) }
        })

        fun deletions(c: JdbcDucklakeCatalog, from: Long, to: Long): List<List<Any?>> = c.readSession(Supplier {
            val table = c.getTable("main", "t", to)!!
            val id = c.getTableColumns(table.tableId, to).single { it.columnName == "i" }
            val columns = listOf(DucklakeColumnHandle(id.columnId, "i", INTEGER, true))
            val handle = ChangeFeedTableHandle(ChangeFeedType.DELETIONS, "main", "t", table.tableId, from, to, columns)
            val output = listOf(ChangeFeedColumns.SNAPSHOT_ID) + columns
            drain(provider(c).createPageSource(null, SESSION, ChangeFeedSplit(table.tableId), handle,
                Optional.empty(), output, DynamicFilter.EMPTY), output)
        })

        fun changes(c: JdbcDucklakeCatalog, from: Long, to: Long): List<List<Any?>> = c.readSession(Supplier {
            val table = c.getTable("main", "t", to)!!
            val id = c.getTableColumns(table.tableId, to).single { it.columnName == "i" }
            val columns = listOf(DucklakeColumnHandle(id.columnId, "i", INTEGER, true))
            val handle = ChangeFeedTableHandle(ChangeFeedType.CHANGES, "main", "t", table.tableId, from, to, columns)
            val output = ChangeFeedColumns.outputColumns(ChangeFeedType.CHANGES, columns)
            drain(provider(c).createPageSource(null, SESSION, ChangeFeedSplit(table.tableId), handle,
                Optional.empty(), output, DynamicFilter.EMPTY), output)
        })

        private fun drain(source: ConnectorPageSource, columns: List<DucklakeColumnHandle>): List<List<Any?>> = source.use {
            buildList {
                while (!source.isFinished) {
                    val page = source.nextSourcePage ?: continue
                    for (position in 0 until page.positionCount) add(columns.mapIndexed { channel, column ->
                        column.columnType.getObjectValue(page.getBlock(channel), position)
                    })
                }
            }
        }

        override fun close() {
            try { pg?.close(); mysql?.close(); quack?.close() }
            finally { Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } } }
        }
    }
}
