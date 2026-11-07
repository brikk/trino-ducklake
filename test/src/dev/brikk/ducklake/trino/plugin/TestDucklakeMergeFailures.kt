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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import io.airlift.json.JsonCodec
import io.airlift.slice.Slice
import io.airlift.slice.Slices.utf8Slice
import io.trino.filesystem.memory.MemoryFileSystem
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER
import io.trino.spi.connector.ConnectorPageSink
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.TypeUtils.writeNativeValue
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import org.apache.parquet.format.FileMetaData
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.IOException
import java.lang.reflect.Proxy
import java.util.Optional
import java.util.concurrent.CompletableFuture

class TestDucklakeMergeFailures {
    @Test
    fun footerSerializationFailureDoesNotBecomeZero() {
        assertThatThrownBy { Fixture().sink().footerSizeOf(FileMetaData()) }
            .isInstanceOfSatisfying(TrinoException::class.java) {
                assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                assertThat(it.cause).isInstanceOf(IOException::class.java)
            }
            .hasMessageContaining("delete file Parquet footer")
    }

    @Test
    fun dataFileFooterSerializationFailureDoesNotBecomeZero() {
        assertThatThrownBy { parquetFooterSize(FileMetaData(), "data file") }
            .isInstanceOfSatisfying(TrinoException::class.java) {
                assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                assertThat(it.cause).isInstanceOf(IOException::class.java)
            }
            .hasMessageContaining("data file Parquet footer")
    }

    @Test
    fun directAndFutureTrinoFailuresArePreserved() {
        val fixture = Fixture()
        val failure = TrinoException(NOT_SUPPORTED, "insert rejected")
        assertThatThrownBy { fixture.sink { throw failure }.finish() }.isSameAs(failure)
        assertThatThrownBy { fixture.sink { CompletableFuture.failedFuture(failure) }.finish() }.isSameAs(failure)
        val lineage = fixture.pageSink { CompletableFuture.failedFuture(failure) }
        assertThatThrownBy { fixture.sink(lineage = lineage).finish() }.isSameAs(failure)
    }

    @Test
    fun nonTrinoInsertFailuresHaveAnErrorCodeAndOriginalCause() {
        val fixture = Fixture()
        val failure = IllegalStateException("writer failed")
        for (finish in listOf<() -> CompletableFuture<Collection<Slice>>>(
            { throw failure }, { CompletableFuture.failedFuture(failure) })) {
            assertThatThrownBy { fixture.sink(finish = finish).finish() }
                .isInstanceOfSatisfying(TrinoException::class.java) {
                    assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                }
                .hasCause(failure)
        }
    }

    @Test
    fun interruptedInsertFinishKeepsTheInterruptFlag() {
        val sink = Fixture().sink { CompletableFuture() }
        Thread.currentThread().interrupt()
        try {
            assertThatThrownBy { sink.finish() }
                .isInstanceOfSatisfying(TrinoException::class.java) {
                    assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                    assertThat(it.cause).isInstanceOf(InterruptedException::class.java)
                }
            assertThat(Thread.currentThread().isInterrupted).isTrue()
        }
        finally {
            Thread.interrupted()
        }
    }

    @Test
    fun sinkTagsDeletesAndBothInsertSinks() {
        val fixture = Fixture()
        val insert = fixture.writeCodec.toJson(fixture.insert)
        val sink = fixture.sink(
            lineage = fixture.pageSink { CompletableFuture.completedFuture(listOf(utf8Slice(insert))) },
            finish = { CompletableFuture.completedFuture(listOf(utf8Slice(insert))) })
        sink.storeMergedRows(Page(
            BIGINT.createNullBlock(),
            writeNativeValue(TINYINT, DELETE_OPERATION_NUMBER.toLong()),
            writeNativeValue(INTEGER, 0L),
            DucklakeMergeRowId.block(7, writeNativeValue(BIGINT, 0L))))

        val fragments = sink.finish().get()
        assertThat(fragments.map { DucklakeMergeFragment.fromSlice(it).kind }).containsExactly(
            DucklakeMergeFragment.Kind.DELETE, DucklakeMergeFragment.Kind.INSERT, DucklakeMergeFragment.Kind.INSERT)
        fixture.finishMerge(fragments)
        assertThat(fixture.commits).containsExactly("commitMerge")
        assertThat(fixture.committedDeletes).hasSize(1)
        assertThat(fixture.committedInserts).containsExactly(fixture.insert, fixture.insert)
    }

    @Test
    fun coordinatorDispatchesByKindNotPath() {
        val fixture = Fixture()
        val deletion = DucklakeDeleteFragment(7, "arbitrary-name.parquet", 1, 100, 20, 1, "parquet")
        fixture.finishMerge(listOf(
            DucklakeMergeFragment(DucklakeMergeFragment.Kind.DELETE, fixture.deleteCodec.toJson(deletion)).toSlice(),
            DucklakeMergeFragment(DucklakeMergeFragment.Kind.INSERT, fixture.writeCodec.toJson(fixture.insert)).toSlice()))
        assertThat(fixture.commits).containsExactly("commitMerge")
        assertThat(fixture.committedDeletes).containsExactly(deletion)
        assertThat(fixture.committedInserts).containsExactly(fixture.insert)
    }

    @Test
    fun malformedFragmentsFailBeforeAnyCommit() {
        val fixture = Fixture()
        val malformed = listOf(
            "not json", "{}", """{"kind":"UNKNOWN","payload":"{}"}""",
            """{"kind":null,"payload":"{}"}""", """{"kind":"INSERT","payload":null}""",
            """{"kind":"DELETE","payload":"not json"}""",
            """{"kind":"INSERT","payload":"not json"}""")
        val valid = DucklakeMergeFragment(DucklakeMergeFragment.Kind.INSERT, fixture.writeCodec.toJson(fixture.insert)).toSlice()
        for (json in malformed) {
            assertThatThrownBy { fixture.finishMerge(listOf(valid, utf8Slice(json))) }
                .isInstanceOfSatisfying(TrinoException::class.java) {
                    assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                    assertThat(it.cause).isNotNull()
                }
                .hasMessageContaining("deserialize merge fragment")
        }
        assertThat(fixture.commits).isEmpty()
    }

    private class Fixture {
        val writeCodec = JsonCodec.jsonCodec(DucklakeWriteFragment::class.java)
        val deleteCodec = JsonCodec.jsonCodec(DucklakeDeleteFragment::class.java)
        val insert = DucklakeWriteFragment("ducklake-delete-is-an-insert.parquet", 100, 20, 1, emptyList())
        val handle = DucklakeMergeTableHandle(
            DucklakeTableHandle("main", "t", 1, 1),
            DucklakeWritableTableHandle("main", "t", 1, listOf(DucklakeColumnHandle(1, "id", BIGINT, true)),
                emptyList(), "memory:///table", Optional.empty(), DucklakeTemporalPartitionEncoding.CALENDAR, "parquet"),
            listOf(DucklakeMergeTableHandle.DataFileRange(7, 0, 1, emptyList(), "memory:///table/data.parquet")))
        val commits = mutableListOf<String>()
        var committedDeletes: List<DucklakeDeleteFragment> = emptyList()
        var committedInserts: List<DucklakeWriteFragment> = emptyList()
        private val catalog = Proxy.newProxyInstance(
            DucklakeCatalog::class.java.classLoader, arrayOf(DucklakeCatalog::class.java)) { _, method, args ->
            when (method.name) {
                "commitMerge" -> {
                    commits.add(method.name)
                    committedDeletes = (args[1] as List<*>).map { it as DucklakeDeleteFragment }
                    committedInserts = (args[2] as List<*>).map { it as DucklakeWriteFragment }
                    2L
                }
                else -> error("Unexpected catalog call ${method.name}")
            }
        } as DucklakeCatalog
        val metadata = DucklakeMetadata(catalog, DucklakeTypeConverter(TESTING_TYPE_MANAGER),
            DucklakeSnapshotResolver(catalog, null, null), writeCodec, deleteCodec,
            DucklakePathResolver(catalog, DucklakeConfig()), DucklakeTemporalPartitionEncoding.CALENDAR)

        fun finishMerge(fragments: Collection<Slice>) {
            metadata.finishMerge(SESSION, handle, emptyList(), fragments, emptyList())
        }

        fun pageSink(finish: () -> CompletableFuture<Collection<Slice>>): ConnectorPageSink =
            Proxy.newProxyInstance(ConnectorPageSink::class.java.classLoader, arrayOf(ConnectorPageSink::class.java)) { _, method, _ ->
                when (method.name) {
                    "finish" -> finish()
                    "getMemoryUsage" -> 0L
                    else -> error("Unexpected insert sink call ${method.name}")
                }
            } as ConnectorPageSink

        fun sink(
            lineage: ConnectorPageSink? = null,
            finish: () -> CompletableFuture<Collection<Slice>> = { CompletableFuture.completedFuture(emptyList()) }): DucklakeMergeSink =
            DucklakeMergeSink(handle, MemoryFileSystem(), deleteCodec, ParquetWriterOptions.builder().build(),
                ParquetReaderConfig().toParquetReaderOptions(), FileFormatDataSourceStats(), "test", false, pageSink(finish), lineage)
    }
}
