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
import dev.brikk.ducklake.catalog.DucklakeColumn
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.TrinoFileSystemFactory
import io.trino.filesystem.TrinoInputFile
import io.trino.filesystem.memory.MemoryFileSystem
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.TypeUtils.writeNativeValue
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import org.apache.parquet.format.CompressionCodec
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.IOException
import java.lang.reflect.Proxy
import java.util.Optional
import java.util.function.Supplier

class TestDucklakeProjectionFailures {
    @Test
    fun malformedMissingColumnDefaultFails() {
        val fixture = Fixture()
        assertThatThrownBy { fixture.read(fixture.split, fixture.column.copy(initialDefault = "not-a-number")) }
            .isInstanceOfSatisfying(TrinoException::class.java) {
                assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                assertThat(it.cause).isInstanceOf(NumberFormatException::class.java)
            }
            .hasMessageContaining("initial_default").hasMessageContaining("added")
    }

    @Test
    fun malformedMissingColumnPartitionFails() {
        val fixture = Fixture()
        assertThatThrownBy { fixture.read(fixture.split.copy(partitionValuesByColumnId = mapOf(2L to "bad")), fixture.column) }
            .isInstanceOfSatisfying(TrinoException::class.java) {
                assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                assertThat(it.cause).isInstanceOf(NumberFormatException::class.java)
            }
            .hasMessageContaining("partition value").hasMessageContaining("added")
    }

    @Test
    fun malformedInlinedEraDefaultFails() {
        val fixture = Fixture()
        assertThatThrownBy { fixture.read(DucklakeInlinedSplit(1, 1, 2), fixture.column.copy(initialDefault = "bad")) }
            .isInstanceOfSatisfying(TrinoException::class.java) {
                assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                assertThat(it.cause).isInstanceOf(NumberFormatException::class.java)
            }
            .hasMessageContaining("inlined initial_default").hasMessageContaining("added")
    }

    @Test
    fun temporalDecimalAndBooleanParseFailuresHaveAnErrorCode() {
        val fixture = Fixture()
        for ((type, value) in listOf(DATE to "invalid-date", DecimalType.createDecimalType(10, 2) to "1.234", BOOLEAN to "unknown")) {
            assertThatThrownBy { fixture.read(fixture.split, fixture.column.copy(columnType = type, initialDefault = value)) }
                .isInstanceOfSatisfying(TrinoException::class.java) {
                    assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                    assertThat(it.cause).isNotNull()
                }
                .hasMessageContaining("initial_default")
        }
    }

    @Test
    fun absentValuesRemainNullAndPartitionStillOverridesDefault() {
        val fixture = Fixture()
        assertThat(fixture.read(fixture.split, fixture.column)).containsExactly(null)
        assertThat(fixture.read(fixture.split, fixture.column.copy(initialDefault = "42"))).containsExactly(42L)
        assertThat(fixture.read(fixture.split.copy(partitionValuesByColumnId = mapOf(2L to "7")),
            fixture.column.copy(initialDefault = "invalid-but-overridden"))).containsExactly(7L)
        assertThat(fixture.read(DucklakeInlinedSplit(1, 1, 2), fixture.column.copy(initialDefault = "42"))).containsExactly(42L)
    }

    @Test
    fun presentInlinedColumnKeepsStoredNullInsteadOfApplyingDefault() {
        val fixture = Fixture()
        fixture.eraContainsColumn = true
        assertThat(fixture.read(DucklakeInlinedSplit(1, 1, 2), fixture.column.copy(initialDefault = "invalid-but-unused"))).containsExactly(null)
    }

    @Test
    fun pageSourceRuntimeAndIoFailuresHaveAnErrorCodeAndOriginalCause() {
        val fixture = Fixture()
        for (failure in listOf(IllegalStateException("read failed"), IOException("read failed"))) {
            assertThatThrownBy { fixture.read(fixture.split, fixture.column, fixture.failingFileSystem(failure)) }
                .isInstanceOfSatisfying(TrinoException::class.java) {
                    assertThat(it.errorCode).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode())
                }
                .hasCause(failure).hasMessageContaining(fixture.split.dataFilePath)
        }
    }

    @Test
    fun pageSourcePreservesTrinoFailure() {
        val fixture = Fixture()
        val failure = TrinoException(NOT_SUPPORTED, "physical field unsupported")
        assertThatThrownBy { fixture.read(fixture.split, fixture.column, fixture.failingFileSystem(failure)) }.isSameAs(failure)
    }

    private class Fixture {
        var eraContainsColumn = false
        val column = DucklakeColumnHandle(2, "added", BIGINT, true)
        private val fileSystem = MemoryFileSystem()
        private val location = Location.of("memory:///data.parquet")
        private val schema = ParquetSchemaConverter(listOf(BIGINT), listOf("stored"), false, false)
        init {
            ParquetWriter(fileSystem.newOutputFile(location).create(), schema.messageType, schema.primitiveTypes,
                ParquetWriterOptions.builder().build(), CompressionCodec.UNCOMPRESSED, "test", Optional.empty(), Optional.empty()).use {
                it.write(Page(writeNativeValue(BIGINT, 1L)))
            }
        }
        val split = DucklakeSplit(location.toString(), emptyList(), 0L, 1L, fileSystem.newInputFile(location).length(),
            "parquet", TupleDomain.all(), 0L, emptyMap(), emptyMap(), emptyMap(), emptySet(), Optional.empty())
        private val catalog = Proxy.newProxyInstance(
            DucklakeCatalog::class.java.classLoader, arrayOf(DucklakeCatalog::class.java)) { _, method, args ->
            when (method.name) {
                "readSession" -> (args[0] as Supplier<*>).get()
                "getSpecVersion" -> "1.0"
                "getAllColumnsWithParentage" -> if (eraContainsColumn)
                    listOf(DucklakeColumn(2, 2, null, 1, 1, "added", "BIGINT", true, null)) else emptyList<DucklakeColumn>()
                "getTableColumns" -> listOf(DucklakeColumn(2, 2, null, 1, 1, "added", "BIGINT", true, null))
                "readInlinedData" -> listOf(listOf(null))
                "resolveSchemaVersionSnapshot" -> 1L
                else -> error("Unexpected catalog call ${method.name}")
            }
        } as DucklakeCatalog

        fun failingFileSystem(failure: Exception): TrinoFileSystem = object : TrinoFileSystem by fileSystem {
            override fun newInputFile(location: Location): TrinoInputFile {
                val input = fileSystem.newInputFile(location)
                return object : TrinoInputFile by input {
                    override fun newInput(): io.trino.filesystem.TrinoInput = throw failure
                }
            }
        }

        fun read(split: ConnectorSplit, column: DucklakeColumnHandle, fs: TrinoFileSystem = fileSystem): List<Long?> {
            val provider = DucklakePageSourceProvider(TrinoFileSystemFactory { fs }, FileFormatDataSourceStats(),
                ParquetReaderConfig().toParquetReaderOptions(), catalog, DucklakeConfig(), DucklakeTypeConverter(TESTING_TYPE_MANAGER))
            return provider.createPageSource(null, SESSION, split, DucklakeTableHandle("main", "t", 1, 2),
                Optional.empty(), listOf(column), DynamicFilter.EMPTY).use { source ->
                buildList {
                    while (!source.isFinished) {
                        val block = source.nextSourcePage?.getBlock(0) ?: continue
                        for (position in 0 until block.positionCount) {
                            add(if (block.isNull(position)) null else BIGINT.getLong(block, position))
                        }
                    }
                }
            }
        }
    }
}
