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

import io.airlift.slice.Slice
import io.trino.parquet.ParquetTypeUtils
import io.trino.spi.connector.ConnectorMergeSink
import io.trino.spi.connector.ConnectorMergeTableHandle
import io.trino.spi.connector.ConnectorMetadata
import io.trino.spi.connector.ConnectorPageSinkId
import io.trino.spi.connector.ConnectorPageSinkProvider
import io.trino.spi.connector.ConnectorPageSourceProviderFactory
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTableCredentials
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.statistics.ComputedStatistics
import io.trino.spi.type.Type
import org.apache.parquet.io.ColumnIO
import java.util.Optional

/** Compile-time adapters: only the selected server's SPI is present in the plugin. */
abstract class DucklakeMetadataSpi : ConnectorMetadata {
    protected abstract fun finishDucklakeMerge(mergeTableHandle: ConnectorMergeTableHandle, fragments: Collection<Slice>)

    override fun finishMerge(session: ConnectorSession, mergeTableHandle: ConnectorMergeTableHandle,
        sourceTableHandles: List<ConnectorTableHandle>, fragments: Collection<Slice>,
        computedStatistics: Collection<ComputedStatistics>) =
        finishDucklakeMerge(mergeTableHandle, fragments)
}

abstract class DucklakePageSinkProviderSpi : ConnectorPageSinkProvider {
    protected abstract fun createDucklakeMergeSink(session: ConnectorSession, mergeHandle: ConnectorMergeTableHandle,
        reportMemory: (Long) -> Unit): ConnectorMergeSink

    override fun createMergeSink(transactionHandle: ConnectorTransactionHandle, session: ConnectorSession,
        mergeHandle: ConnectorMergeTableHandle, tableCredentials: Optional<ConnectorTableCredentials>,
        pageSinkId: ConnectorPageSinkId): ConnectorMergeSink =
        createDucklakeMergeSink(session, mergeHandle) { }
}

abstract class DucklakePageSourceProviderFactorySpi : ConnectorPageSourceProviderFactory {
    protected abstract fun createDucklakePageSourceProvider(reportMemory: (Long) -> Unit): DucklakePageSourceProvider

    override fun createPageSourceProvider(): DucklakePageSourceProvider = createDucklakePageSourceProvider { }
}

internal object DucklakeTrinoApi {
    @Suppress("UnusedParameter") // 484's equivalent helper needs the logical element type.
    fun arrayElementColumn(elementType: Type, column: ColumnIO): ColumnIO = ParquetTypeUtils.getArrayElementColumn(column)
}
