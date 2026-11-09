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

import com.google.inject.Inject
import dev.brikk.ducklake.catalog.DucklakeCatalog
import io.airlift.log.Logger
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.TrinoFileSystemFactory
import io.trino.parquet.ParquetReaderOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.ConnectorPageSourceProvider
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.ConnectorTableCredentials
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.connector.EmptyPageSource
import io.trino.spi.predicate.TupleDomain
import java.io.IOException
import java.util.Optional

/**
 * PageSourceProvider for Ducklake connector: routes each split to its page source. Data files are
 * read through Trino's ParquetPageSource ([DucklakeParquetPageSourceFactory]).
 */
class DucklakePageSourceProvider @Inject constructor(
        private val fileSystemFactory: TrinoFileSystemFactory,
        fileFormatDataSourceStats: FileFormatDataSourceStats,
        parquetReaderOptions: ParquetReaderOptions,
        private val catalog: DucklakeCatalog,
        ducklakeConfig: DucklakeConfig,
        typeConverter: DucklakeTypeConverter,
        reportSharedMemory: (Long) -> Unit = {})
        : ConnectorPageSourceProvider
{
    private val readStrategy = DucklakeReadStrategy.of(ducklakeConfig)
    private val columnTrees = DucklakeColumnTreeCache(catalog, reportSharedMemory)
    private val inlinedRows = DucklakeInlinedRowCodec.Selector(catalog, columnTrees)
    private val parquetPageSources = DucklakeParquetPageSourceFactory(
            catalog, typeConverter, columnTrees, readStrategy, parquetReaderOptions, fileFormatDataSourceStats)
    private val inlinedPageSources = DucklakeInlinedPageSourceFactory(catalog, typeConverter, readStrategy, inlinedRows)
    private val metadataTablePageSources = DucklakeMetadataTablePageSource(catalog)

    private val changeFeedPlanner = DucklakeChangeFeedPlanner(
            catalog,
            DucklakePathResolver(catalog, ducklakeConfig),
            fileSystemFactory,
            parquetReaderOptions,
            fileFormatDataSourceStats,
            readStrategy,
            inlinedRows,
            parquetPageSources,
            DucklakeReadWorkLimits(ducklakeConfig))

    override fun createPageSource(
            transaction: ConnectorTransactionHandle?,
            session: ConnectorSession,
            split: ConnectorSplit,
            table: ConnectorTableHandle,
            tableCredentials: Optional<ConnectorTableCredentials>,
            columns: List<ColumnHandle>,
            dynamicFilter: DynamicFilter): ConnectorPageSource
    = catalog.queryReadSession(session.queryId) {
        readStrategy.pinReadSession(catalog)
        specialSplitPageSource(session, split, table, columns)?.let { return@queryReadSession it }

        val ducklakeSplit = split as DucklakeSplit

        val dynamicFilterPredicate: TupleDomain<DucklakeColumnHandle> = dynamicFilter.currentPredicate
                .transformKeys(DucklakeColumnHandle::class.java::cast)
        val effectivePredicate: TupleDomain<DucklakeColumnHandle> = ducklakeSplit.fileStatisticsDomain
                .intersect(dynamicFilterPredicate)

        if (effectivePredicate.isNone) {
            return@queryReadSession EmptyPageSource()
        }

        val ducklakeColumns: List<DucklakeColumnHandle> = columns.map { it as DucklakeColumnHandle }

        // Split the hidden virtuals: CONSTANT virtuals ($path, $snapshot_id) are injected by the
        // outer wrapper below; POSITIONAL virtuals ($file_row_number, $row_id) stay in the column
        // list passed to the readers so they are injected in-pipeline before delete filtering
        // (alongside the MERGE $row_id handle, which is not a VirtualKind but is positional too).
        val sourceColumns: List<DucklakeColumnHandle> = ducklakeColumns.filterNot { col ->
            val kind: VirtualKind? = col.virtualKind()
            kind != null && !kind.perRow
        }

        log.debug("Creating page source for file: %s", ducklakeSplit.dataFilePath)

        try {
            val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
            val delegate: ConnectorPageSource = parquetPageSources.create(
                    ducklakeSplit, table, sourceColumns, effectivePredicate, fileSystem)
            DucklakeVirtualColumns.injectConstantVirtuals(delegate, ducklakeColumns, { !it.perRow }) { kind ->
                DucklakeVirtualColumns.dataFileVirtualBlock(kind, ducklakeSplit)
            }
        }
        catch (e: IOException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create page source for file: ${ducklakeSplit.dataFilePath}", e)
        }
    }

    /** Page sources for the non-data-file splits; null for ordinary data-file splits ([DucklakeSplit]). */
    private fun specialSplitPageSource(
            session: ConnectorSession,
            split: ConnectorSplit,
            table: ConnectorTableHandle,
            columns: List<ColumnHandle>): ConnectorPageSource? = when (split) {
        is DucklakeMetadataSplit -> metadataTablePageSources.create(split, columns.map { it as DucklakeColumnHandle })
        is DucklakeInlinedSplit -> inlinedPageSources.create(split, columns.map { it as DucklakeColumnHandle })
        is ChangeFeedSplit -> changeFeedPlanner.create(session, table as ChangeFeedTableHandle, columns)
        else -> null
    }

    private companion object {
        val log: Logger = Logger.get(DucklakePageSourceProvider::class.java)
    }
}
