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

import java.util.concurrent.ConcurrentHashMap

/**
 * Holds one [DucklakeMetadata] per open Trino transaction.
 *
 * Catalog writes are not deferred to [commit]: the catalog library has no transaction spanning
 * several calls, so every metadata write publishes its own catalog snapshot when it runs. Trino
 * only runs writes in autocommit transactions here ([DucklakeConnector.isSingleStatementWritesOnly]),
 * so [commit] just releases the transaction. [rollback] undoes the one thing a failed statement
 * can leave half done: a CTAS table whose data never published.
 */
open class DucklakeTransactionManager @Inject constructor(private val metadataFactory: DucklakeMetadataFactory)
{
    private val transactions: MutableMap<DucklakeTransactionHandle, DucklakeMetadata> = ConcurrentHashMap()

    open fun begin(transaction: DucklakeTransactionHandle)
    {
        transactions[transaction] = metadataFactory.create()
    }

    open fun getMetadata(transaction: io.trino.spi.connector.ConnectorTransactionHandle): DucklakeMetadata
    {
        @Suppress("UNCHECKED_CAST")
        val metadata = (transactions as Map<Any, DucklakeMetadata>)[transaction]
            ?: throw IllegalArgumentException("Unknown transaction: $transaction")
        return metadata
    }

    open fun commit(transaction: DucklakeTransactionHandle)
    {
        release(transaction)
    }

    open fun rollback(transaction: DucklakeTransactionHandle)
    {
        release(transaction).dropUnpublishedCreatedTables()
    }

    private fun release(transaction: DucklakeTransactionHandle): DucklakeMetadata =
        transactions.remove(transaction) ?: throw IllegalArgumentException("Unknown transaction: $transaction")
}
