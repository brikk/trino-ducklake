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
import dev.brikk.ducklake.catalog.DucklakeCommitOutcomeUnknownException
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakeException
import dev.brikk.ducklake.catalog.DucklakePostCommitException
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.LogicalConflictException
import dev.brikk.ducklake.catalog.TableColumnSpec
import io.airlift.json.JsonCodec
import io.airlift.slice.Slices.utf8Slice
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnMetadata
import io.trino.spi.connector.ConnectorTableMetadata
import io.trino.spi.connector.ConnectorViewDefinition
import io.trino.spi.connector.RetryMode
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.type.BigintType.BIGINT
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.sql.SQLException
import java.util.Optional

/** What [DucklakeTransactionManager.rollback] may and may not undo after a failed write statement. */
class TestDucklakeTransactionRollback {
    @TempDir
    lateinit var root: Path
    private lateinit var rawCatalog: JdbcDucklakeCatalog
    private lateinit var transactions: DucklakeTransactionManager
    private var commitInsertFailure: RuntimeException? = null
    private var viewWriteFailure: DucklakeException? = null
    private val viewCalls = mutableListOf<String>()
    private val writeCodec = JsonCodec.jsonCodec(DucklakeWriteFragment::class.java)
    private val ctas = SchemaTableName("test_schema", "ctas")

    @BeforeEach
    fun setUp() {
        val warehouse = Files.createDirectories(root.resolve("warehouse"))
        val config = DucklakeTestCatalogEnvironment.createLocalCatalog(root.resolve("catalog.db"), warehouse)
        rawCatalog = JdbcDucklakeCatalog(config.toCatalogConfig())
        rawCatalog.createSchema("test_schema")
        val catalog = object : DucklakeCatalog by rawCatalog {
            override fun commitInsert(tableId: Long, fragments: List<DucklakeWriteFragment>) {
                commitInsertFailure?.let { throw it }
                rawCatalog.commitInsert(tableId, fragments)
            }

            override fun createView(schemaName: String, viewName: String, sql: String, dialect: String,
                    columnAliases: List<String>, tags: Map<String, String?>) {
                viewCalls += "createView"
                viewWriteFailure?.let { throw it }
                rawCatalog.createView(schemaName, viewName, sql, dialect, columnAliases, tags)
            }

            override fun replaceViewMetadata(schemaName: String, viewName: String, sql: String, dialect: String,
                    columnAliases: List<String>, tags: Map<String, String?>) {
                viewCalls += "replaceViewMetadata"
                viewWriteFailure?.let { throw it }
                rawCatalog.replaceViewMetadata(schemaName, viewName, sql, dialect, columnAliases, tags)
            }

            override fun dropView(schemaName: String, viewName: String) {
                viewCalls += "dropView"
                rawCatalog.dropView(schemaName, viewName)
            }
        }
        transactions = DucklakeTransactionManager(DucklakeMetadataFactory(
                catalog,
                DucklakeTypeConverter(TESTING_TYPE_MANAGER),
                DucklakeSnapshotResolver(catalog, config),
                writeCodec,
                JsonCodec.jsonCodec(DucklakeDeleteFragment::class.java),
                DucklakePathResolver(catalog, config),
                config))
    }

    @AfterEach
    fun tearDown() {
        if (::rawCatalog.isInitialized) rawCatalog.close()
    }

    @Test
    fun rollbackBeforeFinishDropsTheCreatedTable() {
        val transaction = DucklakeTransactionHandle()
        transactions.begin(transaction)
        transactions.getMetadata(transaction).beginCtas()
        assertThat(tableExists()).isTrue()

        transactions.rollback(transaction)

        assertThat(tableExists()).isFalse()
    }

    @Test
    fun commitRejectedBeforePublicationDropsTheCreatedTable() {
        val transaction = DucklakeTransactionHandle()
        transactions.begin(transaction)
        val metadata = transactions.getMetadata(transaction)
        val handle = metadata.beginCtas()
        commitInsertFailure = LogicalConflictException("table changed under the CTAS")

        assertThatThrownBy { metadata.finishCtas(handle) }.isInstanceOf(TrinoException::class.java)
        transactions.rollback(transaction)

        assertThat(tableExists()).isFalse()
    }

    @Test
    fun possiblyPublishedCommitKeepsTheCreatedTable() {
        val cause = SQLException("lost acknowledgement", "08006")
        for (failure in listOf(
                DucklakeCommitOutcomeUnknownException("CTAS", null, cause),
                DucklakePostCommitException("CTAS", cause))) {
            val transaction = DucklakeTransactionHandle()
            transactions.begin(transaction)
            val metadata = transactions.getMetadata(transaction)
            val handle = metadata.beginCtas()
            commitInsertFailure = failure

            assertThatThrownBy { metadata.finishCtas(handle) }.hasMessageContaining("COMMIT")
            transactions.rollback(transaction)

            assertThat(tableExists()).`as`(failure.javaClass.simpleName).isTrue()
            rawCatalog.dropTable(ctas.schemaName, ctas.tableName)
        }
    }

    @Test
    fun nonCatalogCommitFailureKeepsTheCreatedTable() {
        val transaction = DucklakeTransactionHandle()
        transactions.begin(transaction)
        val metadata = transactions.getMetadata(transaction)
        val handle = metadata.beginCtas()
        commitInsertFailure = IllegalStateException("driver failed mid-commit")

        assertThatThrownBy { metadata.finishCtas(handle) }.isInstanceOf(IllegalStateException::class.java)
        transactions.rollback(transaction)

        assertThat(tableExists()).isTrue()
    }

    @Test
    fun rollbackAfterPublishedCtasKeepsTheTable() {
        val transaction = DucklakeTransactionHandle()
        transactions.begin(transaction)
        val metadata = transactions.getMetadata(transaction)
        metadata.finishCtas(metadata.beginCtas())

        transactions.rollback(transaction)

        assertThat(tableExists()).isTrue()
        assertThat(rawCatalog.getDataFiles(tableId(), rawCatalog.currentSnapshotId)).hasSize(1)
    }

    @Test
    fun rollbackLeavesRowsAnotherWriterCommitted() {
        val transaction = DucklakeTransactionHandle()
        transactions.begin(transaction)
        val handle = transactions.getMetadata(transaction).beginCtas()
        rawCatalog.commitInsert(handle.tableId, listOf(writeCodec.fromJson(fragment().bytes)))

        transactions.rollback(transaction)

        assertThat(tableExists()).isTrue()
    }

    @Test
    fun rollbackLeavesATableThatReplacedTheCreatedOne() {
        val transaction = DucklakeTransactionHandle()
        transactions.begin(transaction)
        val handle = transactions.getMetadata(transaction).beginCtas()
        rawCatalog.dropTable(ctas.schemaName, ctas.tableName)
        rawCatalog.createTable(ctas.schemaName, ctas.tableName, listOf(TableColumnSpec.leaf("id", "int64", true)),
                null, null)
        val replacement = tableId()
        assertThat(replacement).isNotEqualTo(handle.tableId)

        transactions.rollback(transaction)

        assertThat(tableId()).isEqualTo(replacement)
    }

    @Test
    fun commitReleasesTheTransactionWithoutDroppingAnything() {
        val transaction = DucklakeTransactionHandle()
        transactions.begin(transaction)
        transactions.getMetadata(transaction).beginCtas()

        transactions.commit(transaction)

        assertThat(tableExists()).isTrue()
        assertThatThrownBy { transactions.getMetadata(transaction) }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun createOrReplaceViewIsOneCatalogCallAndAFailureKeepsTheOldView() {
        val view = SchemaTableName("test_schema", "v")
        val transaction = DucklakeTransactionHandle()
        transactions.begin(transaction)
        val metadata = transactions.getMetadata(transaction)
        metadata.createView(SESSION, view, viewDefinition("SELECT 1 AS id"), emptyMap(), false)
        val original = rawCatalog.getView(view.schemaName, view.tableName, rawCatalog.currentSnapshotId)!!
        viewCalls.clear()
        viewWriteFailure = DucklakeException("catalog rejected the view write")

        assertThatThrownBy { metadata.createView(SESSION, view, viewDefinition("SELECT 2 AS id"), emptyMap(), true) }
                .isInstanceOf(TrinoException::class.java)

        assertThat(viewCalls).containsExactly("replaceViewMetadata")
        assertThat(rawCatalog.getView(view.schemaName, view.tableName, rawCatalog.currentSnapshotId)?.sql)
                .isEqualTo(original.sql)

        viewWriteFailure = null
        val before = rawCatalog.currentSnapshotId
        metadata.createView(SESSION, view, viewDefinition("SELECT 2 AS id"), emptyMap(), true)
        val replaced = rawCatalog.getView(view.schemaName, view.tableName, rawCatalog.currentSnapshotId)!!
        assertThat(replaced.sql).isEqualTo("SELECT 2 AS id")
        assertThat(replaced.viewId).isEqualTo(original.viewId)
        assertThat(rawCatalog.currentSnapshotId).isEqualTo(before + 1)
        transactions.commit(transaction)
    }

    private fun DucklakeMetadata.beginCtas(): DucklakeWritableTableHandle =
            beginCreateTable(SESSION,
                    ConnectorTableMetadata(ctas, listOf(ColumnMetadata("id", BIGINT))),
                    Optional.empty(), RetryMode.NO_RETRIES, false) as DucklakeWritableTableHandle

    private fun DucklakeMetadata.finishCtas(handle: DucklakeWritableTableHandle) {
        finishCreateTable(SESSION, handle, listOf(fragment()), emptyList())
    }

    private fun fragment() = utf8Slice(writeCodec.toJson(
            DucklakeWriteFragment("ducklake-ctas.parquet", true, "parquet", 100L, 0L, 1L, emptyList(), emptyMap(), null, null)))

    private fun tableExists(): Boolean = rawCatalog.getTable(ctas.schemaName, ctas.tableName, rawCatalog.currentSnapshotId) != null

    private fun tableId(): Long = rawCatalog.getTable(ctas.schemaName, ctas.tableName, rawCatalog.currentSnapshotId)!!.tableId

    private fun viewDefinition(sql: String) = ConnectorViewDefinition(
            sql, Optional.of("lake"), Optional.of("test_schema"),
            listOf(ConnectorViewDefinition.ViewColumn("id", BIGINT.typeId, Optional.empty())),
            Optional.empty(), Optional.empty(), true, emptyList())
}
