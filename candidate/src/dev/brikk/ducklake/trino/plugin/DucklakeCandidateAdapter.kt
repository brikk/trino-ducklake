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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeChangeFeedDeletion
import dev.brikk.ducklake.catalog.DucklakeDeletionCursor
import dev.brikk.ducklake.catalog.DucklakeDeletionPageEntry
import dev.brikk.ducklake.catalog.DucklakeInlineCursor
import dev.brikk.ducklake.catalog.DucklakeInlinedChangeRow
import dev.brikk.ducklake.catalog.DucklakeInlinedFileDelete
import dev.brikk.ducklake.catalog.DucklakeInlinedFileDeleteCursor
import dev.brikk.ducklake.catalog.DucklakeSnapshot
import dev.brikk.ducklake.catalog.DucklakeSnapshotChange
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeDataFileCursor
import dev.brikk.ducklake.catalog.DucklakeDataFilePageEntry
import dev.brikk.ducklake.catalog.DucklakeAddedFileCursor
import dev.brikk.ducklake.catalog.DucklakeAddedFilePageEntry
import dev.brikk.ducklake.catalog.DucklakeFilePartitionCursor
import dev.brikk.ducklake.catalog.DucklakeFilePartitionValue
import dev.brikk.ducklake.catalog.DucklakeInlinedDataInfo
import dev.brikk.ducklake.catalog.DucklakeNameMappingRow
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionFieldCursor
import dev.brikk.ducklake.catalog.DucklakePartitionFieldPageEntry
import dev.brikk.ducklake.catalog.DucklakeReadSessionControl
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Supplier

internal fun candidateReadAdapter(catalog: JdbcDucklakeCatalog, config: DucklakeConfig): DucklakeCatalog =
    if (config.isEnableFinal11Reads()) PagedDucklakeCatalog(catalog, config) else catalog

/** Typed public candidate APIs only; list shapes remain compatible with the released consumer. */
internal class PagedDucklakeCatalog(
    override val jdbc: JdbcDucklakeCatalog,
    private val config: DucklakeConfig,
) : DucklakeCatalog by jdbc, DucklakeControlledReads {
    private class View(val control: DucklakeReadSessionControl) {
        val budget = PlanningBudget()
        var final11: Boolean? = null
    }
    private val owned = ThreadLocal<View>()
    private val active = ConcurrentHashMap<String, MutableSet<DucklakeReadSessionControl>>()

    override fun <T> readSession(action: Supplier<T>): T = queryRead(null, action)

    override fun <T> queryRead(queryId: String?, action: Supplier<T>): T {
        if (owned.get() != null) return jdbc.readSession(action)
        val view = View(DucklakeReadSessionControl(config.getReadTimeoutSeconds()))
        // Registration/removal and cleanup are atomic per query, including concurrent workers.
        if (queryId != null) active.compute(queryId) { _, controls ->
            (controls ?: ConcurrentHashMap.newKeySet()).apply { add(view.control) }
        }
        owned.set(view)
        try {
            return jdbc.readSession(view.control, Supplier {
                val result = action.get()
                view.budget.checkComplete()
                result
            })
        } finally {
            owned.remove()
            if (queryId != null) active.computeIfPresent(queryId) { _, controls ->
                controls.remove(view.control)
                controls.takeIf { it.isNotEmpty() }
            }
        }
    }

    override fun cancelQuery(queryId: String) {
        // No pool/transaction monitor, executor, retry or connection kill on the cancellation path.
        active.computeIfPresent(queryId) { _, controls ->
            controls.forEach { it.cancel() }
            controls
        }
    }

    private fun paged(): Boolean {
        val view = owned.get() ?: error("Paged planning requires an owned connector read view")
        return view.final11 ?: (jdbc.getSpecVersion() == "1.1").also { view.final11 = it }
    }

    private fun budget(): PlanningBudget = owned.get()?.budget ?: error("No owned planning budget")

    private fun refuse(reason: String): Nothing = budget().abort(planningFailure(reason))

    /** Empty terminal fetch is charged too. Never infer completion from a short/non-live page. */
    private fun <C, R> drain(fetch: (C?, Int) -> List<R>, cursor: (R) -> C, charge: (R) -> Unit): List<R> {
        val result = mutableListOf<R>()
        var after: C? = null
        while (true) {
            budget().page()
            budget().work(config.getReadPageSize() + 1) // SQL lookahead, including terminal calls
            val page = budget().attempt { fetch(after, config.getReadPageSize()) }
            if (page.isEmpty()) return result
            page.forEach { row -> budget().node(); charge(row); result.add(row) }
            val next = cursor(page.last())
            if (next == after) refuse("non-advancing page cursor")
            after = next
        }
    }

    override fun listSchemas(snapshotId: Long): List<DucklakeSchema> = if (!paged()) jdbc.listSchemas(snapshotId) else
        drain<Long, DucklakeSchema>({ after, limit -> jdbc.readPlanningMetadata().listSchemasPage(snapshotId, after, limit) },
            { it.schemaId }, { budget().text(it.schemaName); budget().path(it.path) })

    override fun listTables(schemaId: Long, snapshotId: Long): List<DucklakeTable> = if (!paged()) jdbc.listTables(schemaId, snapshotId) else
        drain<Long, DucklakeTable>({ after, limit -> jdbc.readPlanningMetadata().listTablesPage(schemaId, snapshotId, after, limit) },
            { it.tableId }, { budget().text(it.tableName); budget().path(it.path) })

    override fun getSchema(schemaName: String, snapshotId: Long): DucklakeSchema? = if (!paged()) jdbc.getSchema(schemaName, snapshotId)
        else listSchemas(snapshotId).filter { it.schemaName.equals(schemaName, ignoreCase = true) }.unique("schema name")

    override fun getTable(schemaName: String, tableName: String, snapshotId: Long): DucklakeTable? {
        if (!paged()) return jdbc.getTable(schemaName, tableName, snapshotId)
        val schema = getSchema(schemaName, snapshotId) ?: return null
        return listTables(schema.schemaId, snapshotId).filter { it.tableName.equals(tableName, ignoreCase = true) }.unique("table name")
    }

    override fun getTableById(tableId: Long, snapshotId: Long): DucklakeTable? {
        if (!paged()) return jdbc.getTableById(tableId, snapshotId)
        return listSchemas(snapshotId).flatMap { listTables(it.schemaId, snapshotId) }.filter { it.tableId == tableId }.unique("table ID")
    }

    private fun <T> List<T>.unique(identity: String): T? {
        if (size > 1) refuse("duplicate $identity")
        return singleOrNull()
    }

    override fun getAllColumnsWithParentage(tableId: Long, snapshotId: Long): List<DucklakeColumn> {
        if (!paged()) return jdbc.getAllColumnsWithParentage(tableId, snapshotId)
        val rows = drain<Long, DucklakeColumn>({ after, limit -> jdbc.readPlanningMetadata().getColumnsPage(tableId, snapshotId, after, limit) },
            { it.columnId }, { budget().text(it.columnName, it.columnType, it.initialDefault) })
        validateColumns(rows, tableId)
        return rows.sortedWith(compareBy<DucklakeColumn> { it.columnOrder }.thenBy { it.columnId })
    }

    private fun validateColumns(rows: List<DucklakeColumn>, tableId: Long) {
        if (rows.size > 4096 || rows.any { it.tableId != tableId }) refuse("invalid/oversized column hierarchy")
        val byId = rows.associateBy { it.columnId }
        if (byId.size != rows.size) refuse("duplicate stable column ID")
        for (row in rows) {
            val seen = mutableSetOf<Long>()
            var parent: DucklakeColumn? = row
            while (parent != null) {
                budget().work()
                if (!seen.add(parent.columnId) || seen.size > 64) refuse("column cycle/depth budget")
                parent = parent.parentColumn?.let { byId[it] ?: refuse("missing column parent $it") }
            }
        }
        rows.groupBy { it.parentColumn }.values.forEach { siblings ->
            if (siblings.map { it.columnOrder }.toSet().size != siblings.size) refuse("duplicate sibling column order")
        }
    }

    override fun getTableColumns(tableId: Long, snapshotId: Long): List<DucklakeColumn> {
        if (!paged()) return jdbc.getTableColumns(tableId, snapshotId)
        val rows = getAllColumnsWithParentage(tableId, snapshotId)
        val children = rows.filter { it.parentColumn != null }.groupBy { it.parentColumn }
        fun type(row: DucklakeColumn): String {
            val nested = children[row.columnId].orEmpty()
            val result = when (row.columnType.lowercase(Locale.ROOT)) {
                "list" -> {
                    if (nested.size != 1) refuse("LIST must have one child")
                    "list<${type(nested.single())}>"
                }
                "map" -> {
                    if (nested.size != 2) refuse("MAP must have two children")
                    "map<${type(nested[0])},${type(nested[1])}>"
                }
                "struct" -> "struct<${nested.joinToString(",") { "${it.columnName}:${type(it)}" }}>"
                else -> {
                    if (nested.isNotEmpty()) refuse("scalar column has children")
                    row.columnType
                }
            }
            budget().text(result)
            return result
        }
        return rows.filter { it.parentColumn == null }.map { it.copy(columnType = type(it)) }
    }

    override fun getDataFiles(tableId: Long, snapshotId: Long): List<DucklakeDataFile> =
        if (!paged()) jdbc.getDataFiles(tableId, snapshotId) else drain<DucklakeDataFileCursor, DucklakeDataFilePageEntry>(
            { after, limit -> jdbc.getDataFilesPage(tableId, snapshotId, after, limit) }, { it.cursor }, { chargeFile(it.dataFile) }
        ).map { it.dataFile }

    override fun getDataFilesAddedBetween(tableId: Long, startSnapshot: Long, endSnapshot: Long): List<DucklakeDataFile> =
        if (!paged()) jdbc.getDataFilesAddedBetween(tableId, startSnapshot, endSnapshot) else drain<DucklakeAddedFileCursor, DucklakeAddedFilePageEntry>(
            { after, limit -> jdbc.getDataFilesAddedBetweenPage(tableId, startSnapshot, endSnapshot, after, limit) },
            { it.cursor }, { chargeFile(it.dataFile); budget().events(it.dataFile.recordCount, config.getReadMaxCdcEvents()) }
        ).map { it.dataFile }

    private fun chargeFile(file: DucklakeDataFile) {
        budget().file(config.getReadMaxFiles())
        budget().rows(file, config.getReadMaxRows())
        budget().path(file.path, file.deleteFilePath)
        budget().text(file.fileFormat, file.deleteFileFormat)
    }

    override fun inlineRows(tableId: Long, version: Long, snapshot: Long, ids: List<Long>): List<DucklakeInlinedChangeRow> {
        check(paged()) { "Decoded identity pages require final 1.1" }
        return drain<DucklakeInlineCursor, DucklakeInlinedChangeRow>({ after, limit ->
            reserveProjection(ids.size)
            jdbc.readInlinedDataDecodedPage(tableId, version, snapshot, ids, after, limit)
        }, { DucklakeInlineCursor(it.rowId, it.beginSnapshot) }, { chargeInline(it, false) })
    }

    override fun readInlinedDataDecoded(tableId: Long, schemaVersion: Long, snapshotId: Long, columns: List<DucklakeColumn>): List<List<Any?>> =
        if (!paged()) jdbc.readInlinedDataDecoded(tableId, schemaVersion, snapshotId, columns)
        else inlineRows(tableId, schemaVersion, snapshotId, columns.map { it.columnId }).map { it.values }

    override fun getInlinedChangesBetweenDecoded(
        tableId: Long, schemaVersion: Long, startSnapshot: Long, endSnapshot: Long, columnIds: List<Long>,
    ): List<DucklakeInlinedChangeRow> = if (!paged())
        jdbc.getInlinedChangesBetweenDecoded(tableId, schemaVersion, startSnapshot, endSnapshot, columnIds)
    else drain<DucklakeInlineCursor, DucklakeInlinedChangeRow>({ after, limit ->
        reserveProjection(columnIds.size)
        jdbc.getInlinedChangesBetweenDecodedPage(tableId, schemaVersion, startSnapshot, endSnapshot, columnIds, after, limit)
    }, { DucklakeInlineCursor(it.rowId, it.beginSnapshot) }, { chargeInline(it, true) })

    private fun reserveProjection(width: Int) {
        // Two bounded 4096-node schemas plus physical/default-history probes. Reserve before
        // entering library code; even empty terminal pages perform these internal preflights.
        budget().work(8192 + width * 8)
    }

    private fun chargeInline(row: DucklakeInlinedChangeRow, cdc: Boolean) {
        budget().logicalRow(config.getReadMaxRows())
        if (cdc) budget().events(2, config.getReadMaxCdcEvents())
        budget().value(row.values)
    }

    override fun fileDefault(tableId: Long, columnId: Long, snapshot: Long): Any? {
        check(paged()) { "Decoded file defaults require final 1.1" }
        reserveProjection(1)
        return budget().attempt { jdbc.getMissingFileColumnDefaultDecoded(tableId, columnId, snapshot) }.also { budget().value(it) }
    }

    override fun getDeletionsBetween(tableId: Long, startSnapshot: Long, endSnapshot: Long): List<DucklakeChangeFeedDeletion> =
        if (!paged()) jdbc.getDeletionsBetween(tableId, startSnapshot, endSnapshot)
        else drain<DucklakeDeletionCursor, DucklakeDeletionPageEntry>({ after, limit ->
            // Up to two main arms, a lookahead, and LIMIT2 predecessor per candidate.
            budget().work(2 + (limit + 1) * 3)
            jdbc.getDeletionsBetweenPage(tableId, startSnapshot, endSnapshot, after, limit)
        }, { it.cursor }, {
            val d = it.deletion
            budget().file(config.getReadMaxFiles())
            budget().physical(d.recordCount, config.getReadMaxRows())
            budget().events(d.recordCount, config.getReadMaxCdcEvents())
            budget().path(d.dataFilePath, d.currentDeletePath, d.previousDeletePath)
            budget().text(d.dataFileFormat, d.currentDeleteFormat, d.previousDeleteFormat)
        }).map { it.deletion }

    override fun getInlinedFileDeletesBetween(tableId: Long, startSnapshot: Long, endSnapshot: Long): List<DucklakeInlinedFileDelete> =
        if (!paged()) jdbc.getInlinedFileDeletesBetween(tableId, startSnapshot, endSnapshot)
        else drain<DucklakeInlinedFileDeleteCursor, DucklakeInlinedFileDelete>({ after, limit ->
            jdbc.getInlinedFileDeletesBetweenPage(tableId, startSnapshot, endSnapshot, after, limit)
        }, { DucklakeInlinedFileDeleteCursor(it.dataFileId, it.position, it.snapshotId) }, {
            budget().events(1, config.getReadMaxCdcEvents())
        })

    override fun getInlinedDeletes(tableId: Long, snapshotId: Long): Map<Long, Set<Long>> =
        if (!paged()) jdbc.getInlinedDeletes(tableId, snapshotId)
        else getInlinedFileDeletesBetween(tableId, 0, snapshotId).groupBy { it.dataFileId }
            .mapValues { (_, rows) -> rows.mapTo(mutableSetOf()) { it.position } }

    override fun getDataFilesByIds(tableId: Long, dataFileIds: Collection<Long>): List<DucklakeDataFile> {
        if (!paged()) return jdbc.getDataFilesByIds(tableId, dataFileIds)
        if (dataFileIds.size > config.getReadMaxFiles()) refuse("file lookup input budget")
        return dataFileIds.toSet().chunked(config.getReadPageSize()).flatMap { ids ->
            budget().page()
            budget().work(ids.size + 1)
            budget().attempt { jdbc.getDataFilesByIdsBatch(tableId, ids) }.onEach { budget().node(); chargeFile(it) }
        }
    }

    override fun listSnapshots(): List<DucklakeSnapshot> = if (!paged()) jdbc.listSnapshots() else
        drain<Long, DucklakeSnapshot>({ after, limit -> jdbc.listSnapshotsPage(after, limit) }, { it.snapshotId }, {}).asReversed()

    override fun listSnapshotChanges(): List<DucklakeSnapshotChange> = if (!paged()) jdbc.listSnapshotChanges() else
        drain<Long, DucklakeSnapshotChange>({ after, limit -> jdbc.listSnapshotChangesPage(after, limit) }, { it.snapshotId }, {
            budget().text(it.changesMade, it.author, it.commitMessage, it.commitExtraInfo)
        }).asReversed()

    override fun getPartitionSpecs(tableId: Long, snapshotId: Long): List<DucklakePartitionSpec> {
        if (!paged()) return jdbc.getPartitionSpecs(tableId, snapshotId)
        val fields = drain<DucklakePartitionFieldCursor, DucklakePartitionFieldPageEntry>(
            { after, limit -> jdbc.readPlanningMetadata().getPartitionFieldsPage(tableId, snapshotId, after, limit) },
            { it.cursor }, { budget().work() })
        return fields.groupBy { it.cursor.partitionId }.map { (id, entries) ->
            DucklakePartitionSpec(id, tableId, entries.map { it.field })
        }
    }

    override fun getFilePartitionValues(tableId: Long, snapshotId: Long): Map<Long, List<DucklakeFilePartitionValue>> =
        if (!paged()) jdbc.getFilePartitionValues(tableId, snapshotId) else drain<DucklakeFilePartitionCursor, DucklakeFilePartitionValue>(
            { after, limit -> jdbc.readPlanningMetadata().getFilePartitionValuesPage(tableId, snapshotId, after, limit) },
            { DucklakeFilePartitionCursor(it.dataFileId, it.partitionKeyIndex) }, { budget().text(it.partitionValue) }
        ).groupBy { it.dataFileId }

    override fun getInlinedDataInfos(tableId: Long, snapshotId: Long): List<DucklakeInlinedDataInfo> =
        if (!paged()) jdbc.getInlinedDataInfos(tableId, snapshotId) else drain<Long, DucklakeInlinedDataInfo>(
            { after, limit -> jdbc.readPlanningMetadata().getInlineVersionsPage(tableId, snapshotId, after, limit) },
            { it.schemaVersion }, { budget().text(it.tableName); repeat(8) { budget().work() } }
        )

    private fun names(mappingIds: Set<Long>, partition: Boolean): Map<Long, Map<Long, String>> = mappingIds.associateWith { id ->
        val rows = drain<Long, DucklakeNameMappingRow>({ after, limit -> jdbc.readPlanningMetadata().getNameMappingPage(id, after, limit) },
            { it.mappingColumnId }, { budget().text(it.sourceName) })
        val byId = rows.associateBy { it.mappingColumnId }
        if (byId.size != rows.size) refuse("duplicate mapping ID")
        rows.forEach { row ->
            var parent: DucklakeNameMappingRow? = row
            val seen = mutableSetOf<Long>()
            while (parent != null) {
                budget().work()
                if (!seen.add(parent.mappingColumnId) || seen.size > 64) refuse("name mapping cycle/depth budget")
                parent = parent.parentMappingColumnId?.let { byId[it] ?: refuse("missing mapping parent $it") }
            }
            if (row.isPartition == null) refuse("unknown name mapping partition flag")
        }
        if (rows.any { it.isPartition == true && it.parentMappingColumnId != null }) refuse("nested Hive partition mapping")
        val selected = rows.filter { it.isPartition == partition }
        if (selected.map { it.targetFieldId }.toSet().size != selected.size) refuse("ambiguous target name mapping")
        selected.associate { it.targetFieldId to it.sourceName }
    }

    override fun getNameMaps(mappingIds: Set<Long>): Map<Long, Map<Long, String>> =
        if (!paged()) jdbc.getNameMaps(mappingIds) else names(mappingIds, false)

    override fun getPartitionNameMaps(mappingIds: Set<Long>): Map<Long, Map<Long, String>> =
        if (!paged()) jdbc.getPartitionNameMaps(mappingIds) else names(mappingIds, true)
}

/** Shared totals across ALL paged scopes in an outer view, not per-list post-fetch checks. */
private class PlanningBudget {
    private var failure: RuntimeException? = null
    private var pages = 0
    private var nodes = 0
    private var paths = 0
    private var bytes = 0L
    private var work = 0
    private var files = 0
    private var rows = 0L
    private var events = 0L
    private val rowFiles = mutableSetOf<Pair<Long, Long>>()
    fun checkComplete() { failure?.let { throw it } }
    fun abort(exception: RuntimeException): Nothing {
        if (failure == null) failure = exception
        throw exception
    }
    // A failed page cannot be swallowed into a successful incomplete drain, regardless of
    // which supported driver/library exception type supplied the original diagnostic.
    @Suppress("TooGenericExceptionCaught")
    fun <T> attempt(action: () -> T): T {
        checkComplete()
        return try { action() } catch (e: RuntimeException) { abort(e) }
    }
    private fun refuse(reason: String): Nothing {
        val exception = planningFailure(reason)
        failure = exception
        throw exception
    }
    fun rows(file: DucklakeDataFile, maximum: Long) {
        if (file.recordCount < 0) refuse("negative file row count")
        if (rowFiles.add(file.tableId to file.dataFileId)) {
            physical(file.recordCount, maximum)
        }
    }
    fun page() {
        checkComplete()
        if (++pages > 1024) refuse("total planning page budget")
        work()
    }
    fun node() {
        if (++nodes > 16384) refuse("total planning node budget")
        work()
    }
    fun work(amount: Int = 1) {
        checkComplete()
        if (amount < 0 || amount > 131072 - work) refuse("total planning work budget")
        work += amount
    }
    fun physical(amount: Long, maximum: Long) {
        if (amount < 0 || amount > maximum - rows) refuse("total physical rows budget")
        rows += amount
    }
    fun logicalRow(maximum: Long) = physical(1, maximum)
    fun events(amount: Long, maximum: Long) {
        if (amount < 0 || amount > maximum - events) refuse("total CDC events budget")
        events += amount
    }
    fun value(value: Any?, depth: Int = 0) {
        if (depth > 64) refuse("decoded value depth budget")
        node()
        when (value) {
            is String -> text(value)
            is ByteArray -> byteCount(value.size.toLong())
            is List<*> -> value.forEach { value(it, depth + 1) }
            is Map<*, *> -> value.forEach { (key, item) -> value(key, depth + 1); value(item, depth + 1) }
            else -> byteCount(32)
        }
    }
    private fun byteCount(amount: Long) {
        bytes += amount
        if (bytes > 16L * 1024 * 1024) refuse("total planning byte budget")
    }
    fun file(maximum: Int) { if (++files > maximum) refuse("total file descriptor budget") }
    fun path(vararg values: String?) {
        values.filterNotNull().forEach {
            if (++paths > 20000) refuse("total planning path budget")
            text(it)
        }
    }
    fun text(vararg values: String?) {
        values.filterNotNull().forEach {
            // UTF-8 upper bound, checked before allocating a second encoded copy.
            byteCount(it.length.toLong() * 3)
        }
    }
}

private fun planningFailure(reason: String) =
    TrinoException(NOT_SUPPORTED, "DuckLake read budget exceeded or metadata invalid: $reason; incomplete plan")
