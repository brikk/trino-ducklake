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

import com.google.common.collect.ImmutableMap
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.corpus.GoldenComparator
import dev.brikk.ducklake.corpus.OracleAttachment
import dev.brikk.ducklake.corpus.ReplayEngineSkip
import dev.brikk.ducklake.corpus.ReplayReadEngine
import dev.brikk.ducklake.corpus.ReplayOracleSqlBinder
import io.trino.Session
import io.trino.client.FailureException
import io.trino.client.FailureInfo
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

/**
 * [ReplayReadEngine] adapter: mirrors corpus lake reads through a Trino
 * [DistributedQueryRunner] with the DuckLake connector.
 *
 * Backend axis: owns a [TestingDucklakePostgreSqlCatalogServer] and exposes
 * [metadataRewriter] for the corpus driver — every DuckLake ATTACH the oracle
 * executes is rewritten onto a fresh, isolated PostgreSQL database on this
 * server; [connect] then registers a Trino catalog against the same database
 * (`ducklake.catalog.database-url`) and the oracle's data path, so oracle and
 * engine read the exact same lake, including intermediate states.
 *
 * `accepts` gate: alias-qualified reads only (`<alias>.<table>` — bare names may be the oracle's
 * in-memory temp tables, not the lake) AND [TrinoCorpusDialect] returns a `Run`. The dialect is
 * transpile-first (brikk-sql translates DuckDB→Trino and keys off the transpiler's own
 * unmappable/unsupported/verify signals). [executeQuery] runs the transpiled SQL (after the
 * corpus-specific alias→catalog rewrite).
 */
class TrinoReplayEngine : ReplayReadEngine {

    override val name: String = "trino"

    private val server = TestingDucklakePostgreSqlCatalogServer()
    private val runner: DistributedQueryRunner
    private val counter = AtomicInteger()

    private var currentCatalog: String? = null
    private var currentConfig: DucklakeCatalogConfig? = null
    private var currentAlias: String? = null
    private var currentSession: Session? = null

    /**
     * Trino catalogs already registered, keyed by the lake they point at (rewritten metadata URI +
     * data path). The driver memoises the rewritten target per corpus file, so a DETACH / re-ATTACH
     * cycle calls [connect] again for the SAME lake without re-running [metadataRewriter]; Trino
     * catalogs cannot be re-created under the same name, so we reuse the one we have.
     */
    private val catalogsByLake: MutableMap<Pair<String, String>, String> = HashMap()

    init {
        val session =
            testSessionBuilder()
                .setCatalog("system")
                .setSchema("runtime")
                .build()
        runner = DistributedQueryRunner.builder(session).build()
        runner.installPlugin(DucklakePlugin())
    }

    /**
     * The corpus driver's metadata-rewrite hook: allocate a fresh PG database
     * for the original (duckdb-local) metadata path and return the DuckLake
     * remainder the ORACLE should attach instead.
     */
    val metadataRewriter: (String) -> String = { _ ->
        val db = "corpus_" + counter.incrementAndGet()
        server.createDatabase(db)
        server.getDuckDbAttachUri(db).removePrefix("ducklake:")
    }

    val oracleSqlBinder = ReplayOracleSqlBinder { sql, context ->
        val schemas = context.lakes.filter {
            it.rewritten && it.explicitMetadataSchema == null && it.attachment.metadataUri.startsWith("postgres:")
        }.associate { it.metadataCatalog to "public" }
        TrinoReplayMetadataBindings.rewrite(sql, schemas)
    }

    /** Observe the real stored format in a focused replay test, not the requested setup option. */
    internal fun catalogSpecVersion(attachment: OracleAttachment): String {
        return JdbcDucklakeCatalog(catalogConfig(attachment)).use {
            requireNotNull(it.getSpecVersion()) { "Replay catalog has no stored format version" }
        }
    }

    private fun catalogConfig(attachment: OracleAttachment): DucklakeCatalogConfig = DucklakeCatalogConfig().apply {
        catalogDatabaseUrl = server.getJdbcUrl(databaseName(attachment))
        catalogDatabaseUser = server.getUser()
        catalogDatabasePassword = server.getPassword()
        dataPath = attachment.dataPath
        maxCatalogConnections = 1
    }

    private fun databaseName(attachment: OracleAttachment): String =
        Regex("dbname=([A-Za-z0-9_]+)").find(attachment.metadataUri)?.groupValues?.get(1)
            ?: error("connect(): rewritten metadata URI carries no dbname: ${attachment.metadataUri}")

    override fun connect(attachment: OracleAttachment) {
        // The PG database is in the (rewritten) URI the oracle attached — never rely on
        // "the last database the rewriter minted": a re-ATTACH reuses an earlier one.
        val config = catalogConfig(attachment)
        val catalog = catalogsByLake.getOrPut(attachment.metadataUri to attachment.dataPath) {
            // Unique Trino catalog name per lake; a lake re-attached with a different DATA_PATH is
            // a different catalog (same metadata, other files), hence the composite key.
            val name = "${databaseName(attachment)}_c${catalogsByLake.size}"
            val properties =
                ImmutableMap.builder<String, String>()
                    .put("ducklake.catalog.database-url", requireNotNull(config.catalogDatabaseUrl))
                    .put("ducklake.data-path", attachment.dataPath)
                    .put("fs.hadoop.enabled", "true")
            config.catalogDatabaseUser?.let { properties.put("ducklake.catalog.database-user", it) }
            config.catalogDatabasePassword?.let { properties.put("ducklake.catalog.database-password", it) }
            runner.createCatalog(name, "ducklake", properties.buildOrThrow())
            name
        }
        currentCatalog = catalog
        currentConfig = config
        currentAlias = attachment.catalogAlias
        currentSession =
            testSessionBuilder()
                .setCatalog(catalog)
                .setSchema("main")
                .build()
    }

    /** Cache the last gate decision so accepts() + executeQuery() transpile once. */
    private var lastGate: Pair<String, TrinoCorpusDialect.Gate>? = null

    private fun gateFor(sql: String): TrinoCorpusDialect.Gate {
        lastGate?.let { (cachedSql, gate) -> if (cachedSql == sql) return gate }
        val gate = TrinoCorpusDialect.gate(sql)
        lastGate = sql to gate
        return gate
    }

    override fun accepts(sql: String): Boolean {
        val alias = currentAlias ?: return false
        val run = gateFor(sql) as? TrinoCorpusDialect.Run ?: return false
        return TrinoReplayBindings.referencesLake(run.trinoSql, alias)
    }

    override fun executeQuery(sql: String): List<List<String?>> {
        val session = currentSession ?: error("connect() was not called")
        val catalog = currentCatalog!!
        val alias = currentAlias!!
        // The transpiled Trino SQL (DuckDB-isms translated, time travel mapped, ORDER BY ALL
        // handled) — accepts() already confirmed this is a Run. First resolve the corpus
        // alias→catalog names, then apply the schema-aware MAP access rewrite below:
        // <alias>.<schema>.<t> → <catalog>.<schema>.<t>; then <alias>.<t> (no further dot) →
        // <catalog>.main.<t>.
        val run = gateFor(sql) as? TrinoCorpusDialect.Run
            ?: error("executeQuery on a non-runnable query: ${gateFor(sql)}")
        val s = TrinoReplayBindings.rewrite(run.trinoSql, alias, catalog)
        val result =
            try {
                val typedSql = TrinoMapAccessRewriter.rewrite(s, catalog, session.schema.orElse(null)) { table ->
                    // LIMIT 0 binds the current (or explicitly versioned) table schema without
                    // reading its rows. Do not cache it across corpus DDL or lake re-attachments.
                    val schema = runner.execute(session, "SELECT * FROM $table LIMIT 0")
                    schema.columnNames.zip(schema.types.map { it.displayName }).toMap()
                }
                runner.execute(session, typedSql)
            } catch (e: RuntimeException) {
                throw classifyEngineError(e, currentCatalog) { schema, table ->
                    currentConfig?.let { lakeRelationsNamed(it, schema, table) }
                }
            }
        val types = result.types
        return result.materializedRows.map { row ->
            (0 until row.fieldCount).map { i -> renderTyped(row.getField(i), types[i]) }
        }
    }

    /** Every table (null) and view (its dialect) ever named [schema].[table] in the lake, in any snapshot. */
    private fun lakeRelationsNamed(config: DucklakeCatalogConfig, schema: String, table: String): List<String?> =
        DriverManager.getConnection(config.catalogDatabaseUrl, config.catalogDatabaseUser, config.catalogDatabasePassword)
            .use { connection ->
                connection.prepareStatement(
                    """
                    SELECT NULL FROM ducklake_table t JOIN ducklake_schema s ON t.schema_id = s.schema_id
                    WHERE lower(s.schema_name) = lower(?) AND lower(t.table_name) = lower(?)
                    UNION ALL
                    SELECT v.dialect FROM ducklake_view v JOIN ducklake_schema s ON v.schema_id = s.schema_id
                    WHERE lower(s.schema_name) = lower(?) AND lower(v.view_name) = lower(?)
                    """.trimIndent(),
                ).use { statement ->
                    listOf(schema, table, schema, table).forEachIndexed { i, value -> statement.setString(i + 1, value) }
                    statement.executeQuery().use { rows ->
                        buildList { while (rows.next()) add(rows.getString(1)) }
                    }
                }
            }

    /**
     * Renders a materialized Trino value into DuckDB's text dialect so it
     * compares against the oracle's rendering: ROW → `{'name': v}`, MAP →
     * `{k=v}`, ARRAY → `[v, ...]`, scalars via [GoldenComparator.renderCell].
     * Trino materializes ROWs as [io.trino.testing.MaterializedRow] or List,
     * MAPs as java Map, ARRAYs as List.
     */
    private fun renderTyped(value: Any?, type: io.trino.spi.type.Type): String? {
        if (value == null) return null
        return when (type) {
            is io.trino.spi.type.RowType -> {
                val fields = type.fields
                val children = rowChildren(value)
                fields.indices.joinToString(", ", prefix = "{", postfix = "}") { i ->
                    val name = fields[i].name.orElse("f$i")
                    "'$name': ${renderTypedNested(children[i], fields[i].type)}"
                }
            }
            is io.trino.spi.type.MapType ->
                (value as Map<*, *>).entries.joinToString(", ", prefix = "{", postfix = "}") { (k, v) ->
                    "${renderTypedNested(k, type.keyType)}=${renderTypedNested(v, type.valueType)}"
                }
            is io.trino.spi.type.ArrayType ->
                (value as List<*>).joinToString(", ", prefix = "[", postfix = "]") {
                    renderTypedNested(it, type.elementType)
                }
            else -> GoldenComparator.renderCell(value)
        }
    }

    private fun renderTypedNested(value: Any?, type: io.trino.spi.type.Type): String =
        when {
            value == null -> "NULL"
            type is io.trino.spi.type.RowType ||
                type is io.trino.spi.type.MapType ||
                type is io.trino.spi.type.ArrayType -> renderTyped(value, type)!!
            else -> GoldenComparator.renderNested(value)
        }

    private fun rowChildren(value: Any): List<Any?> =
        when (value) {
            is io.trino.testing.MaterializedRow -> value.fields
            is List<*> -> value
            else -> throw IllegalStateException("Unexpected materialized ROW shape: ${value.javaClass.name}")
        }

    override fun close() {
        runCatching { runner.close() }
        runCatching { server.close() }
    }

    companion object {
        private const val CONNECTOR_PACKAGE = "dev.brikk.ducklake.trino."
        private val UNKNOWN_FUNCTION = Regex("""Function '[^']+' not registered""")
        private val STRING_LITERAL_OPERAND = Regex("""Cannot apply operator: .*\b(var)?char\(\d+\)""")
        private val MISSING_TABLE = Regex("""Table '([^.']+)\.("[^"]*(?:""[^"]*)*"|[^.']+)\.(.+)' does not exist""")

        /**
         * Message text only narrows server-side error codes. Engine NOT_SUPPORTED and function
         * argument mismatches fail; a connector NOT_SUPPORTED gate or unknown function can skip.
         * Bounded character operands approximate DuckDB's implicit literal casts. Relation lookup
         * includes every snapshot: a name ever held by a table or Trino view must fail, not skip.
         * A null lookup means no lake is connected, not an empty relation history.
         */
        internal fun classifyEngineError(
            e: RuntimeException,
            currentCatalog: String?,
            lakeRelationsNamed: (String, String) -> List<String?>?,
        ): RuntimeException {
            val failure = generateSequence<Throwable>(e) { it.cause }
                .filterIsInstance<FailureException>()
                .firstOrNull()
                ?.failureInfo
                ?: return e
            val first = failure.message.orEmpty().lineSequence().first()
            val reason = when (failure.errorInfo?.name) {
                "NOT_SUPPORTED" ->
                    "connector rejects an explicitly unsupported capability: $first".takeIf { thrownByConnector(failure) }
                "FUNCTION_NOT_FOUND" ->
                    "dialect: function does not exist in Trino: $first".takeIf { UNKNOWN_FUNCTION.containsMatchIn(first) }
                "TYPE_MISMATCH" ->
                    "dialect: no implicit cast from a string literal: $first".takeIf { STRING_LITERAL_OPERAND.containsMatchIn(first) }
                "TABLE_NOT_FOUND" -> relationOutsideLake(first, currentCatalog, lakeRelationsNamed)
                else -> null
            }
            return reason?.let { ReplayEngineSkip(it) } ?: e
        }

        private fun thrownByConnector(failure: FailureInfo): Boolean =
            failure.stack.firstOrNull()?.startsWith(CONNECTOR_PACKAGE) == true

        private fun relationOutsideLake(
            message: String,
            currentCatalog: String?,
            lakeRelationsNamed: (String, String) -> List<String?>?,
        ): String? {
            val match = MISSING_TABLE.find(message) ?: return null
            val (catalog, schema, table) = match.destructured
            if (catalog != currentCatalog) return null
            val schemaName = unquote(schema)
            val tableName = unquote(table)
            val dialects = lakeRelationsNamed(schemaName, tableName) ?: return null
            return when {
                dialects.isEmpty() ->
                    "query reads $schemaName.$tableName, which the lake has never contained (oracle-local table or file)"
                dialects.all { it != null && !isTrinoDialect(it) } ->
                    "lake view $schemaName.$tableName has a non-Trino dialect (${dialects.toSet()}); the connector does not serve it"
                else -> null
            }
        }

        private fun isTrinoDialect(dialect: String): Boolean =
            dialect.substringBefore('/').equals(DucklakeTrinoViewCodec.DIALECT, ignoreCase = true)

        private fun unquote(identifier: String): String =
            if (identifier.length >= 2 && identifier.startsWith('"') && identifier.endsWith('"')) {
                identifier.substring(1, identifier.length - 1).replace("\"\"", "\"")
            } else {
                identifier
            }
    }
}
