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
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeView
import dev.brikk.ducklake.trino.plugin.DucklakeCatalogExceptions.translateCatalogExceptions
import io.airlift.log.Logger
import io.trino.spi.StandardErrorCode.ALREADY_EXISTS
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnNotFoundException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorViewDefinition
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.connector.ViewNotFoundException
import java.util.Optional

/** Trino views stored as DuckLake views (`dialect = 'trino'`), see [DucklakeTrinoViewCodec]. */
internal class DucklakeViewMetadata(
        private val catalog: DucklakeCatalog,
        private val snapshotResolver: DucklakeSnapshotResolver)
{
    fun listViews(session: ConnectorSession, schemaName: Optional<String>): List<SchemaTableName>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)

        if (schemaName.isPresent) {
            val schema: DucklakeSchema = catalog.getSchema(schemaName.get(), snapshotId)
                ?: return emptyList()
            return catalog.listViews(schema.schemaId, snapshotId)
                    .filter { view -> isViewAccessible(view) }
                    .map { view -> SchemaTableName(schemaName.get(), view.viewName) }
        }

        return catalog.listSchemas(snapshotId).flatMap { schema ->
            catalog.listViews(schema.schemaId, snapshotId)
                    .filter { view -> isViewAccessible(view) }
                    .map { view -> SchemaTableName(schema.schemaName, view.viewName) }
        }
    }

    /**
     * Existence check used by DROP VIEW / CREATE OR REPLACE VIEW / ALTER VIEW RENAME (via
     * Trino's `Metadata.isView`). Deliberately does NOT decode the definition, so a
     * Trino-family view written by another connector in a format we can't read is still
     * droppable / replaceable — the remedies [DucklakeTrinoViewCodec.decode]'s error points at.
     */
    fun isView(session: ConnectorSession, viewName: SchemaTableName): Boolean
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val view: DucklakeView = catalog.getView(viewName.schemaName, viewName.tableName, snapshotId)
            ?: return false
        return isViewAccessible(view)
    }

    /**
     * Definition for execution (query analysis, COMMENT ON, SHOW CREATE VIEW). Throws
     * INVALID_VIEW with the reason when the view exists but was not written in this
     * connector's format — never returns a guessed or partial definition.
     */
    fun getView(session: ConnectorSession, viewName: SchemaTableName): Optional<ConnectorViewDefinition>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)

        val view: DucklakeView = catalog.getView(viewName.schemaName, viewName.tableName, snapshotId)
            ?: return Optional.empty()
        if (!isViewAccessible(view)) {
            return Optional.empty()
        }

        return Optional.of(DucklakeTrinoViewCodec.decode(view, viewName))
    }

    fun getViews(session: ConnectorSession, schemaName: Optional<String>): Map<SchemaTableName, ConnectorViewDefinition>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val schemas: List<DucklakeSchema> = if (schemaName.isPresent) {
            listOf(catalog.getSchema(schemaName.get(), snapshotId) ?: return emptyMap())
        }
        else {
            catalog.listSchemas(snapshotId)
        }

        val views: ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> = ImmutableMap.builder()
        for (schema in schemas) {
            for (view in catalog.listViews(schema.schemaId, snapshotId).filter { isViewAccessible(it) }) {
                val name = SchemaTableName(schema.schemaName, view.viewName)
                decodeTrinoViewOrSkip(view, name)?.let { def -> views.put(name, def) }
            }
        }
        return views.buildOrThrow()
    }

    fun createView(viewName: SchemaTableName, definition: ConnectorViewDefinition, replace: Boolean)
    {
        // An existing view is replaced in one catalog snapshot, so readers never see it missing
        // and a failed replace leaves the old definition. A concurrent DROP between this check
        // and the replace fails the statement rather than silently creating the view.
        if (replace && catalog.getView(viewName.schemaName, viewName.tableName, catalog.currentSnapshotId) != null) {
            replaceTrinoView(viewName, definition)
            return
        }

        // sql = the Trino SQL, dialect = 'trino', column_aliases = the spec quoted list of column
        // names, everything else Trino needs as `ducklake_tag` rows (see DucklakeTrinoViewCodec).
        // Never put engine payloads in column_aliases: upstream DuckDB parses it at catalog load
        // and refuses the WHOLE catalog otherwise.
        val encoded = DucklakeTrinoViewCodec.encode(definition)
        translateCatalogExceptions {
            catalog.createView(
                viewName.schemaName,
                viewName.tableName,
                definition.originalSql,
                DucklakeTrinoViewCodec.DIALECT,
                encoded.columnAliases,
                encoded.tags,
            )
        }
    }

    fun renameView(session: ConnectorSession, source: SchemaTableName, target: SchemaTableName)
    {
        if (source == target) {
            return
        }

        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val sourceView: DucklakeView? = catalog.getView(source.schemaName, source.tableName, snapshotId)
        if (sourceView == null || !isViewAccessible(sourceView)) {
            throw ViewNotFoundException(source)
        }

        if (catalog.getTable(target.schemaName, target.tableName, snapshotId) != null ||
                catalog.getView(target.schemaName, target.tableName, snapshotId) != null) {
            throw TrinoException(ALREADY_EXISTS, "Relation already exists: $target")
        }

        translateCatalogExceptions {
            catalog.renameView(
                source.schemaName,
                source.tableName,
                target.schemaName,
                target.tableName
            )
        }
    }

    fun setViewComment(session: ConnectorSession, viewName: SchemaTableName, comment: Optional<String>)
    {
        val definition: ConnectorViewDefinition = getRequiredViewDefinition(session, viewName)
        val updated = ConnectorViewDefinition(
                definition.originalSql,
                definition.catalog,
                definition.schema,
                definition.columns,
                comment,
                definition.owner,
                definition.isRunAsInvoker,
                definition.path
        )

        replaceTrinoView(viewName, updated)
    }

    fun setViewColumnComment(session: ConnectorSession, viewName: SchemaTableName, columnName: String, comment: Optional<String>)
    {
        val definition: ConnectorViewDefinition = getRequiredViewDefinition(session, viewName)

        if (definition.columns.none { it.name == columnName }) {
            throw ColumnNotFoundException(viewName, columnName)
        }
        val columns: List<ConnectorViewDefinition.ViewColumn> = definition.columns.map { column ->
            if (column.name == columnName) ConnectorViewDefinition.ViewColumn(column.name, column.type, comment) else column
        }

        val updatedDefinition = ConnectorViewDefinition(
                definition.originalSql,
                definition.catalog,
                definition.schema,
                columns,
                definition.comment,
                definition.owner,
                definition.isRunAsInvoker,
                definition.path
        )

        replaceTrinoView(viewName, updatedDefinition)
    }

    private fun replaceTrinoView(viewName: SchemaTableName, definition: ConnectorViewDefinition)
    {
        val encoded = DucklakeTrinoViewCodec.encode(definition)
        translateCatalogExceptions {
            catalog.replaceViewMetadata(
                viewName.schemaName,
                viewName.tableName,
                definition.originalSql,
                DucklakeTrinoViewCodec.DIALECT,
                encoded.columnAliases,
                encoded.tags,
            )
        }
    }

    fun dropView(session: ConnectorSession, viewName: SchemaTableName)
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        catalog.getView(viewName.schemaName, viewName.tableName, snapshotId)
            ?: throw ViewNotFoundException(viewName)

        translateCatalogExceptions { catalog.dropView(viewName.schemaName, viewName.tableName) }
    }

    private fun getRequiredViewDefinition(session: ConnectorSession, viewName: SchemaTableName): ConnectorViewDefinition
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val view: DucklakeView = catalog.getView(viewName.schemaName, viewName.tableName, snapshotId)
            ?: throw ViewNotFoundException(viewName)
        if (!isViewAccessible(view)) {
            throw ViewNotFoundException(viewName)
        }
        return DucklakeTrinoViewCodec.decode(view, viewName)
    }

    /**
     * Views this connector LISTS: those whose SQL is Trino SQL (dialect `trino`, or a
     * `trino/<variant>` written by some other Trino connector). Non-Trino dialects (e.g.
     * `duckdb`) are hidden — exposing them needs a SQL transpiler.
     *
     * Listing is deliberately wider than serving: a Trino-family view we cannot decode
     * (foreign variant, missing/inconsistent tags) still shows up in SHOW TABLES so an operator
     * can see it and DROP it; using it fails with a precise INVALID_VIEW error from
     * [DucklakeTrinoViewCodec.decode]. Bulk paths (`getViews`) skip such views with a warning
     * rather than failing the whole listing.
     */
    fun isViewAccessible(view: DucklakeView): Boolean
    {
        if (DucklakeTrinoViewCodec.isTrinoFamily(view)) {
            return true
        }
        log.debug("Skipping view %s with non-Trino dialect: %s (transpiler not configured)", view.viewName, view.dialect)
        return false
    }

    /** Bulk resolution (information_schema.views): incompatible views are skipped, loudly logged. */
    private fun decodeTrinoViewOrSkip(view: DucklakeView, viewName: SchemaTableName): ConnectorViewDefinition?
    {
        val reason = DucklakeTrinoViewCodec.incompatibilityReason(view)
        if (reason != null) {
            log.warn("Skipping view %s in bulk view listing — it cannot be executed by this connector: %s " +
                    "(dialect '%s'). Recreate it from this connector or DROP VIEW it.", viewName, reason, view.dialect)
            return null
        }
        return DucklakeTrinoViewCodec.decode(view, viewName)
    }

    private companion object {
        val log: Logger = Logger.get(DucklakeViewMetadata::class.java)
    }
}
