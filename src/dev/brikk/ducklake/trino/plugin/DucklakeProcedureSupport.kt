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
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.TransactionConflictException
import io.airlift.units.Duration
import io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT
import io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND
import io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND
import io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.procedure.Procedure
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType

/** Argument parsing, catalog lookups and registration shared by the `system.*` procedures. */
internal object DucklakeProcedureSupport {
    const val DEFAULT_RETENTION: String = "7d"

    /** A `system.<name>` procedure bound to the public method [methodName] of [target]. */
    fun systemProcedure(
            name: String,
            arguments: List<Procedure.Argument>,
            target: Any,
            methodName: String,
            vararg parameterTypes: Class<*>,
    ): Procedure {
        val type = MethodType.methodType(Void.TYPE, ConnectorSession::class.java, *parameterTypes)
        val handle: MethodHandle = MethodHandles.publicLookup().findVirtual(target.javaClass, methodName, type)
        return Procedure("system", name, arguments, handle.bindTo(target), true)
    }

    fun requireArg(value: String?, name: String): String {
        if (value.isNullOrEmpty()) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "$name is required")
        }
        return value
    }

    fun requireSchemaForTable(schemaName: String?, tableName: String?) {
        if (!tableName.isNullOrEmpty() && schemaName.isNullOrEmpty()) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT,
                    "table_name requires schema_name (a table can't be named without its schema)")
        }
    }

    /**
     * Parses `retention_threshold` (blank means [DEFAULT_RETENTION]) and rejects values below
     * [minimum], naming [floorProperty] and why the floor exists ([refusal]).
     */
    fun parseRetention(value: String?, minimum: Duration, floorProperty: String, refusal: String): Duration {
        val raw = if (value.isNullOrBlank()) DEFAULT_RETENTION else value
        val retention = try {
            Duration.valueOf(raw)
        }
        catch (e: IllegalArgumentException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid retention_threshold '$raw': ${e.message}", e)
        }
        if (retention < minimum) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT,
                    "retention_threshold $retention is below the minimum $minimum (set by $floorProperty); $refusal")
        }
        return retention
    }

    fun resolveSchema(catalog: DucklakeCatalog, schemaName: String, snapshotId: Long): DucklakeSchema =
        catalog.getSchema(schemaName, snapshotId)
                ?: throw TrinoException(SCHEMA_NOT_FOUND, "Schema not found: $schemaName")

    fun resolveTable(
            catalog: DucklakeCatalog,
            schemaName: String,
            tableName: String,
            snapshotId: Long,
    ): Pair<DucklakeSchema, DucklakeTable> {
        val schema = resolveSchema(catalog, schemaName, snapshotId)
        val table = catalog.getTable(schemaName, tableName, snapshotId)
                ?: throw TrinoException(TABLE_NOT_FOUND, "Table not found: $schemaName.$tableName")
        return schema to table
    }

    inline fun <T> translatingConflicts(commit: () -> T): T =
        try {
            commit()
        }
        catch (e: TransactionConflictException) {
            throw TrinoException(TRANSACTION_CONFLICT, e.message, e)
        }
}
