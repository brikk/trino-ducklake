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

import dev.brikk.ducklake.catalog.DucklakeCatalogCorruptionException
import dev.brikk.ducklake.catalog.DucklakeEncryptedCatalogUnsupportedException
import dev.brikk.ducklake.catalog.DucklakeEntityAlreadyExistsException
import dev.brikk.ducklake.catalog.DucklakeEntityNotFoundException
import dev.brikk.ducklake.catalog.DucklakeException
import dev.brikk.ducklake.catalog.DucklakeInvalidOperationException
import dev.brikk.ducklake.catalog.DucklakeSchemaNotEmptyException
import dev.brikk.ducklake.catalog.DucklakeUnsupportedCatalogVersionException
import dev.brikk.ducklake.catalog.TransactionConflictException
import io.trino.spi.StandardErrorCode.ALREADY_EXISTS
import io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS
import io.trino.spi.StandardErrorCode.NOT_FOUND
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY
import io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND
import io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND
import io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT
import io.trino.spi.TrinoException
import java.util.Locale

/** Translation of typed catalog failures to Trino's stable user-facing error classes. */
internal object DucklakeCatalogExceptions
{
    fun translateCatalogExceptions(action: Runnable)
    {
        try {
            action.run()
        }
        catch (e: DucklakeException) {
            throw translateCatalogException(e)
        }
    }

    /** Pure mapping, so every catalog exception class is pinned by a unit test. */
    fun translateCatalogException(e: DucklakeException): TrinoException =
            when (e) {
                is TransactionConflictException -> TrinoException(TRANSACTION_CONFLICT, e.message, e)
                is DucklakeSchemaNotEmptyException -> TrinoException(SCHEMA_NOT_EMPTY, e.message, e)
                is DucklakeEntityNotFoundException -> TrinoException(
                        when (e.entityKind.lowercase(Locale.ROOT)) {
                            "schema" -> SCHEMA_NOT_FOUND
                            "table", "view" -> TABLE_NOT_FOUND
                            "column", "field", "partition column" -> COLUMN_NOT_FOUND
                            else -> NOT_FOUND
                        },
                        e.message,
                        e)
                is DucklakeEntityAlreadyExistsException -> TrinoException(ALREADY_EXISTS, e.message, e)
                is DucklakeInvalidOperationException -> TrinoException(INVALID_ARGUMENTS, e.message, e)
                is DucklakeEncryptedCatalogUnsupportedException,
                is DucklakeUnsupportedCatalogVersionException -> TrinoException(NOT_SUPPORTED, e.message, e)
                is DucklakeCatalogCorruptionException -> TrinoException(GENERIC_INTERNAL_ERROR, e.message, e)
                else -> TrinoException(GENERIC_INTERNAL_ERROR, e.message, e)
            }
}
