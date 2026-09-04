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
import dev.brikk.ducklake.catalog.DucklakeInvalidOperationException
import dev.brikk.ducklake.catalog.DucklakeSchemaNotEmptyException
import dev.brikk.ducklake.catalog.DucklakeUnsupportedCatalogVersionException
import dev.brikk.ducklake.catalog.LogicalConflictException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

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

class TestDucklakeCatalogExceptionMapping {
    @Test
    fun typedCatalogExceptionsMapToStableTrinoCodes()
    {
        assertMapped(LogicalConflictException("stale"), TRANSACTION_CONFLICT)
        assertMapped(DucklakeSchemaNotEmptyException("s", listOf("views")), SCHEMA_NOT_EMPTY)
        assertMapped(DucklakeEntityNotFoundException("schema", "s"), SCHEMA_NOT_FOUND)
        assertMapped(DucklakeEntityNotFoundException("table", "s.t"), TABLE_NOT_FOUND)
        assertMapped(DucklakeEntityNotFoundException("view", "s.v"), TABLE_NOT_FOUND)
        assertMapped(DucklakeEntityNotFoundException("column", "c"), COLUMN_NOT_FOUND)
        assertMapped(DucklakeEntityNotFoundException("field", "r.x"), COLUMN_NOT_FOUND)
        assertMapped(DucklakeEntityNotFoundException("partition column", "p"), COLUMN_NOT_FOUND)
        assertMapped(DucklakeEntityNotFoundException("future kind", "x"), NOT_FOUND)
        assertMapped(DucklakeEntityAlreadyExistsException("table", "s.t"), ALREADY_EXISTS)
        assertMapped(DucklakeInvalidOperationException("bad operation"), INVALID_ARGUMENTS)
        assertMapped(DucklakeEncryptedCatalogUnsupportedException("jdbc:test"), NOT_SUPPORTED)
        assertMapped(DucklakeUnsupportedCatalogVersionException("jdbc:test", "0.3"), NOT_SUPPORTED)
        assertMapped(DucklakeCatalogCorruptionException("broken row"), GENERIC_INTERNAL_ERROR)
    }

    private fun assertMapped(catalogException: dev.brikk.ducklake.catalog.DucklakeException, expected: io.trino.spi.StandardErrorCode)
    {
        val mapped = DucklakeMetadata.translateCatalogException(catalogException)
        assertThat(mapped.errorCode).isEqualTo(expected.toErrorCode())
        assertThat(mapped.message).isEqualTo(catalogException.message)
        assertThat(mapped.cause).isSameAs(catalogException)
    }
}
