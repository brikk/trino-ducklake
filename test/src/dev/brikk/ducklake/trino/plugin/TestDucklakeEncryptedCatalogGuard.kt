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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.DriverManager

/**
 * The connector has no Parquet modular-encryption reader/writer. An encrypted lake must fail at
 * table-handle resolution with a useful error — before encrypted bytes reach the Parquet reader,
 * and before an inlined split can return only part of the table. Metadata tables remain readable
 * so an operator can inspect the lake; metadata-only DDL remains permitted by the catalog library.
 */
class TestDucklakeEncryptedCatalogGuard : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "encrypted-catalog-guard"

    @Test
    fun encryptedLakeFailsClearlyBeforeDataReadButKeepsMetadataAccessible()
    {
        val isolated = getIsolatedCatalog()
        DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { connection ->
            connection.createStatement().use { statement ->
                statement.executeUpdate("UPDATE ducklake_metadata SET value = 'true' WHERE key = 'encrypted'")
            }
        }

        assertQueryFails(
                "SELECT * FROM test_schema.simple_table",
                ".*DuckLake catalog is encrypted; this connector cannot read encrypted data files.*")
        // The metadata-table path resolves the base table but reads no encrypted data file.
        assertThat(computeActual("SELECT count(*) FROM test_schema.\"simple_table\$snapshots\"").onlyValue as Long)
                .isGreaterThan(0)
        // Metadata-only DDL remains useful for diagnosis/administration.
        computeActual("CREATE SCHEMA encrypted_admin")
        computeActual("DROP SCHEMA encrypted_admin")
    }
}
