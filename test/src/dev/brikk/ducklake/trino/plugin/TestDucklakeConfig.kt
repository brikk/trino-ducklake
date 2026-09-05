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

import dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.CALENDAR
import dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.EPOCH
import io.airlift.configuration.ConfigBinder.configBinder
import io.airlift.configuration.ConfigurationFactory
import io.airlift.configuration.ConfigurationInspector
import io.airlift.units.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TestDucklakeConfig {
    @Test
    fun testCatalogPasswordIsRedactedInConfigurationInspection() {
        val password = "synthetic-catalog-password"
        ConfigurationFactory(mapOf(
                "ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake",
                "ducklake.catalog.database-password" to password)).use { factory ->
            assertThat(factory.registerConfigurationClasses { binder ->
                configBinder(binder).bindConfig(DucklakeConfig::class.java)
            }).isEmpty()
            assertThat(factory.validateRegisteredConfigurationProvider()).isEmpty()

            val config = factory.build(DucklakeConfig::class.java)
            assertThat(config.getCatalogDatabasePassword()).isEqualTo(password)
            assertThat(config.toCatalogConfig().catalogDatabasePassword).isEqualTo(password)

            val attribute = ConfigurationInspector().inspect(factory)
                    .flatMap { it.attributes }
                    .single { it.propertyName == "ducklake.catalog.database-password" }
            assertThat(attribute.currentValue).isEqualTo("[REDACTED]")
            assertThat(attribute.defaultValue).isEqualTo("[REDACTED]")
        }
    }

    @Test
    fun testTemporalPartitionEncodingDefaults() {
        val config = DucklakeConfig()

        assertThat(config.getTemporalPartitionEncoding()).isEqualTo(CALENDAR)
        assertThat(config.isTemporalPartitionEncodingReadLeniency()).isTrue()
    }

    @Test
    fun testTemporalPartitionEncodingParsing() {
        val config = DucklakeConfig()
                .setTemporalPartitionEncoding("epoch")
                .setTemporalPartitionEncodingReadLeniency(false)

        assertThat(config.getTemporalPartitionEncoding()).isEqualTo(EPOCH)
        assertThat(config.isTemporalPartitionEncodingReadLeniency()).isFalse()
    }

    @Test
    fun testTemporalPartitionEncodingInvalidValueFails() {
        val config = DucklakeConfig()

        assertThatThrownBy { config.setTemporalPartitionEncoding("invalid-encoding") }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("ducklake.temporal-partition-encoding")
    }

    @Test
    fun testRemoveOrphanFilesMinRetentionDefault() {
        // 7d default (matches Trino's Iceberg connector) — the floor under remove_orphan_files'
        // retention_threshold argument that stops the op deleting freshly-written files.
        assertThat(DucklakeConfig().getRemoveOrphanFilesMinRetention())
                .isEqualTo(Duration.valueOf("7d"))
    }

    @Test
    fun testRemoveOrphanFilesMinRetentionParsing() {
        val config = DucklakeConfig().setRemoveOrphanFilesMinRetention(Duration.valueOf("3d"))
        assertThat(config.getRemoveOrphanFilesMinRetention()).isEqualTo(Duration.valueOf("3d"))
    }

    @Test
    fun testMaintenanceMinRetentionDefaultAndParsing() {
        // 7d default — floors expire_snapshots (retention mode) + cleanup_old_files grace period.
        assertThat(DucklakeConfig().getMaintenanceMinRetention()).isEqualTo(Duration.valueOf("7d"))
        assertThat(DucklakeConfig().setMaintenanceMinRetention(Duration.valueOf("2d")).getMaintenanceMinRetention())
                .isEqualTo(Duration.valueOf("2d"))
    }
}
