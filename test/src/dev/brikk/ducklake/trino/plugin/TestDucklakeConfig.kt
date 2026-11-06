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

import com.google.inject.ConfigurationException
import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
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
    fun catalogPoolDefaultsPreserveLibraryDefaults() {
        val config = DucklakeConfig()
        val mapped = config.toCatalogConfig()
        val defaults = DucklakeCatalogConfig()
        assertThat(config.getMinIdleCatalogConnections()).isEqualTo(1)
        assertThat(config.getCatalogConnectionIdleTimeout()).isNull()
        assertThat(config.getCatalogConnectionMaxLifetime()).isNull()
        assertThat(config.getCatalogConnectionTimeout()).isEqualTo(Duration.valueOf("30s"))
        assertThat(mapped.maxCatalogConnections).isEqualTo(defaults.maxCatalogConnections)
        assertThat(mapped.minIdleCatalogConnections).isEqualTo(defaults.minIdleCatalogConnections)
        assertThat(mapped.catalogConnectionIdleTimeout).isEqualTo(defaults.catalogConnectionIdleTimeout)
        assertThat(mapped.catalogConnectionMaxLifetime).isEqualTo(defaults.catalogConnectionMaxLifetime)
        assertThat(mapped.catalogConnectionTimeout).isEqualTo(defaults.catalogConnectionTimeout)
    }

    @Test
    fun poolPropertiesBindAndJdbcUrlParametersPassThroughUnchanged() {
        val url = "jdbc:postgresql://pgbouncer.invalid:6432/ducklake?prepareThreshold=0&sslmode=require&ApplicationName=Trino%20DuckLake"
        ConfigurationFactory(mapOf("ducklake.catalog.database-url" to url,
            "ducklake.catalog.max-connections" to "6", "ducklake.catalog.min-idle-connections" to "0",
            "ducklake.catalog.connection-idle-timeout" to "45s", "ducklake.catalog.connection-max-lifetime" to "20m",
            "ducklake.catalog.connection-timeout" to "1500ms")).use { factory ->
            val config = factory.build(DucklakeConfig::class.java)
            assertThat(config.getCatalogDatabaseUrl()).isEqualTo(url)
            val mapped = config.toCatalogConfig()
            assertThat(mapped.catalogDatabaseUrl).isEqualTo(url)
            assertThat(mapped.maxCatalogConnections).isEqualTo(6)
            assertThat(mapped.minIdleCatalogConnections).isZero()
            assertThat(mapped.catalogConnectionIdleTimeout).isEqualTo(java.time.Duration.ofSeconds(45))
            assertThat(mapped.catalogConnectionMaxLifetime).isEqualTo(java.time.Duration.ofMinutes(20))
            assertThat(mapped.catalogConnectionTimeout).isEqualTo(java.time.Duration.ofMillis(1500))
        }
    }

    @Test
    fun poolTimeoutBoundsRejectValuesHikariWouldSilentlyReplace() {
        for ((key, valid, invalid) in listOf(
            Triple("ducklake.catalog.connection-idle-timeout", listOf("0s", "10s"), listOf("-1s", "1ms", "9999ms")),
            Triple("ducklake.catalog.connection-max-lifetime", listOf("0s", "30s"), listOf("-1s", "1ms", "29999ms")),
            Triple("ducklake.catalog.connection-timeout", listOf("250ms", "30s"), listOf("-1s", "0s", "249ms")),
        )) {
            for (value in valid + invalid) {
                ConfigurationFactory(mapOf("ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake",
                    key to value)).use { factory ->
                    if (value in invalid) assertThatThrownBy { factory.build(DucklakeConfig::class.java) }
                        .isInstanceOf(ConfigurationException::class.java).hasMessageContaining(key)
                    else {
                        val mapped = factory.build(DucklakeConfig::class.java).toCatalogConfig()
                        val actual = when (key) {
                            "ducklake.catalog.connection-idle-timeout" -> mapped.catalogConnectionIdleTimeout
                            "ducklake.catalog.connection-max-lifetime" -> mapped.catalogConnectionMaxLifetime
                            else -> mapped.catalogConnectionTimeout
                        }
                        assertThat(actual).isEqualTo(java.time.Duration.ofMillis(Duration.valueOf(value).toMillis()))
                    }
                }
            }
        }
    }

    @Test
    fun minIdleConnectionsMustFitThePool() {
        for (minimum in listOf(0, 2, -1, 3)) {
            ConfigurationFactory(mapOf("ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake",
                "ducklake.catalog.max-connections" to "2", "ducklake.catalog.min-idle-connections" to minimum.toString())).use { factory ->
                if (minimum in 0..2) assertThat(factory.build(DucklakeConfig::class.java).toCatalogConfig().minIdleCatalogConnections)
                    .isEqualTo(minimum)
                else assertThatThrownBy { factory.build(DucklakeConfig::class.java) }
                    .isInstanceOf(ConfigurationException::class.java).hasMessageContaining("ducklake.catalog.min-idle-connections")
            }
        }
    }

    @Test
    fun explicitCatalogFormatSelectionIsIndependentAndNeverGuessed() {
        assertThat(DucklakeConfig().getCatalogFormatVersion()).isEqualTo("1.0")
        for (version in listOf("1.0", "1.1")) {
            ConfigurationFactory(mapOf("ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake",
                "ducklake.catalog.format-version" to version)).use { factory ->
                val config = factory.build(DucklakeConfig::class.java)
                assertThat(config.getCatalogFormatVersion()).isEqualTo(version)
                assertThat(config.isEnableFinal11Reads()).isEqualTo(version == "1.1")
                assertThat(config.toCatalogConfig().requiredCatalogFormat).isEqualTo(
                    if (version == "1.0") dev.brikk.ducklake.catalog.DucklakeCatalogFormat.V1_0
                    else dev.brikk.ducklake.catalog.DucklakeCatalogFormat.V1_1)
            }
        }
        assertThatThrownBy { DucklakeConfig().setCatalogFormatVersion("auto").toCatalogConfig() }
            .hasMessageContaining("must be 1.0 or 1.1")
        assertThatThrownBy { DucklakeConfig().setCatalogFormatVersion("1.0").setEnableFinal11Reads(true).toCatalogConfig() }
            .hasMessageContaining("Conflicting DuckLake format")
    }

    @Test
    fun readPageAndDeadlineDefaultsAndBounds() {
        assertThat(DucklakeConfig().getReadPageSize()).isEqualTo(128)
        assertThat(DucklakeConfig().getReadTimeoutSeconds()).isEqualTo(30)
        for ((key, valid, invalid) in listOf(
            Triple("ducklake.read.page-size", listOf(1, 1024), listOf(0, 1025)),
            Triple("ducklake.read.timeout-seconds", listOf(1, 3600), listOf(0, 3601)),
        )) {
            for (value in valid + invalid) {
                ConfigurationFactory(mapOf("ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake",
                    key to value.toString())).use { factory ->
                    if (value in invalid) assertThatThrownBy { factory.build(DucklakeConfig::class.java) }
                        .isInstanceOf(ConfigurationException::class.java).hasMessageContaining(key)
                    else {
                        val config = factory.build(DucklakeConfig::class.java)
                        assertThat(if (key.endsWith("page-size")) config.getReadPageSize() else config.getReadTimeoutSeconds()).isEqualTo(value)
                    }
                }
            }
        }
    }
    @Test
    fun final11IsExplicitOnTheReleasedLibrary() {
        assertThat(DucklakeConfig().isEnableFinal11Reads()).isFalse()
        val config = DucklakeConfig().setEnableFinal11Reads(true)
        assertThat(config.toCatalogConfig().enableFinal11Reads).isTrue()
    }
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

    @Test
    fun testCleanupLimitsDefaultsFromConfigurationFactory() {
        ConfigurationFactory(mapOf(
                "ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake")).use { factory ->
            val config = factory.build(DucklakeConfig::class.java)
            assertThat(config.getCleanupBatchSize()).isEqualTo(256)
            assertThat(config.getCleanupMaxReferenceFiles()).isEqualTo(1_000_000L)
        }
    }

    @Test
    fun testCleanupBatchSizeAcceptsBoundaries() {
        for (value in listOf(1, 1024)) {
            ConfigurationFactory(mapOf(
                    "ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake",
                    "ducklake.cleanup.batch-size" to value.toString())).use { factory ->
                assertThat(factory.build(DucklakeConfig::class.java).getCleanupBatchSize()).isEqualTo(value)
            }
        }
    }

    @Test
    fun testCleanupBatchSizeRejectsOutOfRangeValues() {
        for (value in listOf(0, -1, 1025)) {
            ConfigurationFactory(mapOf(
                    "ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake",
                    "ducklake.cleanup.batch-size" to value.toString())).use { factory ->
                assertThatThrownBy { factory.build(DucklakeConfig::class.java) }
                        .`as`("cleanup batch size %s is invalid", value)
                        .isInstanceOf(ConfigurationException::class.java)
                        .hasMessageContaining("ducklake.cleanup.batch-size")
            }
        }
    }

    @Test
    fun testCleanupMaxReferenceFilesAcceptsBoundaries() {
        for (value in listOf(1L, 10_000_000L)) {
            ConfigurationFactory(mapOf(
                    "ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake",
                    "ducklake.cleanup.max-reference-files" to value.toString())).use { factory ->
                assertThat(factory.build(DucklakeConfig::class.java).getCleanupMaxReferenceFiles()).isEqualTo(value)
            }
        }
    }

    @Test
    fun testCleanupMaxReferenceFilesRejectsOutOfRangeValues() {
        for (value in listOf(0L, -1L, 10_000_001L)) {
            ConfigurationFactory(mapOf(
                    "ducklake.catalog.database-url" to "jdbc:postgresql://example.invalid/ducklake",
                    "ducklake.cleanup.max-reference-files" to value.toString())).use { factory ->
                assertThatThrownBy { factory.build(DucklakeConfig::class.java) }
                        .`as`("cleanup reference-file limit %s is invalid", value)
                        .isInstanceOf(ConfigurationException::class.java)
                        .hasMessageContaining("ducklake.cleanup.max-reference-files")
            }
        }
    }
}
