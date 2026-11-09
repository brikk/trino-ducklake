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

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.DucklakeCatalogFormat
import dev.brikk.ducklake.catalog.DucklakeCleanupLimits.MAX_BATCH_SIZE
import io.airlift.configuration.Config
import io.airlift.configuration.ConfigDescription
import io.airlift.configuration.ConfigSecuritySensitive
import io.airlift.units.Duration
import io.airlift.units.MinDuration
import jakarta.validation.constraints.AssertTrue
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotNull
import jakarta.validation.constraints.Pattern
import java.time.Instant
import java.time.Duration as JavaDuration
import java.time.format.DateTimeParseException
import java.util.Optional
import java.util.OptionalLong

/**
 * Configuration for Ducklake connector.
 */
class DucklakeConfig {
    private var catalogDatabaseUrl: String? = null
    private var catalogDatabaseUser: String? = null
    private var catalogDatabasePassword: String? = null
    private var dataPath: String? = null
    private var maxCatalogConnections: Int = 10
    private var minIdleCatalogConnections: Int = 1
    private var catalogConnectionIdleTimeout: Duration? = null
    private var catalogConnectionMaxLifetime: Duration? = null
    private var catalogConnectionTimeout: Duration = Duration.valueOf("30s")
    private var defaultSnapshotId: OptionalLong = OptionalLong.empty()
    private var defaultSnapshotTimestamp: Optional<Instant> = Optional.empty()
    private var temporalPartitionEncoding: DucklakeTemporalPartitionEncoding = DucklakeTemporalPartitionEncoding.CALENDAR
    private var temporalPartitionEncodingReadLeniency: Boolean = true
    private var removeOrphanFilesMinRetention: Duration = Duration.valueOf("7d")
    private var maintenanceMinRetention: Duration = Duration.valueOf("7d")
    private var cleanupBatchSize: Int = 256
    private var cleanupMaxReferenceFiles: Long = 1_000_000L
    private var enableFinal11Reads: Boolean = false
    private var catalogFormatVersion: String? = null
    private var readMaxFiles = 10_000
    private var readMaxRows = 1_000_000L
    private var readMaxCdcEvents = 100_000L
    private var readTimeoutSeconds = 30
    private var readPageSize = 128

    @Min(1) @Max(1024)
    fun getReadPageSize(): Int = readPageSize
    @Config("ducklake.read.page-size")
    fun setReadPageSize(value: Int): DucklakeConfig { readPageSize = value; return this }

    @Min(1) @Max(3600)
    fun getReadTimeoutSeconds(): Int = readTimeoutSeconds
    @Config("ducklake.read.timeout-seconds")
    fun setReadTimeoutSeconds(value: Int): DucklakeConfig { readTimeoutSeconds = value; return this }

    @Min(1) @Max(100_000)
    fun getReadMaxFiles(): Int = readMaxFiles
    @Config("ducklake.read.max-files")
    fun setReadMaxFiles(value: Int): DucklakeConfig { readMaxFiles = value; return this }

    @Min(1) @Max(10_000_000)
    fun getReadMaxRows(): Long = readMaxRows
    @Config("ducklake.read.max-physical-rows")
    fun setReadMaxRows(value: Long): DucklakeConfig { readMaxRows = value; return this }

    @Min(1) @Max(1_000_000)
    fun getReadMaxCdcEvents(): Long = readMaxCdcEvents
    @Config("ducklake.read.max-cdc-events")
    fun setReadMaxCdcEvents(value: Long): DucklakeConfig { readMaxCdcEvents = value; return this }

    @Pattern(regexp = "1\\.0|1\\.1", message = "must be 1.0 or 1.1")
    fun getCatalogFormatVersion(): String = catalogFormatVersion ?: if (enableFinal11Reads) "1.1" else "1.0"

    @Config("ducklake.catalog.format-version")
    @ConfigDescription("Required DuckLake metadata format: 1.0 (default) or separately qualified read-only 1.1. " +
        "A mismatch is refused; this setting never creates or migrates metadata.")
    fun setCatalogFormatVersion(value: String): DucklakeConfig {
        catalogFormatVersion = value
        return this
    }

    @AssertTrue(message = "ducklake.catalog.format-version=1.0 conflicts with ducklake.experimental.enable-final-1.1-reads=true")
    fun isCatalogFormatSelectionConsistent(): Boolean = catalogFormatVersion != "1.0" || !enableFinal11Reads

    fun isEnableFinal11Reads(): Boolean = getCatalogFormatVersion() == "1.1"

    internal fun validateCatalogFormatSelection() {
        require(getCatalogFormatVersion() in setOf("1.0", "1.1")) { "ducklake.catalog.format-version must be 1.0 or 1.1" }
        require(isCatalogFormatSelectionConsistent()) { "Conflicting DuckLake format selection: explicit 1.0 with the legacy 1.1 flag" }
    }

    @Config("ducklake.experimental.enable-final-1.1-reads")
    @ConfigDescription("Legacy developer alias selecting read-only format 1.1; prefer ducklake.catalog.format-version. " +
            "Requires the separately qualified 1.1 build and disables connector writes and maintenance.")
    fun setEnableFinal11Reads(value: Boolean): DucklakeConfig {
        enableFinal11Reads = value
        return this
    }

    @NotNull
    fun getCatalogDatabaseUrl(): String? {
        return catalogDatabaseUrl
    }

    @Config("ducklake.catalog.database-url")
    @ConfigDescription("JDBC URL for the Ducklake catalog database " +
            "(e.g., jdbc:postgresql://host/db or jdbc:mysql://host:3306/db); the backend dialect " +
            "is inferred from the URL scheme")
    fun setCatalogDatabaseUrl(catalogDatabaseUrl: String?): DucklakeConfig {
        this.catalogDatabaseUrl = catalogDatabaseUrl
        return this
    }

    fun getCatalogDatabaseUser(): String? {
        return catalogDatabaseUser
    }

    @Config("ducklake.catalog.database-user")
    @ConfigDescription("Username for the catalog database (required for PostgreSQL and MySQL)")
    fun setCatalogDatabaseUser(catalogDatabaseUser: String?): DucklakeConfig {
        this.catalogDatabaseUser = catalogDatabaseUser
        return this
    }

    fun getCatalogDatabasePassword(): String? {
        return catalogDatabasePassword
    }

    @Config("ducklake.catalog.database-password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for the catalog database (required for PostgreSQL and MySQL)")
    fun setCatalogDatabasePassword(catalogDatabasePassword: String?): DucklakeConfig {
        this.catalogDatabasePassword = catalogDatabasePassword
        return this
    }

    fun getDataPath(): String? {
        return dataPath
    }

    @Config("ducklake.data-path")
    @ConfigDescription("Base path for relative data file paths (from ducklake_metadata table)")
    fun setDataPath(dataPath: String?): DucklakeConfig {
        this.dataPath = dataPath
        return this
    }

    @Min(1)
    fun getMaxCatalogConnections(): Int {
        return maxCatalogConnections
    }

    @Config("ducklake.catalog.max-connections")
    @ConfigDescription("Maximum number of JDBC connections to the catalog database")
    fun setMaxCatalogConnections(maxCatalogConnections: Int): DucklakeConfig {
        this.maxCatalogConnections = maxCatalogConnections
        return this
    }

    @Min(0)
    fun getMinIdleCatalogConnections(): Int = minIdleCatalogConnections

    @Config("ducklake.catalog.min-idle-connections")
    @ConfigDescription("Minimum idle metadata connections (0..max-connections); default 1")
    fun setMinIdleCatalogConnections(value: Int): DucklakeConfig {
        minIdleCatalogConnections = value
        return this
    }

    @AssertTrue(message = "ducklake.catalog.min-idle-connections must not exceed ducklake.catalog.max-connections")
    fun isCatalogPoolSizeValid(): Boolean = minIdleCatalogConnections <= maxCatalogConnections

    fun getCatalogConnectionIdleTimeout(): Duration? = catalogConnectionIdleTimeout

    @Config("ducklake.catalog.connection-idle-timeout")
    @ConfigDescription("Idle timeout above min-idle-connections; unset uses Hikari's default, 0 disables; otherwise at least 10s")
    fun setCatalogConnectionIdleTimeout(value: Duration?): DucklakeConfig {
        catalogConnectionIdleTimeout = value
        return this
    }

    @AssertTrue(message = "ducklake.catalog.connection-idle-timeout must be unset, 0s, or at least 10s")
    fun isCatalogConnectionIdleTimeoutValid(): Boolean = validPoolLifetime(catalogConnectionIdleTimeout, "10s")

    fun getCatalogConnectionMaxLifetime(): Duration? = catalogConnectionMaxLifetime

    @Config("ducklake.catalog.connection-max-lifetime")
    @ConfigDescription("Maximum pooled connection age; unset uses Hikari's default, 0 disables; otherwise at least 30s. " +
        "Keep below a proxy's server_lifetime.")
    fun setCatalogConnectionMaxLifetime(value: Duration?): DucklakeConfig {
        catalogConnectionMaxLifetime = value
        return this
    }

    @AssertTrue(message = "ducklake.catalog.connection-max-lifetime must be unset, 0s, or at least 30s")
    fun isCatalogConnectionMaxLifetimeValid(): Boolean = validPoolLifetime(catalogConnectionMaxLifetime, "30s")

    @NotNull
    @MinDuration("250ms")
    fun getCatalogConnectionTimeout(): Duration = catalogConnectionTimeout

    @Config("ducklake.catalog.connection-timeout")
    @ConfigDescription("Maximum wait to acquire a metadata connection; default 30s, minimum 250ms")
    fun setCatalogConnectionTimeout(value: Duration): DucklakeConfig {
        catalogConnectionTimeout = value
        return this
    }

    private fun validPoolLifetime(value: Duration?, minimum: String): Boolean =
        value == null || value.compareTo(Duration.valueOf("0s")) == 0 || value.compareTo(Duration.valueOf(minimum)) >= 0

    fun getDefaultSnapshotId(): OptionalLong {
        return defaultSnapshotId
    }

    @Config("ducklake.default-snapshot-id")
    @ConfigDescription("Optional default DuckLake snapshot ID for reads when query/session does not specify a snapshot")
    fun setDefaultSnapshotId(defaultSnapshotId: Long?): DucklakeConfig {
        this.defaultSnapshotId = if (defaultSnapshotId == null) OptionalLong.empty() else OptionalLong.of(defaultSnapshotId)
        return this
    }

    fun getDefaultSnapshotTimestamp(): Optional<Instant> {
        return defaultSnapshotTimestamp
    }

    @Config("ducklake.default-snapshot-timestamp")
    @ConfigDescription("Optional default DuckLake snapshot timestamp (ISO-8601 instant) for reads when query/session does not specify a snapshot")
    fun setDefaultSnapshotTimestamp(defaultSnapshotTimestamp: String?): DucklakeConfig {
        if (defaultSnapshotTimestamp == null) {
            this.defaultSnapshotTimestamp = Optional.empty()
            return this
        }

        try {
            this.defaultSnapshotTimestamp = Optional.of(Instant.parse(defaultSnapshotTimestamp))
        }
        catch (e: DateTimeParseException) {
            throw IllegalArgumentException("Invalid ducklake.default-snapshot-timestamp value: $defaultSnapshotTimestamp", e)
        }
        return this
    }

    @NotNull
    @Deprecated("")
    fun getTemporalPartitionEncoding(): DucklakeTemporalPartitionEncoding {
        return temporalPartitionEncoding
    }

    /**
     * @deprecated DuckLake 1.0 settled on calendar encoding for
     * `ducklake_file_partition_value.partition_value` (spec PR
     * <a href="https://github.com/duckdb/ducklake-web/pull/349">duckdb/ducklake-web#349</a>);
     * the epoch path is retained but no longer expected. Leave on the default
     * (`calendar`) for spec-conformant catalogs. Will be removed once a future
     * spec revision either confirms calendar permanently or formally reintroduces
     * epoch as an alternate.
     */
    @Deprecated("")
    @Config("ducklake.temporal-partition-encoding")
    @ConfigDescription("Deprecated. DuckLake 1.0 settled on calendar; epoch path retained for compatibility with pre-resolution catalogs. Values: calendar (default, spec-conformant) or epoch.")
    fun setTemporalPartitionEncoding(temporalPartitionEncoding: String?): DucklakeConfig {
        if (temporalPartitionEncoding == null) {
            this.temporalPartitionEncoding = DucklakeTemporalPartitionEncoding.CALENDAR
            return this
        }

        try {
            this.temporalPartitionEncoding = DucklakeTemporalPartitionEncoding.fromString(temporalPartitionEncoding)
        }
        catch (e: IllegalArgumentException) {
            throw IllegalArgumentException("Invalid ducklake.temporal-partition-encoding value: $temporalPartitionEncoding (expected: calendar or epoch)", e)
        }
        return this
    }

    @Deprecated("")
    fun isTemporalPartitionEncodingReadLeniency(): Boolean {
        return temporalPartitionEncodingReadLeniency
    }

    /**
     * @deprecated Companion to [setTemporalPartitionEncoding]; see that
     * setter's note for the DuckLake 1.0 spec resolution. Leniency only matters when
     * epoch and calendar interpretations diverge, which a v1-conformant catalog never
     * triggers.
     */
    @Deprecated("")
    @Config("ducklake.temporal-partition-encoding-read-leniency")
    @ConfigDescription("Deprecated. If true, temporal partition pruning keeps files unless both calendar and epoch interpretations exclude them. Only relevant when reading mixed-encoding catalogs from before the DuckLake 1.0 calendar-only resolution.")
    fun setTemporalPartitionEncodingReadLeniency(temporalPartitionEncodingReadLeniency: Boolean): DucklakeConfig {
        this.temporalPartitionEncodingReadLeniency = temporalPartitionEncodingReadLeniency
        return this
    }

    @AssertTrue(message = "Only one of ducklake.default-snapshot-id or ducklake.default-snapshot-timestamp may be set")
    fun isSnapshotDefaultsValid(): Boolean {
        return defaultSnapshotId.isEmpty || defaultSnapshotTimestamp.isEmpty
    }

    @NotNull
    @MinDuration("0s")
    fun getRemoveOrphanFilesMinRetention(): Duration {
        return removeOrphanFilesMinRetention
    }

    @Config("ducklake.remove-orphan-files.min-retention")
    @ConfigDescription("Minimum age a file must have before remove_orphan_files may delete it. " +
            "Floors the procedure's retention_threshold argument so the maintenance op cannot " +
            "delete files an in-flight (possibly cross-engine) writer just produced. Default 7d.")
    fun setRemoveOrphanFilesMinRetention(removeOrphanFilesMinRetention: Duration): DucklakeConfig {
        this.removeOrphanFilesMinRetention = removeOrphanFilesMinRetention
        return this
    }

    @NotNull
    @MinDuration("0s")
    fun getMaintenanceMinRetention(): Duration {
        return maintenanceMinRetention
    }

    @Config("ducklake.maintenance.min-retention")
    @ConfigDescription("Minimum age floor shared by expire_snapshots (retention mode — protects " +
            "recent time-travel) and cleanup_old_files (the grace period before a scheduled file " +
            "is physically deleted — protects in-flight, possibly cross-engine, readers). Default 7d.")
    fun setMaintenanceMinRetention(maintenanceMinRetention: Duration): DucklakeConfig {
        this.maintenanceMinRetention = maintenanceMinRetention
        return this
    }

    @Min(1)
    @Max(MAX_BATCH_SIZE.toLong())
    fun getCleanupBatchSize(): Int = cleanupBatchSize

    @Config("ducklake.cleanup.batch-size")
    @ConfigDescription("Maximum scheduled entries processed per cleanup_old_files call and retained references fetched per page (1..1024).")
    fun setCleanupBatchSize(cleanupBatchSize: Int): DucklakeConfig {
        this.cleanupBatchSize = cleanupBatchSize
        return this
    }

    @Min(1)
    @Max(10_000_000)
    fun getCleanupMaxReferenceFiles(): Long = cleanupMaxReferenceFiles

    @Config("ducklake.cleanup.max-reference-files")
    @ConfigDescription("Maximum retained data/delete references inspected per cleanup_old_files call; " +
            "exceeding it fails before deletion or queue cancellation (1..10000000).")
    fun setCleanupMaxReferenceFiles(cleanupMaxReferenceFiles: Long): DucklakeConfig {
        this.cleanupMaxReferenceFiles = cleanupMaxReferenceFiles
        return this
    }

    fun toCatalogConfig(): DucklakeCatalogConfig {
        validateCatalogFormatSelection()
        val catalogConfig = DucklakeCatalogConfig()
        catalogConfig.catalogDatabaseUrl = catalogDatabaseUrl
        catalogConfig.catalogDatabaseUser = catalogDatabaseUser
        catalogConfig.catalogDatabasePassword = catalogDatabasePassword
        catalogConfig.dataPath = dataPath
        catalogConfig.maxCatalogConnections = maxCatalogConnections
        catalogConfig.minIdleCatalogConnections = minIdleCatalogConnections
        catalogConfig.catalogConnectionIdleTimeout = catalogConnectionIdleTimeout?.let { JavaDuration.ofMillis(it.toMillis()) }
        catalogConfig.catalogConnectionMaxLifetime = catalogConnectionMaxLifetime?.let { JavaDuration.ofMillis(it.toMillis()) }
        catalogConfig.catalogConnectionTimeout = JavaDuration.ofMillis(catalogConnectionTimeout.toMillis())
        catalogConfig.enableFinal11Reads = isEnableFinal11Reads()
        catalogConfig.requiredCatalogFormat = when (getCatalogFormatVersion()) {
            "1.0" -> DucklakeCatalogFormat.V1_0
            "1.1" -> DucklakeCatalogFormat.V1_1
            else -> error("Catalog format selection was not validated")
        }
        return catalogConfig
    }
}
