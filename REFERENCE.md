# Trino DuckLake reference

This reference describes the normal Trino 483/484 build using catalog library 0.15.1
and DuckLake 1.0 metadata. It is not a blanket physical-type or production-safety
certification. Start with the [installation guide](README.md) and [SQL samples](SAMPLES.md).

## Catalog configuration

Set these in `etc/catalog/<catalog>.properties` on the Trino nodes.

| Property | Default | Meaning |
| --- | --- | --- |
| `connector.name` | Required: `ducklake` | Connector factory |
| `ducklake.catalog.database-url` | Required | JDBC URL for the metadata database |
| `ducklake.catalog.database-user` | Unset | Database username, as required by its authentication |
| `ducklake.catalog.database-password` | Unset | Database password, as required by its authentication |
| `ducklake.data-path` | Required | Base location of data files |
| `ducklake.catalog.max-connections` | `10` | Catalog connection-pool limit |
| `ducklake.catalog.min-idle-connections` | `1` | Idle metadata connections to retain; 0–`max-connections` |
| `ducklake.catalog.connection-idle-timeout` | Unset (Hikari default: `10m`) | Retire idle connections above the minimum; `0s` disables, otherwise at least `10s` |
| `ducklake.catalog.connection-max-lifetime` | Unset (Hikari default: `30m`) | Maximum pooled connection age; `0s` disables, otherwise at least `30s`; keep below the proxy's `server_lifetime` |
| `ducklake.catalog.connection-timeout` | `30s` | Maximum wait to acquire a metadata connection; minimum `250ms` |
| `ducklake.catalog.format-version` | `1.0` | Required stored DuckLake format family: `1.0` (also admits the older 0.4 layout), or separately qualified read-only `1.1`; mismatch refuses reads and writes, never migrates metadata |
| `ducklake.default-snapshot-id` | Unset | Catalog-wide read snapshot pin |
| `ducklake.default-snapshot-timestamp` | Unset | Read pin as an ISO-8601 instant |
| `ducklake.remove-orphan-files.min-retention` | `7d` | Minimum orphan-file age |
| `ducklake.maintenance.min-retention` | `7d` | Retention floor for age-based expiration/cleanup |
| `ducklake.cleanup.batch-size` | `256` | Selected requests per cleanup call and reference page size; range 1–1024 |
| `ducklake.cleanup.max-reference-files` | `1000000` | Retained-reference work budget per cleanup call; range 1–10000000 |
| `ducklake.temporal-partition-encoding` | `calendar` | Deprecated compatibility setting; `epoch` is for legacy catalogs |
| `ducklake.temporal-partition-encoding-read-leniency` | `true` | Deprecated legacy-encoding read compatibility |

Set at most one catalog-level snapshot pin. Resolution precedence is query clause,
then session pin, then catalog pin, then the current snapshot. Do not change temporal
encoding settings as a substitute for verifying the stored partition representation.

For the normal 1.0 release, set `ducklake.catalog.format-version=1.0` explicitly or
use its default. The actual stored version is checked in each owned read view and
before writes, including after a native engine migrates an existing catalog. The
connector does not create, downgrade or migrate the underlying DuckLake metadata.
Choose its format separately when initializing it with DuckDB.

Format 1.1 remains a separate read-only validation/release path, documented with
its work limits in [the developer guide](dev-docs/CATALOG-CANDIDATE.md). Selecting
1.1 in a build that only supports 1.0 fails clearly before opening a connection.

### PostgreSQL

Use `jdbc:postgresql://host:5432/database` and database credentials. The driver is
bundled. The database must already contain a DuckLake schema. PostgreSQL is the
maintained shared-catalog path for concurrent Trino/DuckDB access.

#### PgBouncer transaction pooling — local 0.16 trial only

The transaction-pooling isolation fix is currently in the owner-authorized local
0.16 trial, **not the normal 0.15.1 dependency**. With 0.15.1, use direct PostgreSQL
connections or session pooling: transaction pooling can silently weaken a pinned
read session's isolation. Do not treat the prepared-statement setting below as a
fix for that isolation issue. Final 0.16.0 adoption is still pending.

For the fixed catalog behind PgBouncer in **transaction mode**, configure either:

- PgBouncer **1.21 or newer** with `max_prepared_statements > 0` (default `200`
  since 1.22), or
- `prepareThreshold=0` on the PostgreSQL JDBC URL, disabling named server-prepared
  statements for that client.

Without either, operations fail with `prepared statement "S_n" already exists`.
The connector passes JDBC URL parameters through unchanged. For example:

```properties
ducklake.catalog.database-url=jdbc:postgresql://pgbouncer:6432/ducklake?prepareThreshold=0&sslmode=require
```

If the URL already has a query string, append `&prepareThreshold=0`, not another
`?`. Use the pool settings above to size/recycle metadata connections; keep
`connection-max-lifetime` below PgBouncer's `server_lifetime`. Leaving idle timeout
and maximum lifetime unset preserves the catalog/Hikari defaults. These pool
properties configure client connections, not PgBouncer's server pool.

### MySQL

Use `jdbc:mysql://host:3306/database` and database credentials. MySQL Connector/J is
bundled. Initialize the DuckLake schema before connecting Trino, for example with
the DuckDB `mysql` and `ducklake` extensions using `ducklake:mysql:db=... host=...`.
Trino read/write is supported; DuckDB reading the same MySQL-backed lake is not a
qualified interoperability configuration in the maintained lane. Use PostgreSQL
when both engines need to share a catalog.

### Local DuckDB

Use `jdbc:duckdb:/absolute/path/to/metadata.db`; username/password are not needed.
Add the compatible `org.duckdb:duckdb_jdbc:2.0.0.1` JAR to the plugin directory:
it is a test dependency, not bundled with the normal plugin. Initialize the file's
DuckLake metadata schema before starting Trino.

With DuckDB 2.0, initialize using `DUCKLAKE_VERSION '1.0', AUTOMATIC_MIGRATION FALSE`.
A 2.0 metadata write/checkpoint can upgrade the local DuckDB storage independently
of the DuckLake format. Rollback requires a consistent backup, not a driver downgrade.

Multiple connections in one JVM can share a DuckDB file. Multiple OS processes
cannot use it concurrently, even on the same host. A separate Trino worker is
another process; use PostgreSQL/MySQL for that deployment.

### File storage

S3 settings belong to each catalog:

```properties
fs.s3.enabled=true
s3.region=us-east-1
# For an S3-compatible service, also configure its endpoint and credentials:
# s3.endpoint=http://object-store:9000
# s3.path-style-access=true
```

For host-local files, configure Trino's local filesystem explicitly, for example
`fs.local.enabled=true` with an appropriate `local.location`, and use paths that
resolve inside that root. Legacy Hadoop-backed filesystem configuration uses
`fs.hadoop.enabled=true`. Native local roots and Hadoop's default filesystem are
not interchangeable: do not rewrite stored bare paths into file URIs blindly.
All scan nodes need real access to the same data; a local path does not create a
shared mount. Filesystem cache configuration is Trino-version-specific.

## Session and table properties

```sql
SET SESSION ducklake.read_snapshot_id = 5;
RESET SESSION ducklake.read_snapshot_id;
SET SESSION ducklake.write_deletion_vectors = true;
```

| Session property | Default | Meaning |
| --- | --- | --- |
| `read_snapshot_id` | Unset | Read a specific snapshot |
| `read_snapshot_timestamp` | Unset | Read at/before an ISO-8601 instant |
| `data_file_format` | Inherited | Only `parquet`; inherits the declared/latest format, or Parquet for an empty table |
| `write_deletion_vectors` | `false` | Write Puffin instead of Parquet positional tombstones |
| `write_row_lineage` | `true` | Preserve logical row IDs on UPDATE/MERGE rewrites |

Snapshot ID and timestamp session pins are mutually exclusive. Writes use the
connector's single-statement write path; multi-statement write transactions are
not a supported contract.

| Table property | Type | Meaning |
| --- | --- | --- |
| `partitioned_by` | `ARRAY(VARCHAR)` | Columns/transforms, such as `ARRAY['region','year(event_date)','bucket(4,id)']` |
| `data_file_format` | `VARCHAR` | Only `parquet` |
| `location` | `VARCHAR` | Per-table path; URI locations are absolute, other locations are schema-relative; `..` is rejected |

Temporal transforms are `year`, `month`, `day`, and `hour` for supported date/timestamp
inputs; bucket partitioning supports equality-based pruning. Per-file ordering is
not global query ordering. Sorted writes apply only to the supported unpartitioned
Parquet scope, using a catalog sort specification.

## Types

Common mappings used by the supported read/write paths:

| DuckLake | Trino |
| --- | --- |
| `boolean` | `BOOLEAN` |
| `int8`, `int16`, `int32`, `int64` | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` |
| `float32`, `float64` | `REAL`, `DOUBLE` |
| `decimal(p,s)` | `DECIMAL(p,s)`, precision at most 38 |
| `varchar`, `blob` | `VARCHAR`, `VARBINARY` |
| `date` | `DATE` |
| `timestamp_s`, `timestamp_ms`, `timestamp`, `timestamp_ns` | `TIMESTAMP(0)`, `(3)`, `(6)`, `(9)` |
| `list`, `struct`, `map` | `ARRAY`, `ROW`, `MAP` with supported element/field types |
| `json` | Native `JSON`; top-level JSON writes use UTF-8 Parquet storage |

Important physical boundaries:

- The pinned Parquet schema converter does not implement UUID, TIME, TIMETZ or
  TIMESTAMP WITH TIME ZONE writers. A successful type mapping or CREATE TABLE is
  not evidence that INSERT/CTAS/row rewrites can write those schemas. TIME/TIMETZ
  and timestamp-with-zone mappings use microsecond precision where applicable;
  mapping them does not remove the physical writer/reader restrictions.
  DELETE can also encounter writer construction through its MERGE path on an affected schema.
- Unsigned mappings widen `uint8/16/32/64` to `SMALLINT/INTEGER/BIGINT/DECIMAL(20,0)`.
  Top-level writes validate the original unsigned range. This is not a universal
  qualification of unsigned Parquet annotations or nested unsigned validation.
- HUGEINT/UHUGEINT physical encodings and INTERVAL/VARIANT file reads have unresolved
  representation limits. Mapper fallbacks to decimal or VARCHAR are not a guarantee
  of exact or usable file-backed values. Do not infer support from the mapper alone.
- Nested JSON writes and inlined maps with composite keys are not supported.
  Native nested JDBC STRUCT conversion requires source-era field order rather than
  guessing positions from the current schema.
- Geometry-family mappings expose binary values, not spatial operators or spatial
  pruning. No general lossless round-trip promise is made for unqualified encodings.
- ALTER permits a restricted widening set; old-file coercion and timestamp precision
  remain physical qualification boundaries. Narrowing, incompatible changes and
  list-element/map-value type changes are not supported.

## Hidden columns

Hidden columns require explicit quoting; they do not appear in `SELECT *` or `DESCRIBE`.

| Column | Type | Meaning |
| --- | --- | --- |
| `$path` | `VARCHAR` | Backing file path; NULL for inlined rows |
| `$snapshot_id` | `BIGINT` | Data-file begin snapshot; per-row begin snapshot for inline data |
| `$insert_snapshot_id` | `BIGINT` | Native-compatible per-row insertion snapshot; uses embedded file values when present, otherwise the file's begin snapshot |
| `$file_row_number` | `BIGINT` | Zero-based physical file position; NULL for inline data |
| `$row_id` | `BIGINT` | Embedded logical lineage ID, or a present file start plus position; NULL for inline data |
| `$file_size_bytes` | `BIGINT` | Backing file size; NULL for inline data |

If a file has neither embedded lineage nor a valid row-ID start, requesting its
logical `$row_id` fails rather than inventing one. Ordinary value/file-position reads
do not require that logical ID. DuckDB's unprefixed names are not aliases here.

Metadata tables are `<table>$files`, `<table>$snapshots`, `<table>$current_snapshot`
and `<table>$snapshot_changes`. Quote the full table name, for example `"orders$files"`.
Snapshot history is catalog-wide even when accessed through a table name.

## Change feeds and views

```sql
SELECT * FROM TABLE(ducklake.system.table_changes(
    schema_name => 'sales', table_name => 'orders',
    start_snapshot => 3, end_snapshot => 4));
```

`table_insertions` and `table_deletions` return `snapshot_id`, `rowid`, then data
columns. `table_changes` also prepends `change_type` after `rowid`; values are
`insert`, `delete`, `update_preimage`, and `update_postimage`.

Bounds are inclusive. Specify each bound by snapshot ID or by timestamp, never
both forms for one bound. The start is required; the end defaults to the current
snapshot. Data columns use the end-snapshot schema. Retained physical files/history
limit which changes remain readable. Logical row lineage is distinct from the
internal physical mutation locator.

Trino views need this connector's compatible view metadata. DuckDB-dialect views
are not exposed. A Trino-labelled view missing required metadata is refused with
`INVALID_VIEW`; recreate it through this connector rather than guessing its shape.

## Maintenance procedures

Call `CALL <catalog>.system.<procedure>(...)`. Named arguments make scope explicit.

| Procedure | Arguments and scope |
| --- | --- |
| `add_files` | `schema_name`, `table_name`, `files`, optional `allow_missing=false`, `ignore_extra_columns=false`, `hive_partitioning=false`, `file_format='parquet'` |
| `flush_inlined_data` | Optional `schema_name`, `table_name`; both selects one table, schema alone selects that schema, neither selects the catalog |
| `rewrite_data_files` | `schema_name`, `table_name`, optional `file_size_threshold`, `target_file_size` (default `512MB`), `reclaim_sources_immediately=false` |
| `remove_orphan_files` | Optional schema/table scope, `retention_threshold='7d'`, `dry_run=false` |
| `expire_snapshots` | Catalog-wide; age-based `retention_threshold='7d'` or explicit `snapshot_ids`, plus `dry_run=false` |
| `cleanup_old_files` | Catalog-wide; `retention_threshold='7d'`, `dry_run=false`; one bounded batch per call |

`ANALYZE <table>` refreshes catalog statistics. These procedures currently log their
outcomes rather than returning structured result tables.

### Operational safety

- Treat procedure execution as a trusted-operator privilege, including authority
  over supplied file paths. Do not assume per-table access checks are enforced by
  the procedure's EXECUTE permission.
- Flush inlined rows before DELETE/UPDATE/MERGE. Explicit flush of an unsupported
  partitioned table errors; wider flush scopes skip partitioned tables.
- Imported files are registered in place, not copied. Hive partition imports support
  identity partitions; transformed hive partition imports are rejected. Missing
  nested fields and physical type mismatches have additional limits.
- Snapshot expiration destroys selected history but only schedules newly unowned
  files. `cleanup_old_files` performs the later physical deletion. The latest
  snapshot cannot be expired. Explicit snapshot IDs bypass the age floor.
- Cleanup selects at most one configured batch. Repeat calls for more work. Owner
  reference pages are budgeted; exceeding a bound fails before deletion/cancellation.
- Orphan removal targets old, unreferenced DuckLake-managed residue, not arbitrary
  foreign files. Empty directories remain. Do not use it as a generic directory cleaner.
- Dry runs perform safety checks without mutation. Ambiguous/noncanonical path
  identities fail closed; mixed Hadoop/native namespaces are not silently normalized.
- Retention is a grace period, not a reader lease or cross-engine import/cleanup lock.
  Do not overlap independent cleaners/importers on shared objects. Failed/deferred
  oldest cleanup requests can require operator attention before later batches progress.
