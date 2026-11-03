# Trino DuckLake connector

Read and write [DuckLake](https://ducklake.select/) tables from Trino, using a JDBC
metadata catalog and **Parquet data files**. PostgreSQL is the recommended catalog
backend for a Trino cluster or a lake shared with DuckDB.

## Compatibility

| Component | Normal build |
| --- | --- |
| Trino | **483 or 484**, with a separate plugin archive for each version |
| DuckLake catalog format | **1.0** read/write, subject to the limits below |
| Catalog library | **0.12.0**; this library version is not the DuckLake format version |
| DuckDB interoperability | Tested with **DuckDB 1.5.6** |
| Data and delete files | Parquet data; Parquet positional deletes and Puffin deletion vectors |

Install the archive matching your Trino version **exactly**. Do not mix JARs from
different archives. Experimental final-1.1 reads are not enabled in normal builds;
1.1 writes are not supported. Developer qualification settings are separate from
the normal installation.

### Catalog backends

| Backend | Deployment |
| --- | --- |
| PostgreSQL | Shared, multi-process catalog; preferred for Trino/DuckDB interoperability |
| MySQL 8+ | Shared catalog for Trino read/write; DuckDB cross-engine access is not qualified |
| Local DuckDB file | One JVM only; separate worker JVMs or concurrent DuckDB CLI processes cannot share the file |

PostgreSQL and MySQL JDBC drivers are bundled. A local DuckDB catalog requires a
compatible DuckDB JDBC driver added to the plugin directory. SQLite, Turso and
remote Quack catalogs are not part of the normal supported deployment described here.

## Install

Download the matching ZIP or tar.gz and its checksum from
[GitHub Releases](https://github.com/brikk/trino-ducklake/releases). Each archive
contains one `trino-ducklake-<version>/` directory of JARs.
If a release has no archive for your target, build the matching target yourself;
renaming an archive does not change its SPI compatibility.

```sh
sha256sum -c trino-ducklake-<version>.zip.sha256
unzip trino-ducklake-<version>.zip
```

Stop Trino and install that directory as `plugin/ducklake` on **every coordinator
and worker**. The destination must not already contain another plugin version:
replace the old directory rather than merging JARs, and keep backups outside
Trino's `plugin/` directory.

```sh
# Destination does not already exist; adjust the installation path for your server.
cp -r trino-ducklake-<version> /usr/lib/trino/plugin/ducklake
```

The JARs must be directly inside `plugin/ducklake/`, not in another nested directory.

### Build an archive yourself

With **JDK 25** installed (`mise install` uses the repository's tool configuration):

```sh
./gradlew pluginDist                     # Trino 483
./gradlew pluginDist -PtrinoVersion=484   # Trino 484
```

Archives and SHA-256 sidecars are written to `build/distributions/`.
Development and verification instructions are in [dev-docs](dev-docs/README.md).

## Configure a catalog

The metadata database must already contain a compatible DuckLake schema. Trino
does **not** initialize or migrate that schema. Use a DuckDB/DuckLake version
compatible with DuckLake 1.0; the maintained interoperability version is DuckDB 1.5.6.
The bootstrap DuckDB process also needs its own access and credentials for the data storage.

For example, initialize an empty PostgreSQL metadata database from DuckDB:

```sql
INSTALL ducklake;
LOAD ducklake;
INSTALL postgres;
LOAD postgres;
ATTACH 'ducklake:postgres:dbname=ducklake host=<host> port=5432 user=ducklake password=<password>'
    AS lake (DATA_PATH 's3://<bucket>/<prefix>/');
DETACH lake;
```

Create `etc/catalog/ducklake.properties` on the Trino nodes:

```properties
connector.name=ducklake
ducklake.catalog.database-url=jdbc:postgresql://<host>:5432/ducklake
ducklake.catalog.database-user=ducklake
ducklake.catalog.database-password=<password>
ducklake.data-path=s3://<bucket>/<prefix>/

fs.s3.enabled=true
s3.region=<region>
```

Supply S3 credentials through the server's credential provider or catalog-specific
S3 settings. Settings in another catalog's properties file do not configure this
catalog. Every node processing data must be able to access the configured storage;
host-local files are not automatically shared between workers.

Restart Trino after installing the plugin and catalog. See the
[configuration reference](REFERENCE.md#catalog-configuration) for MySQL, local
DuckDB, storage settings and optional properties.

## Use the catalog

```sql
SHOW SCHEMAS FROM ducklake;

CREATE SCHEMA ducklake.sales;
CREATE TABLE ducklake.sales.orders (
    id BIGINT,
    region VARCHAR,
    amount DECIMAL(12,2)
) WITH (partitioned_by = ARRAY['region']);

INSERT INTO ducklake.sales.orders VALUES (1, 'EU', 42.00);
SELECT * FROM ducklake.sales.orders;

SELECT * FROM ducklake.sales."orders$snapshots";
SELECT "$path", "$row_id" FROM ducklake.sales.orders;
```

The connector provides table scans, file/row-group pruning, dynamic filtering,
snapshot reads, schema evolution, INSERT/CTAS, DELETE/UPDATE/MERGE, views, change
feeds, and maintenance procedures. Partitioning supports identity, temporal and
bucket transforms. Sort specifications can order eligible writes within files;
they do **not** guarantee globally sorted query results.

Use [SQL samples](SAMPLES.md) for schema changes, snapshot pinning and DuckDB
interoperability, and the [reference](REFERENCE.md) for types, properties, hidden
columns and change feeds.

## Important limits

- **Type mapping is not a physical compatibility guarantee.** Common numeric,
  string, binary, date and timestamp types, plus supported ARRAY/ROW/MAP shapes,
  have read/write paths. UUID and several time/time-zone types have Parquet-writer
  gaps. Unsigned, huge-integer, interval and VARIANT encodings have additional
  restrictions. Check the [type boundaries](REFERENCE.md#types) for your schema;
  successful DDL alone does not prove that a type can be written or read exactly.
- JSON writes support top-level JSON columns; nested JSON writes are not supported.
  Geometry is exposed as binary data, without spatial operators.
- DELETE/UPDATE/MERGE cannot mutate catalog-inlined rows. First run
  `flush_inlined_data` for the table. Reads can combine inlined and file-backed rows.
- Table renames stay within a schema. DDL permits a restricted widening set, but
  old-file coercion still needs physical-type qualification. Narrowing and
  list-element/map-value changes are rejected.
- Views use this connector's Trino view metadata. Other dialects are not exposed;
  incompatible Trino view definitions are refused rather than guessed.
- Use only Parquet data files. A different declared or stored data-file format is
  rejected, not transparently converted.

## Maintenance

Procedures are called through the catalog's `system` schema:

| Procedure | Purpose |
| --- | --- |
| `add_files` | Register existing Parquet files in a table |
| `flush_inlined_data` | Move eligible inlined rows to Parquet |
| `rewrite_data_files` | Compact data files through the read path |
| `remove_orphan_files` | Remove old, unreferenced connector-managed residue |
| `expire_snapshots` | Remove history and schedule newly unowned files for deletion |
| `cleanup_old_files` | Process one bounded batch of scheduled deletions |

```sql
CALL ducklake.system.flush_inlined_data(schema_name => 'sales', table_name => 'orders');
CALL ducklake.system.remove_orphan_files(
    schema_name => 'sales', table_name => 'orders', dry_run => true);
```

Grant procedure execution only to trusted operators: procedure-level permission is
not a substitute for target-table or raw-storage authorization. Scope calls to a
table where possible, inspect dry runs, retain backups, and understand retention
before deleting history or files. Independent engines' cleaners/importers and
long-running readers do not share a universal coordination lock or reader lease.
Avoid overlapping those operations on the same objects.

The [maintenance reference](REFERENCE.md#maintenance-procedures) describes arguments,
retention floors, batch limits and the distinction between scheduling and physical deletion.

## Documentation

- [Reference](REFERENCE.md): catalog/session/table settings, type boundaries and maintenance.
- [SQL samples](SAMPLES.md): practical queries and interoperability examples.
- [Developer documentation](dev-docs/README.md): build/test/release workflow, implementation
  contracts, active backlog and experimental catalog qualification.
- [DuckLake 1.0 specification](https://ducklake.select/docs/1.0/specification/introduction/).
- [License](LICENSE).
