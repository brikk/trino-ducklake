# Trino DuckLake 0.5.0 — Trino 483 and 484

This release provides **separate plugins for Trino 483 and Trino 484**, using
released **ducklake-catalog 0.14.0** and DuckLake **1.0** read/write metadata.
The shared release tag is `v0.5.0`; each archive name identifies its exact Trino target.

## Downloads and installation

Choose exactly one matching target:

- Trino 483: `trino-ducklake-483-0.5.0.zip` or `.tar.gz`.
- Trino 484: `trino-ducklake-484-0.5.0.zip` or `.tar.gz`.

Each archive has a `.sha256` sidecar and expands to one flat plugin directory.
Verify the checksum, stop Trino, and replace the entire DuckLake plugin directory
on every coordinator and worker. Do not merge old/new JARs, mix target versions,
or rename a 483 archive to use it on 484. Strict SPI version matching remains enabled.
See the [installation guide](https://github.com/brikk/trino-ducklake#install).

## Changes since 483-0.4.0

- **Trino 484 support**, with its own BOM, SPI/parser and compiled adapters.
  484's JSON blocks, Parquet LIST API, merge completion and retained-memory contexts
  are handled without weakening 483 compatibility or changing catalog SQL types.
- **Catalog 0.14.0** replaces 0.12.0. The runtime, fixtures, replay and SLT modules
  are pinned in lockstep and checksum-attested. The catalog also qualifies direct
  Quack JDBC 0.13.0 upstream; that driver is not bundled or a production deployment
  qualification for this connector.
- **DuckDB 2.0 interoperability lane**, tested with JDBC 2.0.0.1. New native lakes
  must explicitly select DuckLake 1.0; see the upgrade precautions below.
- The shared bounded nested-type parser preserves exact quoted field names. Native
  nested JDBC values use structured conversion and stable historical field identity.
- **Actual catalog-format checks:** `ducklake.catalog.format-version=1.0` is the
  normal default and is enforced before reads and writes. A stored 1.1 catalog is
  refused consistently for Parquet and inline data, including after a native
  migration of an already connected catalog. No metadata migration is performed.
- **Per-row snapshot metadata:** `$insert_snapshot_id` reads native insertion
  snapshots, including after flush, ordinary/partial rewrites and recompaction.
  Existing `$snapshot_id` keeps its file-level meaning. Ordinary columns named
  `snapshot_id` can still be created/added; no stored columns are renamed.
- Legacy TIMESTAMPTZ calendar partition values of unknown writer timezone are kept
  as scan candidates, with exact Trino residual filtering, rather than unsafely pruned.
- ANALYZE now publishes one metadata-only snapshot. Existing mutation paths retain
  the original planning snapshot. Unknown-COMMIT and acknowledged-post-commit failures
  must not trigger automatic retry or output deletion; retained output needs reconciliation.
- Experimental read-session, paged-metadata/default, CDC and cancellation work remains
  separately gated. It is **not enabled in these normal release archives**.
- Test tooling uses brikk-sql 0.21.0 and Kotlin 2.4.10. Replay selects the native
  DuckLake 1.0 default without rewriting upstream SQL or goldens; the corpus pin is
  aligned with signed DuckLake `417a9117f5`.
- Test SQL binding now handles quoted catalog/schema/table identifiers and native
  snapshot references structurally, with actual table metadata. Native test queries
  against rewritten PostgreSQL metadata bind to its real schema, not a fake local
  schema or copied metadata file.

## Upgrade precautions

DuckDB 2.0 defaults new native lakes to DuckLake 1.1. For this connector, initialize
or attach using:

```sql
ATTACH 'ducklake:<metadata-connection>' AS lake
    (DATA_PATH '<data-path>', DUCKLAKE_VERSION '1.0', AUTOMATIC_MIGRATION FALSE);
```

Do not edit stored version strings or migrate a production lake to 1.1 for this
release. A DuckDB 2.0 write/checkpoint can upgrade local metadata storage even
without a DuckLake format migration; rollback requires a consistent backup, not
an older JDBC driver. Trino does not initialize or migrate the metadata schema.

PostgreSQL and MySQL JDBC drivers are bundled. Local DuckDB metadata still requires
a separately supplied compatible JDBC 2.0.0.1 JAR and is a **single-JVM** topology,
not a shared-file catalog for separate worker JVMs. DuckDB and Quack JDBC, replay,
SLT, SQL test tooling and Trino SPI are not bundled.

## Scope and verification

The maintained scope remains DuckLake **1.0** with Parquet data. Final-1.1 reads
are default-off and require a separately selected experimental build; **all 1.1
writes remain unsupported**. Quack deployment/authentication and native MySQL
interoperability remain unqualified. Existing type, retention, security and
maintenance limitations in the [reference](https://github.com/brikk/trino-ducklake/blob/v0.5.0/REFERENCE.md) still apply.

Independent 1.0 acceptance on the exact published catalog snapshot passed the
complete normal suites: **1,071 tests on 483/JDK 25** and **1,072 on 484/JDK 26**,
with zero failures/errors/JUnit skips. The extra 484 test was a new explicit-format
scope regression; it and the final focused controls also pass on 483 (25 tests).
Formal catalog 0.14.0 has identical JAR bytes; the final-coordinate release gate
also passes **77/77 tests on each target**. Both targets pass Detekt. Final
archives are rebuilt and checked for layout, target dependencies, SPI/test-only
exclusions and SHA-256 sidecars. Tests use real native/JDBC engines and Trino
workers; these are not multi-host production certification.

The full corpus was **not run**. Unlike the earlier general-only checks, this
release gate uses CI's actual 75-file starter discovery. The 1.0 selection runs
53 files: **930 passing records, 253 oracle/golden query passes, 28 verified Trino
comparisons and no failures**. Fourteen explicitly 1.1 files halt visibly outside
the 1.0 execution scope; they are not counted as passed 1.0. Existing backend,
engine/dialect and parser limitations remain visible. The metadata-logging file's
unsupported `require-env` directive remains unexecuted. No directory was blanket
excluded or golden edited; the old native snapshot fixture skip was removed after
the original fixture passed real Trino comparisons unchanged.

1.1 validation/release is independent and does not hold this 1.0 release hostage.
The catalog's limited read-only 1.1 API is not a broader connector support claim.

Exact acceptance scope and receipts are retained in the
[independent 1.0 verification record](https://github.com/brikk/trino-ducklake/blob/v0.5.0/dev-docs/archive/CATALOG-0.14.0-ADOPTION.md).
