# Independent DuckLake 1.0 acceptance — catalog 0.14.0

This record qualifies the normal **1.0** connector release for Trino **483/484**.
It does not promote 1.1 reads or writes. The 1.1 validation/release remains separate.

## Published artifacts

All modules and catalog fixtures are pinned to Maven Central **0.14.0**, release
source `fd31cf2d0332d269c3ffa605c835eccd45f24817`, tag `v0.14.0`. All four Central
JARs were downloaded independently and compared byte-for-byte with accepted
snapshot `0.14.0-20261103.154002-5`, source
`f9dde22299caeced077f9fbbf982a5bfef226d62`. Every comparison matches.

[`gradle/catalog-release.sha256`](../../../gradle/catalog-release.sha256) is checked
before tests. No local catalog build, copied JAR, moving snapshot or alternate
metadata format is used. The acceptance-only snapshot repository was removed.
Runtime/native pins remain DuckDB 2.0.0 core `d54cbbe3df`, JDBC 2.0.0.1, signed
DuckLake `417a9117f5`, native Quack `f96420ed46`, Java Quack JDBC 0.13.0 and corpus
`417a9117f590e8141742ac0579d02f6a90162e37`.

## Actual acceptance scope

Complete NORMAL suites against the exact accepted snapshot bytes:

| Target / actual test VM | JUnit tests | Failures / errors / JUnit skips |
| --- | ---: | --- |
| 483 / JDK 25 | 1,071 | 0 / 0 / 0 |
| 484 / JDK 26 | 1,072 | 0 / 0 / 0 |

The extra 484 test was added between runs: it verifies that explicit 1.1 fixture
SQL cannot execute after ATTACH or connect a 1.1 reader in the 1.0 lane. It and
the final focused controls pass on 483 as well (25 tests). Final formal-coordinate
and archive receipts accompany this record; the full suites are not falsely
represented as fresh executions against renamed but identical JARs.

After switching to formal **0.14.0** coordinates and final connector **0.5.0**,
the focused release gate passes **77/77 tests on each target** (17 classes each,
zero failures/errors/JUnit skips), including the actual starter corpus selection,
format refusal/migration, native snapshots, rewrites and structural bindings.
483 uses JDK 25; 484 uses JDK 26. Detekt and `pluginDist` pass on both targets.

All four final local archives (`483-0.5.0` and `484-0.5.0`, ZIP and tar.gz) pass
independent unpacked-layout, exact target dependency, catalog-byte, normal-source
lane, SPI/test-tool exclusion and SHA-256-sidecar checks. Each 483 archive has
325 JARs; each 484 archive has 296. The machine-readable archive hashes and
checks are retained in `build/verification/catalog-0.14.0/packaging.json`.

Unlike the earlier general-only qualification, the corpus gate uses **CI's actual
75-file starter discovery** (`general,catalog,time_travel,view`). It executes
53 files with **930 passing records, 253 native oracle/golden query passes,
28 verified Trino comparisons and zero failures**. Fourteen explicit 1.1 files
halt visibly after positive format inspection. Remaining 1.1 SQL is not executed,
prior outcomes stand, and halted files never count as passed 1.0.

Existing backend/parser/dialect limits remain visible (including native DuckDB-view
dialect, `require-env`, `concurrentloop`, `notmusl` and the upstream PostgreSQL
axis). No directory was blanket skipped and no golden was changed. The old
`virtualcolumns/ducklake_snapshot_id.test` file skip was removed only after its
original SQL/goldens passed actual Trino comparisons unchanged. The full corpus
was **not run**.

## Fixes qualified

- `ducklake.catalog.format-version=1.0` is the normal default and maps to the
  library's stored-format lock. Explicit 1.1 requires the separately qualified
  read-only build. A wrong stored format is refused for both Parquet and inline
  reads/writes, including after native migration of an already connected catalog.
  It never rewrites/migrates stored metadata. The 1.0 family admits compatible 0.4.
- Metadata lookup/listing, split/source and function analysis use owned read views.
  Unsupported-format failures preserve the typed cause and NOT_SUPPORTED code.
- SQL catalog/schema/table rebinding uses parsed identifiers/scopes instead of
  `\w+` substitution. Quoted dots/quotes, literals, CTEs and actual table aliases
  preserve their meanings; real 1.0 quoted-table reads return the native rows.
- The PostgreSQL oracle binds only known rewritten metadata catalogs' default
  schema qualifiers to `public`. Explicit metadata schemas, logical lake schemas,
  literals and source/golden files are unchanged. The original NULL-view-alias
  fixture executes all five native query goldens and the actual metadata mutation;
  a real Trino base-table read is an independent control. Its three pre-existing
  native-view-dialect comparison skips are not claimed as Trino view coverage.
- `$insert_snapshot_id` streams the native per-row insertion snapshot, with a
  file-begin fallback only when the reserved Parquet field ID is absent. Ambiguous
  IDs, incompatible types and NULL/negative present values reject. No whole-file
  snapshot array is introduced. Existing `$snapshot_id` keeps its file-level
  contract. No plain `snapshot_id` name is reserved: real CREATE/ADD columns keep
  working; replay binds native virtual references only after proving no stored
  column exists, including identical SQL before/after ADD COLUMN and CTE outputs.
- Ordinary/partial rewrite and recompaction outputs stream/tag both row lineage
  and insertion snapshots with reserved field IDs. Partial file visibility bounds
  still describe source FILE visibility, not older row insertion times. Tests
  compare native/Trino values and historical results to prevent lost origins or
  duplicated historical rows after ordinary-to-partial recompaction.

The view setup does not use metadata copies, fake schemas/views or a shared-file
warehouse scheme. The attempted simultaneous local-file mirror hit a real DuckDB
attachment-name conflict and was removed; PostgreSQL remains the actual mirror
backend. All nodes/native oracles in these tests still run within a single-host
test process; this is not multi-host deployment qualification.

## Evidence and remaining limits

Generated snapshot acceptance XML is under
`build/verification/catalog-0.14.0-20261103.154002-5/`; independent Central checks
and formal-coordinate tests/archives are under `build/verification/catalog-0.14.0/`.
These are build/test outputs, not runtime storage. Workspace JVM temp is used only
after the previously recorded real system-temp quota failure, with the original
3 GiB/parallel test configuration and unchanged concurrency assertions.

Native MySQL interoperability is still not qualified; direct Connector/J tests
pass. Existing type/security/retention/maintenance risks remain in the active
backlog and restrictions inventory. All 1.1 writes remain disabled. Broader 1.1
capabilities and hierarchical-schema qualification are not conditions of the 1.0
release and are not inferred from this acceptance.
