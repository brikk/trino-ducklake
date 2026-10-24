# R07 Trino candidate qualification — 2026-10-23

## October 24 priority correction: legacy TIMESTAMPTZ partition pruning

Reproduced the upstream review's finding through **real matched coordinator/worker
SQL**, not only the matcher: equality at `2021-01-01T05:00Z` returned `[2]` instead
of independently expected `[1,2]`, losing the GMT-12-written row while keeping the
UTC-written row. Actual split planning also discarded legacy file IDs in ordinary
1.0 and natively migrated/mixed 1.1 catalogs (three failing regression tests, zero
skips). Native producer pins remain core `d54cbbe3df` / signed DuckLake `417a9117f5`.

Fix: unknown-writer-zone TIMESTAMPTZ temporal partitions cannot exclude candidates,
regardless of calendar/epoch preference or encoding leniency. Preserve stored values,
exact residual predicates and NULL handling; DATE/unzoned/identity logic is unchanged.
This intentionally narrows unsafe pruning assertions, not semantic goldens. Native
migration/format 1.1 and future UTC writers do not attest old file provenance.

The regression covers native YEAR/MONTH/DAY/HOUR partitions, GMT-12/UTC/Kiritimati
writers, Trino UTC/Kiritimati/Los Angeles sessions, strict/lenient encoding, equality
and exclusive ranges, and a nonmatching row inside the same partition to verify
the residual. Migrated/mixed 1.1 is additionally checked through actual split planning.
Evidence: `/tmp/opencode/trino-r07-next/timezone-red-xml/` and
`timezone-candidate-final.log`; candidate regression + matcher: **33 tests pass**.
The released-lane pruning/residual/time-travel controls pass **80 tests**, with
lint and zero failures/errors/skips (`timezone-legacy-final-2.log`); no legacy
writer control is retagged to bypass the candidate's intentional 1.1 write refusal.

## Decision and boundary

**NO-GO for enabling final-1.1 Trino SQL consumers.** The exact candidate passes the
limited SPI experiments below after connector fixes; this is useful qualification,
not complete R07 or a coordinated rollout. The production connector still lacks
explicit final-1.1 configuration and complete metadata/worker session integration.
Its matched two-node SQL control correctly refuses 1.1. Do not confuse that control
with a positive distributed 1.1 scan.

No push, publication, deployment, automatic migration, connector 1.1 writes,
maintenance/physical cleanup, default-on reads, corpus repin, or semantic golden
change was performed. Native creation/mutation was confined to disposable fixtures
outside the read-only catalog. Only owned test containers and ephemeral ports were
used. Gradle used `/tmp/opencode/trino-r07-gradle`, with a private daemon registry;
no global daemon/service shutdown. Trino still compiles/runs on Java 25; the four
candidate jars independently retain maximum classfile 61 (Java 17).

## Artifact identity and reversible selection

Catalog source: `58ef4d700024a010bfee793aaf92f6ff239e9e5c` in the unmodified sibling
repository. Trial bundle: `/tmp/opencode/catalog-r07-58ef4d7`.

`-Pr07CatalogCandidate=/tmp/opencode/catalog-r07-58ef4d7`:

- Exclusively resolves `dev.brikk.ducklake:{ducklake-catalog,slt-format,ducklake-test-corpus-replay}:0.13.0-SNAPSHOT`
  from that bundle's `maven-repository`, excluding those coordinates from every
  other repository, including the convention plugin's first `mavenLocal`.
- Retains Gradle module metadata and the catalog test-fixtures variant.
- Makes tests depend on `verifyR07Candidate`, checking all **four resolved jar
  files**, not just the requested version. The whole supplied SHA256SUMS manifest,
  including POM/module metadata, was also independently verified with `sha256sum -c`.
- Adds `qualification/src` only for this trial; omitting the property restores the
  released dependencies and original test source layout. No candidate artifact was
  rebuilt, overwritten, or substituted.

| Resolved jar | SHA-256 |
|---|---|
| catalog | `5af1a80adceb751a1ea616831953734a09d14d85ce632192b31f9c31b051bc84` |
| catalog test-fixtures | `ca33bee56b33703ea7bbe7b76153bbaccc0275478fc992d48768eb912a1a056f` |
| corpus replay | `c8f847d4ba8ca8509a8b3a47c709c1e86b34e50c2c4e3c824b79e1256cba85a4` |
| SLT | `6406d8fdf2ed16c7fd59465fbeeeb5d1f6e06274357bfd4b3516f779d2bfc409` |

Exact trial dependency differences:

- Catalog/replay/SLT: released **0.12.0 → 0.13.0-SNAPSHOT**, property only.
- Test-only DuckDB JDBC: **1.5.6.0 → 2.0.0.0**, property only. The actual loaded jar
  must hash to `5170daa90a43296705127ef3c80834ba1cd275ec9d765a9f7eee346ab0087186`.
- Test runtime adds **`dev.brikk.duckdb:quack-jdbc:0.8.0`**, property only. The old
  version-catalog entry is 0.3.0 but was not a declared dependency; the first direct
  Java Quack trial correctly failed with no driver until this explicit addition.
- MySQL Connector/J stays **9.7.0**, Trino **483**, Kotlin **2.3.10**.
- Native fixtures assert core **2.0.0 / d54cbbe3df**, signed DuckLake **417a9117f5**,
  Quack **f96420ed46**, PostgreSQL scanner **c0f5a3c0c4**, MySQL scanner **f7fc0d4271**,
  and reject unsigned-extension mode. The Quack server's executing core is also
  checked through its observed backend profile.
- Consumer corpus remains **`c23aca43b6033acfac2b8cc8d80b15ae7fa729b2`**, different
  from candidate **`417a9117f590e8141742ac0579d02f6a90162e37`**. No repin. The new
  explicitly authored native fixtures are not presented as upstream corpus goldens.

## Connector contracts changed

1. **Decoded 1.1 inline and CDC payloads.** Use the already-public decoded APIs,
   including recursive STRUCT/LIST/MAP values and opaque BLOB bytes. Do not reapply
   legacy source-era field names/defaults to the library's target stable-ID schema.
   A native renamed STRUCT containing BLOB and DATE reproduced an actual failure
   in our raw nested-text parser (`Truncated or invalid \\xNN escape`). Decoded
   conversion fixes it without another text serializer. Legacy 0.4/1.0 raw paths
   retain their previous conversion. Intervals exposed as VARCHAR preserve independent
   months/days/microseconds; unrepresentable TIMETZ seconds offsets explicitly reject.
2. **Tagged Puffin CDC.** Read the physical container, associate each position with
   its earliest embedded snapshot, and window those events. Equal current/previous
   paths no longer self-cancel cumulative tagged containers. Bare/untagged vectors
   keep catalog-based delta behavior. Malformed snapshot properties cannot silently
   become untagged vectors; mixed tagged/untagged vectors and CRC failures reject.
3. **Historical file-backed statistics.** Omit the optimizer estimate before calling
   the intentionally narrowed live-count API. Never replace refusal with gross,
   zero, or an exact aggregate. Unknown-delete statistics stay absent; scans still
   return the real rows. There is no exact aggregate pushdown added.
4. **Owned split planning.** All descriptor/path/partition/name-map/inline-delete
   reads inside `getSplits` now share one owned session (nested callers reuse it).
   This does not implement query-lifetime/worker ownership or a file lease.

No public DTO, split JSON field, constructor, writer fragment or capability
negotiation was changed. Tagged Parquet snapshot filtering was already present on
`main`; it was verified rather than replaced.

## Actual scan evidence

The harness invokes the real `DucklakeSplitManager`, `DucklakePageSourceProvider`,
Trino Parquet/footer reader and `ChangeFeedPageSource`, not a DuckDB SELECT standing
in for Trino. Only fixture production and independent pin checks use DuckDB.

| Metadata route | Positive Trino SPI evidence | Not certified here |
|---|---|---|
| Local DuckDB READ_ONLY | Mixed file/inline values; serialized split on separate catalog instance; historical/lineage/delete/CDC/geometry cases below | Online local RW/RO coexistence; positive distributed SQL |
| Native Quack metadata-only | Mixed file/inline scan, serialized split on separate catalog instance, actual remote engine/profile | Tagged-delete/CDC matrix, outages, leases, distributed SQL |
| Direct Java Quack 0.8.0 | Mixed file/inline scan and matched-instance split wire; observed driver version | No-DuckDB-JDBC JVM isolation (producer is in this test JVM); other route-specific cases |
| PostgreSQL JDBC | Mixed file/inline scan and matched-instance split wire; NULL DuckDB-only profile observations | Full nested evolution/delete/CDC matrix; cancellation/network faults |
| Existing MySQL 1.1 JDBC | Narrow unchanged INTEGER/BLOB/VARCHAR mixed scan and matched-instance wire; nullable SQL-server profile | Native migration/create/mutation qualification, broader scalar/evolution/CDC matrix |

The MySQL reader uses the candidate's enforced UTC, read-only REPEATABLE READ,
utf8mb4/InnoDB validation. Passing this narrow fixture is not independent negative
qualification of every invalid timezone/boolean/storage/type combination. The native
MySQL failures do **not** impose a 1.0-only JDBC read ceiling.

Local native cases additionally verify:

- Mixed file/inline INTEGER/BLOB values, SQL NULL vs literal `NULL` vs empty string;
  byte sequence `00 5c ff`; independently expected seven-row result.
- DATE and nanosecond TIMESTAMP from both raw-contract/decoded library inspection
  and real Trino scan; literal initial default `NULL`, root rename, nested stable-ID
  rename/widening, nested BLOB/DATE, and NULL STRUCT parents.
- Bare deletion-vector-v1 Puffin physically removes exactly rows 0–9 of 100.
- Tagged Parquet with native **NULL partial_max**, equal current/previous paths,
  historical nine-row scan, and separately windowed deletions at their real snapshots.
- Native multi-blob cumulative Puffin: historical scan returns `1,3,4`; CDC returns
  deletion `2` at the earlier snapshot and `4` at the later one, including a window
  whose current and previous paths are identical.
- Native merge with a real inline gap: NULL contiguous start stays NULL, historical
  scan returns `(1,0),(2,1),(3,2)` using embedded absolute IDs; later physical DATA
  rows are filtered out, while the latest file+inline scan returns 1–7.
- Inline UPDATE produces paired pre/post images with original row ID 0; an
  inserted-and-deleted row in that same native transaction produces no CDC event.
- Real native GEOMETRY Parquet reads as exact POINT(1 2) WKB
  `0101000000000000000000f03f0000000000000040`, with NULL preserved. This is a
  VARBINARY scan, **not** Trino spatial operators, geometry-inline support or bbox pruning.
- VARIANT inline payload rejects before returning rows. Existing type-display mapping
  is not proof of VARIANT payload support. TIME_NS/TIMESTAMPTZ_NS, temporal infinities,
  whole-container defaults and broader file-type evolution are not newly qualified.
- Historical file counts refuse and optimizer statistics remain unknown; unknown
  delete count cannot become zero, but the real scan still returns 90 rows. Raw
  min/max stay absent and an out-of-range catalog predicate retains all file splits.
- Caller `CancellationException` survives unchanged; a one-connection reader can
  immediately start a new session. Missing required `file_format` propagates as a
  failed plan, not an empty plan, and releases the connection. These are **not**
  network-interruption, JDBC statement-cancel or physical-file lease tests.
- Complete companion path enumeration uses SQL pages of one, at most four calls,
  at most three paths, a terminal empty page and advancing/no-duplicate cursors.
  An insufficient budget explicitly fails as incomplete. This tests the caller
  protocol, **not** adoption of bounded APIs throughout production planning.

## Verification and reproducibility

All commands use `GRADLE_USER_HOME=/tmp/opencode/trino-r07-gradle mise exec -- ./gradlew`
and `--no-daemon --no-configuration-cache`. Logs/XML are retained under
`/tmp/opencode/trino-r07-evidence/`.

- Before edits: six maintained classes, **69 tests, zero failures/errors/skips**;
  `baseline.log`, `baseline-xml/`.
- Candidate: `test --tests '*TestR07ConsumerTrial' --tests '*TestDucklakePuffinDeleteReader'
  --tests '*TestDucklakeDecodedValueConverter' detekt
  -Pr07CatalogCandidate=/tmp/opencode/catalog-r07-58ef4d7`: **41 tests** (19 new native/SPI
  trial cases, 19 Puffin unit cases, 3 decoded converter cases), zero failures/errors/skips;
  lint passes. `candidate-final.log`, `candidate-final-xml/`.
- Without the property: **138 maintained tests across 15 classes**, zero
  failures/errors/skips, plus detekt, pass on the original released 0.12.0 / JDBC
  1.5.6.0 lane. Includes real SQL change feed, partial DATA/Parquet deletes, nullable
  IDs, split/page sources, catalog statistics, and cross-engine inline ALTER tests.
  `legacy-regression.log`, `legacy-final-xml/`.
- Targeted maintained `types` corpus: **11 files discovered; 9 ran; 81 records
  passed, zero failed; 3 existing record exclusions and 2 existing file exclusions**
  (float/JSON). One JUnit test, zero JUnit skips. Exclusion lists and goldens are
  unchanged. `corpus-types.log`, `corpus-types-xml/`.
- Negative exclusive-repository control: point the property at an empty trial
  repository with the same checksum manifest. Resolution fails and lists **only**
  that file repository, with no local/remote/cached candidate fallback. Expected
  exit 1; `exclusive-repository-negative.log`. The actual bundle is untouched.

Reproduce the positive candidate and maintained corpus runs:

```sh
GRADLE_USER_HOME=/tmp/opencode/trino-r07-gradle mise exec -- ./gradlew \
  test --tests '*TestR07ConsumerTrial' --tests '*TestDucklakePuffinDeleteReader' \
  --tests '*TestDucklakeDecodedValueConverter' detekt \
  -Pr07CatalogCandidate=/tmp/opencode/catalog-r07-58ef4d7 \
  --no-daemon --no-configuration-cache

GRADLE_USER_HOME=/tmp/opencode/trino-r07-gradle mise exec -- ./gradlew \
  test --tests '*TestTrinoCorpusReplay' -Dducklake.corpus.dirs=types \
  --no-daemon --no-configuration-cache
```

The released-lane 138-test command uses these `--tests '*<class>'` filters:
`TestDucklakeConfig`, `TestDucklakeSplitManager`, `TestDucklakeInlinedValueConverter`,
`TestDucklakeDecodedValueConverter`, `TestDucklakePuffinDeleteReader`,
`TestDucklakeRowIdentity`, `TestDucklakeNullableRowIds`, `TestDucklakeCatalog`,
`TestDucklakePageSourceProvider`, `TestDucklakePartialFileFilter`,
`TestDucklakePartialDeleteFilter`, `TestDucklakeChangeFeedPartialDelete`,
`TestDucklakeChangeFeedPartialFile`, `TestDucklakeChangeFeed`,
`TestDucklakeCrossEngineInlinedAlter`; followed by `detekt` and the common flags.

An early harness bug inferred non-void Kotlin test return types. Those preliminary
runs discovered only one trial case and **are not scan acceptance evidence**. Explicit
Unit signatures, a reflection-based discovery regression, and final XML verify all
19 cases execute. Subsequent failures were investigated: test sentinel/numeric/case
assertion mistakes, missing explicitly required Quack driver, and the real connector
nested-BLOB problem above. No failing assertion was skipped or disabled.

The catalog's 839+86 tests/17,602 records are not connector certification. Its strict
native **R02=3, R03=1, R04=1, M01=6+4** failures remain visible, zero skips, not waived
and not rerun as part of this connector's targeted loop. No full corpus was run.

## Remaining no-go gates and catalog feedback

No new catalog-library defect was established; the concrete incompatibilities fixed
here belonged to the connector. Minimal executable repros are the named qualification
tests, especially `nativeCumulativePuffinEqualPathsDoNotCancelCdc` and
`nestedStableIdRenameWithBlobAndTemporalTextDoesNotReapplyOldNames`.

Before any actual SQL enablement: integrate an explicit default-off flag with complete
metadata and lazy worker session ownership; replace/bound materialized production
planning/CDC lists with total page/path/traversal/work budgets; qualify actual matched
worker SQL scans and cancellation/network/retention recovery; extend route-specific
payload/file/evolution/CDC coverage and legacy 0.4 verification. Mixed-version negotiation
was neither implemented nor fabricated. Paths and read sessions never authorize cleanup.

This is a **GO only for further isolated experiments with this exact checksum-attested
candidate**, and **NO-GO for guarded Trino SQL rollout as currently wired**. R07 as a
whole remains open; no Doris or coordinating-session result is inferred.
