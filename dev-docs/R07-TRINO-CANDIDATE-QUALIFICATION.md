# R07 Trino candidate qualification — October 23–24, 2026

## October 24 follow-up: decoded missing-file defaults (`3fd75dc`)

The earlier missing-file-default refusal below is superseded for the new exact
candidate and supported, valid literal histories. The connector now calls the public
`JdbcDucklakeCatalog.getMissingFileColumnDefaultDecoded(tableId, columnId, targetSnapshotId)`
inside its existing owned read session, **only after physical footer/stable-ID
resolution proves the requested field absent**. It never evaluates default text.
The released 0.12 build remains supported through the established narrow optional
API bridge; a missing method refuses explicitly, without a text/NULL fallback.

Worker source opens reload target stable-ID metadata, rather than trusting cached
column trees or a serialized handle's type/default. A mismatched handle type rejects.
Root constants and nested STRUCT/LIST/MAP projection use decoded values and the
existing exact converter. A present physical NULL remains NULL; a missing child
does not materialize a NULL parent or NULL collection element. Existing delete,
row-identity, CDC, read-only/session/work-limit and timezone paths are retained.
Name-map nested fields lacking authoritative child IDs and ambiguous partially
ID-annotated files refuse rather than guessing absence. Non-Parquet file variants,
MySQL missing-field evolution and non-NULL whole-container defaults remain unsupported.
An existing STRUCT with no constructable target child still refuses: this change
does not invent a physical presence anchor to distinguish its NULL/non-NULL parents.

### Artifact identity

Source catalog `3fd75dcd7a31a0b54e40d544970ac106d8522280`; exclusive bundle
`/tmp/opencode/catalog-r07-3fd75dc`, all four modules/fixtures `0.13.0-SNAPSHOT`.
All ten manifest entries (JAR/POM/module files) pass `sha256sum -c ../SHA256SUMS`
from `maven-repository`. Gradle's `verifyR07Candidate` also attests the **actual four
resolved JAR files**, not just requested coordinates:

- Catalog: `d970be18927c1b669b52c17e88d1eb26e112af590794772febdd4a0fb26cbf08`.
- Test fixtures: `ca33bee56b33703ea7bbe7b76153bbaccc0275478fc992d48768eb912a1a056f`.
- Replay: `c8f847d4ba8ca8509a8b3a47c709c1e86b34e50c2c4e3c824b79e1256cba85a4`.
- SLT: `6406d8fdf2ed16c7fd59465fbeeeb5d1f6e06274357bfd4b3516f779d2bfc409`.

An independent ZIP/class-header scan of those four runtime/fixture JARs checks
369 classes, all with maximum classfile 61 (Java 17).

The old `58ef4d7` bundle is unchanged. Test JDBC/Quack/native pins, consumer corpus,
semantic goldens/exclusions and JVM floors are unchanged. No catalog or Doris edits,
candidate overwrite, push, publication, deployment, connector 1.1 writes, automatic
migration, maintenance cleanup or default-on change is authorized by this work.

### Positive connector SQL and targeted verification

The new independently expected `fileDefaultsSql` cases use the real Trino
DistributedQueryRunner with two matched workers and coordinator scheduling disabled,
on **local DuckDB, native Quack, Java Quack and PostgreSQL** metadata routes. Each
checks three real physical files and no inline splits, root initial defaults after
rename/INTEGER→BIGINT widening and later insert-default changes, SQL NULL vs VARCHAR
`NULL`, BLOB `00 5c ff`, pre-epoch nanosecond timestamps, DATE, exact DECIMAL(20,3),
SQL-looking literal text, historical target schema, residual predicates and counts.
Stored NULLs in the later physical file must remain NULL, not become backfills.

Local/PostgreSQL nested-file cases independently check added BLOB/integer defaults,
nested stable-ID rename/widening, stored NULL leaves, NULL root/nested parents and
residuals. The local collection case additionally checks evolved STRUCTs inside
LIST/MAP, NULL list elements, NULL containers and empty containers. Direct source
controls ignore spoofed handle default/name text, reject a spoofed target type and
then recover. PostgreSQL malformed-default/oversize and non-Parquet format controls
repair their disposable metadata and recover through SQL on the same one-connection
pools; MySQL missing-field refusal retains an ordinary positive column scan.
Native SQL is used for fixture production, not as a substitute for these connector
worker scans. This remains a single-host test JVM, not a multi-host distribution.

Candidate command (isolated Gradle registry, no global stop):

```sh
GRADLE_USER_HOME=/tmp/opencode/trino-r07-gradle mise exec -- ./gradlew test \
  --tests '*TestR07ConsumerTrial' --tests '*TestLegacyTimezonePartitions' \
  --tests '*TestDucklakeConfig' --tests '*TestDucklakeReadOnlyMetadata' \
  --tests '*TestDucklakeDecodedValueConverter' --tests '*TestDucklakePuffinDeleteReader' \
  --tests '*TestDucklakeTemporalPartitionMatcher' detekt \
  -Pr07CatalogCandidate=/tmp/opencode/catalog-r07-3fd75dc \
  --no-daemon --no-configuration-cache
```

Expanded candidate result: **118 JUnit tests, zero failures/errors/skips**, plus
detekt; all four resolved-JAR attestations repeat. This includes 44 R07 trial tests
(ten added cases), the existing legacy-timezone regression, matcher, decoded-value,
Puffin, configuration and metadata-session controls. Evidence:
`/tmp/opencode/r07-file-defaults-candidate-2.log` and
`/tmp/opencode/r07-file-defaults-candidate-xml/`.
Preliminary reds are retained, not counted as acceptance: native nested-field ALTER
syntax was corrected to the supported enclosing-STRUCT type change; catalog default
refusals and type mismatches now preserve specific SQL-facing diagnostics. The
non-NULL unknown-history contract discrepancy remains explicitly documented below.

Without the candidate property, the **released 0.12.0 / JDBC 1.5.6.0 lane passes
264 tests across 28 maintained classes**, zero failures/errors/skips, plus detekt.
This is the earlier 23-class released command below, additionally selecting
`TestDucklakeFieldIdResolution`, `TestDucklakeNestedFieldDdl`, `TestDucklakeSetFieldType`,
`TestDucklakeInlinedDeleteHandling` and `TestDucklakeDeleteFileHandling`. It protects
existing field-ID/rename/drop-readd, nested DDL/widening, real writes, inline and
physical deletes as well as the earlier SQL/CDC/session/pruning controls. Evidence:
`/tmp/opencode/r07-file-defaults-legacy.log` and
`/tmp/opencode/r07-file-defaults-legacy-xml/`.

The maintained released-lane `types` corpus directory is also rerun with the command
below: **11 files discovered, 9 run, 81 records passed, zero failed**, with the same
two file / three record exclusions; one JUnit test, zero JUnit skips. No golden,
exclusion or corpus pin changed; no full corpus run. Evidence:
`/tmp/opencode/r07-file-defaults-corpus-types.log` and
`/tmp/opencode/r07-file-defaults-corpus-types-xml/`.

```sh
GRADLE_USER_HOME=/tmp/opencode/trino-r07-gradle mise exec -- ./gradlew test \
  --tests '*TestTrinoCorpusReplay' -Dducklake.corpus.dirs=types \
  --no-daemon --no-configuration-cache
```

### Catalog-owner clarification/repro: non-NULL default with unknown history kind

One newly authored negative assertion exposed a discrepancy with
`docs/FILE-INITIAL-DEFAULTS-1.1.md`'s promise to reject unknown/expression histories:

```sql
-- Disposable native final-1.1 producer, data_inlining_row_limit=0:
CREATE TABLE lake.t(i INTEGER);
INSERT INTO lake.t VALUES(1);
ALTER TABLE lake.t ADD COLUMN n INTEGER DEFAULT 42;
-- Direct mutation of this disposable PostgreSQL metadata fixture only:
UPDATE ducklake_column SET initial_default='42', default_value_type='unknown'
WHERE column_name='n';
-- Actual two-worker connector SQL, explicit read-only 1.1 opt-in:
SELECT n FROM t; -- succeeds instead of the documented history-kind refusal
```

`Final11InlineDefaults.value` in the exact source checks the history kind only when
`initialDefault == null` (lines 34–35). A non-NULL initial value still goes through
the closed literal decoder; no SQL execution or wrong returned value was established.
The retained red connector assertion is in `/tmp/opencode/r07-file-defaults-red-xml/`
and `r07-file-defaults-targeted.log` (unknown-kind case at original test line 518).
The maintained negative test now independently exercises **NULL** expression/unknown
history, malformed/out-of-domain literals, non-NULL container defaults and oversized
text, with repaired-query recovery; it does not claim non-NULL history-kind rejection.
Catalog owner must clarify the documented trust contract or tighten it and provide
new checksum-qualified bytes. The connector cannot inspect hidden history through
this API and adds no SQL bypass or fabricated validation to conceal that discrepancy.

Valid known-literal missing-file reads no longer have the old blanket refusal.
Broad R07 remains **NO-GO**: SQL-paged bounded planning/CDC, actual JDBC statement
cancellation/timeouts and physical-file leases/deployment qualification are still
separate gates. Cancellation/paging implementation belongs to the coordinator;
this work neither substitutes for it nor certifies later unpublished catalog bytes.
Strict native R02=3/R03=1/R04=1/M01=6+4 remain unwaived.

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

## October 24: implemented opt-in SQL and worker integration

**The SPI-only implementation boundary is superseded.** Explicit
`ducklake.experimental.enable-final-1.1-reads=true` now enables the real candidate's
read-only mode. It defaults to false. The released 0.12 lane still compiles and runs;
requesting the flag there rejects with a precise missing opt-in API error. A narrow
optional-API bridge invokes only the candidate's real public setter; it does not
fabricate capabilities or admit a format independently of the library.

Implemented:

- Metadata read operations own an in-session validated view, including bounded
  draining/closing of deferred metadata iterators. Mutating SPI methods reject
  before delegation; worker sink creation rejects before opening a writer, and
  maintenance procedures are not exposed in this mode. CREATE/INSERT rejection and
  unchanged row counts are checked through SQL on every route.
- Each opt-in metadata transaction lazily pins its logical default head on its
  first real metadata read (not while Trino holds its transaction-creation monitor);
  explicit query/session/catalog snapshot overrides retain their precedence. A real
  PostgreSQL native-publication test checks that an existing metadata transaction
  keeps its old table set while a fresh one observes the new table. This is not a
  physical-file lease or one cross-thread/cross-node JDBC transaction.
- Split planning, page-source creation, table-function analysis and **lazy CDC file/
  inline source opens** own read sessions. Cached column-tree hits still validate
  admission on production opt-in source opens. Connector shutdown closes its own
  catalog pool. Local native RW reopening after the cluster closes is verified.
- Consumer work limits are explicit: `ducklake.read.max-files` (10,000; hard maximum
  100,000), `ducklake.read.max-physical-rows` (1,000,000; hard maximum 10,000,000), and
  `ducklake.read.max-cdc-events` (100,000; hard maximum 1,000,000). Limits apply only
  to the opt-in mode. They reject, never truncate or return a partial successful plan.
  Descriptor/inline-version counts and physical candidate rows are checked before
  split emission or file lineage work. Ordinary inline cardinality is counted in
  the owned view before payload transfer, including COUNT/virtual-only scans.
  Parquet DATA scans check **actual footer row totals**, not advisory catalog groups.
  CDC checks combined physical event upper bounds, worst-case decoded inline event
  expansion and source units; repeated whole-file scans for per-snapshot units count
  toward the physical work limit instead of multiplying the work silently.
- Physically unconstructable projected 1.1 Parquet fields cannot silently become
  NULL. VARIANT file payload projection rejects explicitly rather than interpreting
  native storage as VARCHAR. Existing decoded inline/CDC, tagged Puffin, lineage and
  historical-count safety fixes remain intact.
  Missing-file-column initial defaults are explicitly refused in the opt-in path
  until the catalog exposes their decoded projection contract; they are not parsed
  as partition text or guessed as NULL. Inline defaults remain supported/verified.
  Unparseable Hive partition constants likewise throw instead of silently becoming NULL.
  Decoded DECIMAL conversion checks Trino precision explicitly: the larger signed
  128-bit domain cannot silently appear as an out-of-domain DECIMAL(38,0) value.
- Checked JDBC connection failures preserve their message/cause instead of escaping
  an SPI proxy as `UndeclaredThrowableException`. Cancellation remains the original
  caller exception; no connector-level query retry was introduced.

### Positive SQL scope (not native-oracle substitution)

The five routes now have **positive distributed Trino SQL** mixed file/inline scans,
opaque BLOB `00 5c ff`, SQL NULL/literal NULL, COUNT, SHOW TABLES, DESCRIBE, and write
refusal controls. Each uses **two matched Trino workers with coordinator scheduling
disabled**. This is the real DistributedQueryRunner server/HTTP/split/worker path,
in one test JVM on one host; it is not separate-process/multi-host packaging,
object-storage deployment or mixed-version negotiation qualification. The local,
native-Quack and direct-Java-Quack metadata routes use the exact test-provided drivers;
this does not certify a plugin archive with absent optional JDBC drivers.

Additional positive worker SQL covers native historical Parquet/Puffin tagged deletes,
equal-path cumulative Puffin CDC, historical COUNT through actual scans, nested stable-ID
rename/widening/default evolution with BLOB/DATE, pre/post update images and old-schema
history. Migrated 1.0→1.1 mixed-timezone partitions also scan correctly through the
explicit read-only flag. The flag-off SQL refusal remains a separate passing control.
The follow-up worker assertions additionally check merged-file NULL starts/embedded
absolute IDs and snapshot DATA filtering, and exact nanosecond TIMESTAMP/DATE rendering.

Recovery tests check: a rejected file-budget query followed by a valid small-table
scan on the same one-connection pools; inline/CDC budget failures followed by a normal
scan; a real missing metadata column failure followed by repair and recovery; bounded
recovery of separate user queries after terminating only this fixture's idle PostgreSQL
connections. The idle-kill test permits a failed next user query instead of promising
transparent retries, and records the failures before recovery.

The controlled PostgreSQL lock/cancel regression also exposed a real integration bug:
eager head reads inside connector transaction creation blocked Trino's cancellation
monitor, and no-SQL metrics callbacks must not open JDBC sessions. Head pinning is
now lazy and those inherited lifecycle/metrics defaults stay no-SQL. The original
blocked thread dump is retained as `cancel-blocked-threads.txt`; only that disposable
fixture's lock-holder connection was terminated to release the red diagnostic run.
No unrelated container, connection, service or Gradle daemon was stopped.
After that fix, cancellation returns a real failed Trino query while PostgreSQL's
metadata statement is **still waiting on the controlled lock**; releasing the fixture
lock lets the owned session roll back and the next SQL scan succeeds. This positively
qualifies cancellation responsiveness/recovery, **not JDBC Statement.cancel**. The
final idle-connection-kill run observed three failed separate user queries before
recovery; the connector did not retry or synthesize a result.

### October 24 verification commands/evidence

The expanded candidate suite passes **108 JUnit tests**, zero failures/errors/skips,
plus detekt, with all four candidate runtime/fixture SHA-256 attestations repeated.
Evidence: `/tmp/opencode/trino-r07-next/r07-final-candidate-3.log` and
`r07-final-candidate-xml/`. Reproduce with the same isolated Gradle prefix and common
`--no-daemon --no-configuration-cache` flags used below:

```sh
GRADLE_USER_HOME=/tmp/opencode/trino-r07-gradle mise exec -- ./gradlew test \
  --tests '*TestR07ConsumerTrial' --tests '*TestLegacyTimezonePartitions' \
  --tests '*TestDucklakeConfig' --tests '*TestDucklakeReadOnlyMetadata' \
  --tests '*TestDucklakeDecodedValueConverter' --tests '*TestDucklakePuffinDeleteReader' \
  --tests '*TestDucklakeTemporalPartitionMatcher' detekt \
  -Pr07CatalogCandidate=/tmp/opencode/catalog-r07-58ef4d7 \
  --no-daemon --no-configuration-cache
```

The two later merged-row-ID/nanosecond worker assertions are also rerun explicitly
in `r07-extra-worker-assertions.log`; no production source changes follow the expanded
suite. No full corpus, strict native requalification campaign or artifact/source repin
is included in these counts.

Without the override, the expanded **released 0.12.0 / JDBC 1.5.6.0 lane passes 242
tests across 23 maintained classes**, zero failures/errors/skips, plus detekt
(`r07-final-legacy.log`, `r07-final-legacy-xml/`). This includes actual INSERT/CTAS/DDL
controls, read/CDC/partial-file paths, snapshot resolution, pruning/residuals, nullable
IDs, decoded conversion and metadata session/cancellation unit controls. It is the
15-class released-lane command recorded below plus `TestDucklakeReadOnlyMetadata`,
`TestChangeFeedPageSource`, `TestDucklakeSnapshotResolver`, `TestDucklakeInsertAndCtas`,
`TestDucklakeDdl`, `TestDucklakeTemporalPartitionMatcher`, `TestDucklakePartitionPruning`,
and `TestDucklakeIdentityPartitionResidualFilter`, still without the candidate property.

The maintained `types` corpus directory is rerun on that released lane: **81 records
passed, zero failed**, with the same **two file / three record exclusions**; one
JUnit test, zero JUnit skips (`r07-final-corpus-types.log`). Corpus source remains
`c23aca43b6033acfac2b8cc8d80b15ae7fa729b2`, not the candidate's different corpus pin.
No semantic golden or exclusion list changed. All new trial cases are independently
expected native fixtures/connector scans, not rewritten upstream goldens.

### Exact remaining catalog-owner/API requests — broad R07 still NO-GO

**These limits are not a complete coordinator-memory or whole-query/global budget.**
`getDataFiles`, `getInlinedDataInfos`, column/name/partition list APIs and
`getInlinedChangesBetweenDecoded` can materialize before the connector sees their
size. A consumer-side `size > limit` check cannot retroactively bound that transfer.
No whole-catalog preflight materialization, direct metadata SQL bypass, shared-storage
staging or guessed capability was added to hide this gap.

The exact pinned candidate needs SQL-limited, snapshot/session-owned keyset APIs for:

1. Active data/delete associations (composite data-file/delete-file cursor), added
   files and deletion candidates, preserving equal-path/tagged windows and NULL IDs.
2. Column/schema/table enumeration, partition/name maps and inline schema versions,
   with page-local hierarchy resolution plus explicit total page/path/depth/traversal
   budgets. Existing retained-path pages do not replace active scan descriptors.
3. Decoded inline ordinary/CDC payload pages carrying original row/begin/end identity
   and stable target-schema projection. Live-row counts do not bound deleted/transient
   CDC history. A bounded-row iterator without SQL LIMIT is not sufficient.
4. A supported per-read-session statement cancellation/timeout hook. The current
   thread-owned Supplier API does not expose a live JDBC Statement for cancellation.
   Metadata connection failures can occur while setting session state before the
   Supplier body; the connector now preserves that failure rather than hiding it.
5. Public decoded initial-default projection for a missing physical file field,
   qualified by table/stable field ID/target snapshot and backend. The library's
   internal inline projector has this validation, but old `initialDefault: String?`
   alone is not an authorization to evaluate SQL or guess NULL/literal semantics.
   The opt-in connector therefore rejects that file backfill operation while
   retaining ordinary columns and already-qualified inline evolution.

In-flight cancellation, arbitrary network partitions, retention/physical-file lease
recovery, full cross-route CDC/type evolution, unsigned/adversarial file encodings and
multi-process deployment remain limited/unverified. The current signed native producer
explicitly rejects `DUCKLAKE_VERSION '0.4'` (probe in `legacy04-native-probe.log`);
no 1.0 catalog was retagged to manufacture a 0.4 test. A real approved legacy fixture
is needed for fresh 0.4 end-to-end evidence. Released-lane regressions still protect
the unchanged legacy branches, but do not certify that missing fixture.

**GO:** further isolated, known-small, matched-peer read-only SQL qualification with
this exact candidate and explicit flag. **NO-GO:** production/default-on rollout,
unknown-size/adversarial catalog guarantees or marking all R07 complete. All original
no-publication/no-push/no-deployment/no-migration/no-connector-write/no-cleanup boundaries
and strict native failures remain unchanged. Catalog and Doris sources are untouched.

## October 23 checkpoint (historical; superseded by October 24 scope above)

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

## October 23 handoff and catalog feedback (historical)

No new catalog-library defect was established; the concrete incompatibilities fixed
here belonged to the connector. Minimal executable repros are the named qualification
tests, especially `nativeCumulativePuffinEqualPathsDoNotCancelCdc` and
`nestedStableIdRenameWithBlobAndTemporalTextDoesNotReapplyOldNames`.

The October 23 handoff required an explicit default-off flag with complete
metadata and lazy worker session ownership; replace/bound materialized production
planning/CDC lists with total page/path/traversal/work budgets; qualify actual matched
worker SQL scans and cancellation/network/retention recovery; extend route-specific
payload/file/evolution/CDC coverage and legacy 0.4 verification. Mixed-version negotiation
was neither implemented nor fabricated. Paths and read sessions never authorize cleanup.

That checkpoint's SQL integration gaps are addressed by the October 24 implementation
above; its original SPI-only NO-GO is no longer the whole result. Broad R07 still
remains open at the bounded catalog transfer/cancellation/lease/deployment gates.
No Doris or coordinating-session result is inferred.
