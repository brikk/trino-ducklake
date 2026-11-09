# Crutches And Shortcuts

This inventory records the safety restrictions introduced for RV-03 and RV-04. It is not an audit
claim that all older debt is listed here; see [the review](REVIEW-2026-09-05.md) for that assessment.

## Trino 484 retained-memory reporting

The 484 SPI contexts receive conservative reservations for the merge sink's retained delete
positions, lineage cache and insert sinks, plus the scan provider's bounded column-tree cache.
These are retained-state estimates, not a bound on temporary allocations while loading prior
deletes/lineage or serializing output. Maintenance CALLs have no scan memory context and keep
their existing maintenance limits. 483 exposes neither new SPI context and retains its previous
engine accounting contract. The new JSON text-block conversion allocates per output page;
nested JSON writes retain the existing explicit unsupported restriction. See
[Trino compatibility](TRINO-COMPATIBILITY.md) for the compile-time adapter boundary.
Native-row source reservations conservatively count shared values more than once and
measure inline JSON using a temporary single-value block. Page production is byte-budgeted;
this does not retroactively bound the released catalog's older list-returning inline reads.
Read-side delete filters reserve Roaring64 retained storage, not a boxed-long cardinality
estimate. CDC reserves its query-local footers, primitive identity arrays and bitmap sets,
releasing them on close/exhaustion. Shared lineage can be conservatively counted both in
the file cache and in the units. Trino 484's default per-source `MemoryUsageReportingPageSource`
forwards these `getMemoryUsage()` values to its `MemoryContext`; provider-shared column-cache
reservations remain separate. These estimates are not a streaming or query/node-global bound.

## Native snapshot-column compatibility

The hidden `$insert_snapshot_id` uses native per-row insertion snapshots, while
existing `$snapshot_id` keeps its file-begin/inline-row contract. No plain
`snapshot_id` name is reserved, so CREATE/ADD of a real stored column still works.
Replay binds that native virtual name only after checking real table metadata.
Keeping a `$`-prefixed connector name is intentional: Trino's ADD COLUMN checks
hidden names too, so exposing a plain hidden `snapshot_id` would wrongly prevent
adding a real column. Regression coverage includes ADD COLUMN and identical SQL
before/after that stored column appears, plus CTE output-name preservation.
The row virtual reads the reserved Parquet field ID as a
streamed BIGINT, with a file-begin fallback only when that ID is absent; malformed
present fields reject. There is no extra whole-file snapshot array or SQL text
substitution. Existing delete/position pruning restrictions still apply.
Both ordinary and partial rewrite outputs preserve that streamed per-row snapshot
under the reserved field ID. Partial file visibility bounds still use source FILE
visibility, not the older row origins: backdating an ordinary-compacted file to
its rows' original insertion times would duplicate historical rows. Recompaction
checks cover both snapshot values and historical visibility. Older files without
the reserved ID retain the native file-begin fallback; lost origins are not guessed.

## R07: Experimental Planning, Explicit Opt-in Qualification Only

### Qualification fixture format-selection blocker

The four legacy `TestPublishedNestedReads` cases select a 1.1 client for stored 1.0 lakes.
The direct-reader statistics cases `countRefusalAndOwnedSessionFailureRecoveryWithBoundedCompanionPages`
and `unknownDeleteCountIsNotZeroOrAnExactOptimizerEstimate` in `TestR07ConsumerTrial` set
`enableFinal11Reads` after `toCatalogConfig()` has already locked the client to 1.0, then open
stored 1.1 lakes. All six fail at format admission before reaching the metadata constructor;
the same failures reproduce on `bca49a1`. These opt-in tests are not passing qualification.
Repair the fixtures' format selection, not the production admission checks or skip list.

### Current bounds API in released catalog 0.17.0

Normal dependencies use Central 0.17.0, byte-identical to the accepted
`0.17.0-20261106.162112-2` snapshot. The temporary selector/repository/hash file is
retired, and the bounds/write-guard tests are retained in normal sources. No
production planner path adopts the new API: existing file stats/time-travel
behavior is retained. Any later use of current bounds must stay inside the latest
read session, treat omissions as unknown and NaN-bearing ranges as non-pruning;
gross supersets must never become MIN/MAX answers. See
[the retained trial](archive/CATALOG-0.17-TRIAL.md).

### PgBouncer configuration

The PgBouncer fix adopted in 0.16.0 remains in normal catalog 0.17.0.
The old local selector/repository/hash file are removed.
The four pool controls retain their original defaults and the JDBC URL is passed
through intact. Transaction pooling requires PgBouncer >=1.21 with
`max_prepared_statements > 0`, or JDBC `prepareThreshold=0`; keep maximum connection
lifetime below the proxy's `server_lifetime`. This does not qualify MySQL/ProxySQL,
broader 1.1 behavior or deployment. The old connector 0.5.2 release still contains
0.15.1; it is not safe for transaction pooling. The
[local trial record](archive/CATALOG-0.16-LOCAL-TRIAL.md) retains the scoped evidence.

### Catalog 0.15 expiry recovery

The expiry recovery contract introduced in 0.15 remains in 0.17.0. Snapshot
expiry commits before resumable catalog cleanup. A subsequent cleanup failure
throws `DucklakeExpiryCleanupIncompleteException`, retaining the committed
expiry and scheduled-file counts and the original cause. Since 0.15.1, file
counts/budgets advance only after page commit; rolled-back work is not reported.
Resume via
`cleanUpExpiredObjects`, never retry the expiry IDs. The resume API is not yet
exposed as connector SQL; `cleanup_old_files` drains the physical queue only.
The connector still uses the legacy finish-cleanup overload and does not expose
new budget knobs. The library's new bounded overloads now enforce hard file
limits, including dependent delete files. See the
[adoption record](archive/CATALOG-0.15.0-ADOPTION.md); the
[snapshot trial](archive/CATALOG-0.15-TRIAL.md) is historical evidence only.

### Released catalog and independent format validation

Both build modes now consume released catalog/replay/SLT **0.17.0**, with test-only
DuckDB JDBC **2.0.0.1** and direct Java Quack JDBC **0.13.0**. Native test setup
explicitly creates DuckLake 1.0 with automatic migration disabled. This is not
promotion of the candidate planning adapter or final-1.1 read/write admission.
The released shared signature parser is used in both lanes; the old 0.12 no-parser
hook and reflective opt-in/default-method bridges have been removed. The historical
snapshot selection is retired; experimental budgets remain unchanged. References to
the old normal 0.12 lane describe the original qualification, not today's default.
Replay uses the released `ReplaySetupFormat.DUCKLAKE_1_0` native default setting;
it does not rewrite corpus ATTACH text or goldens. Explicit format choices and
SET/RESET retain native semantics. Existing newer lakes cannot be downgraded.
The original 0.14.0 acceptance remains in the archive. The connector explicitly requires the selected stored
format via `ducklake.catalog.format-version` (1.0 by default, 1.1 only in the
separately qualified read-only build). Reads and writes refuse a mismatch, including
after native migration. Normal metadata lookup/listing, split/source and function
analysis use owned read sessions; unsupported-format failures retain NOT_SUPPORTED.
This does not relax 1.1 admission, work budgets or deployment restrictions.

The normal corpus suite uses both mirror and execution scope 1.0. Explicit 1.1
files halt after positive stored-format inspection: earlier results stand, remaining
1.1 feature SQL is not executed, and halted files never count as passed 1.0. Format
inspection errors fail loudly. This is deliberate format selection, not a directory
skip or downgrade. The full JUnit gate uses CI's actual 75-file starter discovery,
not a general-only subset.

The `merge` directory alone currently fails minimum mirror coverage: its queries
are rejected by the existing read-shape/alias gates before connector execution.
The `partitioning` directory also contains implicit newer-format fixtures whose
epoch transforms and NULL type fail in the fixed 1.0 native oracle, with cascading
golden failures. Both reproduce on `d92f048`; neither is a passing qualification
run. Repair the fixtures' format/namespace qualification without lowering coverage
requirements or adding skips.

PostgreSQL replay binds only the default schema qualifier of known rewritten
metadata catalogs using the shared public oracle-SQL hook and brikk-sql AST.
Explicit metadata schemas, logical lake schemas, literals, source files and goldens
are unchanged. This test-only backend adaptation fixes direct native metadata SQL;
it does not add native DuckDB-view dialect support to the production connector.
No copied metadata database, fake schema/view or shared-storage staging is used.

The November 3 experimental verification hit a kernel-confirmed global OOM kill
(test JVM exit 137), then a real `Disk quota exceeded` during native-library
extraction into system temp. Neither run is a pass. After the user freed memory,
verification returns to the original 3 GiB/parallel test configuration, using
workspace-local `java.io.tmpdir`/`TMPDIR` as documented for actual quota failures.
This is ordinary build/test scratch, not production staging or shared storage.
Production settings, explicit concurrency tests, admission/work limits and
assertions were unchanged and no test was excluded from that experimental rerun.
See the adoption record for its historical scope.

Unknown-COMMIT and acknowledged-post-commit failures retain their catalog cause
and diagnostic through the existing non-conflict Trino error mapping. They do not
authorize automatic retry or deletion. Connector-boundary tests inject each failure
after real INSERT/CTAS/UPDATE/DELETE/MERGE publication and verify engine rollback
leaves exactly one publication and readable registered output. This is not a JDBC
network-fault or automated reconciliation guarantee. Operators must retain uncertain
output and reconcile registrations on a fresh connection before retry or cleanup;
the attempted snapshot ID alone is not proof. Existing retention/ownership gaps remain.

The dependency update also re-probes native MySQL in a standalone JVM. DuckDB
2.0 fails native metadata-schema resolution before cross-engine data access;
Trino's direct Connector/J worker tests pass. Native interoperability remains
unqualified, without adding a test skip or claiming the old SSL crash reproduced.
See [the minimal native reproduction](archive/MYSQL-NATIVE-PROBE-2026-11-03.md).

### Legacy TIMESTAMPTZ partition safety

Transformed TIMESTAMPTZ partitions are not used to exclude non-NULL candidates:
legacy native calendar transforms use the writer's timezone, and migrated/mixed
catalogs retain those values. Neither catalog version, query timezone nor the
calendar/epoch leniency option proves file-specific UTC provenance. Exact residual
predicates remain in Trino; DATE/unzoned timestamp and identity pruning are unchanged.
Cost: extra files may be scanned. Removal requires a real file-specific transform/
writer-zone attestation, not a version check or a future native upgrade.

`-Pr07CatalogCandidate` now selects only the experimental adapter and
`qualification/src` tests, on the same 0.17.0 release and driver pins as the normal
build. `verifyCatalogRelease` checks all four JARs against
`gradle/catalog-release.sha256` in both modes. Old snapshot/local-bundle values
fail explicitly. Experimental output versions retain `-r07`; this is not a supported
plugin distribution or default-on rollout. The public bounded type-signature parser
is shared by both modes, preserving quoted names and existing scalar mappings;
parsing is not storage/type admission.
See [the candidate contract](CATALOG-CANDIDATE.md) for current selection and remaining scope;
the [published handoff](archive/HANDOFF-CATALOG-72aae19.md) retains exact evidence.

Legacy native nested JDBC values now use standard STRUCT/ARRAY/MAP conversion, not
`toString()` parsing. Native STRUCTs require source-era field order from the existing
complete identity mapping; no positional target-schema guess or fabricated names are
used. Text-backed payloads retain their path. This does not retroactively bound legacy
list-returning reads or broaden composite-key admission. Native Trino's textual ROW
case canonicalization is separately controlled; source/SPI names and quoted SQL values
remain exact requirements. Rename fixtures use fresh readers, not a concurrency claim.

The November 1 verification uses a workspace-local `java.io.tmpdir` because actual
Kotlin backup writes failed with `Disk quota exceeded` under `/tmp`. Qualification
fixtures honor that standard JVM setting. This is build/test scratch only, not a
production staging path or a shared-filesystem assumption; tests/native pins are unchanged.

October 24 adds `ducklake.experimental.enable-final-1.1-reads=false` by default.
The opt-in now calls the released public flag setter directly. Its former reflective
0.12 compatibility bridge was removed at release adoption. Normal planning rejects
the flag before allocating a catalog connection; read admission is never emulated.

Read-only SPI metadata operations own sessions, materialize deferred metadata
iterators under a limit, and reject mutations before delegation. Worker source
creation and lazy CDC file opens own sessions; shutdown closes each node's catalog
pool. SQL qualification uses two actual Trino workers with coordinator scheduling
disabled, still in the same single-host DistributedQueryRunner JVM. It is not a
multi-process/multi-host deployment or mixed-version negotiation test.

The `10f81f9` qualification introduced a **typed public-API adapter** in
`candidate/src`; the default build retains its normal 1.0 adapter in `released/src`.
Both now consume released 0.17.0, without enabling the experimental adapter by default.
The candidate adapter drains SQL-keyset active/added files, schemas/tables/flat columns,
partition fields/values, immutable name rows and inline registry versions. Each outer
view shares totals: 1,024 page calls (including empty terminals), 16,384 returned nodes,
20,000 paths, 16 MiB conservative UTF-8 text allowance, 131,072 logical work units,
64 hierarchy levels and configured file/physical-row limits. Page size defaults to 128
(1..1024); overflow fails, not a partial plan. Full column/name hierarchies are validated
only after terminal enumeration. Unknown name-map partition flags refuse rather than
become body-carried or Hive partition values. Missing physical inline tables retain
their registry positions and do not get synthetic empty-payload splits.
Legacy 0.4/1.0 list contracts are delegated, not re-labelled SQL-paged.

The `6730bb8` candidate continuation also drains ordinary/CDC inline payloads,
deletion candidates, file-local inline positions, exact retained-file ID batches and
retained snapshot/change history. Ordinary identity-only scans no longer require a
separate live-count/begin-snapshot list. Encoded/decoded/default-copy allocation guards
are enforced inside the library page APIs; candidate projection/default calls reserve
8,192 logical work units plus eight per projected root, even on terminal pages.
Budget and page failures stick across nested calls and caught exceptions. This is a
conservative work admission policy, not complete accounting of internal schema bytes:
the library has bounded source/target/default preflights but no shared accounting hook.

These are **per-owned-view bounds on these APIs**, not a heap/network/DB execution-work
bound or a total query/cross-node budget. Scoped point lookups, stats/comments/views,
and internal schema/default accounting are not all closed. The old view/tag/comment
delegates remain a concrete qualification blocker, not certified bounded metadata.
The candidate omits optional optimizer estimates before the legacy file-column
aggregate; it never replaces unknown statistics with zero.
The released lane and direct legacy SPI fixtures keep their older paths; new public
pages do not retroactively page an unwrapped catalog. Parquet scans check actual footer
row counts. Keep the known-small limits;
there is no direct metadata SQL, whole-catalog preflight or staging workaround.

Owned candidate sessions use single-use controls and a positive default 30-second
deadline (1..3600). Metadata `cleanupQuery` cancels the actual registered controls by
Trino query ID; ordinary/split/function analysis and lazy CDC file-open sessions use
the same typed boundary. PostgreSQL engine cancellation and a two-second JDBC deadline
are checked before fixture unlock, with one-connection recovery. This does **not**
certify worker task-close cancellation during blocking source construction, other
drivers' interruption, pool acquisition/setup/prepare/network partitions, arbitrary
callback computation or retention leases. No helper executor, connection kill or
hidden retry is used in production. Head resolution remains outside transaction creation.

The bounded quality repair snapshots registered controls under the registry lock,
then calls drivers outside it. JDBC/runtime failures are collected so each captured
control is attempted, with the first exception rethrown and later distinct failures
suppressed. A blocking driver still delays the cancellation caller; controls
registered after the snapshot are not covered by that request. The candidate's
schema/table lookup uses ASCII-only case equivalence, preserving distinct Unicode
identifiers and still rejecting genuine duplicate names.

The `3fd75dc` candidate adds decoded, stable-ID/backend-qualified missing-file defaults.
The connector now calls that public API directly; its former narrow optional-method
bridge for released 0.12 was removed. No fallback parses text or guesses NULL.
Footer/stable-ID resolution proves absence before calling it. Fresh
target metadata validates the projected type; handle default text is never trusted.
Nested projection rebuilds only affected container branches, retaining present stored
NULLs, NULL parents/elements and empty containers. This costs extra block allocation
on evolved nested files. The API still reloads target metadata for each absent field;
this is not a paged/whole-query bound. MySQL missing-field evolution, non-NULL whole-
container defaults, nested name-map files without authoritative child IDs, ambiguous
partially ID-annotated files and unconstructable physical fields/VARIANT reject.
Decoded DECIMAL values outside Trino's declared precision cannot silently narrow.
The `10f81f9` candidate corrects the earlier non-NULL unknown-history discrepancy:
the real worker-SQL negative assertion and repaired-query recovery are restored.
Known native expression-kind plus materialized literal (including VARCHAR `NULL`)
remains valid; expression-kind without that value rejects. No expression executes.

No production/default-on rollout follows from these opt-in experiments. Remaining
gates include remaining bounded catalog transfers, broader cancellation/network faults,
retention/lease coordination, broader route/type/legacy qualification and deployment
testing. See [the candidate contract](CATALOG-CANDIDATE.md) and
[retained qualification evidence](archive/R07-TRINO-CANDIDATE-QUALIFICATION.md).

## RV-03: Path Comparison Boundary

**Status: user-approved fail-closed restriction, 2026-09-05.** The alternative was to defer the
repair until Trino exposes an authoritative filesystem-identity API. This implementation never
guesses that unresolved aliases identify different files and therefore permit deletion.

- Native local: `file:/`, `file:///`, and `local:///` use the same root-relative comparison
  namespace when `fs.local.enabled` is true (legacy `fs.native-local.enabled` also works).
- Native S3: `s3`, `s3a`, and `s3n` use the same exact bucket/key namespace when `fs.s3.enabled`
  is true (legacy `fs.native-s3.enabled` also works). Slashes, dots, percent text, and case remain
  literal object-key data. No URI decoding or object-key normalization is performed.
- Other routes: only canonical paths within one identical scheme/authority namespace are
  compared. Azure's container component is included, not discarded as URI credential text.
- Bare paths use Hadoop's configured default filesystem, not necessarily the local disk. Trino
  does not expose that active identity through `TrinoFileSystem`. Any mixture involving an
  unresolved namespace aborts before deletion, even if the differing reference is outside the
  selected scan root. Existing wholly bare-path Hadoop cleanup still runs.
- Non-S3 dot segments, repeated separators, backslashes, ambiguous local authorities, and
  listing namespace changes are refused. Dry runs have the same checks. No tests are skipped
  to hide these refusals; mixed bare/file-URI imports have an explicit refusal regression.

**Cost:** legitimate mixed-namespace or noncanonical layouts may no longer be cleanable through
this procedure until their identities can be established. Do not rewrite metadata paths blindly:
native local URIs are interpreted under `local.location`, whereas bare paths go through Hadoop.
The matcher leaves persisted spellings and filesystem deletion locations unchanged.

**Not a universal physical-identity proof:** symlink/mount aliases, case-folding filesystems,
alternate endpoints, independent catalogs, path replacement, and concurrent import/cleanup need
their own contracts. The helper does not call coordinator-local `toRealPath`, assume shared
worker filesystems, or introduce a private storage format. RV-04 adds retained-owner protection
for scheduled cleanup; the import-versus-orphan-sweep race remains RV-X03.

**Removal criterion:** obtain an authoritative backend/root-aware identity or same-object API
from the actual configured filesystem, preserving object keys/generations and rejecting
unsupported aliases. Add real backend tests before relaxing a guard. An independent Hadoop
configuration, string lowercasing, or filesystem-wrapper reflection is not a replacement.

**Verification:** native-local tests use a real catalog and a non-`/` filesystem root, including
retained and scheduled references. MinIO tests exercise both S3 aliases with a real orphan
deletion control. Unit tests cover exact-key distinctions, current/legacy enable properties,
Azure container separation, and refusal before deletion after an invalid listing entry.

## RV-04: Bounded Practical Cleanup

**User-approved scope, 2026-09-05:** prevent sequential deletion of a still-shared object and
cancel its obsolete selected queue request. The user accepts the remaining simultaneous
import/cleanup race; no upstream agreement is a prerequisite for this bounded improvement.

- One call selects up to 256 requests by default, hard-capped at 1024. Both data and delete
  references are scanned in pages no larger than that batch size, with a combined default
  budget of 1,000,000 references and a configurable hard maximum of 10,000,000.
- SQL path projections include at most 16,385 characters to detect values over the 16,384-character
  limit. Root lookup is limited to two rows to detect duplicates; page-local hierarchy queries
  allow one duplicate sentinel. The reference budget also allows one overflow-detection row.
  These are character/row bounds, not exact byte limits, wall-clock limits, or limits on the
  database's internal scan/sort work.
- Exact selected id/path/relative/timestamp tuples are cancelled/acknowledged in chunks of 64
  predicates. All identical duplicate tuples match; that is not a 64-affected-row guarantee.
- Exact shared-directory requests can be cancelled. Prefix-only overlaps are deferred and left
  scheduled with a warning because a child's later retirement may not reclaim the whole parent.
  Reconcile these manually before running another engine's cleaner.
- A legacy directory request can contain many physical objects. The selected-request cap is not
  a count of recursive filesystem deletions. Independent cleaners, path replacement, external
  owners, and simultaneous import/cleanup remain outside this fix's guarantee.
- Failed/deferred oldest requests remain queued and can occupy subsequent batches. Their cause
  needs operator attention; this implementation adds no retry ledger or queue fairness protocol.

**Dependency gate resolved, 2026-09-05:** the connector now consumes the official `0.9.0`
catalog and corpus-replay artifacts from Maven Central. The temporary feature-snapshot pin is
removed; no local catalog build is required. Published-artifact verification is recorded as
`RV-V11` in the review. This closes the dependency gate, not the concurrency limitations above.
Do not replace the bounded APIs with an unbounded fallback to an older release.

**Verification scope:** real shared-file lifecycle tests cover Trino followed by stock DuckDB
cleanup, last-owner reclamation, and MinIO with an S3 scheme alias. Initial catalog query
verification covered PostgreSQL, MySQL, local DuckDB, and Quack, including hostile path collations
and bounded projections; `RV-V11` records this connector's checks against the official release.
The old orphan-sweep full-reference materialization remains the separate RV-25g item.

## CI-04: Test-Only MAP Access Adaptation

**Status: local corpus-adapter bridge, 2026-09-11.** The ordinary transpilation gate accepts
DuckDB string-key MAP access such as `s.i`, but Trino interprets that syntax as ROW access.
`TrinoMapAccessRewriter` uses the existing brikk-sql parser, qualifier, and type annotator with
table schemas obtained through `SELECT * FROM <table> LIMIT 0`. Proven string-key MAP accesses
become safe brackets, emitted as `element_at`; missing keys therefore return NULL.

- This code is under `test/src`, not in the production plugin or the shared catalog library.
- Metadata probes are deduplicated within one query only. DDL and lake reconnects must refresh
  the types. Explicit table snapshot clauses remain on the metadata probes.
- brikk-sql 0.8.0 does not infer MAP-column bracket result types, so the adapter supplies the
  known MAP value type and re-annotates dependent expressions. Passes are bounded by the
  qualified AST's initial Dot count plus one; key spelling/case is preserved separately from
  identifier normalization.
- Non-text MAP keys receive no guessed implicit casts. If one query references different
  historical schemas/orderings of the same table, the name-keyed schema resolver is not used
  to guess a single type. In either case the original SQL still executes normally: this is not
  a new skip rule or a claim of support for those additional forms.
- ROW access, table aliases, nested MAP/ROW values, CTEs, missing keys, key case/punctuation,
  and type refresh after DDL/reconnect have focused regressions, including real DuckDB/Trino
  comparisons. The normal starter corpus plus `types` also passes in focused runs.

**Removal criterion:** a released, schema-aware transpilation path handles these MAP accesses
and their value-type propagation directly. Do not replace this with a global text replacement
of dotted names; those names can denote table aliases or ROW fields instead.

## CI-02: Row-Identity Reader Limits

**Catalog 0.10.0 integration, 2026-09-11:** the old contiguous-range mutation locator was
replaced with the user-approved, upstream-aligned `ROW(data_file_id, physical_position)`.
Valid NULL starts do not trigger a mutation restriction or an invented start. Logical IDs use
embedded lineage first, then a present start; missing/malformed required identity fails loudly.

Existing reader costs remain relevant: lineage is materialized as a per-file `LongArray`, and
physical positions are counted over whole-file splits with predicate/row-group pruning disabled
when positions are needed. Removal requires a streaming lineage/physical-position read path;
never enable pruning or split-by-row-group while retaining a zero-seeded cumulative counter.
The legacy nonstandard global-ID delete-file reader requires a real catalog start for rebasing;
it cannot infer one from a NULL start. Standard positional deletes do not require that start.

## CI-03 / RV-28: Geometry Mirror Coverage

**Typed normalization adopted from corpus-replay 0.10.0, 2026-09-11.** The four local WKB/WKT
file skips were removed. The original three nested-geometry fixtures use bare table names after
`USE ducklake`; the adapter's existing alias gate excludes bare names because they may identify
oracle-local temporary tables. A passing original fixture therefore proves golden validation,
not execution of its nested SELECT through Trino.

`TestTrinoGeometryReplay` preserves the upstream fixtures' setup and goldens in test-local copies,
explicitly qualifies only their known lake SELECTs, and asserts each SELECT actually returned
Trino rows before the shared mirror comparison passed. These copies are regression fixtures,
not a relaxation of the production adapter gate or a claim that every original corpus query ran.
Removing the general coverage limitation requires trustworthy oracle namespace/temp-table
binding information; blindly accepting or qualifying arbitrary bare names is not a fix.

## Corpus Engine-Error Classification

`TrinoReplayEngine.classifyEngineError` (test code) turns a Trino mirror error into an
engine-skip only by the server-side error code: connector-thrown `NOT_SUPPORTED`,
`FUNCTION_NOT_FOUND` for an unknown function name, `TYPE_MISMATCH` against a bounded
`varchar(n)` literal, and `TABLE_NOT_FOUND` for a name the lake never held as a table and holds
only as a non-Trino-dialect view or not at all. Everything else fails.

- Two parts still read message text, each only inside its error-code branch: the
  `FUNCTION_NOT_FOUND`/`TYPE_MISMATCH` narrowing patterns, and the relation name extracted from
  Trino's `Table '<catalog>.<schema>.<table>' does not exist`. Trino exposes no structured field
  for either.
- The `TABLE_NOT_FOUND` check queries the replay PostgreSQL database's `ducklake_table`,
  `ducklake_view` and `ducklake_schema` directly, across all snapshots, so a table that exists
  at any snapshot fails instead of skipping.
- Production must keep `NOT_SUPPORTED` for deliberate capability gates only; not-found and
  internal/corrupt-state conditions use `*_NOT_FOUND` or `GENERIC_INTERNAL_ERROR`. A new
  `NOT_SUPPORTED` for anything else silently becomes a corpus skip.

**Removal criterion:** the replay library reports which relations a query reads from the lake
versus the oracle session, and Trino reports the unresolved name structurally.

## Catalog-Stats File Pruning Limits

`DucklakeSplitManager.pruneDataFiles` prunes from the table handle's unenforced predicate through
`findDataFileIdsInRange`, with bounds rendered by `DucklakeStatsPredicateNormalizer`. It is
deliberately narrower than the stats allow:

- Only columns whose Trino type is the connector's own mapping of the stored DuckLake type prune
  (integers, `float32`/`float64`, `decimal(p,s)`, `date`, `timestamp[_s|_ms|_ns]`, `timestamptz`,
  `varchar`, `boolean`). Unsigned/hugeint promotions, VARCHAR degradations, UUID, binary, time
  and nested columns keep every file.
- A domain that admits NULL never prunes: the range API ignores null counts.
- Exclusive bounds are sent as inclusive, so `c < 11` keeps a file whose min is 11.
- A file whose `begin_snapshot` precedes the column's current `ducklake_column` version is never
  excluded by that column. Catalog 0.17.0's `setColumnType` keeps bounds verbatim within a
  comparison class, so REAL-era text (`0.1`) reads as a smaller DOUBLE than the widened row
  value after `REAL -> DOUBLE`; upstream DuckLake rewrites (`WriteFloatWidenedStats`) or clears
  such bounds. Cost: after any column-version change (type change, rename) older files of that
  column are scanned.

**Removal criterion:** the catalog rewrites or clears bounds on promotions whose text does not
reread exactly (as upstream does), and exposes per-file stats type provenance; the range API
accepts exclusive endpoints and null-admitting predicates.

## Unbounded maintenance catalog reads

ducklake-catalog 0.17.0 has keyset pages for data/delete file references but none for the
deletion queue (DuckLake 1.0) and none for inlined rows, so two procedures still load a catalog
result whole. The APIs that would remove these are in TODO.md under "Writes and maintenance".

- `remove_orphan_files` loads every referenced path via `listAllReferencedFiles()` and holds it
  as a set. Our own side is bounded: dataset-directory ownership is an O(path depth) set probe,
  and one call lists until it has `MAX_ORPHANS_PER_CALL` (10,000) orphans, deletes those and
  asks for a repeat. Removal criterion: the queue page exists; then page all three reference
  kinds under `ducklake.cleanup.max-reference-files`, as `cleanup_old_files` does.
- `flush_inlined_data` loads each inlined schema version's rows with `getInlinedChangesBetween`.
  Before loading it refuses (`GENERIC_USER_ERROR`, nothing written) when the rows live at the
  read snapshot exceed `ducklake.read.max-physical-rows`. Rows deleted but never flushed are
  loaded too and are not counted, because the catalog cannot count them. Inlined data is not
  bounded by construction: every DuckDB insert under `data_inlining_row_limit` adds rows until
  someone flushes. Removal criterion: a paged inlined change read streamed into the writer.

## Legacy JDBC timestamp offsets

`DucklakeTimestampValues` centralizes timestamp parsing/encoding, but deliberately keeps an
explicit `jdbcOffsetAsInstant` input policy. The legacy JDBC converter normalizes offset-bearing
unzoned timestamp text to UTC. DuckDB and catalog 0.17.0's `InlinedTemporalValues.localTimestampText`
instead ignore the offset and keep wall-clock time; identity-partition and decoded catalog values
follow that rule. The existing JDBC compatibility tests require its different behavior and remain
unchanged. Normal decoded catalog TIMESTAMP values are already `LocalDateTime`.

Removal criterion: separately approve changing the legacy JDBC offset contract, with driver/backend
qualification and an intentional test-contract change. Do not silently treat its text as catalog
TIMESTAMP text. See [WP6 measurements and semantics](archive/WP6-READ-PATH-PERFORMANCE.md).
