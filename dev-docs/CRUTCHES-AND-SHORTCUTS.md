# Crutches And Shortcuts

This inventory records the safety restrictions introduced for RV-03 and RV-04. It is not an audit
claim that all older debt is listed here; see [the review](REVIEW-2026-09-05.md) for that assessment.

## R07: Temporary Candidate, Explicit Opt-in Qualification Only

### Legacy TIMESTAMPTZ partition safety

Transformed TIMESTAMPTZ partitions are not used to exclude non-NULL candidates:
legacy native calendar transforms use the writer's timezone, and migrated/mixed
catalogs retain those values. Neither catalog version, query timezone nor the
calendar/epoch leniency option proves file-specific UTC provenance. Exact residual
predicates remain in Trino; DATE/unzoned timestamp and identity pruning are unchanged.
Cost: extra files may be scanned. Removal requires a real file-specific transform/
writer-zone attestation, not a version check or a future native upgrade.

`-Pr07CatalogCandidate=<bundle>` exclusively selects the local catalog/SLT/replay
0.13.0-SNAPSHOT candidate, checks the four resolved runtime/fixture jar hashes and
adds `qualification/src` to tests. It also selects test-only DuckDB JDBC 2.0.0.0
and Java Quack JDBC 0.8.0. Omitting the property restores released 0.12.0 dependencies
and the existing producer lane. This is not a release pin or supported plugin distribution.

October 24 adds `ducklake.experimental.enable-final-1.1-reads=false` by default.
The opt-in calls only the candidate's actual public flag setter. A small reflective
optional-API boundary preserves the released 0.12 compile/runtime lane: enabling it
there fails explicitly, never emulates admission. Remove that compatibility bridge
only after a separately approved released catalog floor includes the API.

Read-only SPI metadata operations own sessions, materialize deferred metadata
iterators under a limit, and reject mutations before delegation. Worker source
creation and lazy CDC file opens own sessions; shutdown closes each node's catalog
pool. SQL qualification uses two actual Trino workers with coordinator scheduling
disabled, still in the same single-host DistributedQueryRunner JVM. It is not a
multi-process/multi-host deployment or mixed-version negotiation test.

Consumer limits cap descriptors, physical row work, CDC candidates/units and deferred
metadata iteration. Inline ordinary reads count inside their owned view before
payload transfer; Parquet scans check actual footer row counts. **Legacy descriptor,
hierarchy and inline CDC list APIs still materialize before their consumer check.**
This is not a SQL-query memory bound, SQL-paged catalog enumeration, total cross-node
work budget or file lease. The catalog owner must supply the missing bounded APIs;
there is no whole-catalog preflight scan or direct-SQL bypass masquerading as a fix.

An opt-in missing-file-column initial default also rejects until the catalog offers
decoded, stable-ID/backend-qualified default projection. Do not reuse partition-text
parsing or replace an unqualified default with NULL. Ordinary columns and validated
inline defaults still work. Unconstructable physical fields/VARIANT payloads reject,
and decoded DECIMAL values outside Trino's declared precision cannot silently narrow.

No production/default-on rollout follows from these opt-in experiments. Remaining
gates include bounded catalog transfers, in-flight JDBC cancellation/network faults,
retention/lease coordination, broader route/type/legacy qualification and deployment
testing. See [the qualification report](R07-TRINO-CANDIDATE-QUALIFICATION.md).

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
