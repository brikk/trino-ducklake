# Active developer backlog

Only unresolved work belongs here. Current user capabilities/limits are in
[README](../README.md) and [REFERENCE](../REFERENCE.md). Detailed permanent finding IDs,
recorded fixes and reproduction conditions remain in the [live audit](REVIEW-2026-09-05.md);
this list neither closes nor replaces an unresolved audit finding.

The former read/write/special/risk lists are retained in [archive](archive/README.md).
Their open items are consolidated below, not marked complete by that move. Completed
port, feature and release history is not an instruction to redo shipped work.

## Correctness and physical support

- [x] Fail loudly on present-but-unparseable partition/default values; do not project
  SQL NULL merely because conversion failed.
- [ ] Complete legacy/unsigned/type-edge qualification: unsigned Parquet annotations,
  files without IDs/name maps, unsupported encodings, and final-1.1 type/statistics
  behavior. Keep advertised support within actual reader/writer capability (RV-18).
- [ ] Reproduce the recorded `issue_852_schema_cache_growth` mirror divergence
  (`expected [true], got [false]`). Treat it as an unresolved recorded observation,
  not a fresh nightly verdict or a reason to add an unproven skip.
- [ ] Read DuckLake `timetz` from Parquet `INT64 TIME(MICROS, isAdjustedToUTC=true)`. The
  connector maps it to `time(6) with time zone`, which Trino's Parquet reader cannot decode:
  `Unsupported Trino column type (time(6) with time zone) for Parquet column ([col1] optional
  int64 col1 (TIME(MICROS,true)))` at `add_files/add_files_timetz.test:28` and
  `add_files/add_files_type_check_timetz.test:34,46,51` (corpus mirror failures).
- [ ] Read Parquet `FLOAT16` into `real`: `add_files/add_files_type_check_reader_types.test:67`
  fails with `Unsupported Trino column type (real) for Parquet column ([f] optional
  fixed_len_byte_array(2) f (FLOAT16))`; `:74` then fails creating the page source.
- [ ] Map DuckLake `time_ns`; `DucklakeTypeConverter` refuses it with `NOT_SUPPORTED`
  (`add_files/add_files_type_check_reader_types.test:33` is an engine-skip on that gate).
- [ ] Support or preflight-refuse every writable physical type before side effects;
  DDL/type-mapper acceptance is not Parquet-writer support (INSERT into `TIMESTAMP(0)`,
  `TIMESTAMP(6) WITH TIME ZONE` and `UUID` columns fails in the Parquet writer).
- [ ] A row-group predicate on a column promoted `REAL -> DOUBLE` fails the scan of REAL-era
  files: `TupleDomainParquetPredicate` casts the file's Float statistics to Double
  (`ParquetCorruptionException: Corrupted statistics`). Skip the Parquet domain for columns read
  through a widening coercion. Reproduced by any `WHERE c = <double>` on such a table; possibly
  the cause of the `stats/float_to_double_retype_stats.test` mirror errors (unverified).
- [ ] Catalog 0.17.0 keeps FLOAT bounds verbatim across `REAL -> DOUBLE` (and zoneless bounds
  across `TIMESTAMP -> TIMESTAMPTZ`); upstream rewrites or clears them. Trino file pruning guards
  against it (see CRUTCHES "Catalog-Stats File Pruning Limits"), but by upstream's filter SQL
  (`TRY_CAST(max_value AS DOUBLE)`) DuckDB reading a Trino-promoted table would prune on the
  stale text (not reproduced). Report/fix in ducklake-catalog.
- [ ] Honor `add_files.allow_missing` recursively for missing STRUCT children without
  bypassing type validation or changing existing NULL projection semantics.
- [ ] Keep procedure target-table/raw-file authorization and the other unresolved
  security/history findings in the live audit in scope; procedure EXECUTE alone is
  not a completed authorization contract.

## Read performance and metadata

- [x] Keep Roaring/primitive delete membership through filtering instead of expanding
  dense vectors into copied boxed sets.
- [ ] Profile concurrent delete readers and impose a retained-state budget; memory
  reservations alone are not a query/node-global bound.
- [ ] Bound change-feed startup memory/time while retaining cross-window update pairing.
- [ ] Fetch all predicate-column file statistics in a bounded planning query instead
  of repeated active-file scans; unknown statistics must continue to keep files.
- [ ] Add extended metadata only for demonstrated use cases, not catalog-table parity.
- [ ] Extend view coverage across supported backend additions; PostgreSQL is covered.

## Type improvements

- [ ] Define a supported VARIANT/shredding representation and exactness/pruning policy;
  do not infer final-1.1 support from development layouts or mapper fallbacks. File-resident
  VARIANT (a Parquet VARIANT group) is refused with `NOT_SUPPORTED`; the column type is
  exposed as degraded VARCHAR.
- [ ] Evaluate native spatial mapping and bounding-box pruning; current geometry
  handling is binary, not a spatial engine.
- [ ] Retain existing INTERVAL/`uint128` representation boundaries unless a complete
  supported domain mapping is available; do not call unqualified file encodings lossless.

## Writes and maintenance

- [ ] Honor sort specifications on every supported output path or refuse paths that
  cannot; never turn per-file ordering into a global sorted-scan promise.
- [ ] Align writer-option scope/precedence (compression, row groups, rollover,
  inlining and deletion vectors) only where semantics are qualified.
- [ ] Explicitly test/enforce single-statement write behavior. Multi-statement
  transaction support needs a real isolation/publication contract, not a declaration.
- [ ] Reclaim failed output only when the catalog's uncertain/acknowledged commit
  outcome proves it is safe; do not delete possibly committed files.
- [ ] Adopt structured maintenance metrics where appropriate on 484 while preserving
  the 483 SPI boundary. Current procedures log outcomes.
- [ ] Qualify consistent missing-file cleanup versus real filesystem failures across
  backends; do not swallow errors under a generic not-found assumption.
- [ ] Catalog library: page the deletion queue's path references on `DucklakeCatalog` for
  DuckLake 1.0 catalogs, e.g. `listScheduledReferencePage(after: DucklakeScheduledPathCursor?,
  limit: Int): List<DucklakeScheduledPathCursor>`, keyset-ordered by (data_file_id, exact path
  bytes, path_is_relative), `limit` in 1..`DucklakeCleanupLimits.MAX_BATCH_SIZE`, honoring
  `readSession`, with no age filter. This is `DucklakeHierarchyReader.listScheduledReferences`,
  which 0.17.0 offers only to final-1.1 read sessions. With it, `remove_orphan_files` replaces
  `listAllReferencedFiles()` with `listRetainedFileReferencePage(DATA|DELETE)` plus this page
  under `ducklake.cleanup.max-reference-files` (RV-25g). Until then it materializes the whole
  referenced set; see CRUTCHES "Unbounded maintenance catalog reads".
- [ ] Catalog library: give `flush_inlined_data` a keyset-paged inlined change read, e.g.
  `getInlinedChangesPage(tableId, schemaVersion, endSnapshot, afterRowId: Long?, limit,
  columnIds)` returning the rows of `getInlinedChangesBetween(…, 0, endSnapshot, …)` in row-id
  order, so the procedure streams rows into the Parquet writer instead of loading a schema
  version at once. An exact count of those rows (ended rows included) would at least let the
  up-front budget cover them; `countInlinedRows` counts only live rows.
- [ ] Audit only remaining metadata namespaces: Trino view tags/dialect, snapshot
  `changes_made`, delete formats and connector options. Do not invent conflict vocabulary.

## Backend qualification

- [ ] Document/qualify native and direct-Java Quack deployment, authentication and
  exact driver/server versions for this connector.
- [ ] Add Quack cross-engine/concurrent-writer coverage and a bounded CI lane once
  the transport/authentication contract is stable. A fixture is not that qualification.
- [ ] Re-probe DuckDB/MySQL interoperability on assigned dependency updates. If a
  current crash/protocol failure reproduces, retain a minimal reproducer for upstream.

## Catalog 1.1 and rollout

- [ ] Keep development-format writes and all final-1.1 connector writes disabled.
- [ ] Keep [candidate qualification](CATALOG-CANDIDATE.md) separate from the normal
  catalog 0.17 / DuckLake 1.0 lane. Library adoption does not authorize final-1.1 promotion.
- [ ] Complete the remaining bounded transfers/accounting, cancellation/network,
  legacy/type/route, retention and deployment gates before broad read enablement.
- [ ] Qualify schema migration, hierarchy, exactness, row-group metadata, ABI/wire,
  rollback and mixed versions before any 1.1 write admission. A changed version string
  or successfully parsed signature is not proof of write compatibility.

## Architecture and delivery hygiene

- [ ] Repair opt-in qualification fixtures' explicit format selection: the four legacy
  `TestPublishedNestedReads` cases select a 1.1 client for 1.0 lakes; the two direct-reader
  statistics cases in `TestR07ConsumerTrial` enable 1.1 reads after constructing a catalog
  config still locked to 1.0. These six failures reproduce on `bca49a1`, before WP4.
  Match each fixture's stored format without weakening catalog admission or adding skips.
- [ ] Restore useful per-directory corpus qualification without weakening gates:
  `merge` has no accepted alias-qualified lake reads and fails minimum mirror coverage;
  `partitioning` has implicit newer-format fixtures rejected by the fixed DuckLake 1.0
  oracle (epoch transforms and NULL type). Both failures reproduce on `d92f048`.
- [ ] Add shared replay support for `require-env`: the pinned native metadata-logging
  fixture is currently a visible unsupported-directive file-skip, even with
  `DUCKLAKE_CI=1`. Keep SQL/goldens intact and require real Trino mirrors when enabled.
- [ ] Decompose `DucklakeMetadata`/`DucklakePageSourceProvider` by concern while
  preserving SPI and handle shapes: metadata tables, CDC, inline/Parquet sources,
  and statistics are useful boundaries.
- [ ] Apply Kotlin idioms only where wire/SPI/Jackson/Airlift contracts permit.
  Preserve required `@JvmRecord` DTO shapes and interop annotations.
- [ ] Validate release tag/version agreement rather than relying solely on manual
  checks; coordinate with the archive/assembly contract (RV-27, RV-D07).
- [ ] Reproduce the local Compose workbench from a fresh checkout and resolve its
  sibling Quack build/extra-catalog dependencies before describing it as self-contained
  (RV-D08). Current limitations are in [LOCAL-STACK.md](LOCAL-STACK.md).

## Upstream watches — investigate before changing contracts

- [ ] Watch Quack's server-side commit trajectory; do not maintain two guessed
  conflict models if direct-SQL writers become second-class.
- [ ] Watch for an actual cross-engine lease/lock contract for expiration versus
  long-running reads. Retention alone is not that contract.
- [ ] Recheck final-1.1 VARIANT, nested identity, exactness and row-group metadata
  against exact artifacts. Preserve read-only/admission guards meanwhile.
- [ ] Keep commit-author/message/extra-info writes a non-goal unless the upstream
  snapshot schema defines compatible fields; do not invent a storage extension.

Use the [upstream research procedure](RESEARCH-upstreams.md) to update these watches.
