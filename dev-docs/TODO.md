# Active developer backlog

Only unresolved work belongs here. Current user capabilities/limits are in
[README](../README.md) and [REFERENCE](../REFERENCE.md). Detailed permanent finding IDs,
recorded fixes and reproduction conditions remain in the [live audit](REVIEW-2026-09-05.md);
this list neither closes nor replaces an unresolved audit finding.

The former read/write/special/risk lists are retained in [archive](archive/README.md).
Their open items are consolidated below, not marked complete by that move. Completed
port, feature and release history is not an instruction to redo shipped work.

## Correctness and physical support

- [ ] Fail loudly on present-but-unparseable partition/default values; do not project
  SQL NULL merely because conversion failed.
- [ ] Complete legacy/unsigned/type-edge qualification: unsigned Parquet annotations,
  files without IDs/name maps, unsupported encodings, and final-1.1 type/statistics
  behavior. Keep advertised support within actual reader/writer capability (RV-18).
- [ ] Reproduce the recorded `issue_852_schema_cache_growth` mirror divergence
  (`expected [true], got [false]`). Treat it as an unresolved recorded observation,
  not a fresh nightly verdict or a reason to add an unproven skip.
- [ ] Support or preflight-refuse every writable physical type before side effects;
  DDL/type-mapper acceptance is not Parquet-writer support.
- [ ] Honor `add_files.allow_missing` recursively for missing STRUCT children without
  bypassing type validation or changing existing NULL projection semantics.
- [ ] Keep procedure target-table/raw-file authorization and the other unresolved
  security/history findings in the live audit in scope; procedure EXECUTE alone is
  not a completed authorization contract.

## Read performance and metadata

- [ ] Keep Roaring/primitive delete membership through filtering instead of expanding
  dense vectors into copied boxed sets; profile concurrency and impose a real bound.
- [ ] Bound change-feed startup memory/time while retaining cross-window update pairing.
- [ ] Fetch all predicate-column file statistics in a bounded planning query instead
  of repeated active-file scans; unknown statistics must continue to keep files.
- [ ] Add extended metadata only for demonstrated use cases, not catalog-table parity.
- [ ] Extend view coverage across supported backend additions; PostgreSQL is covered.

## Type improvements

- [ ] Define a supported VARIANT/shredding representation and exactness/pruning policy;
  do not infer final-1.1 support from development layouts or mapper fallbacks.
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
