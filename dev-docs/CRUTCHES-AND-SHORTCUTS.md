# Crutches and shortcuts

Every path that is not production-clean: what it is, what it costs, the real fix, when it
can go, and where it lives. Open work without an accepted shortcut is in [TODO.md](TODO.md).

## Path comparison fails closed

- **Shortcut:** orphan and scheduled-file cleanup compare paths only within verified
  namespaces: native-local `file:/`, `file:///`, `local:///` under the configured root
  (`fs.local.enabled`, legacy `fs.native-local.enabled`), and native S3 `s3`/`s3a`/`s3n` by exact
  bucket/key text (`fs.s3.enabled`, legacy `fs.native-s3.enabled`). Other schemes compare
  canonical paths within one scheme/authority (Azure includes the container). Any mixture
  with an unresolved namespace (for example bare Hadoop paths next to file URIs), dot segments,
  repeated separators, backslashes or ambiguous local authorities abort the call before deletion,
  dry runs included. Stored paths and deletion targets are never rewritten.
- **Cost:** legitimate mixed-namespace or noncanonical layouts cannot be cleaned by these
  procedures. Symlink/mount aliases, case-folding filesystems and alternate endpoints are not
  detected.
- **Real fix:** an authoritative, root-aware same-object API from the configured Trino
  filesystem.
- **Removal criterion:** that API exists and real backend tests cover each relaxed guard.
  Lowercasing, an independent Hadoop configuration or filesystem-wrapper reflection do not count.
- **Owner:** `DucklakeFilePathIdentity.kt`.

## Bounded scheduled-file cleanup

- **Shortcut:** `cleanup_old_files` selects at most one batch (default 256, maximum 1024)
  and scans retained data/delete references in pages under a per-call budget (default
  1,000,000, maximum 10,000,000), failing before any deletion or queue change when a bound is
  exceeded. An exact surviving owner keeps the object and cancels only the selected queue
  request. Prefix-only directory overlaps are deferred with a warning. A legacy directory
  request can contain many objects; the batch cap does not count recursive deletions.
- **Cost:** failed or deferred oldest requests stay queued and can occupy later batches; there
  is no retry ledger. Simultaneous import and cleanup, independent cleaners and path
  replacement are not coordinated.
- **Real fix:** a cross-engine ownership/lease protocol (TODO "Upstream waits").
- **Removal criterion:** that protocol exists and is shared with DuckDB's cleaner.
- **Owner:** `DucklakeCleanupOldFilesProcedure.kt`; limits in `DucklakeConfig.kt`; design in
  [DESIGN-shared-file-ownership.md](DESIGN-shared-file-ownership.md).

## Unbounded maintenance catalog reads

ducklake-catalog 0.17.0 has keyset pages for data/delete file references but none for the
deletion queue (DuckLake 1.0) and none for inlined rows.

- **Shortcut:** `remove_orphan_files` loads every referenced path through
  `listAllReferencedFiles()` and holds it as a set. Its own work is bounded: directory ownership
  is a set probe per path depth, and one call keeps at most `MAX_ORPHANS_PER_CALL` (10,000)
  orphans, deletes them and asks for a repeat. `flush_inlined_data` loads each inlined schema
  version with `getInlinedChangesBetween`, after refusing (`GENERIC_USER_ERROR`, nothing written)
  a table whose live inlined rows at the read snapshot exceed `ducklake.read.max-physical-rows`.
- **Cost:** coordinator memory grows with the catalog's referenced-file count, and with
  inlined rows that were deleted but never flushed (loaded but not counted). Inlined data is not
  bounded by construction: every DuckDB insert under `data_inlining_row_limit` adds rows until
  someone flushes.
- **Real fix:** the paged scheduled-reference and paged inlined-change reads in TODO
  "Catalog-library asks".
- **Removal criterion:** those APIs ship; then `remove_orphan_files` pages all three reference
  kinds under `ducklake.cleanup.max-reference-files`, as `cleanup_old_files` does, and flush
  streams rows into the writer.
- **Owner:** `DucklakeRemoveOrphanFilesProcedure.kt`, `DucklakeFlushInlinedDataProcedure.kt`.

## Non-atomic CTAS and per-call write transactions

- **Shortcut:** catalog 0.17.0 publishes each write call in its own snapshot; its write
  transaction is internal and `readSession` refuses writes. `DucklakeTransactionManager.commit`
  only releases the transaction and `DucklakeConnector.isSingleStatementWritesOnly` is `true`, so
  Trino rejects writes inside `START TRANSACTION`. INSERT, UPDATE/DELETE/MERGE and DDL make one
  catalog call each and are atomic. CTAS makes two: `beginCreateTable` calls `createTable`
  (the sinks need table id, column ids and data path), `finishCreateTable` calls
  `commitInsert`. On rollback `DucklakeWriteMetadata.dropUnpublishedCreatedTables` drops the
  created table only if the data commit was never attempted or provably rolled back (any
  `DucklakeException` other than `DucklakeCommitOutcomeUnknownException` or
  `DucklakePostCommitException`), the name still resolves to the same table id, and the table
  has no live data files or inlined rows; otherwise it keeps the table and logs a warning.
- **Cost:** other sessions can see the empty table until the query finishes or Trino's
  asynchronous rollback runs; if the coordinator dies first, the table stays. The checks and
  `dropTable(schema, name)` are separate calls, so a concurrent drop/re-create or insert between
  them is not detected. With an unknown commit outcome the table and output are kept for the
  operator. Files from sinks that finished before the failure are unreferenced:
  `remove_orphan_files` reclaims them under the data root, but not under an absolute table
  `location` outside it (the table-scoped form cannot resolve a dropped table); delete those by
  hand.
- **Real fix:** a create-table-with-data call or a public multi-call write transaction.
- **Removal criterion:** either API ships. A conditional `dropTable(tableId,
  expectedSnapshotId)` alone closes the check-then-drop race.
- **Owner:** `DucklakeWriteMetadata.kt`, `DucklakeTransactionManager.kt`.

## Uncertain commit outcomes are not reconciled

- **Shortcut:** `DucklakeCommitOutcomeUnknownException` and post-commit failures keep their
  catalog cause and map to a non-conflict Trino error. Nothing retries or deletes output.
- **Cost:** operators must reconcile registrations on a fresh connection before retrying or
  cleaning up; the attempted snapshot id alone is not proof. Output of failed attempts is left
  for `remove_orphan_files`.
- **Real fix:** a catalog call that reports whether the attempt published (TODO "Catalog-library
  asks"), then RV-24a output tracking.
- **Removal criterion:** that call exists and failure-injection tests cover INSERT, CTAS,
  UPDATE, DELETE and MERGE.
- **Owner:** `DucklakeCatalogExceptions.kt`, `DucklakeWriteMetadata.kt`.

## Catalog-stats file pruning is narrower than the statistics

- **Shortcut:** `DucklakeSplitManager.pruneDataFiles` prunes from the table handle's unenforced
  predicate through `findDataFileIdsInRange`, with bounds rendered by
  `DucklakeStatsPredicateNormalizer`. Only columns whose Trino type is the connector's own mapping
  of the stored type prune (integers, `float32`/`float64`, `decimal(p,s)`, `date`,
  `timestamp[_s|_ms|_ns]`, `timestamptz`, `varchar`, `boolean`); unsigned/hugeint promotions,
  VARCHAR degradations, UUID, binary, time and nested columns keep every file. A domain that
  admits NULL never prunes. Exclusive bounds are sent as inclusive. A file whose
  `begin_snapshot` precedes the column's current version is never excluded by that column,
  because catalog 0.17.0 keeps bounds verbatim across promotions (REAL-era `0.1` reads as a
  smaller DOUBLE than the widened value). The opt-in 1.1 lane skips catalog-stats pruning on 1.1
  catalogs.
- **Cost:** extra files are scanned, especially after any column-version change (type change,
  rename).
- **Real fix:** the catalog rewrites or clears bounds on promotion, exposes per-file statistics
  type provenance, and its range API accepts exclusive endpoints and NULL-admitting predicates.
- **Removal criterion:** those catalog changes ship (TODO "Catalog-library asks").
- **Owner:** `DucklakeSplitManager.kt`, `DucklakeStatsPredicateNormalizer.kt`.

## Transformed TIMESTAMPTZ partitions never exclude files

- **Shortcut:** temporal transforms on TIMESTAMPTZ partitions never exclude non-NULL
  candidates. Legacy native calendar transforms used the writer's time zone, and migrated or
  mixed catalogs keep those values; no catalog version, query time zone or
  `temporal-partition-encoding-read-leniency` setting proves a file's values are UTC. Exact
  residual predicates stay in Trino. DATE, zoneless TIMESTAMP and identity pruning are unchanged.
- **Cost:** extra files may be scanned.
- **Real fix:** a per-file transform/writer-zone attestation in the catalog.
- **Removal criterion:** that attestation exists. A version check or a future native upgrade
  is not enough.
- **Owner:** `DucklakeTemporalPartitionMatcher.kt`.

## Row-identity reads are whole-file

- **Shortcut:** mutation locators are `ROW(data_file_id, physical_position)`. Lineage is read
  into a per-file `LongArray`, and when physical positions are needed splits cover whole files
  with predicate/row-group pruning disabled. Logical `$row_id` prefers embedded lineage, then a
  present `row_id_start` plus position; missing or malformed identity fails. The legacy
  global-ID delete-file reader needs a real catalog start for rebasing.
- **Cost:** memory per file for lineage, and no row-group pruning on scans that need positions.
- **Real fix:** a streaming lineage/physical-position read path.
- **Removal criterion:** that path exists. Never enable pruning or row-group splits while a
  zero-seeded cumulative position counter remains.
- **Owner:** `DucklakeParquetPageSourceFactory.kt`, `DucklakeDeleteFileReader.kt`.

## Retained-memory reporting is an estimate

- **Shortcut:** on Trino 484 the merge sink (retained delete positions, lineage cache, insert
  sinks) and the scan provider's shared column-tree cache report retained-state estimates to the
  SPI memory contexts. Delete filters report Roaring64 storage; change feeds report query-local
  footers, identity arrays and bitmaps and release them on close. Trino 484 forwards each page
  source's `getMemoryUsage()` through `MemoryUsageReportingPageSource`. Maintenance CALLs get no
  scan context; Trino 483 has neither context. Native-row sources count shared values more than
  once and measure inline JSON with a temporary block.
- **Cost:** temporary allocations while loading deletes/lineage or serializing output are not
  counted, and there is no query- or node-wide bound. Shared lineage may be counted twice.
- **Real fix:** streaming readers and a retained-state budget (TODO "Bounds and performance").
- **Removal criterion:** allocation-heavy paths are streamed or budgeted and profiled.
- **Owner:** `trino-484/src/.../DucklakeTrinoApi.kt`, `DucklakeMergeSink.kt`,
  `DucklakeColumnTreeCache.kt`, `DucklakeLongSet.kt`, `ChangeFeedPageSource.kt`.

## Legacy JDBC timestamp offsets

- **Shortcut:** `DucklakeTimestampValues` keeps an explicit `jdbcOffsetAsInstant` input. The
  legacy 1.0 JDBC inline converter normalizes offset-bearing zoneless timestamp text to UTC;
  DuckDB and catalog 0.17.0 (`InlinedTemporalValues.localTimestampText`) drop the offset and keep
  the wall clock, as identity-partition and decoded catalog values do here.
- **Cost:** the two paths can disagree on such text. The JDBC compatibility tests require the
  current behaviour.
- **Real fix:** move the 1.0 inline path to decoded catalog values (RV-G08).
- **Removal criterion:** a separately approved change of the legacy JDBC contract, with
  driver/backend qualification and an intentional test change.
- **Owner:** `DucklakeTimestampValues.kt`, `DucklakeInlinedValueConverter.kt`.

## Opt-in 1.1 lane: per-view budgets only

- **Shortcut:** the candidate adapter drains SQL-keyset pages for files, schemas, columns,
  partitions, name maps, inline payloads, deletions and history under per-owned-view totals
  (1,024 page calls, 16,384 nodes, 20,000 paths, 16 MiB text allowance, 131,072 work units,
  64 hierarchy levels, configured file/row/CDC-event limits; page size 128, range 1-1024).
  Owned sessions have a 30-second deadline (1-3600); cancellation snapshots registered controls
  under a lock and calls drivers outside it. View, tag, comment and setting reads still use the
  older unbounded delegates. Missing-file defaults reload target metadata per absent field.
  Two-worker qualification runs in one JVM.
- **Cost:** not a heap, database-time, query-wide or fleet-wide bound; internal schema/default
  accounting is incomplete. Worker task-close during blocking construction, pool
  acquisition/setup/network stalls, other drivers' cancellation and retention leases are not
  qualified; a blocking driver delays the cancelling caller.
- **Real fix:** a catalog accounting hook, bounded view/tag/comment/setting APIs, and the gates
  in TODO "Upstream waits".
- **Removal criterion:** those gates close; until then the lane stays opt-in and read-only.
- **Owner:** `candidate/src/.../DucklakeCandidateAdapter.kt`, `DucklakeReadStrategy.kt`;
  contract in [CATALOG-CANDIDATE.md](CATALOG-CANDIDATE.md).

## Opt-in qualification suite is red

- **Shortcut:** none in code; recorded so a red `-Pr07CatalogCandidate` run is not mistaken for a
  regression. 40 qualification tests fail on the current fixtures (measured in WP9), each a
  catalog format-admission refusal caused by the fixture's format selection (TODO "Test, CI and
  release hygiene").
- **Cost:** the opt-in lane has no passing qualification run.
- **Real fix:** each fixture selects the format its lake is stored in.
- **Removal criterion:** the suite passes without weakening admission or adding skips.
- **Owner:** `qualification/src/`.

## Corpus replay runs DuckLake 1.0 only

- **Shortcut:** the normal corpus suite scopes mirror and execution to 1.0. Files that
  explicitly use 1.1 halt after their stored format is inspected: earlier results stand, the rest
  is not executed, and halted files never count as passed. Replay uses
  `ReplaySetupFormat.DUCKLAKE_1_0` without rewriting corpus SQL or goldens.
- **Cost:** no 1.1 corpus coverage.
- **Real fix:** a qualified 1.1 lane.
- **Removal criterion:** 1.1 reads are admitted in the normal build.
- **Owner:** `test/src/.../TestTrinoCorpusReplay.kt`, `TrinoReplayEngine.kt`.

## Corpus engine-error classification reads some message text

- **Shortcut:** `TrinoReplayEngine.classifyEngineError` turns a mirror error into an engine-skip
  only for connector-thrown `NOT_SUPPORTED`, `FUNCTION_NOT_FOUND` for an unknown function,
  `TYPE_MISMATCH` whose failing operator has a proven quoted VARCHAR literal operand, and
  `TABLE_NOT_FOUND` for a name the lake
  never held as a table (checked across all snapshots in the replay PostgreSQL catalog). Within
  those branches the operator types and relation name are read from the message text. Literal
  identity, side and length are checked in the executed SQL's Trino AST at the structured error
  location; missing locations or unrecognized shapes fail rather than skip.
- **Cost:** Trino's operator diagnostics still require message parsing. Any new production
  `NOT_SUPPORTED` that is not a deliberate capability gate silently becomes a corpus skip.
- **Real fix:** the replay library reports which relations a query reads from the lake, and
  Trino reports unresolved names structurally.
- **Removal criterion:** both exist.
- **Owner:** `test/src/.../TrinoReplayEngine.kt`.

## Test-only MAP access rewriting

- **Shortcut:** DuckDB string-key MAP access such as `s.i` is ROW access in Trino.
  `TrinoMapAccessRewriter` types the query with brikk-sql using `SELECT * ... LIMIT 0` schema
  probes and rewrites proven string-key MAP accesses to `element_at`. Non-text keys and queries
  that see conflicting historical schemas of one table are not rewritten; their SQL runs
  unchanged.
- **Cost:** test-only adapter code to maintain.
- **Real fix:** schema-aware transpilation of MAP access in brikk-sql.
- **Removal criterion:** a released transpiler handles these accesses and their value types.
  A global text replacement of dotted names is not a substitute.
- **Owner:** `test/src/.../TrinoMapAccessRewriter.kt`.

## Test-local geometry fixtures

- **Shortcut:** the three upstream nested-geometry corpus files use bare table names, which the
  adapter's lake-alias gate excludes, so they pass on goldens alone. `TestTrinoGeometryReplay`
  runs test-local copies that qualify their lake SELECTs and asserts Trino returned rows.
- **Cost:** the copies can drift from upstream.
- **Real fix:** the replay library reports oracle namespace/temporary-table binding.
- **Removal criterion:** that information exists. Qualifying arbitrary bare names is not a fix.
- **Owner:** `test/src/.../TestTrinoGeometryReplay.kt`.

## Test-only PostgreSQL metadata schema binding

- **Shortcut:** PostgreSQL replay binds only the default schema qualifier of known rewritten
  metadata catalogs, through the shared oracle-SQL hook and the brikk-sql AST. Explicit metadata
  schemas, lake schemas, literals and goldens are unchanged.
- **Cost:** test-only; it does not add DuckDB-view dialect support to the connector.
- **Real fix:** the replay library binds backend metadata namespaces itself.
- **Removal criterion:** it does.
- **Owner:** `test/src/.../TrinoReplayEngine.kt`.
