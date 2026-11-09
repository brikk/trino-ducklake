# Active developer backlog

Only unresolved work belongs here. `RV-` IDs refer to findings in the
[archived 2026-09-05 review](archive/reviews/REVIEW-2026-09-05.md); this list, not the review,
records which are still open. Accepted restrictions and their removal criteria are in
[CRUTCHES-AND-SHORTCUTS.md](CRUTCHES-AND-SHORTCUTS.md); user-visible limits are in
[README](../README.md) and [REFERENCE](../REFERENCE.md).

## Connector defects

Wrong values, unsafe behaviour, or a supported operation that fails.

### Security and maintenance safety

- [ ] RV-02: procedures check only EXECUTE. Inject `ConnectorAccessControl` and check the
  target table's write/admin permission before catalog or filesystem work
  (`DucklakeProcedureSupport.resolveTable`); test a principal with EXECUTE but no INSERT.
- [ ] RV-X09: procedures skip the encrypted-catalog refusal that table reads have, and
  `rewrite_data_files` reads data before the catalog's write gate. Refuse before reading data.
- [ ] RV-14e: change-feed functions skip the same encrypted-catalog refusal
  (`AbstractChangeFeedTableFunction.analyze`).
- [ ] RV-26a: `cleanup_old_files` returns success after a delete `IOException` (the entry stays
  queued). Treat not-found as reclaimed; report other failures to the caller.
- [ ] RV-26b: `remove_orphan_files` treats any listing `IOException` as "no orphans" and still
  deletes the partial list collected so far. Only a missing root means empty.

### Values, history and transactions

- [ ] RV-07: `rewrite_data_files` builds column handles without `initialDefault`, so files from
  before `ADD COLUMN ... DEFAULT` are rewritten with NULL. Pass the default; add a cross-engine
  test with files from before and after the ADD.
- [ ] RV-06: partial `rewrite_data_files` writes only the current schema; values of columns
  (or nested fields) dropped since a retained snapshot are lost. Refuse partial rewrite then,
  or keep the historical physical schema.
- [ ] RV-15b: `flush_inlined_data` tags every existing Puffin position with the file's earliest
  begin snapshot, so later deletions become visible too early in time travel. Keep each
  position's own deletion snapshot.
- [ ] RV-09a: SERIALIZABLE is advertised, but the 1.0 path resolves the current snapshot on each
  call (`DucklakeSnapshotResolver`). Pin the default snapshot per transaction or stop claiming it.
- [ ] RV-14a-d: change-feed splits carry empty name mappings and partition constants
  (`DucklakeChangeFeedPlanner`), feed handles lack initial defaults, and 1.0 inline feed rows
  skip schema-era/nested mapping (`DucklakeInlinedRowCodec.convertRows`). Imported/reordered
  files, omitted Hive partition columns, old-schema defaults and inlined nested renames can
  return wrong values. Reuse the normal scan's per-file metadata.
- [ ] RV-16a-e: `add_files` stores raw Hive path text (no unescaping), registers
  `__HIVE_DEFAULT_PARTITION__` as text, skips empty `key=` values and does not validate typed
  values; split planning drops explicit NULL partition values, which then fall back to
  `initialDefault`. Use one decode rule for registration, projection and pruning, with distinct
  absent / NULL / empty-string states.
- [ ] RV-13a: on 1.0 reads a name-map miss for a nested field falls back to the current name
  (`DucklakeParquetTypeUtils`); after dropping and re-adding `s.x` the new field can read the
  old value. A mapped miss must mean absent.
- [ ] RV-13b: on 1.0 reads a struct whose current children are all physically absent becomes
  NULL instead of a non-NULL row of NULL children. Use the parent's definition levels.
- [ ] RV-17a: current column defaults are not exposed through `ColumnMetadata`, so Trino INSERT
  writes NULL for an omitted column that has a DuckDB `DEFAULT`. Expose supported current
  defaults (distinct from initial defaults).
- [ ] RV-17b: nested inline initial defaults that fail to parse become NULL
  (`InlinedNestedFieldMapper`, `runCatching {}.getOrNull()`); UUID/BLOB/TIME defaults are
  refused rather than decoded. Fail loudly; decode those types.

### Reads, types and statistics

- [ ] A row-group predicate on a column promoted `REAL -> DOUBLE` fails the scan of REAL-era
  files: `TupleDomainParquetPredicate` casts the file's Float statistics to Double
  (`ParquetCorruptionException: Corrupted statistics`). Skip the Parquet domain for columns read
  through a widening coercion. Reproduced by any `WHERE c = <double>` on such a table; possibly
  the cause of the `stats/float_to_double_retype_stats.test` mirror errors (unverified).
- [ ] RV-20a: written DOUBLE statistics drop `-Infinity` (row groups `[-Infinity,-2]` and
  `[-1,-0.5]` give min `-1.0`): `DucklakeStatsExtractor` uses the catalog's
  `DucklakeStatTypes.compare`, which falls back to code-point order for non-finite values. Now
  that catalog-stats pruning runs in real queries this can exclude matching files. Compare typed
  values; emit unknown bounds for non-finite inputs.
- [ ] RV-20b: imported DECIMAL statistics are decoded with the target scale, not the file's
  (`DECIMAL(10,2)` unscaled `12345` imported as `DECIMAL(12,4)` gives `1.2345`).
- [ ] RV-20c: BCE and five-digit-year temporal bounds compare lexically. Add a test; compare
  chronologically.
- [ ] RV-21a-c: bucket partitions hash microseconds for every timestamp precision, raw UUID
  bytes, and the widened signed value of unsigned types; DuckLake hashes the stored
  representation (`ducklake_murmur3.cpp`). Match per storage type and verify against DuckDB.
- [ ] RV-22b-e: `DucklakeDeleteFileReader` skips NULL origin markers (rows stay visible), skips
  NULL positions/snapshots outside the change feed, detects legacy schemas by the first integer
  column, and finds the snapshot marker by name only (R-N6). Reject malformed markers, recognize
  `pos` / legacy `row_id` explicitly, and resolve markers by field ID.
- [ ] RV-18c/d: DuckDB-written UINT32/UINT64 Parquet has no unsigned decoding (`uint32 -> BIGINT`;
  `uint64 -> DECIMAL(20,0)` expects a decimal annotation). Add fixtures above the signed limit;
  decode or refuse. Corpus: `compaction/merge_adjacent_target_size_heuristics.test:64`
  `SELECT count(*), count(DISTINCT i) FROM t` (oracle `1000 1000`) fails with `Unsupported Trino
  column type (decimal(20,0)) for Parquet column ([i] required int64 i (INTEGER(64,false)))`.
- [ ] RV-18f: Parquet INTERVAL bytes reach VARCHAR undecoded. Decode months/days/millis or refuse.
  Silently wrong in the corpus: `add_files/add_files_legacy_parquet_types.test:86`
  `SELECT iv FROM intervals ORDER BY iv` gives oracle `[1 day], [3 months]`, Trino two
  12-byte zero strings.
- [ ] RV-G08: 1.0 inline reads and `flush_inlined_data` still parse raw JDBC values, which is
  backend-sensitive (typed CAST strings); only 1.1 catalogs use the decoded API
  (`DucklakeInlinedRowCodec`). Move both to decoded catalog values.
- [ ] RV-18i: 1.0 inline TIME/TIMETZ values fall back to a UTF-8 slice
  (`DucklakeInlinedValueConverter`). Convert to Trino's native values.
- [ ] RV-18j: ALTER allows integer `-> REAL/DOUBLE`, but old integer files have no reader
  coercion. Add one or disallow the promotion.
- [ ] Files written before a DuckDB `ALTER ... TYPE` to HUGEINT or TIMESTAMP cannot be read:
  trino-parquet has no reader from the old physical type to the promoted Trino type and the
  scan fails with `Unsupported Trino column type (decimal(38,0)) for Parquet column ([c] optional
  int32 c (INTEGER(32,true)))` (also `int64 INTEGER(64,true)` and `double`), or `(timestamp(6))`
  for `int32 c (DATE)`. Corpus: `partitioning/bucket_pruning_type_promotion.test:34` `SELECT k
  FROM h WHERE c = 5` after `ALTER TABLE h ALTER c TYPE HUGEINT` (oracle `1`), and `:39, :48,
  :54, :114, :141, :172, :232`; `partitioning/bucket_pruning_promoted_column.test:68, :74, :79,
  :84, :90` (`SELECT c, k FROM d` after DATE -> TIMESTAMP, oracle `2020-01-05 00:00:00 1`).
  Add reader coercions for DuckDB's allowed promotions, as for RV-18j.
- [ ] RV-18k: `TIMESTAMP -> TIMESTAMPTZ` ALTER accepts any precision; `TIMESTAMP(9)` is truncated
  to microseconds. Allow only lossless precisions.
- [ ] RV-19a: unsigned range checks cover top-level columns only; `STRUCT(u UTINYINT)` exposed as
  `ROW(u SMALLINT)` accepts 300. Recurse into ROW/ARRAY/MAP.
- [ ] RV-19b: `add_files` type compatibility compares widened Trino types, losing the unsigned
  source constraint.
- [ ] RV-20d: `add_files` checks decimal precision and scale separately; also require enough
  integer digits in the target.
- [ ] RV-G06: `add_files.allow_missing => false` is enforced for top-level columns only; enforce
  it recursively for missing STRUCT children (including inside arrays and maps) without
  bypassing type validation or changing existing NULL projection.
- [ ] RV-G24: JSON-annotated Parquet columns in `add_files` fall through to VARBINARY. Map them;
  add a physical JSON fixture.
- [ ] RV-G07: listing columns throws on catalog types the mapper does not know (`timestamp_us`,
  `time_ns`, `unknown`), so one table can break schema-wide enumeration. Define a policy for
  unsupported columns.
- [ ] RV-G20: delete-file format is chosen by file suffix (`DucklakeReadSupport`). Carry the
  stored format into the split; test a valid file with a nonstandard suffix.
- [ ] Reproduce the recorded `issue_852_schema_cache_growth` mirror divergence
  (`expected [true], got [false]`). It is a recorded observation, not a fresh verdict.

### Writes and output lifecycle

- [ ] RV-24a: commit rejection does not delete finished worker outputs; MERGE delete files and
  `rewrite_data_files` / `flush_inlined_data` outputs are abandoned on failure. Track outputs
  until publication and delete known-unpublished files; never delete output whose commit outcome
  is unknown or acknowledged.
- [ ] RV-24b: streams are opened before the writer constructor takes ownership
  (`DucklakePageSink`, `DucklakeMergeSink`), and failed MERGE delete/Puffin files are closed,
  not deleted.
- [ ] RV-18b: MERGE creates its insert sink eagerly, so a pure DELETE fails on a table with a
  column the Parquet writer cannot write. Create the insert sink on the first inserted row.
- [ ] RV-G12: MERGE writes untagged two-column Parquet deletes or bare Puffin blobs. Write
  snapshot-tagged delete history.
- [ ] RV-G13: MERGE delete files lack the reserved Parquet field IDs. Add them and assert the
  physical schema.
- [ ] RV-G10: writer schema annotation keeps an unmatched logical field without a field ID
  (`DucklakeParquetSchemaBuilder`). Reject it; allow only recognized structural wrappers.
- [ ] RV-G04: `ADD COLUMN ... FIRST/AFTER` silently appends. Reject or support it.
- [ ] RV-G05: table/column comments declared in CREATE TABLE or ADD COLUMN are dropped. Persist
  them in the same DDL publication.
- [ ] RV-G25: `ducklake.temporal-partition-encoding=epoch` still applies to new writes. Keep
  epoch for reading legacy catalogs only.

### Risks to reproduce before fixing

- [ ] trino-parquet lowercases file schema names, so a Parquet file with two columns that differ
  only in case (`A`, `a`) resolves both catalog names to one child. Reproduce with a DuckDB
  `add_files` of such a file and refuse ambiguous names instead of binding one.
- [ ] RV-X01: change feeds check SELECT only; row filters and column masks are neither enforced
  nor refused. Reproduce with a restricted user, then refuse or enforce.
- [ ] RV-X02: worker reads open their own sessions and `rewrite_data_files` selects candidates
  outside its split-planning session; expire/flush/rewrite in between can change representation.
  Reproduce with barriers.
- [ ] RV-X03: the orphan sweep snapshots known paths, then lists; `add_files` can register an
  old object in between. Test concurrent registration; needs an ownership protocol (see RV-04
  in CRUTCHES).
- [ ] RV-X05: the temporal matcher drops sub-microsecond detail of a p9 exclusive bound
  (`DucklakeTemporalPartitionMatcher.extractEpochMicros`). Add the helper test before enforcing
  p9 predicates.
- [ ] RV-X06: `ChangeFeedPageSource` treats a null page as end of input and does not forward
  `isBlocked`. Test a delegate that yields null and then data.
- [ ] RV-X07: `retryMode` and `pageSinkId` are ignored. Run task-retry fault injection, or
  refuse retry modes other than `NO_RETRIES`.
- [ ] RV-X08: `rewrite_data_files` and `flush_inlined_data` materialize on the coordinator.
  Verify on shared S3/HDFS and on workers with separate local filesystems.
- [ ] RV-X10: `cleanup_old_files` decides file versus directory from `directoryExists`. Test an
  object key that also has child keys; never delete a prefix because one file was scheduled.
- [ ] RV-X11: legacy positional (no field ID, no name map) imports need producer fixtures before
  any fallback is added.
- [ ] RV-X12: compare literal backslashes in flat versus nested 1.0 inline lists against real
  producer output (`NestedTextParser`).
- [ ] RV-X13: settle float and BCE partition text with producer fixtures
  (`DucklakePartitionValueParser`).
- [ ] RV-X14: `ducklake.catalog.database-url` is not `@ConfigSecuritySensitive`. Test synthetic
  credentials embedded in the URL against configuration reports and logs.

## Connector gaps

Unsupported capability that fails loudly, missing work bounds, or missing coverage.

### Types

- [ ] RV-18a: writers for `UUID`, `TIME(6)`, `TIME(6) WITH TIME ZONE`, `TIMESTAMP(0)` and
  `TIMESTAMP(6) WITH TIME ZONE`. DDL accepts them; INSERT into `TIMESTAMP(0)`,
  `TIMESTAMP(6) WITH TIME ZONE` and `UUID` columns fails in Trino's Parquet writer. Implement, or
  refuse before any DDL or write side effect.
- [ ] RV-18g: read DuckLake `timetz` from Parquet `INT64 TIME(MICROS, isAdjustedToUTC=true)`.
  The connector maps it to `time(6) with time zone`, which Trino's Parquet reader cannot decode:
  `Unsupported Trino column type (time(6) with time zone) for Parquet column ([col1] optional
  int64 col1 (TIME(MICROS,true)))` at `add_files/add_files_timetz.test:28` and
  `add_files/add_files_type_check_timetz.test:34,46,51` (corpus mirror failures). Include
  nested columns.
- [ ] Read Parquet `FLOAT16` into `real`: `add_files/add_files_type_check_reader_types.test:67`
  fails with `Unsupported Trino column type (real) for Parquet column ([f] optional
  fixed_len_byte_array(2) f (FLOAT16))`; `:74` then fails creating the page source.
- [ ] Map DuckLake `time_ns`; `DucklakeTypeConverter` refuses it with `NOT_SUPPORTED`
  (`add_files/add_files_type_check_reader_types.test:33` is an engine-skip on that gate).
- [ ] RV-18e: force HUGEINT/UHUGEINT into Parquet in tests; their DOUBLE-backed files do not
  match the advertised decimal/text mapping. Document the limits or refuse.
- [ ] T-N3: make physical read/write/promotion support an executable type matrix, so README and
  REFERENCE type claims can be checked.
- [ ] Define a supported VARIANT/shredding representation and pruning policy. File-resident
  VARIANT is refused with `NOT_SUPPORTED`; the column type is exposed as VARCHAR.
- [ ] Evaluate native spatial mapping and bounding-box pruning; geometry is exposed as binary.
  Decide the geometry-alias vocabulary (T-N1b) at the same time.

### Bounds and performance

- [ ] RV-25a: partitioned INSERT keeps one open writer per partition seen, and per-page arrays
  grow with historical partition count (`DucklakePageSink`). Cap active writers.
- [ ] RV-25b: sorted INSERT buffers the whole input and sorts at finish without spill.
- [ ] RV-25c: MERGE keeps positions as boxed lists, full lineage arrays, a `TreeSet` union and
  one delete page per file (`DucklakeMergeSink`). Use bitmaps and chunked output.
- [ ] RV-25d: Puffin CRC is checked after decoding, and the per-snapshot map is still boxed
  (`DucklakePuffinDeleteReader`).
- [ ] RV-25e: each change-feed unit (one per origin or deletion snapshot) opens its own scan of
  the file. Scan each file once; bound change-feed startup memory and time while keeping
  cross-window update pairing.
- [ ] Profile concurrent delete readers and add a retained-state budget; memory reservations
  are not a query- or node-wide bound.
- [ ] Fetch all predicate-column file statistics in one bounded planning query instead of a
  catalog call per column; unknown statistics must keep files.

### Features and surfaces

- [ ] RV-G09: writer options (compression, row groups, target size, inlining, deletion vectors)
  come from global config or the session only. Define table > schema > global > session
  precedence and use it in every writer; share writer-option construction (W-N5).
- [ ] RV-G14: honour sort specifications for partitioned and MERGE inserts, or refuse them;
  never promise globally sorted scans.
- [ ] RV-G23: maintenance procedures and dry runs report only to the server log. Return results
  to the caller (484 procedure metrics or result tables) while keeping the 483 SPI boundary.
- [ ] Expose catalog expiry-cleanup resume (`cleanUpExpiredObjects`) as SQL. After a
  `DucklakeExpiryCleanupIncompleteException` an operator has no connector call to resume it;
  `cleanup_old_files` drains only the physical queue.
- [ ] RV-G03: inline rows return NULL `$row_id`. Return real IDs; test across inline and flush.
- [ ] RV-G15: fully deleted files still get delete files. Retire them while keeping
  historical visibility.
- [ ] RV-G16: SHOW CREATE TABLE omits `location` and `partitioned_by`.
- [ ] RV-G17: no table-property mutation. Define supported changes and their snapshot meaning.
- [ ] RV-G18: snapshot 0 is rejected. Add the catalog-backed contract test, then decide.
- [ ] RV-G19: `$files.path` is the raw stored path. Decide raw versus resolved and test it.
- [ ] RV-G21: Hive partition keys are lowercased on import. Add exact-key and collision
  fixtures before changing it.
- [ ] RV-G22: `expire_snapshots` silently ignores `retention_threshold` when `snapshot_ids` is
  given. Make the two modes explicitly exclusive.
- [ ] RV-B17: CREATE/ADD NOT NULL is not supported; test that MERGE cannot write NULL into a
  column the catalog marks NOT NULL.
- [ ] W-N1: catalog map-key nullability differs from upstream's nested metadata convention;
  keep physically required Parquet map keys separate.
- [ ] RV-N04: `DucklakeMergeSink` reports zero completed bytes.
- [ ] RV-N05: unmatched MERGE INSERT rows all route to bucket 0.
- [ ] RV-N06: nested ALTER field matching is case-sensitive. Add a mixed-case test.
- [ ] RV-N07: field IDs use unchecked `Long.toInt()` in `DucklakeParquetSchemaBuilder`. Validate
  range and reserved IDs.
- [ ] RV-N02: retained-size estimates omit `DucklakeSplit.deleteFileSnapshotFilters` and
  `DucklakeColumnHandle.initialDefault`.
- [ ] Audit the remaining metadata namespaces: Trino view tags/dialect, snapshot
  `changes_made`, delete formats and connector options.

### Backends

- [ ] Document and qualify native and direct-Java Quack deployment, authentication and exact
  driver/server versions.
- [ ] Add Quack cross-engine and concurrent-writer coverage and a bounded CI lane once its
  transport/authentication contract is stable.
- [ ] Re-probe DuckDB/MySQL interoperability on dependency updates; keep a minimal reproducer
  for upstream if a crash or protocol failure reproduces.
- [ ] Extend view coverage to every supported backend; PostgreSQL is covered.

## Catalog-library asks

APIs or fixes needed in `ducklake-catalog` before the connector can close an item.

- [ ] RV-11a: publish CTAS atomically. Needs a create-table-with-data call (column ids returned
  before publication, table and first fragments in one snapshot) or a public write transaction
  spanning several calls. Until then CTAS is two snapshots; see CRUTCHES "Non-atomic CTAS".
- [ ] RV-10 and CTAS rollback: conditional drop/truncate by identity, e.g.
  `dropTable(tableId, expectedSnapshotId)`. DROP/TRUNCATE currently mutate by name, so a table
  re-created under the same name after binding (or an old snapshot pin) can be dropped; the
  CTAS rollback check-then-drop has the same race.
- [ ] RV-08: validate newer inlined deletes of file-resident rows in the commit transaction of
  DELETE/UPDATE/MERGE, on every attempt, for all touched source files. Rewrite already has this
  guard in 0.17.0.
- [ ] RV-25g: page the deletion queue's path references for DuckLake 1.0 catalogs, e.g.
  `listScheduledReferencePage(after: DucklakeScheduledPathCursor?, limit: Int):
  List<DucklakeScheduledPathCursor>`, keyset-ordered by (data_file_id, exact path bytes,
  path_is_relative), `limit` in 1..`DucklakeCleanupLimits.MAX_BATCH_SIZE`, honouring
  `readSession`, with no age filter. This is `DucklakeHierarchyReader.listScheduledReferences`,
  which 0.17.0 offers only to final-1.1 read sessions. With it, `remove_orphan_files` replaces
  `listAllReferencedFiles()` with `listRetainedFileReferencePage(DATA|DELETE)` plus this page
  under `ducklake.cleanup.max-reference-files`, and keeps directory ownership separate from file
  ownership.
- [ ] RV-25f: give `flush_inlined_data` a keyset-paged inlined change read, e.g.
  `getInlinedChangesPage(tableId, schemaVersion, endSnapshot, afterRowId: Long?, limit,
  columnIds)` returning the rows of `getInlinedChangesBetween(…, 0, endSnapshot, …)` in row-id
  order, so the procedure streams rows into the writer instead of loading a schema version.
- [ ] Count inlined rows including ended (deleted, never flushed) ones. `countInlinedRows` counts
  only live rows, so the flush budget does not cover deleted rows it still loads.
- [ ] Rewrite or clear statistics on type promotion. 0.17.0 `setColumnType` keeps FLOAT bounds
  verbatim across `REAL -> DOUBLE` (and zoneless bounds across `TIMESTAMP -> TIMESTAMPTZ`);
  upstream rewrites (`WriteFloatWidenedStats`) or clears them. Trino pruning guards against it
  (CRUTCHES "Catalog-stats file pruning"), but by upstream's filter SQL
  (`TRY_CAST(max_value AS DOUBLE)`) DuckDB reading a Trino-promoted table would prune on the
  stale text (not reproduced). Also expose per-file statistics type provenance.
- [ ] Let `findDataFileIdsInRange` take exclusive endpoints and NULL-admitting predicates.
- [ ] Report whether an uncertain commit outcome published, so failed output can be reclaimed
  (RV-24a) instead of left for operator reconciliation.
- [ ] For the opt-in 1.1 lane: a shared accounting hook for internal schema/default bytes and
  nodes, and bounded view/tag/comment/setting APIs.
- [ ] E-N3, E-N8: catalog-owned comment re-checks (primary-key name protection; redundant
  schema-version filter).
- [ ] Inlined rows of a table with an added GEOMETRY column cannot be read on a 1.0 lake: the
  0.17.0 default validation (`Final11InlineDefaults` -> `Final11InlineTypes`) has no `geometry`
  scalar and throws `Final-1.1 inline type geometry is not qualified`. Corpus:
  `data_inlining/data_inlining_alter_geometry.test:42` `SELECT i, v, g FROM t1 ORDER BY i`
  (oracle `1 hello NULL`, ...), and `:57, :96`. Needs the catalog to qualify GEOMETRY defaults.

## Upstream (DuckDB/DuckLake) waits

- [ ] Corpus directory `partitioning` has oracle failures from implicit newer-format fixtures
  (epoch transforms, NULL type) the fixed DuckLake 1.0 oracle rejects. Do not skip-list it;
  recheck when upstream fixes land. Its new mirror failures are listed under connector gaps
  (promoted-column reads) and test hygiene (`'nan'` casts).
- [ ] DuckLake 1.1 writes stay disabled, including development-format writes. Before any 1.1
  write admission, qualify schema migration, hierarchy, exactness, row-group metadata, ABI/wire,
  rollback and mixed versions. A version string or parsed signature is not proof.
- [ ] Before broad 1.1 read enablement: remaining bounded catalog transfers and accounting,
  cancellation/network faults, legacy/type/route coverage, retention and deployment gates. Keep
  the [opt-in candidate lane](CATALOG-CANDIDATE.md) separate from the normal 1.0 lane.
- [ ] Recheck final-1.1 VARIANT, nested identity, exactness and row-group metadata against exact
  artifacts.
- [ ] A cross-engine lease or lock contract for expiration versus long-running reads, and for
  concurrent import versus cleanup. Retention alone is not that contract.
- [ ] Quack's server-side commit model: avoid maintaining two guessed conflict models if
  direct-SQL writers become second-class.

## Test, CI and release hygiene

- [ ] Corpus `merge` is green with 4 verified mirrors; its remaining open reads are DuckDB
  FROM-first queries (`FROM Stock ORDER BY item_id` in `merge_update_insert.test`, `FROM
  my_timeseries` in `merge_partition.test`), which `TrinoCorpusDialect` refuses as "not a read"
  although brikk-sql transpiles them to `SELECT * FROM ...`. Admit FROM-first reads in the gate.
  The other merge refusals are correct: `MERGE ... RETURNING` is a write, `GLOB` has no Trino
  function, and `meta.ducklake_file_partition_value` is the oracle's local metadata catalog.
- [ ] Mirror failures from brikk-sql translations that certify but Trino rejects; report them
  upstream and keep them failing (no skip-list entries):
  - a HAVING reference to a select alias: `compaction/merge_adjacent_after_add_files_schema_evolution.test:114`
    and `:143`, `... GROUP BY id HAVING c > 1` with `COUNT(*) c` (oracle `0`), Trino `Column 'c'
    cannot be resolved`;
  - DuckDB VARIANT field access, which the translation keeps as ROW dereference while the
    connector exposes VARIANT as VARCHAR: `types/variant.test:52` `SELECT COUNT(*) FROM
    variant_tbl WHERE v.c=84` (oracle `1`), Trino `Expression v is not of type ROW`; also
    `stats/variant_float_and_nested_shredded_stats.test:43, :49, :97, :103`,
    `stats/variant_shredded_stats.test:181, :203, :229`, `stats/variant_stats_quote_rewrite.test:39`;
  - lowercase NaN literals: `partitioning/bucket_pruning_nan.test:34` `SELECT k FROM f WHERE d =
    'nan'::REAL ORDER BY k` (oracle `1`), Trino `Cannot cast 'nan' to REAL`; also `:40, :46, :52`
    and `:185, :191` (DOUBLE).

- [ ] Repair the opt-in qualification fixtures' format selection. 40 tests in the
  `-Pr07CatalogCandidate` suite fail on the current fixtures (measured in WP9); every failure
  is a catalog format-admission refusal caused by the fixture, e.g. legacy
  `TestPublishedNestedReads` cases open stored 1.0 lakes with a 1.1 client, and two
  `TestR07ConsumerTrial` direct-reader statistics cases enable 1.1 reads after
  `toCatalogConfig()` has locked the client to 1.0. Fix the fixtures, not catalog admission or
  the skip list.
- [ ] Add shared replay support for `require-env`: the pinned native metadata-logging fixture
  is a visible unsupported-directive file-skip, even with `DUCKLAKE_CI=1`.
- [ ] Resolve the existing `types` corpus oracle/setup failures: `all_types.test:32,42,49`
  cannot convert a JDBC TIME cell, and `null_type.test` uses NULL types refused by the fixed
  DuckLake 1.0 oracle, with cascading setup failures (66 record failures before `USE` scoping;
  68 after it, the two new ones being the VARIANT mirror failures listed above). Do not
  skip-list these files to make the directory green.
- [ ] RV-28a-c: require meaningful mirror coverage per selected corpus directory (the test asserts
  `totalPassed > 50` and one mirror pass overall); return the dialect gate's refusal reason
  instead of a Boolean; make unexpected timeouts fail. Add negative controls (all queries
  declined, missing supported table, timeout after an earlier failure).
- [ ] RV-29d/e: live mirror comparison ignores row order and column types.
- [ ] RV-30a: the nightly corpus test `assumeTrue`s on extensions and corpus files; make it
  mandatory where CI provisions them.
- [ ] RV-30b1/b2: the MinIO fixture turns any setup exception into a skip and uses
  `minio/minio:latest` plus in-image `mc`. Skip only when Docker is unavailable; pin the image.
- [ ] RV-30c: PR test reports and corpus summaries are uploaded only on failure.
- [ ] RV-G11a-d: base fixtures hard-wire PostgreSQL, Quack has no seed fixture, the corpus is
  PostgreSQL-only, and CI has no backend matrix. Build a small backend-neutral suite first.
- [ ] RV-23: add a regression that updates many rows of one file without lineage and asserts it
  is opened once and closed (the fix is in; the test is missing).
- [ ] RV-B15: parameterized test that every type name the connector emits is accepted by the
  catalog.
- [ ] RV-N08: nested reader tests in `TestDucklakePageSourceProvider` count positions without
  loading values. RV-N09: the BLOB audit in `TestDucklakeCrossEngineTypeAudit` checks bytes only
  if the payload is a `ByteArray`. RV-N10: `TestDucklakePartitionPruning` never closes its
  catalog. RV-N11: `TestDucklakeWriteMetadataAndSnapshot` asserts only `rowCount > 0` for SHOW
  STATS.
- [ ] CI-05: a full test run once aborted natively (`double free or corruption`, exit 134); no
  backtrace identified the component. Regular CI tests on JDK 26 to see whether it recurs.
- [ ] Small cleanups: duplicated `handleParquetException` (R-N4); the partial-delete catalog
  probe in `DucklakeSplitManager` that
  `validateDeleteFileFormats` already covers (R-N3); dead non-Parquet format branches in the
  write path (W-N2); unused `topLevelMatches` in `DucklakeAddFilesNameMapper` (P-N1).
- [ ] RV-27c1/c2, RV-D07: the release workflow does not check the tag against the project
  version or require a revision that passed CI; its example tag (`v483-1-ALPHA`) predates the
  `v0.5.x` scheme.
- [ ] RV-D06: unused entries and stale comments in `gradle/libs.versions.toml` (`lance-core`,
  arrow/adbc/jooq/terpal).
- [ ] RV-D08: reproduce the Compose workbench from a fresh clone; it builds from a sibling
  `ducklake-catalog` checkout and mounts extra catalogs. Current limits are in
  [LOCAL-STACK.md](LOCAL-STACK.md).

Not planned: commit author/message/extra-info writes, unless the upstream snapshot schema
defines compatible fields.

Use the [upstream research procedure](RESEARCH-upstreams.md) to update the upstream waits.
