# TODO — Rectify findings from the 2026-09-02 correctness eval

Source: a spec-conformance review of (a) the catalog layer
(`../ducklake-catalog/ducklake-catalog/src/dev/brikk/ducklake/catalog/*.kt`, consumed here as
`dev.brikk.ducklake:ducklake-catalog:0.4.0`) and (b) this connector, against the upstream DuckLake
C++ extension (`../ducklake-catalog/references/ducklake/src/`, catalog spec v1.0, DuckDB 1.5.5 /
Trino 483). Line numbers are as of that review; re-locate by symbol if they drift.

Labels are stable so items can be addressed one at a time:

| Prefix | Area | Lives in |
|---|---|---|
| `E-*` | catalog metadata / commit layer | `../ducklake-catalog` (bump the pin here after) |
| `R-*` | connector read path | this repo |
| `W-*` | connector write path | this repo |
| `T-*` | type mapping + DDL | this repo (some catalog) |
| `TR-*` | trino-side adoptions of ducklake-catalog API changes | this repo |
| `P-*` | maintenance procedures | this repo (some catalog) |

Severity suffix: `C` critical (data loss / silent wrong rows / catalog unloadable by DuckDB),
`H` high, `M` medium, `L` low, `N` nit. Mark `[x]` with a dated one-liner when done. Where two
reviews found the same issue it is listed once with the other labels in parentheses.

**2026-09-04 update:** ducklake-catalog 0.5.0 resolved most `E-*` items in `abb26ce..5ba4864`;
0.6.0 (`861f67c..90927e3`) resolves the remaining catalog parity/DDL items below. The connector is
on 0.6.0. One 0.6 API blocker remains: `listAllReferencedFiles().tableFiles` carries `tablePath`
but not the owning schema path, even though the catalog itself documents table paths as
schema-relative. The connector therefore cannot safely resolve relative files belonging to a
dropped table/schema. P-H1/P-M1 stay open until the ref also carries `schemaPath` + relativity (or
a fully resolved table base path); no connector-side metadata-SQL workaround will be added.

The ducklake-catalog repo resolved most `E-*` items in `abb26ce..5ba4864`
(its own review doc uses labels `W/R/C/Q/S/X`). Items below are annotated with the resolving commit;
the connector-side adoptions those commits require are collected in **`## Connector follow-ups
(TR-*)`** right below (TR = trino; `T-*` stays Types & DDL). After 0.6.0, only the P-H1/P-M1
payload gap above and minor documentation/re-check nits E-N3/E-N8 remain catalog-related.

Cross-cutting themes (fix these as families, not one-offs):
1. **Silent fallbacks instead of fail-loud** — E-M6, R-C1, R-M8, T-M1, W-M7, P-M2, R-L5.
2. **Temporal unit/format mismatches** — W-H1(R-M1), W-H3, W-H2(R-M3), P-H4, E-L5, E-L10.
3. **Encryption is invisible** — E-M2, R-M10, W-H5: one gate at `getTableHandle`/open fixes all.
4. **Name-first instead of field-id-first resolution** — R-H2, R-H3, T-C3, P-H5.
5. **Files written before a commit that fails are never cleaned up** — W-M4, W-M5, P-M6.
6. **Upstream scoped settings ignored** — T-M6 (W-M3, W-L3, W-L4).

---

## Connector follow-ups from ducklake-catalog (`TR-*`)

The catalog repo ran its own review (its `TODO-rectify-from-eval.md`, labels `W/R/C/Q/S/X`) and, in
commits `abb26ce..5ba4864`, resolved nearly every `E-*` item above and changed several APIs. These
are the connector-side adoptions it hands over, collected here so catalog bumps are one piece
of work. Order = suggested order.

- [x] **TR-1 — Map the typed catalog exceptions** (`db9c1f6`, `9f418a3`, `6c104b9`).
  DONE 2026-09-04 `0034446`: every catalog exception class maps to a stable Trino code; mapping
  is unit-tested and unexpected non-catalog runtime failures still propagate unchanged.
  `translateCatalogExceptions` must map `DucklakeEntityNotFoundException` → `NOT_FOUND`/
  `TABLE_NOT_FOUND`/`SCHEMA_NOT_FOUND`, `DucklakeEntityAlreadyExistsException` → `ALREADY_EXISTS`,
  `DucklakeSchemaNotEmptyException` → `SCHEMA_NOT_EMPTY`, `DucklakeInvalidOperationException` →
  `INVALID_ARGUMENTS`/`NOT_SUPPORTED`, `DucklakeEncryptedCatalogUnsupportedException` /
  `DucklakeUnsupportedCatalogVersionException` → `NOT_SUPPORTED`, `DucklakeCatalogCorruptionException`
  → `GENERIC_INTERNAL_ERROR`. Conflicts now arrive unwrapped (no `rootCause()` digging).

- [x] **TR-2 — Encryption gate on the READ side** (`6c104b9`; closes R-M10/W-H5 fully).
  DONE 2026-09-04 `0034446`: normal table handles fail with `NOT_SUPPORTED` before Parquet or
  inlined reads; metadata tables and metadata-only DDL remain available for diagnosis.
  Catalog refuses file writes into an encrypted lake; the connector must also refuse at
  `getTableHandle` (and for inlined splits) using `catalog.isEncrypted()`, with a named
  `NOT_SUPPORTED` error instead of Trino's "not a parquet file".

- [x] **TR-3 — Row counts: `record_count` is gross** (`db23b77`; closes R-L2 too).
  DONE 2026-09-04 `48ce139` (`getLiveRowCount`, nullable counts, `analyzeTable(tableId)`). Closes R-L2.
  `DucklakeMetadata.getTableStatistics` must use `catalog.getLiveRowCount(tableId, snapshotId)`
  instead of `getTableStats().recordCount`; `finishStatisticsCollection` should call
  `analyzeTable(tableId)` (the `(tableId, rowCount)` overload is deprecated). Handle nullable
  `totalValueCount`/`totalNullCount` in the null-fraction estimate (`DucklakeMetadata.kt:472-483`).

- [ ] **TR-4 — Flush via `flushInlinedDataWithSnapshots`** (`1121e3e`; closes P-C1 + P-M5).
  `DucklakeFlushInlinedDataProcedure` must write `_ducklake_internal_snapshot_id` (field id
  2147483539) per row, include deleted inlined rows plus a snapshot-tagged delete file, and call
  `flushInlinedDataWithSnapshots(tableId, List<FlushedInlinedFile>, preservedRowIdStart)`. Until
  then the legacy path still has the P-C1 race.

- [x] **TR-5 — Per-file stats: pass unknown as `null`** (`31d65ef`; closes R-M2/W-M1 write half).
  DONE 2026-09-04 `48ce139` (extractor → `null`; writer asserts TRUE/FALSE for scanned top-level floats only). Closes the write half of R-M2/W-M1; nested float leaves are now honestly unknown.
  `DucklakeStatsExtractor.kt:70` `containsNan = false` → `null` unless values were inspected;
  `value_count`/`null_count` nullable on `DucklakeFileColumnStats` (write together or not at all).

- [x] **TR-6 — Delete-file snapshot filter unconditional** (`f1c63ff`; closes R-L6).
  DONE 2026-09-04 `948106e`: read snapshot carried for every Parquet/Puffin delete file; readers
  inspect the file and fall back correctly for legacy global-row-id and normal two-column files.
  `DucklakeSplitManager.needsPartialDeleteSnapshotFilter` must return true for every
  PARQUET/PUFFIN delete file (drop the `deleteFilePartialMax` gate); the reader already ignores
  it for 2-column files.

- [x] **TR-7 — Change feed: detect 3-column delete files by schema** (`36bdb24`; R-M7 sibling).
  DONE 2026-09-04 `9147149`: Parquet schema is authoritative; regression sets `partial_max = NULL`
  on a real multi-snapshot delete file and verifies per-snapshot change-feed attribution.
  `DucklakePageSourceProvider.isConsolidatedDelete` must read the file schema (or try
  `readPositionsWithSnapshots` and fall back) instead of `currentDeletePartialMax`; flush-written
  files have `partial_max = NULL` and multi-snapshot embedded ids.

- [x] **TR-8 — Pass the read snapshot to `commitDelete`/`commitMerge`** (`0f0e4e8`).
  DONE 2026-09-04 `48ce139`.
  `DucklakeMetadata.kt` ~`:1415/1418` must call the `(tableId, fragments, readSnapshotId)`
  overloads with `tableHandle.snapshotId`; the old signatures are `@Deprecated` and degrade the
  stale-delete guard to the attempt's base snapshot.

- [ ] **TR-9 — Write snapshot-tagged (3-column) delete files** (`1121e3e`, W-D6 upstream v1.5 shape).
  `DucklakeMergeSink` should carry prior positions with the superseded file's embedded ids (or its
  `begin_snapshot`), tag new ones with `readSnapshot + 1`, and populate
  `DucklakeDeleteFragment.embeddedSnapshotMin/Max`; the catalog then DELETEs the superseded row
  and back-dates the new one like DuckDB. Combine with **W-C1** (sort positions) and **W-C2**
  (one delete file per data file per commit).

- [ ] **TR-10 — Wrap planning in `readSession`** (`b28fefb`).
  `getTableHandle` → `getSplits` reads should run inside `catalog.readSession { ... }` so a
  concurrent destructive op (expire, partial rewrite, consolidated delete) can't make two reads
  disagree on the file set.

- [ ] **TR-11 — Use the decoded inlined-value API** (`884bdaf`; closes R-L10, T-M8 read half).
  Switch to `readInlinedDataDecoded` / `getInlinedChangesBetweenDecoded` and retire
  `DucklakeInlinedValueConverter`'s raw-form parsing (BYTEA/text/`CAST(... AS VARCHAR)` handling
  now lives in `InlinedValues.decode`).

- [ ] **TR-12 — Column type strings are validated at the catalog boundary** (`f925232`).
  `DucklakeTypeNames.canonical/validate` now rejects non-spec names (e.g. `integer`); confirm
  `DucklakeTypeConverter.toDucklakeType` output is accepted for every Trino type we allow and that
  the connector maps `DucklakeInvalidOperationException` (TR-1) to a clear DDL error.

- [x] **TR-13 — Bump the pin to the released `0.5.0`** (drop `-SNAPSHOT`) once published; the
  DONE 2026-09-04 `48ce139` (0.5.0), then 0.6.0 after the catalog parity follow-up release.
  Harness: `9dfd6cc` fixes the corpus engine for the replay driver's re-ATTACH `connect()`.
  scoped `mavenLocal()` in `buildlogic.kotlin.brikk` stays for the dev loop.

---

## Catalog layer (`E-*`)

### Critical — interop breakers (DuckDB / pg_ducklake cannot load the catalog)

- [x] **E-C1 — Trino view JSON written into `ducklake_view.column_aliases`.** DONE 2026-09-03.
  Was: `insertViewRow` stored the connector's `ConnectorViewDefinition` JSON in `column_aliases`;
  upstream `ParseQuotedList` (`ducklake_metadata_manager.cpp:746,769`, `ducklake_util.cpp:17-20`)
  throws on it → every snapshot with an active Trino view made the **whole catalog** unloadable
  for DuckDB/pg_ducklake.
  Fix (catalog lib 0.5.0-SNAPSHOT + connector):
  * `DucklakeQuotedList` — byte-for-byte `ToQuotedList`/`ParseQuotedList` parity; now the single
    home for the spec's quoted encoding (`WriteChange`/`InterveningChanges` delegate to it).
  * `DucklakeView` carries `columnAliases: List<String>` + `tags: Map<String,String>`
    (+ `malformedColumnAliases` when a foreign writer's payload is found — flagged per view, so one
    bad view can't fail `listViews` for a schema). `getView`/`listViews` LEFT JOIN `ducklake_tag`
    in ONE query. `createView`/`replaceViewMetadata` take aliases + tags and write row + tags in one
    snapshot; `dropView` end-snapshots tags; `renameView` keeps `view_id` (tags ride along) and
    refuses to launder a malformed alias list.
  * Connector `DucklakeTrinoViewCodec`: `dialect='trino'` (flat — `/brikk` suffix removed, no
    legacy read path), real column names in `column_aliases`, `comment` tag (interoperable with
    DuckDB `COMMENT ON VIEW` — verified both directions), `trino.column_types` /
    `trino.column_comments` / `trino.catalog` / `trino.schema` / `trino.owner` /
    `trino.run_as_invoker` / `trino.path` tags. No JSON anywhere.
  * Fail-nicely for other Trino connectors: `isView` (used by DROP / CREATE OR REPLACE / RENAME)
    answers existence for any `trino`-family view without decoding; `getView` throws
    `INVALID_VIEW` naming view, reason and remedy when tags are missing/misaligned or the dialect
    is a `trino/<variant>`; `getViews` skips such views with a WARN. Never a guessed definition.
  * Tests: `TestDucklakeQuotedList`, `TestJdbcDucklakeCatalogViewTags` (incl. stock DuckDB
    attaching a catalog with a foreign-dialect view), `TestDucklakeTrinoViewCodec`,
    `TestDucklakeCrossEngineCatalogMetadata.testDuckdbLoadsCatalogWithActiveTrinoView…` and
    `…testForeignTrinoDialectViewIsListedButRefusedAndDroppable`.
  Remaining: legacy `trino/brikk` rows in existing catalogs are now just "foreign incompatible"
  views (listed, refused, droppable) — those catalogs were already unloadable by DuckDB; operators
  should `DROP` + recreate them. Publish catalog lib 0.5.0 and bump the pin from `-SNAPSHOT`.

- [x] **E-C2 — `dropColumn` cascades only one nesting level.**
  DONE in ducklake-catalog `9f418a3` (full-subtree cascade; `TestJdbcDucklakeCatalogDropCascadesInterop`).
  `JdbcDucklakeCatalog.kt:3030-3038` end-snapshots `column_id = X OR parent_column = X` only.
  Upstream `RemoveColumns` (`ducklake_table_entry.cpp:1190-1193`) recurses into all descendants.
  Dropping `list<struct<...>>`, `map<k, struct<...>>` or `struct<struct<...>>` leaves grand-children
  active with a missing parent → `AddChildColumn` (`ducklake_metadata_manager.cpp:401-412,736-741`)
  throws for every later snapshot. Fix: reuse `dropField`'s `collectSubtreeIds` (`:3271-3289`).
  Tests never drop `array(row)`/`row(row)`.

### High

- [x] **E-H1 — `dropSchema` ignores views/macros.**
  DONE in ducklake-catalog `9f418a3` (tables/views/macros checked; typed `DucklakeSchemaNotEmptyException`). **Connector follow-up → TR-1.**
  `JdbcDucklakeCatalog.kt:2383-2402`, `DucklakeWriteTransaction.kt:128-140` (`hasTablesInSchema`
  checks `ducklake_table` only). Upstream `ducklake_catalog.cpp:568-572,588-592` throws on a
  view/macro whose schema is gone → catalog unloadable. Fix: refuse (or cascade) when active
  views/macros exist.

- [x] **E-H2 — `mapping_id` allocated from `next_catalog_id`; upstream uses `next_file_id`.**
  DONE in ducklake-catalog `9856c16` (`mapping_id` from `next_file_id`; DuckDB name-map cache watermark verified).
  `JdbcDucklakeCatalog.kt:3425` vs `ducklake_transaction_state.cpp:532`. Independent counters →
  id collision between Trino `add_files` and DuckDB `ducklake_add_data_files`; `GetColumnMappings`
  (`:3911-3940`) and our `getNameMaps` (`:1441-1464`) group by `mapping_id`, merging two maps →
  wrong source-name→field-id resolution → silently wrong column data. Fix: allocate from
  `next_file_id`.

- [x] **E-H3 — `renameColumn` / `setColumnType` / `setFieldType` discard defaults.**
  DONE in ducklake-catalog `9923a40` (`replaceColumnVersion` copies every attribute incl. `parent_column`; nested-rename duplicate root row fixed too).
  `JdbcDucklakeCatalog.kt:3076-3086, 3125-3135, 3215-3226` hardcode `DEFAULT_VALUE='NULL'`,
  `DEFAULT_VALUE_TYPE='literal'`, never copy `INITIAL_DEFAULT` (SELECTs at `:3051`, `:3099` don't
  read them). Upstream copies `column_info` (`ducklake_transaction_state.cpp:1173-1177`,
  `ducklake_table_entry.cpp:1050,1108-1123,1413-1428`). Files written before a DuckDB-added
  `DEFAULT 42` column read NULL after a Trino rename, in both engines. Also `renameColumn`'s SELECT
  (`:3051-3056`) lacks `PARENT_COLUMN IS NULL` while its UPDATE (`:3070`) has it → on a nested field
  it updates 0 rows and inserts a duplicate top-level row.

### Medium

- [x] **E-M1 — Conflict matrix omits insert↔delete rules.**
  DONE in ducklake-catalog `bd37ecb` (matrix re-ported from v1.5 row by row incl. insert↔delete and compaction kinds).
  `ConflictMatrix.kt:147-156` and `:163-180` vs upstream `ducklake_transaction_state.cpp:203-209,
  217-224`. Add both directions including the `*_inlined` variants.

- [x] **E-M2 — Encryption ignored entirely** (also R-M10, W-H5).
  DONE in ducklake-catalog `6c104b9` — catalog side: `isEncrypted()`/`getSpecVersion()`; every file-writing op refuses on an encrypted lake (`DucklakeEncryptedCatalogUnsupportedException`). **Connector follow-up → TR-2** (refuse READS too, map the exception).
  `getDataFiles` (`:381-402`) never selects `encryption_key`; fragments never set it;
  `ducklake_metadata.encrypted` never read. Upstream `ducklake_initializer.cpp:204-212`,
  `ReadDataFile :1044-1050`, writer `ducklake_insert.cpp:343,483-489,712-716`. Reading an encrypted
  lake fails with Trino's opaque "not a parquet file" (inlined splits still return rows); writing
  commits **plaintext** files with `encryption_key=NULL` into an `encrypted=true` lake, which DuckDB
  reads without complaint — a silent confidentiality breach. Minimum fix: read `encrypted` at open
  and refuse with `NOT_SUPPORTED`. Full fix: per-file key generation + Parquet modular encryption.

- [x] **E-M3 — No `ducklake_metadata.version` gate.**
  DONE in ducklake-catalog `6c104b9` (`getSpecVersion()`; writes refused unless version ∈ {0.4, 1.0}). Connector: map `DucklakeUnsupportedCatalogVersionException` (TR-1).
  Upstream `ducklake_initializer.cpp:151-189` refuses non-1.0. Read version at open, refuse anything
  but `1.0` with a clear message.

- [x] **E-M4 — Trino ALTER on an inlining-enabled table doesn't create a new inlined-data table.**
  DONE in ducklake-catalog `a8bf82c` (`recordColumnSchemaChange` → new `ducklake_inlined_data_<t>_<v>` registered in the same commit; DuckDB oracle test).
  Upstream `ducklake_transaction_state.cpp:1247-1268` → `WriteNewInlinedTables`
  (`ducklake_metadata_manager.cpp:2559-2596`); DuckDB's next inlined insert uses
  `LatestInlinedTableQuery` (`:2486-2492, 2705-2719`) → column-count mismatch or positional
  mis-bind after a rename.

- [x] **E-M5 — `renameSchema` allocates a new `schema_id`.**
  DONE in ducklake-catalog 0.6.0 `26cd2ca`. The review's proposed same-id replacement was invalid
  because `schema_id` is a primary key: the correct fix keeps the new id but preserves the UUID,
  migrates schema-scoped settings, versions tags/comments, and records every table's schema history.
  `JdbcDucklakeCatalog.kt:2826-2844`. Orphans schema-scoped settings (`scope_id`) and schema
  comments (`ducklake_tag.object_id`); no `ducklake_schema_versions` rows for re-pointed tables.
  Keep the id; end-snapshot + re-insert (upstream pattern).

- [x] **E-M6 — Swallowed `DataAccessException` → silent under-return of inlined rows/deletes.**
  DONE in ducklake-catalog `6696853` (`rethrowUnlessMissingTable` at all 14 sites; user identifiers quoted too).
  `getInlinedDataInfos :1547-1555`, `hasInlinedRows :1567`, `countInlinedRows :1586`,
  `hasInlinedDeletes :1614`, `getInlinedDeletes :1653`, `readInlinedData :1805`,
  `readInlinedBeginSnapshots :1833`, `readInlinedRowIds :1860`, `getInlinedChangesBetween :1712`,
  `getInlinedFileDeletesBetween :1737` treat any DB error as "table doesn't exist". Transient failure
  on `getInlinedDeletes` resurrects deleted rows; on `readInlinedData` drops live rows. Probe
  existence via `ducklake_inlined_data_tables`; propagate real errors.

- [x] **E-M7 — Compaction changes row identity** (see also P-verified: the connector embeds
  DONE in ducklake-catalog `1121e3e` (rewrite registers at the smallest retired source's `row_id_start`, never advances `next_row_id`; `InsertMode.REWRITE`).
  `_ducklake_internal_row_id`, so DuckDB reads original ids; but the catalog still advances
  `next_row_id` and sets a fresh `row_id_start`). `rewriteDataFiles*` (`:3690`, `:3722`) →
  `applyInsertFragments` (`:3420`, `:3515`); upstream `ducklake_stats.cpp:206-215` preserves. Decide
  and align; `DucklakeCatalog.kt:620-623` documents the current choice.

- [x] **E-M8 — Quack backend: one transaction routed two ways (verify).**
  DONE in ducklake-catalog `5ba4864` (real server-side BEGIN/COMMIT/ROLLBACK through the wrapper; all UPDATE/DELETE/DROP routed; `TestJdbcDucklakeCatalogOnQuackWrites`).
  `QuackWrappedMetadataQuery.kt:77-90` UPDATE/DELETE via `quack_query_by_name`; inserts via the
  attached catalog on the local JDBC tx. Upstream `quack_metadata_manager.cpp:15-31` routes the
  whole batch through one call. Confirm enlistment or route everything one way.

### Low

- [x] **E-L1** — Default schema/table paths use raw names (`:2375`, `:2417`); upstream
  DONE in ducklake-catalog `21ca360` (UUID fallback for names outside `[A-Za-z0-9_-]`).
  `GeneratePathFromName` (`ducklake_catalog.cpp:236-255`) falls back to UUID unless `[A-Za-z0-9_-]`.
- [x] **E-L2** — `dropTable` (`:2565-2613`) doesn't end-snapshot `ducklake_tag`/`column_tag`/
  DONE in ducklake-catalog `21ca360` (drop retires tags) + `e5b3dcf` (expire GCs dead `ducklake_tag` rows).
  `sort_info` (upstream `DropTables :2278-2290`); `expireSnapshots` never GCs `ducklake_tag`.
- [x] **E-L3** — Comments don't bump `schema_version` (`:2933`, `:2967`) → DuckDB caches stale.
  DONE in ducklake-catalog `21ca360`.
- [x] **E-L4** — `ducklake_schema_versions` rows with `table_id = NULL` (`:2102-2110`); upstream
  DONE in ducklake-catalog `21ca360` (one row per created/altered table, none for view/schema DDL).
  never writes them. Stop.
- [x] **E-L5** — Timestamp time-travel ordered by `snapshot_id DESC` (`:243`) vs upstream
  DONE in ducklake-catalog 0.6.0 `90927e3`: `snapshot_time DESC, snapshot_id DESC`; snapshot and
  scheduled-file timestamps use backend `CURRENT_TIMESTAMP`.
  `snapshot_time DESC` (`:4116-4126`); `snapshot_time` from app clock (`:2156`) vs DB `NOW()`.
  Prefer the DB clock.
- [x] **E-L6** — `record_count` decremented on delete (`:3949-3955`); upstream never does.
  DONE in ducklake-catalog `db23b77` (`record_count` is gross; `getLiveRowCount` added). **Connector follow-up → TR-3.**
- [x] **E-L7** — `ducklake_table_column_stats.contains_nan` NULL instead of `false` (`:3586`,
  DONE in ducklake-catalog `21ca360` + `31d65ef` (explicit `false` for floats; tri-state MergeStats semantics).
  `:1320`) vs upstream explicit boolean (`:4441-4446`).
- [x] **E-L8** — `orZero(row_id_start)` (`:423`, `:553`) aliases NULL to 0. Fail loud.
  DONE in ducklake-catalog 0.6.0 `90927e3`: NULL is catalog corruption.
- [x] **E-L9** — Compaction recorded as `deleted_from_table`+`inserted_into_table` (`:3697-3698`)
  DONE in ducklake-catalog `bd37ecb` (`rewrite_delete` / `merge_adjacent` change kinds).
  → concurrent DuckDB inserts abort; upstream `merge_adjacent` doesn't conflict with inserts.
- [x] **E-L10** — Temporal stats compared lexically (`DucklakeStatTypes.kt`) vs upstream value
  DONE in ducklake-catalog `0e6c6f8` (`ComparisonClass`; temporal/boolean by value, text by code point, blob/interval/nested never pruned; `BoundsAccumulator` = upstream MergeStats).
  compare (`ducklake_stats.hpp:18-20`). Breaks on `(BC)` dates / mixed renderings (see W-H3).
- [x] **E-L11** — `getDataFiles` orders by `file_order` (`:409`), NULL for upstream files →
  DONE in ducklake-catalog 0.6.0 `90927e3`: deterministic `data_file_id` tie-break.
  nondeterministic. Order by `data_file_id`.

### Nits

- [x] **E-N1** — `InterveningChanges.applyEntry` case-sensitive; upstream `CIEquals`.
  DONE in ducklake-catalog `8dfe621` (CI kinds; unknown kinds tolerated conservatively).
- [x] **E-N2** — `ConflictMatrix.kt:159-162` rationale wrong; real guard is
  DONE in ducklake-catalog `bd37ecb` (matrix rewritten; comments regenerated).
  `checkDeleteFileOverlap` (`JdbcDucklakeCatalog.kt:4139-4177`). Fix comment.
- [ ] **E-N3** — `LogicalConflictCheck.kt:46-47` claims PK protection on names; there is none.
  Superseded: `bd37ecb` records `created_table`/`created_view` on rename so name collisions are matrix-detected; re-check the comment wording.
- [x] **E-N4** — `ConflictMatrix.kt` header cites `ducklake_transaction.cpp:1184-1314`; now
  DONE in ducklake-catalog `bd37ecb`.
  `ducklake_transaction_state.cpp:140-282`.
- [x] **E-N5** — `hasPartialDeleteFilesRequiringSnapshotFilter` (`:594-610`) effectively dead.
  DONE in ducklake-catalog `f1c63ff` (predicate no longer gated on `partial_max`; contract rewritten as advisory).
- [x] **E-N6** — `attemptWriteTransaction :2125` `throw e as RuntimeException` → possible CCE.
  DONE in ducklake-catalog `db9c1f6` (typed `Ducklake*Exception`s propagate unwrapped; only unexpected failures wrapped in `DucklakeException`). **Connector follow-up → TR-1.**
- [x] **E-N7** — `default_value_dialect` left NULL vs upstream `'duckdb'` (`:2397`).
  DONE in ducklake-catalog `21ca360`.
- [ ] **E-N8** — `getInlinedDataInfos :1504-1508` schema_version filter redundant.
  Not re-checked after the inlined-read refactor (`6696853`); trivial.

---

## Connector — read path (`R-*`)

### Critical

- [x] **R-C1 — Identity-partition predicates are `FULLY_ENFORCED` but not enforced for files the
  pruner can't decide on → over-return.** `DucklakeMetadata.kt:1804-1808` classifies identity as
  `FULLY_ENFORCED`; `:669-671` drops it from `remainingFilter`. `DucklakeSplitManager.
  pruneByPartitionValues` then *keeps* a file whenever it can't decide: retired/foreign
  `partition_id` (`:474-478`), no `ducklake_file_partition_value` row (`:485-487` — every file
  written before `SET PARTITIONED BY`, and every `add_files` file per P-H3), parse failure
  (`:550-552` — UUID/VARBINARY/TIME/CHAR identity keys, which `DucklakePartitionValueParser.
  parseIdentity:81` rejects). Inlined splits (`:96-104`) get no predicate at all
  (`createInlinedPageSource`, `DucklakePageSourceProvider.kt:331-398`) and upstream inlines into
  partitioned tables (`ducklake_insert.cpp:798-801`). Upstream never "enforces" via pruning — the
  filter is always re-evaluated per row (`ducklake_metadata_manager.cpp:1651-1687` is selection
  only). Repro: `CREATE TABLE t`; `INSERT`; `ALTER TABLE t SET PARTITIONED BY (region)`;
  `SELECT * FROM t WHERE region='US'` returns every row of the pre-partition file. Untested (all
  tests partition before inserting). Fix: classify identity as `PARTIALLY_ENFORCED` (engine
  re-filters), or fail loud when a split can't be proven to satisfy the enforced domain.
  DONE 2026-09-04 `ff25bee`: identity is partially enforced; a cross-engine regression writes an
  unpartitioned file, evolves to identity partitioning, writes another file, and verifies both
  predicates return only matching rows.

### High

- [ ] **R-H1 — `uint32`/`uint64` values ≥ 2³¹/2⁶³ read back negative** (also T-C2).
  `DucklakeTypeConverter.kt:104-109` maps `uint32→BIGINT`, `uint64→DECIMAL(20,0)`. DuckDB writes
  UINTEGER as INT32 `UINT_32`, UBIGINT as INT64 `UINT_64` (`parquet_writer.cpp:107-114,187-194`);
  Trino 483 `ColumnReaderFactory.isIntegerAnnotation` ignores signedness and sign-extends. A
  DuckDB-written 4294967295 reads as `-1`. Silent wrong data; only Trino-write→DuckDB-read is
  tested. Fix: custom decoders (mask/widen) or map `uint32→BIGINT` via a post-read fixup and
  `uint64` via unsigned→decimal conversion.

- [ ] **R-H2 — Column resolution is name-first, not field-id-first; predicate pushdown is
  name-only.** `resolveColumnIO` (`DucklakePageSourceProvider.kt:1557-1566`) tries the bare current
  name first, then field id; `toParquetTupleDomain` (`:1443-1461`) maps domains by lowercased name.
  Upstream `ducklake_multi_file_reader.cpp:223` is `BY_FIELD_ID`, name mapping only for files
  without ids or with `mapping_id` (`:544-559`). Consequences: swap-rename (`a→tmp, b→a, tmp→b`)
  reads the other column's bytes; rename `a→x` + `ADD COLUMN a` then `WHERE a IS NULL` pushes to the
  old physical `a` (null_count=0) → row group pruned → under-return. Fix: field-id-first; pushdown
  must go through the same resolution as projection.

- [ ] **R-H3 — Nested struct fields resolved by name, not field id** (also T-C3).
  `DucklakeParquetTypeUtils.kt:52-66` `groupColumnIO.getChild(fieldName)` with the *current*
  Trino name; no field id, era name, or `initial_default`. `getNameMaps` (`JdbcDucklakeCatalog.kt:
  1450-1452`) drops nested name-map rows ("handled by Trino's reader" — it isn't). Upstream
  `CreateColumnFromFieldId` recurses with per-child identifier + default
  (`ducklake_multi_file_reader.cpp:186-205,474-478`). Nested RENAME → NULL; nested DROP+re-ADD →
  stale data resurrected; nested `ADD ... DEFAULT` → NULL. `DESIGN-nested-field-evolution.md:13-14`
  asserts field-id binding that doesn't exist. Inlined path (`InlinedNestedFieldMapping.kt`) does
  do id mapping — parquet should match it.

- [ ] **R-H4 — Catalog-stat file pruning never executes in production; its value encoding is
  wrong if enabled.** `pruneDataFiles` (`DucklakeSplitManager.kt:236-247`) and
  `buildFileStatisticsDomain` (`:303-306`) early-return on `constraint.summary.isAll`; Trino 483
  `SplitSourceFactory` always passes `TupleDomain.all()` as the summary (verified in bytecode) —
  only unit tests hand in a real domain. Latent: `normalizePredicateValue` (`:378-386`) stringifies
  raw Trino natives (REAL bits, unscaled decimal long, micros) which the catalog then compares
  against DuckLake stat strings → e.g. `DECIMAL(10,2) x >= 100.00` becomes `"10000" > "150.00"` →
  file wrongly pruned. Fix: drive from `tableHandle.unenforcedPredicate` AND render values in
  DuckLake's canonical stat text first (share the writer's stringifier, see W-H3).

### Medium

- [ ] **R-M2 — `contains_nan` hard-coded `false` on write; NaN row groups dropped from min/max**
  PARTIAL `48ce139` (TR-5): `false` is no longer claimed for unobserved leaves — NULL/unknown instead. Remaining: scan nested float leaves so they can be TRUE/FALSE too, and keep NaN-bearing row groups out of min/max only when `contains_nan` is TRUE.
  Catalog `31d65ef` now accepts `containsNan = null` (unknown) and NULL counts with upstream MergeStats semantics — the connector should pass `null` unless it inspected values (→ **TR-5**).
  (also W-M1). `DucklakeStatsExtractor.kt:70-73,150-163`. Upstream tracks `has_nan`
  (`ducklake_insert.cpp:100-102`) and pruning consults it (`GenerateConstantFilterDouble`). A
  multi-row-group Trino file with NaN in some groups stores a finite max + `contains_nan=false` →
  `x > max` prunes NaN rows in both engines. Fix: compute from the writer (top-level AND nested
  float leaves) or write SQL NULL (unknown) when not verified.

- [ ] **R-M3 — Bucket hash diverges from DuckDB for UUID and non-micro timestamps** (also W-H2).
  `DucklakePartitionComputer.kt:287-296` hashes `epochMicros` for every `TimestampType`;
  `:313-322` hashes UUID as 16 raw bytes. Upstream `ducklake_murmur3.cpp` hashes the internal
  representation: INT64 raw stored unit (s/ms/ns), UUID (INT128) → `default` → `Value::ToString()`
  (36-char text). Bucket is `NOT_ENFORCED`, so a mismatch = wrongly pruned file = under-return on
  the Trino read side; and DuckDB skips Trino-written files on `WHERE uuid_col = ...`. Fix both the
  computer and `DucklakeBucketPartitionMatcher` (same function).

- [ ] **R-M4 — `$snapshot_id` is constant per file; upstream reads the embedded per-row column.**
  `DucklakePageSourceProvider.kt:1617-1619` emits `split.beginSnapshot`. Upstream
  `GetVirtualColumnExpression` (`ducklake_multi_file_reader.cpp:607-621`) reads field id
  2147483539 (`_ducklake_internal_snapshot_id`) when present. Wrong for every compacted/flushed
  partial file (the change feed already reads that column — `readInsertOriginSnapshots`).

- [ ] **R-M5 — `$row_id` is NULL on inlined rows.** `DucklakePageSourceProvider.kt:388-391` vs
  upstream `ducklake_multi_file_reader.cpp:390-391` (stored `row_id` prepended). Change feed uses
  `row.rowId`; table scan should too.

- [ ] **R-M6 — Type promotion allows INT→REAL/DOUBLE that Trino's reader cannot read.**
  `DucklakeTypePromotion.kt:43-46`. Trino `ColumnReaderFactory` reads DOUBLE only from
  DOUBLE/FLOAT primitives → `Unsupported Trino column type (double) for Parquet column (INT32)` on
  every old file after the ALTER (also for DuckDB-side promotions). Either add coercing readers or
  reject the promotion.

- [ ] **R-M7 — Change feed on a consolidated *puffin* delete file throws.**
  Related catalog change `36bdb24` (change feed offers every candidate delete file). Connector must detect 3-column files by schema, not `partial_max` → **TR-7**; puffin path still unhandled.
  `newlyDeletedPositionsBySnapshot` (`:788-808`) routes any `partial_max > snapshot` delete to
  `DucklakeDeleteFileReader.readPositionsWithSnapshots` (parquet-only); `DucklakePuffinDeleteReader`
  can read per-blob snapshots but is never called there.

- [ ] **R-M8 — Hive-partition value / `initial_default` parse failure silently → NULL** (also
  T-M1). `buildMissingColumnBlock` (`:1582-1591`), `resolveInlinedEraDefaults` (`:320-325`),
  `InlinedNestedFieldMapper` `runCatching{}.getOrNull()` (`InlinedNestedFieldMapping.kt:116-118`),
  `parseHivePartitionColumnValues :777` `?: continue`. Upstream throws
  (`hive_partitioning.cpp:147-149`, `ducklake_multi_file_reader.cpp:455-458`, `DefaultCastAs`).
  `parseIdentity` handles no VARBINARY/UUID/TIME/TIMETZ/JSON, so a DuckDB `ADD COLUMN b BLOB
  DEFAULT '\x00'` projects NULL for old rows. Throw.

- [ ] **R-M9 — TIMESTAMPTZ temporal partitions computed in UTC; upstream uses the writer's
  session zone.** `DucklakeTemporalPartitionMatcher.TemporalValue.from :229-243` vs
  `ducklake_partition_data.cpp:138-145` (ICU `year()/month()/...` in session TZ). PARTIALLY_ENFORCED
  so the engine re-filters, but a mismatch drops files near boundaries → under-return when the
  DuckDB writer wasn't UTC. (Write side W-L5 is the mirror.)

- [ ] **R-M11 — Legacy files with neither field ids nor a name map.** Upstream falls back to
  positional (`ducklake_multi_file_reader.cpp:545-558`); connector resolves by name and yields NULL
  on a miss.

### Low

- [ ] **R-L1** — `DucklakeSnapshotResolver.kt:89-91` rejects snapshot id 0 (valid upstream).
- [x] **R-L2** — `getTableStatistics :424` uses current-only `getTableStats` under time travel;
  DONE with TR-3 (`48ce139`).
  column stats are per-snapshot → inconsistent estimates.
- [ ] **R-L3** — Change-feed data columns built without `initialDefault`
  (`AbstractChangeFeedTableFunction.kt:100-106`); inlined change rows skip nested identity mapping
  (`inlinedRecordPageSource :647`).
- [ ] **R-L4** — `$files.path`/`delete_file_path` expose raw (possibly relative) catalog paths
  while `$path` is resolved (`buildFilesRows :432,438`).
- [ ] **R-L5** — `DucklakeDeleteFileReader.readNonNullValues :567-569` silently skips NULL `pos`
  (upstream throws, `ducklake_delete_filter.cpp:213-215`); `collectRowIdsInOrder :210-212` silently
  falls back to positional ids on a NULL lineage value.
- [ ] **R-L6** — Delete-file snapshot filter applied only when `partial_max > S`
  CATALOG HALF DONE `f1c63ff`; connector half → **TR-6**.
  (`needsPartialDeleteSnapshotFilter :560-567`); upstream applies `<= S` to *any* 3-column file
  (`ducklake_delete_filter.cpp:69-75`).
- [ ] **R-L7** — Puffin dispatch by `.puffin` extension in scan (`isPuffinPath :1487-1494`) vs
  extension-or-`format` in change feed (`:1020`). Use `format`.
- [ ] **R-L8** — `readLineage` materialises the whole lineage column per split (`:1305-1310`) and
  again per file in the change feed — a second full pass over every file.
- [ ] **R-L9** — Hive key matching lower-cased (`:777,804`); upstream exact.
- [ ] **R-L10** — PG-inlined structs with temporal/blob children arrive as `CAST('…' AS DATE)`
  tokens (`ducklake_util.cpp:151,307`); `NestedTextParser.parseScalar :671-679` throws.

### Nits

- [ ] **R-N1** — `DucklakeInlinedValueConverter.kt:465-467` dead expression (`"$normalized:00"`).
- [ ] **R-N2** — `DucklakeSplit.kt:107-175` five "kept for callers" constructors;
  `deleteFilePath()` test-only.
- [ ] **R-N3** — `validateNoUnfilterablePartialFiles :658-667` extra round trip for a case
  `validateDeleteFileFormats` already rejects.
- [ ] **R-N4** — `toLocation`/`handleParquetException`/close-and-wrap duplicated between
  `DucklakePageSourceProvider` and `DucklakeDeleteFileReader`.
- [ ] **R-N5** — `DucklakePageSourceProvider` (1848 lines) mixes parquet scan, inlined, metadata
  tables and the change-feed planner; `createParquetPageSource` ~230 lines. Split.
- [ ] **R-N6** — `_ducklake_internal_snapshot_id` matched by name
  (`DucklakeDeleteFileReader.kt:90`); upstream by field id 2147483539.

---

## Connector — write path (`W-*`)

### Critical

- [x] **W-C1 — Parquet delete files written with UNSORTED positions → DuckDB refuses to read the
  table.** `DucklakeMergeSink.kt:290-301` builds `unionPositions: LinkedHashSet<Long>` = prior
  positions (file order) then new positions in *arrival* order; `:382-385` writes in that order.
  Upstream writer uses an ordered `set<PositionType>` (`ducklake_delete.hpp:68`); reader hard-fails
  `"Invalid delete data - row ids must be sorted and strictly increasing"`
  (`ducklake_delete_filter.cpp:217-221`). Any second DELETE whose position is lower than an earlier
  one (`DELETE WHERE id=4` then `DELETE WHERE id=2`), or any MERGE/UPDATE whose matched rows arrive
  in join order, makes the table unreadable cross-engine until superseded.
  `TestDucklakeCrossEngineTrinoDeleteRead.kt:45-53` only deletes ascending. Puffin path unaffected.
  Fix: sort before writing (`TreeSet`/sorted array). Add a descending-order cross-engine test.
  DONE 2026-09-04 `0841dc7`: cumulative positions use `TreeSet`; DuckDB oracle deletes file-local
  position 4 then position 1 and reads the survivors successfully.

- [ ] **W-C2 — One data file can receive multiple delete files in one commit → DuckDB reads
  surviving rows twice.** Each `DucklakeMergeSink` has its own `deletesByDataFile` (`:85`) and emits
  one fragment per data file *it* saw (`:235-250`); `finishMerge` (`DucklakeMetadata.kt:1389-1424`)
  forwards without grouping by `dataFileId`; catalog inserts one `ducklake_delete_file` row per
  fragment (`JdbcDucklakeCatalog.kt:3917-3944`). `DucklakeMetadata` implements neither
  `getUpdateLayout` nor `getInsertLayout`, so rows of one data file span sinks whenever they cross a
  page or the MERGE join reorders them. Upstream invariant: ≤1 active delete file per data file; the
  file list is a plain `LEFT JOIN` (`ducklake_metadata_manager.cpp:1680-1690`) → the data file
  appears twice, each copy filtered by a *different partial* delete set → duplicate rows in
  DuckDB/pg_ducklake; the two files also disagree with each other. Fix: implement
  `getUpdateLayout` partitioning by data file (Trino's standard approach) **and/or** merge fragments
  by `dataFileId` in `finishMerge` (re-union positions → one file). Catalog should also assert the
  invariant at commit.
  RECONFIRMED 2026-09-04: a temporary pre-commit guard caught duplicate fragments in the existing
  `TestDucklakeMerge.testMergeDeleteOnly`, so this is a normal distributed MERGE path, not merely
  theoretical. Guard-only was removed because it regressed supported MERGE. Real fix needs
  `getUpdateLayout` + a `ConnectorNodePartitioningProvider` keyed by source `data_file_id`.

### High

- [ ] **W-H1 — `timestamp_s/_ms/_ns` file stats decoded as microseconds** (also R-M1).
  `DucklakeStatsExtractor.kt:174-179` always treats INT64 stats as µs, but Trino's
  `ParquetSchemaConverter` writes p≤3 as MILLIS and p>6 as NANOS, and `DucklakeTypeConverter.kt:
  187-191` maps those DuckLake types to p=0/3/9. min/max off by 10³ → DuckDB prunes via
  `TRY_CAST(min_value ...)` (`ducklake_metadata_manager.cpp:1167-1215`) → silently empty results
  for range predicates in DuckDB; also poisons `ducklake_table_column_stats`. Fix: decode by the
  parquet logical-type unit.

- [ ] **W-H3 — Temporal min/max use Java ISO text, not DuckDB's canonical form → cross-engine
  table-stat merge corrupts bounds.** `DucklakeStatsExtractor.kt:176-184` → `LocalDateTime.
  toString()` (`2024-01-15T10:30`, drops `:00`) / `Instant.toString()` (`...Z`). Upstream
  `Timestamp::ToString` → `2024-01-15 10:30:00[.ffffff]`, tz `…+00`. The catalog merges temporal
  bounds *lexically* (`DucklakeStatTypes.kt:23-28,116-126`, used at `JdbcDucklakeCatalog.kt:
  3564-3568`); `' ' < 'T'`, so a DuckDB `2024-01-15 23:00:00` loses to a Trino `2024-01-15T00:00`
  → table-level max regresses → DuckDB folds filters against `ducklake_table_column_stats` → empty
  scans. Fix: one canonical DuckDB-format stringifier shared by the stats extractor, add_files,
  and the pruner (R-H4); fix E-L10 to compare by value.

- [ ] **W-H4 — `TIMESTAMP WITH TIME ZONE`, `TIME`, `TIME WITH TIME ZONE`, `UUID` columns are
  accepted at DDL but cannot be written; a pure DELETE on such a table also fails** (also T-H1).
  `DucklakePageSink.kt:139-140`, `DucklakeMergeSink.kt:361`, `DucklakeFlushInlinedDataProcedure.kt:
  234`, `DucklakeRewriteDataFilesProcedure.kt:386` use Trino's `ParquetSchemaConverter`, which (483
  bytecode) has no TimestampWithTimeZone/Time/TimeWithTimeZone/Uuid branches → `Unsupported
  primitive type` at sink construction; `createMergeSink` always builds the insert sink
  (`DucklakePageSinkProvider.kt:86`). `ParquetWriters` *does* have value writers for these — only the
  schema builder is missing. `DucklakeTypeConverter.toDucklakeType:205-230` accepts them;
  `DucklakeTypePromotion.kt:57` allows TIMESTAMP→TIMESTAMPTZ, leaving the table un-insertable.
  README `:198,200,201,206` says Write=Yes; `beginCreateTable` tstz comment
  (`DucklakeMetadata.kt:1145-1153`) describes a path that can't run. Fix: own schema builder
  (`DucklakeParquetSchemaBuilder` already exists — extend it to build the MessageType directly with
  INT64 TIMESTAMP(MICROS, adjustedToUTC=true), INT64 TIME(MICROS), FLBA(16) UUID).

- [ ] **W-H6 — Read side declares sorted `LocalProperty` for every table with a sort spec, but
  writes only sort in one gated case.** `getTableProperties` (`DucklakeMetadata.kt:282-310`) vs
  writes sorting only when unpartitioned + parquet (`:1057-1062`); partitioned INSERTs and *all*
  UPDATE/MERGE inserts (`beginMerge :1335-1344` passes no `sortColumns`) write unsorted files. Also
  the property claims a per-driver sorted stream while a driver processes many files. Trino may
  plan streaming/window operators with `preSortedOrderPrefix` on unsorted input → wrong results.
  Upstream sorts partitioned inserts too (`ducklake_insert.cpp:784-794`). Fix: stop advertising
  `LocalProperty` (per-file sortedness isn't a stream property), and sort on every write path.

### Medium

- [ ] **W-M2 — Row-group stat aggregation not conservative.** `DucklakeStatsExtractor.kt:89-102`
  sets `hasStats` if *any* row group has min/max, skipping groups without. Upstream requires all
  (`parquet_writer.cpp:1040-1044,1067`). parquet-mr omits stats when 1024-byte binary truncation
  can't produce a valid bound → too-tight bounds. Require all groups.

- [ ] **W-M3 — Upstream write settings ignored** (also T-M6). Compression hard-coded ZSTD
  (`DucklakePageSink.kt:430`, `DucklakeMergeSink.kt:369`); row-group size / `parquet_version` /
  `parquet_compression_level` / `parquet_row_group_size[_bytes]` / `target_file_size` come from
  Trino's `ParquetWriterConfig` (`DucklakePageSink.kt:72-77`); file rollover threshold = row-group
  size (`:114`) → ~one row group per file. Upstream reads these from `ducklake_metadata`
  (`ducklake_insert.cpp:492-517`, precedence table>schema>global `ducklake_catalog.cpp:947-989`).

- [ ] **W-M4 — No cleanup of written files when the commit fails; no connector-level rollback.**
  `DucklakeTransactionManager.kt:42-56` commit/rollback are no-ops ("will be implemented later");
  `finishInsert/finishMerge` commit immediately; on a non-retryable conflict the data/delete files
  are orphaned; `DucklakeMergeSink.abort()` (`:433-438`) doesn't delete already-written delete
  files. Upstream deletes written files on rollback. DuckDB's `cleanup_old_files` never finds these.

- [ ] **W-M5 — CTAS is two snapshots and non-atomic.** `beginCreateTable` commits DDL
  (`DucklakeMetadata.kt:1129-1138`) before data; a failed CTAS leaves an empty table. Upstream is
  one transaction (`ducklake_insert.cpp:60-66`).

- [ ] **W-M6 — Multi-statement transactions silently autocommit per statement.** No
  `isSingleStatementWritesOnly()` override in `DucklakeConnector.kt`, yet every `finish*` commits →
  `ROLLBACK` after INSERT inside `START TRANSACTION` leaves data committed. Declare
  single-statement-writes-only until real transactions exist.

- [ ] **W-M7 — Schema builder silently omits `field_id` on unmatched fields.**
  `DucklakeParquetSchemaBuilder.kt:62-64` (`?: field`) and `:140-142`. DuckDB maps by field_id →
  such a column reads NULL in DuckDB. Throw.

### Low

- [ ] **W-L1** — Delete files lack upstream field ids (`file_path`→2147483646, `pos`→2147483645;
  `ducklake_delete.cpp:45-47`).
- [ ] **W-L2** — Fully-deleted data files still get a delete file; upstream end-snapshots the data
  file (`TryDropFullyDeletedFile`, `ducklake_delete.cpp:443`).
- [ ] **W-L3** — `data_inlining_row_limit` never honoured on write (always files).
- [ ] **W-L4** — `write_deletion_vectors` is a session property, not the per-table option upstream
  consults.
- [ ] **W-L5** — Temporal transforms / identity text for `timestamptz` computed in UTC; DuckDB
  uses session TZ and renders identity with an offset. Cosmetic + compaction grouping (see R-M9).
- [ ] **W-L6** — `temporal_partition_encoding=epoch` (deprecated knob, `DucklakeConfig.kt:39-151`)
  writes non-spec partition values. Remove.
- [ ] **W-L7** — Float identity partition text Java-formatted (`1.0E7`) vs DuckDB (`10000000.0`).
- [ ] **W-L8** — BC dates render `-0001-01-01` vs DuckDB `0002-01-01 (BC)` → `TRY_CAST` NULL →
  file pruned in DuckDB.

### Nits

- [ ] **W-N1** — `toColumnSpec :822` map `key` `nulls_allowed=false`; upstream `true` for all
  nested children (`ducklake_table_entry.cpp:1435`).
- [ ] **W-N2** — Residual multi-format plumbing after formats were removed: `fileFormat` gating and
  `openNewWriter` throw (`DucklakePageSink.kt:129-156,405-412`), nullable `messageType/
  primitiveTypes`, 4-level `resolveWriteFormat` (`DucklakeMetadata.kt:1236-1251`),
  `validateDataFileFormatIsParquet`.
- [ ] **W-N3** — `finishMerge` classifies fragments by trial-parse + `"ducklake-delete-"` prefix
  (`:1395-1405`). Use a tagged envelope.
- [ ] **W-N4** — `DucklakeMergeSink.findDataFileRange` O(F) linear (`:430-431`) despite
  `rangeByRowIdStart`.
- [ ] **W-N5** — `ParquetWriterOptions` built twice (`DucklakePageSink.kt:72-77`,
  `DucklakePageSinkProvider.kt:79-84`).
- [ ] **W-N6** — Footer-size fallback `catch (IOException) { 0 }` (`ParquetFileWriter.kt:124-126`,
  `DucklakeMergeSink.kt:417-419`) — log it.
- [ ] **W-N7** — Stale comments: `DucklakeTransactionManager` "will be implemented later";
  `DucklakePageSink.kt:125-128` still cites "the duckdb writer".

---

## Types & DDL (`T-*`)

### Critical

- [ ] **T-C1 — `int128`/`uint128`/`interval` cannot be read from DuckDB-written parquet.**
  `DucklakeTypeConverter.kt:116-117,148`: `int128→DECIMAL(38,0)`, `uint128→VARCHAR`,
  `interval→VARCHAR`. DuckDB writes HUGEINT/UHUGEINT as physical **DOUBLE**
  (`parquet_writer.cpp:88-91`) and INTERVAL as **FLBA(12)** (`:115-118,238-243`). Trino 483
  `ColumnReaderFactory` has no DOUBLE→Decimal, DOUBLE→Varchar or FLBA→Varchar path → `Unsupported
  Trino column type`. `TestDucklakeCrossEngineTypeAudit.kt:593-643` inserts 1 row → inlined
  (upstream default `data_inlining_row_limit`=10), so parquet is never exercised. README `:212,
  222-223` claims Read=Yes. Fix: custom readers (DOUBLE→DECIMAL(38,0) is lossy by construction —
  consider mapping `int128` to DOUBLE-backed decimal only with a documented caveat, or reject) and
  INTERVAL FLBA(12) decode (months/days/millis → Trino INTERVAL or VARCHAR text). Fix the audit test
  to force a flush.

- [ ] **T-C3 — Struct field names lowercased and matched case-sensitively → mixed-case struct
  fields read as NULL** (also R-H3). `DucklakeTypeConverter.kt:58` lowercases the whole type string
  incl. struct field names from `resolveColumnType` (`JdbcDucklakeCatalog.kt:4002-4008`);
  `DucklakeParquetTypeUtils.kt:63` binds children by exact name. DuckDB preserves case
  (`ducklake_field_data.cpp:85-86`). DuckDB-created `STRUCT(Name VARCHAR)` → Trino field `name` →
  lookup misses → silently NULL. Write side: `DucklakeParquetSchemaBuilder.annotateGroup:111` looks
  up `children[child.name]` lowercased → no field_id on that child → DuckDB also misses. Field names
  containing `:` `,` `<` `>` or edge spaces mis-split (`:70-75`, `splitTopLevelCommas`). Fix:
  build the Trino type from the column *tree* (`DucklakeColumn` children), never from the type
  string; lowercase only the type keyword.

### High

- [ ] **T-H2 — `timetz` and `variant` unreadable from parquet (README says Read=Yes).**
  `ColumnReaderFactory` has no `TimeWithTimeZoneType`; upstream writes TIMETZ as INT64
  TIME(MICROS, UTC) (`parquet_writer.cpp:199-207`). `variant→VARCHAR` (`DucklakeTypeConverter.kt:
  147`) but upstream writes VARIANT as a **group** (`:330`) → `DucklakeParquetTypeUtils.kt:114`
  `columnIO as PrimitiveColumnIO` ClassCastException. Reject at `getTableHandle` with a named error
  until supported; fix README.

- [ ] **T-H3 — Type promotions the connector allows that upstream forbids.**
  `DucklakeTypePromotion.kt:57` allows any `TimestampType` p∈{0,3,6,9} → TIMESTAMPTZ; upstream
  (`ducklake_table_entry.cpp:906-915` + `cast_rules.cpp:252-292`) allows only µs TIMESTAMP →
  TIMESTAMP_TZ. Connector then writes `timestamptz` over MILLIS/NANOS files; TIMESTAMP(9)→tstz
  truncates. Conversely (safe, optional) connector rejects upstream-allowed `DATE→TIMESTAMP*`,
  `TIMESTAMP_S→_MS→µs→_NS`, `INT→DECIMAL`, decimal widening, `DECIMAL→FLOAT/DOUBLE`,
  `BIGINT→HUGEINT`.

- [x] **T-H5 — `dropColumn` skips upstream's partition/sort guards.** `DucklakeMetadata.
  DONE in ducklake-catalog 0.6.0 `f64072a`: active partition (including descendants), sort, and
  last-top-level-column guards; typed connector mapping landed in TR-1.
  dropColumn:888-893` → `JdbcDucklakeCatalog.dropColumn:3026-3043` no checks; upstream
  `ducklake_table_entry.cpp:841-865` refuses dropping a column in the active partition or sort spec.
  Leaves `ducklake_partition_column.column_id` dangling → both engines' inserts break.

### Medium

- [ ] **T-M2 — Upstream vocabulary the connector cannot parse: `time_ns`, `timestamp_us`,
  `unknown`** (`ducklake_types.cpp:32,35,48`). `toTrinoType` throws `NOT_SUPPORTED`, and because
  `listTableColumns:687-726` maps every column, one such column breaks `information_schema.columns`
  / `SHOW COLUMNS` for the whole schema. Map `timestamp_us`→TIMESTAMP(6); skip-with-warning or
  expose unsupported columns as hidden.

- [ ] **T-M3 — `ADD COLUMN ... FIRST/AFTER` silently appends at end; comments dropped.**
  `DucklakeMetadata.addColumn:881-886` ignores `position` (Trino's default throws) and
  `column.comment`; `createTable:744-774` / `beginCreateTable:1099-1138` drop table and column
  comments (upstream persists as `ducklake_tag`/`ducklake_column_tag`).

- [x] **T-M4 — Reserved column names not rejected.** Upstream `ducklake_util.cpp:343-347`,
  DONE in ducklake-catalog 0.6.0 `f64072a`: CREATE/ADD/RENAME reject all inlined-system names.
  `ducklake_table_entry.cpp:730-737,777-784` refuse `row_id`, `begin_snapshot`, `end_snapshot`,
  `_ducklake_internal_snapshot_id`, `_ducklake_internal_row_id`. Connector `toColumnSpec:802-828`
  has no check → DuckDB's next inlined insert into that table fails.

- [ ] **T-M5 — add_files type check accepts signed parquet ints into unsigned columns.**
  `DucklakeAddFilesTypeChecker.kt:68-73,109-121` vs upstream `ducklake_add_data_files.cpp:701-723`
  (unsigned sources only). Also stricter than upstream on timestamps (`:80-83` vs `:743-755`) and
  looser on TIMESTAMPTZ/TIME precision (upstream exact, `:830-841`).

- [ ] **T-M6 — `ducklake_metadata` scoped settings not honoured** (W-M3, W-L3, W-L4, E-M2, E-M3).
  Connector reads only `data_path` and its private `data_file_format` (`JdbcDucklakeCatalog.kt:
  1943-1949,2496-2505`). `parquet_compression`, `parquet_row_group_size`, `target_file_size`,
  `hive_file_pattern`, `data_inlining_row_limit`, `encrypted`, `version` ignored; upstream
  precedence table>schema>global (`ducklake_catalog.cpp:947-989`). Implement a settings resolver
  in the catalog lib and consume it in the sinks.

- [ ] **T-M7 — NOT NULL: connector doesn't declare `ConnectorCapabilities.
  NOT_NULL_COLUMN_CONSTRAINT`** → `CREATE TABLE ... NOT NULL` / `ADD COLUMN ... NOT NULL` refused
  by the engine (fail-loud but undocumented), and no sink-side enforcement exists for DuckDB-created
  `nulls_allowed=false` columns. Also W-N1 (map key nullability).

- [ ] **T-M8 — Inlined TIME/TIMETZ values**: `DucklakeInlinedValueConverter.convertScalar:120-121`
  falls back to `Slice` → `writeNativeValue(TimeType, Slice)` ClassCastException.

### Low

- [ ] **T-L1** — `getTableMetadata:273-279` returns empty properties → `SHOW CREATE TABLE` omits
  `partitioned_by`/`location`; no `setTableProperties` (no `SET PARTITIONED BY`/`SET SORTED BY`).
- [x] **T-L2** — `column_order`: top-level 1-based / children 0-based (`insertColumnTree:2532,
  DONE in ducklake-catalog 0.6.0 `90927e3`: every new row uses `column_order = column_id`.
  2542-2544`); upstream `column_order = column_id`. Interoperable; mixed-writer tables non-monotone.
- [ ] **T-L3** — Bounded-varchar and `CharType` branches in `DucklakeAddFilesTypeChecker.kt:
  98-104` dead (CHAR rejected upstream of them).
- [x] **T-L5** — `renameTable`/`dropTable` clash checks case-sensitive (`JdbcDucklakeCatalog.kt:
  DONE in ducklake-catalog 0.6.0 `f64072a`: schema/table/view lookup, resolution and clashes are
  case-insensitive like DuckDB.
  2759`); upstream catalog is case-insensitive.

### Nits

- [ ] **T-N1** — `DucklakeTypeConverter.kt:105` stale `TODO` (range checker exists); `:153-155`
  `point/linestring/...` not in upstream vocabulary (only `geometry`) — dead entries + tests
  (`TestDucklakeTypeConverter.kt:96-118`).
- [ ] **T-N2** — `DucklakeSessionProperties.kt:102` doc says "Default false"; default is `true`
  (`:73`).
- [ ] **T-N3** — Five independent type tables that drift: `DucklakeTypeConverter`,
  `DucklakeStatTypes` (catalog), `DucklakeUnsignedRangeChecker.checkerFor`,
  `DucklakeAddFilesNameMapper.parquetPrimitiveToTrino`, README §Type System. Consolidate.

### Type-mapping status (upstream `column_type` ↔ Trino)

| upstream | DuckDB parquet physical | Trino | status |
|---|---|---|---|
| boolean, int8..int64 | BOOLEAN, INT32/INT64 | BOOLEAN, TINYINT..BIGINT | OK |
| uint8, uint16 | INT32 UINT_8/16 | SMALLINT, INTEGER | OK |
| uint32 | INT32 UINT_32 | BIGINT | **R-H1** |
| uint64 | INT64 UINT_64 | DECIMAL(20,0) | **R-H1** |
| int128 / uint128 | DOUBLE | DECIMAL(38,0) / VARCHAR | **T-C1** |
| float32, float64 | FLOAT, DOUBLE | REAL, DOUBLE | OK |
| decimal(p,s) | INT32/INT64/FLBA | DECIMAL(p,s) | OK |
| varchar, blob, json | BYTE_ARRAY | VARCHAR, VARBINARY, JSON | OK |
| date | INT32 DATE | DATE | OK |
| time | INT64 TIME µs | TIME(6) | read OK, **write W-H4** |
| time_ns | — | — | **T-M2** |
| timetz | INT64 TIME µs UTC | TIME(6) WITH TZ | **T-H2 / W-H4** |
| timestamp / timestamp_us | INT64 µs | TIMESTAMP(6) | OK / `_us` **T-M2** |
| timestamp_s/_ms/_ns | INT64 | TIMESTAMP(0/3/9) | read OK; stats **W-H1** |
| timestamptz | INT64 µs UTC | TIMESTAMP(6) WITH TZ | read OK, **write W-H4** |
| interval | FLBA(12) | VARCHAR | **T-C1** |
| uuid | FLBA(16) | UUID | read OK, **write W-H4** |
| variant | group | VARCHAR | **T-H2** |
| geometry | BYTE_ARRAY | VARBINARY | unverified |
| list/struct/map | groups | ARRAY/ROW/MAP | tokens OK; **T-C3** |

---

## Maintenance procedures (`P-*`)

### Critical

- [ ] **P-C1 — `flush_inlined_data` end-snapshots rows inserted after its read but never writes
  PARTIAL in ducklake-catalog `1121e3e`: `flushInlinedDataWithSnapshots` deletes only `begin_snapshot <= upToSnapshot` (upstream shape, no race). The legacy 2-arg `flushInlinedData` the connector calls still end-snapshots **every** live row (`JdbcDucklakeCatalog.kt:3380-3392`) — the race is open until **TR-4** lands.
  them → permanent row loss.** `DucklakeFlushInlinedDataProcedure.kt:97` reads at
  `currentSnapshotId`, writes Parquet, then `catalog.flushInlinedData` (`:197`) end-snapshots
  **every** live row: `JdbcDucklakeCatalog.kt:2722-2724` `.set(endSnapshot, new).where(endSnapshot.
  isNull)`. The conflict matrix only runs when `currentSnapshotId > transactionStartSnapshotId`
  (`:2091`), and that start id is captured **inside** `executeWriteTransaction` (`:2051-2052`) —
  after the read. Upstream `DELETE ... WHERE begin_snapshot <= <read snapshot>`
  (`ducklake_metadata_manager.cpp:5105-5115`, `ducklake_flush_inlined_data.cpp:197`) inside one
  transaction spanning the read. A DuckDB inlined INSERT at S1 during the flush (read S0) gets
  `end_snapshot=S2` and is in no file; a concurrent inlined DELETE is resurrected. This is the
  exact workload inlining targets. Fix: pass the read snapshot; end-snapshot only `begin_snapshot <=
  readSnapshot`, and abort if any row has `end_snapshot` in `(readSnapshot, new)`.

### High

- [ ] **P-H1 — `remove_orphan_files` known-set omits dropped-but-unexpired tables → deletes
  CATALOG API PARTIAL in 0.6.0 `861f67c`: `listAllReferencedFiles()` now includes every table at
  every snapshot and separates scheduled paths. **Blocked:** each `DucklakeTableFilePathRef` lacks
  the schema path required to resolve its schema-relative `tablePath`, especially after table or
  schema drop. Catalog must add that path (or return the resolved table base). The connector stays
  on the old path rather than guess. **Still the top open procedure data-loss item.**
  catalog-referenced files.** Targets from `listTables/listSchemas(snapshotId)` (`DucklakeRemove
  OrphanFilesProcedure.kt:141,146,190-193`) are `activeAt` filtered (`JdbcDucklakeCatalog.kt:266,
  283`); `listReferencedFilePaths` only runs for those. Upstream `GetKnownFilesForCleanupQuery`
  (`:4549-4573`) joins **all** data/delete file rows with no liveness filter. After `DROP TABLE t`
  (still time-travelable), a sweep deletes `t`'s files → `FOR VERSION AS OF` fails in both engines;
  later `expire_snapshots` schedules a missing path and `cleanup_old_files` sticks (P-M2). Since
  table dirs are `"$tableName/"`, `DROP TABLE t; CREATE TABLE t` reuses the directory, so even the
  **table-scoped** call deletes the old table's files. No test drops a table first.

- [x] **P-H2 — `rewrite_data_files(reclaim_sources_immediately => true)` merges sources carrying
  deletes, then deletes the delete files → time travel silently loses rows.** `selectCandidates`
  (`DucklakeRewriteDataFilesProcedure.kt:218-223`) filters only format/size/`partialMax == null`;
  reads apply deletes (`:337-345`); partial commit back-dates the merged file to `min(source begin)`
  and `scheduleAndDeleteRewriteSources` (`JdbcDucklakeCatalog.kt:3765-3781`) deletes the delete
  files. Upstream skips candidates with `!delete_files.empty() || has_inlined_deletions`
  (`ducklake_compaction_functions.cpp:281-286`) and throws if one slips through (`:496-499`). Row
  inserted s1, deleted s3, merged s5 → `FOR VERSION AS OF s2` omits it; irreversible after cleanup.
  Fix: exclude delete-bearing sources from partial rewrites (upstream), or write a snapshot-tagged
  delete file for the merged output.
  DONE 2026-09-04: partial candidates now exclude existing `delete_file_path` and any data-file id
  returned by `getInlinedFileDeletesBetween`; ordinary rewrite still applies deletes safely because
  source files remain available to older snapshots. Regression compacts clean peers while keeping
  the delete-bearing source and verifies the deleted row remains visible before its delete snapshot.

- [ ] **P-H3 — `add_files` on a partitioned table accepts files with missing/incomplete partition
  values → enforced partition predicates leak rows** (interacts with R-C1). `DucklakeAddFiles
  Procedure.kt:292-297` sets `partition_id = activeSpec` unconditionally; `remapPartitionValuesTo
  PartitionKeyIndex` (`:327-348`) copies whatever hive keys were found (none when
  `hive_partitioning` defaults to `false`, `:100`). Upstream `ducklake_add_data_files.cpp:
  1205-1231` throws on size mismatch (`add_file_partitioned.test`), default `hive_partitioning` is
  AUTOMATIC. Fix: require full coverage or reject.

- [ ] **P-H4 — `add_files` min/max decoded with the wrong physical type / time unit → wrong stats →
  pruning skips matching files.** `toThriftFileMetaData` (`DucklakeAddFilesProcedure.kt:492-533`)
  never calls `meta.setType(...)`, so `columnMeta.getType()` is null in `convertStatValue` (`:91-92`)
  → `decodeDecimalUnscaled` (`:208-217`) takes the big-endian branch for INT32/INT64 decimals (the
  exact corruption its comment warns about). Timestamps decoded as µs while the checker admits
  MILLIS/NANOS (`DucklakeAddFilesTypeChecker.kt:80-87`). INT32→BIGINT/FLOAT→DOUBLE widenings hit
  `BufferUnderflowException` → swallowed → null. Poisons `ducklake_table_column_stats` for DuckDB
  too. No decimal/timestamp test in `TestDucklakeAddFiles`. Fix with W-H1/W-H3 (one stat
  stringifier keyed by parquet physical+logical type).

- [x] **P-H5 — `rewrite_data_files` identifies files by basename only → same-basename files
  collide.** `candidatesByBasename = candidates.associateBy { basename(it.path) }`
  (`DucklakeRewriteDataFilesProcedure.kt:145`, also `:163,179,274`). Hive layouts
  (`region=US/data.parquet`, `region=EU/data.parquet`) or duplicate basenames: `associateBy` keeps
  one → one merged file with both partitions' rows labelled with one partition, only one source
  end-snapshotted → **duplicate rows at latest** + mis-partitioned file. Match on resolved path or
  carry `dataFileId` in the split.
  DONE 2026-09-04: candidates are keyed by the same resolved path carried by the split. Regression
  moves two active files to `branch-0/shared.parquet` and `branch-1/shared.parquet`, rewrites them,
  and verifies both source ids retire and rows are not duplicated.

### Medium

- [ ] **P-M1 — Root-relative scheduled paths resolved against the table path in the orphan
  Catalog 0.6.0 separates scheduled paths correctly, but adoption is blocked by the table-file
  schema-path gap described in P-H1. Fix/adopt together.
  known-set.** `listReferencedFilePaths` (`JdbcDucklakeCatalog.kt:627-632`) mixes
  `ducklake_files_scheduled_for_deletion` (root-relative) into the table-relative list; procedure
  resolves all with `resolveKnown(ref, tableDataPath)` (`:152-155,322-323`). A DuckDB-scheduled
  `main/t/x.parquet` resolves to `.../main/t/main/t/x.parquet` → not known → deleted once mtime >7d,
  bypassing the `schedule_start` grace period. Upstream `:4575-4583` uses `{DATA_PATH} || f.path`.

- [ ] **P-M2 — `cleanup_old_files`: missing file leaves the row stuck forever; all delete
  failures swallowed.** `DucklakeCleanupOldFilesProcedure.kt:98-106` catches every `IOException`,
  WARNs, keeps the row. Hadoop FS `deleteFile` throws `FileNotFoundException` for a missing file
  (verified) → row never removed; S3 native succeeds → inconsistent. `CALL` returns success when
  every delete failed. Upstream `RemoveFiles` throws (`ducklake_cleanup_files.cpp:137-145`). Treat
  not-found as success (remove row); propagate other failures.

- [x] **P-M3 — `.db` in `MANAGED_FILE_EXTENSIONS` can match a DuckDB metadata catalog under the
  data path.** `DucklakeRemoveOrphanFilesProcedure.kt:357` `listOf(".parquet", ".puffin", ".db",
  ".vortex")`. Upstream tests `metadata_in_data_path.test` layout; a `ducklake-*.db` catalog there
  is unreferenced, prefixed, old → sweep deletes the **entire catalog**. `.db/.vortex/.lance` are no
  longer written. Drop them (keep `.parquet`, `.puffin`).
  DONE 2026-09-04 `f121266`: `.db` is never eligible; regression preserves an aged
  `ducklake-metadata.db` while deleting the Parquet control. Legacy `.vortex`/`.lance` residue
  remains reclaimable.

- [ ] **P-M4 — Known-set vs listing compared as raw strings; `add_files` stores the user path
  verbatim.** `isDeletableOrphan :287-288` no scheme/`//`/`.` normalisation; `add_files` registers
  `filePath` as given with `pathIsRelative=false` (`DucklakeAddFilesProcedure.kt:300-302`); upstream
  uses `GetRelativePath`. `file:///w/main/t/ducklake-abc.parquet` vs listed `/w/...` → live file
  deleted after 7d.

- [ ] **P-M5 — Inlined rows never physically removed → unbounded metadata growth.** Flush
  PARTIAL in ducklake-catalog `1121e3e`: the new `flushInlinedDataWithSnapshots` physically deletes flushed rows (upstream shape). The legacy 2-arg `flushInlinedData` the connector still calls keeps end-snapshotting → growth continues until **TR-4** lands.
  end-snapshots (`JdbcDucklakeCatalog.kt:2696,2710-2731`); `expireSnapshots` GCs none (only whole
  dead tables `:743-746`). Upstream deletes flushed rows (`:5105-5124`). Correctness OK; every latest
  read scans a growing table. Add GC of end-snapshotted inlined rows in `expireSnapshots`.

- [ ] **P-M6 — Procedures leave output files behind when the commit fails**
  (`DucklakeRewriteDataFilesProcedure.kt:246-259`, `DucklakeFlushInlinedDataProcedure.kt:191-201`).
  Delete on failure (family with W-M4).

### Low

- [x] **P-L1** — expire: `deleteDeadTableMetadata` (`JdbcDucklakeCatalog.kt:733-750`) doesn't drop
  DONE in ducklake-catalog `f7cbda9` (`ducklake_inlined_delete_<tid>` dropped post-commit) + `e5b3dcf` (`ducklake_tag` GC).
  `ducklake_inlined_delete_<tid>` (upstream `:5013-5015`) nor `ducklake_tag` rows (`:5043`).
- [ ] **P-L2** — expire: `snapshot_ids` + `retention_threshold` both given → ids silently win
  (`DucklakeExpireSnapshotsProcedure.kt:87`); upstream errors. Log at least.
- [ ] **P-L3** — rewrite: cross-engine inlined-file-delete race not caught by
  `assertNoNewerDeleteOnRewriteSources` (checks `ducklake_delete_file` only). Upstream parity, but
  our `changes_made` tokens (E-L9) mean older DuckDB versions won't conflict either.
- [ ] **P-L4** — No `cleanup_all`; dry-run results only in server logs for all three dry-run
  procedures. Return a result set or at least a count.
- [ ] **P-L5** — add_files: `JSON` logical annotation unhandled (`DucklakeAddFilesNameMapper.kt:
  393-440`) → BINARY→VARBINARY → rejected against a `json` column; upstream maps it
  (`ducklake_add_data_files.cpp:776-782`).
- [ ] **P-L6** — remove_orphan: `removeEmptiedDatasetDirectories` (`:203-240`) can delete empty
  schema/table dirs of legitimately-empty tables.

### Nits

- [ ] **P-N1** — Dead code: `DucklakeAddFilesProcedure.kt:55,46` unused imports; `isS3`/
  `stripFileScheme` (`:379-385`); `TopLevelMatch`/`topLevelMatches` (`DucklakeAddFilesNameMapper.kt:
  85,114-119,172`) built, never read.
- [ ] **P-N2** — Orphaned KDoc on `capSourceFiles` (`DucklakeRewriteDataFilesProcedure.kt:
  190-191`).
- [ ] **P-N3** — `parseRetention` ×3 and `resolveTable` ×3 duplicated across procedures.
- [ ] **P-N4** — `NOT_SUPPORTED` used for not-found and FS failures (`RemoveOrphan :140,183,343,
  345`; `Rewrite :433,435`; `Flush :103,141,175,207,209`; `AddFiles :139,142`). Use
  `SCHEMA_NOT_FOUND`/`TABLE_NOT_FOUND`/`GENERIC_INTERNAL_ERROR`.
- [ ] **P-N5** — `DESIGN-maintenance.md:3,53-56` still says "first procedure — the rest is
  roadmap" while §4 marks everything done.

---

## Suggested order of attack

1. Interop breakers that brick DuckDB reads of the catalog/table: **E-C1, E-C2, W-C1, W-C2, E-H1**.
2. Data loss: **P-C1, P-H1, P-H2, P-H5, P-M3**.
3. Silent wrong rows: **R-C1 (+P-H3), R-H1, R-H2/R-H3/T-C3, E-H2, E-H3, W-H1/W-H3/P-H4, R-M3, R-M2**.
4. Fail-loud gates that are cheap and remove whole classes of risk: **E-M2 (encryption), E-M3
   (version), T-H2/W-H4 (reject unsupported types at handle time), E-M6 (stop swallowing)**.
5. Everything else by severity.

---

## Verified correct in this pass (no action)

**Catalog:** visibility predicate `begin <= S AND (end IS NULL OR end > S)` everywhere;
latest/by-id/by-time snapshot lookup; data-file ↔ active delete-file LEFT JOIN;
`row_id_start`/`mapping_id`/`partial_max` read; change-feed window predicates; delete-file
end-snapshot protocol; column tree via `parent_column` with `column_id` as field id and
`list/struct/map` + `element/key/value` tokens; `default_value='NULL'` sentinel; `changes_made`
token spelling/quoting; snapshot row shape and id allocation (except E-H2); `schema_version` bump
set; retry backoff; conflict rules other than E-M1; stats counts, NaN handling, typed min/max
merge; table-stats upsert; flush preserves `row_id_start`; partition transform vocabulary;
inlined table naming and read filters; `expireSnapshots` dead-row logic; path composition with
per-level `path_is_relative`; name mapping rows and `is_partition` split; SQL portability across
PG/MySQL/DuckDB.

**Read:** partial data-file visibility (`_ducklake_internal_snapshot_id <= S`, fail-loud if the
marker is missing); delete-file schema `(file_path, pos[, snapshot])` and `<= S` filtering;
Puffin bare-blob layout + PFA1 container; row id from field id 2147483540 else `row_id_start +
position`; NULL partition value + `__HIVE_DEFAULT_PARTITION__` + URL-encoding; temporal transforms
= DuckDB calendar; bucket `(murmur3 & INT_MAX) % N` for int8–64/DATE/TIMESTAMP(6)/VARCHAR/BLOB;
`footer_size` semantics; add_files top-level name map authoritative; change feed `[start, end]`
inclusive and update pairing by `(snapshot_id, rowid)`; pushdown/row-group pruning disabled whenever
positions matter.

**Write:** `PARQUET:field_id` on every leaf and nested child; 3-level list / `key_value` map;
lineage column name + field id 2147483540 appended last and excluded from stats; stats leaf order;
`value_count`/`column_size_bytes` semantics; decimal stat byte order; `record_count`/`file_size_
bytes`/`footer_size`; relative paths under the table path; `partition_id` + `partition_key_index`;
hive path escaping byte-identical to `URLEncode(encode_slash=true)`; delete file union of prior
parquet/puffin/legacy-global files with file-local positions; `delete_count` delta; Puffin blob
layout; transform strings; unsigned range validation; lazy writer open; MERGE update pairing.

**Types/DDL:** emitted `column_type` strings all in upstream vocabulary; `decimal(p,s)`;
`initial_default` application semantics and DuckDB `Value::ToString` parsing for common types;
sort `ASC/DESC` + `NULLS_FIRST/LAST`; `renameTable` end-snapshot + re-insert; `addField`
struct-only + duplicate check; `dropField` full-subtree cascade; integer/float promotions a strict
subset of upstream; TIMESTAMP(0/3/9) parquet encodings readable by DuckDB.

**Procedures:** expire never touches the latest snapshot (checked twice), faithful port of
`DeleteSnapshots`, schedule rows absolute, dry-run pure, one JDBC tx; cleanup `schedule_start <
cutoff`, root-relative resolution, shared file-id sequence; orphan 7-day floor, mtime gate,
recursive listing, known set at all snapshots for in-scope tables; rewrite (non-partial) begin =
new snapshot, sources + delete files end-snapshotted in-tx, stale-read abort; rewrite (partial)
`begin/partial_max` per `GetCompactionChanges`; flush preserves ids via lineage +
`flushRowIdStart`, `flushed_inlined` token accepted upstream; add_files case-insensitive nested
name mapping, hive precedence, `map_by_name` dedup, type rules equal-or-stricter than upstream.
