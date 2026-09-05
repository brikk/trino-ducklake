# DESIGN — nested struct field evolution on non-parquet reads (step 2)

**Status:** archived historical design. The non-Parquet DuckDB-engine path moved out of this repo.
The remaining Parquet path now resolves nested fields by catalog column identity (field ID first,
then era-name fallback) and builds ROW types from the case-preserving catalog tree. Nested
field-ID-less add_files mappings remain tracked as R-H5 in `TODO-rectify-from-eval.md`.

## Problem

`ALTER TABLE t ADD COLUMN s.child <type>` / `DROP COLUMN s.child` mutate a struct column `s`. After
the change, OLD data files were written with the *old* struct shape. Reading them must reconcile the
file's shape against the table's *current* shape.

- **Parquet (current):** `DucklakeParquetTypeUtils` recursively binds children by catalog field ID,
  then era/current name only when the identity existed. Missing children NULL-fill; nested rename
  and drop/readd are regression-tested. (The original name-first implementation described here was
  not correct.)
- **Non-parquet (duckdb/vortex/lance via the DuckDB engine): broken.** The read path projects struct
  columns *as-is* and binds Arrow struct children *positionally*:
  - ADD FIELD → `DucklakeArrowToPageConverter.appendRowEntry` throws `NOT_SUPPORTED` when the old
    file's Arrow struct has fewer children than the current ROW type.
  - DROP FIELD → no guard fires (file has *more* children); positional `getChildByOrdinal` misbinds
    every subfield after the dropped one → **silent wrong results** (only a trailing drop is safe).

T2-A fixed exactly this class of problem for **top-level** columns: it resolves each file's column
names at the file's `begin_snapshot` and, in `DuckDbSelectSqlBuilder`, aliases renames and projects
`CAST(NULL AS type)` for columns added later. It stops at top level — `resolveFileColumnNames` filters
`parentColumn == null` and keeps only `columnId → name` (drops nested structure and types entirely).

## Why non-parquet has no field-id

DuckDB/Arrow structs are matched by **name + position**, not a persisted field-id. So per-file
reconciliation must, like T2-A, map by the catalog's stable `column_id` ↔ the field **name that
column_id had at the file's `begin_snapshot`**. The catalog can already produce the file-era nested
shape: `JdbcDucklakeCatalog.getTableColumns(tableId, fileBeginSnapshot)` +
`resolveColumnType` recursively rebuild `struct<...>` from the child rows active at that snapshot.
T2-A simply never plumbs the nested part through.

## Approach

Normalize each struct to the **current** shape inside the SELECT, so everything downstream (the Arrow
converter) keeps seeing the current type and stays positional.

1. **Per-file nested resolution.** Extend the per-file resolution (`resolveFileColumnNames` /
   `DucklakePageSourceProvider`) to carry, for each struct, the per-`column_id` field info active at
   the file's `begin_snapshot`: `{ columnId → (fileFieldName, fileType) }`, recursively. Source it
   from `getAllColumnsWithParentage(tableId, fileBeginSnapshot)` (already exists) rather than the
   top-level-only `getTableColumns` filter.

2. **Struct-reshaping projection in `DuckDbSelectSqlBuilder`.** For a struct column whose file shape
   differs from the current type, emit a `struct_pack` that builds the current struct from the file's
   columns, keyed by `column_id`:
   - subfield present in the file → `currentName := fileExpr.fileName`
   - subfield added after the file → `currentName := CAST(NULL AS <type>)`
   - subfield dropped → omit it (it isn't in the current type)
   - renamed/reordered → handled for free (we map by `column_id`, emit current names in current order)
   - recurse for struct-of-struct.
   Example (file has `s{a,b}`, current is `s{a, c}` — `b` dropped, `c` added):
   `struct_pack(a := s.a, c := CAST(NULL AS INTEGER)) AS s`.

3. **Converter.** Once the SELECT yields the current shape, `appendRowEntry`'s count guard +
   positional binding are correct as-is. Keep the guard as a backstop (it now means "bug", not
   "evolution"). No converter change anticipated; confirm with tests.

## Scope / non-goals

- Structs only, including struct-in-struct. **Rows inside arrays/maps are out** — Trino's own
  `addField` carries `// TODO add support for rows inside arrays and maps`, so the engine won't send
  them. Gate that case if it ever arrives.
- `setFieldType` (nested SET TYPE) and `renameField` stay out (SET TYPE is deferred repo-wide; rename
  has the same coupling and wasn't requested).

## Test plan (step 2)

Per format (duckdb / vortex / lance), an `AbstractDucklakeNestedFieldEvolutionFormatTest`:
- write file A (old struct) → ADD FIELD → write file B (new struct) → SELECT reads A with the new
  subfield NULL-filled and B valued; struct-of-struct; ADD in the middle (reorder); DROP a
  non-trailing field reads correctly; time-travel to before the change still reads A by its old shape;
  DELETE/UPDATE interplay. Lance/vortex go through the same arrow-scan path.
- Remove the step-1 non-parquet gate and its gate test in the same change.

## Risks

- `struct_pack` / nested `CAST(NULL AS STRUCT(...))` semantics across the duckdb-native vs vortex/lance
  arrow-scan read modes — verify both.
- Deeply nested reshaping cost (string-built SQL) — bounded by schema depth, not row count.
- Interaction with the existing top-level T2-A projection — the reshape must compose with rename/add
  at top level (same `column_id` keying makes this consistent).
