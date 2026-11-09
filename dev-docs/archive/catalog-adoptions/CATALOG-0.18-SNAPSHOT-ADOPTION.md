# Catalog 0.18 snapshot adoption

> Normal and `-Pr07CatalogCandidate` builds consume published snapshot
> `0.18.0-20261109.223927-4` (source `a4332aab275f7bbdbfbf3be17a296cb7fa4fbffb`) until formal
> 0.18.0. Consumer baseline `d799179`, Trino 483 / JDK 25.

## Coordinates

Repository `https://central.sonatype.com/repository/maven-snapshots/`, exclusive for the three
modules; receipt `gradle/catalog-release.sha256`, checked by `verifyCatalogRelease`.

| Artifact | SHA-256 |
| --- | --- |
| `dev.brikk.ducklake:ducklake-catalog:0.18.0-20261109.223927-4` | `33a6c410e21d5c54c0d2a7cf24155876b10d97c113f2ead8b1101b4488e47c4b` |
| `…:ducklake-catalog:0.18.0-20261109.223927-4:test-fixtures` | `dd8eea0415dfa9a0c7f02ed270e4aa66d87c329d03469579f91c2d5361f0a9bd` |
| `dev.brikk.ducklake:ducklake-test-corpus-replay:0.18.0-20261109.223927-4` | `72146c7e5c5c10e6a4f828d76e6507e92eacb3f1db21c8d8072375fe67a2577f` |
| `dev.brikk.ducklake:slt-format:0.18.0-20261109.223927-4` | `d48d254b5b422e44e64b21e90902e53f5fd372f0df9dc2da8633985776c5a3d1` |

`0.18.0-20261109.212506-3` lacks the replay change and is not accepted.

## What changed

- Replay: in a 1.0-only execution scope, an otherwise-failing oracle error with DuckDB's
  `DuckLake 1.0 does not support <feature> - attach with AUTOMATIC_MIGRATION set to TRUE`
  records one `format-out-of-scope:` skip and halts the file.
- Catalog: internal `Final11*` classes renamed `Spec11*`; ~45 exception messages reworded
  (`is not qualified` -> `is not supported`); `timestamp_us` statistics now prune. No public API
  change.
- Consumer: no source or API changes needed; no new compile errors or catalog deprecations.
  Added `TestTrinoFormatScopes.implicit11RefusalHaltsTheFileAfterItsExpectedRefusalPassesInThe10Lane`
  (fails on 0.17.0).

## Corpus, per directory

Records pass/fail/skip; mirrors attempted/passed/failed/skipped.

| Dir | Ran before -> after | Records before | Records after | Format halts | Mirrors before | Mirrors after |
| --- | --- | --- | --- | --- | --- | --- |
| partitioning | 33 -> 28 | 723/88/1 | 640/40/5 | 0 -> 5 | 123/82/40/122 | 106/66/40/117 |
| general | 16 -> 16 | 265/0/39 | 265/0/39 | 1 -> 1 | 47/8/0/74 | 47/8/0/74 |
| types | 13 -> 11 | 140/68/5 | 126/5/7 | 0 -> 2 | 25/18/2/28 | 25/18/2/28 |
| merge | 5 -> 5 | 57/0/0 | 57/0/0 | 0 -> 0 | 4/4/0/8 | 4/4/0/8 |
| add_files | 59 -> 57 | 955/47/4 | 946/27/6 | 0 -> 2 | 94/70/20/152 | 94/70/20/152 |
| macros | 18 -> 15 | 234/20/0 | 226/0/3 | 0 -> 3 | 0/0/0/68 | 0/0/0/68 |
| data_inlining | 74 -> 72 | 1142/74/9 | 1123/54/11 | 0 -> 2 | 125/112/4/263 | 124/111/4/258 |
| alter | 52 -> 51 | 981/61/1 | 975/10/2 | 0 -> 1 | 79/78/0/99 | 79/78/0/99 |
| geo | 12 -> 11 | 123/14/0 | 117/0/1 | 0 -> 1 | 13/13/0/29 | 13/13/0/28 |

- Exactly the 16 files of the catalog oracle audit halt (CRUTCHES "Corpus replay runs DuckLake 1.0
  only"); `epoch_version_gate.test:25` (expected refusal) passes, the halt is at `:43`.
- Partitioning records are 680 oracle passes, 0 oracle failures, 5 skips; the 40 failures are the
  unchanged mirror failures (`'nan'` casts, promoted-column reads).
- Outside the halted files, the 136 failure locations are identical. Message differences: which
  file a multi-file read fails on first (`bucket_pruning_type_promotion`, `add_files_timetz`,
  `add_files_retyped_bounds`) and the reworded `Final-1.1 inline type geometry is not supported`.
- Mirror deltas are post-refusal mirrors in halted files (partitioning: 17 attempted, 16 passed,
  5 skipped; data_inlining: 1 passed, 5 skipped; geo: 1 skipped).
- Halted files are excluded from ran files; `macros` still fails the one-mirror minimum (0 mirrors
  before and after).

## Other verification

| Check | Result |
| --- | --- |
| Full unit suite, 483 / JDK 25 (starter corpus) | 1,216 pass, 0 fail, 0 skip |
| 484 `compileKotlin compileTestKotlin` | pass |
| detekt (483, 484, `-Pr07CatalogCandidate`) | clean |
| `-Pr07CatalogCandidate` test, before / after | 1,249 / 1,250 pass, 40 / 40 fail; same 40 tests, same exceptions; +1 new test |
