# Code-quality remediation, November 2026

Eleven work packages following the 2026-11-06 code review. Open follow-ups are in
[TODO.md](../../TODO.md); accepted restrictions are in
[CRUTCHES-AND-SHORTCUTS.md](../../CRUTCHES-AND-SHORTCUTS.md).

| Package | Commit |
| --- | --- |
| WP1 Corpus mirror errors classified by Trino error code; `NOT_SUPPORTED` kept for deliberate gates | `d92f048` |
| WP2 Fail loudly on footer, projection-default and merge-fragment errors; tagged merge fragments | `970e24e`, `18f4780` |
| WP3 Catalog-stats file pruning driven by the unenforced predicate, with typed bounds | `bca49a1` |
| WP4 Dead code, AI scratch files and nullable test seams removed | `68ab007` |
| WP5 Shared procedure support; bounded orphan removal and flush; aborted Parquet files deleted | `090ad97` |
| WP6 Roaring delete membership; one Parquet footer read per CDC file per query | `8a51f6c` |
| WP7 Failed CTAS drops its table on rollback; `CREATE OR REPLACE VIEW` in one snapshot | `1e7ce7a` |
| WP8 Shared test catalog fixture; replay error classification unit tests | `8b16264` |
| WP9 `DucklakeMetadata` and `DucklakePageSourceProvider` split; read strategy chosen once | `b1f6e62` |
| WP10 Comment sweep | `0511be5` |
| WP11 Developer documentation reorganized | this record's commit |

## WP6 read-path measurements

`TestDucklakeReadPathPerformance.millionRowsWithThirtyPercentDeletesAndThreeOriginSnapshots`:
one uncompressed Parquet file of 1,000,000 BIGINT rows with embedded lineage, origins
spread over snapshots 2-4, and one Puffin delete file removing 300,000 positions at
snapshot 5, read from Trino's in-memory filesystem. OpenJDK 25.0.2, Trino 483, catalog
0.17.0, G1 with a 3 GiB heap; median of five runs after three warmups. Allocation is
caller-thread Java heap allocation (`ThreadMXBean`), not off-heap or peak live heap.

| Operation | Before ms | After ms | Before allocated bytes | After allocated bytes | Allocation reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| Puffin decode + 1M-row filter | 18.9 | 11.2 | 63,253,840 | 4,972,936 | 92.1% |
| Parquet scan + deletes | 21.5 | 12.6 | 112,346,536 | 24,327,296 | 78.3% |
| CDC source, snapshots 2-5 | 229.5 | 148.8 | 939,273,920 | 357,896,256 | 61.9% |

## Follow-up closure

- Replay literal-cast skips require the exact operand, side and VARCHAR length in the executed
  Trino AST at the server's error location. CHAR, casts, bounded expressions, unrelated literals
  and unprovable locations fail. `general` and `types` have zero skip-to-fail changes:
  `general` retains 8 mirror passes, 0 mirror failures and 74 mirror skips; `types` retains
  18 mirror passes, 0 mirror failures and 30 mirror skips. Its 66 existing oracle/setup failures
  remain visible and are tracked in TODO.
- Partial-file change-feed windows now assert all IDs exactly (`0..99`, `100..199`), separately
  from origin snapshots/data. S3 schema evolution asserts exactly three two-field rows after
  DROP COLUMN. Both strengthened checks pass.
- Delete readers share owned-source cleanup; Parquet construction closes untransferred sources
  in `finally`; merge writers use `use`. Fault injection covers all seven delete-reader entry
  points, Parquet construction, merge finish/write, bitmap corruption, multi-writer abort and
  multi-table rollback, including original classifications and secondary close failures.
- `toLocation` and split enumeration are shared. Read strategies rely on catalog format
  admission, not a second version-string check; the released-format test also asserts the real
  catalog's `DucklakeCatalogFormatNotAdmittedException` before connector planning.
- Detekt baseline: 42 entries before, 37 after; generic-catch entries: 8 before, 3 after.
  Retained entries:
  - `DucklakePageSink.abort`: an unexpected writer failure must not stop other aborts/output deletion.
  - `DucklakeWriteMetadata.dropUnpublishedCreatedTables`: unexpected catalog/driver failures must not stop other rollbacks.
  - `DucklakeStatsExtractor.convertStatValue` (outside this follow-up): undecodable optional bounds become unknown, retaining files.
- Verification: Trino 483 full suite with `general`: 1,177 tests, zero failures/errors/skips;
  separate `types` comparison as above; Trino 484 test compile and 22 classifier/output-lifecycle
  tests pass; detekt passes on both targets. Output tracking uses each target's actual output-stream
  API through test-only adapters.
