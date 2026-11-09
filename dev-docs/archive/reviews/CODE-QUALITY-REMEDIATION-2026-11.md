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
