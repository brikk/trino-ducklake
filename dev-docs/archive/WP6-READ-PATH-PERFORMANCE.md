# WP6 read-path performance and verification

Base: `090ad97` (WP1–WP5). Only WP6 is implemented; no catalog library or vendored source changes.

## Measurement

`TestDucklakeReadPathPerformance.millionRowsWithThirtyPercentDeletesAndThreeOriginSnapshots`
creates one real, uncompressed Parquet file in Trino's `MemoryFileSystem`: 1,000,000 BIGINT rows,
embedded lineage, and origins distributed deterministically across snapshots 2, 3 and 4. One
DuckLake Puffin delete file deletes positions where `position % 10 < 3` at snapshot 5 (300,000).
The change-feed source scan covers all four snapshots and asserts their exact event counts.
The ordinary Parquet scan asserts 700,000 survivors and their checksum. Decode/filter is also
measured separately. This measures the connector source pipeline, not SQL planning, network or
physical-disk latency.

Same host and JVM settings before/after: OpenJDK 25.0.2, Trino 483, catalog 0.17.0, G1,
`-Xms3g -Xmx3g -XX:G1HeapRegionSize=32M`, and the repository's other unchanged test JVM flags.
Three warmups, then five measured runs; medians below. Allocation is caller-thread Java heap
allocation from `com.sun.management.ThreadMXBean`, including correctness checks but excluding
fixture creation and warmups. It does not measure native/off-heap allocation or peak live heap.
There are no disabled tests or timing assertions.

| Operation | Before ms | After ms | Before allocated bytes | After allocated bytes | Allocation reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| Puffin decode + 1M-row filter | 18.922307 | 11.164855 | 63,253,840 | 4,972,936 | 92.1% |
| Parquet scan + deletes | 21.516033 | 12.610240 | 112,346,536 | 24,327,296 | 78.3% |
| CDC source, snapshots 2–5 | 229.502588 | 148.754629 | 939,273,920 | 357,896,256 | 61.9% |

The isolated before/after runs selected this timed method plus the unchanged three converter
suites. Commands (the baseline selected the whole performance class, which then had only this
one method):

```sh
mise exec -- ./gradlew test \
  --tests '*TestDucklakeReadPathPerformance.millionRowsWithThirtyPercentDeletesAndThreeOriginSnapshots' \
  --tests '*TestDucklakeInlinedValueConverter' \
  --tests '*TestDucklakePartitionValueParser' \
  --tests '*TestDucklakeDecodedValueConverter'
```

Local raw receipts: `/tmp/opencode/wp6-before-report.html`,
`/tmp/opencode/wp6-after-report.html`. The fixture remains in the normal test suite.

## Converter decisions

All three entry points use `DucklakeTimestampValues`; decoded containers, decimal narrowing
checks, TIME and TIMETZ validation retain their distinct validated-input responsibilities.
The original `TestDucklakeInlinedValueConverter`, `TestDucklakePartitionValueParser` and
`TestDucklakeDecodedValueConverter` files are unchanged and pass together.

- **Offset-bearing TIMESTAMP without time zone:** DuckDB/catalog wall-clock semantics win for
  partition/catalog-decoded text. The old partition parser rejected offsets, while the legacy
  JDBC converter normalized them to UTC. Catalog 0.17.0 (source tag `v0.17.0`),
  `InlinedTemporalValues.localTimestampText`, calls `timestamp(splitOffset(...).first)`; it
  discards the offset, not the wall clock. `TestDucklakeTimestampValues` also checks against
  actual DuckDB `epoch_us(TIMESTAMP '...')`, including pre-epoch dates. The conflicting existing
  JDBC test requires instant normalization: that compatibility behavior remains explicit via
  `jdbcOffsetAsInstant=true`, not silently imposed on catalog text. Changing that legacy
  contract is deliberately left for separate approval; it is recorded in CRUTCHES.
- **TIMESTAMPTZ:** the inlined converter's wider hour-only/compact/colon offset support wins
  over the partition parser's narrower syntax. DuckDB accepts all three, and catalog 0.17.0's
  `InlinedTemporalValues.splitOffset` accepts them too. Instant normalization to UTC, a UTC
  default for internal zoneless input, nanosecond fractions and pre-epoch floor division are
  shared. Tests compare native DuckDB for offset spellings and pin typed/numeric negative epochs.
- Numeric JDBC timestamps remain epoch microseconds. Short/long Trino representations and
  existing precision behavior are retained; overflowing epoch arithmetic now fails rather than wraps.

## Delete membership, footers and accounting

- `DucklakeLongSet` uses the existing Roaring dependency's `Roaring64NavigableMap`; low-word
  file-local positions remain Roaring32 containers, and high words/global IDs are not narrowed.
  No dependency was added. The resolved 483 runtime selects RoaringBitmap 1.6.15 via its BOM.
  Primitive membership/iteration and bitmap union/difference reach both row filters and CDC
  planning. The Set view is only an API/catalog compatibility boundary. Split/catalog wire
  sets and write-side sorted delete output are not redesigned in this read-path package.
- Puffin's payload is the portable Roaring64 representation. The vendored
  `ducklake/src/storage/ducklake_deletion_vector.cpp` uses `Roaring64Map::readSafe`/`write` and
  checks the serialized size. Duplicate high-word keys now reject as corrupt rather than let
  the Java library silently overwrite tombstones. Cumulative snapshot vectors are differenced
  as bitmaps, retaining the earliest deletion event without expanding membership into boxed sets.
- `DucklakeChangeFeedFiles` is query-local, shared by insert/delete/inline-file-delete planning
  and lazy data-file opens. It reads both identity columns in one primitive-array pass and
  reuses the same Parquet footer. Cached opens skip footer-hint prefetch as well as footer parse.
  A regression counts exactly one tail read across three insert units and a delete unit with
  a real nonzero footer hint; a fresh query must read it again. No cross-query cache or filesystem
  sharing assumption is introduced.
- Bitmap retained-size estimates include container/map overhead. Delete transforms and CDC
  report their own retained state plus delegate buffers; CDC releases its units/cache on close
  or exhaustion. Trino 484's default `ConnectorPageSourceProvider` overload wraps this in
  `MemoryUsageReportingPageSource`, forwarding these corrected values to its per-source
  `MemoryContext`. The provider-shared column-cache context remains separate. Shared lineage
  may be conservatively counted twice. No new hard memory/concurrency bound is claimed.
- `pinReadSession()` names the formerly discarded `catalog.currentSnapshotId` access. In the
  published catalog 0.17.0, `readSession` owns a repeatable-read transaction and nested sessions
  reuse it. Resolving `dsl` validates the stored version/layout once in that view; the first
  catalog read establishes the database snapshot. The call does not store a snapshot ID for
  the whole distributed query and is not a retention lease. Its comment states this side effect.
  `getSnapshotTimestampFromVersion` already returns a used value and was made internal in WP4;
  there is no remaining side-effect-only call to rename there.

## Verification

- Full default 483 unit/integration suite: **1,141 tests in 140 suites, 0 failures, 0 errors,
  0 skipped**. Includes the ordinary starter corpus selection, not the full corpus.
- Separate `general` and `time_travel` corpus runs: both pass. Time travel: 7 files,
  179 records passed, 55 oracle/golden query passes, 10 mirror passes, 0 mirror failures.
  Existing dialect/transaction/view skips remain; no skip list or classification changed.
- `compileTestKotlin -PtrinoVersion=484`: passes on JDK 25.
- Detekt on both targets: clean, no baseline additions or new suppressions.
- Targeted CDC, partial-file/time-travel, Puffin, overlap/vocabulary and converter tests pass.
  Regression coverage includes the full long position domain, compressed retained memory,
  duplicate high words, native timestamp semantics, one-footer reuse and memory release.

Intentionally not done: later work packages; upstream-red `merge`/`partitioning`; final-1.1
write enablement; streaming lineage/origin arrays, concurrency/global retained-state budgets
and bounded CDC startup. These are existing separate obligations, still recorded in TODO and
CRUTCHES; bitmap compression and accounting do not fulfill them. Nothing is pushed.
