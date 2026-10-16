# DuckLake read path — open items

The shipped surface is documented in [README.md](../README.md). Completed implementation history
is archived at [`archive/TODO-READ-MODE-2026-07.md`](archive/TODO-READ-MODE-2026-07.md); review
findings with stable IDs remain in [REVIEW-2026-09-05.md](REVIEW-2026-09-05.md).

## Correctness

- [ ] **Fail loud on present-but-unparseable catalog values.** Partition values and initial
  defaults must not silently become SQL NULL.
- [ ] **Finish legacy/unsigned/type edge qualification.** Revalidate unsigned Parquet decoding,
  legacy files without field IDs/name maps, unsupported physical encodings, and final DuckLake
  1.1 type/statistics semantics before expanding support claims.
- [ ] **Resolve full-corpus `issue_852_schema_cache_growth` divergence.** The nightly mirror
  currently reports two `expected [true], got [false]` records; do not skip-list without a proven
  engine/harness cause.

## Puffin Deletion Vector Reads

- [ ] Keep Roaring/primitive membership through delete filtering instead of expanding dense
  vectors into copied boxed `HashSet<Long>` collections. Profile concurrent dense-vector scans
  and enforce a reasonable resource bound.

## Type-Support Improvements

- [ ] Variant/shredding: retain read-only safety for `1.1-dev*`; choose a Trino representation and
  exactness/pruning policy only against final 1.1 metadata.
- [ ] Geometry: evaluate a native Trino spatial mapping and bounding-box pruning; current values
  remain losslessly exposed as VARBINARY.
- [ ] Interval and `uint128`: keep lossless VARCHAR mappings unless Trino gains types that can
  represent the complete DuckLake domains.

## R6: Change Feed and Extended Metadata Parity

- [ ] Bound change-feed startup memory/time without losing cross-window update pre/post pairing.
- [ ] Add metadata surfaces only for demonstrated use cases; avoid mirroring catalog tables merely
  for API breadth.

## R7: Cross-Backend View Tests

- [ ] Extend view coverage when another supported catalog backend is added; PostgreSQL is covered.

## Robustness / Performance Follow-ups

- [ ] Fetch file statistics for all predicate columns in one bounded catalog query per plan rather
  than repeating an active-file scan per column; unknown stats must continue to keep files.
