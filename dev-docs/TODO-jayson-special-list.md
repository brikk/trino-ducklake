# Strategic / cross-cutting backlog

This is the short list for work that does not fit a read/write feature checkbox. The original
2026 completeness sweep, including shipped items and detailed evidence, is archived at
[`archive/TODO-jayson-special-list-COMPLETED-2026-07.md`](archive/TODO-jayson-special-list-COMPLETED-2026-07.md).
Data-plane DuckDB, Vortex, Lance, indexing, and pushdown work moved to
[`brikk/duckbridge`](https://github.com/brikk/duckbridge) and is not tracked here.

## Active

- [ ] **Decompose connector monoliths.** Split `DucklakeMetadata` and
  `DucklakePageSourceProvider` by concern without changing Trino SPI or serialized handle shapes.
  Good extraction boundaries are metadata tables, change-feed planning, inlined reads, Parquet
  page-source construction, and statistics.
- [ ] **Finish remote DuckDB/Quack catalog qualification.** Keep local DuckDB, native Quack, and
  direct Java Quack concerns in the catalog plane. Remaining connector work is deployment
  documentation, cross-engine/concurrent-writer coverage, and CI; see
  [TODO-WRITE-MODE.md](TODO-WRITE-MODE.md#duckdb-as-catalog-backend-local--quack) and
  [CATALOG-BACKENDS.md](CATALOG-BACKENDS.md).
- [ ] **DuckLake 1.1 transition.** Keep `1.1-dev*` read-only. Requalify the final metadata layouts,
  type/statistics semantics, migration path, and mixed-version behavior when upstream changes the
  version to final `1.1`; do not infer write support from a version-string change.

## Opportunistic hygiene

- [ ] **Kotlin cleanup where wire/SPI constraints do not dictate Java shapes.** Prefer data classes,
  default arguments, and delegation for internal types. Preserve Trino SPI signatures,
  `@JvmRecord` Jackson DTOs, and Airlift configuration conventions.
