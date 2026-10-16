# DuckLake write path — open items

The shipped surface is documented in [README.md](../README.md). Completed implementation history
is archived at [`archive/TODO-WRITE-MODE-2026-07.md`](archive/TODO-WRITE-MODE-2026-07.md); review
findings with stable IDs remain in [REVIEW-2026-09-05.md](REVIEW-2026-09-05.md).

## DuckDB-as-Catalog Backend (Local + Quack)

- [ ] Document and qualify native/direct-Java Quack catalog deployment in this connector.
- [ ] Add cross-engine and concurrent-writer Quack coverage with exact driver/server versions.
- [ ] Add a bounded CI lane after the transport and authentication contract is stable.

## `add_files` Follow-Ups

- [ ] Honor `allow_missing` recursively for missing STRUCT children while retaining fail-loud type
  validation and existing NULL projection semantics.

## Write correctness

- [ ] Ensure every writable Trino type has a valid Parquet schema/value writer; reject unsupported
  types before side effects.
- [ ] Apply DuckLake sort specs consistently to every supported output path or explicitly reject
  paths that cannot honor the spec.
- [ ] Align configurable writer settings (compression, row groups, rollover, inlining, deletion
  vectors) with catalog scope/precedence where semantics are qualified.
- [ ] Declare/enforce the connector's single-statement write behavior until real multi-statement
  transactions exist.
- [ ] Make failed-commit output cleanup safe under the catalog's uncertain/acknowledged commit
  outcome contract.

## Maintenance Operations

- [ ] Return structured result counts if a suitable Trino API is adopted. Current procedures are
  implemented and log outcomes; see [DESIGN-maintenance.md](DESIGN-maintenance.md).

## Catalog Namespace Hygiene

Non-Parquet format namespaces moved to `brikk/duckbridge`. In this connector, audit only metadata
tokens that it still writes: Trino view tags/dialect, snapshot `changes_made`, delete-file formats,
and connector-owned options. Never invent upstream conflict vocabulary.

## DuckLake 1.1 writes

- [ ] Keep all `1.1-dev*` writes disabled. Enable final 1.1 writes only after schema migration,
  hierarchy, exactness, row-group metadata, ABI/wire, rollback, and mixed-version gates pass.
