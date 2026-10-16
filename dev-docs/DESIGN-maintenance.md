# Maintenance operations — current contract

**Status:** implemented. Historical design and implementation notes are archived at
[`archive/DESIGN-maintenance-implementation-history.md`](archive/DESIGN-maintenance-implementation-history.md).

## Safety model

- Files are immutable. Catalog commits retire metadata; they do not modify data files in place.
- File visibility is the half-open snapshot interval `[begin_snapshot, end_snapshot)`.
- Snapshot expiration schedules files; `cleanup_old_files` performs later physical reclamation.
- Physical deletion is retention-gated and checks retained owners before deleting or acknowledging
  a selected queue entry. Exact guarantees and accepted races are documented in
  [DESIGN-shared-file-ownership.md](DESIGN-shared-file-ownership.md) and
  [CRUTCHES-AND-SHORTCUTS.md](CRUTCHES-AND-SHORTCUTS.md).
- Bulk catalog reads used by cleanup are paged and budgeted. Do not replace them with unbounded
  materialization.

## Shipped surfaces

| Surface | Scope | Contract |
|---|---|---|
| `ANALYZE` | table | Rebuild catalog statistics without changing table data |
| `remove_orphan_files` | table/schema/catalog | Remove old unreferenced DuckLake-written residue; dry-run supported |
| `expire_snapshots` | catalog | Remove selected history and schedule newly dead files; never expires latest |
| `cleanup_old_files` | catalog | Process one bounded scheduled-deletion batch after retention |
| `flush_inlined_data` | table/schema/catalog | Move eligible inlined rows to Parquet while preserving identity/count |
| `rewrite_data_files` | table | Compact through the real read path; supports ordinary and partial-emitting shapes |

The public SQL signatures and current defaults live in the README procedure table. This document
records invariants, not a second copy of every argument.

## Important boundaries

- The connector reads/writes Parquet data files; Puffin is supported for deletion vectors.
- `remove_orphan_files` must not delete arbitrary foreign files placed under a data path.
- Concurrent cleaners/importers across independent engines have no universal lock. Ambiguous path
  identity fails closed; simultaneous import/cleanup remains an accepted limitation.
- A future sub-file split or row-group-pruning path must preserve authoritative physical row
  positions before enabling mutation scans.
- Procedure result tables are not implemented; Trino's `Procedure` SPI is void and current calls
  log counts.

## Open follow-ups

- Return structured maintenance results if a suitable Trino surface is adopted.
- Treat missing scheduled files as successful reclamation while propagating other filesystem
  failures consistently across backends.
- Clean up procedure output files after failed catalog commits without deleting files from an
  uncertain/acknowledged commit outcome.
