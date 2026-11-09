# Maintenance operations — current contract

**Status:** implemented. Historical design and implementation notes are archived at
[`archive/designs/DESIGN-maintenance-implementation-history.md`](archive/designs/DESIGN-maintenance-implementation-history.md).

## Safety model

- Files are immutable. Catalog commits retire metadata; they do not modify data files in place.
- File visibility is the half-open snapshot interval `[begin_snapshot, end_snapshot)`.
- Snapshot expiration schedules files; `cleanup_old_files` performs later physical reclamation.
- The catalog commits snapshot removal before paged catalog cleanup. A
  `DucklakeExpiryCleanupIncompleteException` means the history is already expired;
  resume with the catalog's `cleanUpExpiredObjects`, not the same expiry IDs.
  Physical `cleanup_old_files` is not that catalog-GC resume operation.
- Physical deletion is retention-gated and checks retained owners before deleting or acknowledging
  a selected queue entry. Exact guarantees and accepted races are documented in
  [DESIGN-shared-file-ownership.md](DESIGN-shared-file-ownership.md) and
  [CRUTCHES-AND-SHORTCUTS.md](CRUTCHES-AND-SHORTCUTS.md).
- `cleanup_old_files` reads the catalog in bounded pages under a per-call budget. Do not replace
  that with unbounded materialization. `remove_orphan_files` and `flush_inlined_data` still load
  a whole catalog result; see CRUTCHES "Unbounded maintenance catalog reads".

## Shipped surfaces

| Surface | Scope | Contract |
|---|---|---|
| `ANALYZE` | table | Rebuild catalog statistics without changing table data |
| `remove_orphan_files` | table/schema/catalog | Remove old unreferenced DuckLake-written residue; at most 10,000 files per call; dry-run supported |
| `expire_snapshots` | catalog | Remove selected history and schedule newly dead files; never expires latest |
| `cleanup_old_files` | catalog | Process one bounded scheduled-deletion batch after retention |
| `flush_inlined_data` | table/schema/catalog | Move eligible inlined rows to Parquet while preserving identity/count; refuses a table above `ducklake.read.max-physical-rows` live inlined rows |
| `rewrite_data_files` | table | Compact through the real read path; supports ordinary and partial-emitting shapes |

The public SQL signatures and current defaults live in the [maintenance reference](../REFERENCE.md#maintenance-procedures). This document
records invariants, not a second copy of every argument.

## Important boundaries

- The connector reads/writes Parquet data files; Puffin is supported for deletion vectors.
- `remove_orphan_files` must not delete arbitrary foreign files placed under a data path.
- Concurrent cleaners/importers across independent engines have no universal lock. Ambiguous path
  identity fails closed; simultaneous import/cleanup remains an accepted limitation.
- A future sub-file split or row-group-pruning path must preserve authoritative physical row
  positions before enabling mutation scans.
- Procedure result tables are not implemented; current calls log counts. The 483 SPI uses
  void handles; 484 has optional procedure metrics support, which this connector has not adopted.

Open maintenance work is in [TODO.md](TODO.md).
