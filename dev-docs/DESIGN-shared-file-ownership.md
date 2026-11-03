# Shared physical-file ownership

## Current practical scope

`add_files` registers existing objects in place and can create shared ownership.
Our scheduled cleaner checks retained owners before deletion. This protects the
ordinary sequential A/B ownership case; it is not a universal concurrent ownership
protocol. Permanent decisions and original evidence are retained in
[the RV-04 record](archive/DESIGN-shared-file-ownership-2026-09.md) and
[the live review](REVIEW-2026-09-05.md#rv-04-scheduled-cleanup-is-not-safe-for-shared-physical-ownership).

## Required limits

| Limit | Default / maximum |
| --- | --- |
| Selected requests/reference page size | 256 / 1024 |
| Combined retained data/delete references | 1,000,000 / 10,000,000 |
| Path/root text | 16,384 characters, with bounded overflow sentinels |
| Queue mutation predicates per chunk | 64; selected requests remain capped at 1024 |

Bound work through SQL-limited pages and per-call budgets, not a whole-catalog
coordinator collection. A lookahead row distinguishes exact exhaustion from overflow.
These limits do not bound database scan/sort time or the physical members of a
legacy directory deletion.

## Plan before mutation

Within an owned `catalog.readSession`, select one candidate batch, resolve the
bounded root, and drain retained data/delete references using numeric keyset
cursors and a shared budget. Include dropped-but-unexpired owners. Resolve only
the hierarchy needed by each page. The deletion queue itself is not ownership.
No candidates means no root/owner scan.

Plan the whole bounded batch before any physical deletion or queue cancellation.
Apply the [path-identity restrictions](CRUTCHES-AND-SHORTCUTS.md#rv-03-path-comparison-boundary)
without rewriting stored paths or deletion targets.

## Reclamation and queue handling

- An exact retained owner preserves the object and cancels only the selected obsolete
  queue request. Match original file ID, path bytes, relative flag and timestamp;
  normalized path or file ID alone is insufficient.
- Final-owner retirement can schedule the object again. Unowned objects are deleted,
  then their exact requests acknowledged. Failed deletions remain queued.
- Identical duplicate queue rows can coalesce; they are not separate guessed requests.
- Dry runs perform the same owner/path/budget checks without deletion or cancellation.

## Accepted boundaries

Independent imports/cleaners, path replacement, external catalogs and expired-reader
leases remain uncoordinated. A later stock DuckDB cleaner does not inherit our checks.
Exact shared directories are protected; prefix-only directory overlaps remain deferred
because retiring a child need not reschedule the whole parent. Persistent failed or
deferred oldest requests can occupy later batches and need operator reconciliation.

No metadata schema extension, shared scratch, Trino-only lease or stronger cross-engine
protocol is implied. Do not erase these limits merely because sequential tests pass.
