# Trino DuckLake 0.5.1 — Trino 483 and 484

- Updates the catalog library to **0.15.0**. Snapshot expiration now cleans up dead
  files and metadata in smaller, resumable transactions.
- Provides separate plugins for **Trino 483 and 484**. DuckLake **1.0** remains
  the supported read/write format; 1.1 writes remain unsupported.

## Upgrade notes

If snapshot cleanup fails after expiration, **the snapshots stay expired**. The
error now reports this explicitly. Do not retry the same expiry IDs; resume catalog
cleanup using the catalog's `cleanUpExpiredObjects` administrative API.
`cleanup_old_files` only deletes already-scheduled files; it does not resume that
metadata cleanup. No new SQL procedure is introduced in this release.

Download `trino-ducklake-483-0.5.1` or `trino-ducklake-484-0.5.1` to match your
Trino version exactly. ZIP and tar.gz archives include SHA-256 sidecars. Replace
the entire plugin directory on every coordinator and worker; do not mix old and
new JARs.
