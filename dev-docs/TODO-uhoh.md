# Upstream risk watch

This parking lot is for current upstream risks that are not yet actionable connector features.
Recheck it during the upstream refresh described in [RESEARCH-upstreams.md](RESEARCH-upstreams.md).
Historical non-Parquet data-plane concerns moved to `brikk/duckbridge`; completed and triaged
review evidence remains in `archive/`.

## Watches

- [ ] **Server-side commit trajectory (Quack).** Upstream can stage metadata and publish through a
  server-side commit function. Re-evaluate whether the Quack backend should use that protocol if
  direct-SQL writers become second-class, but do not maintain two guessed conflict models.
- [ ] **Expire/cleanup versus long-running cross-engine reads.** Another engine can reclaim a file
  after Trino resolves its snapshot but before the scan finishes. This remains an accepted spec-
  level coordination gap; revisit only when upstream provides a cross-engine lease/lock contract.
- [ ] **DuckLake 1.1 types and metadata.** Variant/shredding, nested schema identity, exactness, and
  row-group metadata need explicit Trino mappings. `1.1-dev*` stays read-only; re-run qualification
  against final `1.1` artifacts rather than treating development layouts as stable.
- [ ] **DuckDB MySQL extension stability.** Trino read/write through direct MySQL JDBC is supported,
  but DuckDB 1.5.6 cross-engine access to a MySQL-backed DuckLake catalog remains deferred. Re-run
  the standalone probe on DuckDB updates and file an upstream issue with a minimal reproduction if
  the crash/protocol failures persist.
