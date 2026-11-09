# Trino DuckLake 0.5.2 — Trino 483 and 484

- Updates the catalog library to **0.15.1**.
- Fixes the file count reported when cleanup fails after snapshot expiration:
  rolled-back work is no longer counted as completed. This corrects progress
  reporting; no files were lost or incorrectly scheduled by the earlier bug.

Separate downloads are available for **Trino 483 and 484**, as ZIP or tar.gz with
SHA-256 checksums. Choose the matching Trino version and replace the entire plugin
directory on every coordinator and worker; do not mix old and new JARs.
