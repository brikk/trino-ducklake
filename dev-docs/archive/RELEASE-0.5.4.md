# Trino DuckLake 0.5.4 — Trino 483 and 484

- Updates the catalog library to **0.17.0**, reducing metadata round trips during
  writes. Query planning, time travel and SQL interfaces are unchanged.
- No configuration changes are required when upgrading from 0.5.3.

Separate ZIP/tar.gz downloads and SHA-256 checksums are available for **Trino 483
and 484**. Choose the exact matching target and replace the entire plugin directory
on every coordinator and worker; do not mix old and new JARs.
