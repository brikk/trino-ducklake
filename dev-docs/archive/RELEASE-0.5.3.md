# Trino DuckLake 0.5.3 — Trino 483 and 484

- Updates the catalog library to **0.16.0**, fixing read-session isolation behind
  PgBouncer in transaction-pooling mode.
- Adds metadata connection-pool settings, with unchanged defaults:
  `ducklake.catalog.min-idle-connections`, `ducklake.catalog.connection-idle-timeout`,
  `ducklake.catalog.connection-max-lifetime`, and `ducklake.catalog.connection-timeout`.

## PgBouncer configuration

Transaction pooling requires **PgBouncer 1.21+ with `max_prepared_statements > 0`**,
or **`prepareThreshold=0` on the PostgreSQL JDBC URL**. JDBC URL parameters pass
through unchanged. Keep `ducklake.catalog.connection-max-lifetime` below the
proxy's `server_lifetime`.

See the [configuration reference](https://github.com/brikk/trino-ducklake/blob/v0.5.3/REFERENCE.md#pgbouncer-transaction-pooling)
for details. Do not use transaction pooling with the older connector 0.5.2.

Separate ZIP/tar.gz downloads and SHA-256 checksums are available for **Trino 483
and 484**. Choose the exact matching target and replace the whole plugin directory
on every coordinator and worker; do not mix old and new JARs.
