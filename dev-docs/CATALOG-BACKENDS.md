# Catalog backend contract

The catalog library owns metadata JDBC access (HikariCP/jOOQ); backend selection
comes from the URL. Trino filesystem access is a separate data-file plane. Using a
DuckDB metadata backend does not embed DuckDB as the production query/data engine.

## Normal deployment scope

| Backend | Scope and boundary |
| --- | --- |
| PostgreSQL | Maintained shared/multi-process and Trino/DuckDB interoperability path |
| MySQL 8+ | Trino read/write through bundled Connector/J; DuckDB cross-engine access is not qualified |
| Local DuckDB | One JVM; a compatible DuckDB JDBC driver must be supplied separately |
| Native/direct-Java Quack | Experimental fixtures and scoped candidate checks; not normal deployment qualification |
| SQLite/Turso | Not a supported Trino deployment configuration |

Normal catalog/replay dependencies are **0.16.0**. The separately selected
[experimental planning mode](CATALOG-CANDIDATE.md) uses the same released artifacts,
not snapshots or a moving API floor.
Current user configuration is in [REFERENCE.md](../REFERENCE.md#catalog-configuration).
The [owner-authorized local 0.16 trial](archive/CATALOG-0.16-LOCAL-TRIAL.md) is
historical evidence only. Its selector/repository/hash file are retired; all four
formal Central JARs match the trial bytes. The pool configuration work is retained.
Normal test drivers are DuckDB JDBC **2.0.0.1** and Java Quack JDBC **0.13.0**.
The catalog also qualifies Java Quack JDBC 0.13.0 upstream (Java 17, signed
DuckDB 2.0 peer); this connector retains its own scoped worker/session checks.
That driver qualification does not close the remaining deployment/authentication gates.

## Initialization and connection failure

The connector does not create/migrate DuckLake metadata tables. Bootstrap a compatible
catalog before connecting. An unreachable server/database or invalid credentials can
fail eager pool initialization. A reachable but uninitialized catalog can load the
connector and fail on its first metadata read; the missing-schema diagnostic should
identify that condition rather than pretend the lake is empty.

PostgreSQL/MySQL drivers are runtime dependencies. DuckDB JDBC and Quack drivers used
by tests are not automatically production dependencies. Do not infer a deployable
backend merely from a fixture accepting its JDBC URL.

### PgBouncer transaction pooling

The normal 0.16.0 catalog fixes transaction-local REPEATABLE READ setup in
`readSession`. The older 0.15.1 reader in released connector 0.5.2 is not safe
behind transaction pooling. For transaction pooling with the fixed catalog,
require PgBouncer >= 1.21
and `max_prepared_statements > 0`, or JDBC `prepareThreshold=0`. Keep the client
pool's maximum lifetime below the proxy's `server_lifetime`. See the
[operator instructions](../REFERENCE.md#pgbouncer-transaction-pooling).
The connector trial checks configuration mapping and actual JDBC parameter delivery;
it does not rerun the catalog owner's PgBouncer integration suite or qualify ProxySQL.

## Process topology and qualification

Multiple DuckDB connections in one process can share a file. A separate Trino worker
JVM or DuckDB CLI process is another process, even on the same host. Do not propose
shared-file mounting as a way around that backend boundary.

The normal MySQL path uses direct JDBC, not DuckDB's `mysql` extension. The historical
native-extension failures motivate its cross-engine qualification gap, but are not
fresh reproductions on every new pin. Re-probe when assigned; do not publish an
unverified current-crash claim or make the native extension a requirement for Trino reads.
The [November 3 native probe](archive/MYSQL-NATIVE-PROBE-2026-11-03.md) on DuckDB
2.0.0/JDBC 2.0.0.1 fails in native metadata-schema resolution, not a reproduced
SSL crash. New real worker tests cover the connector's separate Connector/J path.

Quack deployment, authentication, exact driver/server versions, concurrent writers
and bounded CI remain [open integration work](TODO.md#backend-qualification). A remote
catalog connection does not make host-local data files reachable by Trino workers.

[Historical backend investigations](archive/CATALOG-BACKENDS-history.md) retain the
old probe evidence and implementation rationale without defining the current build.
