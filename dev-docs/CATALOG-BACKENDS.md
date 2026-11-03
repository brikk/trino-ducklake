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

Normal catalog/replay dependencies are **0.12.0**. The separately selected
[candidate](CATALOG-CANDIDATE.md) has exact snapshot/fixture pins, not a moving API floor.
Current user configuration is in [REFERENCE.md](../REFERENCE.md#catalog-configuration).

## Initialization and connection failure

The connector does not create/migrate DuckLake metadata tables. Bootstrap a compatible
catalog before connecting. An unreachable server/database or invalid credentials can
fail eager pool initialization. A reachable but uninitialized catalog can load the
connector and fail on its first metadata read; the missing-schema diagnostic should
identify that condition rather than pretend the lake is empty.

PostgreSQL/MySQL drivers are runtime dependencies. DuckDB JDBC and Quack drivers used
by tests are not automatically production dependencies. Do not infer a deployable
backend merely from a fixture accepting its JDBC URL.

## Process topology and qualification

Multiple DuckDB connections in one process can share a file. A separate Trino worker
JVM or DuckDB CLI process is another process, even on the same host. Do not propose
shared-file mounting as a way around that backend boundary.

The normal MySQL path uses direct JDBC, not DuckDB's `mysql` extension. The historical
native-extension failures motivate its cross-engine qualification gap, but are not
fresh reproductions on every new pin. Re-probe when assigned; do not publish an
unverified current-crash claim or make the native extension a requirement for Trino reads.

Quack deployment, authentication, exact driver/server versions, concurrent writers
and bounded CI remain [open integration work](TODO.md#backend-qualification). A remote
catalog connection does not make host-local data files reachable by Trino workers.

[Historical backend investigations](archive/CATALOG-BACKENDS-history.md) retain the
old probe evidence and implementation rationale without defining the current build.
