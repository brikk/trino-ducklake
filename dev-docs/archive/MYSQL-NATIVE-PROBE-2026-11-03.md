# DuckDB 2.0 / MySQL interoperability probe

This is scoped native-engine evidence, not a connector test skip or a general
MySQL support verdict. `TestDucklakeReleasedMySql` independently passes real
Trino worker CTAS/INSERT/UPDATE/DELETE/read/ANALYZE through Connector/J on both
483/JDK 25 and 484/JDK 26, using the published catalog 0.13.0 fixture.

## Native reproduction

The dependency adoption re-probed the historical native MySQL limitation in a
standalone JVM, so a possible native crash could not kill the connector test JVM.
Runtime: JDK 25.0.2, DuckDB JDBC 2.0.0.1, signed DuckLake `417a9117f5`, MySQL
`mysql:8.4` Testcontainer with database `ducklake` and a user granted that database.
The JDBC JAR SHA-256 is
`6cb8b0e0fc51b2e45aead2d2c1e1fd09dd10267db2966e69aaab0de67319f826`.

Against a fresh database, with the actual port and a standard owned temporary
data directory substituted:

```sql
INSTALL ducklake;
LOAD ducklake;
INSTALL mysql;
LOAD mysql;
ATTACH 'ducklake:mysql:db=ducklake host=127.0.0.1 port=<port> user=test password=test'
    AS lake (DATA_PATH '<test-temp-directory>',
             DUCKLAKE_VERSION '1.0', AUTOMATIC_MIGRATION FALSE);
CREATE TABLE lake.main.t(i INTEGER);
```

ATTACH initializes metadata, but native CREATE fails while resolving the latest
snapshot. The generated MySQL query qualifies catalog relations as
`` `main`.`ducklake_snapshot` `` / `` `main`.`ducklake_metadata` `` rather than
the configured database. MySQL reports:

```text
Binder Error: Failed to query most recent snapshot for DuckLake: PREPARE error
SELECT command denied to user 'test'@'172.17.0.1' for table 'ducklake_snapshot'
```

The attempted standard `METADATA_SCHEMA 'ducklake'` configuration also fails on
a fresh fixture: native initialization issues `CREATE SCHEMA ducklake`, and
MySQL rejects it because database `ducklake` already exists. Neither probe
reached a Java-catalog mutation or a cross-engine read. No native crash was
observed in these runs; the old 1.5.x SSL-crash evidence is historical, not this
release's observed failure.

## Boundary

Do not grant unrelated databases, rewrite native metadata SQL, change a stored
DuckLake version, or use this failure to skip Trino's direct-JDBC tests. The
native path needs backend-schema qualification before interoperability can be
advertised. Connector/J support does not depend on ongoing native MySQL reads.

Local raw evidence at adoption time:
`/tmp/opencode/catalog-013-mysql-native-probe.log`; the standard dependency
receipts and passing connector test XML are under
`build/verification/catalog-0.13.0/`. These are generated evidence paths, not
required runtime storage or a deployment scheme.
