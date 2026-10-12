# trino-ducklake 483-0.4.0

This release targets Trino 483 and adopts the released DuckLake catalog, catalog test fixtures,
corpus replay, and SLT format at `0.12.0`. It also includes the test-only brikk-sql `0.18.0`
upgrade from the current main line.

Catalog `0.12.0` is a regression and compatibility-preparation release with no public API,
production implementation, runtime dependency, or corpus-pin changes relative to `0.11.0`.
The maintained lane remains DuckLake format 1.0 and DuckDB/JDBC 1.5.6; this connector does not
claim DuckLake 1.1 or final DuckDB 2.0 support.

## Verification

- Maven Central release variants for catalog runtime, test fixtures, corpus replay, and SLT format
  resolve at `0.12.0` with no snapshot references.
- Detekt passed.
- The complete JUnit suite was forcibly executed on JDK 26: **992 tests in 109 suites**, with zero
  failures, errors, or skipped JUnit tests. The corpus test used its bounded default starter
  directories; this was not a full-corpus run.
- ZIP and tar.gz distributions have one `trino-ducklake-483-0.4.0/` root, contain
  `ducklake-catalog-0.12.0.jar`, contain no SNAPSHOT or prior plugin version, and pass their
  generated SHA-256 checks.
