# trino-ducklake 483-0.3.0

This release targets Trino 483 and adopts the released DuckLake catalog, catalog test fixtures,
corpus replay, and SLT format at `0.11.0`. Cross-engine tests align DuckDB JDBC to `1.5.6.0`.

## Highlights

- Omit column statistics when active-file coverage is unknown instead of exposing incomplete
  values (`CI-01`).
- Preserve nullable catalog row-ID starts and use upstream-aligned
  `(data_file_id, physical_position)` mutation locators for DELETE, UPDATE, and MERGE (`CI-02`).
- Adopt logical-type-aware nested geometry normalization and remove obsolete WKB/WKT corpus skips
  (`CI-03`).
- Translate proven string-key DuckDB MAP-dot access through the typed corpus adapter without
  conflating MAP access, ROW fields, or table aliases (`CI-04`).
- Run the regular CI suite on JDK 26 while retaining Java 25 compilation compatibility as an
  ongoing native-stability check (`CI-05`).
- Package only the current plugin version directory in release archives, preventing stale local
  release directories from entering a later archive.

## Verification

- Maven Central release variants for catalog runtime, test fixtures, corpus replay, and SLT format
  resolve at `0.11.0`; DuckDB JDBC resolves at `1.5.6.0`.
- Detekt passed.
- The complete JUnit suite passed on JDK 26: **992 tests in 109 suites**, with zero failures,
  errors, or skipped JUnit tests. The corpus test used its bounded default starter directories;
  this was not a full-corpus run.
- ZIP and tar.gz distributions were assembled with one `trino-ducklake-483-0.3.0/` root,
  contained `ducklake-catalog-0.11.0.jar`, contained no SNAPSHOT or prior plugin version, and
  passed their generated SHA-256 checks.
