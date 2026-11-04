# Catalog 0.15.0 adoption

Normal and experimental builds now use Maven Central **0.15.0**, source
`948bf0744480d74595703a8c9b42dbc5abe8e937`, tag `v0.15.0`. All four JARs were
independently downloaded and matched against `gradle/catalog-release.sha256`.
The snapshot trial selector/repository/hash file is removed. Its maintenance
regressions now live in the normal test source set; the historical trial record
is [retained separately](CATALOG-0.15-TRIAL.md).

Only the catalog JAR differs from the accepted trial: typed post-expiry cleanup
failure, hard file budgets including delete-file fan-out, and legacy result-copy
ABI restoration. Replay, SLT and fixture JARs remain byte-identical.

## Targeted adoption checks

Per owner direction, compile and run only:

- `TestDucklakeExpireSnapshots`
- `TestDucklakeCleanupOldFiles`
- `TestDucklakeCatalogMaintenance` (including the three changed contracts)
- `TestDucklakeCreateCatalog.createCatalogViaSqlThenQueryIt` (smoke)

Run sequentially for 483/JDK 25 and 484/JDK 26, capped at four active processors
and two Gradle workers. Do not rerun full suites, corpus, packaging or release
verification. The push uses `[skip ci]` to avoid a redundant full main-CI run.
Generated results are under `build/verification/catalog-0.15.0/`.

483/JDK 25: **38/38 tests passed**, zero failures/errors/skips; production/test
compilation and artifact-hash checks passed. The invocation took 56 seconds.
484/JDK 26: **38/38 tests passed**, zero failures/errors/skips; production/test
compilation and artifact-hash checks passed. All three post-trial changes pass
on both targets. No full suite or corpus was run for the formal adoption.

The failure regression calls the real expiry procedure with PostgreSQL-injected
cleanup failure, asserts the typed exception and its committed counts/cause,
checks that history stays expired, then resumes cleanup without retrying expiry.
The fan-out regression uses real Trino DELETE files and a budget of one, checking
that every step stays within budget and eventually finishes without removing
live data. Legacy `copy(int,int)` and both Kotlin default-argument masks are
invoked by their old JVM signatures.

No SQL procedure surface or 1.1 admission change is part of this dependency
adoption. Catalog-GC recovery still requires the catalog administrative API;
`cleanup_old_files` only drains already-scheduled physical deletion requests.
