# Published catalog 0.17 consumer trial

> Historical trial evidence. Normal builds now use Central 0.17.0, source
> `5a1c9bab66f9100e926791e1e6d1cbef8c75e667`. All four formal JARs match the tested
> snapshot bytes. The trial selector/repository/hash file are retired; the three
> focused regressions now live in `TestDucklakeCatalogBounds` in normal sources.
> Production planning is unchanged; formal promotion does not repeat the tests.
>
> Promotion checks passed: independent Central downloads matched all four hashes
> and the accepted snapshot JARs byte-for-byte. Gradle verified the resolved formal
> artifacts, and production plus test-source compilation passed for final 0.5.4.
> No behavioral tests or corpus were rerun; release CI builds the two target archives.

Normal dependencies remain on Maven Central **0.16.0**. Only
`-PcatalogTrialVersion=0.17.0-20261106.162112-2` selects the exact published
snapshot from source `5377b5390b48de730d23f1043c3e006e211fb848`.
All four JARs must match `gradle/catalog-trial.sha256`. An exclusive Central
Portal repository rule excludes local Maven/JAR substitution; output names carry
`-catalog-trial`. The three unchanged fixture/replay/SLT hashes match 0.16.0.

## Scope

Production planning is unchanged: it still uses `getColumnStats`, including for
time travel. The new `getCurrentColumnBounds` is exercised only under `trial/test`.
The normal catalog provider opens the JDBC implementation directly; the experimental
wrapper uses interface delegation, not a handwritten implementation missing the API.

Focused real PostgreSQL/two-worker tests cover:

- Requiring an owned read session for current bounds; numeric bounds and NULL/NaN
  flags from native-written metadata; exact Trino MIN/MAX after a partial delete.
- Omitted table-level bounds remaining unknown while historical file statistics
  and current/historical Trino aggregates continue to return the correct values.
- The combined write-guard query retaining `require_commit_message` refusal for
  INSERT/DDL without publishing a snapshot, then permitting writes after it is disabled.
  Existing normal format-lock and encrypted-catalog tests cover the other guard keys.

No planner adoption or MIN/MAX optimization is inferred from these checks. Any later
planner adoption must use latest-session bounds only, keep NaN-bearing ranges
non-pruning and fall back for omitted columns; these are gross supersets, not answers.

## Requested run

Run the normal suite **once on Trino 483/JDK 25**, including the focused trial
tests, and Detekt in the same invocation:

```sh
./gradlew test detekt -PcatalogTrialVersion=0.17.0-20261106.162112-2 \
    -Dducklake.corpus.dirs=general,catalog,time_travel,view --max-workers=2 \
    -Pkotlin.compiler.execution.strategy=in-process
```

The run uses `-XX:ActiveProcessorCount=4` and ordinary build-local JVM scratch.
This is the default starter corpus only—not the full corpus. No second target,
duplicate CI, packaging, push or release is part of this request.
Generated receipts live under `build/verification/catalog-0.17.0-20261106.162112-2/`.
If the trial passes, the owner cuts formal 0.17.0 and gives the next instruction.

## Result

**PASS** on Trino 483/JDK 25: **1,085 tests in 132 classes**, zero failures,
errors or JUnit skips, including all three focused trial controls. Detekt passed.
The successful invocation took 4m57s. All four independently downloaded snapshot
JAR hashes matched the supplied receipt and the resolved test artifacts.

The first invocation stopped at Detekt before tests ran: two existing pool-test
helpers were too deeply nested. They were flattened without changing assertions;
the successful invocation was the only normal-suite execution. No production
source, corpus SQL/goldens, format admission or skip list was changed.

Normal dependencies remain on 0.16.0. No second target, duplicate CI, push or
release was performed. Await the owner's formal release/adoption instruction.
