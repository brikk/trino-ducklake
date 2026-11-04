# Catalog 0.15 consumer trial

Normal builds remain on released **0.14.0**. The opt-in
`-PcatalogTrialVersion=0.15.0-20261104.182540-2` selects only that published
Central Portal build (source `98d7018eafb661f7a7c3c34dc733278bb0aa7aee`).
An exclusive repository rule prevents local Maven/JAR substitution; all four
JARs are checked against `gradle/catalog-trial.sha256`. Trial output names
carry `-catalog-trial` and are not the published 0.5.0 distribution.

No DuckLake 1.1 admission or new SQL procedure is enabled by this trial.
`trial/test` is compiled only with the trial property, keeping 0.15-only API
references out of the ordinary 0.14 build.

## Requested checks

Run the normal suite once per Trino target, sequentially, with the existing
starter corpus selection—not the full corpus. Limit Gradle workers and the
JVM's active processor count to keep the shared development node available.

```sh
./gradlew test detekt -PcatalogTrialVersion=0.15.0-20261104.182540-2 \
    -Dducklake.corpus.dirs=general,catalog,time_travel,view --max-workers=2 \
    -Pkotlin.compiler.execution.strategy=in-process
# Second run: add -PtrinoVersion=484 -PtestJavaVersion=26.
# JAVA_TOOL_OPTIONS includes -XX:ActiveProcessorCount=4 for these trial runs.
```

The normal expiry/cleanup tests exercise the existing SQL procedures. Additional
real PostgreSQL/two-worker trial controls cover:

- Expire-only mode, one-file cleanup budgets, ascending per-table paging,
  table-scoped dropped-table metadata cleanup, idempotence and live-row safety.
- A database-injected cleanup failure through `CALL system.expire_snapshots`:
  snapshots stay expired; recovery uses `cleanUpExpiredObjects`, never retries
  the expiry IDs. Existing `cleanup_old_files` then reclaims scheduled storage.
- Duplicate identical queue tuples and an already-missing physical file: the
  existing physical-cleanup procedure must acknowledge both without harming live data.

Results and generated JUnit XML are kept under
`build/verification/catalog-0.15.0-20261104.182540-2/`.

The 483/JDK 25 normal suite passed **1,075 tests in 130 classes**, with no
failures, errors or JUnit skips, including all three trial maintenance controls.
Detekt passed. The starter corpus retained its 930 passing records and 28 verified
Trino comparisons, with existing format/dialect/parser limits visible and no
changed SQL, goldens or skip lists. This was one normal-suite execution, not a
full-corpus run; the Gradle invocation completed in 5m10s.

The 484/JDK 26 normal suite also passed **1,075 tests in 130 classes**, with no
failures, errors or JUnit skips, including the same three maintenance controls.
Detekt passed; the invocation completed in 5m04s. The runs were sequential and
four-CPU capped; no main/release CI or packaging run was added to this trial.

**Consumer result: PASS for the existing DuckLake 1.0 connector paths.** No blocking
API issue was found in those paths. The API suggestions below are non-blocking for
the current legacy-overload procedure; they matter before adding bounded/resumable
SQL maintenance surfaces. Doris can take the development node next.

## Integration review

The normal connector opens the JDBC catalog directly; its experimental wrapper
uses Kotlin interface delegation, so new catalog methods are not swallowed by a
handwritten wrapper. The existing expiry procedure calls the legacy overload
once and has no internal retry loop. Its physical cleanup selects bounded exact
queue tuples and acknowledges identical duplicates together.

The 0.15 expiry call is not atomic across snapshot removal and cleanup. On failure,
do not assume time-travel history was retained. `cleanup_old_files` only drains
the physical-deletion queue; it does not resume catalog garbage collection.
Until a separate SQL resume surface is adopted, interrupted catalog cleanup needs
`cleanUpExpiredObjects` through an administrative catalog client.

API feedback to consider before exposing connector budget knobs:

- A typed post-expiry cleanup failure/result would let consumers distinguish
  committed snapshot removal from an expiry transaction failure without parsing SQL
  errors, and give an unambiguous resume instruction.
- `maxFiles` is documented as approximate. The collector removes a data file's
  associated delete files together; that fan-out is not a strict total-file budget.
  Do not advertise it as a hard per-call work bound. A hard cap or an explicit
  bounded overshoot contract would help consumer maintenance limits.

This is trial evidence and developer guidance, not public release notes or
permission to publish/promote the catalog or connector.
If the formal release has the same JAR bytes, this acceptance carries forward:
verify hashes and coordinate resolution rather than repeating the full suites.
