# Catalog 0.13.1 connector adoption

## Artifact and runtime identity

Both normal and experimental planning builds consume Maven Central catalog,
catalog test fixtures, replay and SLT **0.13.1**, with strict version constraints.
Release source: `f35e105fef69598895b11bf7320e67501ea2e623`, tag `v0.13.1`.
The historical timestamp snapshot is no longer a build option. Its old receipt
is retained only as [archival evidence](catalog-72aae19.sha256).

`verifyCatalogRelease` runs before tests in both modes and attests all four JARs
against [`gradle/catalog-release.sha256`](../../gradle/catalog-release.sha256).
The resolved bytes were independently compared with Central. Catalog, fixtures
and SLT are byte-identical to 0.13.0; only replay changes in 0.13.1.

Test-only drivers are DuckDB JDBC **2.0.0.1** and Java Quack JDBC **0.13.0**.
Their Central JAR SHA-256 values are respectively:

```text
6cb8b0e0fc51b2e45aead2d2c1e1fd09dd10267db2966e69aaab0de67319f826
0935b75d3aa86a320b9f1d0330fbd8dfa551c4c2505aa11cc1be612dfdf455e0
```

Native identity: DuckDB 2.0.0 core `d54cbbe3df`, signed DuckLake `417a9117f5`,
native Quack `f96420ed46`. The corpus gitlink is
`417a9117f590e8141742ac0579d02f6a90162e37`, aligned with the native extension.
No golden files were edited. The upstream catalog qualifies Quack JDBC 0.12.1;
the 0.13.0 checks here are connector-side scoped qualification, not a claim that
the upstream catalog suite was rerun with that driver.

## Adoption changes

- Normal tests explicitly create DuckLake 1.0 and disable automatic migration.
  Replay selects `ReplaySetupFormat.DUCKLAKE_1_0`, the released native default
  setting API. It does not rewrite corpus SQL, ATTACH clauses or goldens. Explicit
  format choices and SET/RESET retain native meaning; existing newer lakes cannot
  be downgraded.
- The shared bounded signature parser now serves both build modes, preserving
  quoted nested names and existing scalar/type-admission boundaries. Redundant
  0.12 optional-method reflection for the opt-in setter and decoded defaults was
  removed. Normal 1.1 admission rejects before allocating a catalog connection.
- Existing DELETE/MERGE/flush/rewrite call sites already pass their planning
  snapshots; no snapshotless mutation overload was substituted. ANALYZE now has
  an explicit one-new-snapshot assertion.
- Unknown-COMMIT and acknowledged-post-commit failures retain their causes and
  diagnostics through non-conflict error mapping. A test-only connector boundary
  injects each failure after real INSERT, CTAS, UPDATE, DELETE and MERGE publication;
  the engine does not republish or remove registered output during failure cleanup.
  This is not a JDBC network-partition simulator or automated reconciliation.
- Direct Java Quack checks cover 20 owned sessions on a one-connection pool,
  evolved inline/Parquet reads, worker INSERT/UPDATE/DELETE, the existing inline
  mutation guard, a real native flush and native observation of Trino output.
  Separate MySQL worker tests cover the Connector/J path.

## Verification

- Trino 483 / JDK 25 normal suite: **1,042 tests**, zero failures/errors/JUnit skips,
  with the corpus restricted to `general`.
- 483 focused replay checks with `DUCKLAKE_CI=1`: **2 tests**, zero failures/errors/
  JUnit skips. The original cache fixture stores real spec 1.0 and passes **2/2**
  Trino mirrors with an unchanged file digest.
- Trino 484 / JDK 26 normal suite: **1,043 tests**, zero failures/errors/JUnit skips,
  with the corpus restricted to `general` (includes the new cache-fixture test).
- Trino 484 / JDK 26 experimental selection (`TestPublished*`, `TestR07*`,
  `TestLegacyTimezonePartitions`): **91 tests**, zero failures/errors/JUnit skips,
  including actual Java Quack 0.13.0 read/session/default checks.
- Detekt passes for both normal targets and the 484 experimental build.
- Both normal targets' ZIP and tar.gz archives pass root/layout, target-matched
  Trino JAR, exact catalog-byte, checksum and dependency-exclusion checks. Normal
  implementation JARs contain the shared signature parser and no experimental
  `PagedDucklakeCatalog` classes. Neither driver, test fixtures, replay, SLT,
  SQL tooling, JUnit nor Trino SPI is bundled. 483 has 325 JARs; 484 has 296.

Archive SHA-256 values (`trino-ducklake-<target>-0.5.0-SNAPSHOT`):

```text
1396a469529ddbc2b4f5c640bd49fb0a15a62be3048e468d4ea512d702e2a61c  483.zip
0db6f95b968c4dceab6873f6ac840677515b8f784d235eeab3798d836d5f0499  483.tar.gz
2185e0041c08e3232995a1a2d1c03c2ebaf46c38fce0f4a9a0aed0b4acd7e80a  484.zip
38ece19ad8bccb4fbf6f4e61ea9c9236300f162aad515037813a6f1df730c7d4  484.tar.gz
```

The first experimental attempt correctly refused the old 2.0.0.0 driver checksum
in its fixture guard. That assertion now pins Central's actual 2.0.0.1 checksum
above and identifies unexpected bytes in its diagnostic. The next attempt was
interrupted by a kernel-confirmed global OOM kill (exit 137, Java PID 3767780,
2026-11-03 13:39:16); partial XML, including an interrupted/skipped case, is not
counted as verification. A smaller/serial diagnostic attempt then failed during
native-library extraction with `java.io.IOException: Disk quota exceeded` at
`DuckDBNative.unpackAndLoad`; its native tests are not counted as verified either.
After the user stopped other jobs, the final rerun uses the original 3 GiB/parallel
configuration with workspace-local `java.io.tmpdir`/`TMPDIR`, following the existing
quota-recovery workflow. This is standard build/test scratch. Explicit concurrency
cases, worker counts, production/work-budget settings and assertions are unchanged,
and no test is excluded.

The final experimental rerun command was:

```sh
mkdir -p build/tmp/catalog-0.13.1
JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }-Djava.io.tmpdir=$PWD/build/tmp/catalog-0.13.1" \
TMPDIR="$PWD/build/tmp/catalog-0.13.1" mise exec -- ./gradlew test \
  -Pr07CatalogCandidate -PtrinoVersion=484 -PtestJavaVersion=26 \
  --tests '*TestPublished*' --tests '*TestR07*' --tests '*TestLegacyTimezonePartitions' \
  --no-daemon -Pkotlin.compiler.execution.strategy=in-process
```

The `general` replay has 22 files, 276 passing records, 86 oracle/golden queries,
eight verified Trino mirrors and no failures. Existing coverage limits remain
visible: 39 record skips, four upstream PostgreSQL-axis file skips, one
unsupported-`require-env` file skip, and measured mirror-gate/engine skips.
`general/ducklake_metadata_logging.test` is that unsupported-directive file;
setting `DUCKLAKE_CI=1` does not make this parser execute it. No directive was
removed, no new skip was added, and that file is not claimed as verified. The
shared directive support remains in the [active backlog](../TODO.md).

The full corpus was **not run**. The old-pin/new-native geometry golden mismatch
is resolved by aligning the corpus pin; the original three nested geometry
mirror regressions pass without golden edits.

Generated XML and Central comparison receipts are under
`build/verification/catalog-0.13.1/`. They are build/test evidence, not production
storage. The earlier [native MySQL probe](MYSQL-NATIVE-PROBE-2026-11-03.md) still
applies: the catalog bytes are unchanged, and native MySQL interoperability is
not promoted merely because direct Connector/J tests pass.

## Preserved restrictions

No final-1.1 default-on access, migrations or connector 1.1 writes are enabled.
Experimental budgets/cancellation/retention/deployment gates remain in the
[planning contract](../CATALOG-CANDIDATE.md) and
[restrictions inventory](../CRUTCHES-AND-SHORTCUTS.md).
Uncertain output must be retained and reconciled before retry or cleanup;
an attempted snapshot ID is not proof of publication or ownership.
