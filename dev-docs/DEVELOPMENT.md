# Development workflow

## Build and dependency boundaries

This is a single-project Gradle build at the repository root. Use the checked-in
`./gradlew`, JDK 25 (`mise install`), and the version catalog in
[`gradle/libs.versions.toml`](../gradle/libs.versions.toml).

```sh
./gradlew pluginDist                     # Trino 483
./gradlew pluginDist -PtrinoVersion=484   # Trino 484
./gradlew pluginAssemble                 # exploded plugin directory
```

The catalog and replay framework are published dependencies, not source projects
built here. The normal catalog/replay/SLT version is pinned snapshot
**0.18.0-20261109.223927-4**. The SQL transpiler,
metadata and native verifier are **brikk-sql 0.21.0**, test-only. Compiler/BOM Kotlin
is **2.4.10**. Production packages do not bundle DuckDB JDBC, replay, SLT, SQL tooling
or the Trino SPI.

The Trino BOM, parser, implementation JARs and selected `trino-<version>/src` adapter
must match one target. See [Trino compatibility](TRINO-COMPATIBILITY.md). Experimental
final-1.1 planning remains a separate [opt-in mode](CATALOG-CANDIDATE.md), not a
capability enabled by the dependency upgrade.

## Tests

Initialize the `ducklake` corpus submodule when running integration/corpus tests:

```sh
git submodule update --init --recursive ducklake
./gradlew detekt
./gradlew test --tests '*TestDucklakeTypeConverter'
./gradlew test --tests '*TestTrinoCorpusReplay' -Dducklake.corpus.dirs=general
```

Docker or Podman's Docker-compatible API is needed for Testcontainers. Native
cross-engine tests install DuckDB extensions and need network access or an existing
compatible extension cache. Required fixtures and actual JUnit execution matter:
inspect XML failures/errors/skips, not just a green command exit.

- The default native JDBC producer is **2.0.0.1**, with PostgreSQL fixtures. Native
  setup selects DuckLake **1.0**, with automatic migration disabled. The optional
  direct-Java Quack fixture uses **quack-jdbc 0.13.0**; neither driver is bundled.
  The corpus is pinned to `417a9117f590e8141742ac0579d02f6a90162e37`, matching
  signed DuckLake `417a9117f5`. Replay uses `ReplaySetupFormat.DUCKLAKE_1_0` to
  set the native default; corpus SQL/goldens and explicit format options are unchanged.
  The normal suite selects both mirror and execution format scope 1.0: explicit
  1.1 fixtures halt visibly, retaining prior outcomes and never counting as passed
  1.0. Native metadata SQL is structurally bound to known rewritten PostgreSQL
  metadata namespaces only; real lake schemas/literals/explicit metadata schemas
  are unchanged. Full JUnit runs use CI's actual starter set, not general-only.
  `verifyCatalogRelease` checks all four catalog/replay/SLT/fixture JAR hashes
  against `gradle/catalog-release.sha256` before tests in both build modes.
- `-Dducklake.test.catalog-backend=DUCKDB_LOCAL` selects a local DuckDB metadata file;
  `DUCKDB_QUACK` selects an experimental transport fixture, not a production guarantee.
- Add `-PtrinoVersion=484` for that target and `-PtestJavaVersion=26` for the other test VM.
- CI tests both targets on JDK 26 while compiling with the JDK 25 toolchain. Detekt
  and tests are separate steps to avoid overlapping their peak memory use.
- Ordinary corpus runs use a selected directory; the test's no-property default is
  `general,catalog,time_travel,view`. The scheduled corpus workflow is broader.
  **Do not run the full corpus locally unless explicitly requested.**
- Corpus gates admit only selected, translatable lake reads. Oracle/setup totals do
  not prove engine comparison. Candidate replay controls require measured positive
  mirrors; wrong hashes and engine skips cannot satisfy that requirement.

`-Dducklake.corpus.dirs=...` has a significant leading `d`; a misspelled JVM property
can silently leave the default directory selection in effect.

### Local scratch failures

If real errors show a system-temp quota failure, use a workspace-local JVM temp
directory for build/test scratch. Do not delete other sessions' files, add production
staging, relax assertions or classify interrupted tests as passing:

```sh
mkdir -p build/tmp/local-verification
JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }-Djava.io.tmpdir=$PWD/build/tmp/local-verification" \
TMPDIR="$PWD/build/tmp/local-verification" \
mise exec -- ./gradlew test --tests '*TestDucklakeTypeConverter' --no-daemon \
  -Pkotlin.compiler.execution.strategy=in-process -Pkotlin.incremental=false
```

The [local Compose workbench](LOCAL-STACK.md) has additional checkout dependencies;
it is not a fresh-clone production deployment recipe.

## Releasing

1. Verify the intended source/dependency lane and update the shared connector release
   suffix in `build.gradle.kts` deliberately. Normal release jobs do not select R07.
2. Run scoped tests and quality gates for both targets. Build both `pluginDist` sets.
3. Check archive roots, matching implementation JAR versions, absence of test-only/SPI
   JARs, and SHA-256 sidecars. Keep old JARs out of plugin directories.
4. Commit verified work. Create/push a `v<version>` tag only when explicitly authorized.
   The release workflow builds both targets and attaches archives/checksums to that tag.

From 0.5.0, tags use the shared suffix: `v0.5.0` has `483-0.5.0` and `484-0.5.0`
archive versions. Older tags used the default target prefix (`v483-0.4.0`).
Check the shared release suffix and both target prefixes deliberately.

Tag/version agreement needs a manual check; do not assume the workflow validates it.
Do not push a branch or publish artifacts merely because local verification passed.

## Documentation ownership

- Root README, REFERENCE and SAMPLES are current user/operator guidance.
- `dev-docs/` holds current developer contracts and one [active backlog](TODO.md).
- Release notes, dated experiments, completed handoffs and superseded plans belong in
  a topic folder of the [archive](archive/README.md), with a one-line index entry.
- Keep [accepted restrictions](CRUTCHES-AND-SHORTCUTS.md) current. Archiving a document
  does not complete its unresolved work: move open items to [TODO.md](TODO.md) first.
