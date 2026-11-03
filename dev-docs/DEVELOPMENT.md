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
built here. The normal catalog/replay version is **0.12.0**. The SQL transpiler,
metadata and native verifier are **brikk-sql 0.21.0**, test-only. Compiler/BOM Kotlin
is **2.4.10**. Production packages do not bundle DuckDB JDBC, replay, SLT, SQL tooling
or the Trino SPI.

The Trino BOM, parser, implementation JARs and selected `trino-<version>/src` adapter
must match one target. See [Trino compatibility](TRINO-COMPATIBILITY.md). Experimental
catalog qualification is a separate [candidate lane](CATALOG-CANDIDATE.md), not an
implicit dependency upgrade.

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

- The default native JDBC producer is **1.5.6.0**, with PostgreSQL fixtures.
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

Tag/version agreement needs a manual check; do not assume the workflow validates it.
Do not push a branch or publish artifacts merely because local verification passed.

## Documentation ownership

- Root README, REFERENCE and SAMPLES are current user/operator guidance.
- `dev-docs/` holds current developer contracts and one [active backlog](TODO.md).
- Dated experiments, completed handoffs/releases and superseded plans belong in
  [archive](archive/README.md), with links to any current replacement.
- Keep [accepted restrictions](CRUTCHES-AND-SHORTCUTS.md) and the live audit intact.
  Archiving a tracker does not complete unresolved work or waive a safety boundary.
