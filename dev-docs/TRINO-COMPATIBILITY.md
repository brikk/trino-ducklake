# Trino build targets

The released catalog lane builds against Trino **483** (default) or **484**:

```sh
./gradlew pluginDist -PtrinoVersion=483
./gradlew pluginDist -PtrinoVersion=484
```

The property selects the complete Trino BOM and one `trino-<target>/src` directory.
Both archives use the same connector release suffix, but have distinct Trino prefixes.
Strict SPI matching remains enabled; never install a 483 plugin on 484 or mix jars from
the two distributions. Assembly synchronizes each target directory to remove stale jars and
excludes the server-provided SPI, even when a library introduces it transitively.
Checksum sidecars are individual Gradle outputs with their archives as inputs, so
switching targets cannot reuse the other target's checksum task state.
CI and nightly corpus jobs have independent target lanes; the release workflow builds
and attaches both distributions to the same release. The default Compose stack stays on 483;
set both `TRINO_VERSION=484` and the matching `PLUGIN_VERSION` to use the other archive.

## Corpus SQL tooling

`brikk-sql-jvm`, `brikk-sql-metadata-jvm` and `brikk-sql-verify-trino` use **0.21.0**.
They are `testImplementation` dependencies for corpus transpilation, schema-aware MAP
adaptation and native-grammar verification, never production plugin dependencies. The
Trino-specific verifier replaces the umbrella artifact so unrelated engine verifiers
are not loaded. The compiler and enforced Kotlin BOM use **2.4.10**, matching the
new artifacts' published runtime instead of forcing their stdlib back to 2.3.10.

The verifier requests parser 484 by default. Tests explicitly constrain `trino-parser`
to the selected target: allowing Gradle to upgrade only the parser would mix a 483
engine with a 484 AST/grammar. A regression checks the parser/SPI manifest versions
and `JSON_SERIALIZE`'s changed reserved-keyword behavior. The new sealed verifier API
admits only `Accepted`; native `Rejected` results retain their diagnostic, and an
unavailable/unverified parser fails loudly instead of turning into a corpus skip.

## API differences

- 484 `finishMerge` returns optional output metadata. The adapter commits using the same
  shared implementation, then returns no additional output metadata.
- 484 passes memory contexts into merge sink and scan provider creation. The adapters
  forward these to shared retained-state reporting (delete positions, lineage arrays,
  insert sinks and the provider's bounded column-tree cache). Maintenance CALLs do not
  receive a scan context and retain their existing bounded maintenance paths.
- 484's Parquet LIST helper takes the logical element type; each target calls its real
  helper rather than emulating or reflectively invoking a different API.
- 484 JSON no longer uses VARCHAR-compatible blocks. Inline/default conversion obtains
  the native value through SPI block operations. Parquet writes render top-level JSON
  blocks through `Type.getSlice` into UTF-8 VARCHAR blocks, matching DuckLake's existing
  physical storage. Reads use the target's native Parquet JSON reader. Catalog and SQL
  types remain JSON, including nulls; there is no VARCHAR type degradation.
- Native inline/CDC rows are written directly into byte-budgeted SPI pages because
  `InMemoryRecordSet` rejects 484 JSON objects in its Java-class size estimator. The
  source reports conservative retained-row memory and releases its iterator on close.

The version-specific adapters are compiled normally. No source rewriting, reflective SPI
dispatch, disabled tests or relaxed version checks are involved. The R07 catalog candidate
remains a separate opt-in qualification lane; this change does not promote its catalog floor.

## Verification (2026-10-28)

Both targets compile production and test sources on JDK 25 and pass detekt. Focused tests
cover native/inline type conversion, JSON read/write/UPDATE/flush/rewrite, dynamic catalog
creation, read smoke, CTAS/inserts, MERGE, field IDs, inline schema evolution, change feeds,
complex types and the `general` corpus directory. All selected tests passed without skips;
the full suite/corpus and R07 candidate lane were not run. Engine checks use Trino's
DistributedQueryRunner with the existing PostgreSQL/DuckDB fixtures, not a production cluster.

Sequential 483 → 484 packaging was checked for both ZIP and tar.gz: one flat plugin root,
only matching-version Trino implementation jars, no bundled SPI or test-only DuckDB JDBC,
and matching SHA-256 sidecars. Both distributions coexist under `build/distributions`.

### SQL tooling update verification (2026-10-31)

With brikk-sql 0.21.0 and Kotlin 2.4.10, **248 tests across 20 focused classes** passed
on each target, with no failures or JUnit skips. This adds corpus-gate, MAP adaptation,
actual MAP/ROW mirror and nested geometry regressions to the earlier focused coverage;
corpus replay remains limited to `general`, not the full corpus. Both targets pass detekt.
Both ZIP/tar.gz sets and checksums were rebuilt and checked: matching Trino jars and
Kotlin stdlib 2.4.10, with no brikk-sql tooling, SPI or test-only DuckDB JDBC bundled.
