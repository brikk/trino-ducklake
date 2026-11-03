# Trino build compatibility

Supported build targets are **483** (default) and **484**. `-PtrinoVersion=...`
selects the complete Trino BOM, one `trino-<version>/src` adapter, compile/test
classpath and archive prefix. Unsupported targets fail during configuration.

Strict SPI version matching remains enabled. Do not install a differently targeted
plugin or mix implementation JARs. Assembly synchronizes each target's directory;
the server-provided SPI is excluded even when introduced transitively. Checksum
tasks track each archive and its own sidecar, not the shared output directory.

## API boundary

- 484 `finishMerge` returns optional output metadata. The adapter invokes the shared
  commit implementation and returns no extra output metadata.
- 484 passes memory contexts to merge sinks and scan providers. The adapters forward
  retained-state reservations for merge state/insert sinks and the bounded shared
  column-tree cache. Maintenance CALLs have no scan context. These reservations are
  estimates, not bounds on every temporary allocation.
- The Parquet LIST helper in 484 needs the logical element type; each adapter calls
  its actual target's helper.
- 484 JSON uses native JSON blocks rather than VARCHAR-compatible Slice blocks.
  Inline/default conversion uses the SPI native value; top-level Parquet writes
  render JSON through `Type.getSlice` to UTF-8 VARCHAR blocks without changing the
  catalog/SQL type. Native inline/CDC rows are emitted as bounded pages because
  `InMemoryRecordSet`'s class-based estimator rejects native JSON objects.

Adapters compile normally. No source rewriting, reflective SPI dispatch or relaxed
version check substitutes for target compatibility. Accepted memory/type restrictions
are recorded in [the inventory](CRUTCHES-AND-SHORTCUTS.md).

## Test SQL tooling

The compiler/BOM is Kotlin **2.4.10**. Test-only brikk-sql **0.21.0** supplies the
transpiler, metadata and Trino-specific native verifier; unrelated verifier engines
are not dependencies. None of these SQL-tooling JARs is shipped in the plugin.

The verifier requests parser 484 by default. A strict test constraint selects the
actual target's parser, preventing a 483 engine from receiving a 484 AST/grammar.
Tests check parser/SPI versions and the changed `JSON_SERIALIZE` keyword behavior.
Only `VerifyResult.Accepted` admits SQL. Rejections retain their native diagnostic;
unavailable/unverified parser states fail loudly rather than becoming corpus skips.

## Workflows and evidence

CI and the scheduled corpus job have separate 483/484 targets. Release jobs build
both archive sets with the same connector release suffix. The local workbench must
select matching `TRINO_VERSION` and `PLUGIN_VERSION` values explicitly.

See [development](DEVELOPMENT.md) for commands, [the catalog candidate](CATALOG-CANDIDATE.md)
for the separate opt-in lane, and [the October verification record](archive/TRINO-VERIFICATION-2026-10.md)
for historical implementation counts. Those counts are not a current full-suite verdict.
