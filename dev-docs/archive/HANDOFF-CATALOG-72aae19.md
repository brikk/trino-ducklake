# Published catalog read-correctness handoff — November 1, 2026

> Completed scoped handoff and verification record. Current candidate configuration and
> remaining qualification boundaries are maintained in dev-docs/CATALOG-CANDIDATE.md.

## Exact opt-in dependency identity

Catalog source: `72aae19d0deaf05a6c61e6782a6d263325ff23dd` (N03, H15, N19).
Repository: `https://central.sonatype.com/repository/maven-snapshots/`.
All three modules use **`0.13.0-20261101.082015-33`**: `ducklake-catalog`,
`ducklake-test-corpus-replay`, and `slt-format`, including catalog test fixtures.
All four JAR hashes are tracked in [`gradle/r07-catalog-snapshot.sha256`](../../gradle/r07-catalog-snapshot.sha256).

```sh
./gradlew verifyR07Candidate -Pr07CatalogCandidate
./gradlew test -Pr07CatalogCandidate -PtrinoVersion=484 --tests '*TestPublished*'
```

Omit the candidate property to retain released catalog/replay **0.12.0** and the
existing native producer lane. Both Trino **483 and 484** remain supported targets.
The candidate's native producer remains DuckDB JDBC **2.0.0.0**, Java Quack **0.8.0**;
no native/corpus/golden pin was changed. `-Pr07CatalogCandidate=<exact version>` is
also accepted; obsolete bundle paths fail clearly. The timestamp is strict on all
three modules so published base-SNAPSHOT transitives cannot select another build.
Repository selection is exclusive to those modules in this opt-in lane, not `mavenLocal`.
No sibling checkout or copied JAR is needed. Candidate output names end in `-r07`.

## Consumer changes and evidence boundaries

- **N03:** the candidate string-to-Trino-type path calls the catalog's public
  `DucklakeTypeSignature.parse`. It preserves field names/order, including quoted
  punctuation, literal backslashes, whitespace, Unicode and mixed case. Decimal
  parameter commas are not field separators. Existing scalar mappings/type-manager
  use and explicit unsupported-type boundaries remain shared. The default 0.12
  source set neither references the new parser nor changes its old conversion path.
  The paged adapter's own signature producer also quotes unsafe names using public
  `DucklakeQuotedList` encoding; ordinary ASCII spellings remain unchanged.
- Parser regressions check malformed syntax/arity and depth/input budget refusal.
  Real metadata and two-worker SQL regressions cover STRUCT, STRUCT in LIST/MAP,
  nested NULLs, empty containers, literal `NULL`, decimals, and a nested-field rename
  retaining IDs/parents/order. Local and PostgreSQL inline/Parquet routes are selected
  explicitly rather than inferred from successful row counts.
- Legacy native JDBC STRUCT/ARRAY/MAP cells are converted as typed values instead
  of parsing `toString()`. STRUCT attributes require complete source-era field order;
  current targets are reached through the existing stable-ID/name mapping, including
  dropped fields and added defaults. Missing order, width/key mismatches and unsupported
  composite MAP keys fail explicitly. PostgreSQL retains nested-text parsing, with
  corrected quoted-name/VARCHAR escape handling; BLOB hex escapes remain literal.
  This also repairs an existing shared legacy native-value path; no new stored format
  or DuckDB JDBC dependency is added to the production connector.
- **H15:** public renderer checks compare native bound FLOAT/DOUBLE bits/text,
  including shortest subnormals, FLOAT fallback, negative zero and signed NaN.
- **N19:** typed hashes preserve Boolean provenance, sort-mode behavior, VARCHAR
  lookalikes and raw cells. Real Trino replay fixtures require positive measured
  mirror passes, not setup/oracle totals. An authored wrong-hash negative must fail
  before mirroring while its following valid record still reaches the actual engine.

This is scoped consumer adoption, not production/default-on enablement, a release,
migration or permission for connector 1.1 writes. Existing read-only guards remain.
It does not certify untested routes/types, physical leases, cross-process/fleet
behavior or all R07 bounded metadata/cancellation surfaces. The two workers run in
one DistributedQueryRunner JVM; this is not a multi-host deployment claim.
The full connector corpus is not part of this handoff.

Trino's native SQL-type text round-trip can canonicalize a simple mixed-case ROW
field name (the formatter emits an unquoted identifier, then the parser lowercases
it). A native-only control reproduces this without the connector. Exact names are
asserted at catalog/SPI metadata and through `DESCRIBE` strings, not inferred from
TestingTrinoClient's reconstructed `MaterializedResult.types`. Actual quoted-field
worker reads still require independent values; no source field is renamed to pass.
DDL runs between closed reader sessions in the single-process local DuckDB fixture,
avoiding its duplicate-metadata-catalog attachment conflict. This proves retained
IDs/order and old-data readability after rename, not concurrent DDL/cache invalidation.

## Candidate verification

Both exact-published candidate targets pass with no JUnit failures/errors/skips:

| Target | Test VM | Focused checks |
| --- | --- | --- |
| Trino 484 | JDK 25 | 27 tests / 5 classes: new type, native/text value, eight-route read and replay controls |
| Trino 483 | JDK 25 | 92 tests / 11 classes: the same coverage plus conversion/config/read-only, three existing R07 nested/default SQL cases and cancellation fan-out |
| Trino 484 | JDK 26 | 6 tests / 2 classes: floating/Boolean conformance and actual replay mirrors |
| Trino 483 | JDK 26 | 6 tests / 2 classes: floating/Boolean conformance and actual replay mirrors |

Each replay positive requires **8 actual mirror passes**, zero mirror skips/failures;
its separate wrong-hash negative requires exactly one oracle failure and only the
following valid query to mirror. A JUnit runtime check asserts the actual VM matches
the requested Gradle launcher (stdout is not used as a VM witness).
Detekt passes for both candidate targets. Exact resolution/hashes are checked by
`verifyR07Candidate` before each candidate test invocation.

Local evidence: `build/verification/catalog-72aae19/verification.json`, per-phase logs
and `*-xml` snapshots. Preserved red checkpoints distinguish test-client type-name
canonicalization, a fixture's malformed ALTER quote, and the real native/text/paged
consumer defects from completed passing runs. No corpus exclusion or golden changed.

## Released-lane and artifact verification

Both default targets pass **182 tests across 15 classes** on JDK 25, with zero
failures/errors/skips, plus detekt. This includes the unchanged `general` corpus
selection, nested/type/JSON controls and a new real legacy-producer PostgreSQL
inline read with escaped field names and VARCHAR values. Normal catalog/replay
remain 0.12.0; this is not a default dependency promotion.

Both default ZIP/tar.gz archives and checksum sidecars pass inspection. They contain
catalog 0.12.0, no candidate 0.13 artifacts, and no test-fixture/replay/SLT JARs.
Default plugin class inspection finds neither the paged candidate implementation
nor a reference to the new public catalog parser. The released 483 build was left
as the final ordinary checkout output.

Plugin implementation is the commit containing this report, based on `25c84f5`.
Exact locally verified main-JAR SHA256s (not publication coordinates):

| Target/lane | SHA256 |
| --- | --- |
| 483 candidate (`-r07`) | `5b0a9523eada54be8d2566b1d4388517fb576d0e9120ddc13e8b06c3d55e9a72` |
| 484 candidate (`-r07`) | `c987cdfbfa77e1ccc4e08e8cd5fd3645a21f7fa04d38db7fab039379d92f4773` |
| 483 released | `82ead2cd5852b5ac673824bf424bf5db0b180f9e744862820d90abdb7d22aef7` |
| 484 released | `a99addefd87fed45a0d25f4a0c892cae975bb2d58335db66e3197ce6a9e2dcd7` |

No full suite/corpus, strict upstream baseline rerun, production rollout, publication
or push is implied by these focused checks.

## Verification recipe

The ordinary system temp mount rejected real Kotlin backup writes with `Disk quota
exceeded`; the initial interrupted attempts are not acceptance evidence. Keep logs
and disposable build/test scratch on the workspace filesystem without deleting other
sessions' files or changing production data-file paths:

```sh
mkdir -p build/verification/catalog-72aae19/tmp
JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }-Djava.io.tmpdir=$PWD/build/verification/catalog-72aae19/tmp" \
TMPDIR="$PWD/build/verification/catalog-72aae19/tmp" \
mise exec -- ./gradlew test -Pr07CatalogCandidate -PtrinoVersion=484 \
  --tests '*TestPublished*' --no-daemon \
  -Pkotlin.compiler.execution.strategy=in-process -Pkotlin.incremental=false
```

Use `-PtrinoVersion=483` for the other target, and `-PtestJavaVersion=26` for the
other supported test VM. Test fixtures honor `java.io.tmpdir`; no tests are disabled
and no native bytes, expected results or capability guards are changed by this recipe.
