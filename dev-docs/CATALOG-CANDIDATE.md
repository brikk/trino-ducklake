# Experimental catalog planning

## Current selection

Both builds consume released **0.17.0** from Maven Central, with DuckDB JDBC
**2.0.0.1** and test-only Java Quack JDBC **0.13.0**. This adopts the released
library, not default-on final-1.1 planning. The historical timestamp snapshot is
retired. All three modules remain in lockstep:

- `dev.brikk.ducklake:ducklake-catalog`, including its test-fixtures variant;
- `dev.brikk.ducklake:ducklake-test-corpus-replay`;
- `dev.brikk.ducklake:slt-format`.

Repository: `https://repo.maven.apache.org/maven2/`.
All three modules have strict release pins, including fixture self-dependencies.
All four JARs are checked in both builds against
[the tracked SHA-256 receipt](../gradle/catalog-release.sha256).
No sibling build, copied JAR, snapshot or `mavenLocal` substitution is used.

```sh
./gradlew verifyCatalogRelease
./gradlew verifyR07Candidate -Pr07CatalogCandidate
./gradlew test -Pr07CatalogCandidate -PtrinoVersion=484 --tests '*TestPublished*'
```

The flag without a value, or with the exact pinned version as its value, selects
`candidate/src` and adds `qualification/src` tests. Old snapshot/bundle-path values fail
explicitly. Outputs carry `-r07`, separate from ordinary plugin outputs. Omitting
the flag selects `released/src` and normal 1.0 planning, with identical dependencies.

Building this lane does **not** automatically enable final-1.1 access. The separate
catalog setting `ducklake.experimental.enable-final-1.1-reads=true` is default-off,
read-only, and disables connector writes and maintenance procedures. It cannot be
enabled using the normal build: its planning adapter still rejects admission.

Use `ducklake.catalog.format-version=1.0` for normal validation and select `1.1`
only for the separately qualified read-only build. The old developer flag above is
a compatibility alias for selecting 1.1; it is not required for 1.0. A contradictory
explicit 1.0 plus the old true flag is rejected. The required stored-format lock is
enforced by the catalog on reads and writes, not merely inferred from the flag.
Normal corpus tests scope execution and mirroring to 1.0, so native 1.1 feature
SQL cannot interfere with the 1.0 release. 1.1 qualification remains a separate task.

## Implemented scope

- Owned, controlled read sessions and the typed paged planning/inline/history APIs
  are used in the candidate's final-1.1 path. Existing work budgets remain enforced;
  legacy list APIs are not relabelled as SQL-paged.
- The public bounded type-signature parser preserves quoted field names in **both**
  build lanes since release adoption. Paged
  metadata production uses the public quote encoding too; scalar mappings and type
  refusals stay shared with the normal implementation.
- Native nested JDBC values use source-era identity/order mapping, not `toString()`
  parsing or current-position guesses. Text-backed payloads retain their decoder,
  including distinct SQL NULL, literal `NULL`, quoted text and BLOB escapes.
- Scoped local/PostgreSQL inline/Parquet reads cover nested names, types, values,
  NULL/empty containers and retained IDs after rename, on matched two-worker runners.
- Floating/Boolean replay conformance is checked on JDK 25/26. Positive replay tests
  require actual measured mirrors; wrong hash goldens do not count as engine coverage.

Use `-PtrinoVersion=483` for the other target and `-PtestJavaVersion=26` for that test
VM. The actual runtime is asserted, not inferred from captured stdout. Follow
[the development workflow](DEVELOPMENT.md) for fixture prerequisites and temp-quota recovery.

## Remaining qualification boundaries

Release adoption is not experimental final-1.1 production/default-on approval.
A stable artifact name alone does not close these gates:

- Bounded admission/accounting is not complete for all point metadata, views, tags,
  comments, statistics and internal schema/default work. Per-owned-view budgets are
  not total JVM heap, database execution-time or fleet-wide query budgets.
- Actual PostgreSQL cancellation/deadline and recovery have focused checks; worker
  close during synchronous construction, pool acquisition/setup/prepare/network
  stalls, other drivers and arbitrary callback work are not all qualified.
- Physical-file retention/leases and independent-engine cleanup remain uncoordinated.
- Broader legacy fixtures, physical type/encoding combinations, untested backend
  routes, mixed versions and multi-process/multi-host deployment remain separate.
- All connector final-1.1 writes remain disabled. Existing development-format
  read-only restrictions remain; no migration or write admission follows from parsing.

The Trino test client can canonicalize simple mixed-case ROW names when re-parsing
textual types. Exact names are checked at SPI/catalog metadata and DESCRIBE, with
quoted worker reads and an independent native-only control; fields are not renamed.
Local single-process rename fixtures use fresh readers and do not certify concurrent
DDL or cache invalidation.

## Retained evidence

- [Published N03/H15/N19 handoff](archive/HANDOFF-CATALOG-72aae19.md): source/artifact
  identity, exact test selections/counts and plugin hashes at `1187848`.
- [R07 checkpoints](archive/R07-TRINO-CANDIDATE-QUALIFICATION.md): earlier scoped
  session/cancellation/default/CDC work and its unresolved boundaries. Local-bundle
  commands there are historical and no longer select the current build.
- [Restrictions inventory](CRUTCHES-AND-SHORTCUTS.md): active costs, limitations and removal criteria.
