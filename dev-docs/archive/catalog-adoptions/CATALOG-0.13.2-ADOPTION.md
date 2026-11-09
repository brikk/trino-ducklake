# Catalog 0.13.2 release-pin follow-up

Both planning modes now consume Maven Central catalog, catalog test fixtures,
replay and SLT **0.13.2** in lockstep. Release source:
`640b38e665d9dba7cd51373640077ad2ff5daafd`, tag `v0.13.2`.

All four Central JARs were downloaded independently, checked against
[`gradle/catalog-release.sha256`](../../../gradle/catalog-release.sha256), and
compared byte-for-byte with the tested 0.13.1 JARs. All are identical. There are
no library-code, replay-API, native-peer or driver-pin changes. Drivers remain
test-only DuckDB JDBC **2.0.0.1** and direct Java Quack JDBC **0.13.0**.

0.13.2 adds upstream qualification of that existing Quack driver. The catalog
publisher reports Java 17 / signed DuckDB 2.0 peer results of **93/93** Quack,
**35/35** catalog direct-driver and **27/27** replay direct-Quack mirrors. Those
are upstream results, separate from this connector's verification.

## Connector verification scope

The [0.13.1 verification](CATALOG-0.13.1-ADOPTION.md) still covers these identical
library bytes: 483/JDK 25 normal suite 1,042 tests, 484/JDK 26 normal suite 1,043,
and 484 experimental checks 91, all passing. It is not represented as a new full
suite execution against the renamed coordinates.

The follow-up reruns focused direct-Quack/MySQL worker, commit-outcome,
configuration/exception mapping, nested-signature, native replay-conformance,
geometry and actual setup-format mirror tests on both Trino targets:

- 483 / JDK 25: **33/33 tests**, zero failures/errors/skips.
- 484 / JDK 26: **33/33 tests**, zero failures/errors/skips.
- `verifyR07Candidate -Pr07CatalogCandidate -PtrinoVersion=484` also attests all
  four 0.13.2 artifacts in experimental mode. Its unchanged 91-case runtime
  qualification is covered by the identical 0.13.1 bytes, not claimed as rerun.

Detekt and `pluginDist` pass for both targets. All four archives pass checksum,
flat-root layout, target-matched Trino dependency, exact 0.13.2 catalog byte and
dependency-exclusion checks. There are no stale catalog/implementation JARs,
experimental planner classes, test fixtures, replay/SLT/SQL tooling, test-only
DuckDB/Quack drivers, JUnit or Trino SPI in the normal plugins.

Archive SHA-256 (`trino-ducklake-<target>-0.5.0-SNAPSHOT`):

```text
8a1756c36b2afe9de24437022495ffab3bbf894ec0083cadc90563ff1b699a90  483.zip
59d97d0b133c6beb69c435368afe4a1b2e2cae8f2177d00152bef2a8fb853c0a  483.tar.gz
8ea4c8a45197612bc8c6b94e445d0a0e06471288f40bbb8f0d14a3322b9cbb77  484.zip
70f08d74995321cf04e0af30b70de4d40e81d477c7a54067484f261eaee470b5  484.tar.gz
```

The automatic artifact-attestation task runs against the new coordinates, and
both plugin archives are rebuilt. Generated XML, Central comparisons and archive
receipts are under `build/verification/catalog-0.13.2/`.

No full corpus is run for this pin-only update. Normal DuckLake 1.0 setup,
default-off experimental reads, all 1.1 write refusals, work budgets and existing
native-MySQL/`require-env` coverage limits remain unchanged. The new upstream
driver qualification does not certify connector deployment/authentication or
close the other experimental rollout gates.
