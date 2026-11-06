# Local catalog 0.16 PgBouncer configuration trial

This is an **owner-authorized exception** to published-only dependencies, not a
release or an adoption of a moving upstream build. Normal builds remain pinned
to Maven Central **0.15.1**.

## Exact local selection

`-PcatalogTrialVersion=0.16.0-SNAPSHOT` selects all catalog/replay/SLT modules and
catalog fixtures exclusively from `mavenLocal` (`~/.m2`). Source:
`acec1f1b0208e545efad19d2afd3c574e870170d` (not pushed). The four JARs must match
`gradle/catalog-local-trial.sha256`; no remote fallback or copied JAR is used.
Output names include `-local-catalog-trial`, separate from released 0.5.2.

The owner will publish final 0.16.0 with additional changes later. **Wait** after
this trial: no push, release, publication or switch of the normal dependency.

## Configuration

The connector exposes these existing catalog pool fields using Airlift durations:

- `ducklake.catalog.min-idle-connections`: default 1; 0..maximum pool size.
- `ducklake.catalog.connection-idle-timeout`: unset preserves Hikari's 10-minute
  default; 0 disables, otherwise >=10s.
- `ducklake.catalog.connection-max-lifetime`: unset preserves Hikari's 30-minute
  default; 0 disables, otherwise >=30s. Keep below PgBouncer `server_lifetime`.
- `ducklake.catalog.connection-timeout`: default 30s; >=250ms.

The connector passes JDBC URL parameters through unchanged. The fixed local
catalog still requires PgBouncer >=1.21 with `max_prepared_statements > 0`
(default 200 since 1.22), or JDBC `prepareThreshold=0`, in transaction mode.
The normal 0.15.1 dependency does **not** contain the transaction-local read-session
isolation fix; use direct PostgreSQL or session pooling there.

## Targeted checks only

```sh
# Normal dependency compatibility: config binding/defaults/validation only.
./gradlew test --tests '*TestDucklakeConfig' --max-workers=2
# Exact local snapshot: same checks plus a real PostgreSQL catalog/driver smoke.
./gradlew test --tests '*TestDucklakeConfig' --tests '*TestDucklakeCatalogPool' \
    -PcatalogTrialVersion=0.16.0-SNAPSHOT --max-workers=2
# Second target: add -PtrinoVersion=484 -PtestJavaVersion=26.
```

Runs are sequential and use `-XX:ActiveProcessorCount=4`. The configuration checks
cover unchanged SDK defaults, nullable timeout defaults, duration conversions,
zero/positive boundaries, min/max pool consistency and exact URL preservation.
The direct-PostgreSQL smoke opens a real catalog/read sessions with the bound pool
settings and a `prepareThreshold=0&ApplicationName=...` URL; PostgreSQL's own
activity view confirms delivery of the URL parameters to its JDBC connections.
It is not an end-to-end PgBouncer test; the owner's catalog suite owns that coverage.

Generated receipts and JUnit XML are under `build/verification/catalog-local-016/`.
No full suite, corpus, packaging or release verification is part of this trial.

Results: normal 0.15.1/483 configuration tests **19/19**; exact local 0.16 trial
configuration/driver tests **20/20 on 483/JDK 25** and **20/20 on 484/JDK 26**.
Zero failures, errors or JUnit skips. Resolved trial JAR paths are all under
`~/.m2`, and all four hashes match the owner's receipt. Work is committed locally
only; wait for the owner's final 0.16.0 publication and adoption instruction.
