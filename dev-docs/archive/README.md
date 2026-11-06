# Retained development history

These records are evidence, not current build/support instructions. Start at the
[developer index](../README.md) or [user guide](../../README.md). Old artifact pins,
branch names, paths and measured test counts retain their original scope/date.

## Recent integration and qualification

- [Catalog 0.17 snapshot trial](CATALOG-0.17-TRIAL.md): current-bounds/write-guard
  regressions and one normal suite; superseded by byte-identical Central 0.17.0.
- [Catalog 0.16 local trial](CATALOG-0.16-LOCAL-TRIAL.md): PgBouncer pool controls,
  unchanged defaults and JDBC URL passthrough; superseded by byte-identical Central 0.16.0.
- [Catalog 0.15.0 adoption](CATALOG-0.15.0-ADOPTION.md): formal dependency and targeted
  expiry/maintenance checks after the [snapshot trial](CATALOG-0.15-TRIAL.md).
- [Catalog 0.14.0 / independent 1.0 acceptance](CATALOG-0.14.0-ADOPTION.md): actual
  CI starter selection, stored-format locks, structural SQL binding and row snapshots.
- [Catalog 0.13.2 follow-up](CATALOG-0.13.2-ADOPTION.md): byte-identical release pins
  and upstream qualification of the already selected Quack driver.
- [Catalog 0.13.1 adoption](CATALOG-0.13.1-ADOPTION.md): released artifacts, native
  setup-format selection, exact corpus alignment and scoped connector verification.
- [Native MySQL re-probe](MYSQL-NATIVE-PROBE-2026-11-03.md): DuckDB 2.0 metadata-schema
  failure, separate from passing Trino direct-JDBC worker checks.

- [Catalog 72aae19 handoff](HANDOFF-CATALOG-72aae19.md): completed N03/H15/N19 consumer
  work at `1187848`, exact bytes, scoped tests and remaining boundaries.
  Current selection/gates: [catalog candidate](../CATALOG-CANDIDATE.md).
- [R07 checkpoints](R07-TRINO-CANDIDATE-QUALIFICATION.md): older planning/session/default/
  CDC trials. Local-bundle commands are superseded, not instructions for current pins.
- [October Trino verification](TRINO-VERIFICATION-2026-10.md): target/tooling implementation
  evidence. Current API/build contract: [Trino compatibility](../TRINO-COMPATIBILITY.md).
- Release notes: [0.5.4 — catalog write efficiency](RELEASE-0.5.4.md),
  [0.5.3 — PgBouncer and pool configuration](RELEASE-0.5.3.md),
  [0.5.2 — cleanup progress reporting](RELEASE-0.5.2.md),
  [0.5.1 — catalog maintenance](RELEASE-0.5.1.md),
  [0.5.0 — 483 and 484](RELEASE-0.5.0.md),
  [483-0.3.0](RELEASE-483-0.3.0.md), [483-0.4.0](RELEASE-483-0.4.0.md).

## Superseded current-document snapshots

- [Pre-cleanup README](README-SUPPORT-2026-11-03.md): retained for traceability, including
  claims corrected by the current user guide/reference. Do not use it as a capability matrix.
- [Backend investigations](CATALOG-BACKENDS-history.md) and
  [local-stack walkthrough](LOCAL-STACK-history.md).
- [Virtual-column design](DESIGN-virtual-columns-history.md),
  [shared-ownership decisions](DESIGN-shared-file-ownership-2026-09.md),
  [maintenance implementation history](DESIGN-maintenance-implementation-history.md),
  [nested-field evolution](DESIGN-nested-field-evolution.md).
- Previous trackers: [read](TODO-READ-MODE-before-2026-11-03.md),
  [write](TODO-WRITE-MODE-before-2026-11-03.md), [cross-cutting](TODO-special-before-2026-11-03.md),
  [upstream watches](TODO-upstream-risks-before-2026-11-03.md).
  Their unresolved items are consolidated in [the active backlog](../TODO.md), not closed.

## Older audit, research and moved-component context

- [Original review reconciliation](TODO-rectify-from-eval-2026-09.md); the
  [live review](../REVIEW-2026-09-05.md) retains current permanent IDs/statuses.
- [August upstream tracking snapshot](UPSTREAM-TRACKING-2026-08-26.md),
  [research log](RESEARCH-LOG.md), [research questions](RESEARCH-TODO.md).
- Comparisons: [DataFusion](COMPARE-datafusion-ducklake.md),
  [pg_ducklake](COMPARE-pg_ducklake.md), [other Trino plugin](COMPARE-awitten1-trino-ducklake-plugin.md).
- [DuckLake 1.0 spec impact](DUCKLAKE_1_0_IMPACT.md), [reuse audit](REUSE.md),
  [cross-engine write report](REPORT_CROSS_ENGINE_WRITE.md),
  [temporal encoding resolution](REPORT-temporal-partition-encoding-resolution.md).
- Older trackers: [read](TODO-READ-MODE-2026-07.md), [write](TODO-WRITE-MODE-2026-07.md),
  [special-list closeout](TODO-jayson-special-list-COMPLETED-2026-07.md),
  [investigation list](TODO-possible-terra-issues.md).
- Moved-component plans: [DuckDB data-plane separation](PLAN-duckdb-parity-moveout.md),
  [Doris extension](PLAN-trino-doris-extension.md); [scan-materialization exploration](THOUGHTS-parquet-soft-scan-materialization.md).
- [Former transpilation-gate notes](NOTE-transpile-first-corpus-gate.md).

## Kotlin-port records

[Plan](kotlin-port/PLAN.md), [port start](kotlin-port/PORT-START.md),
[before-fix handoff](kotlin-port/BEFORE-RESUME.md), [after-slice triage](kotlin-port/AFTER-SLICES.md),
[reconciliation](kotlin-port/RECONCILE-SWEEP.md), [idiom plan](kotlin-port/IDIOM-CLEANUP-PLAN.md),
[resolved misfires](kotlin-port/IDIOM-CLEANUP-MISFIRES.md),
[idiom rationale](kotlin-port/_idioms/idioms.md).
Source/JSON audit artifacts remain in `.ai/kotlin-port`; no outstanding port-era
number is assumed fixed merely because its old plan is archived.
