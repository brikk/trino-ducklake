# Archive

Dated evidence and superseded documents. Nothing here is a current instruction:
current contracts are linked from the [developer index](../README.md), open work is in
[TODO.md](../TODO.md). Pins, paths and test counts below hold only for their date.

## releases

- [0.5.4](releases/RELEASE-0.5.4.md): catalog write efficiency.
- [0.5.3](releases/RELEASE-0.5.3.md): PgBouncer and pool configuration.
- [0.5.2](releases/RELEASE-0.5.2.md): cleanup progress reporting.
- [0.5.1](releases/RELEASE-0.5.1.md): catalog maintenance.
- [0.5.0](releases/RELEASE-0.5.0.md): Trino 483 and 484; upgrade precautions.
- [483-0.4.0](releases/RELEASE-483-0.4.0.md) and [483-0.3.0](releases/RELEASE-483-0.3.0.md).

## catalog-adoptions

- [0.18 snapshot adoption](catalog-adoptions/CATALOG-0.18-SNAPSHOT-ADOPTION.md): replay halts at DuckDB's implicit 1.1 refusals.
- [0.17 snapshot trial](catalog-adoptions/CATALOG-0.17-TRIAL.md): bounds/write-guard checks before Central 0.17.0.
- [0.16 local trial](catalog-adoptions/CATALOG-0.16-LOCAL-TRIAL.md): PgBouncer pool controls before Central 0.16.0.
- [0.15.0 adoption](catalog-adoptions/CATALOG-0.15.0-ADOPTION.md) and its [snapshot trial](catalog-adoptions/CATALOG-0.15-TRIAL.md): resumable expiry cleanup.
- [0.14.0 adoption](catalog-adoptions/CATALOG-0.14.0-ADOPTION.md): independent DuckLake 1.0 acceptance and stored-format locks.
- [0.13.2 follow-up](catalog-adoptions/CATALOG-0.13.2-ADOPTION.md): release pins and Quack driver qualification.
- [0.13.1 adoption](catalog-adoptions/CATALOG-0.13.1-ADOPTION.md): released artifacts and native setup-format selection.
- [72aae19 handoff](catalog-adoptions/HANDOFF-CATALOG-72aae19.md) with its [hashes](catalog-adoptions/catalog-72aae19.sha256): read-correctness consumer work at `1187848`.
- [Native MySQL probe](catalog-adoptions/MYSQL-NATIVE-PROBE-2026-11-03.md): DuckDB 2.0 metadata-schema failure.

## r07

- [R07 candidate qualification](r07/R07-TRINO-CANDIDATE-QUALIFICATION.md): October 2026 planning, session, cancellation and CDC checkpoints for the opt-in 1.1 lane.

## reviews

- [Review 2026-09-05](reviews/REVIEW-2026-09-05.md): source of the RV finding IDs used in TODO.md.
- [Rectification tracker 2026-09](reviews/TODO-rectify-from-eval-2026-09.md): the eval findings that review reconciled.
- [Code-quality remediation 2026-11](reviews/CODE-QUALITY-REMEDIATION-2026-11.md): work-package commits and WP6 read-path measurements.
- [Trino target verification 2026-10](reviews/TRINO-VERIFICATION-2026-10.md): 483/484 adapter introduction.

## designs

- Earlier versions of current contracts: [maintenance](designs/DESIGN-maintenance-implementation-history.md), [shared-file ownership (RV-04 decisions)](designs/DESIGN-shared-file-ownership-2026-09.md), [virtual columns](designs/DESIGN-virtual-columns-history.md), [catalog backends](designs/CATALOG-BACKENDS-history.md), [local stack](designs/LOCAL-STACK-history.md).
- [Nested field evolution](designs/DESIGN-nested-field-evolution.md): design for the removed non-Parquet readers.
- [DuckDB data-plane moveout](designs/PLAN-duckdb-parity-moveout.md): completed plan.
- [DuckLake 1.0 spec impact](designs/DUCKLAKE_1_0_IMPACT.md), [reuse audit](designs/REUSE.md), [cross-engine write report](designs/REPORT_CROSS_ENGINE_WRITE.md), [temporal partition encoding resolution](designs/REPORT-temporal-partition-encoding-resolution.md).

## surveys

- [Research log](surveys/RESEARCH-LOG.md), [research questions](surveys/RESEARCH-TODO.md) and [August 2026 upstream baseline](surveys/UPSTREAM-TRACKING-2026-08-26.md).
- Comparisons with [DataFusion](surveys/COMPARE-datafusion-ducklake.md), [pg_ducklake](COMPARE-pg_ducklake.md) (kept at the archive root because code comments cite that path) and [awitten1/trino-ducklake](surveys/COMPARE-awitten1-trino-ducklake-plugin.md).

## trackers

- Backlogs replaced by TODO.md on 2026-11-03: [read](trackers/TODO-READ-MODE-before-2026-11-03.md), [write](trackers/TODO-WRITE-MODE-before-2026-11-03.md).
- July 2026 trackers: [read](trackers/TODO-READ-MODE-2026-07.md), [write](trackers/TODO-WRITE-MODE-2026-07.md), [special list](trackers/TODO-jayson-special-list-COMPLETED-2026-07.md), [possible issues](trackers/TODO-possible-terra-issues.md).
