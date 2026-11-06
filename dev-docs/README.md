# Developer documentation

User/operator instructions live in the root [README](../README.md),
[REFERENCE](../REFERENCE.md) and [SAMPLES](../SAMPLES.md). This directory is for
maintainers, not a second user manual or an append-only release diary.

## Start here

- [DEVELOPMENT.md](DEVELOPMENT.md): build/test/release commands, dependency boundaries,
  fixture prerequisites and local verification discipline.
- [TODO.md](TODO.md): one current backlog, containing only unresolved work and watches.
- [CATALOG-CANDIDATE.md](CATALOG-CANDIDATE.md): opt-in planning on the published release, scoped
  qualification and remaining 1.1/default-on gates. Normal builds use catalog 0.16.0,
  without promoting experimental final-1.1 planning.
- [CATALOG-0.17-TRIAL.md](CATALOG-0.17-TRIAL.md): exact published snapshot trial;
  normal dependencies remain on 0.16.0 and production planning is unchanged.
- [REVIEW-2026-09-05.md](REVIEW-2026-09-05.md): permanent finding IDs, recorded repairs
  and unresolved security/value/history risks. Kept active because obligations remain;
  its dated observations are not automatically fresh reproductions.

## Current implementation contracts

- [TRINO-COMPATIBILITY.md](TRINO-COMPATIBILITY.md): 483/484 BOM, SPI, parser and packaging boundaries.
- [CATALOG-BACKENDS.md](CATALOG-BACKENDS.md): metadata connection/process topology and qualification scope.
- [DESIGN-maintenance.md](DESIGN-maintenance.md): current maintenance invariants.
- [DESIGN-shared-file-ownership.md](DESIGN-shared-file-ownership.md): bounded owner checks and accepted races.
- [DESIGN-virtual-columns.md](DESIGN-virtual-columns.md): hidden handles, logical/physical identity and injection.
- [CRUTCHES-AND-SHORTCUTS.md](CRUTCHES-AND-SHORTCUTS.md): active restrictions, costs and removal criteria.

## Tools and retained evidence

- [LOCAL-STACK.md](LOCAL-STACK.md): the optional Compose workbench and its actual checkout limitations.
- [RESEARCH-upstreams.md](RESEARCH-upstreams.md): source-backed upstream research procedure.
- [Archive index](archive/README.md): completed handoffs/releases, superseded trackers,
  old surveys/designs and Kotlin-port evidence.

Archive a document once it is evidence rather than current instructions. Carry all
unfinished tasks and necessary caveats into the active backlog/contract first. A
move to archive does not resolve a risk, qualify a route or authorize production rollout.
