# Developer documentation

User and operator documentation is the root [README](../README.md),
[REFERENCE](../REFERENCE.md) and [SAMPLES](../SAMPLES.md).

## Working on the connector

- [DEVELOPMENT.md](DEVELOPMENT.md): build, test and release commands, dependency boundaries
  and fixture prerequisites.
- [TODO.md](TODO.md): the one backlog of open work, including open review findings.
- [CRUTCHES-AND-SHORTCUTS.md](CRUTCHES-AND-SHORTCUTS.md): accepted shortcuts, their cost, the
  real fix and when each can go.
- [LOCAL-STACK.md](LOCAL-STACK.md): the optional Compose workbench.
- [RESEARCH-upstreams.md](RESEARCH-upstreams.md): how to survey DuckDB/DuckLake upstream changes.

## Contracts

- [TRINO-COMPATIBILITY.md](TRINO-COMPATIBILITY.md): Trino 483/484 targets, SPI adapters and
  test SQL tooling.
- [CATALOG-BACKENDS.md](CATALOG-BACKENDS.md): metadata backends, process topology and
  connection pooling.
- [CATALOG-CANDIDATE.md](CATALOG-CANDIDATE.md): the opt-in DuckLake 1.1 read lane.
- [DESIGN-maintenance.md](DESIGN-maintenance.md): maintenance procedure invariants.
- [DESIGN-shared-file-ownership.md](DESIGN-shared-file-ownership.md): cleanup with shared files.
- [DESIGN-virtual-columns.md](DESIGN-virtual-columns.md): hidden columns and row identity.

Release notes, dated evidence and superseded documents are in the [archive](archive/README.md).
