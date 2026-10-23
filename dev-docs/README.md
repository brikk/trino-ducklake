# Developer documentation

Start with the root [README](../README.md) for the supported product surface. This directory keeps
only current engineering contracts, active backlogs, release notes, and the latest dated review at
top level. Superseded plans, completed trackers, and research evidence live in [`archive/`](archive/).

## Current contracts and designs

- [CATALOG-BACKENDS.md](CATALOG-BACKENDS.md) — metadata backend behavior and support.
- [DESIGN-maintenance.md](DESIGN-maintenance.md) — shipped maintenance invariants and limits.
- [DESIGN-shared-file-ownership.md](DESIGN-shared-file-ownership.md) — bounded retained-owner safety.
- [DESIGN-virtual-columns.md](DESIGN-virtual-columns.md) — hidden metadata-column behavior.
- [CRUTCHES-AND-SHORTCUTS.md](CRUTCHES-AND-SHORTCUTS.md) — accepted restrictions and removal criteria.

## Active work

- [TODO-READ-MODE.md](TODO-READ-MODE.md) — concise read-path backlog.
- [R07-TRINO-CANDIDATE-QUALIFICATION.md](R07-TRINO-CANDIDATE-QUALIFICATION.md) — exact temporary
  catalog trial, native SPI evidence and the remaining SQL rollout no-go boundary.
- [TODO-WRITE-MODE.md](TODO-WRITE-MODE.md) — concise write-path backlog.
- [TODO-jayson-special-list.md](TODO-jayson-special-list.md) — strategic/cross-cutting backlog.
- [TODO-uhoh.md](TODO-uhoh.md) — upstream risks to recheck, not committed feature work.
- [REVIEW-2026-09-05.md](REVIEW-2026-09-05.md) — latest repository-wide stable-ID review and
  implementation follow-ups. It is a dated audit, not a duplicate daily TODO list.

## Process and releases

- [RESEARCH-upstreams.md](RESEARCH-upstreams.md) — upstream refresh procedure and current baselines.
- `RELEASE-*.md` — release-specific compatibility and verification notes.

## Archive rule

Move a document to `archive/` when its implementation shipped, its component moved to another
repository, or a newer review/tracker superseded it. Keep a short status banner and update inbound
links. Do not erase evidence from an active correctness finding merely to shorten a live tracker.
