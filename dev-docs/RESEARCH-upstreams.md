# Upstream research procedure

`vendor/` holds ignored, read-only research checkouts; none is built or shipped here.
The configured projects/remotes are in [`vendor/clone-related-projects.sh`](../vendor/clone-related-projects.sh).
The `ducklake` git submodule is a separate pinned test fixture, not a research baseline
to update implicitly. Authoritative current dependency pins are in
[`gradle/libs.versions.toml`](../gradle/libs.versions.toml).

## Refresh procedure

1. Read the last dated survey in [the research log](archive/surveys/RESEARCH-LOG.md) and
   [the retained August baseline](archive/surveys/UPSTREAM-TRACKING-2026-08-26.md#latest-baselines).
   Those are survey checkpoints, not claims about today's upstream release state.
2. Inspect [the upstream waits](TODO.md#upstream-duckdbducklake-waits).
3. Fetch missing/existing mirrors without pulling or changing their checked-out refs:

   ```sh
   ./vendor/clone-related-projects.sh
   git -C vendor/ducklake log --oneline <surveyed-sha>..origin/<branch>
   git -C vendor/ducklake diff --stat <surveyed-sha>..origin/<branch>
   ```

4. Inspect actual remote refs and relevant release branches; do not assume a historical
   branch name is still the active release line. Compare submodule gitlinks explicitly
   when they move. Fetching does not populate their working trees.
5. Triage substantive catalog/schema, identity/history, type/encoding, pruning, inline,
   deletion, maintenance, view, partition/sort and catalog-backend changes against
   current code and exact artifacts. Dependency/tag announcements alone are not fixes.
6. Classify findings as existing parity, a real gap, a source-backed risk needing
   reproduction, or documentation-only. Preserve that distinction: no unproven skip,
   silent type degradation or "environment variance" explanation.
7. Add only genuinely unresolved, actionable items to [TODO.md](TODO.md), linking
   evidence. Do not clone a parallel backlog or reopen archived work by its old label.
   Large investigations can have a focused report in `dev-docs/` while active.
8. Record date, exact refs/SHAs, repositories actually inspected, relevant evidence
   and scope in an archived survey/log entry. Tag-only checks must not be described
   as source audits, native tests or consumer qualification.

## Boundaries

- Do not use `--pull`, migrate catalogs, repin a corpus/native producer, publish,
  enable unsupported reads/writes or push merely as part of a survey.
- Data-plane DuckDB/Vortex/Lance work is not this connector's catalog-plane backlog.
- If library changes need consumer adoption, use exact published coordinates and
  verify variants/transitives/bytes before claiming the connector uses those fixes.
- A successful parser or oracle-only test is not actual engine mirror coverage.

Historical comparisons and earlier procedural detail are retained in [archive](archive/README.md).
