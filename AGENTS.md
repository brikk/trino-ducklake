# Operating rules for agents in this repo

## Consult before big workarounds (HARD RULE)
Do NOT invent large workarounds, architectural crutches, or "clever" bypasses
without checking with the user FIRST. Do not replace real catalog/value paths with
shared-storage scratch, assume a JVM ABI failure without evidence, or add a
warehouse scratch scheme where standard build/test temporary storage is sufficient.
When you hit a wall:
1. State the problem and the options plainly.
2. Say which is a real fix vs a crutch, and what each costs.
3. Ask before implementing anything that (a) writes to shared storage,
   (b) assumes coordinator and workers share a filesystem, (c) skips/disables tests,
   (d) masquerades as another format/engine, or (e) degrades a type/semantic.

## Never blame the harness/system to turn a red bar green
If trino / pg_ducklake / DuckDB pass a test we fail, the bug is almost
certainly ours. Root-cause it (reproduce, read the actual code paths) before
skip-listing. A skip reason must be TRUE and specific, never "environment
variance" hand-waving.

## Prefer failing loud over silently wrong
Guards throw an appropriate `TrinoException` rather than return wrong rows. The
corpus may classify an explicitly unsupported capability as an engine-skip; that
is not permission to hide an implementation defect. Never silently over/under-return.

## Verify against the real thing
Trino targets 483 and 484 have separate BOMs, adapters and plugin archives. The build
toolchain is JDK 25; CI also executes tests on JDK 26. A test-JVM classpath conflict
is not proof of a deployed-plugin failure. Verify the selected target, actual runtime
and resolved/packaged JARs before proposing a workaround.

## Detekt metric advisories

Prioritize readable, cohesive code over metric compliance. These enabled Detekt rules
are advisory review signals, not mandatory limits or build/CI gates:
- `MaxLineLength`: 120 characters.
- `LongMethod`: 60 lines.
- `CognitiveComplexMethod`: complexity 15.
- `CyclomaticComplexMethod`: complexity 14.
- `ReturnCount`: 4 returns, excluding guard clauses and lambda returns.

Do not wrap lines, split methods, add trivial helpers, introduce mutable state, or
rewrite clear control flow solely to clear an advisory. Extract helpers when they
express meaningful responsibilities, isolate invariants, or remove duplication.
Advisories require neither suppressions nor baseline entries; keep their findings visible.
Do not mechanically undo existing refactorings: this is policy, not production behavior.

Detekt 2.x uses per-rule `severity: warning` and `failOnSeverity = Error`. Keep all
other rule enforcement unchanged; do not globally enable `ignoreFailures` or disable
lint. If changing Detekt versions, verify warning-only findings pass and an unrelated
error still fails; do not assume this configuration works on older Detekt versions.

## Testing discipline
- Bound bulk maintenance reads with SQL-limited pages/batches and enforce reasonable
  per-call work budgets before destructive side effects. Do not materialize an
  unbounded catalog in coordinator memory.
- Per-corpus-dir runs (~1 min) are the verification loop; NEVER the full corpus
  unless explicitly asked.
- When a fix is complete and fully verified, commit it before reporting completion
  to the user, and include its commit SHA in the report. Do NOT push unless the
  user explicitly asks.
- Keep `dev-docs/CRUTCHES-AND-SHORTCUTS.md` current — every non-production-clean
  path goes there.

See `dev-docs/README.md`, `dev-docs/TODO.md` and
`dev-docs/CRUTCHES-AND-SHORTCUTS.md` for current developer guidance and restrictions.

## Commit identity (Buzz agents)

When you (a Buzz agent) commit on someone's behalf, attribute the commit to the
**human who issued the request**, not to the machine's global git identity.

1. The requester is the signed author pubkey of the triggering Buzz event.
2. Look that pubkey up in [`.buzz/authors.toml`](.buzz/authors.toml) to get the
   git `name` / `email`.
3. Commit as that identity, crediting the model as a co-author:

   ```bash
   GIT_AUTHOR_NAME="<name>" GIT_AUTHOR_EMAIL="<email>" \
   git commit -m "<subject>

   Co-Authored-By: Claude <model> <noreply@anthropic.com>"
   ```

4. **Pubkey not in `.buzz/authors.toml`?** Stop and ask — never silently fall
   back to the machine's global identity. Add the person (reviewed) first.

This matters for a **shared** Buzz agent that acts for several people. A person's
own standalone agent that only ever commits as its owner can ignore it — that
agent's global git identity is already correct.
