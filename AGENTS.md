# Operating rules for agents in this repo

## Consult before big workarounds (HARD RULE)
Do NOT invent large workarounds, architectural crutches, or "clever" bypasses
without checking with the user FIRST. Today we had to undo several doozies
(a temp-Parquet-on-shared-storage inlined-data path that can't work on a
distributed FE/BE cluster; a wrong "JDK-25 ABI break" diagnosis with an
assume-skip; an over-engineered warehouse-dot-dir scratch scheme when the OS
temp dir / a real fix was the answer). When you hit a wall:
1. State the problem and the options plainly.
2. Say which is a real fix vs a crutch, and what each costs.
3. Ask before implementing anything that (a) writes to shared storage,
   (b) assumes FE and BE share a filesystem, (c) skips/disables tests,
   (d) masquerades as another format/engine, or (e) degrades a type/semantic.

## Never blame the harness/system to turn a red bar green
If trino / pg_ducklake / DuckDB pass a test we fail, the bug is almost
certainly ours. Root-cause it (reproduce, read the actual code paths) before
skip-listing. A skip reason must be TRUE and specific, never "environment
variance" hand-waving.

## Prefer failing loud over silently wrong
Guards throw `DorisConnectorException` (which the corpus classifies as an
engine-skip) rather than return wrong rows. Never silently over/under-return.

## Verify against the real thing
The Doris FE runs JDK 17 with a curated jar set (`output/fe/lib`). Test-JVM
classpath issues (e.g. the fat `~/.m2` fe-thrift shadowing parquet-format) are
NOT runtime issues — prove which you're looking at before working around it.

## Testing discipline
- Per-corpus-dir runs (~1 min) are the verification loop; NEVER the full corpus
  unless explicitly asked.
- When a fix is complete and fully verified, commit it before reporting completion
  to the user, and include its commit SHA in the report. Do NOT push unless the
  user explicitly asks.
- Keep `dev-docs/CRUTCHES-AND-SHORTCUTS.md` current — every non-production-clean
  path goes there.

See `jvm/doris-ducklake/dev-docs/` for the plan, friction log, and crutch
inventory.

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
