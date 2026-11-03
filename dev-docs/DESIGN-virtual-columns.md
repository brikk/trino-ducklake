# Hidden-column implementation contract

Public usage and NULL behavior are in [REFERENCE.md](../REFERENCE.md#hidden-columns).
`VirtualKind` in `DucklakeColumnHandle.kt` is the authoritative registry.

## Identity and handles

| Handle | ID | Representation |
| --- | --- | --- |
| MERGE mutation locator | `-100` | Physical row locator; distinct from queryable logical identity |
| `$path` | `-101` | `VARCHAR` |
| `$snapshot_id` | `-102` | `BIGINT` |
| `$file_row_number` | `-103` | `BIGINT` |
| `$row_id` | `-104` | `BIGINT`, logical identity |
| `$file_size_bytes` | `-105` | `BIGINT` |

Real column IDs are nonnegative. Reuse the enum/factories and `virtualKind()` /
`isVirtual()` instead of adding sentinel checks at call sites. The hidden flag, not
omission from handles/metadata, excludes these columns from `SELECT *` and DESCRIBE.
Read-only virtuals must be rejected on write paths.

Logical `$row_id` prefers the reserved embedded Parquet lineage field, even when
a catalog start is also supplied. Otherwise it needs a valid `rowIdStart` plus
the authoritative physical file position. Missing identity is never zero. Ordinary
value/file-position scans do not need a logical ID. Mutation ownership must use its
physical locator, not inferred nonoverlapping ranges of logical IDs.

## Page-source integration

- File-constant virtuals use the constant/RLE injection layer.
- Physical file-position and logical-ID injection happens in-pipeline before delete
  filtering. Position-sensitive scans must not lose rows before those offsets are
  established; preserve the reader's pruning restrictions.
- Inline snapshot values come from each versioned row's begin snapshot. Other
  file-bound virtuals are NULL for inline rows. Candidate identity-only inline scans
  use the same owned physical-version stream, not a separately guessed live count.
- File-size/path metadata does not create a physical retention lease or certify a
  file still exists when a worker opens it.

Do not expose DuckDB unprefixed aliases or add `$filename`/`$file_index` by accident:
they are not current public names. No serialized handle shape or sentinel meaning
may change casually. Preserve split/handle wire and nested/source identity tests.

The [archived design](archive/DESIGN-virtual-columns-history.md) retains the original
decision rationale, alternatives and dated implementation notes.
