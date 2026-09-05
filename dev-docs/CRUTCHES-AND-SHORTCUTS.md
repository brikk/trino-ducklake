# Crutches And Shortcuts

This inventory records the safety restrictions introduced for RV-03 and RV-04. It is not an audit
claim that all older debt is listed here; see [the review](REVIEW-2026-09-05.md) for that assessment.

## RV-03: Path Comparison Boundary

**Status: user-approved fail-closed restriction, 2026-09-05.** The alternative was to defer the
repair until Trino exposes an authoritative filesystem-identity API. This implementation never
guesses that unresolved aliases identify different files and therefore permit deletion.

- Native local: `file:/`, `file:///`, and `local:///` use the same root-relative comparison
  namespace when `fs.local.enabled` is true (legacy `fs.native-local.enabled` also works).
- Native S3: `s3`, `s3a`, and `s3n` use the same exact bucket/key namespace when `fs.s3.enabled`
  is true (legacy `fs.native-s3.enabled` also works). Slashes, dots, percent text, and case remain
  literal object-key data. No URI decoding or object-key normalization is performed.
- Other routes: only canonical paths within one identical scheme/authority namespace are
  compared. Azure's container component is included, not discarded as URI credential text.
- Bare paths use Hadoop's configured default filesystem, not necessarily the local disk. Trino
  does not expose that active identity through `TrinoFileSystem`. Any mixture involving an
  unresolved namespace aborts before deletion, even if the differing reference is outside the
  selected scan root. Existing wholly bare-path Hadoop cleanup still runs.
- Non-S3 dot segments, repeated separators, backslashes, ambiguous local authorities, and
  listing namespace changes are refused. Dry runs have the same checks. No tests are skipped
  to hide these refusals; mixed bare/file-URI imports have an explicit refusal regression.

**Cost:** legitimate mixed-namespace or noncanonical layouts may no longer be cleanable through
this procedure until their identities can be established. Do not rewrite metadata paths blindly:
native local URIs are interpreted under `local.location`, whereas bare paths go through Hadoop.
The matcher leaves persisted spellings and filesystem deletion locations unchanged.

**Not a universal physical-identity proof:** symlink/mount aliases, case-folding filesystems,
alternate endpoints, independent catalogs, path replacement, and concurrent import/cleanup need
their own contracts. The helper does not call coordinator-local `toRealPath`, assume shared
worker filesystems, or introduce a private storage format. RV-04 adds retained-owner protection
for scheduled cleanup; the import-versus-orphan-sweep race remains RV-X03.

**Removal criterion:** obtain an authoritative backend/root-aware identity or same-object API
from the actual configured filesystem, preserving object keys/generations and rejecting
unsupported aliases. Add real backend tests before relaxing a guard. An independent Hadoop
configuration, string lowercasing, or filesystem-wrapper reflection is not a replacement.

**Verification:** native-local tests use a real catalog and a non-`/` filesystem root, including
retained and scheduled references. MinIO tests exercise both S3 aliases with a real orphan
deletion control. Unit tests cover exact-key distinctions, current/legacy enable properties,
Azure container separation, and refusal before deletion after an invalid listing entry.

## RV-04: Bounded Practical Cleanup

**User-approved scope, 2026-09-05:** prevent sequential deletion of a still-shared object and
cancel its obsolete selected queue request. The user accepts the remaining simultaneous
import/cleanup race; no upstream agreement is a prerequisite for this bounded improvement.

- One call selects up to 256 requests by default, hard-capped at 1024. Both data and delete
  references are scanned in pages no larger than that batch size, with a combined default
  budget of 1,000,000 references and a configurable hard maximum of 10,000,000.
- SQL path projections include at most 16,385 characters to detect values over the 16,384-character
  limit. Root lookup is limited to two rows to detect duplicates; page-local hierarchy queries
  allow one duplicate sentinel. The reference budget also allows one overflow-detection row.
  These are character/row bounds, not exact byte limits, wall-clock limits, or limits on the
  database's internal scan/sort work.
- Exact selected id/path/relative/timestamp tuples are cancelled/acknowledged in chunks of 64
  predicates. All identical duplicate tuples match; that is not a 64-affected-row guarantee.
- Exact shared-directory requests can be cancelled. Prefix-only overlaps are deferred and left
  scheduled with a warning because a child's later retirement may not reclaim the whole parent.
  Reconcile these manually before running another engine's cleaner.
- A legacy directory request can contain many physical objects. The selected-request cap is not
  a count of recursive filesystem deletions. Independent cleaners, path replacement, external
  owners, and simultaneous import/cleanup remain outside this fix's guarantee.
- Failed/deferred oldest requests remain queued and can occupy subsequent batches. Their cause
  needs operator attention; this implementation adds no retry ledger or queue fairness protocol.

**Release gate:** the connector currently consumes locally verified `0.9.0-rv04-SNAPSHOT` from
the catalog branch `fix/rv04-bounded-cleanup`. This unique version avoids overwriting artifacts
from concurrent catalog work. Catalog API commit: `4cdc725`. It is not published remotely. Merge/release the bounded catalog
API and repin to matching published catalog/replay artifacts before pushing/deploying this
consumer change. Do not replace it with an unbounded fallback to the old 0.8.0 API.

**Verification scope:** real shared-file lifecycle tests cover Trino followed by stock DuckDB
cleanup, last-owner reclamation, and MinIO with an S3 scheme alias. Catalog query tests run on
PostgreSQL, MySQL, local DuckDB, and Quack, including hostile path collations and bounded projections.
The old orphan-sweep full-reference materialization remains the separate RV-25g item.
