# Crutches And Shortcuts

This initial inventory records the safety restriction introduced for RV-03. It is not an audit
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
worker filesystems, or introduce a private storage format. Shared ownership remains RV-04;
the import-versus-orphan-sweep race remains RV-X03.

**Removal criterion:** obtain an authoritative backend/root-aware identity or same-object API
from the actual configured filesystem, preserving object keys/generations and rejecting
unsupported aliases. Add real backend tests before relaxing a guard. An independent Hadoop
configuration, string lowercasing, or filesystem-wrapper reflection is not a replacement.

**Verification:** native-local tests use a real catalog and a non-`/` filesystem root, including
retained and scheduled references. MinIO tests exercise both S3 aliases with a real orphan
deletion control. Unit tests cover exact-key distinctions, current/legacy enable properties,
Azure container separation, and refusal before deletion after an invalid listing entry.
