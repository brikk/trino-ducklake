# RV-04: Shared Physical File Ownership

**Status: bounded practical fix implemented; stronger concurrent protocol deferred.** Updated 2026-09-05.
Companion: [RV-04 review finding](REVIEW-2026-09-05.md#rv-04-scheduled-cleanup-is-not-safe-for-shared-physical-ownership).

## Current Practical Scope

**RV-04-D21 (approved decision, supersedes D01):** Check retained owners before our cleaner
deletes a scheduled file. Prevent the ordinary sequential A/B sharing failure without requiring
upstream coordination. The user accepts the residual simultaneous import/cleanup race. A useful
bounded check is not rejected merely because it cannot establish a universal ownership protocol.
`add_files` remains zero-copy and still permits sharing.

**RV-04-D22 (enforced limits):** Never materialize the entire catalog to perform this check.

| Limit | Default / maximum | Meaning |
|---|---|---|
| Selected requests and reference page size | 256 / 1024 | `ducklake.cleanup.batch-size`; one queued batch per call, SQL-limited reference pages |
| Combined data/delete reference budget | 1,000,000 / 10,000,000 | `ducklake.cleanup.max-reference-files`; overflow aborts before any deletion/cancellation |
| Path field length | 16,384 characters | SQL projections fetch one overflow character, then reject oversized values; resolved paths are checked too |
| Root/hierarchy sentinels | Two root rows; page-local parent count + 1 | Detect duplicate roots/parents without unbounded parent/history fetches |
| Queue mutation chunks | 64 predicates | Input capped at 1024 selected tuples; identical duplicate rows coalesce and may exceed 64 affected rows |

The reference budget permits one lookahead row to distinguish exact exhaustion from overflow.
Memory is bounded by candidates plus a reference page, not total retained file count. This is
not a bound on database scan/sort time, bytes independent of character encoding, or the number
of physical members removed by one legacy-directory request. Old unbounded catalog APIs remain
for existing consumers, but this cleanup path never calls them.

**RV-04-D23 (implemented planning):** Read one candidate batch, the bounded root, and all needed
reference pages within `catalog.readSession`. Data and delete files use separate numeric keyset
cursors and a shared total budget. Each page resolves only its own latest table/schema hierarchy,
including retired files and dropped-but-unexpired owners. The deletion queue is not counted as
ownership. No candidates means no root or owner scan. RV-03 comparison keys and refusal rules
apply; raw stored paths and deletion targets stay unchanged. Plan the entire bounded batch before
any cancellation or physical deletion.

**RV-04-D24 (implemented cancellation/reclamation):** An exact retained owner preserves the
physical object and cancels the selected obsolete deletion request. Match its original file ID,
path bytes, relative flag, and timestamp; do not cancel by normalized path or ID alone. This
removes the stale instruction that a later stock DuckDB cleaner might otherwise consume.
Normal final-owner retirement schedules a new request, allowing eventual reclamation. Unowned
objects are deleted and then their exact selected requests acknowledged; failed deletions remain
queued. Dry runs perform the same checks but neither cancel nor delete.

**RV-04-D25 (remaining limits):** Simultaneous imports, independent cleaners, path replacement,
external catalogs, and expired-reader leases remain uncoordinated. Stock DuckDB cleanup alone
still has its upstream behavior. Exact shared directories are protected, but prefix-only
overlaps are deferred with a warning and remain scheduled: retiring a child cannot be assumed
to reschedule/reclaim the whole parent. Reconcile those requests before another engine cleans
them. Persistent failed/deferred oldest entries can occupy later batches and need operator
attention. These limitations are explicit, not blockers requiring D10-D20's full protocol first.

**RV-04-D26 (verification and delivery):** Connector commit `fb1a3a9` implements this scope.
Real tests exercise A/B sharing, A's request cancellation,
subsequent stock DuckDB cleanup, final-owner retirement/reclamation, and an S3 alias against
MinIO. Other regressions cover tiny pages, budget overflow before mutation, hostile collations,
bounded root/path projections, whitespace filenames, precise acknowledgments, and directory
overlaps. The connector now consumes the official catalog and corpus-replay `0.9.0` release
from Maven Central, replacing the temporary feature snapshot. The dependency gate is resolved;
see `RV-V11` in the review for published-artifact verification. Earlier development verification
remains recorded under `RV-V10`; its snapshot is no longer a build requirement.

<details>
<summary>Historical full cross-engine proposal, retained for traceability and not a prerequisite for D21-D26</summary>

**RV-04-D01 (previous decision, superseded by D21): Cross-engine design first.** Preserve unrestricted stock DuckDB
interoperability, including shared imports and native maintenance. A Trino-only rejection,
lock, or cleanup check is not a complete fix. This document selects requirements and an upstream
coordination direction, not a storage architecture, schema migration, or trigger implementation.
RV-03's path-comparison repair is independent and necessary, but cannot close RV-04's races.

## Evidence Baseline
- `S/` = `src/dev/brikk/ducklake/trino/plugin/`, Trino connector revision `dcf7105`.
- Trino pins `ducklake-catalog` **0.8.0** in `gradle/libs.versions.toml:16`.
- `C/` = sibling `ducklake-catalog/ducklake-catalog/src/dev/brikk/ducklake/catalog/`;
  inspected sibling HEAD `db75f2b` declares **0.9.0-SNAPSHOT**, not Trino's released dependency.
- `U/` = Trino's `ducklake/` submodule, pinned `c23aca43`; sibling reference checkout is
  `ducklake-catalog/references/ducklake` at `d8a1881e`. Relevant import/cleanup behavior agrees;
  these revisions must not be presented as one release or as evidence of a deployed repair.
- `T/` = `test/src/dev/brikk/ducklake/trino/plugin/`. Tests cited below were read, not run here.
- Handoff `C/JdbcDucklakeCatalog.kt` line hints were registration `4306-4315,4372-4382`,
  expiry `830-925,1126-1172`, list/ack `1194-1208`, partial rewrite `4810-4825`, and superseded
  deletes `5022-5041`. Some have drifted across revisions; use the named functions below as anchors.

## Current Behavior
**RV-04-D02 (observation): Import is registration in place, not copying or durable borrowing.**
`S/DucklakeAddFilesProcedure.kt:168-193,307-317` deduplicates spellings within one call, reads
footers, and commits absolute-path fragments. `C/JdbcDucklakeCatalog.kt`'s `commitAddFiles` /
`applyInsertFragments` allocates new metadata file IDs without checking other physical owners
or pending deletion. Its shared insertion path also serves ordinary writes, flush, and rewrite.
`U/src/functions/ducklake_add_data_files.cpp:492-495` sets `created_by_ducklake=false`, but
`U/src/storage/ducklake_transaction.cpp:64-84` uses that flag for transaction-local rollback
cleanup only. It is not persisted as a never-delete/externally-owned attribute: the data-file
and schedule schemas at `U/src/storage/ducklake_metadata_manager.cpp:202-212` contain no such flag.
Import options at `ducklake_add_data_files.cpp:1271-1281` offer no copy/move/ownership mode.

**RV-04-D03 (observation): Stock behavior explicitly permits sharing and later reclamation.**
`U/test/sql/add_files/add_files_overlapping_globs.test:84-129` reuses paths across tables while
deduplicating a call's overlapping inputs. `add_removed_files.test:26-43` reimports a dropped
table's path. `add_files_compaction.test:41-44,71-98` imports user-created files, compacts them,
then expects cleanup to remove the imports. Imported paths are therefore deletion-eligible;
neither an external location nor a non-`ducklake-` basename implies borrowed ownership.
`T/TestDucklakeAddFiles.kt:104-132` also shares a source file but does not retire its other owner.

**RV-04-D04 (requirement): Cover every producer, not just snapshot expiry.**
The direct schedule producers in the inspected implementations are:

| Producer | Catalog implementation | Upstream metadata-manager implementation |
|---|---|---|
| Expiry of dead data files, including fully expired dropped tables | `expireSnapshots` -> `scheduleAndDeleteDeadFiles` -> `scheduleFile` | `DeleteSnapshots`, data schedule insert at `4923` |
| Expiry of dead delete files, including files attached to removed data IDs | Same helpers, `findDeadDeleteFiles` | `DeleteSnapshots`, delete schedule insert at `4987` |
| Partial/history-replacing compaction | `rewriteDataFilesPartial` -> `scheduleAndDeleteRewriteSources`; handles data and attached delete paths, although current eligibility rejects delete history | `WriteMergeAdjacent:4703-4745` schedules source data paths |
| Superseded snapshot-tagged delete files | `replaceDeleteFiles` -> `scheduleFile`, reached by tagged delete/MERGE and flush replacement | `DeleteOverwrittenDeleteFiles:3833-3868` |

DROP/TRUNCATE, ordinary end-snapshotting rewrites, and plain delete-file replacement feed later
expiry rather than scheduling immediately. Include native/manual and automatic maintenance
callers of these primitives. Orphan sweeps and failed-write/rollback cleanup delete outside the
schedule and must join the same lifecycle audit; importing an uncommitted writer's output is a race too.

**RV-04-D05 (observation): Scheduling is per retired metadata ID, deletion is by path.**
Expiry removes one dead owner's metadata and schedules its path without checking another owner.
`C/JdbcDucklakeCatalog.kt`: `listFilesScheduledForDeletion` / `removeScheduledFileRows` split list/ack;
`S/DucklakeCleanupOldFilesProcedure.kt:79-107` deletes between them with no retained-owner check.
Stock cleanup similarly selects at bind (`U/src/functions/ducklake_cleanup_files.cpp:98-100`),
calls `fs.RemoveFiles` at `137-150`, then acknowledges IDs. A copied schedule list is not revoked
by another transaction deleting its rows. Import A's file into B, retire/expire A, then clean:
B's live data can be deleted. Reimporting an already-scheduled path exposes the same defect.

## Safety Invariants
**RV-04-D06 (requirement): Identify immutable physical objects, not catalog file IDs.**
An ownership key must identify the storage namespace/backend and exact immutable object,
including a generation/version where applicable. Different `data_file_id`/`delete_file_id`
values, table IDs, or catalog IDs do not establish different bytes. Resolve table-relative data
paths and root-relative scheduled paths before comparison, preserving the necessary historical
schema/table hierarchy. Reuse RV-03's backend-aware alias rules; do not normalize arbitrary object
key slashes/escapes. Local aliases, symlinks/hardlinks, and object versions need explicit semantics.
A path alone is sufficient only under an enforced no-overwrite/no-reuse contract, not an assumption.
Re-registering the same existing immutable object is not overwrite/reuse in this sense.

**RV-04-D07 (requirement): Retained references are catalog-wide and history-aware.**
Protect every data/delete object required by any surviving snapshot, considering both the table's
and file's `[begin_snapshot,end_snapshot)` visibility. An end-snapshotted file or dropped table
can still own retained history. Latest-only enumeration is insufficient; conservatively protecting
all remaining metadata rows is safe over-retention. Count physical identity across tables, roles,
and duplicate registrations, not just matching IDs. Replacement of A's historical representation
does not eliminate B's reference to A's original bytes. Unresolvable identity must block deletion.

**RV-04-D08 (scope/open question): One authority cannot discover arbitrary external owners.**
The initial proof boundary is one coordinated catalog over immutable objects. Independent catalogs
sharing paths, external overwrite/reuse, raw storage deletion, and lifecycle policies are excluded
from that proof, not silently made safe. Supporting multiple catalogs needs a common authority,
storage-enforced protection, or independently owned copies. These are unresolved costs and scope
decisions, not permission to ban existing stock DuckDB operations or claim unrestricted completion.

**RV-04-D09 (requirement): Grace periods are not mutual exclusion or reader leases.**
An old imported object keeps its old mtime. Waiting days cannot prevent a new owner from arriving
after a known-file scan. Retained-history protection is distinct from protecting a query whose
snapshot has since expired. Stock scans can cache file lists before later filesystem reads;
no proof has been established that a full metadata-table lock covers that pipeline. The stronger
claims in `DESIGN-maintenance.md` about age alone guaranteeing safety do not establish RV-04 safety.
Specify and verify reader lifetime/expiry semantics upstream rather than inventing such a proof.

## Required Coordination
**RV-04-D10 (requirement): Registration starts before reading the footer.**
For each discovered object, establish protection against deletion before opening/reading metadata;
the protection must remain effective through publication or abort. A glob is discovery, not a
reservation. Verify the immutable generation being read, and ensure batch failure releases only
that operation's protections. All registration paths, including native DuckDB and catalog-library
callers, must participate. Checking only inside `commitAddFiles` leaves pre-built fragments stale.
Stock DuckDB reads full Parquet metadata before `AppendFiles`
(`U/src/functions/ducklake_add_data_files.cpp:168-202,1246-1267`), outside any JVM protection.

**RV-04-D11 (requirement): Publication and reclamation have one ordering point per object.**
Publication must atomically validate its still-valid protection/generation, publish the reference,
and invalidate obsolete deletion eligibility. Reclamation must atomically establish zero retained
references and zero valid registrants before it gains permission to delete. Serializing different
metadata IDs or locking only existing rows does not cover a newly appearing owner. A lost/expired
registrant cannot later commit cached footer metadata: it must fail or reacquire protection and
reread/revalidate the object. Waiting for a SQL lock and then inserting old metadata is not safe.

**RV-04-D12 (requirement): Retirement produces candidates, not unconditional unlink authority.**
Every D04 producer must coordinate reference removal, candidate creation, and later eligibility.
Keep path-resolution evidence before removing historical hierarchy. If another owner remains,
preserve the object and ensure its eventual last retirement can still enqueue reclamation.
Cleanup and orphan discovery must use the same object-wide protocol; protecting only scheduled
files leaves `remove_orphan_files` / `ducklake_delete_orphaned_files` as a bypass.

**RV-04-D13 (requirement): Fence deletion through storage completion and acknowledgement.**
After coordinated eligibility and the required grace, delete only the claimed immutable object,
then acknowledge that exact claim/generation. Do not infer recursive prefix deletion from a
file path. SQL rollback cannot undo an unlink. Duplicate workers, delayed storage requests, lost
connections, ambiguous commit outcomes, and retries must not delete a newly registered/replaced
object. Proven not-found may be idempotent; authorization/outage errors retain retryable state.

**RV-04-D14 (alternatives/open question): Durable state and recovery are not designed yet.**
A durable deletion tombstone could reject late publication after reference rows disappear;
durable import reservations could cover footer reads without holding long SQL transactions.
These are candidate mechanisms, not selected schema. They require operation IDs, fencing epochs,
lease/renewal rules, crash recovery, and an idempotent reconcile/ack contract. Lease expiry alone
does not stop a paused client or a delayed delete already submitted to storage. Tombstone GC must
prove stale registrants cannot return; removing the schedule row immediately is not that proof.
Define recovery after each catalog/storage boundary, including delete-success/ack-failure and
commit-success/client-timeout, before choosing locks, durable state, or a combined mechanism.

## Existing Schedules
**RV-04-D15 (requirement): Reconcile unsafe old schedules before enabling corrected cleanup.**
Inventory and resolve all schedule rows against every retained owner, including historical-only
owners, aliases, duplicate paths, and reimports. Already-missing referenced objects are data-loss
incidents, not successful cleanup. Ambiguous paths must remain protected and diagnosable.
Reconciliation needs an exclusive cross-engine transition: stop/drain or fence old cleaners,
including ones with bind-time cached lists, and coordinate concurrent imports and schedulers.
Cancel/supersede unsafe deletion intents while preserving eventual last-owner collection.
A one-time SQL scrub races; a private "protected" flag is ignored by stock cleanup, which drains
the existing table. Legacy clients must not consume newly interpreted schedules during migration.

## Alternatives And Delivery
**RV-04-D16 (assessment): Partial measures and their costs.** None is selected here.

| Alternative | Benefit and limitation |
|---|---|
| Trino retained-owner recheck or duplicate-import guard | Small local mitigation; races with imports and excludes legitimate sharing if used as a guard. Native cleanup still deletes paths. |
| JVM mutex or database advisory lock | Coordinates willing callers only. Stock DuckDB's generated metadata SQL and filesystem operations do not acquire our lock. |
| Mandatory SQL/table locking | Can serialize conflicting metadata statements, at substantial contention cost; does not by itself reserve objects before footer reads or fence cached cleanup lists and filesystem I/O. |
| Schema triggers/constraints | Could enforce parts of publication for SQL writers, but are backend-specific, affect upstream schema/upgrade contracts, and cannot retroactively protect pre-insert reads or undo unlink. No trigger implementation is chosen. |
| Durable tombstones/reservations | Can express recoverable lifecycle state; needs upstream participation, stale-client fencing, migration, storage semantics, and operational GC. |
| Copy-on-import or borrowed/never-delete ownership | Separates bytes or avoids reclamation, but changes stock semantics; copying costs I/O/storage and borrowed mode needs durable upstream metadata plus external retention responsibility. |
| Disable destructive maintenance / exclusive maintenance window | Operational containment with storage growth or downtime, not unrestricted interoperability and not RV-04 completion. |

**RV-04-D17 (recommendation): Agree the contract upstream, then implement in dependency order.**
1. DuckLake maintainers and engine/catalog owners agree physical identity, sharing, reader/import
   lifetime, deletion authority, stale-client behavior, and upgrade semantics; preserve stock SQL
   workflows. Require changes to native import, all D04 schedulers, cleanup, orphan cleanup, and
   affected rollback paths, not just a new metadata column or a JVM API.
2. Specify backend-neutral begin-registration / validated-publication / retirement / claim-delete /
   exact-ack semantics, capability negotiation, and migration/recovery. Prove them per supported
   catalog backend and filesystem; PostgreSQL advisory locks are not a portable specification.
3. Implement/release coordinated upstream DuckDB and catalog support, then adopt the released
   catalog API in Trino; extend beyond today's split list/delete/ack APIs. Audit Doris,
   pg_ducklake, and other writers/cleaners sharing the catalog. Do not treat sibling HEAD as shipped.
4. Reconcile existing schedules under D15, verify engine-version combinations, then enable the
   new deletion contract. Any temporary guard or mixed-version restriction requires explicit
   approval and must be described as incomplete, not substituted for D01.

**RV-04-D18 (open questions):** Which future stock DuckDB/catalog versions can carry the contract?
If unchanged current stock clients must run indefinitely, their bypasses keep this proposal blocked.
Which storage backends offer enforceable immutable/versioned reads and conditional deletion?
What protects in-flight readers after snapshot expiry, and how are independent catalogs handled?
These need upstream agreement and evidence, not assumptions hidden in a Trino implementation.

## Required Verification
**RV-04-D19 (requirement): Oracle and concurrency tests must prove both safety and reclamation.**
Keep current-stock characterization separate from future-contract acceptance; an unsafe stock
outcome is evidence of the upstream gap, not permission to skip the invariant. Required cases:

- Stock DuckDB sharing, dropped-path reimport, and imported-file compaction/cleanup SQL above;
  verify actual rows and storage objects, not merely metadata counts or successful CALLs.
- Source-to-destination sharing followed by each D04 producer and cleanup, with Trino/DuckDB
  exchanging registrar, scheduler, and cleaner roles; retain a historical-only destination too.
- Reimport before/after scheduling, pre-existing unsafe schedule rows, aliases from RV-03,
  distinct IDs for one object, delete-file ownership, and deletion after the last retained owner.
- Deterministic barriers before footer open, after footer read, before registration commit,
  after candidate-list/bind, after eligibility/claim, during unlink, and before acknowledgement.
- Both race winners, batch rollback, paused/expired registrants, two cleaners, delayed storage
  completion, database/process crashes, ambiguous commits, and restart/recovery at each boundary.
- Native DuckDB orphan cleanup and rollback versus registration; demonstrate that JVM/advisory
  locks alone do not protect it. Exercise a stock scan paused between metadata selection and I/O.
- Real supported catalog backends and shared object storage with independent engine processes;
  test documented multiple-catalog/overwrite boundaries without claiming unsupported safety.

Existing `TestDucklakeExpireSnapshots` checks retirement/cleanup and surviving snapshots, while
`TestDucklakeAddFiles` checks sharing separately. Neither establishes the combined protocol.
Use focused suites and selected corpus directories, never a full-corpus run by default.

**RV-04-D20 (deferred full-protocol gate, not the D21 practical-fix gate):** The stronger guarantee remains blocked until the upstream cross-engine contract
and scope are agreed, every registrar/scheduler/deleter participates, legacy schedules and stale
clients are safely handled, recovery is specified and implemented, and D19 passes against real
stock DuckDB and released dependencies. A design document, RV-03 fix, local guard, or JVM-only
green test is not completion of that stronger guarantee. The practical fix implements none of
that distributed protocol and does not need to claim it.

</details>
