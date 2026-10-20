# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns the contracts and M2 scope.
The [prompt](20260903_decoupled_coordination_prompt.md) owns working rules.
The [PR](https://github.com/MaterializeInc/materialize/pull/38696) provides a
reviewer-facing summary and current verification. Detailed run records belong
in linked evidence, not in either document.
This is a resume pointer, not a work log. Replace current state and remove resolved
work. Do not append a paragraph for each fix, experiment or verification run.

## Current state

Implementer session: `2026-09-14-13-05-31-256`. Branch:
`decoupled-coordination`, based on `d9a2c5dc40`. Keep that upstream base pinned.

Native adapter-loss recovery, retention and compatible-version warm handover have
runtime evidence. Keep their proof scopes closed and their regular regressions
enabled. Incremental protection publication, removal of the whole-map API,
prepared-MV admission and the replacement-MV replay correction are implemented.
Cheaper preparation has not resolved foreground catalog-write stalls.

M2 remains open on progress under contention and remaining integration failures.
Replica publications can keep committing while foreground statements lose their
attempts and time out. Long inline retries also occupy coordinator dispatch.
Keep commit conflicts, queueing and coordinator occupancy distinct.

## Current comparison and decision point

Replica advancement/release and compaction-bound publications now reschedule
definitive conflicts at the normal staggered cadence. Grant acquisition is
unchanged. Focused checks pass. The matched fixed-seed comparison on actual
8-CPU/16-GiB hosts shows fewer publication attempts and better foreground
progress, but one shard still times out on SQL. Committed bounds advance.
The ALTER SINK trace identifies transaction-open rejection of committed updates
not yet applied to the owner's projection, followed by candidate conflicts.
Its inline retry blocks dispatch even without holding DDL admission.

The completed comparison is under `scratch/ci139787/parallel{1,2}/report.md`.
The bounded repeat reproduced a 321-second append with nested blob-upload retries
under shared memory pressure. MinIO sizing does not see the parent cgroup limit.
This is scratch evidence, not an explanation of CI timeouts. Its CPU capture
identifies unnecessary protection-index reconstruction during empty storage
cleanup. The correction is in `0286e789`. Formatting, catalog Clippy and all
35 catalog transaction tests pass on scratch.

Further [asynchronous adapter catalog ownership](20260903_decoupled_coordination.md#adapter-catalog-ownership)
work is on hold at Aljoscha's request. The owner is not implemented. The bounded
table-lifecycle corrections are implemented and reviewed, with verification below.
Do not resume owner work. These correctness fixes are not demonstrated
repairs for the original contention. The Persist-traffic investigation is separately
owned and retains its worker.

The caller inventory and reviews are under `scratch/async-catalog-ownership/`.
Prerequisites through `c984bee8` pass formatting, scratch adapter Clippy, five
group-committer tests and two DDL-completion tests. Lifecycle revision `68a45ac0`
and its compile cleanups through `45a37509` pass affected-crate Clippy, including
test targets, and 44 distinct boundary tests. The reviewed correction makes finalization
obtain definitive, shard-addressed Forget through the existing committer after
bootstrap. SQL DROP only detaches local routes. Read-only adoption shares bootstrap
schema descriptions and leased-read installation without writable oracle or
user-table registration. Tests cover peer/alias detachment, lost retirement
completion, physical closure ordering, independent finalization eligibility and
read-only schema adoption.

Three existing environmentd bootstrap/builtin-migration regressions pass on an
unchanged rerun. The initial run timed out in independent-oracle bootstrap and
migrated-MV readiness, while the flag-off migration test passed. Those two initial
timeouts remain unexplained. No deadlines, correctness assertions or production
settings were changed. This is not a contention comparison or full M2 verification.

If owner work resumes, use the approved ownership contract and existing fixed-seed
comparison. Dispatch responsiveness alone does not establish successful foreground
commits, SQL completion or compaction-bound advancement. No owner contention
comparison or traffic experiment is currently running here. Shared content-priority
signals and arbitration changes remain unapproved.

## Other work and scope

Keep separate failures visible in the PR, including unexplained statement-logging,
HTTP-session cleanup and other SQL timeouts. Do not assume they share the
contention cause. Allocator/cache/pool tuning, collector rescheduling and the
diagnostic driver's file-descriptor issue remain parked.

Existing-environment conversion and further upstream integration are deferred.
M3 owns adapterd/controllerd and independent serving adapters. M2 cancellation
uses explicit retirement through the serving administrative endpoint for fresh
protected environments and compatible deployments. Comprehensive orchestratord
recovery is deferred. Its draft is preserved at
`m2-deferred-orchestratord-retirement` (`5bb35005`), outside the landing history.

The retired zero-row OCC timing fixture's response-ordering coverage gap is
accepted. Keep production linearization checks and focused tests, without
building a larger synchronization harness for M2.

## Worker and evidence

The traffic investigation retains `i-01023ce0e1db179f9`. Leave it untouched.
Aljoscha authorized a second worker for bounded lifecycle verification:
`i-069b0283acccc3174` (`table-lifecycle`), m8i.4xlarge, 16 vCPUs, 64 GiB RAM,
500 GiB disk, expiry 2026-10-22 07:40:25 UTC. Verification covers adapter,
storage-client, storage-controller and compute-client libraries and tests.
Clippy and boundary tests pass on `45a37509`. Runtime results and the initial
failures are preserved off-worker. No build or test process remains running.
Use `bin/ci-builder run stable <command>` for pinned tooling.
Saved evidence: `~/.local/share/aj/m2-evidence/scratch/`.
Lifecycle verification evidence: `scratch/table-lifecycle-verification/`.

Scratch's 16-GiB cap is not a faithful CI reproduction: swap, host-derived buffer
budgets and CPU topology differ. Both upstream and the branch OOM there.
Verify effective configuration and actual CI host resources, not queue labels.
The native debug-test startup failure reproduces before the publication change
at a 4-MiB stack. An 8-MiB diagnostic override passes both revisions, but is not
a production setting or substitute for optimized CI verification.

For specific settled questions, consult the archive's
[Iceberg missing-progress decision](20260903_decoupled_coordination_log_archive.md#2026-09-14-sink-admission-and-missing-progress-decisions),
[administrative-write boundary](20260903_decoupled_coordination_log_archive.md#2026-09-10-protected-mode-scope-and-administrative-writes-clarified)
and [temporary-object cleanup scope](20260903_decoupled_coordination_log_archive.md#2026-09-14-bootstrap-and-acceptance-boundaries).
The archive is not a backlog. Current design contracts supersede its proposals.
