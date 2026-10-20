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

Next, implement [asynchronous adapter catalog ownership](20260903_decoupled_coordination.md#adapter-catalog-ownership).
Refactor the existing live catalog owner, not a separate maintenance follower or
detached Persist appender. This includes foreground scheduling changes needed to
keep dispatch available, rather than only moving periodic callbacks. Establish
ownership of locally mutable catalog state and cover durable callers such as
refresh, allocation and catalog-upper advancement. Bootstrap and prewarming
semantics remain intact.

Reuse transaction and request-completion machinery. Keep pending protection,
definitive write completion and ordered committed effects consistent across the
boundary. Do not transfer blocking to a shared mutex or an inline reply wait.
General retry frameworks, shared arbitration, SQL behavior changes and the M3
process split remain outside this correction. Surface disproportionate scope
rather than expanding silently.

After focused ownership/concurrency review, run the existing fixed-seed comparison.
Measure SQL completion, dispatch occupancy, committed-bound advancement and
memory/I/O cost separately. Preserve deadlines, the 1s cadence and workload
resources. Removing dispatch blockage does not establish shared-write progress.

Evidence: `scratch/maintenance-append-repeat/report.md`, `cpu-analysis/report.md`
and `minio-sizing.md` within that directory. No further experiment or resource
tuning is underway.

This comparison is the decision point:

- If the contention failure is resolved, continue combined verification, remaining
  integration repairs and history cleanup. Do not expand M2 acceptance.
- If foreground stalls remain, bring Aljoscha the evidence, remaining uncertainty
  and a recommended correction before expanding beyond this ownership refactor.
  Distinguish dispatch responsiveness from shared-write arbitration. State the
  tradeoff in SQL progress, compaction delay/retained history and added coordination.
- An inconclusive run permits a bounded diagnostic follow-up, not an assumed
  fix or an open-ended instrumentation campaign. Escalate if obtaining a useful
  comparison itself requires substantial work.

Shared content-priority signals and arbitration changes are not approved.

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

Scratch worker: `i-01023ce0e1db179f9`, last recorded expiry
2026-10-21 05:41:44 UTC. Verify its state and renew within the prompt's approved
budget. Use `bin/ci-builder run stable <command>` for pinned tooling.
Saved evidence: `~/.local/share/aj/m2-evidence/scratch/`.

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
