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

M2 is at verification closeout, pending analysis of the controlled comparison.
No further implementation is currently identified as necessary. The asynchronous
catalog-owner refactor is not part of M2 and must not resume.

## Verification and closeout

CI140105 at `2bde8879` finishes with 94 passed jobs. Cargo, cluster frontier
checks, Kafka RTR, both parallel shards and the closed native recovery/handover
regressions pass. The only failed jobs are deferred upstream merge skew and
existing-environment source-to-view conversion. The build is not green.
CI140108 passes both shards of the original fixed-seed parallel comparison,
with the same workload, diagnostic settings and normal CI resource budget.
Capture analysis of foreground commits and committed-bound progress is pending
under `scratch/combined-publication/ci140108-parallel{1,2}/`.

Publication scheduling and atomic maintenance are implemented. Immediate replica
grants preserve the maintenance deadline. Combined client/recovery requirements
and bounds retain validation, CAS and definitive completion, including due bounds
across installation gates. Affected-crate Clippy and 39 focused boundary tests
pass. Review and red/green evidence are under `scratch/combined-publication/`.

The matched 60s comparison of `a02f782b` and corrected revision `2bde8879` completes
all 180 rounds in both arms with the same pinned driver, backends and uncapped
scratch resources. All three continuously observed user read frontiers advance
during sustained work. Peak active-work write-minus-read lag falls from 463s to
111s. Persist write payload is 0.286 versus 0.276 MB/s, with 131 versus 108
successful catalog commits. The comparison does not isolate each change's traffic
contribution. Production defaults remain unchanged. Evidence and limits are in
`scratch/combined-publication/report.md`, not a claim of resolved CI contention.

The publisher refreshes and enacts the catalog prefix before sampling, preserving
fencing and conflict checks. All three unchanged cluster frontier regressions
pass on scratch and CI. The oracle-allocation race remains, so this is not a
starvation-freedom guarantee.

CI140090's parallel-shard timeout remains unexplained rather than attributed to
the publisher correction: SQL progress stopped for about 18 minutes, while
watchdog warnings, publication conflicts and late Persist delays did not identify
the blocking owner. No logged OOM or process exit established a cause. Evidence
is under `scratch/combined-publication/ci140090-parallel{1,2}/`.

The independent table-lifecycle corrections remain implemented and reviewed.
Finalization requires definitive, shard-addressed Forget through the existing
committer. Local adoption/detachment is separate from shared retirement and write
authority. Affected-crate Clippy and 44 boundary tests pass. Three existing
bootstrap/builtin-migration regressions pass unchanged on rerun, with two initial
timeouts still unexplained. Evidence is under `scratch/table-lifecycle-verification/`.
These fixes are not demonstrated repairs for the original contention.

Finish the two capture analyses, then close out the evidence, PR summary and
implementation history. Distinguish successful foreground commits, SQL completion
and committed-bound advancement. Do not infer physical reclamation or a scheduling
guarantee from those results, or reopen closed proof scopes. Shared content-priority
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
failures are preserved off-worker. The lifecycle fixture is stopped. This worker
is now used for the publication slice above.
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
