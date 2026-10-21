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

M2 verification at the 1s default and history consolidation are complete within
the approved scope. Verification of the approved 60s default remains open.
The asynchronous catalog-owner refactor is not part of M2 and must not resume.

## Verification and closeout

Aljoscha approved a 60s publication default after M2 closeout. Revision `e8084213`
changes that default and the publication-measurement workflow's explicit interval,
without changing deadlines or assertions. CI140146 finishes with 83 passed,
12 failed and one timed-out job, including a clusterd OOM in parallel shard 2.
Separate short-window fixture assumptions from product regressions.
The webhook build superseded manual CI140145 without its seed override, so it
is not the matched comparison requested there. Evidence is under
`scratch/cadence60-default/`. Scratch access is restored. The paused-cluster and
testdrive failures reproduce at 60s. The failed table shard later tombstones
without intervention, so cleanup is delayed rather than permanently stuck.
Scoped 1s fixture settings preserve all assertions and deadlines. Three cluster
workflows, four testdrive files, both SQL logic files, all 22 native library tests
and full adapter-loss recovery pass on scratch. The SQL failures depended on
input compaction, not a REFRESH correctness violation.

CI140149 at `b3555da7` finishes with 93 passed jobs, including both parallel
shards and native recovery/handover. Besides the two deferred exclusions, only
Testdrive 4 fails: introspection's AS OF 0 rejection assumes an advanced read
frontier. The scoped correction in `d1ed7559` has scratch red/green evidence.
Testdrive 3 passes this build. Its earlier inter-file slowdown remains unexplained
and did not reproduce in the short scratch comparison.

CI140154 at `57c5fd56` passes all testdrive and both parallel shards, with 88 passed
jobs after one unchanged restart-proof retry. MySQL/Postgres RTR still fail on
explicit-DDL revision conflicts. Both full workflows pass unchanged on scratch.
A reduced rename transaction reproduces invalidation by six controller
`ClusterRuntime` initialization commits, advancing its revision from 8 to 14.
This matches the original CI startup window, without tracing its rejecting update.
Evidence is in `scratch/cadence60-default/ci140154/runtime-boundary/`.

Awaiting Aljoscha's decision: move managed-to-unmanaged lifecycle validation into
catalog transaction admission, then exclude runtime bookkeeping from SQL planning
invalidation, or exempt only equivalent initialization. Blanket exclusion alone
can bypass the sequencer-only guard after catch-up. The independent boundary review
is `ci140154/cluster-runtime-planning-boundary.md` under the same evidence root.
No production correction is implemented.

The restart fixture assumes that a 1h interval
pauses publication, but startup/reconfiguration stagger from zero. Independent
review identifies this as a pre-existing probabilistic assumption, not evidence
against the 60s default. Keep its assertions and production scheduling unchanged
pending a fixture decision. The full scratch run and unchanged CI retry pass,
without repairing that probabilistic assumption.

The exact CI140146 seed still needs resolution. Its 60s parallel shard 2 OOM
repeats, killing quickstart at about 10.7GiB anonymous RSS. CI140148's 1s control
passes shard 2 but times out in shard 1. Equal parent-cgroup limits were not
directly captured for the control. Existing logs show publication conflicts and
an unchanged ManyRows index bound through recovery, not attribution of memory to
retained history. A bounded scratch initialization at the aggregate CI memory
limit and 8-CPU quota completes without OOM, with quickstart below 2GiB RSS.
It has no swap and different hardware, so it does not clear the CI failure.
Evidence is under `scratch/cadence60-default/ci140146/parallel2-retry/` and
`scratch/cadence60-memory/`.

Do not repeat `CI_HEAP_PROFILES` on the constrained fixture. CI140156's collector
materially perturbs it: the first OOM task table has 189 Python processes with
8.84GB aggregate anonymous RSS, versus one in the unprofiled retry. Detached,
overlapping collectors launch per-request mzcompose processes without timeouts.
The saved quickstart heap has 3.28GB sampled live bytes, including 2.13GB on upsert
allocation stacks, but establishes neither original RSS ownership nor a causal
retention regression. Evidence and limits are in
`scratch/cadence60-default/ci140156/`. A further capture requires bounded direct
requests and observer accounting, not allocator/cache tuning. The prompt's
restriction against changing the 1s default is superseded by approval.

CI140105 at `2bde8879` finishes with 94 passed jobs. Cargo, cluster frontier
checks, Kafka RTR, both parallel shards and the closed native recovery/handover
regressions pass. The only failed jobs are deferred upstream merge skew and
existing-environment source-to-view conversion. The build is not green.
CI140108 passes both shards of the original fixed-seed parallel comparison,
with the same workload, diagnostic settings and normal CI resource budget.
Both captures independently witness successful foreground commits, SQL completion
and advancing committed user bounds. Shard 1 has 310 → 412 logged content commits
in matched 300s windows. Shard 2's previously timed-out MV completes in 20.636s,
with one durable commit. Boot records confirm 8 CPUs and 16,350,281,728 bytes RAM
for both runs, with unchanged workload, backends and deadlines.

This is not uniformly faster: shard 1's JSONB statement takes 22.623s versus
3.879s. Publication-open rejections increase and adapter maintenance wins remain
scarce. The comparison proves observed progress, not starvation freedom or an
isolated causal attribution. Detailed accounting and limits are in
`scratch/combined-publication/ci140108-parallel1/report.md` and
`scratch/ci140108-parallel2/report.md`.

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
contribution. Evidence and limits are in
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

The consolidated history preserves the verified implementation tree and all other
branch/tag tips. Keep successful foreground commits, SQL completion and
committed-bound advancement distinct. Do not infer physical reclamation or a scheduling
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
