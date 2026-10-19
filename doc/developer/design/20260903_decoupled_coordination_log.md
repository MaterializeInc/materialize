# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns the contracts. The
[implementer prompt](20260903_decoupled_coordination_prompt.md) owns current
steering. The [archive](20260903_decoupled_coordination_log_archive.md) is
historical context, not a backlog. Detailed implementation and verification
status belong in the PR.

## Resume here

Implementer session: `2026-09-14-13-05-31-256`.

The active branch is `decoupled-coordination`, linear on upstream `d9a2c5dc40`.
Keep that base pinned. Further upstream integration is explicitly deferred.
Side-branch work is accounted for and original trees are preserved in local
archival tags. Preserve designer commits and useful implementation boundaries
when folding our remaining fixups. Detailed verification belongs in the
[PR status](https://github.com/MaterializeInc/materialize/pull/38696).

Use the dedicated x86_64 scratch worker for focused builds, tests and
interactive debugging. The parallel-check workflow requires SQL Server, which
does not run natively on ARM. Keep the 16-vCPU, 64-GiB, 500-GiB configuration,
one worker, and renew its expiry as needed for this work without asking again.
Current worker: `i-01023ce0e1db179f9`. Last recorded expiry:
2026-10-20 20:27:43 UTC. Verify and extend it before further work.
Scratch evidence is copied off-worker under
`~/.local/share/aj/m2-evidence/scratch/`.
Launch with `bin/scratch create decoupled-coordination
--git-rev <commit>`, then reuse that worker and its caches. The shared editing VM
must not host heavy workloads. CI remains the broad integration check, not the
only place to compile or reproduce a failure. Keep the workload's contention and
protection contracts intact when reducing a reproduction.

Use `bin/ci-builder run stable <command>` on the worker for pinned build tooling.
Start with repository concurrency defaults and adjust from observed resource use.
If a compilation cap is needed, set `CARGO_BUILD_JOBS` inside the builder command,
not just in the host environment. Preserve the reproduction's internal concurrency.
Check `getconf PAGESIZE` is 4096 before running ARM binaries. Keep runtime
reproductions in disposable mzcompose environments and preserve useful evidence
before resetting them. `bin/scratch create` also transfers local AI-tool config
and credentials as part of its standard setup.

Finish [realization-owned protection](20260903_decoupled_coordination.md#client-read-protection)
and [Kafka placement](20260903_decoupled_coordination.md#kafka-placement-and-recovery).
Execution replacement transfers committed grants into opaque recovery holds until
the successor establishes its own protection. DROP/scale-down retire only affected
realizations. Promotion retires older deployments' protection in its authority CAS,
preserving warmed executions. Kafka uses deployment-local placement and fresh,
untimed producer admission. Keep the existing retention, restart and outage fixtures
tied to explicit lifecycle actions, including silence retaining protection.

The serving retirement endpoint durably excludes the explicit target and retracts
only its resources, atomically against promotion. It rejects the active deployment,
preserves raw encodings and fences retired handles before later timestamps.
M2 cancellation covers fresh protected environments and compatible deployments,
using the serving administrative endpoint. Retire the explicit prewarming target
before teardown and use a fresh identity for another attempt. Comprehensive
orchestratord recovery is deferred, including legacy capability discovery,
initial/downtime cancellation, different-version rescue and soft-delete/restore.
Unsupported cancellation must not report success or release protection.
The unverified caller draft is preserved locally at tag
`m2-deferred-orchestratord-retirement` (`5bb35005`), outside the M2 landing history.

Promotion also retracts retired memberships, runtime configuration, replica settings
and replica-owned observation selections in its authority CAS. Raw snapshot rows
preserve pre-migration encodings. Shared declarations, SQL-owned plans, historical
observations and surviving owners remain intact.

The creator-admission acknowledgement fix preserves results even without a changed
catalog projection. Replica conflict coverage now requires real grant changes and
published advancement. The outage fixture checks physical compaction before replica
SIGKILLs, without shortening abandoned Persist reader leases or extending deadlines.
Keep earlier branch-built catalog conversion outside scope.

Repair the nonstructural conflict path under
[cooperating catalog writers](20260903_decoupled_coordination.md#cooperating-catalog-writers).
Replica grant and permission publications contend on the shared catalog upper,
outside the adapter's local DDL lock. Those publications do not invalidate SQL
planning. Requeueing after a failed attempt allows other SQL DDL to change the
planning revision, amplifying a metadata conflict into queueing and replanning.

Common transaction retries rebuild candidates across frontier-only prefixes,
including compaction bounds and maintained frontiers, at open and after a lost
append. Selected plans and input holds remain with their owners. Caught-up effects
survive failure and merge with successful create/bound/drop effects. Verify the
combined path in existing workloads, including queue delay and storage-handle
contention. Own-grant accounting, execution authority, sampled outgoing publications
and publication barriers remain binding. Config candidates with shared precommit
effects are excluded from immediate reconstruction.

The adapter carries its operation-scoped oracle allocation through catalog
admission instead of allocating again after candidate construction. Lower-bound-only
writers retain durable allocation. Preserve future-jump checks and completion of
the actual commit timestamp before acknowledgement. Verify the reduced
refresh-to-commit window in the existing workloads. Oracle retries include
concurrent-update serialization failures, but successful query-await latency and
other coordinator waits still need separate attribution.

Bounded immediate retry work is permitted within the existing transaction path.
Avoid treating a single extra peer-grant-only attempt as the complete repair.
Backoff leaves the coordinator available. The four staged commit paths release
DDL admission on definitive conflict suspension, then reacquire and revalidate.
Fixed-birth acquisition and legacy transaction-end admission ownership stay intact.
Never release admission for an unresolved submitted write. Keep structural validation:
dependency existence alone does not establish unchanged definitions, configuration,
authorization or derived operation sets. No blanket removal of legacy serialization,
new retry framework or general structural read-set machinery is required.

Keep the 1s publication default and use existing workloads to verify SQL completion,
retry cost and actual compaction-bound advancement separately. Finite before/after
bounds demonstrate permission advancement only for the observed objects, not
physical Persist since or progress of other objects. Initial admission, deletion
and completion are excluded. Catalog upper movement alone proves none of these.
SLT/Zippy timeouts remain unresolved. Do not assume a shared cause with the MV
contention without statement-correlated evidence. Detailed captures and verification
belong in the PR.
Protected storage bootstrap preserves persisted output history independently
of writer recovery inputs. Retain same-shard alias accounting and recovery checks,
without legacy cross-shard since joins or dependency holds. The existing persistence
workflow verifies lagging export-to-sink recovery across repeated restarts.
Kafka RTR's intermittent explicit CREATE SOURCE planning-revision loss is unresolved.
Protection publication overlaps purification but is not a proven invalidator.
Temporary, fixture-enabled kind/count and revision diagnostics must correlate the
captured revision, actual committed change and rejecting branch before a repair.
CI138634 passes Kafka RTR and verifies both catalog and adapter diagnostic emission,
but captures no rejection.
Remove those diagnostics once the mutation is identified. Keep the transaction guard.

Catalog MVs use the restricted identity view and shared index. Retained metrics
keep direct raw reads so their reconstruction windows do not pin the shared trace.
Keep general bootstrap admission atomic across the selected index plan, bound and
creator protection, including fixed recovery timestamps and same-build plan reuse.
Replicas execute committed selections, without forced adoption.

Keep allocator/cache/pool tuning paused. The focused subscribe-packing repair
reduces its single-UPDATE buffer from 448 MiB to 224 MiB, with serialization still
allocating 224 MiB. It does not establish whole-workload progress.

The pinned-upstream d9a2c5dc40 and branch comparison both OOM under scratch's
8-CPU/16-GiB cap. That reproduction differs from CI: scratch has no swap, derives
pool budgets from its 64-GiB host, and its selected CPU IDs cover four physical
cores with SMT. CI reports eight physical cores and about 15 GiB of swap, with
observed use. Treat those OOMs as separate stress observations, not a CI reproduction
or evidence of a branch-only memory regression.

With an aggregate eight-CPU quota and host memory headroom, d83f2451 completes
the original shard in 515s versus pinned upstream's 233s. Both execute the same
104 checks in the same launch order, with matching common images and explicit
settings apart from the native-protection parameter. Recorded SQL completion
medians are 1.920s and 0.331s respectively. Native's completed catalog awaits stay
below one second despite substantial throttling. Finite committed compaction
bounds advance. These outcomes do not reproduce or resolve CI's long waits.
Upstream resource capture has gaps, so its sampled memory maximum is not a
matched peak comparison. Evidence is under `scratch/catalog-state-attribution/`
and `scratch/original-shard-cpu8/` in the off-worker evidence directory.

At matched post-workflow age, forty process heaps estimate 24.2 GiB on native
versus 4.3 GiB upstream. Most of the difference is allocated by blob reads and
encoding. Process-0 replicas, which follow the catalog, show the large RSS
difference. Process-1 replicas do not. Followers share the existing process-local
Persist cache rather than creating another cache. An ownership capture records
9.7 GiB cached across 32 follower processes versus 7 MiB across seven process-1
peers, with nearly balanced blob-read completion counters. That run hits the
parked driver file-descriptor error and is not acceptance.
The bounded follower-client A/B remains experimental, not production policy.
The cached control passes in 507s. The uncached candidate fails in 1164s with five
sink-history/statistics SELECT timeouts. Whole-workflow sampled memory peaks are
32.20/28.73 GiB, final surviving-process cache occupancy 9.36/1.08 GiB, and MinIO
S3 egress 9.69/34.02 GB. The candidate runs longer and completes less work, so
traffic totals are not a matched-work comparison. Both use eight CPUs with memory
headroom, not CI's 16-GiB environment. Neither records new OOM events or sampled
full-memory pressure. Captured catalog catch-up remains subsecond in both arms.
These results do not establish a contention fix or the timeout's causal owner.
Preserve the experiment under `scratch/catalog-cache-ab/`, without more cache
tuning. In the preserved failure, `mz_sinks`' durable output and its imported
shared catalog index advance far behind the raw catalog. Native introspection
locates lag at the raw-catalog import into that index. A paired scheduling capture
instead finds the frontier-observation index dominating the same worker:
`mz_cluster_replica_frontiers_ind`'s arrangement consumes 29.4s over a roughly
45s capture interval. A CPU profile identifies repeated batch sealing/extraction,
with strongly skewed inputs. The bounded candidate routes six non-retained
current-view fence lookups through the existing shared catalog index. Sharing
does not establish a memory or SQL-latency improvement. Keep this candidate
provisional rather than treating fewer raw readers as the contention repair.
Original-budget stalls include repeated stale candidates and long catalog
certification while holding admission. The capture separates slow oracle-handle
and snapshot round trips from short backing-oracle reads. Background clients now
reuse the coordinator's live epoch handle without caching timestamps. Cargo
regressions pass and the new traces confirm the handle round trip is gone.
Both original-budget parallel shards still time out on CREATE MV. One victim
spends 292s outside admission and commits none of its 19 candidates while replicas
commit 5,359 aggregate publications during its execute lifetime. Focus on this
foreground/publication asymmetry at the shared catalog boundary, not further
statement-by-statement continuation migrations. In a reduced three-check comparison,
snapshot-local, strongest-first shared traversal lowers median creator expansion
from 3.43ms to 1.66ms. Both arms commit all seven MVs and complete SQL quickly, so
this establishes a narrower validation window, not resolution of CI starvation.
Six focused protection tests and catalog Clippy pass. Original-budget CI139528
passes 4,289 Cargo tests but both parallel shards still fail. Ordinary adapter
expansion falls from 15.074ms to 7.026ms median at similar map cardinality, while
the jsonb victim loses all 11 candidates alongside 5,674 successful replica
publications. Its fetched-target-to-CAS median remains 40.060ms versus 5.216ms
for winning publications. Even a DROP with microsecond validation loses.
The original replica-targeted MV completes in 44.316s with one content commit,
but that is not fleet-wide progress. Hold content-priority/yield work. Carry
incremental local hold accounting through changed per-client frontier publication,
with additive acquisition/creator grants and exact acknowledgement. Then finish
MV admission from its prepared definition and dependencies without reconstructing
unchanged SQL or edges on frontier-only retries. Preserve shared committed-diff
effects. Use a small scaling check and boundary regressions before original-budget
verification. Reassess arbitration only if contention remains after these slices.
Committed bounds advance elsewhere, without proving physical reclamation or
fleet-wide progress. Snapshot waits and append conflicts remain open. A legacy CREATE
CLUSTER retry turn occupies the coordinator for 8.87s. Keep the refresh-to-commit
cost distinct from queue amplification before expanding continuations.
The earlier captured clusterd OOM kill follows job cancellation, not its initiation.
The requested CI queue also accepts larger hosts. Keep exact-instance diagnostic
placement separate from production changes and out of the landing history.
Keep the Python driver's file-descriptor issue parked.
Retained lookups, the two peek-only views and generic batcher machinery are
unchanged. Input-skew amplification remains an observed risk, not a resolved
property. Keep this post-workload diagnosis distinct from CI's long DDL awaits.

Attribute introduced state, allocation churn or overlapping work across the replica
population. The largest existing allocation is not necessarily the regression.
No further tuning campaign or production allocator-policy change.
Preserve consolidation, row ordering, batch frontiers and the wire contract.
Do not turn this into a transport protocol or memory-policy redesign.
Compare actual CI agent resources, not requested queue labels.
Keep the actual CI DDL timeout investigation distinct from confirmed OOM failures.
Those timeouts have no established OOM cause. Preserve historical reads, source
flow control, protection and the original resource budget.
Retained DDL continuations use their newly reacquired turn for fresh certification
and full replanning when preparation is already stale at entry. Definitive failed
attempts and invalidation after work still release admission. Keep that distinction,
without repeated planning loops or dependency-existence shortcuts.
Successful individual content commits and SQL responses do not close workload
starvation. Verify the combined change in existing regressions and keep append
losses separate from queue amplification.
Keep coordinator occupancy separate from lock ownership: an inline source
transaction can delay cleanup of another, already committed admission holder.
Ordinary item rename uses the existing catalog-commit continuation. Verify its
retry yielding and preserve DDL transaction accumulation, COMMIT and rollback.
Keep long individual catch-up and post-commit response delays separate: sequential
client and bounds publication awaits can exclude coordinator command processing
even after a rename's inline completion finishes. Preserve publication barriers,
complete-prefix application and definitive submitted-write outcomes. Do not assume
the late rename explains the earlier MV timeouts.
Reduced decoding and a larger-host pass do not establish original-workload progress.
Keep the driver file-descriptor issue parked. SQL completion and actual
per-object compaction-bound advancement still need separate verification.
Do not attribute these to the acknowledgement bug or claim lifecycle changes solve
them without correlated evidence. Detailed captures and verification are in the PR.

The metric-sink registration panic has a distinct collector-identity collision.
Verify independent live sinks and independent teardown while preserving all companion
observations and same-label predecessor exclusion. Keep this separate from the
parked collector-scheduling change. No retry-deadline increase is needed.
Keep the ON REFRESH compaction-driven reactivation assertion open separately from
the SLT job timeout. Preserve that scheduling contract rather than rewriting its count.

The brittle zero-row OCC timing fixture is retired by design decision. Coverage
no longer directly tests response ordering in the delayed-oracle race. The
production linearization check and existing focused tests remain required.
Parking a group committer does not freeze independent catalog completion. Do not
replace the fixture with a non-witness pass, longer sleeps or a larger M2
synchronization harness. This is a testing-scope limit, not a correctness exemption.

Continue the parallel-workload repairs. Repeated catalog conflicts amplify
client read-protection publication and ordinary DDL into long coordinator-owned
intervals. Measured write and subscribe handoffs overlap these intervals.
Pause further staged-retry expansion. Ordinary CREATE MV now computes its birth
in common catalog admission with definition, selected plan and logical/actual-input
protection, without preliminary grants. Final commit retries retain the immutable
selection and reconsider current compaction permission. Its boundary coverage
does not establish end-to-end workload progress.
The measured ordinary MV uses one successful content commit rather than two.
Its three identified failures cover final admission, not its full SQL lifetime.
REFRESH retains its existing admission and grants, protecting chosen timestamps
through planning without changing warmup or retention. The common boundary is
not a general admission solver for dependent batches of new MVs and indexes.
SQL requires these CREATE statements to run singly.
Verify CREATE MV before changing DROP's written-plan preparation. DROP already
commits selected-import protection atomically. Existing logical-history protection
can make some preliminary grants redundant, but a newly selected index may need
an early grant to preserve its trace through preparation and contention. Existing
owners cannot advance required history merely to accommodate an import. Do not
promise zero preliminary DROP publications or repeat expensive optimization blindly.
Keep necessary commit retries nonblocking and submitted outcomes definitive.
Measure successful catalog commits separately from failed attempts and other I/O,
then verify foreground and frontier-advancement progress in existing workloads.

Implement the [cooperating-writer contention requirements](20260903_decoupled_coordination.md#cooperating-catalog-writers):
incremental catch-up and transaction repreparation, staggered maintenance, bounded
randomized backoff, and retries that relinquish the coordinator. Apply this at
adapter and replica owners. Waiting for new protection keeps the requesting
operation pending, not the coordinator occupied. Revalidate against refreshed
state rather than retrying stale bounds or blindly reusing candidates.

Shared typed snapshots, storage metadata sharing, scoped uniqueness lookups and
incremental OID occupancy are implemented. Temporary item OIDs are covered by
durable rows without an extra catalog-wide scan. Replica retries, adapter
protection maintenance (including timeline windows) and frontend protection
requests yield between attempts.
Maintenance is staggered. DROP retains prepared selections and input holds across
staged metadata-conflict retries. Reuse checks the full preparation revision and
incarnation, and each attempt owns and resolves its publication barrier.
Local invalidation replans the original statement after releasing its DDL lock
and waking waiters. The existing fixture now exercises repeated-conflict
cancellation. APPLY REPLACEMENT retains preparation in its existing cancellable
watch context and invalidates cache entries only after a definitive commit.
SQL CREATE ROLE and COMMENT use a shared retained catalog-commit stage. Startup
role creation retains its separate owner. The existing parallel password fixture
must verify the demonstrated role stall rather than infer success from cheaper
candidate work.
Explicit DDL COMMIT retains extracted operations and completion effects across
retries, with final variable/response completion on success, error or cancellation.
Protocol cleanup precedes continuation creation. The original transaction revision
takes precedence over statement-level validity checks.
Written-plan preparation retains its candidate, immutable selections, current
optimized replacement and earlier holds across single-attempt acquisition retries.
The existing SQL continuations yield and revalidate revision/incarnation on resume.
Only fallback to different imports repeats optimization. Introspection SUBSCRIBE
timestamp admission now yields single-attempt grants. The analogous local initial
MV acquisition continuation is used only by the fixed-birth path, not ordinary
automatic admission or a template for more intermediate stages.
CREATE INDEX also retains its written selection and notices through a staged
catalog commit, rebuilding creator protection per attempt. Verify its dispatcher
yield independently of maintenance success. Client-protection attempts catch up
after allocation, immediately before commit, retaining terminal fencing and yielding on
structural invalidation. Stale opens and CAS losses still occur despite that
refresh and continued dispatch. Sampled bounds keep their original
prefix checks. Adapter candidates remain substantially slower than replica
candidates, although both use the same machinery and their delta sizes differ.
CI138291 verifies exact candidate reuse and its boundary coverage. Duplicate final
application is eliminated for single-batch publications, but workload stalls remain.
Cached-upper rejection avoids most already-losing appends, but catch-up remains
costly. Both adapter and replica owners retain committed grants, pending acquisition
barriers and advancement opportunities. The CREATE INDEX holders dominating
the timed-out MV's DDL queue repeatedly lose content attempts to peer metadata.
Creator and acquisition grants extend committed protection, leaving advancement
and release to aggregate publication. Their small deltas do not close the
plain-view timeout. Aggregate client and bounds maintenance dominate measured
DDL-holder waits without making progress themselves. Follow their repeated
refresh/open failures before choosing another repair. The examined catch-up path
is incremental. A Persist batch-history lookup rescan remains unattributed, not
a justified repair. Do not substitute another grant-size fix or skip freshness.
Keep catalog catch-up cost, end-to-end SQL completion and aggregate advancement
independently open. Removing renewal does not resolve them. Credit publications
only after commit. The plain view's exact queue residence remains unmeasured.
Failed background advancement/bounds publication returns at the existing staggered
publication cadence. Dependency cleanup stays at publication completion, not per
token.
Dequeued DDL retains its guard through freshness and resumes below transaction
setup. Remaining MV waits include other holders' retries and post-planning
revision invalidation. Do not expand the scheduler or promise FIFO.
Creator dry-run admission and catalog catch-up are the measured expensive waits.
The largest cumulative dry-run component in selected holders is Persist listen
fetching, not update application. Some individual waits instead spend most time
acquiring storage. Neither establishes pure I/O or CPU cost.
Creator admission catches up after allocation and revalidates retained work.
Ordinary DDL now derives its creator grant in the real admission candidate,
without a separate preview/open. It retains the oracle minimum, committed grants,
freshness checks and definitive publication barrier. Bootstrap's explicit preview
and reconstruction windows remain unchanged. Verify the combined path in CI.
Slow-call diagnostics separate progress, lease, batch preparation, fetching and
collection without changing their behavior. Identify that cost before choosing a
repair.
Identify publication owners from actual commits rather than inferring their rate
or role from update-batch sizes.
Preserve eager freshness, cancellation, revision checks, transaction-end ownership
and definitive writes. Follow the bounded metadata-retry contract above, without
changing publication cadence or safety checks. No broader scheduling or protocol
change is approved.
The frontier INSERT's year-3000 read wait is separate.
The source-table EXPLAIN timeout remains unlocalized between certification and
grant acquisition. Keep its protection and deadline unchanged.
MV outputs use atomic creator protection alongside indexes, without an extra
content commit. The creation boundary is verified. This closes the identified
output-admission gap, not the unproven attribution of scheduled-MV stalls.
CREATE/ALTER ROLE prepare redacted password verifiers once per logical operation,
not per candidate. IDs, validation and audit effects remain candidate-owned.
This removes repeated hashing, but aggregate candidate timings do not isolate
hashing's cost from scans or scheduling delays.

Keep the 1s publication default while making contention cheap and nonblocking.
Cadence comparisons are deferred. Preserve statement deadlines,
commit-before-use, Persist leases and definitive write outcomes. Verify with the
existing failing workflows, measuring retry work, foreground progress, advancement
and retained frontiers rather than only job success.

Investigate increasing native warm catch-up cost in the existing Short Zippy
workflow. The retained-metrics hydration index reconstructs at its initial
committed bound across deployments. Its imports include the shared catalog shard.
The authority-query filter removes the demonstrated dense catalog feedback.
The remaining measured cost is the two-slot system-cluster hydration queue.
Physical replay volume and the underlying compute cost remain unmeasured. The
30-day metrics policy justifies the retained bound. Preserve committed history,
state layout and readiness. Keep temporary diagnostics scoped to the unresolved
hydration question, then remove them and the fixed Zippy seed.

Preserve atomic creator protection at index admission and early reconstruction
protection through bootstrap. The zero-replica boundary and refresh-MV queries
pass. Bootstrap previews use the same stale-prefix refresh and original planning
revision guard as commit. Common admission still owns the bound, and actual
execution still checks readability. Keep that slice closed rather than expanding
its proof. The remaining one-second EXPLAIN timeout is a separate failure.

Keep the registered-peek DROP stall separate until its blocking await is identified.
Earlier SLT timeout observations
remain in the PR, not additional investigation campaigns.

Resolve the CREATE MV end-to-end wait in the existing parallel
workload. The earlier 292s statement's pre-admission interval remains unpartitioned.
Statement-correlated captures demonstrate cumulative DDL queueing and repeated
commit attempts while holding the guard. CI138291 partitions a successful 200s
CREATE: most retry-ready handoff time is occupied by client/bounds publication,
whose losing durable appends cost more than candidate work. Earlier captures
also identify introspection protection work. Repair these measured catalog costs
before changing scheduling.
Commit-count reduction alone does not close the user-visible latency gap.

Keep the approved Persist-arbitrated OCC contract: subscribe-certified targets,
freshness and future-time checks, refolding at every changed target, actual txns
upper on conflict, and catalog completion before acknowledgement. Catalog-only
completion stages table keepalives without waiting for them on the coordinator.
Bounded client admission applies only to message-bearing rounds, after selected
messages. Timer-only rounds retain their ordering.

Catalog timing probes expand around awaited expressions rather than adding
owning async wrappers along nested refresh paths. Nested timings overlap.
Remove temporary probes after the repair is verified. Use the scratch worker for
the focused debugging loop and CI for combined verification, keeping the existing
failing workflows and deadlines.
Identify the runtime blob backend from the launched service configuration, not
testdrive's independent Persist arguments. The parallel fixture uses S3-compatible
MinIO for its catalog. Attribute the measured blob-fetch waits at that backend,
keeping time inside future polls distinct from the remainder, which also includes
request construction and future destruction. Neither is CPU or disk time.
`Tasked` polls the measured response on main Tokio, independently of the awaiting
coordinator. Reused Hyper drivers can belong to main or Persist's runtime.
Preserve the wrapper's protection against callers that stop polling.
Exact request-ID correlation shows a short server interval and longer client
header interval on scratch, but does not identify the delay owner or explain CI.
The CI138877 parallel-1 seed still times out with ordinary `info` logging on
actual 8-CPU/16-GB agents in CI138893. Verbose diagnostics are not the sole cause.
The larger-agent control pass does not isolate logging cost. Use ordinary
seed and agent selection, and keep broad diagnostics opt-in. S3 timing uses the
separate `mz_persist::s3_read_diagnostics` target to avoid enabling Listen traffic.
No scheduling repair is selected.
Collector scheduling and expansion caching remain parked.

The hydration-restart failure completes bootstrap and then lacks qualifying output
for system-cluster collections. Its logged zero is a readiness fallback, not a raw
execution observation. The existing capture cannot distinguish missing reports
from unhydrated execution or establish lost progress across restart. Use existing
runtime observations in the same failing workflow before changing readiness.
The fixture records compute-owned hydration counts only after a failed wait,
with a bounded scrape that preserves the original failure. A passing scratch
run with hydrated compute and usable observations does not explain the CI failures.

Preserve the independent DROP-metric retirement observation and paused-index
advancement coverage within the explicit protection-lifetime contract. Keep history
across deployments. The history episode setup can pass while the expected row
remains absent at the deadline. The sampler promises approximate freshness, not
completion within the fixture's retry budget. Serial visits and per-visit retention
also exist in the pinned baseline. Delayed collection alone establishes neither
a semantic regression nor the cause of broader contention. Cleanup rescheduling
is parked. Do not redesign the collector or extend deadlines to pass this assertion.
Keep the fixture-timing issue separate from the main catalog-contention repair.
Temporary cutoff/outcome logs remain until the observed episode is fully accounted
for. Earlier sampling and catalog restore costs remain PR observations, not
additional investigation campaigns.

Native ownership, read protection, external-sink safeguards and compatible-version
warm handover remain required. The outage, targeted DDL and bounded-throughput
proof scopes stay closed except for the changed protection-lifetime boundaries.
Pre-feature conversion and independent query-client isolation are outside M2.
The adapterd/controllerd split and SQL adapter lifecycle
belong to M3, not this repair or M2 acceptance. Surface consequential ownership or
ordering changes before implementing them.

## Historical reference, only when changing these areas

These are accepted boundaries, not additional implementation tasks:

- [Iceberg missing-progress behavior](20260903_decoupled_coordination_log_archive.md#2026-09-14-sink-admission-and-missing-progress-decisions).
  Kafka placement and admission follow the current design, not the archived
  heartbeat-based takeover rules.
- [Administrative writes](20260903_decoupled_coordination_log_archive.md#2026-09-10-protected-mode-scope-and-administrative-writes-clarified):
  `--force` bypasses the liveness guard, not CAS or deployment fencing.
- [Temporary SQL object cleanup](20260903_decoupled_coordination_log_archive.md#2026-09-14-bootstrap-and-acceptance-boundaries):
  same-generation restart preserves potentially live foreign owners.
  Crashed-owner resources may remain until promotion. No session-liveness
  framework is required.
