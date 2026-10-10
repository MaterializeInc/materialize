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

Admission yielding and peer-grant catch-up at transaction open are implemented.
The next boundary is cheap, validated retry across nonstructural changes, including
compaction bounds and maintained frontiers, both at open and after a lost append.
Retain selected plans and input holds, apply committed diffs and their effects,
and recompute affected admission and cleanup decisions. Do not broaden the
peer-only predicate without handling those effects, or replay a stale candidate.
Own-grant accounting, execution authority and publication barriers remain binding.

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
retry cost and actual compaction-bound advancement separately. Per-object advancement
under contention remains unproven. Catalog upper movement alone is not evidence of it.
The diagnostic also samples finite before/after compaction bounds by object ID.
Verify those witnesses in CI before claiming permission advancement. Initial
admission, deletion and completion are excluded. Physical Persist since and other
objects' progress remain separate observations.
SLT/Zippy timeouts remain unresolved. Do not assume a shared cause with the MV
contention without statement-correlated evidence. Detailed captures and verification
belong in the PR.
Kafka RTR's intermittent explicit CREATE SOURCE planning-revision loss is unresolved.
Protection publication overlaps purification but is not a proven invalidator.
Temporary, fixture-enabled kind/count and revision diagnostics must correlate the
captured revision, actual committed change and rejecting branch before a repair.
CI138634 passes Kafka RTR and verifies both catalog and adapter diagnostic emission,
but captures no rejection.
Remove those diagnostics once the mutation is identified. Keep the transaction guard.
Zippy's growing hydration wait is accompanied by
increasing raw catalog decode work across independent imports. These measurements
are not CPU time or unique replay volume. Preserve historical reads and source
flow control when assessing a repair at that boundary.
Do not attribute these to the acknowledgement bug or claim lifecycle changes solve
them without correlated evidence. Detailed captures and verification are in the PR.

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
Remove temporary probes after the repair is verified. Heavy builds and runtime
verification remain CI-owned, using the existing failing workflows and deadlines.

Preserve the independent DROP-metric retirement observation and paused-index
advancement coverage within the explicit protection-lifetime contract. Keep history
across deployments. The history episode setup can pass while the expected row
remains absent at the deadline.
Temporary fixture-enabled cutoff/outcome logs distinguish a missed visit from a
fresh-cutoff no-op. Keep the assertion and deadline, and remove those logs once
answered. Earlier sampling and catalog restore costs remain PR observations,
not additional investigation campaigns.

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
