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

Prioritize the parallel-workload stalls. Repeated catalog conflicts amplify
client read-protection publication and ordinary DDL into long coordinator-owned
intervals. Measured write and subscribe handoffs overlap these intervals.
Transaction opens, losing CAS calls and conflict synchronization dominate the
longest measured publication, not one oracle wait or successful completion.
The bounded retry trim rejects stale opens before snapshot construction and
refreshes through the already-synchronized conflict prefix. It does not establish
workload liveness. Do not reuse candidates across arbitrary metadata changes or
retry stale bounds.

The approved cadence correction coalesces advancement-only replica aggregates at
the existing publication interval. New or stronger protection still commits before
use, delayed releases retain committed protection, and heartbeat renewal remains
independent. The current 1s interval still has demonstrated runtime contention.
A single 5s comparison in the existing workloads has been requested, not approved.
Keep heartbeat/grace, retry policy and deadlines unchanged. Measure contention and
retained frontiers before proposing a default change, not a fixture-only slowdown.
Keep this correction separate from the conflict-work trims.

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

The shared adapter client also loses protection after renewal repeatedly fails
for longer than the unchanged-heartbeat grace. Earlier query timeouts precede
that loss. No intervening successful renewal or premature reclamation is shown,
although the reclaiming peer and exact commit are unlogged. Fix publication
progress, not grace or closure checks. Keep the registered-peek DROP stall
separate until its blocking await is identified. Earlier SLT timeout observations
remain in the PR, not additional investigation campaigns.

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

Preserve the independent DROP-metric retirement observation and the unchanged
paused-index advancement assertion. Keep history across deployments. Earlier
hydration sampling and catalog restore/readiness costs remain observations in
the PR, not additional investigation campaigns.

Native ownership, read protection, external-sink safeguards and compatible-version
warm handover remain required. The outage, targeted DDL and bounded-throughput
proof scopes stay closed. Pre-feature conversion and independent query-client
isolation are outside M2. Surface consequential ownership or ordering changes
before implementing them.

## Historical reference, only when changing these areas

These are accepted boundaries, not additional implementation tasks:

- [Sink takeover and commit/recovery rules](20260903_decoupled_coordination_log_archive.md#2026-09-14-cutover-decisions-approved-by-aljoscha),
  including provisionally accepted five-minute abandoned-incarnation Kafka
  takeover, not a production failover target or a reason to add another lease.
- [Kafka pre-open admission and Iceberg missing-progress behavior](20260903_decoupled_coordination_log_archive.md#2026-09-14-sink-admission-and-missing-progress-decisions).
- [Administrative writes](20260903_decoupled_coordination_log_archive.md#2026-09-10-protected-mode-scope-and-administrative-writes-clarified):
  `--force` bypasses the liveness guard, not CAS or deployment fencing.
- [Temporary-owner cleanup](20260903_decoupled_coordination_log_archive.md#2026-09-14-bootstrap-and-acceptance-boundaries):
  same-generation restart preserves potentially live foreign owners.
  Crashed-owner resources may remain until promotion. No session-liveness
  framework is required.
