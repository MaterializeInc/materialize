# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns the contracts. The
[implementer prompt](20260903_decoupled_coordination_prompt.md) owns current
steering and workflow. The
[archive](20260903_decoupled_coordination_log_archive.md) is historical context,
not required reading or a backlog.

Keep this handoff current in place. Remove resolved or superseded items.
Implementation and validation details belong in the PR, not a diary here.

## Resume here

Implementer session: `2026-09-14-13-05-31-256`.

Index admission landed in `ed751836`, with the restart-fixture update in
`d740a5fc`. Continue concrete integration repairs alongside native warm
promotion, preserving transaction effects and rejecting imports that cannot
support required history.

Never-admitted builtin identities may be recorded at catalog open, with no
protected-history promise until their first selection admits the plan and inputs.
An existing index's first selection for another build is reconstruction and must
preserve committed requirements. Coexistence must preserve the already-serving
deployment's builtin compatibility and visibility, not rely on a global bootstrap
barrier. SQL creation commits definition, selection and protection together.

Promotion uses the existing concurrent-writer protocols, not exclusive Persist
output ownership per deployment. No generic per-output fence or all-output
completion gate is required. Escalate concrete protocol safety gaps, not
overlapping writes that preserve correctness throughout.

Stable ReplicaId integration and deployment-qualified persisted observations are
published through `aa3496d0`. Continue their runtime verification. Membership keys,
scoped settings and plan owners distinguish deployment, while declarations and
carried replicas keep their logical ID. Active reference frontiers exclude the
pending deployment's rows for the same ID. Controller retirement must preserve
peer memberships without changing the established single-deployment MV behavior.
Equal names are not correspondence.

Filter current status to the active deployment, not history. Historical
observations include prewarming and retiring activity. Sum processes within a
deployment before applying existing chart aggregates, preserving chart interfaces
and observations across promotion. No historical serving-deployment timeline is
needed. Direct consumers must retain compatibility with schemas lacking the
deployment column.

Keep the serving controller's existing managed-pin cascade even when a prewarmer
retains that replica ID. Preserve the peer's membership and replica comment.
Prewarming retirement never cascades shared MVs. No broader pinning redesign.

Continue native warm-promotion runtime integration. Readiness uses
local hydration and output progress, with active-deployment reference frontiers,
rather than compute-controller inventory or shared Persist progress. The
replica-set owner's policy distinguishes missing capacity from intentional zero,
including managed ON REFRESH. Keep that boundary and the existing stability,
cutoff and external-authorization semantics.

Shared declarations remain authoritative: acknowledged replica DDL must survive
promotion even if it follows prewarming's snapshot. Initial carryover cannot
replace ongoing reconciliation or force deployments to have identical sets.
Hydration, burst state and reconfiguration progress are deployment-local.
Preserve declaration-backed pins and comments through promotion and
managed/unmanaged conversion, including peers without a local realization.

Empty catalog refreshes no longer submit table writes. Continue the zero-row OCC
fixture investigation at its race setup: protection acquisition can publish
metadata and close the intended oracle window. Preserve its before/after witness
and deadline. Do not change catalog transaction completion semantics for the test.

M2 remains active. Continue integration and upstream reconciliation without
reopening completed native handover, outage, targeted DDL or bounded-throughput
proofs. Evidence and current CI status belong in the PR description.

The released-version upgrade smoke test hits the stable-builtin deletion guard:
26.43's
`mz_catalog.mz_cluster_replica_frontiers` Source becomes a View here. This is the
deferred pre-feature conversion boundary, separate from native handover. Do not
weaken the guard or silently skip the check. Use CI for all CPU/RAM-heavy builds
and tests. Keep local work to editing, lightweight checks and evidence review.

Keep the replacement-MV restart/DROP fixture's explicitly scoped paired
heartbeat/grace override, production defaults and convergence deadline.
Verify temporary-object storage retirement with that same paired fixture timing,
preserving immediate SQL invisibility after promotion.
The renamed index-DROP/reconnect fixture checks dependent results and progress,
not physical replacement counters. Native reuse checks export/dataflow continuity.

Investigate retained introspection-input progress behind the observed prewarming
readiness failure and first-query latency after promotion. Keep readiness and
latency requirements unchanged. Remove failure-only diagnostics once attributed.
The approved simulated-version harness repair scopes the Persist version to its
own clusterd launches, but simulated prewarming still fails to become ready.
Establish its remaining cause without changing written-plan identity or admission
checks. Keep this separate from actual compiled-version handover.
Verify shared workload-class metric enrichment with maintained collection metrics,
not obsolete controller counters. Paused-index checks use committed compaction
bounds because replica observations do not exist with zero replicas.
Publication stalls and missing hydration-history episodes require separate causes,
not blanket fixture rewrites. Keep the earlier unexplained RTR conflict visible.

Admission and retirement, not physical liveness, determine required versions. Preserve true
binary identity separately from the authorized format target, including for
read-only shard initialization and explicit upgrades. Participants follow committed admission
changes live. Admission, retirement and format authorization remain atomic without
a separate membership registry. After durable retirement, zombies may fail on
newer formats. Do not add pre-publication initialization or orphan-cleanup
machinery for this race.

## Additional settled scope

- M2 excludes native warm handover with replica-targeted MVs on managed
  clusters. Reject before promotion, preserving the active deployment and MVs.
  Keep single-deployment behavior and handover for explicitly declared replicas
  on unmanaged clusters. Broader MV pinning semantics remain future work, not
  the core replica identity and membership model.
- For these milestones, `mz_cluster_replicas` shows the catalog's active
  deployment and stays shared and materializable. Own-deployment routing,
  reconciliation and readiness use internal inventory with deployment identity.
  Query-relative public visibility is deferred to the design's future work.
- Address-based external replicas are test infrastructure, not an independent
  warm-upgrade deliverable. Do not borrow the serving deployment's identity.
- DROP INDEX names the objects whose plans the writer rewrote, not still-running
  physical dependents.

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
