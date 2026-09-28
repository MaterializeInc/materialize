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

The deployment implementation has scoped replicas and managed runtime, shared
explicit declarations, and native prewarming. Align its per-realization IDs with
the design's stable ReplicaId contract before considering replica scoping
complete: preserve declaration/carryover identity, distinguish runtime by
deployment, and allocate fresh IDs for independent creates. Equal names are
not correspondence.

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

The remaining query-client closure investigation concerns long serving-coordinator
publication stalls that may exhaust reclamation grace. Establish the actual
heartbeat/reclamation sequence without changing grace. Keep this separate from
planning-snapshot absence, which now consults the live protection writer.

M2 remains active. The next end-to-end outcome is same-version native prewarming
and warm promotion, then compatible-version writer coexistence and handover.
The outage, targeted DDL and bounded-throughput proofs remain closed. Continue
concrete integration repairs without starting another acceptance campaign.

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
