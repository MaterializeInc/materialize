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

The uncommitted integration establishes initial index bounds from input permission
at plan selection, persists selected imports, and derives advancing logical/import
protection without duplicate index requirement records. Existing publishers propose
zero-replica index advancement from input progress. Shared acquisition no longer
depends on installation, and native reconstruction honors current committed bounds.

Bootstrap preparation reuses the main query client locally, retains import holds
through selection, checks issuer liveness in that transaction, and activates the
client only at the existing late handoff. Recovery frontiers come from current
committed requirements, not an invented later timestamp for an unsuitable import.

Next, finish integration verification, including the existing transaction and
zero-replica EXPLAIN fixtures, and land the coherent admission change. Preserve
transaction effects and reject imports that cannot support required history.

Never-admitted builtin identities may be recorded at catalog open, with no
protected-history promise until their first selection admits the plan and inputs.
An existing index's first selection for another build is reconstruction and must
preserve committed requirements. Coexistence must preserve the already-serving
deployment's builtin compatibility and visibility, not rely on a global bootstrap
barrier. SQL creation commits definition, selection and protection together.

M2 remains active. The next end-to-end outcome is same-version native prewarming
and warm promotion, then compatible-version writer coexistence and handover.
The outage, targeted DDL and bounded-throughput proofs remain closed. Continue
concrete integration repairs without starting another acceptance campaign.

## Additional settled scope

- Filter `mz_cluster_replicas` to the current deployment.
  Internal observations retain deployment identity.
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
