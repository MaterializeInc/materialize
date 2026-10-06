# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns the contracts. The
[implementer prompt](20260903_decoupled_coordination_prompt.md) owns current
steering. The [archive](20260903_decoupled_coordination_log_archive.md) is
historical context, not a backlog. Detailed implementation and verification
status belong in the PR.

## Resume here

Implementer session: `2026-09-14-13-05-31-256`.

Linear upstream integration retains native ownership on the shared compute/storage
runtime and keeps connection state at the storage boundary. The replica-reported
hydration stability gate uses native query protection and deployment-local
observations. Scheduling timestamps are obtained off the coordinator loop.
History is consolidated on `decoupled-coordination`,
with side-branch work accounted for and original trees preserved in local archival
tags. Verify the combined integration and approved corrections in regular PR CI.
Preserve designer commits and useful implementation boundaries.
Upstream OIDs are preserved and the two unreleased raw sources use fresh OIDs.
Older branch-built catalogs are not a compatibility requirement.

Prioritize the remaining liveness failures in parallel checks and catalog
read-protection restart. Successful protection publications can occupy the
coordinator for tens of seconds. The stale-bound replay correction resamples
after metadata conflicts, but does not explain that successful-path cost or
establish the cause of every timeout. Investigate at the publication/transaction
boundary without weakening protection or extending fixture deadlines.

Keep the scheduled-compaction 1/0/1 membership and audit assertions. Its final
observation uses a fixed sleep and a real-time window. Establish convergence and
the window prerequisite before changing the fixture. The unfinished RBAC-view
SLT and the intermittent DROP rewrite serialization failure also need diagnosis,
not golden rewrites. The imported hydration-stability restart workflows require
their own existing workflow execution, not inference from warm handover.

Comment-ID collision coverage now uses dynamic setup in the SQL integration
harness. Preserve actual collisions and exact comment attribution when adjusting
its setup or cost. EXPLAIN reports selected recovery plans, not necessarily the
running dataflow after imported-index removal.

The missing hydration-history episode remains an intermittent, unattributed
failure. Existing scoped collector logging identifies visits, captured cutoffs
and completed row counts. Use a failing capture before changing retention,
scheduling or episode grouping. Keep history across deployments. Remove the
diagnostic once its question is answered.

Continue concrete integration failures, including catalog-cluster hydration
progress during repeated deployment handover. Keep native ownership enabled,
read protection, external-sink safeguards and compatible-version fresh-environment
handover. The outage, targeted DDL and bounded-throughput proofs stay closed.
Pre-feature environment conversion and independent query-client isolation are
outside M2. Use CI for CPU/RAM-heavy builds and tests.

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
