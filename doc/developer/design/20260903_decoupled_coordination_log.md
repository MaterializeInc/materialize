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

Prioritize the remaining read-then-write timeouts in parallel checks. The
catalog-only completion regression passes: table keepalives remain staged but
are not awaited on the coordinator. Current failing DML lacks evidence of its
OCC permit ownership, selected timestamp and subscribe progress. Scoped RTW
debug events enabled only for parallel CI distinguish those waits. Remove the
diagnostic once attributed. Neither this correction nor stale-bound resampling
establishes the cause of every timeout. Keep protection and deadlines unchanged.
The restart fixture brackets sink admission before catalog diagnostics to avoid
consuming its physical-lag window. That adjustment also needs CI verification.

Keep the scheduled-compaction 1/0/1 membership and audit assertions. Its final
observation uses a fixed sleep and a real-time window. Establish convergence and
the window prerequisite before changing the fixture. Streaming statements
captures the compaction window in the timestamped job log. RBAC-view SLT
completes in CI137762, so its temporary streaming diagnostic is removed.
The imported hydration-stability
restart and no-dataflow workflows pass in CI137762, alongside warm handover.

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
