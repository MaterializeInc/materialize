# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns the contracts. The
[implementer prompt](20260903_decoupled_coordination_prompt.md) owns current
steering. The [archive](20260903_decoupled_coordination_log_archive.md) is
historical context, not a backlog. Detailed implementation and verification
status belong in the PR.

## Resume here

Implementer session: `2026-09-14-13-05-31-256`.

The linear replay onto upstream `49b61f3904` is complete. Reconciliation restores
native ownership on the shared compute/storage runtime and keeps connection state
at the storage boundary. Finish the no-loss history consolidation and verify the
combined integration and approved corrections in regular PR CI. Preserve designer
commits and useful implementation boundaries. Do not publish merge commits.
Upstream OIDs are preserved and the two unreleased raw sources use fresh OIDs.
Older branch-built catalogs are not a compatibility requirement.

Verify the retained contracts in the existing workflows:

- The two retention fixtures share their scoped heartbeat/grace settings from
  startup and wait for their specific retired incarnations before measuring
  advancement. Production timing and the advancement checks stay unchanged.
- Managed adoption validates the current replicas before DDL records scheduled
  RF0. Equivalent ALTER/RESET paths agree. The scheduler owns deployment-local
  running replicas, not shared intent.
- Promotion includes adapter startup through SQL readiness, within a bounded,
  reported interval. The first query retains its three-second limit without a
  warm-up query or production readiness change.

The obsolete native peek-history test is retired. Preserve meaningful query
cleanup coverage, adding coverage only for a demonstrated gap. Do not preserve
obsolete tests with weaker or tautological assertions.

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
