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
are not awaited on the coordinator. CI137778 traces show advancing subscribe
progress and repeated global-oracle timestamp rejection while foreground writes
hold admission permits. Baseline has the same oracle precheck, while native catalog
publishers add independent allocations. The approved correction makes Persist
arbitrate certified timestamps, caps proposals by the oracle and returns the actual
txns upper on conflict. Preserve freshness, future-time checks, catalog completion
before acknowledgement and refolding at every new target. Verify in existing CI.
This does not fix lag-induced conflicts. Early traces establish coordinator queue
pressure and delayed subscribe observation without identifying one wait owner. The frontier
UPDATE also repeatedly replans before execution. Keep coordinator/DDL stalls
distinct unless evidence connects them. Remove scoped RTW diagnostics when their
question is answered. Keep protection and deadlines unchanged.
The DROP-preparation correction refreshes the owner's committed requirement
after metadata contention without deriving it from an import's available history.
The unchanged selected-plan-explain regression passes in CI137817 and CI137825. Its companion
retention fixture uses blind INSERTs for compaction work, not read-dependent
UPDATEs, and passes in CI137825. That isolates the fixture and does not resolve
the parallel UPDATEs. Cluster1 also passes after removing the transient retained
compaction-command count assertion, without changing runtime measurements.

All five SQL logic test shards pass in CI137778, including scheduled-compaction's
unchanged 1/0/1 membership and audit assertions. Preserve those assertions.
Earlier real-time-window observations do not create another investigation campaign.
The imported hydration-stability restart and no-dataflow workflows pass in
CI137762, alongside warm handover.

Comment-ID collision coverage now uses dynamic setup in the SQL integration
harness. Preserve actual collisions and exact comment attribution when adjusting
its setup or cost. EXPLAIN reports selected recovery plans, not necessarily the
running dataflow after imported-index removal.

The missing hydration-history episode recurs in CI137825 Testdrive4 at line 596.
Existing scoped collector logging identifies visits, captured cutoffs
and completed row counts. Inspect this failing capture before changing retention,
scheduling or episode grouping. Keep history across deployments. Remove the
diagnostic once its question is answered.

Zippy passes in CI137778. Retain the CI137762 observation: seven promotions
complete while readiness grows from 42s to 363s, then backup/restore exhausts the
job budget with only the catalog shard unfinished after 353s. This is not evidence
of an unfinished promotion or a new investigation campaign.
Keep the observed retention costs and unproven plateau visible. Preserve native
ownership, read protection, external-sink safeguards and compatible-version
handover on fresh environments. The outage, targeted DDL and bounded-throughput
proofs stay closed.
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
