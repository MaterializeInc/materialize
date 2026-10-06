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
The replay onto upstream `d9a2c5dc40` retains all 138 local commits. Accept
upstream's unconditional frontend OCC and removal of legacy coordinator write
locks, while preserving fresh catalog certification, read protection, the
adapter-owned table writer and the approved Persist-arbitrated OCC correction.
The moved background PeekClient constructor no longer takes the removed flag.
The renamed two-instance txn-WAL workflow remains paired with its nightly entry.
Independent static review found no blocking integration issue. Compilation and
runtime verification remain CI-owned.
The imported drop-only prewarming workflow is removed because it asserts legacy
restarts. Native retirement and SQL DROP membership contracts remain unchanged.
CI137934 at `094725b6` passed Cargo, Clippy, all five SLT shards, native outage
and warm promotion. Both parallel shards still time out. Publication and retention
fixtures fail because upstream's summary-only test policy omits their required
per-shard metrics. Both fixtures now explicitly request those series, retaining
all assertions. Verify that correction and the temporary handoff tracing in CI.
The oracle-only rejection is removed, so timestamp-conflict replies now describe
actual txns conflicts. Both focused committer tests passed in CI137885.
Upstream OIDs are preserved and the two unreleased raw sources use fresh OIDs.
Older branch-built catalogs are not a compatibility requirement.

Prioritize the remaining read-then-write timeouts in parallel checks. The
catalog-only completion regression passes: table keepalives remain staged but
are not awaited on the coordinator. CI137946 handoff traces locate dominant waits
before coordinator receipt: roughly 115s in shard1 and 147s in shard2, while
thousands of native responses are dispatched. Matching subscribe batches wait
48s and 82s after reaching the adapter. Catalog/worker waits are much smaller for
these attempts. Client commands must get a bounded batch each productive service
round, after the selected messages, without removing maintenance priority.
Verify that admission correction in the existing parallel workflows. Keep the
temporary handoff events through this comparison, then remove them.
Long inline catalog/publication work also delays dispatch. Dispatch gaps ending
with publication-race warnings implicate maintenance, but do not isolate every
await inside those gaps. Fair admission does not eliminate slow branch bodies.
Preserve freshness, future-time checks, catalog completion before acknowledgement,
refolding at every new target and existing deadlines. Lower-priority read/timeline
branches retain their ordering. No broader scheduler redesign is included.
The DROP-preparation correction refreshes the owner's committed requirement
after metadata contention without deriving it from an import's available history.
The unchanged selected-plan-explain regression passes in CI137817 and CI137825. Its companion
retention fixture uses blind INSERTs for compaction work, not read-dependent
UPDATEs, and passes in CI137825. That isolates the fixture and does not resolve
the parallel UPDATEs. Cluster1 also passes after removing the transient retained
compaction-command count assertion, without changing runtime measurements.

All five SQL logic test shards pass in CI137778, including scheduled-compaction's
unchanged 1/0/1 membership and audit assertions. Preserve those assertions.
CI137885 SLT1 first fails its one-second EXPLAIN FILTER PUSHDOWN on
`mv_aligned_to_past`, after both `mv12` queries complete. The automatic diagnostic
rewrite later exhausts the job budget while still making progress. Its final SQL
is not logged. Do not treat this as proof of a permanent recovery stall.
Earlier real-time-window observations do not create another investigation campaign.
The imported hydration-stability restart and no-dataflow workflows pass in
CI137762, alongside warm handover.

Comment-ID collision coverage now uses dynamic setup in the SQL integration
harness. Preserve actual collisions and exact comment attribution when adjusting
its setup or cost. EXPLAIN reports selected recovery plans, not necessarily the
running dataflow after imported-index removal.

The missing hydration-history episode recurs in CI137825 Testdrive4 at line 596.
The capture shows a zero-row visit followed 24s later by a one-row append,
beyond the roughly 20s observation budget. A scoped 60s observation wait preserves
the retention assertions and serial collector behavior. Temporary collection logs
are removed. The appended row's identity and initial raw hydration completion were
not logged, so this is evidence of delayed sampling, not a proven write/read mismatch.
Keep history across deployments.

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
