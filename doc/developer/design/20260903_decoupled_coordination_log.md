# Decoupled coordination: current handoff

## Current objective

The fresh idle comparison and logical-component audit are complete. Native uses
1.24 versus 0.63 CPU cores and 2865 versus 1354 MiB PSS. Catalog-output
completeness drives most extra CAS,
while protection journal consumption/compaction drives most extra blob bytes.
See the [report](20260903_decoupled_coordination_logical_load.md) for exact units,
repeat measurements, source attribution and limits.

## Next concrete action

Agree a bounded repair. The narrowest demonstrated redundant work is repeated
immutable-plan decoding for producer metadata. The larger write-cost target is
catalog-output completeness fanout, which needs an explicit latency contract.
Filtering irrelevant protection rows before the shared SQL arrangement and
reducing non-execution catalog/cache ownership are separate scopes. Do not change
Persist internals or publication cadence. No runtime needs to remain running.

## Live limitations

Broad CI remains non-green and was not rerun. The last broad evidence is
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image. Registry visibility, merge skew, the prior Persist
JSON-order check and the stale hydration expectation remain unresolved.
The last full formatting check failed on an unchanged generic call in
`src/catalog/src/catalog.rs`. Changed-file Python checks pass. This audit changed
no Rust production sources. The PR stays draft.

M3 still needs arbitrary-release compatibility and external-sink handover.
The birth-ordering concern is source-inspected, not reproduced. The original
four-adapter workload has not been rerun with the txn-WAL retirement repair.
The [follow-up inventory](20260903_decoupled_coordination_followups.md) preserves
these and the other architectural opportunities.

## Verified outcomes

Shared-input plans, two-adapter churn and catalog-replica recovery with retained
frontier reads passed. Catalog goldens and the isolated index inventory assertion
passed. The two-version prototype previously demonstrated compatible full writers,
surviving sessions and explicit retirement. The durable authority projection
removed retained introspection's catalog-history dependency. These outcomes do
not establish arbitrary-version compatibility or green CI.

## Essential evidence and local state

- Current evidence: `~/.local/share/aj/m3-evidence/logical-component-load/`.
  CPU/counter windows passed. Valid heaps precede CPU profiling. The baseline's
  post-window inventory failure was reproduced and repaired without rewriting
  its original failed status. Exact current scan-origin rates remain unmeasured.
  Instrumentation is absent from production sources. Owned runtimes are stopped.
- Shared-input lifecycle and plan evidence: `shared-frontier-inputs/` under the
  same evidence parent. Its initial symlink-discovery run remains excluded.
- Previous [backend attribution](20260903_decoupled_coordination_backend_load.md),
  [resource review](20260903_decoupled_coordination_resource_review.md),
  [read reductions](20260903_decoupled_coordination_observed_progress.md) and
  [scan attribution](20260903_decoupled_coordination_replica_scans.md).
- Lifecycle/plan evidence: `~/.local/share/aj/m3-evidence/lifecycle/verification.md`
  and `shared-plans/verification.md`. Authority evidence is in
  `authority-projection/replacement/`, txn-WAL evidence in `txn-retirement/repair.md`.
- Older details: [archive](20260903_decoupled_coordination_log_archive.md).

The unrelated persist-traffic worktree is untouched. Keep heavy work sequential,
use low concurrency and preserve a 30 GiB free-disk reserve. When switching older
source trees through the same target directory, invalidate workspace packages
rather than risk stale Cargo metadata. Preserve the third-party cache.
