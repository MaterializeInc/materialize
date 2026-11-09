# Decoupled coordination: current handoff

## Current objective

Choose the next scoped usage-side improvement after the
[one-minute publication comparison](20260903_decoupled_coordination_publication_interval.md).
Slower protection publication cuts CAS and blob work, but does not erase frontier
publication/reduction costs or the additional memory footprint. Total scan deltas
are unstable because notification loss changes fallback traffic.

## Next concrete action

Agree the next optimization with Aljoscha. Retaining immutable selected-plan/schema results is an isolated
candidate. Removing legacy frontier progress requires removing inactive SQL input
dependencies first. Do not stop those writers alone or change Persist/defaults
without a separate decision.

Evidence: `~/.local/share/aj/m3-evidence/publication-interval/`.
`comparison.json`, `scan-summary.json` and `reader-scan-residual.md/json` retain
cost, caller and input-identity evidence. All three primary 60s fixtures and the
fresh scan pair passed. Temporary instrumentation is absent from production
sources. Owned runtimes stopped, five metadata containers were removed, and
about 135 GiB is free. Independent review is complete. No runtime or worker
is still running.

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

- Current comparison and raw evidence are linked above. Primary and scan-audit
  instrumentation differ, so their windows are not pooled. Reader correction,
  policy progress and background recovery are distinct contracts.
- Reference evidence: `~/.local/share/aj/m3-evidence/logical-component-load/`.
  Valid heaps precede CPU profiling. The baseline's post-window inventory failure
  was reproduced and repaired without rewriting its original failed status.
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
