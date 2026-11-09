# Decoupled coordination: current handoff

## Current objective

Make this branch native-only. Remove legacy monolithic ownership, execution,
upgrade and introspection alternatives while preserving adapter policy, shared
query/table machinery, durable protection and native multi-version retirement.
Performance changes remain separate from this cleanup.

## Next concrete action

Compile and exercise the native Rust TestHarness conversion. The fixture now
starts the real controller initialization/reconciliation loop and hosts a
join-only adapter behind a test-only orchestrator backend. A retained deployment
handle separates adapter restart from replica and relay lifetime. This conversion
is uncommitted and not yet compile- or runtime-verified.

The legacy frontier slice is committed and independently reviewed. Targeted Rust
and catalog SQL checks passed. A fresh debug deployment preserved SQL progress
through peer join/restart/removal and retained frontier observations through a
catalog-replica restart. Managed processes were stopped afterwards. The affected
source-enumeration block in `catalog.td` passed, not the full Kafka-dependent file.

`bin/environmentd` now reports the unsupported launcher before touching state.
Launcher tests and formatting checks pass. Broader native-only serving,
controller execution, introspection and harness conversion remains unfinished.

Essential worker/evidence pointers:
`~/.local/share/aj/m3-evidence/native-only/work-notes.md`, `frontier-review.md`,
`harness-decision.md`, `controller-alternatives.md`, and `native-frontiers.log`.
Only isolated PostgreSQL remains running for fixture verification. A two-job
compilation check is running. Pre/post-cleanup builtin schemas are incompatible
and admission rejects that pairing. Use matching post-cleanup schemas for the
version demonstration.

The prior [publication comparison](20260903_decoupled_coordination_publication_interval.md)
remains the load baseline, not a performance claim for this cleanup.

## Live limitations

Broad CI remains non-green and was not rerun. The last broad evidence is
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image. Registry visibility, merge skew, the prior Persist
JSON-order check and the stale hydration expectation remain unresolved.
The last full formatting check failed on an unchanged generic call in
`src/catalog/src/catalog.rs`. The prior changed-file Python checks passed. Current Rust cleanup verification
is pending. The PR stays draft.

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
