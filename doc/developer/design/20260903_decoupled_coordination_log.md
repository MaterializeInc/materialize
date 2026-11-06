# Decoupled coordination: current handoff

## Current objective

Backend-load attribution is complete. Most additional CAS calls are empty
catalog-output progress, not retries. Duplicate frontier-source reads explain a
large part of GET counts, while catalog publication and distributed compaction
dominate extra blob bytes. See the
[report](20260903_decoupled_coordination_backend_load.md) and
[measurements](20260903_decoupled_coordination_backend_load.json).
No production repair was made. Persist-internal changes remain deferred.

## Next concrete action

Agree on a bounded caller-side repair. Start with shared frontier inputs or
validated intermediate arrangements, preserving public semantics, the durable
authority barrier and recovery/read-hold lifetimes. The report also identifies
missing common source filtering in the dependency MV and independent progress
timers on shared shards. MV output coalescing needs an explicit latency decision.
Do not assume the measured load is entirely removable.

## Live limitations

Broad CI remains non-green and was not rerun. The last broad evidence is
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image. Registry visibility, merge skew, the prior Persist
JSON-order check and the stale hydration expectation remain unresolved.
Full repository formatting also hits an existing overlong catalog generic call.
The PR stays draft.

M3 still needs arbitrary-release compatibility and external-sink handover.
The birth-ordering concern is source-inspected, not reproduced. The original
four-adapter workload has not been rerun with the txn-WAL retirement repair.
The [follow-up inventory](20260903_decoupled_coordination_followups.md) retains
these and the other architectural opportunities.

## Verified outcomes

The backend comparison and separate stored-plan witness passed. Counters and
payload reconcile with actual operations. The earlier resource comparison,
consensus-read reductions and two-adapter lifecycle checks remain valid for
their recorded candidates. The two-version prototype demonstrated full writers,
surviving sessions and explicit retirement using compatible prototype plan
encodings. The authority projection decoupled retained introspection from catalog
history. None of this establishes arbitrary-version compatibility or green CI.

## Essential evidence and local state

- Latest: `~/.local/share/aj/m3-evidence/backend-load-attribution/`, including
  `native-audit-1793996274/`, `main-audit-1793998684/` and
  `native-plans-1793999722/`. Includes ledgers, source audits, stored plans,
  derivation scripts, supported build logs and binary hashes.
- CPU/memory: [resource report](20260903_decoupled_coordination_resource_review.md),
  evidence under `~/.local/share/aj/m3-evidence/native-main-usage/`.
- Consensus: [read-removal report](20260903_decoupled_coordination_observed_progress.md)
  and [scan attribution](20260903_decoupled_coordination_replica_scans.md).
- Lifecycle and shared plans: `~/.local/share/aj/m3-evidence/lifecycle/verification.md`
  and `shared-plans/verification.md` in the same evidence root.
- Authority: `~/.local/share/aj/m3-evidence/authority-projection/replacement/`.
  Txn-WAL: `~/.local/share/aj/m3-evidence/txn-retirement/repair.md`.
- Earlier details: [archive](20260903_decoupled_coordination_log_archive.md).

All owned runtimes are stopped. Temporary production instrumentation is removed,
with reproducible patches under `misc/benchmarks/native-idle/`. The isolated
upstream worktree is removed. Its workspace artifacts must not be reused against
older-mtime native sources: Cargo reused stale OID metadata until workspace
packages were rebuilt. Keep the third-party cache, but invalidate workspace
packages when switching these source trees through one target directory.
The unrelated persist-traffic worktree is untouched. Keep heavy work sequential,
use low concurrency and preserve a 30 GiB free-disk reserve.
