# Decoupled coordination: current handoff

## Current objective

The shared-frontier-input repair is verified. Both public frontier indexes now
reuse validated arrangements. Matched measurements show blob GETs falling from
14.09/s to 10.05/s, with no remaining duplicate observation-key reads in the
catalog replica. CPU, memory and write traffic are largely unchanged.
See the [report](20260903_decoupled_coordination_shared_frontiers.md).

## Next concrete action

Agree the next scope before changing more code. The remaining cost is dominated
by catalog journal traffic, distributed compaction and empty progress writes to
catalog outputs. The report proposes caller-side investigation without slowing
publication, adding a designated owner or changing Persist. No runtime needs to
remain running for this decision.

## Live limitations

Broad CI remains non-green and was not rerun. The last broad evidence is
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image. Registry visibility, merge skew, the prior Persist
JSON-order check and the stale hydration expectation remain unresolved.
Full formatting still fails on an unchanged generic call in `src/catalog/src/catalog.rs`.
Changed Rust files and Python checks pass. The PR stays draft.

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

- Current evidence: `~/.local/share/aj/m3-evidence/shared-frontier-inputs/`.
  The report links accepted runs, provenance, independent review and checks.
  The initial symlink-discovery run is excluded. Temporary instrumentation is
  removed from production sources. Owned runtimes and metadata container are stopped.
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
