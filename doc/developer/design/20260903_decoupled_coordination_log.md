# Decoupled coordination: current handoff

## Current objective

Make this branch native-only. Remove legacy monolithic ownership, execution,
upgrade and introspection alternatives while preserving adapter policy, shared
query/table machinery, durable protection and native multi-version retirement.
Performance changes remain separate from this cleanup.

## Next concrete action

Continue removing adapter-side process actuation and combined bootstrap
alternatives. Preserve controllerd's catalog-authorized reconciliation and
explicit retirement, adapter policy, endpoint discovery and observations.

A user decision is pending on retiring the unused legacy final-preflight hook
with an explicit unsupported result. It has no enabled CI caller, but can run
an old-to-old check when manually configured. Keep that hook unchanged pending
the answer. Do not add image-classification machinery or conversion/reset
workarounds. Research: `final-preflight-boundary-research.md` in the evidence
directory below.

Convert native multi-generation harnesses to shared relay topology and
explicit retirement. Verify compatible writers before and after promotion,
warmed replicas and surviving deployments. Current-source script selection is
preserved, but its multi-container runtime remains unverified. Remaining
controller-owned compute/storage execution also needs removal.

## Current cleanup outcome

Exclusive replica cleanup based on deployment age or local allocator/inventory
state is removed. Catalog-authorized cleanup, explicit retirement and service-name
compatibility remain. All affected targets compile. Protected cleanup and native
owner reconciliation tests passed, with independent review. The old retry interval
remains accepted but ignored for persisted configuration compatibility.

Native frontier publishers and SQL selectors, controller-backed Rust/SLT fixtures,
and the controller-managed container entrypoint are committed and reviewed.
Targeted SQL/schema, authentication, retained-status and lifecycle checks passed.
Fresh debug-container checks covered SQL/HTTP, controller and adapter recovery,
retained process identities, surviving sessions and persisted table/MV contents.
This was a cached image with current binaries and entrypoint mounted, not a rebuilt
image or broad CI. Port6881 is SASL SQL, not HTTP. Relay diagnostics use6883.

Historical legacy-upgrade cases, incident-1007 repair paths and mixed
pre-native/native platform plans now produce explicit skipped results before
workflow startup/reset. Obsolete old-checkout preflight selection is removed,
and its CI triggers retain explicit unsupported skips. Old-only and
all-native plans remain selected. Prepared randomized plans execute once.
Skips continue, failures remain fail-fast, and SQL errors retain scenario/source
identity. Reporting tests, a real blocked parallel-worker test and a real CLI
invocation in CI mode passed. The local CLI still eagerly acquires initial images
before workflow dispatch. Its probe timed out during pulls, without starting
services. Newly pulled unused image tags were removed.

Evidence under `~/.local/share/aj/m3-evidence/native-only/`:
- `exclusive-cleanup-review.md`, `exclusive-cleanup-check.log`,
  `exclusive-cleanup-combined-tests.log`.
- `container-runtime/verification.md`, `native-bind-review.md`,
  `native-bind-reviewed-green.log`, `native-bind-reviewed-check.log`.
- `unsupported-reporting-review.md`, `unsupported-reporting-combined.log`.
- `fixed-upgrade-combined.log`, `fixed-upgrade-cli-corrected.log` and
  `junit_mzcompose_local-incident-skip.xml` cover the removed fixed historical paths.
  Full CI pipeline generation was not run.
- `platform-boundary-followup.md`, `platform-boundary-combined.log`,
  `platform-cli/` for actual CLI output, JUnit and cleanup evidence.
- `upgrade-boundary-classification.md` for remaining workflow boundaries.
  Its original claim that legacy-upgrade already had named cases was incorrect.
- `controller-alternatives.md`, `frontier-review.md`, `harness-decision.md`,
  `native-frontiers.log`, and `work-notes.md` for implementation/evidence pointers.

All owned verification services are stopped. Superseded native benchmark binaries
were removed, with checksums retained in `archived-binary-cleanup.log`. Baseline
artifacts, measurement reports and current binaries remain. Recreate isolated
prerequisites before runtime checks. Pre/post-cleanup builtin schemas are incompatible, so use
matching post-cleanup schemas for native version demonstrations.
The prior [publication comparison](20260903_decoupled_coordination_publication_interval.md)
remains the load baseline, not a performance claim for this cleanup.

## Live limitations

Broad CI remains non-green and was not rerun. The last broad evidence is
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image. Registry visibility, merge skew, the prior Persist
JSON-order check and the stale hydration expectation remain unresolved.
The last full formatting check failed on an unchanged generic call in
`src/catalog/src/catalog.rs`. The prior changed-file Python checks passed. The native Rust/SLT fixture, observation SQL and targeted native-container checks
pass. Broad container/upgrade suites and remaining execution cleanup are still
outstanding. The PR stays draft.

Arbitrary-version compatibility and external-sink handover remain unverified.
Pre-native-to-native conversion is out of scope.
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
