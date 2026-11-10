# Decoupled coordination: current handoff

## Current objective

Make this branch native-only. Remove legacy monolithic ownership, execution,
upgrade and introspection alternatives while preserving adapter policy, shared
query/table machinery, durable protection and native multi-version retirement.
Performance changes remain separate from this cleanup.

## Next concrete action

Convert native multi-generation launch/test harnesses to a shared relay and
explicit retirement. Start with the promotion/retirement helper split and one
working native lifecycle. Verify compatible writers before and after promotion,
warmed replicas and surviving deployments. Preserve current-source script
coverage. The caller inventory and exact API/topology contracts are in
`native-lifecycle-harness-seams.md` under the evidence directory below.

Replica-side execution cleanup is a separate unit. Its inspected boundary is in
`native-replica-execution-boundary.md`. A user decision is pending on preserving
the headless driver's programmable low-level coverage through a test-only native
runtime, rather than retaining catalog-free production execution. Keep query
transport, native nonleader workers and both Timely topologies. No implementation
or runtime verification of that unit is claimed.

A decision is also pending on retiring the unused legacy final-preflight hook
with an explicit unsupported result. It has no enabled CI caller, but supports
manually configured old-to-old checks. Leave it unchanged pending the answer.
Do not add image-classification machinery or conversion/reset workarounds.
See `final-preflight-boundary-research.md`.

## Cleanup outcome

Adapter-side provisioning and controller-owned storage/compute execution are
removed. Controllerd owns processes. Replica followers own maintained execution.
Adapters retain cluster policy, validated catalog transactions, serving-timeline
protection and query-owned transient work. Native query clients are required,
without legacy execution or local-protection fallbacks. Shared timestamp/as-of
solvers and all native introspection publishers and SQL consumers remain.
Combined/read-only serving, private migration execution, alternate protection
catalogs and exclusive age-based cleanup are removed. Required formats remain.

Targeted native debug checks pass for startup, adapter restart preserving
execution, readiness, configuration, durable protection, SQL SUBSCRIBE/COPY/EXPLAIN
and complete Kafka ALTER SINK behavior. The latest service binaries contain the
compute-owner removal. Independent review found no new correctness regression.
Its readiness coverage gap is addressed at the native query-client boundary.
A pre-existing startup activation race remains separately recorded below.

Earlier native container checks covered controller/adapter recovery, surviving
sessions and persisted table/MV contents. Historical mixed pre-native/native
upgrade plans report explicit unsupported skips. Old-only and all-native plans
remain selected. These outcomes are targeted evidence, not broad CI or a fresh
multiversion proof for this cleanup.

## Live limitations

Broad CI remains non-green and was not rerun. The last broad evidence is
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image. Registry visibility, merge skew, the prior Persist
JSON-order check and stale hydration expectation remain unresolved. The last
full formatting check failed on an unchanged generic call in
`src/catalog/src/catalog.rs`. Changed-file Rust formatting and combined all-target
checks pass. The PR stays draft.

Native multi-generation harness conversion and broad container/upgrade suites
remain outstanding. Pre/post-cleanup builtin schemas are incompatible. Use
matching post-cleanup schemas for native version demonstrations.
Pre-native-to-native conversion is out of scope. Arbitrary-version compatibility
and external-sink handover remain unverified.

Catalog catch-up can start introspection for a peer-created replica before
QueryClient activation. This startup race predates the compute-owner removal.
The call chain is source-reviewed, not reproduced or fixed. The birth-ordering
concern is also source-inspected, not reproduced. The original four-adapter
workload has not been rerun with the txn-WAL retirement repair. These and the
other architectural opportunities remain in the
[follow-up inventory](20260903_decoupled_coordination_followups.md).

## Essential evidence and local state

Current cleanup evidence: `~/.local/share/aj/m3-evidence/native-only/`.

- `native-compute-owner-verification.md` links source review, exact commands,
  passing native checks, binary hashes and the standalone Kafka run. The four
  compute introspection inputs remain deliberately. Publisher/consumer evidence:
  `native-compute-introspection-boundary.md`.
- `native-storage-owner-verification.md`, `query-copy-verification.md`,
  `private-migration-verification.md`, `adapter-serving-mode-verification.md`,
  `alternate-protection-verification.md`, `native-bootstrap-verification.md`
  and `adapter-actuator-verification.md` preserve earlier removal evidence.
- `container-runtime/verification.md`, `unsupported-reporting-review.md`,
  `pg-upgrade-boundary.md`, `platform-boundary-followup.md` and
  `exclusive-cleanup-review.md` preserve harness, reporting and ownership evidence.

All owned verification services are stopped. Test metadata, Kafka fixture
containers, network and volume were removed. About 45 GiB is free. Current
services, baseline artifacts and measurement reports remain. Superseded build
outputs and inactive incremental caches were reclaimed. Cleanup records are in
`native-compute-*-cleanup.log` and `native-compute-reserve-reclaim.log`.
Keep heavy work sequential, use one low-priority Cargo job and serial tests,
and preserve a 30 GiB disk reserve. The unrelated persist-traffic worktree is
untouched. When switching older source trees through the same target directory,
invalidate workspace packages rather than risk stale Cargo metadata. Preserve
the third-party cache.

The prior [publication comparison](20260903_decoupled_coordination_publication_interval.md)
remains the load baseline, not a performance claim for this cleanup. Primary and
scan-audit instrumentation differ, so their windows are not pooled. Reference
measurements are in `~/.local/share/aj/m3-evidence/logical-component-load/`.
Valid heaps precede CPU profiling. The baseline inventory failure was reproduced
and repaired without rewriting its original failed status.

Earlier lifecycle/plan evidence is in `lifecycle/verification.md`,
`shared-plans/verification.md` and `shared-frontier-inputs/` under the same evidence
parent. It demonstrated compatible full writers, surviving sessions and explicit
retirement before this cleanup. Authority evidence is in
`authority-projection/replacement/`, txn-WAL evidence in `txn-retirement/repair.md`.
Further context: [backend attribution](20260903_decoupled_coordination_backend_load.md),
[resource review](20260903_decoupled_coordination_resource_review.md),
[read reductions](20260903_decoupled_coordination_observed_progress.md),
[scan attribution](20260903_decoupled_coordination_replica_scans.md) and the
[archive](20260903_decoupled_coordination_log_archive.md).
