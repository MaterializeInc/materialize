# Decoupled coordination: current handoff

## Objective

Complete M3 under the [prompt](20260903_decoupled_coordination_prompt.md) and
[design](20260903_decoupled_coordination.md). Two different compatible versions
must serve SQL, DDL and table writes before and after promotion. Promotion changes
active execution authority without retiring the old deployment. Explicit HTTP
retirement fences only its target, then releases its protection and processes.
Preserve warmed replicas, surviving deployments, shared pubsub and one authorized
execution per external sink. Read-only prewarming and build-metadata variants do
not satisfy the required [local walkthrough](../controllerd.md).

M2 remains closed within its approved scope. Preserve native execution and the
60s publication default. Kubernetes/operator runtime remains deferred.

## Current implementation and next action

The worktree separates already-admitted SQL writers from private replica
prewarming transactions. This is unfinished and unverified. A real durable-API
witness exercises shared DDL from two semantic versions, promotion, old-version
restart and explicit retirement. Before the change it fails on the pending
writer's shared-DDL restriction.

Admission and promotion are now separate from the private transaction namespace.
The focused check is `/tmp/m3-full-writer-join.log`. Inspect its result before
syncing the worker. Next preserve old membership, protection and compatibility
bounds through promotion until explicit retirement, then connect the working path
through controllerd and adapterd. Return to the local two-version demonstration
as soon as that path is usable.

Reuse existing sink-specific reconciliation: inactive generations have no desired
external sinks, and the follower stops their attempts without stopping queries or
other execution. Global AllowWrites is not the sink revocation boundary. Preserve
Kafka fencing/recovery and existing late-approved-open limitations. Recheck active
authority after asynchronous sink input acquisition. Do not add a new handover
protocol merely to make catalog promotion an atomic Kafka barrier.

## Verified outcomes and remaining gaps

The default development launcher manages independent adapters, replicas and shared
pubsub. SQL/HTTP adapter lifecycle, data-preserving stop/restart, initialization
recovery and collection inspection are implemented. Default-debug two-adapter
restart/placement witnesses and the full frontier integration test pass. Frontiers
use replica observations and committed logical storage permission, with incremental
conflict handling and catalog-authorized cleanup. The original startup-spin
symptom itself remains unreproduced.

Full-writer version overlap, explicit post-promotion retirement and the executed
upgrade walkthrough are not verified. Remaining M3 acceptance also includes
matched query-performance measurements, diagnostic/notice requirements and broad
regressions. The PR remains draft.

Broad CI is not green. [Build141419](https://buildkite.com/materialize/test/builds/141419)
completed Rust compilation but failed because `pubsubd` is not public on DockerHub.
Registry visibility needs its owner, not a gate bypass. Upstream merge-skew failures
remain outside the authorized integration scope. Newer changes need broad CI.

## Worker and evidence

Worker `i-0f5a60529f57d6ac7`:16vCPU/61GiB/500GiB, expires
**2026-10-30T18:13:29Z**. Use toolchain1.98.1, CARGO_BUILD_JOBS=2 and
RUST_MIN_STACK=8388608 for tests. Extend expiry proactively within the approved
budget. Sync explicit revisions only to an idle, clean worker.

Private PostgreSQL18 listens on loopback26257. Its connection file is
`/home/ubuntu/m3-evidence/native-postgres-url.txt`. Never print credentials or
unfiltered process arguments. Private harnesses/evidence live in
`~/.local/share/aj/m3-evidence/` locally and `/home/ubuntu/m3-evidence/` remotely.
Run harnesses with `bin/pyactivate`. Keep heavy verification serial.

Validated off-worker evidence is in the local `startup-spin/` subdirectory:
- `logical-frontier-runtime-pass.json.gz`: two-adapter restart and placement.
- `logical-frontier-full-test-pass.json.gz`: full debug frontier test, binary
  hashes and source/test revisions.
- `committed-initialization-frontiers-pass.json.gz`: restart/recovery witnesses.
- `frontier-retirement-pass.json.gz`: authorized cleanup and restart preservation.

Current lifecycle baseline logs: `/tmp/m3-full-writer-before.log` and
`/tmp/m3-full-writer-before-repeat.log`. Detailed previous evidence remains in the
existing archive and [follow-up inventory](20260903_decoupled_coordination_followups.md),
not as additional M3 scope.
