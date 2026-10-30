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

The worktree admits compatible full SQL writers, retains them across promotion,
and fences them only on explicit retirement. Controller startup does not promote
or repeat shared initialization. Promotion activates its local configuration
through the existing controller loop and catalog/WAL protocol. A reproduced stale
controller overwrite is fixed by checking active authority at the synchronized
prefix covering the configuration write timestamp.

Strict all-target Clippy and the default `bin/controllerd --build-only` passed.
The linker initially exhausted disk. Removing disposable incremental cache and
repeating the same build succeeded (`local-controller-build-repeat.log`).
Generation 0 is running from fixed v26.46.0-dev.0 artifacts through launcher task385.
It serves DDL, indexed reads and table writes. Controller-only restart preserved
adapter/replica PIDs and data. Pinning the relay binary replaced that relay while
preserving its endpoint (`local-artifact-pinning-pass.json`).

Next: build a v26.47.0-dev.0 development artifact set from this prototype, changing
only environmentd/clusterd package versions for validation. Preserve the artifacts
and restore those temporary manifest/lock changes afterward. Use
`--controller-binaries-directory` for both deployments, one relay provisioner and
`--pubsub-url` for the candidate. Execute writes/DDL before and after promotion,
then explicit retirement while checking survivor sessions and replica PIDs. This
is a two-development-version test, not compatibility with an older released binary.
The required distinct-version walkthrough remains unexecuted.

Catalog library, read-protection/oracle integration, and initialization/race tests
passed. Runtime all-target compilation passed. Evidence logs:
`local-catalog-lib-corrected.log`, `local-compute-protection-corrected.log`,
`local-read-write-corrected.log`, `local-activation-boundary.log`, and
`local-activation-check.log`. The full library pass precedes the configuration
repair, which has targeted coverage. Runtime review found no extra authority leak.
Its objection to restarting the promoted adapter is rejected: the prompt explicitly
allows that brief restart. Surviving sessions on other adapters remain required.

Controller HTTP exposes `POST /api/deployments/{generation}/retire` with required
JSON `environment_id`. It validates that identity before mutation and returns a
receipt after durable retirement. Launcher commit `9465b60cd0` plus worktree safety
changes implement `--retire-generation`, validate the receipt, then stop only that
generation. Python tests, Black and Ruff pass (`local-prebuilt-launcher.log`). Real HTTP
rejects wrong-environment and active-generation retirement
(`local-retirement-rejections.json`). Successful retirement remains unverified. Never use environment-wide `--stop` for this.
Keep one managed relay provisioner and hand it off explicitly before retiring its
controller. Agent74/task359 has the process-lifetime evidence.

Private local PostgreSQL18 is running in `aj-m3-full-writer-postgres` on loopback
26257. Its credential URL file is `full-writer/local-postgres-url.txt` under the
local evidence directory. The fresh `materialize_controllerd` runtime database has
its own `full-writer/local-runtime-postgres-url.txt` file for `MZDEV_POSTGRES`.
The default controller state directory now holds the live generation-0 environment.
PostgreSQL uses max_connections=1000 and a 2GiB memory limit. Its default 100 slots
were saturated by mostly idle pooled connections. Do not print credentials.
This is separate from the scratch DB.

Scratch SSO expired, but Aljoscha explicitly authorized local VM verification
instead. Scratch is an optimization, not a blocker. This VM has 18GiB RAM and limited
free disk, so keep heavy jobs serial and monitor resources. Prior local `/tmp` logs
are no longer present. Confirm remote job state before any future worker sync.

Temporary-item cleanup remains unimplemented. Aljoscha proposes reusing
`mz_sessions_raw` session-to-execution ownership. Agent77/task378 confirmed the
serving path rejects retired TEMP DDL through CAS conflict and refresh, but
`Catalog::transact` does not itself validate a supplied execution incarnation.
First TEMP DDL needs failure-preserving durable session-publication completion.
Retirement must clean durable Items before removing their session mapping. Use
`remove_ephemeral_items_for_owners`, not local-only `drop_temp_items`. The retired
row sweep also needs incremental observations rather than repeated full snapshots.
Normal disconnect already drops Items before retracting its session. Do not guess
ownership from missing rows or introduce new session catalog records.

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

Detailed previous evidence remains in the existing archive and [follow-up inventory](20260903_decoupled_coordination_followups.md),
not as additional M3 scope.
