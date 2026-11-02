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

## Next concrete action

The readiness and batch-format repairs are committed and pushed through
`606f1a2d`. Their targeted tests, storage library suite and strict all-target Clippy
pass. The newer default-development candidate built successfully and is pinned at
`full-writer/artifacts/v26.47.0-dev.0-readiness-fix`. Version overrides are restored.

The live attempt served shared DDL and writes on both versions but timed out waiting
for ReadyToPromote, before any promotion request. Candidate diagnostics reported
lagging frontier-related collections. An unchanged driver repeat later timed out
connecting to the newer adapter. Agent83 / task439 identified persistent deficits
in s836 (mz_frontiers_ind), s854 (mz_cluster_replica_frontiers_ind), s788
(mz_hydration_statuses_ind) and s805 (mz_show_cluster_replicas_ind). Initial
s451/s488/s730 hydration failures recovered. Reader-heartbeat gaps and eventual
expiry are also confirmed, but a common cause is not established.

Next capture synchronized replica-local output/import/hydration frontiers for
those four IDs, adapter observations and the active reference, with separate SQL
connection/SELECT 1 timing. Distinguish execution/input lag from observation lag
before editing code. Keep fresh lease health visible. The private
`run-bounded-promotion.py` now provides bounded waits and automatic cleanup for
future walkthroughs. It passes Black/Ruff/syntax checks but has not been executed.

No promotion or retirement has executed. Generation 0 remains active. After
promotion succeeds, verify explicit relay handoff and retirement. Both versions
must retain SQL, DDL and writes across promotion, with warmed replicas unchanged.

## Live blockers and pending decisions

- **Version overlap:** transferred txn-WAL batches used the newer binary's version
  instead of the admitted format. `37ed82b7ef` fixes envelope labels without changing
  writer-key identity or decode guards. Real cross-version transfer/snapshot tests
  and strict Persist Clippy pass. The rebuilt pair passed pre-promotion SQL/DDL/writes, but promotion remains unverified. Evidence:
  `local-batch-version-final-{test,clippy}.log`.
- **Readiness:** the repair chooses a snapshot cut after acquiring a
  Persist reader and retains that reader through consumption or cancellation.
  Unavailable status history means not ready. Do not replace selected-cut semantics
  with latest-state sampling or dispatch QueryClient acquisition back to the
  coordinator from its own handler. Evidence: `local-snapshot-lifetime-final.log`
  and `local-protected-snapshot-final-{full-lib,clippy}.log`. The lifetime regression
  reproduced early expiry before repair. Its maintenance step reapplies committed
  permission because reader expiry alone does not recompute Persist since.
- **Adjacent lifetime bug:** the standalone CollectionReader helper has the same
  early-drop pattern. Scope approval was requested before changing that path.
- **Persist suite:** `state_inspect_serde_json` fails unchanged on object-key order.
  Parsed JSON matches the golden. Do not rewrite the golden or weaken assertions.
  Evidence: `local-batch-version-full-lib{,-repeat}.log`.
- **Bootstrap contention:** planning-revision changes can abort startup. Agent78 /
  task392 identified a preparation/admission retry boundary with retained
  intermediate holds and transaction-wise catch-up. Do not retry all bootstrap or
  ignore invalidation. No retry repair is implemented.
- **Temporary-item cleanup:** reuse `mz_sessions_raw` session-to-execution ownership.
  First TEMP DDL needs failure-preserving durable session-publication completion.
  Retirement must remove durable Items before their session mapping, using
  `remove_ephemeral_items_for_owners`, not local-only `drop_temp_items`. Observe
  retired rows incrementally rather than repeatedly snapshotting them. Normal
  disconnect already drops Items first. Agent77 / task378 also found that serving
  CAS/refresh rejects retired TEMP DDL, but `Catalog::transact` does not itself
  validate the supplied execution incarnation. Do not infer ownership from absent
  rows or add new session catalog records.

## Preserved runtime and evidence

The runtime is stopped. Catalog, blobs and artifact hardlinks remain intact.
Generation 0 uses HTTP6882 and generation 1 HTTP6883. Shared declarations contain
`a1` (default) and `a2` (candidate), giving two adapters per deployment. Keep one
relay provisioner: generation 1 uses `--pubsub-url http://127.0.0.1:6879`.

Use old `full-writer/artifacts/v26.46.0-dev.0-temp-fix` against new
`full-writer/artifacts/v26.47.0-dev.0-readiness-fix`. Do not use the earlier newer
`-temp-fix` set, which lacks the repairs.
Validation-only version/lock overrides were restored. These are distinct prototype
development versions, not evidence of released-version compatibility.

The latest evidence is `local-readiness-live-promotion{,-repeat}.log`,
`live-promotion-before.json`, `local-readiness-{old,new}-runtime.log`, and
`local-readiness-failure-extract.txt`. Automatic cleanup completed successfully
(`local-readiness-post-test-stop.log`). All controllerd, adapterd, clusterd and
pubsubd processes are stopped. Future runtime drivers must include bounded waits
and cleanup on failure, rather than relying on this session resuming promptly.

Use `--retire-generation G --controller-url URL`, not environment-wide `--stop`,
for retirement. The launcher validates the HTTP receipt and exact process identity
before removing the target generation. Hand off relay supervision before retiring
its controller. Agent74 / task359 records process-lifetime constraints. Preserve
sink-specific authorization and Kafka recovery. Global AllowWrites is not sink
revocation, and promotion does not introduce an atomic Kafka handover protocol.

Private PostgreSQL18 runs as `aj-m3-full-writer-postgres` on loopback26257 with
max_connections=1000 and a 2GiB memory limit. Under the evidence root, test
credentials are in `full-writer/local-postgres-url.txt`, and runtime credentials
for `MZDEV_POSTGRES` are in `full-writer/local-runtime-postgres-url.txt`.
The runtime database is `materialize_controllerd`, with state in the default
controller directory. Never print credentials or unfiltered process arguments.

Keep heavy jobs serial and low priority on this 18GiB VM. Stop idle runtime
processes and preserve database volumes, evidence and candidate binaries. Scratch
is optional, not a blocker. Confirm remote worker state before syncing anything.

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

The prior scratch worker `i-0f5a60529f57d6ac7` had an expiry of
2026-10-30T18:13:29Z. Do not assume it still exists. Use the local VM and archived
evidence. Keep heavy verification serial. Private harnesses/evidence live under
`~/.local/share/aj/m3-evidence/`. Never print credentials or unfiltered process
arguments.

Validated off-worker evidence is in the local `startup-spin/` subdirectory:
- `logical-frontier-runtime-pass.json.gz`: two-adapter restart and placement.
- `logical-frontier-full-test-pass.json.gz`: full debug frontier test, binary
  hashes and source/test revisions.
- `committed-initialization-frontiers-pass.json.gz`: restart/recovery witnesses.
- `frontier-retirement-pass.json.gz`: authorized cleanup and restart preservation.

Detailed previous evidence remains in the existing archive and [follow-up inventory](20260903_decoupled_coordination_followups.md),
not as additional M3 scope.
