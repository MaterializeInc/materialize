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

M3 pursuit is paused in favor of smaller scoped goals. The current goal is to
decouple catalog history from retained builtin metrics, then verify recovery on an
isolated copy of the original deployment. Do not resume the full upgrade
walkthrough yet.

The retention cause is established: metric indexes imported raw catalog authority
lookups, propagating their history windows to the catalog journal. The durable
`mz_catalog_deployment_authority` projection now separates those histories. All
authority consumers use it, while other catalog facts remain unchanged. Structural,
behavior and selected-plan checks pass. A fresh default-debug two-adapter run
advanced raw catalog compaction beyond the authority snapshot retained by the
indexes. Both adapters could still read that old authority snapshot after restart.

Resolve the existing-state migration scope next. Aljoscha accepts rebuilding the
affected derived builtin/index history rather than backfilling it. However,
protected joins require matching builtin definitions, and there is no supported
cold schema-cutover entrypoint. Replacement also needs consistent native bounds
and requirements, not just new shards. Approval was requested to include that
operation here or pursue it as a separate goal. Do not bypass admission, fencing
or reader protection, or reset original catalog, metric sources, user data or WAL.

The original retained deployment has not been migrated. Its since advancement
and replay-cost improvement remain unverified. Fresh-deployment results do not
satisfy that part of the goal. Evidence and exact next steps are in
`~/.local/share/aj/m3-evidence/authority-projection/progress.md`.

## Live blockers and pending decisions

- **Version overlap:** the rebuilt pair passed pre-promotion SQL/DDL/writes after
  the txn-WAL format repair. Promotion and explicit post-promotion retirement
  remain unverified. No promotion or retirement has executed on the retained
  deployment. Generation 0 remains active.
- **Readiness:** long reads have been moved off the coordinator. Keep the selected
  snapshot cut and reader alive through consumption or cancellation. Unavailable
  status history means not ready. Do not replace this with latest-state sampling
  or dispatch QueryClient acquisition back to the coordinator from its own handler.
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

The prior upgrade attempt is recorded in `local-readiness-live-promotion{,-repeat}.log`,
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
processes, preserve the original retained state and needed evidence, and remove
obsolete fixture data and binaries. Scratch is optional, not a blocker. Confirm
remote worker state before syncing anything.

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

The current projection evidence is under `authority-projection/`:
- `progress.md`: next action, migration blocker and exact verification pointers.
- `fresh-1793720779/native-plan-imports.json`: actual retained-index imports.
- `fresh-1793720779/samples.json`: catalog and authority progress.
- `fresh-1793720779/restart-1793721954/result.json`: historical authority reads
  after restart with raw catalog since beyond that timestamp.
- `slt-verify.log`: passing catalog SQL and selected-plan checks.

The retention diagnosis is in `catalog-history/findings.md`. The original state
remains untouched. A separate scratch creation attempt,
`c9ee5c87-decoupled-coordination`, timed out waiting for SSH and returned no instance
ID. It had a six-hour expiry. Inventory it after reauthentication before launching
another worker.

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
