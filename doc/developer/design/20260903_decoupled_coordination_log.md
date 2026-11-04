# Decoupled coordination: current handoff

## Current outcome

The durable deployment-authority projection is implemented in `89086bb252`.
All authority consumers use it. Retained metric indexes no longer import raw
catalog history for authority, while other catalog facts remain unchanged.
Structural, behavior, selected-plan and catalog SQL checks pass.

Aljoscha rejected migration and requested a fresh representative replacement.
That retention/reconstruction witness passed independent review: two prototype
versions, four adapters, eight replicas, one relay and 30 minutes of concurrent
writes with interleaved view DDL. Metrics retention remained 30 days, protection
publication one minute and reader leases 15 minutes.

Raw physical since advanced from `1793731492539` to `1793733145652`, while
authority since stayed at `1793731492539`. Historical timestamp
`1793731622877` was rejected for raw catalog but still returned identical
nonempty authority, hydration and arrangement results on all four adapters,
including after a cold restart. User data survived by count and ID-sum checks.
Observed raw trace records fell from a peak of 92,369 to 40,397.

This is not a general health or performance pass. Sampled write/read maxima were
240.89s and 20.53s, with about six CPU cores consumed. The earlier unchanged
seeded workload panicked during txn-WAL data-shard application. The passing
repeat does not resolve that race. Promotion, authority transitions, sink
handover and multi-day behavior were not demonstrated.

## Next concrete action

Choose the next scoped repair, rather than resuming the full M3 walkthrough:

- **Txn-WAL closure race:** a data-shard register/forget apply operation panicked
  when CAS discovered a closed upper. Cached closure on entry is already accepted.
  Saved metadata identifies all five closed WAL participants as dropped temporary
  tables with catalog retirement and committed Forget. Reproduce delayed apply
  with independent Persist caches. Separately, stale new writes encounter a
  membership assertion after Forget rather than a recoverable rejection.
- **Post-admission index planning:** an index created after both builds join
  works on its creating build but stalls the other build waiting for a selection.
  Adapters must incorporate peer-created maintained objects into own-build
  planning. The successful retention witness uses the old setup's order: seeded
  index before candidate admission.
- **Long stalls:** trace the write/read outliers before attributing remaining
  CPU or latency to catalog replay. Frontier progress alone does not explain them.

Evidence, source boundaries and proposed regression shapes for the first two are
in the existing [follow-up inventory](20260903_decoupled_coordination_followups.md).
Neither repair is implemented or newly authorized by this verification task.

## Replacement and evidence

Private evidence root: `~/.local/share/aj/m3-evidence/authority-projection/replacement/`.
The preserved replacement is `run-1793731479`. Its PostgreSQL container
`aj-authority-replacement-run-1793731479` and all runtime processes are stopped.
State, blobs and exact artifact binaries are retained. The original environment,
its database and obsolete binaries were discarded with Aljoscha's authorization,
not migrated. Cleanup reclaimed about 27 GiB. About 47 GiB remains free.

Set `MZDEV_POSTGRES` and `MZDEV_BLOB` from the `url` and `blob` fields in
`run-1793731479/database.json`. Use `launch.json` for HTTP addresses. Start the saved container before using
`bin/controllerd --controller-data-directory <run>/state
--controller-binaries-directory <artifacts>` with the saved environment variables.
Generation 0 provisions pubsub. Generation 1 must use that relay via `--pubsub-url`.
Both versioned artifact sets are in `artifacts/` under the evidence root. Do not use the old
default directory or old artifact paths, which were removed.

Essential evidence within the replacement run:

- `verified-summary.json`: measured results and limitations.
- `samples.json` and `*-mz_catalog_raw-state.json`: committed and physical progress.
- `historical-{before,after}.json`: full expected rows and matching observations.
- `historical-after-compaction-live.json`, `raw-historical-read*.json`: retained
  metrics remain readable when raw catalog rejects the same timestamp.
- `{initial,after-restart}-native-plan-imports.json`: exact-build selected inputs.
- `measurement-stopped-processes.json`: all 15 measured processes stopped before
  restart. `result.json` records final cleanup, including the launcher fallback.
- `effective-policy-settings.json`, `usage.jsonl`: unchanged policies and resource
  measurements. `obsolete-environment-cleanup.json` is in the parent directory.

Keep failed evidence too: runs `1793730433` and `1793730854` reproduce the missing
index plan. Run `1793731130` records the data-shard panic. The initial negative-read
probe guessed the error wording incorrectly. SQLSTATE 22000 and the contradictory
timestamp constraints confirm the expected rejection. The local launcher can
also hit psutil pidfd `EINVAL` while stopping processes. Owned-process fallback
cleanup is recorded, not evidence of graceful shutdown.

## Remaining M3 boundaries and blockers

M3 still follows the [prompt](20260903_decoupled_coordination_prompt.md) and
[design](20260903_decoupled_coordination.md): compatible versions serve SQL, DDL
and writes before and after promotion. Retirement remains explicit. Preserve
warmed replicas, surviving deployments, shared pubsub and one authorized execution
per external sink. The [upgrade walkthrough](../controllerd.md) remains unverified.
These are prototype version variants, not released-version compatibility evidence.

Keep readiness reads off the coordinator, with their selected snapshot cut and
reader lifetime intact. Temporary-item cleanup still needs failure-preserving
session publication and Items-before-session retirement cleanup. Bootstrap
planning-revision contention still needs its scoped retry boundary. The standalone
CollectionReader lifetime issue and other architectural opportunities remain in
the follow-up inventory and prior handoff history. M2 stays closed within scope.

Broad CI is not green. [Build141419](https://buildkite.com/materialize/test/builds/141419)
failed on the unpublished `pubsubd` image after Rust compilation. Registry
visibility and upstream merge-skew failures remain unresolved. The prior Persist
`state_inspect_serde_json` object-key-order failure was not repaired or masked.
Current work has targeted verification, not a new broad-CI pass. The PR stays draft.

Keep heavy work bounded and low-concurrency on this 18 GiB VM. Scratch is optional.
The attempted worker `c9ee5c87-decoupled-coordination` timed out waiting for SSH
with a six-hour expiry and no returned instance ID. Inventory it after
reauthentication before launching another worker. Prior evidence remains in the
[archive](20260903_decoupled_coordination_log_archive.md), not as added scope.
