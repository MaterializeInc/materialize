# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. M2 and the
60s publication default remain closed.

**Next action:** after scratch reauthentication, inspect replacement worker
`i-0622cbf6d7530a395` and its build cache. Its deletion deadline has passed.
The debug build hit its one-hour limit while compiling `mz-compute`, so no
final acceptance ran. Resume the same bounded build if its cache survives.
Run lifecycle, DDL, adapter loss, scheduled-refresh MV and the compatible-version
walkthrough once on the final implementation. Keep the PR draft.

The bounded source audit found no further reachable monolithic alternative in
local launch/deployment or storage/introspection publication. Retained serialized
tags are not active publishers. Statistics and compute-introspection ownership
remain explicitly deferred. See `native-final-bounded-audit.md`.

## Live blockers and constraints

- Scratch SSO expired again. The original timing worker was confirmed absent
  before provisioning its replacement. Timing evidence is archived locally.
  The replacement's current state is unverified. No heavy local builds.
- [CI 144357](https://buildkite.com/materialize/test/builds/144357), at `f46813b3`,
  passes Linux/macOS Clippy, formatting, docs and dependency lint. It remains red:
  merge-skew encounters source conflicts, and the x86 build fails its image-publicity
  check because pubsubd is not public on DockerHub. Image-dependent runtime coverage
  is incomplete. Do not merge main or change registry permissions without approval.
- Final-head lifecycle/progress, compatible-version and combined-ingestion acceptance
  remain outstanding. Earlier successes do not certify the latest source.
- The scheduled-MV admission suite previously failed intermittently after a fixed
  compaction wait. Its next intermittent failure requires diagnosis, not another rerun.
  The process-orchestrator image-replacement fixture also previously logged background
  panics despite reporting passes.
- The retry cut is approved. Do not resume statement-by-statement scheduling
  conversions. `remaining-inline-owner-inventory.md` records deferred runtime/startup
  limitations. Promoted-adapter storage initialization can reject structural catch-up.
  Preserve validation rather than removing its guard.
- Kubernetes/operator/Terraform integration, positive builtin-schema migration and
  pre-native conversion remain deferred. No Persist redesign, publication-cadence
  changes, resource increases or unrelated optimization. Debug upsert OOM also
  reproduces on the pinned upstream base.

## Completed outcomes

Replica reconstruction defers ordinary builtin-view optimization outside required
schema dependency closures. It preserves original optimizer features and exact
execution schemas. Schema/cache, enforced-key Avro, incremental replay, follower
execution and negative-control checks pass. Full CI lint now passes too.
Matched debug runs reduce replica startup CPU from 91.47 to 43.75 CPU-seconds and
fresh serving from 54.63 to 50.51 seconds. Warm restart remains about 8.19 seconds.
No RAM improvement is established. Controller initialization and the adapter's own
reconstruction remain on the fresh-start path. This is a same-source control,
not a new upstream comparison or final M3 acceptance.

Native item admission requires protection and immutable plans. Shared dependency,
refresh and shard rows derive from durable state. Startup pruning is conditional,
and compute-free storage usage claims its interval before publication. Completed
retry repairs preserve prepared work and protection while yielding at their owners.
Further retry scheduling remains deferred rather than claimed cooperative.

Earlier native workflows demonstrated ownership, temporary/subscription identities,
compatible writers, promotion and explicit retirement. Optimized KafkaSources,
backup/restore and the 60s four-index workflow passed. The 1s variant is stress only.
The accepted 26.46/26.47 pair shares implementation and plan encoding. It is not
arbitrary-release or migration coverage and needs current-head revalidation.

## Worker and evidence

Replacement worker `i-0622cbf6d7530a395`: 16 CPUs, 61 GiB RAM, 500 GiB disk.
Scheduled shutdown: 2026-11-21 23:31:19 UTC. Deletion: 2026-11-22 00:27:20 UTC.
Recheck state and expiry before reuse. Extend within existing caps when needed.
Builds use one job, four CPUs, 32 GiB, no swap and a 30 GiB disk reserve.
Runtime uses a 28 GiB/6-CPU user slice plus an 8 GiB/2-CPU root slice.
Never overlap builds and measured runtimes.

Evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses scratch. Worktree:
`/home/ubuntu/materialize-m3-controller-followups`, provisioned clean at `8ef1fc1b`.
Verify and preserve remote work before syncing an explicit revision to an idle,
clean worker. `worker-recovery-20261121.json` records provisioning and resource caps.
`build-final-native-driver.log` records the timeout, 31.5 GiB peak memory and no swap.

Do not assume the old timing binaries or any completed final artifacts survive.
`final-acceptance/` contains locally prepared source receipts, pair-build and
lifecycle harnesses. The versioned lifecycle preserves its predecessor assertions
and adds the existing clean-controller-exit check. Container-driver repointing
remains unverified. The baseline worktree expired.
Pinned upstream remains `d9a2c5dc402fd9fa359607305669d05f27215cc4`.
Preserve the unrelated temporary TOML file in the editing worktree.

Essential pointers under the evidence root:
- `replica-metadata-verification.md`: schema contracts, source receipts and controls.
- `replica-startup-comparison.md` and its evidence archive: matched phase/CPU/RSS
  measurements, successful teardown and pending data cleanup.
- `ci-144357-outcome.md`: verified lint results and remaining CI blockers.
- `native-final-bounded-audit.md`: inspected ownership boundaries and exclusions.
- `remaining-inline-owner-inventory.md`: approved retry cut and retained limitations.
- `current-frontier-lifecycle-audit.md`, `current-frontier-ddl-audit.md` and
  `current-frontier-adapter-loss-audit.md`: prior acceptance fixtures and evidence.
- `mv-refill-verification.md`, `native-plan-admission-verification.md`,
  `storage-usage-final-checks.log` and `plan-introspection-red-green.md`: completed
  boundary repairs and their negative controls.

The [follow-ups](20260903_decoupled_coordination_followups.md) retain ownership/load
opportunities and deferred scope. The [local guide](../controllerd.md) contains the
required user-facing demo and compatible-version walkthrough.
