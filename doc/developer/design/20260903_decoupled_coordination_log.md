# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. M2 and the
60s publication default remain closed.

Finish conditional startup history pruning first. Its implementation uses the
existing fixed-timestamp write path, discarding conflicts. Compilation and the
two-adapter regression remain outstanding. A deterministic snapshot-before-write
failpoint needs approval. Also establish protection for its preselected snapshot
cut before claiming the whole path safe.

Then finish catalog-derived dependency/refresh rows, the `mz_storage_shards`
view, and conditional storage-usage collection. The shard view and publisher
removal are uncommitted and unverified. Dependencies need physical imports in
the existing selected-plan record, distinct from its broader validation imports.
Refresh derivation needs approval for evaluated schedule metadata in the existing
Item record, avoiding a change to displayed SQL. No adapter reduction is approved.

The clean pinned-upstream debug startup comparison passed. Fresh startup was
30.271s upstream versus 53.080s in the prior native capture. Catalog opening was
17.004s versus 22.544s. Whole restart was 7.272s versus 18.003s, mostly explained
by shutdown, not catalog reconstruction. These are single observations with
explicit topology/readiness differences, not current-head acceptance or a speedup
claim. See `startup-readiness-comparison.md` for phase timings and qualifications.

Cold profiling found about 17.6 CPU-seconds within the adapter's 22.7s catalog
open and about 78.3 CPU-seconds across four replicas in the captured startup
window. Parallel builtin reconstruction coincided with runtime-slice throttling.
Caller attribution remains unreliable, so no parser optimization is selected.
Investigate a narrower replica catalog boundary without skipping validation or
weakening cache compatibility. See `startup-readiness-comparison.md`.

The shutdown timeout is reproduced and repaired at the launcher boundary:
unreaped non-children took 10.03s before and 0.13s after. The strengthened regression
fails on the original implementation and the full launcher suite passes on the
repair. TERM/KILL deadlines and identity checks are unchanged. Full-environment
before/after timings remain outstanding. See `shutdown-zombie-wait.md`.

The approved promotion-only exit status is implemented as 168, preserving 5s
backoff for other exits. Compilation, tests and runtime timing remain outstanding.
No publication cadence, readiness interval or resource-limit changes are approved.

## Live gaps and constraints

- Scratch SSO expired. Renew with `bin/scratch login` before runtime verification.
  Keep builds off the editing VM. Test-hook and refresh-metadata decisions are
  pending as described above.
- Declaration reconciliation, login sync and SQL owners with separate completion
  effects remain in the conditional-write retry survey.
- The unchanged MV admission suite had an intermittent compaction-scheduling
  failure after a fixed 5s wait. Its exact rerun passed without changes, but the
  cause remains unresolved. Include the refresh-scheduled MV case in final
  acceptance. Its next intermittent failure requires diagnosis, not another rerun.
- Run lifecycle, DDL and adapter-loss acceptance once on the final head after
  removal and retry work. That run and current-head compatible version-pair
  verification remain outstanding.
- Promoted-adapter storage initialization can reject structural catch-up.
  Revalidate preparation rather than removing the guard.
- Broad current-head CI and combined ingestion remain open. The last inspected
  [CI build](https://buildkite.com/materialize/test/builds/143929), at `6e7a3107`,
  passed lint/docs checks but image builds were blocked by public pubsubd visibility.
  Merge-skew failed before compilation and dependent runtime jobs did not run.
  See `current-ci-143929.md`. Do not merge main without approval.
- Debug upsert OOM also reproduces on pinned upstream. Do not raise memory or
  shrink workloads. Positive builtin migrations, pre-native conversion and
  Kubernetes/operator/Terraform integration remain deferred. No Persist redesign
  or unrelated optimization.

## Verified outcomes and limits

Targeted review, Clippy and boundary/SQL checks passed for cache reclamation,
startup registration contention, native MV admission and recency cleanup, staged
namespace DDL and retained managed-policy retries. Policy task tests simulate the
coordinator, not real CAS interference. Namespace-specific forced contention and
external-source recency integration remain unverified. Detailed evidence and
limits are preserved in the
[previous handoff](https://github.com/aljoscha/materialize/blob/dbdfa516/doc/developer/design/20260903_decoupled_coordination_log.md).

Earlier debug lifecycle checks cover native ownership, temporary/subscription
identities, compatible full writers and explicit retirement. The accepted
26.46/26.47 pair shares implementation and plan encoding and predates recent
cleanup. It is not arbitrary-release or migration coverage. Optimized KafkaSources,
backup/restore and the 60s four-index workflow passed. The 1s variant is stress only.
The [follow-ups](20260903_decoupled_coordination_followups.md) retain approved scope
limits and the architecture-generated load assessment.

## Worker and evidence

Worker `i-02f17d8a7d9534f5e`: 16 CPUs, 61 GiB RAM. Shutdown 2026-11-18 19:00 UTC,
tag expiry 2026-11-19 18:05 UTC. Shutdown stops the instance and preserves its
disk. Completed runtimes stopped and restored caps. Never overlap
builds and runtimes. Builds use one job, four CPUs, 32 GiB, no swap and a 30 GiB disk
reserve. Runtime uses a 28 GiB/6-CPU user slice plus an 8 GiB/2-CPU root slice.
Baseline environmentd uses each native adapter's 4 GiB/1-CPU allocation within
the user slice. Extend this owned worker within existing caps when needed.

Local evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses scratch. Native worktree:
`/home/ubuntu/materialize-m3-controller-followups`, base `17da75c2` plus verified
source overlays. Trust hashes, not the displayed Git revision. Clean baseline:
`/home/ubuntu/materialize-m3-optimized-base`, pinned `d9a2c5dc402fd9fa359607305669d05f27215cc4`.

Essential pointers:
- `dependency-refresh-fixed-evidence/`, its archive and runtime log preserve
  the completed reproduction, raw observations, final MV contents and cleanup.
  `remaining-introspection-ownership.md` explains scope and the first fixture error.
- `startup-base-debug-evidence/`, its archive and phase summary preserve the clean
  baseline. Build and runtime logs record unchanged Cargo.lock and full completion.
- `startup-readiness-comparison.md` links prior native before/after evidence and
  the upstream comparison. Prior native timing binaries were removed, manifests
  retained. Those timings predate cache pruning and MV admission cleanup.
- Scratch `controller-followups/{dependency-refresh-repro,startup-base-debug}/`
  contains private binaries and manifests for the completed witnesses.
- `cold-catalog-profile-evidence/` and its archive contain completed CPU capture,
  raw samples, PID mappings and throttling. The comparison report records profiler
  limitations and the failed initial capture.
- `native-mv-admission-test.log` preserves the intermittent assertions and
  `native-mv-admission-reproduction.log` the unchanged passing rerun.
- Preserve accepted pair artifacts under
  `upsert-oom-investigation/native-compatible-frontier-current/` and use the
  restoration hook in `run-current-frontier-pair.sh` when rebuilding.

Preserve `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
