# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. M2 and the
60s publication default remain closed.

The login repair now retains provisioning and membership work across
bounded coordinator turns. Structural changes recompute the operations. Existing
strict/fail-open policy, manual grants and once-only session registration remain
intact. Real peer-contention coverage passed, failed with inline retries restored,
then passed again with the repair. Existing authentication and abandoned-startup
cleanup checks passed. Independent review and targeted lint passed.

**Next action:** migrate the remaining fixed-response SQL completion owners
through the existing staged commit boundary, starting with CREATE/ALTER NETWORK
POLICY. Keep separate completion effects with their owners. Testing failpoints
are approved. See `startup-retry-verification.md` for the completed login
repair's receipts and limitations.

Run final-head native acceptance and the compatible version-pair walkthrough after
the retry work, not after each small change.

The replica catalog-open attribution is complete. Bring the proposed metadata-only
replica boundary back for approval before implementation. Do not change Persist,
publication cadence, readiness intervals or resource limits.

## Live gaps and constraints

- Scratch access is restored. Keep builds off the editing VM and preserve the
  existing resource caps. Test-hook and plan-record metadata decisions are settled.
- Repository-wide formatting fails on an unchanged tracing macro in
  `src/adapter/src/coord/caught_up.rs:603`. Changed-file formatting and whitespace
  checks pass. See `plan-introspection-red-green.md`.
- SQL owners with separate completion effects remain in the conditional-write
  retry survey. No finite overall login retry limit or strict/fail-open
  authentication policy change is approved.
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

Shared dependency, refresh and shard rows derive from durable state. Startup
history pruning is conditional, and compute-free storage-usage collection claims
its interval before publication. Declaration reconciliation retains IDs, plans
and guarded candidates across turns. Targeted peer/restart checks and negative
controls verified these repairs. See the evidence pointers below.

Debug timing showed whole restart at 8.155s versus 18.003s before, and promotion
at 7.986s versus 12.736s. Shutdown and promotion-only relaunch repairs remove fixed
waits. Fresh startup remains 54.326s versus pinned upstream's 30.271s. These are
single observations with topology/readiness qualifications, not final acceptance.
Cold profiling attributes 70.88 replica CPU-seconds to ordinary builtin-view
optimization. No metadata-only replica implementation is approved. See
`startup-readiness-comparison.md` and `catalog-function-costs.md`.
The broader launcher suite still has background panics in its image-replacement
fixture, so it is not clean coverage of that behavior.

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

Worker `i-02f17d8a7d9534f5e`: 16 CPUs, 61 GiB RAM. Last scheduled shutdown: 2026-11-19 18:00 UTC.
Recheck instance state and retention tags before restarting or extending it. Shutdown stops the instance and preserves
its disk. Completed runtimes stopped and restored caps. Never overlap
builds and runtimes. Builds use one job, four CPUs, 32 GiB, no swap and a 30 GiB disk
reserve. Runtime uses a 28 GiB/6-CPU user slice plus an 8 GiB/2-CPU root slice.
Baseline environmentd uses each native adapter's 4 GiB/1-CPU allocation within
the user slice. Extend this owned worker within existing caps when needed.

Local evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses scratch. Native worktree:
`/home/ubuntu/materialize-m3-controller-followups`, base `ee425edf` plus verified
storage-usage, declaration and startup-retry overlays. Trust source receipts,
not the displayed Git revision. Clean baseline:
`/home/ubuntu/materialize-m3-optimized-base`, pinned `d9a2c5dc402fd9fa359607305669d05f27215cc4`.

Essential pointers:
- `startup-retry-verification.md`, `startup-final-source.json` and
  `startup-cleanup-verified.log` cover the login repair.
- `declaration-retry-verification.md`, `declaration-final-source.json` and
  `declaration-live-v1.log` cover retained declaration and metric-plan preparation.
- `storage-usage-claim-review.md`, `storage-usage-final-checks.log` and
  `storage-usage-restored-green.log` cover compute-free interval claims.
- `catalog-function-costs.md` links the cold attribution, source receipts and
  scratch-only diagnostic patch. Diagnostic runtime/database state was cleaned up.
- `pruning-red-green.md` links the passing test, blind-append failure and source
  hashes. `history-plan-green-restore-build.log` tracks fixed-binary restoration.
- `plan-view-v4-source.json` and `plan-view-catalog-tests-v4.log` identify the
  passing catalog checks. Earlier test logs preserve the non-null schema failures.
  `plan-introspection-red-green.md` records completed SQL boundary checks and
  the real-producer witness, including the blind-publisher failure.
- `dependency-refresh-fixed-evidence/`, its archive and runtime log preserve
  the reproduction of missing rows during peer join/restart, raw observations,
  final MV contents and cleanup. This is evidence of the bug, not a passing repair.
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
