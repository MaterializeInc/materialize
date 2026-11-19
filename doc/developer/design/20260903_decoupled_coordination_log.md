# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. M2 and the
60s publication default remain closed.

Conditional startup history pruning passed the debug two-adapter regression,
including both history tables and restart. The unchanged test fails with the
former blind append, producing a negative row multiplicity. The fixed source is
restored. See `pruning-red-green.md` for exact evidence and coverage limits.

Dependency, refresh and shard rows now derive from catalog state rather than
adapter publishers. Selected plans carry physical imports, refresh schedule and
original storage as-of, without Item fields or adapter reductions. Catalog
contracts and affected SQL checks pass. The real-producer test passes across two
adapters, restart, index-drop replan and scheduled-MV replacement. Restoring the
former publishers makes it lose rows during peer join. The restored implementation
passes again. See `plan-introspection-red-green.md` for evidence and limits.

Storage-usage collection now claims its finish-time interval in the existing
batch-ID transaction, without catalog-server compute. Cached claims skip scans.
Only the winner publishes. Bounded retries release the catalog lock during backoff
and reschedule after exhaustion. A crash after claiming can leave an interval empty.
The two-adapter row/restart test passes, fails with blind publication, and passes
again after restoration. Contention, retirement and targeted Clippy checks pass.
See `storage-usage-claim-review.md` for evidence and coverage limits.

Declaration reconciliation now retains guarded candidates across subscription
turns. Metric planning/publication runs outside the coordinator while retaining IDs
and immutable revisions. Common bounded catalog retries include empty-upper
contention. Deterministic boundary tests passed, failed with the guards removed,
and passed after restoration. A live peer-declaration fixture verified metric-plan
selection and query execution on the realized replica using normal debug stacks.
Retention/protection checks and targeted lint passed. See
`declaration-retry-verification.md` for evidence and limits.

Next address login synchronization and the remaining SQL completion owners in the
conditional-write survey, then run final-head native acceptance and the compatible
version-pair walkthrough. Do not rerun the
full lifecycle suites per small change. The replica catalog-open attribution is
complete. Bring the proposed metadata-only replica boundary back for approval
before implementation. See `catalog-function-costs.md`.

The clean ee425edf debug timing workflow passed. Whole restart fell from 18.003s
to 8.155s, with shutdown falling from 10.425s to 0.508s. Promotion-to-SQL fell from
12.736s to 7.986s, and logs confirm immediate relaunch. Initialization itself is
not faster. Fresh startup remains 54.326s, with 23.431s catalog opening, against
pinned upstream's 30.271s and 17.004s. These are single observations with explicit
topology/readiness differences, not statistical or full acceptance claims. See
`startup-readiness-comparison.md` for phases, provenance and qualifications.

Cold function profiling attributes 70.88 replica CPU-seconds to ordinary
builtin-view optimization, 86.4% of measured item reconstruction CPU across four
replicas. Packing and state cloning are small. Replicas execute selected physical
plans, but the current catalog representation still reconstructs ordinary-view MIR.
A narrower metadata-only boundary needs a design decision preserving descriptions,
logical dependencies and validation. Cache namespace compatibility must not be
weakened. See `catalog-function-costs.md` for measurements, options and limits.

The shutdown timeout is reproduced and repaired at the launcher boundary:
unreaped non-children took 10.03s before and 0.13s after. The strengthened regression
fails on the original implementation and the full launcher suite passes on the
repair. TERM/KILL deadlines and identity checks are unchanged. The current timing
run confirms the full-environment improvement. See `shutdown-zombie-wait.md`.

The approved promotion-only exit status is implemented as 168, preserving 5s
backoff for other exits. The policy regression fails with unconditional backoff
and passes with the exception. The wider suite reports passes but logs background
panics in an existing current-thread image-replacement fixture, so it is not clean
image-replacement coverage. The debug timing run confirms the removed delay. See
`promotion-restart-status.md`.
No publication cadence, readiness interval or resource-limit changes are approved.

## Live gaps and constraints

- Scratch access is restored. Keep builds off the editing VM and preserve the
  existing resource caps. Test-hook and plan-record metadata decisions are settled.
- Repository-wide formatting fails on an unchanged tracing macro in
  `src/adapter/src/coord/caught_up.rs:603`. Changed-file formatting and whitespace
  checks pass. See `plan-introspection-red-green.md`.
- Login sync and SQL owners with separate completion effects remain in the
  conditional-write retry survey. Do not add a finite overall login retry limit
  or alter strict/fail-open authentication behavior without agreement.
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

Worker `i-02f17d8a7d9534f5e`: 16 CPUs, 61 GiB RAM. Shutdown 2026-11-19 18:00 UTC.
Recheck retention tags before extending it. Shutdown stops the instance and preserves
its disk. Completed runtimes stopped and restored caps. Never overlap
builds and runtimes. Builds use one job, four CPUs, 32 GiB, no swap and a 30 GiB disk
reserve. Runtime uses a 28 GiB/6-CPU user slice plus an 8 GiB/2-CPU root slice.
Baseline environmentd uses each native adapter's 4 GiB/1-CPU allocation within
the user slice. Extend this owned worker within existing caps when needed.

Local evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses scratch. Native worktree:
`/home/ubuntu/materialize-m3-controller-followups`, base `ee425edf` plus verified
verified storage-usage and declaration-retry source overlays. Trust hashes, not the displayed Git revision. Clean baseline:
`/home/ubuntu/materialize-m3-optimized-base`, pinned `d9a2c5dc402fd9fa359607305669d05f27215cc4`.

Essential pointers:
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
