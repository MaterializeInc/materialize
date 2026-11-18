# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. M2 and the
60s publication default remain closed.

The focused startup comparison passed before and after the reviewed repairs.
The first completed-check gap fell from 20.170s to 10.063s, with no overall startup
speedup demonstrated. A stale revision caused a discarded 56.8ms check, not a
long read. Refreshing known-stale local state preserves cadence and certification.
Restart publication now belongs to the tick. Clippy, targeted catalog tests and
the focused debug runtime checks passed. See `startup-readiness-comparison.md`.

Release-version cache pruning is verified and committed. It preserves live build
variants, unknown namespaces and Written plans, refreshes incrementally, and retries
peer contention while serving cache commands. Clippy and cache API tests passed,
including a forced peer CAS and stale empty-cache regression. Independent review
found no remaining safety issue. Catalog enqueue-path runtime coverage is limited.

MV/REFRESH admission cleanup is the remaining uncommitted change. Clippy and
independent review passed. The unchanged materialized-view SQL checks are running.

Then finish the remaining conditional-write survey, including startup wrappers and
inline DDL owners. Recheck `multi-writer-retry-survey.md` and
`native-only/native-ci-recovery/current-remaining-retry-sites.md` against source.
Runtime-level activation exhaustion/tick continuation coverage remains outstanding.
Bring changes to write guarantees or public semantics to the user before landing.

## Steering and live gaps

- Restart admission retains its guards and fresh initialization still precedes
  children. HTTP and reconciliation may proceed while restart configuration is
  pending. Adapter readiness does not certify configuration publication.
- Prune ordinary cache namespaces by unused release version. Retain dev variants
  of live releases, unknown namespaces and all authoritative Written plans.
- Keep the current sink selection rule, without durable placement records.
  Its difference from upstream and dev-build cache accumulation are recorded in
  the [follow-ups](20260903_decoupled_coordination_followups.md#approved-m3-scope-limits).
- Run full lifecycle, DDL and adapter-loss acceptance once on the final head after
  removal and retry work, not per cleanup. This run and current-head compatible
  version-pair verification remain outstanding.
- Promoted-adapter storage initialization can still reject structural catch-up.
  Revalidate preparation rather than removing the guard.
- Broad current-head CI and combined ingestion remain open. The last inspected
  [CI build](https://buildkite.com/materialize/test/builds/143929), at `6e7a3107`,
  passed lint/docs checks but image builds were blocked by public pubsubd visibility.
  Merge-skew failed before compilation and dependent runtime jobs did not run.
  See `current-ci-143929.md`. Do not merge main without approval.
- Debug upsert OOM also reproduces on pinned upstream. Do not raise memory or
  shrink workloads. Positive builtin migrations, pre-native conversion and
  Kubernetes/operator/Terraform integration remain deferred. No Persist redesign,
  publication changes or unrelated optimization.

## Verified outcomes and limits

Native ownership, temporary/subscription identities, compatible full writers and
retirement have earlier targeted debug lifecycle coverage. The accepted 26.46/26.47
pair shares implementation and plan encoding, predates recent cleanup, and is not
arbitrary-release or migration coverage. Optimized KafkaSources, backup/restore
and the debug 60s four-index workflow passed. The 1s variant remains stress only.

Recent targeted checks cover native bootstrap/restart preservation, committed-plan
EXPLAIN, cluster policy, query statistics, staged ownership/privilege DDL, and
real WAL registration/append contention. Broader current-head acceptance is not
implied. Detailed outcomes, coverage limits and evidence pointers are in the
[previous handoff](https://github.com/aljoscha/materialize/blob/000adecc/doc/developer/design/20260903_decoupled_coordination_log.md).
The remaining architecture-generated load is assessed in the
[follow-ups](20260903_decoupled_coordination_followups.md#native-only-resource-comparison).

## Worker and evidence

Worker `i-02f17d8a7d9534f5e`: 16 CPUs, 61 GiB RAM. Shutdown 2026-11-18 16:00 UTC,
tag expiry 16:05. Timing runtimes are stopped. The current focused SQL test unit is
`aj-native-mv-admission-test`. Never overlap builds and runtimes. Builds use one job,
four CPUs, 32 GiB, no swap and a 30 GiB disk reserve. Runtime uses a 28 GiB/6-CPU
user slice plus an 8 GiB/2-CPU root slice. Extend the worker within these caps
without asking when needed. Restore caps and stop owned runtimes after each run.

Local evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses scratch. Verification worktree:
`/home/ubuntu/materialize-m3-controller-followups`, base `17da75c2` plus checked
source overlays. Verify hashes, not the displayed Git revision.

Essential current pointers:
- Scratch `controller-followups/native-cache-mv-final-build.log` records passing
  Clippy, cache tests and the SQL test binary build. Local
  `native-cache-mv-source.sha256` matches every tested changed source.
- Local `native-mv-admission-test.log` is the focused MV runtime check.
- `startup-before-evidence.tar.gz`, extracted `startup-before-evidence/`, and
  `startup-before-phase-summary.json` preserve the completed baseline.
- `startup-after-evidence.tar.gz`, extracted `startup-after-evidence/`,
  `startup-after-phase-summary.json` and `startup-readiness-comparison.md` contain
  the completed comparison. `summarize-startup-timings.py` extracts log-linked phases.
- Scratch `controller-followups/startup-timing-{before,after}/` contains private
  debug binaries and exact source/binary manifests. Both generations use the same
  26.46 build in this timing experiment, not the accepted distinct-version pair.
- `native_startup_readiness_20261027_aj.py` and
  `run-native-startup-readiness.sh` are the focused harness and capped wrapper.
  They measure restarts and promotion, not full lifecycle/DDL/adapter-loss acceptance.
- Scratch `controller-followups/startup-timing-after-build.log` contains scoped
  Clippy, catalog tests and build results. Cache pruning and MV admission cleanup
  are not in these artifacts.
- Preserve accepted pair artifacts under
  `upsert-oom-investigation/native-compatible-frontier-current/` and use the
  reviewed restoration hook in `run-current-frontier-pair.sh` for rebuilding.

Preserve `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
