# Decoupled coordination: current handoff

## Objective and next action

Finish native-only ownership, execution and harness cleanup. Preserve adapter
policy, validated transactions, durable read protection, compatible full writers
and explicit retirement. Keep Persist and the 60s publication default unchanged.

Finish publication-starvation verification before the remaining harnesses. Owners
submit runtime samples and proposed client changes together. Each transaction
attempt derives protection against its current catalog candidate. Frontier-only
batches apply late bounds incrementally and derive consolidated final effects and
change marks. Other batches retain full reprojection. Independent findings are
resolved, and affected type-checks, retention tests and change-mark tests pass.

The corrected four-index run still failed its release deadline. Its exact repeat
passed. In the failed trace, incremental projection ran for both adapters and both
catalog-server replicas. Adapter final application fell to roughly 8–12ms, but
candidate construction still took 165–202ms and lost most commits. This is not a
resolved starvation bug or green acceptance.

The profiled run passed, but both deployments saturated their four-CPU limits:
3.92/3.73 average cores, with throttling in 85%/62% of quota periods. Most CPU
samples could not be unwound, so the profile does not establish function-level
attribution. The timing evidence still shows expensive candidate construction.

The user approved one six-CPU-per-deployment diagnostic with a twelve-CPU
aggregate cap, then restoration of the original four/eight caps. Memory, workload,
cadence, assertions and retry limits remain unchanged. Passing alone is not
acceptance. Compare per-publisher candidate timings, CPU/throttling and commit
success rates, and obtain usable CPU attribution or narrower stage measurements
before another behavioral change.

The six-CPU diagnostic failed the unchanged post-DROP permission deadline.
Its old/new adapter candidate medians with late constraints were 125/77ms,
but only 0/261 and 30/345 traced derived attempts committed, respectively.
Catalog-server replicas committed 80/328 and 66/365. More CPU did not resolve
starvation. All containers are removed and the aggregate cap is restored to eight
CPUs. Restoration also passed an independent supervisor-SIGKILL check.

The narrower original-cap measurement passed, without resolving intermittency.
Dependency discovery is expensive: plan discovery takes 24–27ms at the median,
including 18–19ms in logical walks. Each publisher also revalidates 50 maintained
requirements in about 11ms. Reduction takes 1–3ms and upper observations less
than 1ms. Preliminary application remains substantial and is not decomposed.
These are wall timings, not CPU attribution. Full evidence and limitations are in
`publication-work-findings.md` below.

The candidate retains each View/MV expression's direct versioned dependencies
alongside the expression. Construction and replacement keep them paired. Current
catalog traversal, name-resolution references and all protection validation remain
unchanged. There is no candidate, bound or transitive-dependency cache. Independent
review found no safety defect. Changed-input replacement, version preservation,
reconstruction parity and protection tests pass, as do affected type-checks and
the scratch debug build.

Next: analyze the dependency-reuse measurement running under the original limits
(`aj-native-parallel-8417584f`). Do not claim reduced starvation
from a single pass. Measurement commit `4b94781a` and transaction repair
`e112e930` remain local and unpushed.

Keep the original workloads, bounds and resource caps. Do not increase publication
cadence, retries or timeouts, split publication, or add consensus refreshes merely
to pass the test.

The upsert investigation reproduced the original-limit debug OOM on pinned
upstream base `d9a2c5dc`, twice. Both native and baseline sampled heaps are dominated
by decoded upsert rows and processing buffers. Native ownership is not required
to reproduce the failure. Startup timing and progress differ, so this does not
exclude a native contribution to severity. No native-specific repair is evidenced.
A general upsert input-memory bound is separate work, not part of this cleanup.
See `upsert-oom-investigation/findings.md` under the evidence root below.

## Verified outcome

Native startup reuses validated Local expressions without cache maintenance or a
second plan-store open. In the debug frontier fixture, new-replica startup falls
from 31–33s to roughly 5s and the complete test passes twice. Metadata-only
identity recovery has red/green catalog coverage and retained-storage safety
coverage. The current native stress run completes one promotion/retirement cycle
and final checks without a captured bootstrap panic. The full one-index protection
workflow also passes, including concurrent DROP, paused caps and release. Its
slowest committed-permission observation took 23s. These passes do not explain
the earlier intermittent failures or establish a steady-state resource saving.
The complete source-failpoint lifecycle also passes. Its final status query
returns `running` in 1.84s after promotion and retirement, rather than the earlier
673s. The index and source workflows used PostgreSQL metadata and shared-file
blobs. Their original private runner receipts incorrectly label blobs as Minio,
corrected in `retained-workflow-backend-correction.json` alongside the evidence.

Controllerd owns processes, replica followers own maintained execution, and
adapters retain policy, transactions, protection and transient queries. Native
startup is mandatory. Temporary objects carry execution ownership while retaining
session visibility. Subscription identities remain execution-qualified through
observations, logging and dependencies. Bounded independent audits found no
reachable legacy alternative in the inspected startup and command-ingress paths.

Replay-history cloning, compaction and both obsolete gauges are removed. Affected
all-target/library checks, current-binary metrics and HTTP-proxy workflows pass.
The accepted command-size expectation passes the complete compute-client library suite.
The repaired upsert load-test runs its unchanged original scenarios successfully.
No performance or memory saving from cleanup has been measured.

The current local `bin/controllerd` walkthrough and full `0dt ddl` workflow pass
with 26.46/26.47 package versions sharing implementation and plan encoding. They
cover concurrent SQL/DDL/table writers around promotion, warmed execution,
protection, explicit retirement, rejected retired writers, sink authority and
singleton CDC handover. Earlier checks cover both Timely topologies and surviving
subscriptions. This is compatible-prototype coverage using mounted binaries,
not arbitrary-release, builtin-migration or rebuilt-image CI coverage.

## Live gaps and approved deferrals

- Native parallel promotion has a passing fixed-binary seed-42 repeat, but the
  preceding run fails on `SHOW CLUSTER REPLICAS` with a catalog-server receive
  timeout. No resource guard fires. The repeat's resource observations show CPU
  throttling, which does not establish the cause of the earlier timeout.
  Cache-enabled stress promoted twice but exited with the second retirement still
  running. It skipped final checks. The adapter exits at promotion are expected,
  not evidence of crashes. The exact repeat completed retirement but crash-looped
  afterward in storage bootstrap. Controllerd also aborts when reporting the child
  panic. The first run's retirement delay remains unattributed.
  The repaired candidate's first run instead exhausted retirement's existing
  retries with `catalog changed; retry the request`. The latest run passes one
  cycle, not a reproducibility claim for the earlier two-cycle failure.
- Original-limit bounded-memory upsert-index hydration still fails. Native and
  both baseline runs report clusterd OOMKilled/exit137 at 3.5 GiB with ample host
  RAM. The 256-MiB control bounds Persist feedback, not decoded input. Heap stacks
  do not identify the retaining queue. Periodic jemalloc gauges are not exact
  simultaneous accounting. Do not raise limits or shrink workloads to hide this.
- Data-ingest, randomized zippy/restore and bumped-version platform acceptance
  remain incomplete. Parallel stress and one-index publication now have current
  passes, but the current four-index case fails and earlier intermittent failures remain unexplained. The source
  status latency repeat passes. Adapter-loss needs a Persist CLI/jobs artifact.
- Positive native builtin-schema migrations, pre-native conversion and current-native
  Kubernetes/operator/Terraform integration are unsupported within M3. Keep guards
  and visible unsupported outcomes without disabling historical/unrelated tests.
- Broad CI is not green. Last reviewed evidence is
  [Build141419](https://buildkite.com/materialize/test/builds/141419). Registry/image
  gates, merge skew, Persist JSON-order checking and stale hydration expectations
  remain unresolved. Full bin/fmt reports unchanged long catalog-file lines.
  Changed-file checks pass. The PR stays draft.

## Worker and essential evidence

Evidence root: `~/.local/share/aj/m3-evidence/native-only/`.

Worker `i-0b3699392b71bd7d2` is accessible. Its lease ends at 23:00 UTC November 15.
Helpers and receipts are in `upsert-oom-investigation/`. Comparison debug binaries
are pinned there as `native-publication-marks-candidate`, verified by
`marks-publication-build-source-sha256.json` and
`native-publication-marks-binaries.json`. The eight-file transfer is recorded in
`publication-marks-source-transfer.json`. The completed original-cap measurement
used `native-publication-work-candidate`, verified by
`work-publication-build-source-sha256.json` and
`native-publication-work-binaries.json`. The dependency-reuse measurement is
running with `native-publication-deps-candidate`. Its source and binary manifests
are in `deps-publication-build-evidence.tar.gz`.
Local disk has about 40GiB free. Worker disk has about 240GiB free.

No Persist source changed. Runtime supervision has independent systemd post-stop
cleanup, verified with a real SIGKILL. CLI authentication is restored. No access
rules changed. Prior worker `i-064fa517ec1d8bbdb` is
terminated and its old helper is not reusable.

Keep heavy work sequential and off the local VM. Worker aggregate limits are
40 GiB, eight CPUs and no swap. Builds use one job, 32 GiB and four CPUs.
Preserve 30 GiB disk and 2 GiB available RAM. Completed runs' containers are removed.
Do not overlap builds with test runtimes.

Essential pointers, relative to the evidence root:
- Cache implementation checks and complete frontier passes:
  `upsert-oom-investigation/cache-build-evidence.tar.gz`,
  `frontier-cached-evidence.tar.gz`, and `frontier-cached-repeat-evidence.tar.gz`.
- Cache-enabled incomplete stress: `upsert-oom-investigation/parallel-cached-evidence.tar.gz`.
  Crashing repeat: `parallel-cached-repeat-evidence.tar.gz` in that directory.
  Its `aj-native-parallel-d593aae2/5fa43c4b68d3.log` contains the bootstrap panics.
- Confirmed retained-metadata regression and initial build receipts:
  `upsert-oom-investigation/retained-red-evidence.tar.gz`.
  Passing repair checks and runtime provenance: `retained-green-evidence.tar.gz`.
  Repaired retirement conflict: `parallel-retained-evidence.tar.gz`.
  Expected-retirement policy failure: `parallel-retained-repeat-evidence.tar.gz`.
  Policy red/green logs and patch: `retirement-policy-*.log` and
  `retirement-error-policy.patch` in the same directory.
  Passing current stress: `parallel-retirement-policy-evidence.tar.gz`.
  Current index protection/publication: `index-retained-evidence.tar.gz`.
  Current source lifecycle/status latency: `source-status-retained-evidence.tar.gz`.
  Failed four-index release: `index-four-retained-evidence.tar.gz`. Its private
  failure-only probe has a port-helper argument error, so limiting-grant state
  was not captured. `native_index_four_probe_fixed.py` corrects that probe only
  and is not used in the unchanged repeat.
  Passing exact repeat: `index-four-retained-repeat-evidence.tar.gz`.
  Recurrence with working probes: `index-four-probe-evidence.tar.gz` and
  `index-four-limiting-grants.json`.
  Transaction-stage trace: `index-four-trace-evidence.tar.gz`,
  `index-four-trace-analysis.json` and `index-four-trace-findings.md`.
  Six-CPU diagnostic: `index-four-six-cpu-evidence.tar.gz`,
  `publication-six-cpu-findings.json`, `publication-six-cpu-all-processes.txt`,
  and `six-cpu-restoration-sigkill.log`. Its CPU counters cover full deployment
  lifetimes, not the original profile's shorter window.
  Per-publisher comparison: `six-cpu-publication-comparison.{md,json}`.
  Measurement build: `work-publication-build-evidence.tar.gz` and
  `work-publication-source-updates.json`. Completed measurement:
  `index-four-work-evidence.tar.gz`, `publication-work-phases.json` and
  `publication-work-findings.md`.
  Dependency-reuse candidate: `deps-publication-source-updates.json`,
  `deps-publication-build-evidence.tar.gz` and `index-four-deps-launch.json`.
  CPU/quota findings: `publication-cpu-findings.json`,
  `publication-cpu-evidence.tar.gz` and `index-four-marks-profile-evidence.tar.gz`.
  Corrected build: `marks-publication-build-evidence.tar.gz`.
  Corrected runtime failure/repeat: `index-four-marks-trace-evidence.tar.gz`,
  `index-four-marks-repeat-evidence.tar.gz`, and
  `publication-marks{,-repeat}-all-processes.txt`.
  Diagnostic runtime: `index-four-diagnostic-trace-evidence.tar.gz`,
  `index-four-diagnostic-repeat-evidence.tar.gz`, and
  `publication-trace-diagnostic{,-repeat}-summary.json`.
  Per-publisher baseline: `publication-diagnostic-all-processes.txt`.
  Review resolution: `publication-incremental-review-resolution.json`.
  Diagnostic build: `diagnostic-publication-build-evidence.tar.gz`.
  Static projection advice: `publication-final-projection-review.json`.
  Combined-operation tests/build: `combined-publication-build-evidence.tar.gz`.
  Combined-operation runtime failure/repeat:
  `index-four-combined-trace-evidence.tar.gz`,
  `index-four-combined-repeat-evidence.tar.gz`,
  `publication-trace-combined{,-repeat}-summary.json`, and
  `index-four-combined-limiting-grants.json`.
  First rederivation tests/build: `publication-build-evidence.tar.gz`.
  First rederivation runtime failure/repeat/trace:
  `index-four-publication{,-repeat,-trace}-evidence.tar.gz`.
  Per-stage traces with different observation windows: `publication-trace-{baseline,repaired}-summary.json`.
  Remaining zippy artifacts and restore-contract audit: `zippy-acceptance-audit.md`.
- Passing unchanged stress repeat and observed resource pressure:
  `upsert-oom-investigation/parallel-fixed-repeat-evidence.tar.gz`.
- Fixed-binary stress failure:
  `upsert-oom-investigation/parallel-fixed-failure-evidence.tar.gz`.
- Compiled decoder and full plan verification:
  `upsert-oom-investigation/compiled-decoder-regression-evidence.tar.gz` and
  `frontier-slt-defaults-evidence.tar.gz`. Earlier SLT probes omitted standard
  launcher defaults. Their broad generated rewrite was discarded, not committed.
- Compiled frontier failure and post-failure SQL evidence:
  `upsert-oom-investigation/frontier-testdrive-failure-evidence.tar.gz` and
  `frontier-diagnostic2-probes.log`. These are failures, not acceptance passes.
  `frontier-trace2-evidence.tar.gz` contains the traced startup/progress failure.
- Parallel failure: `upsert-oom-investigation/parallel-first-evidence.tar.gz`,
  `parallel-runtime-repeat.log`, and `frontier-decoding-proof-evidence.tar.gz`.
  Build: `frontier-build-worker.log`, `frontier-build-driver.log`,
  `guard_frontier_build.py` and `build_frontier_candidate.sh`. Logs are now durable
  on the worker after an SSH pipe interruption. The build includes sqllogictest.
  Prepared verifiers: `native_frontier_testdrive.py`, `native_frontier_slt.py` and
  `native_parallel_fixed.py`. SLT runs the actual native harness in a container
  without systemd, so its replica children inherit the container budget.
  `parallel-provenance-review.md` documents exact source-guard exceptions.
  The missing mc fixture was built from its unchanged repository recipe.
- Upsert diagnosis, controls and independent review:
  `upsert-oom-investigation/findings.md` and `comparison-review.md` there.
  Its native/baseline profile archives, repeated-baseline archive, function-level
  reports and build/binary manifests preserve the detailed measurements.
- Local lifecycle: `native-driver-scratch/native-history-lifecycle-complete-evidence.tar.gz`.
- Distinct-version DDL/sink/CDC:
  `native-driver-scratch/native-history-compatible-ddl-evidence.tar.gz`.
- Metrics/proxy and repaired upsert load-test: `native-driver-scratch/` archives
  `native-history-metrics-evidence.tar.gz` and `native-history-upsert-evidence.tar.gz`.
- Runtime/error-policy reviews and detailed earlier outcomes:
  `native-driver-scratch/native-handoff-before-upsert-baseline.md`.
  Both versioned native candidates and their manifests remain under that directory.

The [follow-up inventory](20260903_decoupled_coordination_followups.md) separates
remaining architectural opportunities from this cleanup. The
[publication comparison](20260903_decoupled_coordination_publication_interval.md)
and `logical-component-load/` remain the performance baseline, not measured
benefits of the removals. Preserve native observation SQL inputs until replaced.

Preserve the unrelated
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
