# Decoupled coordination: current handoff

## Objective and next action

Finish native-only ownership, execution and harness cleanup. Preserve adapter
policy, validated transactions, durable read protection, compatible full writers
and explicit retirement. Keep Persist and the 60s publication default unchanged.

Publication starvation remains the immediate blocker. Combined publications derive
protection against the current transaction, apply late frontier bounds incrementally,
and preserve consolidated effects and change marks. Direct versioned expression
dependencies now travel with definitions, and protection traversal borrows entries
instead of cloning them. Targeted checks pass. No validation, version-sensitive
lookup, publication owner or durable format changed.

The unchanged four-index workload still fails and passes on exact repeats.
Borrowed lookups reduced matching logical traversals from about 19ms to 2ms and
MV-input validation to about 1ms, but preliminary application remains expensive.
In the failing capture the old adapter committed 0/252 attempts, with a 137ms
candidate and 61ms preliminary application. These are wall timings, not CPU
attribution. The same-update-count timing spread and quota pressure remain material.

Projection counters rule out state copying and sorting as the main cost. External
CPU tracing attributes about 86% of measured application residence to typed update
application. Retained large preliminary applications consume about 24ms on CPU,
with additional scheduling delay. These are bounded diagnostic observations, not
loss-free accounting or precise userspace CPU. Async batch assembly is not the
evidenced bottleneck. Both latest diagnostic runs passed, but publication conflicts
remain frequent. No runtime is left running.

Next: distinguish costs inside typed application, including possible debug-filter
overhead, before another behavioral optimization. Keep the same workload and caps.
Existing application keeps catalog maps structurally shared. Full final reprojection
was absent from the matched adapter attempts. Empty upper progress already rebases.

The approved six-CPU diagnostic also failed. Original four/eight-CPU caps are
restored, and post-stop restoration passed a supervisor-SIGKILL check. A passing
repeat is not resolved starvation or green acceptance. Commits remain local and
unpushed. Detailed comparisons and limitations are linked below.

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

Controllerd owns processes, replica followers own maintained execution, and
adapters retain policy, transactions, protection and transient queries. Native
startup is mandatory. Temporary objects retain execution ownership and session
visibility. Subscription identities remain execution-qualified through observations,
logging and dependencies. Bounded audits found no reachable legacy alternative in
the inspected startup and command-ingress paths. Obsolete replay-history work and
its diagnostics are removed. No overall memory saving is established.

Earlier debug passes cover expression-cache reuse, metadata recovery, retained
storage, native stress, one-index permission/release, source lifecycle/status,
metrics and HTTP proxy behavior. They do not explain preceding intermittent
failures or establish latest-binary acceptance. Evidence includes backend-label
corrections where original runner receipts named the wrong blob store.

The recorded controllerd walkthrough and complete `0dt ddl` pass use 26.46/26.47
package versions sharing implementation and plan encoding. They cover full writers,
warmed execution, promotion, protection, explicit retirement and singleton CDC/sink
authority. Earlier checks cover both Timely topologies and surviving subscriptions.
This is compatible-prototype coverage with mounted binaries, not arbitrary-release,
builtin-migration or rebuilt-image CI coverage. Remaining current-binary harnesses
and explicit deferrals follow.

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
`native-publication-work-binaries.json`. The projection measurement is running
with `native-publication-projection-candidate`, verified by
`projection-publication-build-evidence.tar.gz`.
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
  Runtime failure and analysis: `index-four-deps-evidence.tar.gz`,
  `publication-deps-findings.md`, `publication-deps-phases.json` and
  `publication-deps-cpu.json`. Repeat: `index-four-deps-repeat-launch.json`.
  Borrowed-lookup build: `borrow-publication-build-evidence.tar.gz`.
  Runtime failure/repeat: `index-four-borrow{,-repeat}-evidence.tar.gz`,
  `publication-borrow{,-repeat}-matched.json` and
  `publication-borrow{,-repeat}-cpu.json`.
  Projection measurement: `projection-publication-build-evidence.tar.gz` and
  `index-four-projection-evidence.tar.gz`. Analysis:
  `publication-projection-findings.md` and `publication-projection-phases.json`.
  External CPU attribution: `publication-bpf-findings.md`,
  `publication-bpf-evidence.tar.gz`, `publication-bpf-correlated.json`, and
  `index-four-bpf-evidence.tar.gz`. This run is diagnostic-only, not acceptance.
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
