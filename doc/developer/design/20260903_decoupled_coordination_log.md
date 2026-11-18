# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the current [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection and
adapter-owned policy. M2, including its 60s publication default, remains closed.
Do not restore legacy execution or weaken validation to make tests pass.

The current 26.46/26.47 debug pair built successfully, restored its source inputs
and passed the local lifecycle assertions and unchanged DDL workflow. Adapter-loss
is running as `aj-native-parallel-a105e001.service`. Inspect its result and cleanup
before running the prepared subscription-identity fixture. The pair shares
implementation and plan encoding, not release-skew or migration coverage.
The optimized comparison and 60s four-index acceptance are complete. Finish the
remaining pair checks before further readiness tuning. Debug builds remain
required for the local demo and phase-level restart/startup measurements.

Afterward, finish the prompt's conditional-write convergence and startup work.
Recheck `~/.local/share/aj/m3-evidence/multi-writer-retry-survey.md` against current
code. Bring changes to write guarantees or public behavior to the user before
landing. Current-head broad CI and combined ingestion coverage remain open.

## Completed outcomes and limits

Native ownership, execution-qualified temporary objects/subscriptions, current-
transaction protection derivation, incremental table registration and retirement
are implemented. External sinks use catalog-selected replicas. Expression caches
preserve other build namespaces. Earlier binaries passed PostgresCdc, both Timely
topologies and compatible-version lifecycle coverage.

The current debug local walkthrough verified full writers before and after
promotion, warmed execution, shared-relay transfer, retirement isolation and
retired-reopen rejection. The surviving held snapshot remained protected across
retirement. The candidate adapter restarted at promotion as permitted. This does
not establish SQL SUBSCRIBE continuity or a panic-free lifecycle: the running
old controller panicked during explicit retirement. See
`current-frontier-lifecycle-audit.md` for phase timings and limits. Initial
provisioning waits exclude the separate, roughly ten-minute candidate catch-up.

The complete current-pair DDL workflow passed with Kafka, PostgreSQL, MySQL,
SQL Server and webhook ingestion, Kafka sink handover, both versions writing
across promotion, warmed indexes and explicit retirement. The independent log
audit found no Materialize panic or OOM event. A PostgreSQL connection-level FATAL
was followed by successful CDC recovery. This is local debug coverage, not full
CI. See `current-frontier-ddl-audit.md` for evidence and limits.

The frontier rewrite derives execution keys from catalog records rather than
frontier rows. Debug builds, indexed semantic regressions, full catalog-plan
checks and independent source review passed. It removes two joins and one
DISTINCT, with no demonstrated whole-workflow speedup.

Native and pinned upstream `d9a2c5dc` both completed optimized KafkaSources with
`--actions=10000 --max-execution-time=20m`, seed 42, unchanged resource limits and
a 60-minute job cap. Final validation, backup/restore and postrestore validation
passed. Native retained controller retirement panics. Neither run had observed
OOM kills, but both had memory-limit pressure. The timed budget produced unequal
completed work, and the storage topologies differ. Common-generation observed
readiness averaged 26.8s upstream and 40.3s native. See
`optimized-kafka-comparison.md` for every generation, provenance and caveats.

The current debug four-index workflow passed at 60s, including pause-cap,
peer-DROP and final unpause assertions. All observed derived publication attempts
in the measured unpaused windows succeeded. The windows are short, not a steady-
state capacity measurement. See `current-four-index-60s-audit.md`.
The 18-publisher, 1s variant remains stress, not an acceptance gate.

The statistics replay question has measured physical evidence. Its Persist upper
was current, physical since was zero with three zero-holding leased readers, and
an independent critical-reader bound retained 30 days of history. Removing the
zero leases would not compact away this short run's history. Candidates kept the
historical index's as-of near environment creation, so reconstruction grew with
environment age. This explains replay span, not CPU cost or the native/upstream
readiness difference. No retention or Persist change was made.

Detailed older outcomes and evidence remain in the
[previous handoff](https://github.com/aljoscha/materialize/blob/a92b058ebb45/doc/developer/design/20260903_decoupled_coordination_log.md).

## Live blockers and boundaries

- Outgoing controllerd panics on catalog-confirmed retirement in both optimized
  and current debug runs. Workflow success does not establish crash-free
  acceptance. Clean shutdown of an already-running retired controller awaits
  approval for the exit-behavior change. Keep retired-reopen rejection distinct.
- Configuration reconciliation still retries conflicts inline. The proposed
  boundary retains handles and desired rows, rebuilds snapshot-relative diffs
  after conflicts, and keeps startup pending with backoff rather than halting.
  Readiness and write ordering remain unchanged. This proposal awaits approval.
  See `current-config-convergence-survey.md` for current-source evidence.
- Promoted-adapter storage initialization can return `CatalogOutOfSync` when
  catch-up changes its captured transient revision. Revalidate preparation rather
  than removing that guard.
- Debug upsert OOM also reproduces on the pinned upstream base. No native-specific
  cause is established. Do not increase memory or shrink the workload.
- The last inspected CI failure is
  [Build143686](https://buildkite.com/materialize/test/builds/143686), at `27a9d4ca`.
  Scoped local fixes pass, but current-head broad CI is unverified. DockerHub
  `materialize/pubsubd` visibility needs team-testing intervention. Merge-skew
  checking conflicts before compilation. Do not merge main without approval.
  Full local formatting lacked `buf`. See `current-head-ci-143686.md`.
- Positive builtin-schema migrations, pre-native conversion, Kubernetes, operator
  and Terraform integration remain deferred. Keep rejection guards and unsupported
  coverage explicit. Unrelated tracing/performance changes need separate scope.
- Statistics publisher overlap and retired partition cleanup remain source-reading
  leads in the [follow-ups](20260903_decoupled_coordination_followups.md).

## Worker and evidence

Worker: `i-02f17d8a7d9534f5e`, 16 CPUs, 61 GiB RAM. Shutdown is
2026-11-18 05:29 UTC, tag expiry 05:35. Docker runtime caps are eight CPUs/40 GiB
aggregate and four CPUs/6 GiB per deployment container. The host lifecycle uses
its separate reviewed system/user slice envelope. Builds use one job, four
CPUs/32 GiB. Never overlap builds and runtimes. Keep heavy work off the local VM.
Extension within these caps needs no approval.

Evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses the worker. Worker source is base `59a59d89` plus the exact
frontier overlay. The pair temporarily changes package versions. Its required
systemd ExecStopPost hook restores source only after the build cgroup has drained.
Do not bypass `run-current-frontier-pair.sh` or use incomplete artifacts.

Essential pointers:
- `current-frontier-lifecycle-audit.md` and `.json`, plus
  `current-frontier-lifecycle-result.log`. The locally verified archive is
  `current-frontier-lifecycle-evidence.tar.gz`.
- `current-frontier-ddl-audit.md` and `.json`, plus locally verified
  `current-frontier-ddl-evidence.tar.gz` and `current-frontier-ddl-result.log`.
- `current-frontier-adapter-loss-driver.log`. The subsequent
  `native_frontier_subscriptions.py` is staged in `upsert-oom-investigation/`
  and runs unchanged `subscribe-execution-identity` on the current pair.
- `current-frontier-pair-build-driver.log`, `current-pair-systemd-review.md` and
  `current-pair-worker-provenance.log`. Pair artifacts are in sibling
  `upsert-oom-investigation/native-compatible-frontier-current/`. The reviewed
  `native_frontier_lifecycle.py` is in `upsert-oom-investigation/`.
- `optimized-kafka-comparison.md` and `.json`, plus
  `native-optimized-kafka-evidence.tar.gz` and `base-optimized-kafka-evidence.tar.gz`.
  Both optimized pins and the original debug pin are preserved separately.
- `current-four-index-60s-audit.md` and `.json`,
  `current-four-index-60s-evidence.tar.gz`, and `current-four-index-60s-result.log`.
- `source-statistics-physical-since.md`, `source-statistics-live-evidence.tar.gz`
  and `source-statistics-current-asofs.log`.
- `frontier-static-key-verification.md` and `source-review-frontier-static-keys.md`.
  Earlier debug timing evidence had no valid before/after operator pair.

Preserve `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
