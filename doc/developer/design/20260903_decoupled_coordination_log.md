# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the current [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection and
adapter-owned policy. M2, including its 60s publication default, remains closed.
Do not restore legacy execution or weaken validation to make tests pass.

Native optimized KafkaSources completed with the CI shape: `--actions=10000
--max-execution-time=20m`, seed 42, unchanged resource caps and a 60-minute job cap.
Final validation, backup/restore and postrestore validation passed in 23m17s.
The crash audit found retired-generation controller panics. No OOM kills were
observed, but memory-limit pressure remained. This is not crash-free or broad CI
acceptance. Detailed work counts, readiness timings and failures are in
`native-optimized-kafka-audit.md` and `.json`.

The pinned upstream base `d9a2c5dc` is building with the same optimized profile.
Run it once with the same arguments, then report time to ready per generation
side by side. Distinguish the configured action limit from the work completed
before the time budget. The proposed 90-minute cap was rejected.

Complete the separate 60s four-index run and current version-pair rebuild before
further readiness tuning. Debug builds remain required for the local demo and
startup measurements, including candidate readiness. The statistics replay span
and physical compaction holders are now measured below. Do not resume operator
profiling or readiness tuning before the outstanding acceptance work.

The replica-frontier rewrite is committed and pushed. Execution keys derive from
catalog ClientIncarnation records rather than changing frontier rows. Its
maintained plan removes two joins and one DISTINCT. Debug builds, indexed semantic
regressions and the full catalog-plan check passed. Independent source review
found no blocker. No other catalog-plan expectation changed.

The timing probe obtained deltas and names for 64 unchanged operator/worker keys.
Among sampled dataflow roots, current replica frontiers cost 1.42s, current storage
frontiers 0.83s, and public frontiers 0.65s. These are scheduling durations, not CPU
samples or a complete census. Address lookup timed out. Do not sum nested scopes.

Historical source statistics gates readiness through current statistics,
hydration status and replica display. A live read-only capture found its Persist
upper current, physical since zero with three zero-holding leased readers, and
an independent critical-reader bound 30 days behind wall clock. Removing the zero
leases would not compact away this short run's history. Candidates kept the
historical index's as-of near environment creation, so its reconstruction span
grew to 14 minutes by generation 10. This explains the replay span, not its CPU
cost or the native/upstream readiness difference. See
`source-statistics-physical-since.md` and its raw evidence. No retention or Persist
change was made.

The unchanged KafkaSources comparison did not demonstrate a speedup. Parent CPU
changed by less than one percent, both runs approached the aggregate memory cap,
and neither had a new OOM. The new timing probe stopped when readiness ended,
before its second sample. That debug comparison ended before final validation.
Those debug runtimes, probes and owned Docker resources were cleaned up. The
small query/plan fixtures are not acceptance.

## Remaining acceptance and repairs

1. Complete the matching upstream optimized run and readiness comparison.
   Keep the native retirement panics visible despite completed finalization.
2. Run the separate 60s four-index publication acceptance case and report actual
   commit rates. The 18-publisher, 1s variant is stress, not an acceptance gate.
3. Rebuild the accepted 26.46/26.47 pair from current source. Rerun lifecycle, DDL,
   adapter-loss and the documented local version-upgrade walkthrough. This pair
   shares implementation and plan encoding, not release-skew or migration coverage.
4. Finish the prompt's conditional-write convergence and startup work. Recheck
   `~/.local/share/aj/m3-evidence/multi-writer-retry-survey.md` against current code.
5. Resolve CI failures and obtain broad current-binary coverage.

## Completed outcomes and limits

Native startup is mandatory. Controllerd owns processes, replica followers own
maintained execution, and adapters retain policy, transactions and transient
queries. Temporary objects and public subscription identities carry execution
ownership. Protection retries derive bounds in the current transaction. Table
registration retains handles and incremental WAL state. Retirement reuses its
listener and preserves full-prefix validation and atomic exclusion plus cleanup.

All external sinks now use catalog-selected replicas. Targeted regressions,
all-target clusterd Clippy and independent review passed. Runtime Iceberg writer
conflicts disappeared with positive table updates. This is not a full output audit.
Expression caches preserve other build namespaces, with measured
startup improvement but no full-workflow speedup claim.

Earlier binaries passed full PostgresCdc, both Timely topologies and compatible
version lifecycle coverage. Some individual ingestion and 1s stress runs passed.
Combined ingestion and latest-build lifecycle acceptance remain open. Detailed
outcomes and older evidence links are in the
[previous handoff](https://github.com/aljoscha/materialize/blob/afdb6552c96bf0ad7ddde198fbf9af53755d7266/doc/developer/design/20260903_decoupled_coordination_log.md).

## Live blockers and boundaries

- A promoted-adapter storage initialization can return `CatalogOutOfSync` when
  catch-up changes its captured transient revision. Revalidate preparation rather
  than removing that guard.
- Confirmed retirement can still produce a predecessor-controller panic. Keep
  clean retirement handling separate from cache ownership and launcher cleanup.
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
- Bring changes to write guarantees or public behavior to the user before landing.
  Potential statistics publisher overlap and retired partition cleanup are only
  source-reading leads in the [follow-ups](20260903_decoupled_coordination_followups.md).

## Worker and evidence

Worker: `i-02f17d8a7d9534f5e`, 16 CPUs, 61 GiB RAM. Shutdown is
2026-11-18 02:32 UTC, tag expiry 02:37. Runtime caps are eight CPUs/40 GiB aggregate,
four CPUs/6 GiB per deployment. Builds use one job, four CPUs/32 GiB. Never overlap
builds and runtimes. Keep heavy work off the local VM. Extension within these caps
needs no approval.

Current evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses the worker. Debug runtime binaries are pinned in sibling
`upsert-oom-investigation/native-expression-cache-candidate/`, with the tested
frontier rewrite. Worker source is base `59a59d89` plus the recorded scoped overlay.
The preceding binaries and manifests are preserved in
`upsert-oom-investigation/preserved-59a59d89-frontier-baseline/`.
Use build receipts and hashes, not image labels or historical manifests, for identity.
The debug fixture's ancillary binaries are preserved older artifacts. All seven
optimized executables, including helpers, are now built and pinned in
`upsert-oom-investigation/native-ci-optimized-candidate/`. The CI-shaped native
KafkaSources run completed and its owned resources were cleaned up. The baseline
optimized build is active. See `base-optimized-build-driver.log`.
See `native-optimized-tools-root-fixed-driver.log` and
`native-optimized-pinning.log` for build and identity receipts.

- Current timing and plan evidence: `simple-timing-findings.md`,
  `simple-timing-deltas.json`, `simple-timings-kafka-evidence.tar.gz`,
  `readiness-simple-timings-partial.tar.gz`,
  `frontier-plan-validation-evidence.tar.gz`, and `frontier-plan-validation-driver.log`.
  Rewrite verification: `frontier-static-key-verification.md`,
  `frontier-semantic-evidence.tar.gz`, `frontier-plan-capture-evidence.tar.gz`,
  `frontier-plan-recheck-driver.log`, and `source-review-frontier-static-keys.md`.
  Optimized acceptance: `native-optimized-kafka-evidence.tar.gz`,
  `native-optimized-kafka-audit.md`, and `native-optimized-kafka-audit.json`.
  Earlier debug outcome: `frontier-rewrite-runtime-comparison.md`,
  `frontier-rewrite-kafka-evidence.tar.gz`, and
  `frontier-rewrite-timings-evidence.tar.gz`. The probe has no valid timing pair.
- Dependency and producer analysis: `readiness-dependency-findings.md`,
  `readiness-dependency-timeline.json`, `source-statistics-replay-survey.md`.
  Physical-state evidence: `source-statistics-physical-since.md`,
  `source-statistics-live-evidence.tar.gz`, `source-statistics-current-asofs.log`.
- Earlier probe and small-query validation: `operator-probe-partial-findings.md`,
  `operator-probe-kafka-evidence.tar.gz`, `operator-query-validation-evidence.tar.gz`.
- CPU evidence: `readiness-stack-analysis.md`, `readiness-stack-evidence.tar.gz`,
  `stack-run-outcome.md`. Most stacks did not unwind. Offline decoder OOMs must not
  be attributed to workload runs. Compare cumulative cgroup counter deltas.
- Sink evidence: `sink-placement-runtime-comparison.md`,
  `sink-placement-verification.tar.gz`, `sink-fixed-kafka-evidence.tar.gz`.
- Build/check provenance: `combined-check-evidence.tar.gz`,
  `current-build-fixture-receipt.tar.gz`, `sink-build-provenance.log` and
  `sink-placement-build-2-driver.log`.

Preserve `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
