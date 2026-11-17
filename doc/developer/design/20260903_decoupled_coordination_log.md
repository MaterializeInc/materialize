# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the current [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection and
adapter-owned policy. M2, including its 60s publication default, remains closed.
Do not restore legacy execution or weaken validation to make tests pass.

Measure the verified replica-frontier rewrite in the unchanged bounded
KafkaSources workflow. Execution keys now derive from catalog ClientIncarnation
records rather than changing frontier rows. The maintained plan removes two joins
and one DISTINCT. No other catalog-plan expectation changed. Debug builds,
indexed semantic regressions and the full catalog-plan check passed. Independent
source review found no blocker. Workload speedup remains unverified.

The timing probe obtained deltas and names for 64 unchanged operator/worker keys.
Among sampled dataflow roots, current replica frontiers cost 1.42s, current storage
frontiers 0.83s, and public frontiers 0.65s. These are scheduling durations, not CPU
samples or a complete census. Address lookup timed out. Do not sum nested scopes.

Historical source statistics also gates readiness through current statistics,
hydration status and replica display. Its raw-input probe lagged while catalog
inputs were current, but that probe follows logical backpressure. It does not
isolate Persist from downstream work. Keep these two findings distinct.

The latest unchanged KafkaSources workload again exhausted its 1800s diagnostic
guard. Final validation and backup/restore remain unverified. Runtimes and probes
are stopped. Do not repeat full runs merely to collect the same evidence or
increase workload limits. The small query/plan fixtures are not acceptance.

## Remaining acceptance and repairs

1. Finish KafkaSources, including final validation and backup/restore.
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
2026-11-17 19:25 UTC, tag expiry 19:27. Runtime caps are eight CPUs/40 GiB aggregate,
four CPUs/6 GiB per deployment. Builds use one job, four CPUs/32 GiB. Never overlap
builds and runtimes. Keep heavy work off the local VM. Extension within these caps
needs no approval.

Current evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses the worker. Debug runtime binaries are pinned in sibling
`upsert-oom-investigation/native-expression-cache-candidate/`, from code `59a59d89`.
Use build receipts and hashes, not image labels or historical manifests, for identity.
Ancillary binaries are preserved older artifacts, not newly rebuilt ones.

- Current timing and plan evidence: `simple-timing-findings.md`,
  `simple-timing-deltas.json`, `simple-timings-kafka-evidence.tar.gz`,
  `readiness-simple-timings-partial.tar.gz`,
  `frontier-plan-validation-evidence.tar.gz`, and `frontier-plan-validation-driver.log`.
  Rewrite verification: `frontier-static-key-verification.md`,
  `frontier-semantic-evidence.tar.gz`, `frontier-plan-capture-evidence.tar.gz`,
  `frontier-plan-recheck-driver.log`, and `source-review-frontier-static-keys.md`.
  New SQL binaries are in worker `target/debug`. The pinned workload directory
  above still contains the preceding build and must be refreshed before measuring.
- Dependency and producer analysis: `readiness-dependency-findings.md`,
  `readiness-dependency-timeline.json`, `source-statistics-replay-survey.md`.
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
