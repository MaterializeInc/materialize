# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the current [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, protection and adapter-owned
policy. Do not restore legacy execution paths or weaken validation.

Resolve the Iceberg replica-placement decision below, then continue readiness
attribution before choosing a performance change. The current debug workload hit
its unchanged deadline at action 400 after nine promotions. Final validation and
backup/restore remain unverified. Its runtimes are cleaned up, with no recorded
OOM events. Scoped Python/Rust checks, catalog oracle regressions and the fresh
debug build passed. Runtime acceptance remains open.

The late generation-8 capture found both catalog-server processes using about one
CPU under a saturated eight-CPU budget. Candidate source-statistics inputs were
already near wall clock. Readiness logs identify source-status and public-frontier
indexes as the final blockers in this run. Trace those inputs from the start of
readiness. Neither these late samples nor earlier leaf profiles identify the
full readiness critical path.

Both cache-repair comparison runs reached action 409 and exhausted the unchanged
1800s guard during generation 10 readiness. Each completed nine promotions and
retirements, recovering two HTTP contention responses. Full KafkaSources and its
backup/restore remain unverified. Do not raise guards, resource caps or retry limits.

After resolving the concrete blockers:

1. Finish unchanged KafkaSources, including final validation and backup/restore.
2. Run four-index `index-read-protection` once at the 60s publication default and
   report actual commit rates, not attempts.
3. Rebuild the accepted 26.46/26.47 pair from current source and rerun lifecycle,
   DDL, adapter-loss and the documented local version-upgrade walkthrough.
4. Complete the conditional-write and startup work in the prompt. Recheck the
   leads in `~/.local/share/aj/m3-evidence/multi-writer-retry-survey.md` against source.
5. Resolve current-head CI failures and obtain broad current-binary coverage.

## Verified outcomes and limits

Controllerd owns processes, replica followers own maintained execution, and
adapters keep policy, transactions, protection and transient queries. Native
startup is mandatory. Temporary objects carry execution ownership. Subscription
identities are execution-qualified through observations, logging and dependencies.

Protection publications derive bounds inside the transaction, including proposed
client changes. Compatible frontier-only conflicts retry without resampling.
Table registration retains adopted handles and incremental WAL state. Only missing
shards need timestamped registration, with fresh catalog validation. Controller
activation prepares its four configuration tables.

Retirement now retains exact raw inputs through the existing listener and
recomputes cleanup against the current prefix. Its bounded request releases that
preparation on completion. Targeted tests, independent review and repeated runtime
retirements passed. It preserves full-prefix validation and atomic exclusion plus
cleanup, including compatible-build fields.

Cache opens no longer evict other build namespaces. Exact-build validation and
Persist-format upgrades are unchanged. Red/green regression tests, targeted
checks, debug builds and independent review passed. Matched full-restart adapter
initialization fell from 50–57s to 9–14s in the first comparison, with the benefit
repeated. Later candidate catalog-open fell from 10–12s to about 1–2s. Readiness
still costs about ten minutes across nine deployments. Obsolete cache namespaces
remain for later reclamation. This is not a full-workflow speedup claim.

PostgresCdc passed its full default workload and final validation with the capture
resource fix, including standalone storaged kill/start. It has no backup/restore.
Individual ingestion workloads and the four-index 1s stress passed on earlier
builds. Combined ingestion remains unverified. Earlier builds also passed the
accepted distinct-version lifecycle, both Timely topologies, subscriptions,
source status, metrics and HTTP proxy coverage. These are not latest-build acceptance.

## Live blockers and decisions

- Iceberg lacks Kafka's catalog-derived replica selector. Two replicas in the
  active deployment repeatedly conflict and restart the same sinks. Applying the
  existing selector to Iceberg awaits user approval because it changes placement.
  Commit conflict checks must remain. The evidence does not establish duplicate
  committed output or quantify this contention's share of CPU.
- Readiness/hydration remains the largest measured Kafka phase. Map the remaining
  system-cluster blockers and attribute their work or input waits before tuning.
- One pre-cache run had a recovered promoted-adapter storage-initialization
  `CatalogOutOfSync` exit. Source retries only while catch-up preserves a captured
  transient revision. Do not remove that guard without revalidating preparation.
- Retired predecessor controllers sometimes panic on confirmed retirement. The
  diagnostic reproduces this for generation 4. Keep this separate from cache
  ownership. Process cleanup remains the launcher's receipt-checked operation.
- Native parallel stress needs repeat coverage on current binaries. Debug upsert
  hydration OOM at original limits also reproduces on the pinned upstream base.
  No native-specific cause is established. Do not raise memory or shrink workloads.
- Current-head CI is red: [Build143686](https://buildkite.com/materialize/test/builds/143686)
  covers `27a9d4cad532`. Scoped lint/type/format corrections pass locally, but
  broader CI has not validated them.
  DockerHub visibility for `materialize/pubsubd` needs team-testing intervention.
  Merge-skew checking conflicts before compilation. Do not integrate main without
  approval. See `current-head-ci-143686.md` for job evidence. Local Python checks
  pass. Full local formatting is blocked by missing `buf`, not a protobuf finding.
- The accepted 26.46/26.47 pair shares implementation and plan encoding. Do not
  present it as release-skew or positive migration coverage.
- Four-index publication at 1s is stress, not acceptance. Keep the 18-publisher
  conflict-rate limit in the [follow-ups](20260903_decoupled_coordination_followups.md).
  Do not optimize candidate cost further for that regime.
- Positive builtin-schema migrations, pre-native conversion, Kubernetes, operator
  and Terraform integration are deferred. Keep rejection guards and unsupported
  coverage explicit. A tracing-layer overhead fix is separate scope.
- Bring changes to write guarantees or public behavior to the user before landing.

## Worker and essential evidence

Worker `i-02f17d8a7d9534f5e` replaces the expired worker and its deleted disk.
It has 16 CPUs, 61 GiB and 500 GiB disk, with shutdown at 2026-11-17 19:25 UTC
and tag expiry at 19:27 UTC. Keep runtime aggregate limits at eight CPUs/40 GiB and
four CPUs/6 GiB per deployment. Builds use one job, four CPUs/32 GiB. Do not overlap
builds and runtimes. Keep heavy work off the local VM. Extending or restarting the
worker within these caps needs no approval.

Evidence root: `~/.local/share/aj/m3-evidence/native-only/upsert-oom-investigation/`.

Replacement setup and verification live in sibling `native-ci-recovery/`:
`worker.json`, `remote.py`, `combined-driver.log`, and
`combined-lint-inputs.json`. `combined-check-evidence.tar.gz` preserves successful
verification of the lint changes against base `7339c6def589`. The worker then
synced clean commit `e3094b50ed8f`. `current-build-3-driver.log` records the successful
debug build. `current-build-fixture-receipt.tar.gz` preserves verified source and
binary provenance plus restored image identities. `kafka-runner-recovery.md`
identifies the preserved ancillary binaries and exact fixture recipes. Do not use
historical runtime manifests to authenticate replacement binaries.

`current-kafka-diagnostics.tar.gz` preserves the completed recovery runs. The first
stopped on a diagnostic Docker inventory race. The narrow observer repair passed
a real Docker red/green check without changing workload guards. The repeat,
`aj-native-parallel-a39d02c7`, hit the original deadline. See
`diagnostic-outcome-a39.json` and `inventory-race-check.log`.

`readiness-inputs-a39d02c7.tar.gz`, `readiness-input-capture-analysis.md` and
`g8-input-capture-controller.log` preserve the late SQL/counter capture and matched
readiness decisions. The first observer lacked permission to inspect container
processes. The root observer attached late, not at the start of readiness.
`sink-execution-contention-survey.md` traces the separate Iceberg placement gap.
The post-promotion replica SIGKILLs were intentional workload action 324, not a
promotion failure.

- Earlier cache-repair binaries and checks: `expression-cache-build-evidence.tar.gz`.
  Runner: `native_zippy_expression_cache.py`. Diagnostic helpers:
  `watch_readiness_profile.py` and `profile_readiness_cpu.py`.
- Matched phase evidence: `expression-cache-kafka-phase-analysis.md` and
  `expression-cache-kafka-repeat-analysis.md`, with exact archive citations.
  Runtime archives: `zippy-expression-cache-KafkaSources[-repeat]-evidence.tar.gz`.
- Readiness attribution: `readiness-cpu-attribution.md`, backed by
  `readiness-cpu-profile-complete-evidence.tar.gz` and
  `zippy-readiness-profile-evidence.tar.gz`. It separates CPU counters, sampled
  functions, input uncertainty, quota pressure and the retirement panic.
- Input capture plan: `readiness-input-observability.md`. The one-shot
  `probe_readiness_inputs.py` passed local deadline/error-handling checks but has
  not run against Materialize. Attach process/deployment provenance and invoke
  serially under a supervisor. Missing SQL rows do not prove completed inputs.
- Earlier retirement and startup failures: `retirement-kafka-phase-analysis.md`
  and `retirement-kafka-repeat-analysis.md`. Raw-key contracts and checks:
  `retirement-retry-source-analysis.md`, `retirement-scoped-build-evidence.tar.gz`.
- Cache mechanism: `startup-expression-cache-source-analysis.md`.
- Passing Postgres and capture provenance:
  `zippy-capture-fixed-PostgresCdc-evidence.tar.gz` and its crash-review log.
- Older detailed evidence: `handoff-before-cleanup.md` and
  `handoff-before-ingest-verification.md`. Historical passes do not validate
  current binaries.

Leave `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`
alone.
