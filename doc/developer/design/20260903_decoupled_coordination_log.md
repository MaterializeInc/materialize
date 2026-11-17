# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the current [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, protection and adapter-owned
policy. Do not restore legacy execution paths or weaken validation.

Use the validated single-relation timing queries to distinguish historical
source-statistics replay from aggregation before choosing a performance change.
The completed stack diagnostic
`aj-native-parallel-c4d197b0` again reached action 409 and nine promotions before
the unchanged deadline. Final validation and backup/restore remain unverified.
Its runtimes and profilers are stopped, with no workload OOM recorded.

During the sampled readiness window, s2 used 30.61 CPU seconds over 30.40 seconds.
Its compute worker accounted for 77% of its sampled user CPU. Only s2 blocked
readiness in the corresponding polls, with unhydrated collections falling from
135 to 19. Most stacks could not unwind, so this identifies the busy execution
thread, not the responsible operator. Do not repeat the full workload merely to
collect the same CPU evidence.

The final dependency census narrows this run's tail to historical source
statistics feeding current statistics, hydration status and replica display.
All four share one lagging frontier in the penultimate poll. Their other compute
imports and the public-frontier indexes are Ready. Raw statistics input progress
was not captured in that run. The subsequent `aj-native-parallel-f6e23a91` probe
captured raw-statistics import progress about 15 minutes behind wall clock while
catalog inputs were current. The probe sits after logical backpressure, enabled
by the test defaults, so this does not isolate Persist from downstream work.
Its timing query timed out and the observer stopped without retrying. The full
workload hit its unchanged deadline at action 373. All runtimes are stopped.

A small fresh fixture validated simpler scheduling-counter and separate identity
queries within the same query deadlines. This is query validation, not readiness
or KafkaSources acceptance. Use that approach rather than repeating the failed
multi-join timing query. No production change follows from these samples yet.

The earlier late generation-8 capture found both catalog-server processes using about one
CPU under a saturated eight-CPU budget. Candidate source-statistics inputs were
already near wall clock. Readiness logs identify source-status and public-frontier
indexes as the final blockers in that run. Trace those inputs from the start of
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

All external sinks now use the existing catalog-derived replica selector, matching
baseline's all-sink scheduling policy. Iceberg no longer bypasses it. Deployment,
incarnation and post-acquisition checks remain. A red/green regression covers
Kafka/Iceberg selection and catalog membership changes. Sink tests, all-target
clusterd Clippy and independent review passed. Runtime logs show competing-writer
failures disappearing while all Iceberg tables continue receiving successful
catalog updates. This is not external row-equivalence or full workload acceptance.
Readiness timings did not improve uniformly. Source placement is unchanged.

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

`sink-placement-red-3-driver.log` records the regression failing on unselected
Iceberg admission. `sink-placement-green-driver.log` records successful sink tests
and all-target Clippy. `sink-placement-inputs.json` records the checked source.

`sink-fixed-kafka-evidence.tar.gz` and `sink-placement-runtime-comparison.md`
preserve the post-fix run and comparison. Its SQL observer obtained no input
frontiers: the first attempt preceded replica startup, and the later live-side
mapping query exceeded its observation deadline. Do not treat either as an empty
frontier. Replica reconstruction uses the deployment-supplied plan build, so the
unchanged controller binary's earlier embedded SHA does not split its cache
namespace from the rebuilt clusterd. Exact binary hashes remain in the receipts.

`readiness-stack-evidence.tar.gz`, `readiness-stack-analysis.md` and
`stack-run-outcome.md` preserve the completed diagnostic and time-aligned readiness
census. `readiness-leaves.txt` recovers all sampled instructions independently of
failed unwinds. Offline decoding required 4 GiB peak memory, after the workload
stopped. Its initial 2 GiB decoder OOMs are not workload OOMs. Workload limits
remain unchanged.

`readiness-dependency-findings.md` and `readiness-dependency-timeline.json` record
the final blocker chain and AS OF crossing. `source-statistics-replay-survey.md`
separates intended retained history from unmeasured publication/cleanup concerns.
`operator-probe-kafka-evidence.tar.gz` and `operator-probe-partial-findings.md`
preserve the completed probe, workload failure and backpressure caveat.
`operator-query-validation-evidence.tar.gz` and
`operator-query-validation-owner-driver.log` preserve the small fixture's query
results and cleanup. Its first setup attempt failed under root's Git ownership
checks, reproduced without SUDO_UID, before any containers started. Running as
the repository owner resolved that setup error. Failed observations remain unknown.

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
