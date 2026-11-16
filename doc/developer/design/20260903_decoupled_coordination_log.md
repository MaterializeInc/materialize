# Decoupled coordination: current handoff

## Next action

Collect cache-repair repeat `aj-native-parallel-6de307a2` and compare startup
phases with the saved runs. The first post-fix run reached action 409 but hit
the unchanged guard during generation-10 readiness. The full workflow remains
incomplete. Workload and limits are unchanged. The new regression reproduces
cross-executable eviction on the old code and passes with the repair. Targeted
checks and the debug build passed. Independent review found no blocking issue.
Exact-build validation and Persist-format upgrade behavior are unchanged. Obsolete build
namespaces remain for later reclamation. Matched full-restart adapter initialization
fell from 50–57s to 9–14s, and later candidate catalog-open fell from 10–12s to
1.2–1.4s. Readiness still consumed about ten minutes across nine deployments.
This is a phase-specific improvement, not full workflow acceptance.

The retirement repair is pushed. Both Kafka runs completed nine promotions and
retirements, recovering HTTP contention through existing retries, but exhausted
the unchanged 1800s guard at actions 349 and 345. Neither passed the full workflow.
Startup and readiness dominate measured elapsed time. Do not raise guards,
resource caps or retry limits.

PostgresCdc passed its full default workload and final validation with the capture
resource fix, within the original limits and deadline. The driver now closes
selectors and pipes rather than relying on cyclic garbage collection. That run's
containers are cleaned up.

The registration repair passed targeted checks and seven promotion cycles with
SQL resuming. KafkaSources first hit the 1800s guard during generation 8 readiness.
Its unchanged repeat failed after generation 6 promotion when retirement exhausted
the existing HTTP contention retries. Neither run passed. Keep guards, workload
and retry limits unchanged.

Investigate retirement contention and the remaining hydration delay. The first run ended
with three system indexes unhydrated. The same IDs in the fresh repeat identify
`mz_source_statistics_ind`, `mz_hydration_statuses_ind` and
`mz_show_cluster_replicas_ind`. This identifies candidates, not their root cause.

Then, in order:

1. Finish KafkaSources and PostgresCdc at seed 42 and default length, including
   backup/restore where the scenario has it.
2. Run `index-read-protection` with four indexes once at the 60s publication
   default and report per-publisher commit rates.
3. Rebuild the 26.46/26.47 pair from the current source and rerun lifecycle,
   DDL and adapter-loss coverage against it.
4. Harden the conditional-write retry sites and reduce startup latency, as
   the prompt describes. The survey with sites and phase timings is
   `~/.local/share/aj/m3-evidence/multi-writer-retry-survey.md`.
5. Push the bookmark and get a broad CI run on the current head.

## Current state

Controllerd owns processes, replica followers own maintained execution, and
adapters keep policy, transactions, protection and transient queries. Native
startup is mandatory and the controller-owned execution paths are removed.
Temporary objects carry execution ownership. Subscription identities are
execution-qualified through observations, logging and dependencies.

Protection publications derive bounds inside the catalog transaction, including
the publisher's proposed client changes. Frontier-only conflicts retry without
resampling.

Table registration now retains adopted handles and incremental WAL state across
retries. Only missing shards receive timestamped registration after fresh catalog
validation. Controller activation prepares its four configuration tables and
defers registration contention to its loop. Compilation, WAL tests, catalog
regressions, HTTP tests and the debug binary build passed. Independent review
found no DROP/Forget ordering issue. Full runtime acceptance remains incomplete.

Verified before the registration repair, same build, original resource limits:

- Every declared ingestion workload at default duration, including promotions,
  explicit retirements and forced restarts.
- `index-read-protection` with four indexes at a 1s publication interval, twice.

Verified on earlier binaries only: distinct-version lifecycle and DDL, both
Timely topologies, surviving subscriptions, source status, metrics and the HTTP
proxy.

## Decisions

- The 26.46/26.47 pair, which shares implementation and plan encoding, is the
  accepted version pair for M3. Report it as such, not as release-skew or
  migration coverage.
- `index-read-protection` at a 1s interval is a stress test, not an acceptance
  gate. It runs 18 publishers at 60 times the default cadence, and slow
  publishers can starve under that commit rate. Do not optimize candidate cost
  further for it. The scaling limit belongs in the
  [follow-up inventory](20260903_decoupled_coordination_followups.md).
- Deferred beyond M3: positive builtin-schema migrations, conversion of
  pre-native environments, and Kubernetes, operator and Terraform integration.
  Keep the guards and report affected coverage as unsupported.
- A tracing layer allocates spans that the output filter rejects, which inflated
  the targeted DEBUG measurements. A tracing fix is separate scope.

## Open gaps

- KafkaSources and backup/restore are unverified. PostgresCdc passed on the
  registration-repair binaries with the capture fix, including standalone
  storaged kill/start and final validation. This scenario has no backup/restore.
  The combined data-ingest
  invocation is unverified despite the individual scenario passes.
- Native parallel stress has one repaired single-cycle pass. Repeat coverage on
  the current binaries is incomplete.
- The debug upsert hydration OOM at original limits reproduces on the pinned
  upstream base as well. No native-specific cause is established. Do not raise
  memory limits or shrink the workload.
- Broad CI is not green. The last reviewed run is
  [Build141419](https://buildkite.com/materialize/test/builds/141419), which
  predates the latest removals.

## Worker and evidence

Worker `i-0b3699392b71bd7d2` has 16 CPUs and 61 GiB. Runtime limits are 40 GiB
and eight CPUs in aggregate, with four CPUs and 6 GiB per deployment. Builds use
one job, 32 GiB and four CPUs on the worker. Do not overlap builds and runtimes,
and keep heavy work off the local VM. Restarting the worker or extending its
expiry within these limits needs no approval. Its shutdown timer is set for
2026-11-16 at 23:30 UTC. Only KafkaSources project
`aj-native-parallel-6de307a2` is active. Its runner is
`native_zippy_expression_cache.py`.

Evidence lives in
`~/.local/share/aj/m3-evidence/native-only/upsert-oom-investigation/`. The
detailed pointers to binaries, receipts, runners and individual runs are in
`handoff-before-cleanup.md` and `handoff-before-ingest-verification.md` there.
Historical passes are not acceptance of the latest binary.

Registration-starvation evidence is in `controller-table-stage-live-tail.log`
and `zippy-controller-table-stage-evidence.tar.gz`. Successful repair checks and
binary provenance are preserved locally in `controller-adoption-build-evidence.tar.gz`.
The Kafka failure is in `zippy-adoption-KafkaSources-evidence.tar.gz`, with phase
analysis in `zippy-adoption-phase-analysis.md`. The runner is
`native_zippy_adoption.py`. Neither the full zippy workflow nor backup/restore
passed. The bounded build resumed after its resource guard, without raising caps.
The repeat and Postgres evidence are `zippy-adoption-KafkaSources-repeat-evidence.tar.gz`
and `zippy-adoption-PostgresCdc-evidence.tar.gz`. Retirement's raw-key and prefix
constraints are surveyed in `retirement-retry-source-analysis.md`. The repair's
successful checks and exact source/binary provenance are preserved locally in
`retirement-scoped-build-evidence.tar.gz`. Its runtime runner is
`native_zippy_retirement.py`.
The timed-out run's complete evidence is saved locally in
`zippy-retirement-KafkaSources-evidence.tar.gz`. Its containers are cleaned up.
`retirement-kafka-phase-analysis.md` documents successful retirement receipts,
readiness and startup time, and a recovered generation-6 storage-initialization
`CatalogOutOfSync` exit. No panic or OOM was observed. The startup exit remains
an open defect, separate from retirement. The unchanged repeat is archived in
`zippy-retirement-KafkaSources-repeat-evidence.tar.gz` and analyzed in
`retirement-kafka-repeat-analysis.md`. It records three retired predecessor
controllers panicking on confirmed retirement. These are not new-generation
startup failures, but clean retirement shutdown remains unresolved.
Capture regression evidence is in `capture-fd-regression-before.log` and
`capture-fd-regression-after.log`. Passing Postgres evidence, harness provenance
and worker checks are in `zippy-capture-fixed-PostgresCdc-evidence.tar.gz`.
`zippy-capture-fixed-PostgresCdc-crash-review.log` records no crash signatures or
OOM events.

`startup-expression-cache-source-analysis.md` identifies cross-executable cache
eviction at controller startup. It records source and archived timing evidence,
not a measured repair. The repair preserves exact-build validation
and the existing shard-format upgrade call. Its guarded red/green build driver is
`build_expression_cache.py`. Successful checks, source hashes and binary receipts
are preserved locally in `expression-cache-build-evidence.tar.gz`. The first
post-fix runtime archive is `zippy-expression-cache-KafkaSources-evidence.tar.gz`.
`expression-cache-kafka-phase-analysis.md` gives matched before/after phases,
remaining costs and verification limits.

Leave the untracked
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`
alone.
