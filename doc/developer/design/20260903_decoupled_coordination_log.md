# Decoupled coordination: current handoff

## Next action

Repair KafkaSources retirement contention, then rerun its unchanged workflow.
Retirement currently repeats a full raw snapshot before each exact-prefix write.
Investigate retaining retirement-relevant raw rows through the existing listener,
recomputing cleanup after catch-up. Preserve unknown fields, full-prefix validation
and atomic admission plus cleanup. Do not add a listener or raise retry limits.
The source analysis below covers seed alignment and successful own-write updates.

PostgresCdc passed its full default workload and final validation with the capture
resource fix, within the original limits and deadline. The driver now closes
selectors and pipes rather than relying on cyclic garbage collection. The worker
is idle and the completed run's containers are cleaned up.

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
expiry within these limits needs no approval.

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
constraints are surveyed in `retirement-retry-source-analysis.md`, not yet implemented.
Capture regression evidence is in `capture-fd-regression-before.log` and
`capture-fd-regression-after.log`. Passing Postgres evidence, harness provenance
and worker checks are in `zippy-capture-fixed-PostgresCdc-evidence.tar.gz`.
`zippy-capture-fixed-PostgresCdc-crash-review.log` records no crash signatures or
OOM events. No runtime remains active.

Leave the untracked
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`
alone.
