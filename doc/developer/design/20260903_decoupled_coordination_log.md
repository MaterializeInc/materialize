# Decoupled coordination: current handoff

## Next action

Diagnose the controller wait that blocks deployment retirement in zippy
KafkaSources. After a late promotion the retirement POST stays outstanding and
`/api/inventory` times out, while the adapter reports `IsLeader`. It reproduced
on an unchanged rerun. Find the specific await that holds the controller's
administrative loop and bound it. Do not rework controller concurrency or raise
the runner's guard.

Then, in order:

1. Finish KafkaSources and PostgresCdc at seed 42 and default length, including
   backup/restore where the scenario has it.
2. Run `index-read-protection` with four indexes once at the 60s publication
   default and report per-publisher commit rates.
3. Rebuild the 26.46/26.47 pair from the current source and rerun lifecycle,
   DDL and adapter-loss coverage against it.
4. Push the bookmark and get a broad CI run on the current head.

## Current state

Controllerd owns processes, replica followers own maintained execution, and
adapters keep policy, transactions, protection and transient queries. Native
startup is mandatory and the controller-owned execution paths are removed.
Temporary objects carry execution ownership. Subscription identities are
execution-qualified through observations, logging and dependencies.

Protection publications derive bounds inside the catalog transaction, including
the publisher's proposed client changes. Frontier-only conflicts retry without
resampling.

Verified on the current binaries, same build, original resource limits:

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

- Zippy scenarios and backup/restore are unverified. The combined data-ingest
  invocation is unverified despite the individual scenario passes.
- Native parallel stress has one repaired single-cycle pass. Repeat coverage on
  the current binaries is incomplete.
- The debug upsert hydration OOM at original limits reproduces on the pinned
  upstream base as well. No native-specific cause is established. Do not raise
  memory limits or shrink the workload.
- Broad CI is not green. The last reviewed run is
  [Build141419](https://buildkite.com/materialize/test/builds/141419), which
  predates the latest removals.
- Commits since November 10 are local only.

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

Leave the untracked
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`
alone.
