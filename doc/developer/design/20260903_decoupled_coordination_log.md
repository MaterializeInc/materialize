# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the current [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection and
adapter-owned policy. M2, including its 60s publication default, remains closed.
Do not restore legacy execution or weaken validation to make tests pass.

The current 26.46/26.47 debug pair passed local lifecycle, DDL, adapter-loss and
subscription-identity coverage and debug restart measurements. All owned runtimes
are stopped. A small controller-cache publication repair is uncommitted in
`src/environmentd/src/controllerd.rs` and `controllerd/startup.rs`. Source review
and its bounded debug build pass. Next, review the pending private
`native_controller_cache_probe.py`, then compare the preserved old debug pin and
`native-controller-cache-candidate` sequentially. Verify actual cache hits and
recovery, including cache-disable behavior, before claiming improvement or committing.
The version pair shares implementation and plan encoding, not release-skew or
migration coverage. Optimized comparison and 60s four-index acceptance are complete.

Afterward, finish the prompt's conditional-write convergence and startup work.
Recheck `~/.local/share/aj/m3-evidence/multi-writer-retry-survey.md` against current
code. Bring changes to write guarantees or public behavior to the user before
landing. Current-head broad CI and combined ingestion coverage remain open.

## Completed outcomes and limits

Native execution ownership, temporary-object/subscription identities, protection
derivation, incremental registration, external execution selection and expression
cache namespace preservation are implemented. Current debug acceptance covers
both writers across promotion, warmed execution, relay transfer, explicit
retirement, Kafka/PostgreSQL/MySQL/SQL Server/webhook ingestion, Kafka sink
handover, adapter-loss recovery and colliding subscription identities. The latter
covers joins, independent cleanup, incumbent continuity across promotion and
survivor progress after retirement. See the linked audits for exact scope.

These are targeted local checks, not full CI. The host walkthrough still exposes
a live-retirement controller panic. Candidate adapter restart at promotion remains
permitted. DDL, adapter-loss and subscription log audits found no Materialize panic
or OOM. Successful SQL/compaction values are not all saved separately, so some
claims rely on executed unchanged assertions rather than numerical replay.

Debug measurements passed at unchanged limits: adapter-only restart 9.2s,
promotion request to serving 12.6s excluding catch-up, and whole-native relaunch
24.4s after a separate 10.4s stop. PostgreSQL remained alive. The adapter did not
repeat the observed builtin optimization events after retained-state restart,
while the controller did. This supports the controller-cache repair, not a claim
that all adapter items hit. See `current-frontier-restart-audit.md`.

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
`remote.py` accesses the worker. Worker source is base `59a59d89`, the exact
frontier overlay and the two controller files in `controller-cache-source-inputs.json`.
The accepted pair remains unchanged. Pair building temporarily changes package versions. Its required
systemd ExecStopPost hook restores source only after the build cgroup has drained.
Do not bypass `run-current-frontier-pair.sh` or use incomplete artifacts.

Essential pointers:
- `current-frontier-restart-audit.md` and `.json`, plus locally verified
  `current-frontier-restart-evidence.tar.gz` and `current-frontier-restart-result.log`.
- `controller-cache-enabled-build-driver.log`, `controller-cache-pinning.log`,
  `controller-cache-publication-review.md` and its `-followup.md`. The initial
  build was stopped to honor disabled caching even when native plan storage
  retains a cache handle. The corrected build passed. Candidate artifacts and
  `native-controller-cache-binaries.json` are in `upsert-oom-investigation/`.
- `current-frontier-lifecycle-audit.md` and `.json`, plus
  `current-frontier-lifecycle-result.log`. The locally verified archive is
  `current-frontier-lifecycle-evidence.tar.gz`.
- `current-frontier-ddl-audit.md` and `.json`, plus locally verified
  `current-frontier-ddl-evidence.tar.gz` and `current-frontier-ddl-result.log`.
- `current-frontier-adapter-loss-audit.md` and `.json`, plus the locally verified
  `current-frontier-adapter-loss-evidence.tar.gz`.
- `current-frontier-subscriptions-audit.md` and `.json`, plus locally verified
  `current-frontier-subscriptions-evidence.tar.gz`. An earlier wrapper-only
  version-receipt failure is retained in `current-frontier-subscriptions-driver.log`.
- `current-builtin-cache-startup-survey.md` and `current-cache-build-identities.log`.
  Controllers and adapters have distinct cache namespaces in the pinned pair.
  Controllers discard misses. Same-binary whole-restart misses remain unproven.
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
