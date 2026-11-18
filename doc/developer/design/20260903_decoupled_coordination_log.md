# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. M2 and the
60s publication default remain closed. Do not restore legacy execution or weaken
validation to make checks pass.

Next, finish conditional-write convergence: controller configuration publication,
remaining startup retry wrappers, and inline DDL owners. Recheck
`~/.local/share/aj/m3-evidence/multi-writer-retry-survey.md` and
`native-only/native-ci-recovery/current-remaining-retry-sites.md` against current
source. The adapter configuration twin was unreachable and is now removed, not a
remaining retry target. Verify controller tick continuation under activation
contention. Then complete startup phase attribution and current-head acceptance.
Bring changes to write guarantees or public behavior to the user before landing.
Startup attribution is running alongside removal, not behind it: the instrumented
pre-tick-repair debug build is `aj-startup-timing-before-build`. Use the focused
`native_startup_readiness_20261027_aj.py` harness, not full acceptance.

## Live decisions and boundaries

- Approved: move restart-time configuration publication into the existing runtime
  tick, retaining admission checks before serving and fresh initialization before
  children. Preserve SQL-readiness configuration guarantees and tick continuation.
- Approved: reclaim expression-cache namespaces for release versions unused by
  live deployments. Keep dev-build variants of live releases and never reclaim
  authoritative Written plans. Implementation remains outstanding.
- Keep the current sink selection rule. Do not add durable placement records.
  The difference from upstream and dev-build cache accumulation are recorded in
  the [follow-ups](20260903_decoupled_coordination_followups.md#approved-m3-scope-limits).
- Run full lifecycle, DDL and adapter-loss acceptance once on the final head after
  removal and retry work, not per cleanup. That final run remains outstanding.
- Startup timing work proceeds alongside cleanup: attribute the 20s gap between
  readiness checks and report before/after per-phase restart and promotion timing.
- Promoted-adapter storage initialization can return `CatalogOutOfSync` after
  structural catch-up. Revalidate preparation rather than removing the guard.
- Debug upsert OOM also reproduces on pinned upstream. No native-specific cause
  is established. Do not increase memory or shrink the workload.
- Broad current-head CI and combined ingestion coverage remain open. The last
  inspected [CI build](https://buildkite.com/materialize/test/builds/143929), at
  `6e7a3107`, passed Linux/macOS Clippy, formatting, docs and doctests. Image builds
  were blocked by public `materialize/pubsubd` visibility, requiring team-testing.
  Merge-skew failed before compilation. Dependent runtime jobs did not run.
  See `current-ci-143929.md`. Do not merge main without approval.
- Positive builtin migrations, pre-native conversion, Kubernetes, operator and
  Terraform integration remain deferred. Preserve rejection guards and historical
  coverage. No Persist redesign, cadence changes or unrelated optimization.

## Verified outcomes and limits

Native ownership, temporary/subscription identities, protection derivation,
compatible full writers and explicit retirement have targeted debug lifecycle
coverage. The accepted 26.46/26.47 pair shares implementation and plan encoding.
It is not arbitrary-release or migration coverage, and predates recent cleanup.
Optimized KafkaSources completed on native and pinned upstream with final
validation and backup/restore. The debug four-index 60s workflow passed. The 1s
variant remains stress, not acceptance.

Recent checks cover controller cache publication, configuration exhaustion,
retirement preparation and natural controller exit. Same-build live retirement
exited zero before cleanup, rejected retired reopening, and preserved survivor
SQL, held snapshots and warmed execution. Historical version-pair and Kafka logs
still contain the old outgoing-controller panic. They have not been rerun with
the repair. Retired reopening remains a fatal startup rejection.

Group commits prepare batches before allocation and reuse them across conflicts.
Runtime registration likewise retains adopted handles, freshly certifies catalog
bindings and handles finalized targets. Real WAL/certificate races, snapshot/listen
assertions, negative controls, targeted Clippy and independent review passed.
The obsolete adapter configuration publisher, system-table resets, extra reset WAL
reader and non-native bootstrap/writer alternatives are removed. Unchanged
configuration startup/restart, adapter restart preservation and CREATE/ALTER
infrastructure recovery checks pass. Serving now requires the bootstrap-created
client, with no alternative registration. Bootstrap plan/publication helpers
also require that client. Independent review, Clippy, native
configuration/restart and independent-oracle bootstrap checks pass. No dedicated
supersession race was added. Persisted-object EXPLAIN now has only the native
selected-plan path. Clippy and unchanged materialized-view/physical-plan SQL
suites pass. No new join/replacement EXPLAIN race coverage is claimed.
The monolithic cluster-policy write alternative is removed. Clippy and unchanged
managed/unified cluster SQL suites pass. Fresh multi-deployment policy/promotion
coverage remains outstanding. No performance improvement is claimed.

ALTER DEFAULT PRIVILEGES, REASSIGN OWNED and ALTER OWNER use the existing staged commit
owner instead of inline retries. Independent review, Clippy and unchanged
`default_privileges.slt`/`object_ownership.slt` pass with standard repository SLT
parameters. The initial direct invocation omitted those parameters and failed at
load-generator feature gates, not assertions. No forced-conflict/cancellation
coverage for these dispatches is claimed. Other inline DDL owners remain.

Optimizer statistics now require the native query client in both peek paths.
Clippy, enabled-statistics SQL in both paths (including observed timeout fallback)
and unchanged plan-insights coverage pass. Initial fixture failures were missing
feature admission and transaction-block ALTER SYSTEM, reproduced before repair.
The preexisting cardinality suite remains halted and is not counted as coverage.

Candidate promotion in the latest lifecycle run waited on the existing 600000ms
stability gate after catching up. Do not classify that wait as unexplained replay
or CPU work. The earlier statistics replay audit separately found zero-holding
leases plus a 30-day critical retention bound. No retention change was made.
The post-cleanup resource comparison still found more native CPU, memory and I/O.
Attribution and proposals are in the [follow-ups](20260903_decoupled_coordination_followups.md#native-only-resource-comparison),
not implemented optimizations.

Detailed historical outcomes and caveats are preserved in the
[previous handoff](https://github.com/aljoscha/materialize/blob/01ceb94c/doc/developer/design/20260903_decoupled_coordination_log.md).

## Worker and evidence

Worker `i-02f17d8a7d9534f5e`: 16 CPUs, 61 GiB RAM. Shutdown 2026-11-18 16:00 UTC,
tag expiry 16:05. All owned runtimes are stopped. Builds use one job, four CPUs,
32 GiB, no swap and a 30 GiB disk reserve. Never overlap builds and runtimes.
Host lifecycle/tests use a 28 GiB/6-CPU user slice plus an 8 GiB/2-CPU root slice.
Docker workload caps remain eight CPUs/40 GiB aggregate and four CPUs/6 GiB per
deployment. Extend the worker within these caps without asking when needed.

Local evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses scratch. Current verification worktree is
`/home/ubuntu/materialize-m3-controller-followups`, base `17da75c2` plus explicit
source overlays. Verify hashes, not just its Git revision. Remote logs are under
`~/.local/share/aj/m3-evidence/controller-followups/`.

Essential pointers:
- Query statistics: local `native-query-statistics-fixed-test.log`; earlier
  failures are in `native-query-statistics-reproduce.log`.
- Cluster policy: local `native-cluster-policy-test.log`, including the build
  result and standard-parameter SQL run.
- Persisted EXPLAIN: local `native-selected-explain-test.log`, including the
  build result and standard-parameter SQL run.
- Client adoption: local `mandatory-bootstrap-client-test.log` and remote
  `mandatory-bootstrap-client-recheck.log`. The mandatory helper follow-up is
  covered by local `native-bootstrap-protection-test.log`.
- Current retry checks: remote `registration-certificate-red.log`,
  `registration-finalization-red.log`, `append-wal-final.log`,
  `controller-unit.log`, `retirement-red.log`, `cache-red.log` and
  `native-table-writer-only.log` and `staged-owner-ddl-build.log`.
  Local `staged-owner-ddl-defaults-test.log` records the complete SQL suite pass,
  with the applied parameter receipt in remote `staged-owner-ddl-parameters.txt`.
  `staged-alter-owner-test.log` covers ownership cascades and index notices.
- Recent runtime cleanup: local `native-clean-exit-audit.md`,
  `native-clean-exit-evidence.tar.gz`, `adapter-config-removal-test.log`,
  `bootstrap-reset-test.log` and `native-bootstrap-only-test.log`.
- Accepted pair and lifecycle: `current-frontier-{lifecycle,ddl,adapter-loss,
  subscriptions,restart}-audit.md` and their evidence archives. Preserve accepted
  artifacts under `upsert-oom-investigation/native-compatible-frontier-current/`.
  Pair rebuilding changes versions and requires the reviewed restoration hook in
  `run-current-frontier-pair.sh`. Do not bypass it or use incomplete artifacts.
- Latest same-build debug artifacts: scratch
  `controller-followups/native-debug-inputs/`, with source and binary provenance.
  They predate runtime-registration and subsequent bootstrap removals.
- Performance/acceptance: `optimized-kafka-comparison.md`,
  `current-four-index-60s-audit.md`, `source-statistics-physical-since.md`,
  `controller-cache-comparison-audit.md`, `current-builtin-cache-startup-survey.md`
  and `native-only-load-report.md`.

Preserve `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
