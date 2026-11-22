# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. Keep the PR draft.

**Next action:** settle fixed-refresh privilege revalidation and verify that repair.
Meanwhile, attribute the controller's cold startup gap. Sink fallback, the history
coverage split and SRF expectations are committed with targeted verification.
Run final-head acceptance after the remaining repairs, not per change.

Catalog decoding, plan coverage, native catalog fixtures, temporary cleanup and
RF convergence coverage have local verification. CI remains red on the older
pushed head. Detailed outcomes and limitations are linked below.

## Live failures and constraints

[CI 144427](https://buildkite.com/materialize/test/builds/144427) now executes
runtime jobs and is now terminal with failures. The old image-publicity blocker
is absent. Newly completed failures are being classified. Known failures include:

- Fixed-refresh concurrency verification remains open. Sink fallback passes its
  red/green boundary test. Strict retention and restart-event coverage both pass. The imported Cockroach
  SRF fixture passes with its two approved expected rows and unchanged SQL.
  Catalog decoding, EXPLAIN coverage, replica removal, temporary cleanup and RF
  coverage also have locally verified repairs that this CI build predates.
- An unsupported-upgrade skip trips the CI wrapper's empty-service-log check.
- Merge-skew source conflicts. Merging main remains unauthorized. Other CI jobs
  have finished. Preserve the completed build's evidence when publishing fixes.

The four scoped repairs are approved. One new decision remains: recheck CREATE
and USAGE privileges after fixed-refresh catalog catch-up, rather than allowing
concurrent revocation to pass the narrower dependency check. Recommendation: recheck.
The implementation remains uncommitted. Review required target-namespace validation
and stale-candidate rejection, now added. The post-acquisition revision check lives
at the shared CREATE/EXPLAIN boundary. Runtime verification has not passed: the
fixture lacked the RETAIN HISTORY feature flag, now fixed locally. It now uses
AT CREATION so changed input contents detect accidental timestamp resampling.

Strict latest-five retention passes through the real pruning API. The complete
revised restart workflow independently verifies new starting/running events for
the source, table and sink. No ordering barrier, count relaxation or production
retention change was added. Evidence: `status-history-repaired-evidence.tar.gz`
(project `aj-native-parallel-c09cb31b`) and `sink-status-red-green.log`.

Current upstream main's command framework rechecks authorization but does not yet
cover CREATE MATERIALIZED VIEW. A rebase is not recommended solely for this repair.
No rebase or cherry-pick was performed. See `upstream-ddl-command-survey.md`.

Do not weaken assertions, raise deadlines or resource limits, change Persist or
the 60s publication default, or add unrelated optimizations. The retry-scheduling
cut remains approved. Positive builtin-schema migrations, pre-native conversion
and Kubernetes/operator/Terraform integration remain deferred. Debug upsert OOM
also reproduces on the pinned upstream base.

Candidate Persist outputs remain non-writing until promotion. Temporary ownership
is checked durably before promotion and through SQL projections afterward.

The successful versioned walkthrough followed an earlier resource-guard interruption.
A separate metadata probe demonstrates a guard weakness but does not identify the
original PID. The successful diagnostic rerun retained the same assertion. The
historical intermittent scheduled-MV failure has not been proven eliminated. Any
recurrence needs diagnosis, not another rerun.

## Verified outcomes

The history failure is diagnosed: a captured five-row retention result includes
both new restart transitions and three older rows per key. A separate run retained
five old rows plus the new pair. The fixture assumes a startup ordering that
native publishers do not promise. No production change or count relaxation was
made. See `status-history-diagnosis.md` and its row-level proof.

A private debug probe deterministically rejects a fixed-refresh CREATE after
unrelated CREATE SECRET commits during optimization completion. Both uncontended
controls succeed. The finish-stage planning-revision guard returns 40001, not a
timeout. This identifies a current path, not CI's exact conflicting transaction.
The uncommitted repair revalidates unchanged dependencies without repurifying the
fixed timestamp. See `fixed-refresh-diagnostic.md` for the original reproduction.

Typed replica removal, follower liveness, foreign temporary-owner cleanup and
native catalog fixtures pass targeted checks. The JSON-kind decoder guard passes
unchanged `tables.td`, including tombstone validation and consistency checks.
Generated catalog plans pass verification. The unreleased v97 snapshot is current,
with older snapshots untouched. RF coverage polls membership and requires one
successful read, without strengthening ALTER acknowledgment. Both testdrive and
the remaining unified-cluster SLT pass. Targeted Rust checks now also pass for
shared plan namespaces, rewrite conflicts/cancellation, pending-index isolation,
webhook metrics, the native CLI and hydration-history isolation. Complete
`catalog.td`, `cluster.slt` and `cockroach/srfs.slt` pass with native builtin
inventory expectations. SRF verification is in `srfs-current-runtime-fixed-runner.log`.
Targeted repair unit results are in `approved-native-repair-unit-recheck.log` and
`approved-native-repair-validation-checks.log`.

Runtime implementation `8ef1fc1b` passes the distinct-package-version walkthrough,
unchanged scheduled-MV suites, full DDL/combined ingestion, adapter loss,
subscription identity and 60s four-index workflows. Independent evidence reviews
confirm their assertions and document limits. The version pair shares source and
plan encoding, not arbitrary release compatibility. PG/MySQL/SQL Server handover
checks pass with live predecessors and forced reconnects. PG/MySQL use explicitly
reduced input volume, not their default-volume performance workloads.

Replica reconstruction defers builtin optimization outside required schema closures,
preserving optimizer features and exact schemas. Matched debug startup CPU fell
from 91.47 to 43.75 CPU-seconds, fresh serving from 54.63 to 50.51 seconds, with no RAM
improvement established. These are same-source startup comparisons, not a final
upstream steady-state benchmark. The source audit found no further reachable
monolithic alternative in its inspected boundaries. Statistics and compute
introspection ownership remain deferred.

Shared dependency, refresh and shard rows derive from durable state. Conditional
maintenance and bounded retry repairs preserve protection and validation. The
image-replacement test's hidden task panics were repaired using its required
multithread runtime, with unchanged assertions and passing tests/Clippy.

## Worker and evidence

Worker `i-04aaff9ac16f9c30c`: 16 CPUs, 61 GiB RAM, 500 GiB disk. Automatic shutdown:
2026-11-22 18:02:13 UTC. Deletion: 2026-11-23 19:02:53 UTC. Recheck access and expiry.
Builds use one job, 4 CPUs, 32 GiB, no swap. Container runs use 40 GiB/eight CPUs
aggregate. Direct native runs use 28 GiB/six CPUs plus 8 GiB/two CPUs for fixtures.
Never overlap builds and runtimes. Keep 30 GiB disk reserve and local builds light.

Evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
`remote.py` accesses the worker. Main scratch worktree:
`/home/ubuntu/materialize-m3-controller-followups`, based on `8ef1fc1b` with
recorded runtime source overlays.
`/home/ubuntu/materialize-m3-fixture-checks` is the separate test worktree with the
recorded decoder, follower and test patches. Preserve its work before reuse.
`native-decoder-json-guard/artifacts/` contains the verified debug binaries and
matched SLT/clusterd pair, with build provenance beside it. Its `testdrive-helper/`
contains matching testdrive and `libduckdb.so`. Set its library path when mounting
that helper. Cached testdrive v26.43 cannot read the v26.46 Persist state. Do not
mix sibling binaries. `native-decoder-current/` is the earlier failing control.

Essential evidence pointers:
- `decoder-matched-testdrive-final.log`, `decoder-plan-checks.log`, and
  `storage-shards-decoder-investigation.md`: complete guarded-view verification,
  generated plan checks and stored-view controls. `native-catalog-fixture-checks.log`
  and `v97-stability-{before,after}.log` cover the catalog fixtures and encodings.
- `remaining-native-fixture-build-retry.log` and
  `remaining-native-fixture-runtime.log`: compiled fixtures and targeted Rust
  tests. The runtime used isolated metadata and the existing aggregate caps.
- `native-catalog-inventory-counts-check.log` and `native-cluster-slt-check.log`:
  complete catalog and cluster checks. `native-catalog-inventory-evidence.tar.gz`
  preserves the catalog runs, including the earlier inventory failures.
- `native-unified-{rf,slt}-check.log`: complete RF testdrive and remaining SLT
  coverage. `catalog-decoder-and-rf-final.tar.gz` preserves runtime evidence.
- `ci-144427/`: current annotations, job logs and failure inventory. Older
  `ci-144357-outcome.md` does not describe current CI blockers.
- `final-native-{lifecycle,ddl,adapter-loss,subscriptions,four-index}-outcome.md`:
  independently reviewed runtime outcomes, archive paths and coverage limits.
- `final-cdc-evidence/`: completed live-reader handover/reconnect captures.
- `tables-repro-run.log`: reproduced unchanged CI failure. `final-acceptance/`
  contains private bounded drivers, including decoder probes and the exact
  helper-only temporary-cleanup overlay.
- `replica-removal-before-bounded.log`, `replica-removal-after.log`, and
  `liveness-fixture-outcome.md`: typed-removal red/green evidence, measured stack
  repair and passing follower coverage.
- `ci-144427/terminal-failures-research.md`: final failure inventory, including
  additional Cargo fixtures and a fixed-refresh CREATE invalidation that needs
  diagnosis before reopening the deferred retry scope.
- `stall-readonly-report.md`: pre-promotion inventory stall and its measured
  frontiers. `temporary-cleanup-outcome.md` records the corrected passing fixture,
  archive, provenance and cleanup evidence.
- `ddl-version-observation-boundary.md`: private observer repair that retains the
  workflow's existing readiness wait and SQL-version assertions.
- `replica-boundary-independent-review.md`, `replica-metadata-verification.md` and
  `replica-startup-comparison.md`: implementation review and startup evidence.
- `native-final-bounded-audit.md`, `remaining-inline-owner-inventory.md` and the
  [follow-ups](20260903_decoupled_coordination_followups.md): scope and retained work.

After CI repairs and targeted verification, clean owned runtimes/resources, assess
remaining architecture-generated load separately, and consolidate own fixups without
rewriting others' work. Keep pinned upstream `d9a2c5dc402fd9fa359607305669d05f27215cc4`.
Preserve the unrelated temporary TOML file in the editing worktree.
