# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. Keep the PR draft.

**Next action:** collect the decoder control/sibling probes and verify the rebuilt
builtin. Replica startup now returns typed removal.
The follower suite passes after reducing the liveness fixture's debug stack frame
and testing backoff with an ownership change rather than compatible frontier updates.

Temporary cleanup passes both protection modes, including foreign-owner retention
through promotion and explicit retirement. Candidate Persist outputs remain
non-writing by policy. Durable ownership is checked before promotion, and SQL
projections afterward. Both the captured failed diagnostic and successful run are
cleaned up. Their measured output frontiers confirm progress resumes at promotion.

The unchanged `tables.td` reproduces the catalog JSON error. An inline argument
guard passes, but the stored builtin takes an additional optimizer pass. Verify
the rebuilt view before accepting the SQL repair. No decoder relaxation is approved.

## Live failures and constraints

[CI 144427](https://buildkite.com/materialize/test/builds/144427) now executes
runtime jobs and is now terminal with failures. The old image-publicity blocker
is absent. Newly completed failures are being classified. Known failures include:

- Catalog decoding in `tables.td`, replica availability after RF0 to RF1, and
  sink-status history counts. Replica-removal and temporary-cleanup failures have
  locally verified repairs that the completed CI build predates.
- Missing catalog EXPLAIN coverage and an unsupported-upgrade skip that trips the
  CI wrapper's empty-service-log check.
- Merge-skew source conflicts. Merging main remains unauthorized. Other CI jobs
  have finished. Preserve the completed build's evidence when publishing fixes.

Pending user decisions: restore the public sink-status `created` fallback, and
test history retention at its snapshot independently of startup-event publication
rather than impose a new ordering barrier. Neither status repair is implemented.

Do not weaken assertions, raise deadlines or resource limits, change Persist or
the 60s publication default, or add unrelated optimizations. The retry-scheduling
cut remains approved. Positive builtin-schema migrations, pre-native conversion
and Kubernetes/operator/Terraform integration remain deferred. Debug upsert OOM
also reproduces on the pinned upstream base.

The successful versioned walkthrough followed an earlier resource-guard interruption.
A separate metadata probe demonstrates a guard weakness but does not identify the
original PID. The successful diagnostic rerun retained the same assertion. The
historical intermittent scheduled-MV failure has not been proven eliminated. Any
recurrence needs diagnosis, not another rerun.

## Verified outcomes

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
`/home/ubuntu/materialize-m3-controller-followups`, clean at `8ef1fc1b`.
`/home/ubuntu/materialize-m3-fixture-checks` is the separate test worktree with the
verified orchestrator test patch. Preserve its work before reuse. Native debug
artifacts are pinned separately from the shared target, whose clusterd is now the
candidate package version. SLT requires a matched sibling clusterd. Use
`native-slt-tools/`, not an unmatched shared-target pair.

Essential evidence pointers:
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
