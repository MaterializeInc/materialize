# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. Keep the PR draft.

**Next action:** publish the verified repairs and triage the next CI build,
including the unsupported-upgrade wrapper failure. Measure the controller's
current cold-open stages before choosing a startup change. Run final-head
acceptance once after the remaining repairs, not per change.

## Decisions in force

- Fixed-refresh revalidation keeps the chosen timestamp and protection. It
  rechecks dependencies, the target namespace and CREATE/USAGE privileges. It
  borrows the approach of upstream main's DDL command framework without a
  rebase, because main does not cover CREATE MATERIALIZED VIEW yet. See
  `upstream-ddl-command-survey.md`.
- Merging main is not authorized. Keep pinned upstream
  `d9a2c5dc402fd9fa359607305669d05f27215cc4`.
- The pre-native upgrade-smoke job is marked unsupported. List it with its
  reason in the PR description.
- Deferred: statistics and compute introspection ownership, positive
  builtin-schema migrations, pre-native conversion, and
  Kubernetes/operator/Terraform integration. The retry-scheduling cut stays
  approved. See the [follow-ups](20260903_decoupled_coordination_followups.md).
- Do not weaken assertions, raise deadlines or runtime limits, change Persist or
  the 60s publication default, or add unrelated optimizations.

## Outstanding work

- **CI.** [Build 144427](https://buildkite.com/materialize/test/builds/144427)
  ran runtime jobs and failed. It predates the local repairs listed under
  verified outcomes. Still failing without a repair: the unsupported-upgrade
  skip trips the wrapper's empty-service-log check, and the merge check fails
  on conflicts with main. Inventory: `ci-144427/terminal-failures-research.md`.
- **Controller cold startup.** About 18 s of the roughly 20 s is builtin view
  reconstruction, from older captures. Re-measure on current code before
  choosing a change. The replica metadata shortcut cannot supply exact published
  column metadata. Adapter-cache reuse alone cannot remove empty-cache startup
  work. See `controller-cold-start-attribution.md` and
  `controller-cold-start-options.md`. No cache-contract change is approved.
- **Final-head acceptance** after the above: lifecycle, DDL and combined
  ingestion, adapter loss, subscriptions, 60s four-index, scheduled-MV suites
  and the version-pair walkthrough.
- **Two unexplained events.** A walkthrough run stopped on the resource guard
  and passed on rerun. A probe shows a guard weakness but not the original
  process. The scheduled-MV suite failed once intermittently. A recurrence of
  either needs diagnosis, not a rerun.

## Verified outcomes

Runtime implementation `8ef1fc1b` passed the distinct-package-version
walkthrough, scheduled-MV suites, DDL with combined ingestion, adapter loss,
subscription identity and the 60s four-index workflow. PostgreSQL, MySQL and
SQL Server handover passed with live predecessors and forced reconnects, at
reduced input volume. The version pair shares source and plan encoding. It is
not release-skew coverage. Outcomes and limits:
`final-native-{lifecycle,ddl,adapter-loss,subscriptions,four-index}-outcome.md`
and `final-cdc-evidence/`.

Committed since that implementation, with targeted checks:

- Fixed-refresh creation retains its snapshot across unrelated DDL and rejects
  changed dependencies, namespaces and privileges. The two-adapter regression
  passes with the repair and fails without it. See `fixed-refresh-outcome.md`.
- Sink status reports `created` when no observation applies.
- Status-history coverage is split: exactly the latest five rows survive a
  prune, and a restart delivers `starting` and `running`. No production change.
- Shard-lookup decoding keeps its guard in the stored view. `tables.td` passes.
- Replica removal before startup registration returns the typed error.
- Catalog, cluster and SRF expectations match the native builtin inventory.
- The unreleased v97 catalog snapshot is current. Older snapshots are untouched.
- Dependency, refresh and shard rows derive from durable state. Startup pruning
  and storage-usage collection write conditionally.
- Replicas defer builtin view optimization outside required schema closures.
  Matched debug runs: replica startup CPU 91.5 to 43.8 CPU-seconds, fresh
  serving 54.6 to 50.5 s. See `replica-startup-comparison.md`.

Candidate Persist outputs stay non-writing until promotion. Temporary ownership
is checked durably before promotion and through SQL afterward. Debug upsert OOM
also reproduces on the pinned base.

## Machines and evidence

Resource rules are in the prompt. Builds on scratch use every vCPU. The editing
VM is available for compiles, targeted tests and the full `bin/controllerd`
setup. Runtime limits stay fixed: container runs 40 GiB and eight CPUs, direct
native runs 28 GiB and six CPUs plus 8 GiB and two CPUs for fixtures. Never
overlap builds and runtimes. Keep a 30 GiB disk reserve.

Worker `i-04aaff9ac16f9c30c`: 16 vCPUs, 61 GiB RAM, 500 GiB disk. Deletion is
scheduled for 2026-11-23 19:02:53 UTC. Recheck access and extend it.
`remote.py` in the evidence root accesses the worker.

Scratch worktrees:

- `/home/ubuntu/materialize-m3-controller-followups`: main worktree, based on
  `8ef1fc1b` with recorded runtime source overlays.
- `/home/ubuntu/materialize-m3-fixture-checks`: test worktree with the decoder,
  follower and test patches. Preserve its work before reuse.

The fixed-refresh overlay is verified in the fixture worktree. Source and binary
receipts are preserved off-worker in `mv-final-receipts/`. Do not replay old
transfer packets. Audit the idle worker before replacing its recorded overlay.

Verified debug binaries are in `native-decoder-json-guard/artifacts/`, with
provenance beside them. Its `testdrive-helper/` holds the matching testdrive
and `libduckdb.so`, which needs its library path set. Cached testdrive v26.43
cannot read v26.46 Persist state. Do not mix sibling binaries.

Evidence root: `~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.
The previous, longer handoff with every log pointer is
`handoff-before-cleanup-2026-11-23.md` there. Key entries:

- `ci-144427/`: annotations, job logs and the failure inventory.
- `final-acceptance/`: bounded drivers, decoder probes and the
  temporary-cleanup overlay.
- `status-history-diagnosis.md`, `storage-shards-decoder-investigation.md`,
  `liveness-fixture-outcome.md`, `temporary-cleanup-outcome.md`: diagnoses
  behind the committed repairs.
- `replica-metadata-verification.md`, `replica-boundary-independent-review.md`:
  replica startup change.
- `native-final-bounded-audit.md`, `remaining-inline-owner-inventory.md`: scope
  and retained work.

Preserve the unrelated temporary TOML file in the editing worktree.
