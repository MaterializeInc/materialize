# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. Keep the PR draft.

**Next action:** finish the active scheduled-MV acceptance run. The other current
native workflows and repaired catalog-completion fixture passed. The proposed
ALTER SINK overlap-acquisition repair awaits approval. Its read-floor difference
is confirmed by source, but is not yet tied to the CI timeout. Keep startup
optimization proposals separate.

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

- **CI.** [Build 144492](https://buildkite.com/materialize/test/builds/144492)
  ran runtime integration. The registry blocker is resolved. The stale Rust
  expectation reproduced locally and its test-only repair passes, with fresh CI
  still required. A parallel platform check times out waiting for Iceberg sink
  progress before the catalog write. Actual sink lag versus observation failure
  remains unresolved. See `ci-144492/triage.md` and `alter-sink-read-floor.md`
  there. Merge skew remains accepted.
- **Startup proposal.** Current debug cold startup reaches controller HTTP in
  19.81 s and SQL in 48.96 s. Ordinary builtin optimization costs about 15 CPU-s,
  mostly logical optimization. Exact metadata rules out simply enabling replica
  deferral on the controller. Paired cache publication could avoid duplicate
  adapter work, but needs compatibility and startup-order decisions. No such
  change is approved. See `current-controller-startup-outcome.md`.
- **Remaining acceptance:** scheduled-MV suites are active on pinned debug
  artifacts. No build overlaps them. The focused Rust repair retains successful
  creation, zero reallocation and all existing deadlines.
- **Two unexplained events.** A walkthrough run stopped on the resource guard
  and passed on rerun. A probe shows a guard weakness but not the original
  process. The scheduled-MV suite failed once intermittently. A recurrence of
  either needs diagnosis, not a rerun.

## Verified outcomes

Implementation `c1b310c5` passed the default-debug two-version walkthrough,
DDL with combined ingestion, adapter loss, subscription identity and the 60s
four-index workflow. Both versions wrote before and after promotion, with warmed
execution and surviving protection preserved. Retired reopening was rejected.
The pair shares implementation and plan encoding, not arbitrary-release
compatibility. DDL used a stricter resource split, so do not compare its timing
with earlier container runs. Other container runs retained the original caps.
Evidence: `current-native-acceptance/`.

Targeted checks cover fixed-refresh snapshot retention and current privilege
validation, conditional shared maintenance, catalog-derived introspection,
status-history retention and replica reconstruction. Earlier connector handover
runs kept predecessors live and forced reconnects, at reduced input volume.
Detailed outcomes: `fixed-refresh-outcome.md`, `final-cdc-evidence/`,
`replica-startup-comparison.md` and `native-final-bounded-audit.md`.

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

- `/home/ubuntu/materialize-m3-final-head`: clean checkout of `c1b310c5`.
  The profiling overlay is restored and its runtimes and containers removed.
  Current primary binaries are in `current-controller-baseline/artifacts/`.
  The verified version pair is in
  `current-native-acceptance/native-compatible-frontier-current/artifacts/`.
  Use their manifests, not the shared Cargo target, for acceptance.
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
