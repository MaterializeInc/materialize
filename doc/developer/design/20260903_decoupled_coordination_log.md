# Decoupled coordination: current handoff

## Objective and next action

Finish native-only M3 under the [prompt](20260903_decoupled_coordination_prompt.md).
Preserve compatible full writers, explicit retirement, durable protection,
adapter-owned policy, shared query transport and required formats. Keep the PR draft.

**Next action:** resolve the private pubsubd image path, then rerun relevant CI.
It cannot be made public for this prototype. Preserve public-image checks for
other images and native runtime coverage. The scoped ALTER SINK repair now has
a failing-before, passing-after Iceberg witness. Its role in the original broad
CI timeout remains unverified. Keep startup optimization proposals separate.

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

- **CI.** [Build 144502](https://buildkite.com/materialize/test/builds/144502)
  again reports pubsubd as non-public on Docker Hub. Build 144492 ran runtime
  integration but skipped the normal x86 image-build step. That did not prove
  registry provisioning was resolved. A scoped private-image path is still
  required. The stale Rust expectation and ALTER read-floor repair are verified
  locally, with fresh CI still required. See `ci-144492/triage.md`,
  `alter-sink-overlap/outcome.md` and `ci-144502/`. Merge skew remains accepted.
- **Startup proposal.** Current debug cold startup reaches controller HTTP in
  19.81 s and SQL in 48.96 s. Ordinary builtin optimization costs about 15 CPU-s,
  mostly logical optimization. Exact metadata rules out simply enabling replica
  deferral on the controller. Paired cache publication could avoid duplicate
  adapter work, but needs compatibility and startup-order decisions. No such
  change is approved. See `current-controller-startup-outcome.md`.
- **Two unexplained events.** A walkthrough run stopped on the resource guard
  and passed on rerun. A probe shows a guard weakness but not the original
  process. The scheduled-MV suite failed once intermittently. A recurrence of
  either needs diagnosis, not a rerun.

## Verified outcomes

ALTER SINK acquires earlier protected input history through existing durable
admission rather than reusing a later adapter grant. Ordinary SQL acquisition
is unchanged. The debug Iceberg witness timed out at 300s before the repair and
completed in 0.238s afterward, with external rows verified. The unchanged
commit-interval check passed before and after. Targeted protection tests and
independent review cover admission and races. See `alter-sink-overlap/outcome.md`
for the exact witness, limits, receipts and failed harness attempts.

Implementation `c1b310c5` passed the default-debug two-version walkthrough,
DDL with combined ingestion, adapter loss, subscription identity, the 60s
four-index workflow and scheduled/replica-targeted MV suites. Both versions wrote before and after promotion, with warmed
execution and surviving protection preserved. Retired reopening was rejected.
The pair shares implementation and plan encoding, not arbitrary-release
compatibility. DDL used a stricter resource split, so do not compare its timing
with earlier container runs. Other container runs retained the original caps.
Evidence: `current-native-acceptance/`. Its `catalog-completion-outcome.md`
records the test-only repair's reproduced failure, passing result and checks.
The worker is idle, with a clean source checkout and no native processes or
running containers. Receipts are preserved locally.

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
  The scoped ALTER SINK binaries are in `alter-sink-overlap/artifacts-after/`,
  with separate source and binary receipts. They have targeted coverage, not a
  repeat of the full acceptance suite.
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
