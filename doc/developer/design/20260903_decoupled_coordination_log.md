# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Assess remaining architecture
load separately. Do not change Persist or the 60s publication default.

## Next concrete action

Finish verification of the uncommitted native startup boundary. Clusterd now
requires complete catalog arguments on every process, provisioning inputs are
nonoptional, and process zero requires both native endpoints. Parser coverage
checks omitted and partial context on leaders and nonleaders. The reconciliation
fixture also checks generated native arguments for both processes. Formatting
passes, but Rust compilation/tests and runtime verification are not yet run.
Independent static review found no blocking issues. The upsert fixture has finished and cleaned up. Run the targeted Rust checks
on scratch before committing the startup boundary. Public Rust ownership selectors and their attached legacy
branches remain the next removal, not a completed outcome.
The limits launchers now declare inventory before starting every process and
refresh IDs after catalog resets. Generator definitions and counts are unchanged.
Unit checks cover ordering, missing inventory, nonleader context and fresh IDs.
Independent review found no correctness issues. The actual helper passed the
original Tables workload and replica-targeted queries across its seven-process
layout on capped scratch. This does not cover the full limits suite,
authentication setup or rebuilt-image CI. Evidence:
`native-driver-scratch/limits-launcher-evidence/aj-native-cluster-faf2e302/`.

Platform launch migration also passes targeted actual indexed read/write actions
across external replica restart, container restart and managed-replica kills.
Historical launches and the unsupported-upgrade guard remain unchanged. This is
not full platform-scenario acceptance. Evidence:
`native-driver-scratch/platform-action-evidence/aj-native-cluster-dfe8ab86/`.

The upsert load-test launcher migration passes its boundary test and independent
review. The unchanged full input, initial ingestion and reachable rehydration
scenarios passed on scratch. The workflow then failed at the pre-existing
four-element scenario tuple. Approval to split it remains pending. Do not call
this full-workflow acceptance. Source, original SQL payload, unchanged OOM
counters and teardown receipts are preserved in
`native-driver-scratch/upsert-load-evidence/aj-native-cluster-f8933d08/`.
See `native-external-launcher-audit.md`, `native-replica-execution-boundary.md`
and `final-preflight-boundary-research.md` for the remaining boundaries. The current
production-ingress caller audit is `native-ingress-current-audit.md`.

Owned worker `i-064fa517ec1d8bbdb` is available for this next unit, with a one-day
expiry recorded in `native-driver-scratch/worker.json`. Connect through its
instance-specific `native-driver-scratch/remote.py`. No access rules changed.
Completed runtime fixtures, including the upsert load test, are removed.
Keep the compiled candidate and evidence before releasing the worker.

Do not run heavy suites or builds on the local VM. Polling reserves did not prevent
an overload that required a restart. Scratch test containers must share the
`aj-native-tests.slice` hard budget: 40 GiB memory, no swap, eight CPUs. The runner
verifies kernel limits, container membership and aggregate OOM counters. Keep the
2 GiB RAM and 30 GiB disk reserves as additional guards, not the primary limit.
See `native-driver-scratch/scratch-hard-budget-effective.txt` and
`isolation_verify_capped.py`. Local interrupted fixtures were removed with logs
preserved in `native-driver-scratch/local-restart-cleanup/`.


## Verified outcome

Controllerd owns processes, replica followers own maintained execution, and
adapters retain policy, transactions, serving protection and transient queries.
Legacy combined/read-only serving, private migration execution, alternate
protection catalogs and exclusive age-based process cleanup are removed.

Selected native debug checks cover startup/restart, protection, queries,
compatible full writers, promotion and explicit retirement, warmed replicas,
source/sink handover, temporary-object ownership, pruning and table preservation.
Temporary objects retain session visibility and execution ownership. Promotion
preserves them. Disconnect, confirmed replacement and retirement reclaim them.
Format 97 requires fresh experimental state, with no ownership backfill.

Subscription IDs are execution-qualified across public observations, compute
logging and physical-dependency joins. Two-adapter default-dev coverage verifies
colliding local IDs, per-replica and merged progress, independent cleanup,
promotion survival and explicit retirement. Cold native SLT startup and targeted
catalog SQL checks pass with the existing test-harness stack budget on both
hosting contexts. Production stacks, Persist and publication settings are unchanged.
Commits: `8c9e18f0d7` (subscriptions), `917c98e010` (SLT harness).
Evidence and review dispositions: `native-subscription-frontier-assessment.md`.
These checks used mounted candidate binaries, not rebuilt-image CI.

Native renderer and external benchmark launch/restart checks pass in both
Timely topologies, preserving historical-image comparisons. Replica isolation,
connection metrics/death detection, tracing, Kafka resumption, limiter recreation
and original upsert rehydration/failpoint workflows pass with native ownership.
Bounded-memory table hydration and logical backpressure pass at their original
limits, including post-restart checks. Their payloads, budgets and crash coverage
remain intact. These are targeted mounted default-debug candidate outcomes,
not full-suite or rebuilt-image CI acceptance. Evidence directories under
`native-driver-scratch/`: `verified/`, `benchmark-verified/`, `isolation-verified/`,
`tracing-evidence/`, `cluster-launcher-evidence/`, `upsert-launcher-evidence/` and
`bounded-memory-evidence/`. Kafka evidence: `aj-native-kafka-resumption-55360511/`.

The pinned 26.46/26.47 prototype pair shares implementation and plan encoding,
not arbitrary release compatibility. Its lifecycle runs precede the isolated
pgwire stack fix. That fix separately passes initial and resumed subscription
FETCH through both protocols with production worker stacks. The kept workflow
reproduces the old overflow. These are local outcomes, not broad CI acceptance.

## Live blockers and decisions

- Catalog-free production replica ingress remains. The programmable native driver
  replacement is verified. External launchers still need native context and
  refreshed IDs on replica recreation. Preserve historical-image-only paths.
  The dormant final-preflight hook also needs its boundary decision. See
  `native-external-launcher-audit.md` and `final-preflight-boundary-research.md`.
- Forced-migration cases and some platform/stress workflows remain unconverted.
  Preserve source/sink workloads, predecessors and compatible-image coverage.
  Pre-native-to-native upgrades are unsupported, not reset or converted.
- Bounded-memory upsert-index hydration reproducibly OOMs the external replica
  at its unchanged 3.5-GiB limit. Last samples show roughly 3.0–3.1 GiB anonymous
  memory and little file cache. A source audit found no omitted native memory or
  backpressure setting, but no matched successful legacy configuration capture
  exists. Do not attribute causality or raise the limit without evidence.
  Failure receipts: `native-driver-scratch/bounded-memory-evidence/`
  `aj-native-cluster-6ed29d27/` and `aj-native-cluster-c74e55f5/`.
  Fixtures are removed. Cleanup now runs even when resource assertions fail.
- Full upsert stress exceeded the local deployment memory cap. Worker cleanup is
  repaired, but full-stress acceptance remains blocked. Do not shrink the workload
  or raise caps blindly. The full workload still needs a measured retry, not a
  smaller substitute.
- Data-ingest/parallel stress, randomized zippy/restore and bumped-version platform
  runtime acceptance remain incomplete. Selected same-build table preservation
  passed. An earlier index-publication stall and a 673-second source-status query
  remain unexplained. Passing retries do not resolve them. Startup/readiness and
  other unresolved cases remain in the
  [follow-up inventory](20260903_decoupled_coordination_followups.md).

Broad CI is not green. Last reviewed broad evidence is
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image gate. Registry visibility, merge skew, Persist JSON-order
checking and stale hydration expectations remain unresolved. Full bin/fmt fails
on unchanged long lines in `src/catalog/src/catalog.rs` and
`src/catalog/tests/read-write/oracle.rs`. Changed-file checks pass. The PR stays
draft.

## Essential evidence and local state

Evidence root: `~/.local/share/aj/m3-evidence/native-only/`.

- Cursor fix, negative control and review:
  `native-compatible-current/subscription-cursors-verification.md`.
  Latest fixed binaries and manifests: `native-compatible-current/subscription-stack-fixed/`.
- Pinned compatible lifecycle, basic coexistence and inactive-predecessor coverage:
  `native-compatible-current/verification.md`, `basic-verification.md` and
  `offline-candidate-verification.md` in that directory.
- Pruning, selected platform preservation and the blocked stress case:
  `native-compatible-current/forward-pruning-verification.md`,
  `platform-tables-verification.md` and `upsert-verification.md` there.
- Ownership/removal evidence: `native-follower-only-verification.md`,
  `native-temporary-owner-verification.md`, `native-retirement-verification.md`
  and `native-cdc-runtime-verification.md`.
- Detailed earlier evidence pointers are preserved in
  `native-handoff-before-subscription-stack-final.md` and the
  [archive](20260903_decoupled_coordination_log_archive.md). The
  [publication comparison](20260903_decoupled_coordination_publication_interval.md)
  and `../logical-component-load/` are the performance baseline, not a claim about
  cleanup's performance impact.

All completed runtime fixtures are removed. The owned scratch worker is idle. Obsolete 26.47 demo binaries were reclaimed,
with their manifests and evidence retained. Verify the earlier baseline artifact locations before reuse. The identity-fixed production candidate is preserved
locally with matching source and binary manifests. Keep heavy work sequential, enforce 30 GiB disk and 2 GiB
available-memory reserves, and use one low-priority Cargo job. Preserve baseline
artifacts,
third-party caches, the persist-traffic worktree and unrelated
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
