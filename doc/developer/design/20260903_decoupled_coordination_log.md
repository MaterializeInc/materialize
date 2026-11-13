# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Assess remaining architecture
load separately. Do not change Persist or the 60s publication default.

## Next concrete action

Prepare a current default-development runtime candidate and verify native
lifecycle/progress across real processes, including split/unified topology and
query churn. Existing mounted binaries predate the latest ownership removals.
Keep heavy work sequential on capped scratch. Pending deployment/upgrade scope
decisions below remain explicit, not implicit test deferrals.

Storage-local restart, sink reconstruction and async drop alternatives are
removed. Maintained commands require execution identities. Ingestion recovery,
replacement fencing, query isolation, lifetime and shared-ordering tests pass.
Independent review found no concrete issue. Catalog-backed sink tests initially
overflowed the default test stack, reproduced exactly, then all passed with the
repository's existing 8-MiB test-stack setting. Production stacks and memory caps
are unchanged. Final all-target checks pass without warnings after import-only
cleanup. These are targeted outcomes, not full native-only acceptance.

Evidence under `native-driver-scratch/`: `native-storage-restart-check.log`,
`native-storage-restart-source-sha256.json`, `native-sink-reproduce.log`,
`native-sink-standard-stack.log`, `native-storage-restart-final-check.log` and
`native-storage-restart-final-source-sha256.json`. Audit:
`native-storage-restart-drop-audit.md`.

Cluster profiling now declares native replica inventory and retains its launch
override. Managed reconfiguration no longer starts an unused external process
(`7231bb96`). Launcher boundary tests and Black/Ruff pass. AST comparison confirms
all profiling requests/assertions and managed reconfiguration assertions are
unchanged. Actual profiling HTTP acceptance on rebuilt binaries is not verified.

Recent completed removals cover mandatory native startup (`c27eabf8`), compute
transport lifecycle (`b3a88e96`), and storage lifecycle/query-only role clients
(`f6ad2178`, fixtures `0679833f`). Targeted all-target and native runtime checks
pass. Exact logs and source manifests live under `native-driver-scratch/` with
prefixes `native-ingress-`, `native-compute-final-` and `native-storage-final-`.
These are not rebuilt-image CI or full native-only acceptance.

Limits, platform restart actions and upsert load-test launchers now establish
native inventory before process startup. Targeted runtime evidence is under
`native-driver-scratch/{limits-launcher-evidence,platform-action-evidence,upsert-load-evidence}/`.
The full upsert load-test remains incomplete because of its malformed scenario
tuple. Current remaining harness inventory: `native-remaining-launchers-current.md`.

Owned worker `i-064fa517ec1d8bbdb` is available for this next unit, with a one-day
expiry recorded in `native-driver-scratch/worker.json`. Connect through its
instance-specific `native-driver-scratch/remote.py`. No access rules changed.
Completed runtime fixtures, including the upsert load test, are removed.
Keep the compiled candidate and evidence before releasing the worker.
Unused local Docker images were pruned. Local disk headroom is 39 GiB. Volumes
and retained candidate binaries were preserved.

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

- Asked whether to explicitly defer positive forced builtin-migration coverage
  for M3 and mark its two nightly workflows unsupported while keeping rejection
  and compatible-promotion coverage. These are current-build workflows, not
  pre-native upgrades. Native startup rejects their force flags. Do not remove
  guards or change them into ordinary promotion tests to claim preserved coverage.
  Evidence and native continuity assertion options:
  `native-builtin-migration-assertions.md`. Approval remains pending.
- Catalog-free production ingress and the two remaining cluster launches are
  repaired. Current-build cloudtest, operator and Terraform deployment harnesses
  still target monolithic environmentd. Asked whether to defer these native
  deployment families explicitly for M3. Do not disable them without approval.
  Current inventory: `native-remaining-launchers-current.md`. Preserve
  historical-image-only paths.
  The dormant final-preflight hook also needs its boundary decision. See
  `native-external-launcher-audit.md` and `final-preflight-boundary-research.md`.
- The manual upsert load-test has a four-element tuple unpacked as two values.
  Approval to split it into the intended two scenarios remains pending. Native
  launch, original input and reachable rehydration passed, not the full workflow.
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

All completed runtime fixtures are removed. See the current action for active
scratch validation. Obsolete 26.47 demo binaries were reclaimed,
with their manifests and evidence retained. Verify the earlier baseline artifact locations before reuse. The identity-fixed production candidate is preserved
locally with matching source and binary manifests. Keep heavy work sequential, enforce 30 GiB disk and 2 GiB
available-memory reserves, and use one low-priority Cargo job. Preserve baseline
artifacts,
third-party caches, the persist-traffic worktree and unrelated
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
