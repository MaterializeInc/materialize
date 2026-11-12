# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Assess remaining architecture
load separately. Do not change Persist or the 60s publication default.

## Next concrete action

Convert replica-isolation next. Start every process from committed inventory,
refresh identity after drop/recreate, and replace controller-log compaction checks
with baseline-and-advance assertions on actual replica read frontiers and Persist
since. Preserve the disruption matrix and check its expectations against native
protection and retention. See `native-replica-isolation-boundary.md`.

Production ingress removal still needs the remaining external launchers and
multi-process nonleader coverage in both Timely topologies. The launcher inventory
is in `native-external-launcher-audit.md` and `native-replica-execution-boundary.md`.

Owned worker `i-064fa517ec1d8bbdb` is available for this next unit, with a one-day
expiry recorded in `native-driver-scratch/worker.json`. Connect through its
instance-specific `native-driver-scratch/remote.py`. No access rules changed.
Keep the compiled candidate and evidence before releasing the worker.

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

Native programmable renderer scripts preserve dynamic plans and within-owner
mixed exports. Their default-build tests and PostgreSQL/MinIO composition pass
in both Timely topologies. External benchmark launch/restart checks also pass in
both topologies for native and historical v26.30 environments, preserving table
contents and continued writes. Native checks use retained default-debug binaries.
This covers actual launcher/executor methods, not the full benchmark workflow,
multi-process ingress or rebuilt-image CI. Fixtures are removed. Evidence:
`native-driver-scratch/verified/` and `native-driver-scratch/benchmark-verified/`.

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

All owned fixtures are removed. Obsolete 26.47 demo binaries were reclaimed,
with their manifests and evidence retained. The stack-fixed frontier-bug baseline
and upstream baselines remain. The identity-fixed production candidate is preserved
locally with matching source and binary manifests. Keep heavy work sequential, enforce 30 GiB disk and 2 GiB
available-memory reserves, and use one low-priority Cargo job. Preserve baseline
artifacts,
third-party caches, the persist-traffic worktree and unrelated
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
