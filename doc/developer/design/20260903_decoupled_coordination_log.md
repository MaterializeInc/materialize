# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Assess remaining architecture
load separately. Do not change Persist or the 60s publication default.

## Next concrete action

Repair the confirmed subscription-frontier gap through query-owned publication
and its SQL dependencies without restoring the adapter publisher for
maintained objects. Preserve per-replica frontier rows. Public transient IDs
collide across adapters. Owner-qualified public subscription IDs await a UX
decision. See `native-subscription-frontier-assessment.md` and
`native-progress-dependency-audit.md`.

The SQL reproduction is complete: a live cursor delivers data, but global and
per-replica subscription frontiers are absent while maintained index frontiers
advance. See `native-compatible-current/subscription-frontier-reproduction.md`.
The default-debug FETCH crash is fixed and verified. No runtimes are running.

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

The pinned 26.46/26.47 prototype pair shares implementation and plan encoding,
not arbitrary release compatibility. Its lifecycle runs precede the isolated
pgwire stack fix. That fix separately passes initial and resumed subscription
FETCH through both protocols with production worker stacks. The kept workflow
reproduces the old overflow. These are local outcomes, not broad CI acceptance.

## Live blockers and decisions

- Subscription frontier publication is missing in the runtime reproduction.
  Its public-ID decision is above. Keep missing observations distinct from
  completed frontiers and preserve surviving deployments' queries.
- Catalog-free replica ingress remains. Replacing headless programmable coverage
  with a test-only native runtime needs scope approval. Preserve shared query
  transport, native nonleader workers and both Timely topologies. The dormant
  final-preflight hook's old-to-old coverage also needs a decision. See
  `native-replica-execution-boundary.md` and `final-preflight-boundary-research.md`.
- Forced-migration cases and some platform/stress workflows remain unconverted.
  Preserve source/sink workloads, predecessors and compatible-image coverage.
  Pre-native-to-native upgrades are unsupported, not reset or converted.
- Full upsert stress exceeded the local deployment memory cap. Worker cleanup is
  repaired, but full-stress acceptance remains blocked. Do not shrink the workload
  or raise caps blindly. Scratch SSO was expired on the last checks. Local work
  does not depend on it.
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
with their manifests and evidence retained. The fixed candidate and 26.46 negative
control remain. Keep heavy work sequential, enforce 30 GiB disk and 2 GiB available-memory
reserves, and use one low-priority Cargo job. Preserve baseline artifacts,
third-party caches, the persist-traffic worktree and unrelated
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
