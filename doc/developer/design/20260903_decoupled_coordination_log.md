# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Assess remaining architecture
load separately. Do not change Persist or the 60s publication default.

## Next concrete action

Resolve the remaining headless-driver and forced-migration boundaries before
removing their legacy execution assumptions. The proposed headless replacement
keeps arbitrary-plan scripts in a test-only native runtime, not a production
maintenance socket. Its launch-interface change awaits approval.
Controller startup explicitly rejects forced migrations, so those cases are
not native coverage. Continue current-source preservation checks independently.
Do not reset or convert pre-native fixtures. Catalog-free ingress and dormant
final-preflight coverage still need the scope decisions below.

The current-source distinct-version full DDL and sink-handover demonstration
passed. Its receipts and limits are in `native-compatible-current/verification.md`.
No production Kafka metrics change was needed. The replacement-deployment
workflow also passes with no active-generation processes: candidate SQL/table
writes work before promotion, while maintained output waits for authority.
See `native-compatible-current/offline-candidate-verification.md`.
The basic coexistence workflow also passes with the distinct-version pair,
including fresh source/sink input and cursor subscriptions after predecessor
shutdown. See `native-compatible-current/basic-verification.md`.
Forward-write candidate correction pruning also passes with the original workload
and memory bound. Correction records drain to zero without replica restarts.
Anonymous-memory growth is 362.4 MiB against a 384 MiB bound, a narrow single-run
margin. See `native-compatible-current/forward-pruning-verification.md`.
All runtime fixtures are removed.

## Verified outcome

Controllerd owns processes, replica followers own maintained execution, and
adapters retain policy, transactions, serving protection and transient queries.
Legacy combined/read-only serving, private migration execution, alternate
protection catalogs and exclusive age-based process cleanup are removed.
Required formats and native introspection inputs/publishers remain.

Targeted debug checks cover native startup/restart, readiness, durable protection,
queries, Kafka ALTER SINK, retained writers/sessions, explicit retirement and relay
continuity. Same-build platform lifecycle, MV pruning and default/larger index
protection cohorts passed. PG, MySQL and SQL Server bounded CDC cases passed prewarming,
reader handover, forced reconnect with both deployments alive, and ingestion after
retirement. The full DDL workflow also passed with first-request retirement while peers
continued publishing. Temporary objects retain session visibility and execution
ownership through ALTER, rename, privileges and promotion. Disconnect, confirmed
replacement and explicit retirement reclaim their metadata while preserving
survivors. Format 97 requires fresh experimental state, with no ownership backfill.
The current-source 26.46/26.47 prototype pair also passes full DDL, source and
external-sink handover. Both versions write and create indexed objects after
promotion. Warmed replica identities and the incumbent session survive. Worker
statistics establish old sink operator withdrawal, and new-owner commit increments
plus Kafka markers establish production through both SQL writers. Explicit
retirement rejects the old writer while the survivor continues.

The pair shares implementation and plan encoding, not arbitrary release
compatibility. These are selected local outcomes, not rebuilt-image or broad CI
acceptance. The native-only follower cleanup also passes all clusterd unit tests,
scoped all-target checks and independent review.

## Live blockers and decisions

- Catalog-free replica ingress remains. Replacing headless programmable coverage
  with a test-only native runtime needs scope approval. Preserve shared query
  transport, native nonleader workers and both Timely topologies. The dormant
  final-preflight hook's old-to-old coverage also remains pending a decision.
  See `native-replica-execution-boundary.md` and `final-preflight-boundary-research.md`.
- Remaining native lifecycle callers include forced-migration cases and
  platform/stress workflows.
  Preserve source/sink workloads, predecessors and compatible-image coverage.
  Pre-native-to-native upgrades are explicitly unsupported, not reset/converted.
- Current-source preservation, upsert/data-ingest/parallel stress,
  randomized zippy/restore and bumped-version platform runtimes remain unverified.
  Distinct-version DDL and sink handover are verified for the same-implementation
  prototype pair, not arbitrary release combinations.
- An earlier index-publication stall and a 673-second source-status query remain
  unexplained. Passing retries do not resolve them. Startup activation,
  birth-ordering concerns and the original four-adapter workload after the
  txn-WAL retirement repair remain in the
  [follow-up inventory](20260903_decoupled_coordination_followups.md).

Broad CI is not green. Last reviewed broad evidence is
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image gate. Registry visibility, merge skew, Persist JSON-order
checking and stale hydration expectations remain unresolved. Full formatting last
failed on an unchanged generic call in `src/catalog/src/catalog.rs`. Changed-file
formatting and scoped all-target Rust checks pass. The PR stays draft.

## Essential evidence and local state

Evidence root: `~/.local/share/aj/m3-evidence/native-only/`.

- Current-source compatible lifecycle: `native-compatible-current/verification.md`.
  Binary/source manifests pin both versions. The full DDL run is
  `native-compatible-current/aj-native-compatible-18207d30/`.
- Native follower cleanup: `native-follower-only-verification.md`.
  Earlier compatible stability and temporary-object runs remain in
  `native-compatible/verification.md`. Their superseded binaries were removed
  for disk headroom. Source hashes and runtime receipts remain.

- Temporary ownership: `native-temporary-owner-verification.md`, including the
  ordinary alias/protection path, restart replay, conflicting-prefix rejection,
  negative controls and full SQL lifecycle receipts. The existing dependent-temp
  rename restriction remains in the follow-up inventory.
- Retirement: `native-retirement-verification.md`. The published repair in
  `854d883cb8` preserves raw candidates across empty progress and rebuilds after
  genuine content. Full debug DDL and controlled negative cases passed.

- CDC: `native-cdc-admission-review.md`, `native-cdc-shutdown-audit.md`,
  `native-pg-handover-review.md`, `native-cdc-runtime-verification.md` and
  `native-*-handover-latest`. Candidate/source
  receipts are `native-cdc-build-binaries.sha256` and
  `native-cdc-build-source.sha256`. Rust 1.98.1 build and both changed crates'
  all-target checks passed. The shutdown audit's auxiliary cancellation gaps are
  outside this repair. Temporary overlap is allowed. Superseded readers must
  eventually stop reconnecting, not satisfy a zero-overlap contract.
- Index/pruning: `native-index-resource-verification.md`,
  `native-index-compact-observation-review.md`, `native-pruning-verification.md`.
  The default index run used earlier binaries, the larger cohort used the CDC
  candidate. Full catalog dumps were replaced by certified permission reads.
- Platform/stress: `native-platform-runtime-verification.md`,
  `native-bumped-platform-verification.md`, `native-zippy-verification.md`,
  `native-parallel-lifecycle-verification.md` distinguish runtime from wiring proof.
- Earlier removal evidence and detailed pointers are preserved in
  `native-handoff-before-cdc-final.md`. Prior version proof is
  `../lifecycle/verification.md`. The [publication comparison](20260903_decoupled_coordination_publication_interval.md)
  and `../logical-component-load/` remain the performance baseline, not a cleanup
  performance claim. Further history is in the [archive](20260903_decoupled_coordination_log_archive.md).

Completed runtime fixtures were removed. Unused images and first-party build caches
were reclaimed. Keep measuring available space. Keep heavy work sequential,
enforce 30 GiB disk and 2 GiB available-memory
reserves, and use one low-priority Cargo job. Scratch SSO was expired on the last
checks. Local verification does not depend on it.

Preserve baseline artifacts, third-party caches, the persist-traffic worktree and
unrelated `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
