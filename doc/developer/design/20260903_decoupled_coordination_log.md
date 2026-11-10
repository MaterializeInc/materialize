# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Assess remaining architecture
load separately. Do not change Persist or the 60s publication default.

## Next concrete action

Finish distinct-version sink handover verification on fresh format 97. The compiled
26.46/26.47 prototype pair passes the existing promotion/retirement stability
workflow. Both versions serve DDL and writes after promotion, the incumbent session
survives, and candidate process/index identities and the relay remain stable.
This pair shares implementation and plan encoding, not arbitrary release compatibility.
See `native-compatible/verification.md` and `aj-native-compatible-f8dd06d6/` beneath it.

The full DDL extension is uncommitted and unverified. Independent review found that
inactive workers can unregister the producing worker's Kafka sink metrics, so
metric disappearance cannot establish producer withdrawal. Approval was requested
for a small storage-side metrics ownership repair. Do not apply it without agreement.
The separate cross-version temporary-object lifecycle also passed in
`native-compatible/aj-native-compatible-a99c9dde/`. No conversion from pre-native
or earlier experimental catalog formats is supported.

Temporary execution ownership and reclamation are implemented and verified.
The existing maintenance turn follows incremental ownership state and commits
catalog-validated whole-owner cleanup. The full same-build debug SQL lifecycle
passes at the unchanged publication cadence. Rust boundary checks, negative
controls and independent production review also passed. Evidence and limits:
`native-temporary-owner-verification.md`, with runtime receipts in
`aj-native-temporary-0d28dffc/`.

All owned runtime fixtures are stopped and removed. The unused legacy storage
constructor and re-export are removed. Storage, compute and clusterd all-target
checks passed after correcting a missed nontemporary catalog-test argument.
The pinned version pair predates that API cleanup. Preserve its source receipts
and rebuild for further production changes. Builds/tests remain sequential and
resource-guarded. No Persist or production publication defaults changed.

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
These are selected local outcomes, not broad CI or complete distinct-version sink acceptance.

## Live blockers and decisions

- Catalog-free replica ingress remains. Replacing headless programmable coverage
  with a test-only native runtime needs scope approval. Preserve shared query
  transport, native nonleader workers and both Timely topologies. The dormant
  final-preflight hook's old-to-old coverage also remains pending a decision.
  See `native-replica-execution-boundary.md` and `final-preflight-boundary-research.md`.
- Remaining native lifecycle callers include read-only/fencing expectations,
  forced-migration cases and platform/stress workflows.
  Preserve source/sink workloads, predecessors and compatible-image coverage.
  Pre-native-to-native upgrades are explicitly unsupported, not reset/converted.
- Current-source preservation, upsert/data-ingest/parallel stress,
  randomized zippy/restore and bumped-version platform runtimes remain unverified.
  Full distinct-compatible-version DDL and external-sink handover remain outstanding.
  The fresh prototype stability run covers serving, promotion and retirement only.
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
