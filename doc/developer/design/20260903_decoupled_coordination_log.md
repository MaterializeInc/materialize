# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Assess remaining architecture
load separately. Do not change Persist or the 60s publication default.

## Next concrete action

CDC admission/readiness and bounded PG, MySQL and SQL Server lifecycle coverage
are verified. `native-cdc-runtime-verification.md` records source/binary receipts,
review fixes, successful fixtures and limits. Published in `18888155b6`.

The current full native `workflow_ddl` passed in `aj-native-ddl-569f7b9d`, including
the corrected post-restart recovery probe, sources/sinks, subscriptions, retained
peer writes and explicit retirement. Formatting, preservation comparison and
helper tests pass. The recovery-order helper has negative/positive evidence.

Retirement exhausted three explicit contention409 responses in two earlier
runs. The successful diagnostic measured3.537s and1.491s conflict responses, then
1.826s success. Its failure-only extra retry window did not run. Oracle task2
examines whether a caller retry budget or retirement progress repair is needed.
Do not treat this passing retry as resolving contention or broaden into a raw
catalog cache/follower without justification. No retry policy was changed.

The host restart interrupted the previous diagnostic. Its labelled resources
were removed after saving logs/inspections in
`aj-native-ddl-6cc880ed/interrupted-cleanup.json`. No runtime remains active.
Detailed evidence: `native-ddl-runtime-findings.md`,
`native-ddl-preserved-workload.txt`, `native-ddl-recovery-order-*.log`.

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
retirement. These are selected local outcomes, not broad CI or fresh
distinct-version acceptance.

## Live blockers and decisions

- Temporary-item ownership is approved: retain the session UUID, commit the
  creating adapter incarnation with each Item, preserve disconnect cleanup and
  reclaim only catalog-confirmed replaced/retired executions. Promotion preserves
  Items. No correctness dependency on `mz_sessions`. The user approved a format
  bump with fresh experimental environments, without migration or ownership
  backfill. Use existing fail-closed admission and preserve compatible full
  writers sharing the new format. See `native-temporary-format-decision.md`
  and `native-temporary-owner-implementation-map.md`.
- Catalog-free replica ingress remains. Replacing headless programmable coverage
  with a test-only native runtime needs scope approval. Preserve shared query
  transport, native nonleader workers and both Timely topologies. The dormant
  final-preflight hook's old-to-old coverage also remains pending a decision.
  See `native-replica-execution-boundary.md` and `final-preflight-boundary-research.md`.
- Remaining native lifecycle callers include read-only/fencing expectations,
  forced-migration cases, temporary cleanup and platform/stress workflows.
  Preserve source/sink workloads, predecessors and compatible-image coverage.
  Pre-native-to-native upgrades are explicitly unsupported, not reset/converted.
- Full DDL, current-source preservation, upsert/data-ingest/parallel stress,
  randomized zippy/restore and bumped-version platform runtimes remain unverified.
  A fresh distinct-compatible-version demonstration and external-sink handover
  remain outstanding. Earlier version proof predates this cleanup.
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

All owned runtime fixtures were removed. About 35 GiB is
free. Keep heavy work sequential, enforce 30 GiB disk and 2 GiB available-memory
reserves, and use one low-priority Cargo job. Scratch SSO was expired on the last
checks. Local verification does not depend on it.

Preserve baseline artifacts, third-party caches, the persist-traffic worktree and
unrelated `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
