# Decoupled coordination implementation handover

## Status and scope

Milestone 2 is incomplete. Lifecycle components still run inside the adapter.
Protected plan explanations, runtime notices, and the in-progress runtime installer
consume committed selections. Missing runtime plans/imports enter a retry queue.
Mixed bootstrap still contains writer planning. Do not report the checkpoint as
an adapter-loss demonstration or a completed plan-store cutover.

Read the selected decisions in `20260903_decoupled_coordination.md` and the active
steering in `20260903_decoupled_coordination_prompt.md` before continuing.

- M2 is one adapter and one build. Keep per-build keys, but defer cross-build
  repair, version upgrades, and prewarming-owned selections.
- Retired-build cleanup is deferred. Fencing permits retirement of an older
  build's selections and entries. An unfenced owner's entries are off limits.
- The writer owns plans and notices. Lifecycle components install written plans
  and wait on missing plans/imports, rather than planning replacements.
- No plan publication/projection/replay machinery, provenance records, controller
  state RPC, dummy controllers, or volatile hold forwarding.
- Use “lifecycle components”. Do not introduce names based on the retired
  controller-placement terminology.
- Table WAL time and webhook batching/idle time remain adapter-driven. Their
  pause during sole-adapter downtime is acceptable. Source-fed maintenance and
  compaction must continue.
- Separate commits are permitted at coherent review boundaries. Continue through
  integration rather than stopping at a green intermediate check.

## History and workspace

The workspace is jj-managed. Preserve the user's design commits and rewritten
history. The draft PR is https://github.com/MaterializeInc/materialize/pull/38696.
The bookmark is `decoupled-coordination`. The immutable store, writer selection,
and plan-explanation checkpoints are pushed. Current validation status is in the
draft PR.

- `a6ace2b0`: request-side storage, history/webhook writers, metadata-backed reads,
  real-time recency, request progress, and logical maintained admission.
- `a187bdee`: user design/terminology steering, following the Written plans and
  own-build rewrite decisions. Preserve its ancestors as well.
- `f3db4f49`: independently committed immutable expression-store API and tests.
- jj change `xttrvyms`: catalog selectors and partial writer integration covered
  by this handover. Use `jj log` for its final commit hash.

There are no editing sub-agents to resume. Build artifacts and temporary logs
were partly cleared after disk exhaustion. Do not rely on old `/tmp` paths as
durable evidence or regenerate Cargo.lock to recover a build.

## Implemented boundaries

### Immutable bytes and atomic selections

`src/catalog/src/expr_cache.rs` reuses the expression shard. Written entries are
keyed by build, export GlobalId, and UUID revision and contain `GlobalExpressions`.
`write_plans` acknowledges durability and rejects conflicting revision reuse,
including after CAS contention. `read_plans` synchronizes and does not generate
missing values. Codec and shutdown errors are distinct from misses. Optional
cache reconciliation/invalidation skips written entries.

The catalog has a typed `WrittenPlan` collection keyed by GlobalId and build,
with a UUID selection. Schema version 95 and its snapshot/migration are included.
Selections use the existing catalog transaction/CAS, not a cross-shard protocol.
A lost catalog CAS leaves an unselected immutable entry.

`Op::SetWrittenPlan` checks its expected predecessor within the transaction.
Surviving selections' import existence is checked against the final candidate
state, not an operation prefix. Selection updates invalidate planning snapshots
but do not generate dataflow installation implications. Build ownership at these
low-level APIs remains a caller obligation, not an enforced authorization check.

### Writer integration

`Catalog::prepare_item_plan`, `write_plans`, and `read_written_plans` are in
`src/adapter/src/catalog.rs`. Create index/MV/metric-sink finish stages append
selection ops to the object transaction. Protected startup opens the expression
store even if optional cache reuse is disabled. Object drops unselect only the
current build's entries.

`Coordinator::prepare_written_plan_rewrites` in `coord/ddl.rs`:

- Builds post-DDL candidate state with `transact_incremental_dry_run`, excluding
  selector ops until plans are repaired.
- Reads own-build selections, including candidates supplied by the op batch.
- Rebuilds surviving plans whose imports disappear using explicit candidate
  catalog state. CREATE INDEX alone does not invoke this path.
- Uses the optimizer's `replan` ordering for indexes to prevent self-imports and
  mutual cycles among rewritten indexes.
- Acquires client protection for existing physical inputs through commit. Inputs
  born in the batch are left to transaction-time birth protection.
- Updates replacement item versions and removes retired output selections.
- Trims notices whose dependencies disappear or change catalog-item identity,
  using the writer's before/after catalogs, without persisted provenance.
- Batches immutable writes, then adds selectors to the DDL transaction.
- Rejects planning-state changes discovered while acquiring protection.

The three maintained optimizer builders in `coord.rs` now accept explicit
`Arc<CatalogState>`. No Coordinator catalog swap or new wrapper was introduced.

MV creation additionally freezes compatible access paths on the writer. It
reuses a compatible initial plan or replans with eligible indexes/source paths,
keeps MIR/LIR/raw notices together, and holds physical input protection through
`catalog_transact_with_context`. Logical admission remains independent of index
readiness. See `ensure_materialized_view_access_paths` in
`coord/sequencer/inner/create_materialized_view.rs`.

Bootstrap reads existing own-build selections and preserves their optimizer
choices even if current optimizer features differ. It writes selections for
newly prepared plans before installation. This is still inside the mixed
Coordinator bootstrap, not an independently running writer/applier split.

### Other local changes

Compaction proposals are not retried unchanged after a planning-changing catalog
refresh. They must be resampled because enacting new DDL can add execution holds.
Pending proposal work remains queued on failure. This addresses a code-level
overlapping-controller counterexample, but has no runtime race demonstration.

Request append paths use adapter-local read-only state. Protected hydration-history
collection uses query-connection readiness instead of controller status. Protected
PeekClient construction omits legacy storage access, which remains available only
to unprotected readers.

## Remaining work and risks

1. Complete the written-plan bootstrap split. Protected runtime installation uses
   selected bytes, while unprotected installation retains cache/fallback planning.
   Runtime retries revisit physical dependencies and use current catalog identities.
   Missing selected bytes still error in writer notice refresh and mixed bootstrap.
   The agreed full-application rule defers all bound publication and client
   reclamation while runtime installation is pending. Controller servicing and
   unrelated catalog effects continue. Pending work retries with capped backoff and
   is exposed through logs and metrics. No scoped exception or durable
   physical-import protection is implemented.
2. Exercise selected-plan EXPLAIN and writer-owned runtime notice updates through
   `restart/selected-plan-explain`. Protected installation no longer writes notices.
   The request task holds one catalog snapshot and is cancellable. MV pushdown
   explanations and timestamp explanations still have controller dependencies.
3. Exercise dependent rewrites at the coordinator/SQL boundary. Compilation is
   not proof of the new rewrite routine. Include multiple equivalent indexes,
   same-batch new storage inputs, create/select/drop of one owner, replacements,
   failed CAS, and CREATE INDEX leaving existing selections unchanged.
4. MV creation and DROP rewrites share all-ready-selected-replica eligibility,
   including target selection. Neither a cached frontier nor a catalog bound is
   itself a hold. Test loss of permission/readability between filtering and
   acquisition. Acquisition errors
   currently abort rather than necessarily retrying with an alternative path.
5. Validate birth protection for new inputs excluded from live-catalog acquisition,
   especially aliases sharing existing shards. The review finding was addressed
   in code, but the same-batch race has not been demonstrated.
6. Verify bootstrap dependency order and historical as-of selection against fixed
   written plans, not against assumptions that installation can replan. Preserve
   pending replacement semantics and do not skip promised historical output.
7. Extract lifecycle ownership and startup. Mixed Coordinator bootstrap, catalog
   following, controller configuration/status effects, read policies, physical
   finalization, and request-side legacy handles remain coupled. Do not use a
   dummy controller or state proxy to bridge them.
   Protected PeekClient/statistics/hydration plumbing no longer requires legacy
   storage handles. DROP INDEX reports only committed plan rewrites and no longer
   consults physical controller dependencies. Timestamp and MV pushdown explanations
   remain request-side controller consumers.
8. Demonstrate autonomous source-fed maintained work and compaction through adapter
   death/restart. The earlier two-process COPY coexistence CI proof is useful but
   is not this acceptance test. Include isolated request failures and recovery.

Review evidence: storage/selector review found operation-prefix import validation,
which was changed to final-state validation and tested. Dependent-rewrite review
found self-import/cycle risk and live-catalog acquisition of newly born inputs.
Both have code changes, but still require the coordinator-level regressions above.

## Verification

Targeted commands for the existing store, selection, and planning boundaries:

```sh
cargo test -p mz-catalog --lib expr_cache::tests --locked
cargo test -p mz-catalog --test written-plans --locked
cargo test -p mz-adapter --lib written_plan_selection --locked
cargo test -p mz-adapter --lib materialized_view --locked
cargo check -p mz-adapter --all-targets --locked
cargo clippy -p mz-catalog -p mz-durable-cache -p mz-adapter --all-targets --locked -- -D warnings
```

The MV optimizer test covers access-path selection and immutable readback, not
an independent installation race. `restart/selected-plan-explain` covers SQL
rewrites, introspection, and restart. The in-progress environmentd
`test_peer_index_pending_installation` uses a real cooperative catalog writer to
exercise missing selection, unrelated installation, and dropping pending work.
Same-batch physical dependency ordering and reclamation contention need further
production coverage. Current validation results belong in the draft PR.
