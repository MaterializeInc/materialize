# Architecture review — `adapter::coord::sequencer`

Scope: `src/adapter/src/coord/sequencer/` (12,030 LOC total: `sequencer.rs`, `inner.rs`, `inner/`).

See also `inner/ARCH_REVIEW.md` for leaf-level findings.

## 1. Per-statement `Message` variants for staged re-queue

**Files**
- `src/adapter/src/coord.rs:363-406` — 9 `*StageReady` variants in `Message` enum (`PeekStageReady`, `CreateIndexStageReady`, `CreateViewStageReady`, `CreateMaterializedViewStageReady`, `SubscribeStageReady`, `IntrospectionSubscribeStageReady`, `SecretStageReady`, `ClusterStageReady`, `ExplainTimestampStageReady`).
- `src/adapter/src/coord/message_handler.rs:152-177` — 9 match arms, all identical bodies: `self.sequence_staged(ctx, span, stage).boxed_local().await`.

**Problem**
Each staged statement type must add: (a) an `XxxStage` enum in `inner/`, (b) `impl Staged for XxxStage`, and (c) a new `Message::XxxStageReady` variant in `coord.rs` + a new match arm in `message_handler.rs`. Steps (c) are pure boilerplate: the 9 `message_handler` arms are syntactically identical except for destructuring the concrete stage type. The `Message` enum, which is the coordinator's main reactive dispatch surface, accumulates one variant per staged statement type.

**Deletion test**
If the 9 concrete variants were collapsed into one — `Message::StagedReady { ctx: Option<ExecuteContext>, span: Span, stage: Box<dyn Staged<Response=ExecuteResponse>> }` — complexity vanishes: zero arms need updating when a new statement type is added. The type erasure does require `Staged` to be object-safe (add the `async_trait` or a helper wrapper), which is mechanically solvable.

**Risk**
`IntrospectionSubscribeStageReady` uses `()` instead of `ExecuteContext`; a polymorphic variant needs to accommodate that difference. This is solvable (e.g. a `MaybeCtx` wrapper), but adds surface. If Rust adds async-trait-in-dyn support via `return_position_impl_trait_in_trait`, this becomes cleaner.

**Benefits**
- Adding a new staged statement type drops from a 3-file change to a 1-file change.
- `Message` stops growing by one variant per statement type.
- `message_handler.rs` needs no change at all for new staged types.

## 2. (Honest skip) DDL serialization in `sequencer.rs`

The `serialized_ddl` / `LockedVecDeque` logic in `sequence_plan` (commit-path) serializes DDL transactions. It is a 30-line inline policy check, not a structural pattern that creates friction. Extracting it would relocate complexity without reducing it.

## 3. (Honest skip) RBAC check placement

`rbac::check_plan` is called once near the top of `sequence_plan`, before the match. This is the correct placement — upstream of all statement-type dispatch — and is not duplicated elsewhere. No friction.

## What this review did not reach

- Whether the `Staged` trait's `validity()` + `stage()` two-method contract is the best interface for the stage types — covered in `inner/ARCH_REVIEW.md`.
- Whether the `sequence_staged` driver loop could be generalized to handle back-pressure from the internal command channel. That is a runtime concern, not a structural one.
