# fixpoint_logical_02 cutover plan

> **STATUS 2026-06-21: Phase 1 LANDED (commit 65acb4126d). Cutover BLOCKED.**
>
> Phase 1 (the shared `fixpoint_logical_02()` helper + `raise::logical_fixpoint_02` reuse post-pass) is committed and sound (95 unit tests pass).
>
> The cutover itself is blocked:
> * Moving eqsat *before* `fixpoint_logical_01` is UNSOUND (typecheck crash at `column_knowledge.slt:510`: eqsat runs before `EquivalencePropagation` propagates join equivalences, breaking impossible-condition detection on outer joins).
> * Keeping eqsat *after* `fixpoint_01` and making the standalone `fixpoint_02` a conditional strangler-fig (`Box::new(fixpoint_logical_02()); if !ctx.features.enable_eqsat_optimizer,`) is sound and behaviorally equivalent to Phase 1, but only removes a redundant pass and requires a full corpus golden rewrite that bakes in pre-existing eqsat-on plan-quality regressions.
> * Root cause: eqsat consumes `fixpoint_02`'s output and lacks NATIVE outer-join / redundant-self-join collapse, so eqsat-on already loses fast-path peeks (e.g. `column_knowledge.slt:480`, a LEFT-self-join on a full PRIMARY KEY) regardless of placement. Fixing that needs a bottom-up keys/FD e-class analysis + an outer-join collapse rule — the real prerequisite.
>
> **Decisions pending (user):** (1) accept eqsat-on plan-quality losses and do the corpus golden rewrite to get CI green, or build native outer-join collapse first; (2) whether the marginal conditional-cutover is worth committing (needs full-corpus equivalence validation). Do NOT reorder eqsat before `fixpoint_01`.

**Goal:** Subsume `fixpoint_logical_02` into the eqsat raise-time cleanup (Option-B reuse), then delete the standalone pass and move eqsat earlier.

**Strategy:** Reuse, not native rules.
Run the exact production `fixpoint_logical_02` transform list as one eqsat post-pass via a shared `pub fn` helper, so the two run an identical list and the cutover is a one-line deletion.
Native-rule migration is future work: SemijoinIdempotence once a bottom-up keys/FD e-class analysis exists; reduce-expr-fold after the scalar e-graph.
RelationCSE is already native (e-graph congruence + `cse::eliminate_common_subexpressions`).

## Phase 1: build the reuse capability (this change)

### Files
* `src/transform/src/lib.rs`
  * Extract the inline `fixpoint_logical_02` `Fixpoint` (lib.rs:784-804) into a shared `pub fn fixpoint_logical_02() -> Fixpoint`.
  * In `logical_optimizer`, replace the inline construction with `Box::new(fixpoint_logical_02())`. No behavior change here.
* `src/transform/src/eqsat/raise.rs`
  * Add `pub(crate) fn logical_fixpoint_02(expr: &mut MirRelationExpr, commit_wcoj: bool)`.
  * Logical-only (early-return on `commit_wcoj`), mirroring `demand_pushdown`/`reduce_reduction`.
  * Build a local `TransformCtx` (`TransformCtx::local` with `OptimizerFeatures::default()`, `empty_typechecking_context()`, `DataflowMetainfo::default()`, `metrics None`, `global_id None`).
  * Clone-and-adopt-on-success: run `fixpoint_logical_02().transform(&mut work, &mut ctx)`; adopt only if `Ok` and `work.arity() == expr.arity()`.
* `src/transform/src/eqsat.rs` (`optimize_inner`)
  * Replace the `raise::reduce_reduction(&mut raised, commit_wcoj)` call with `raise::logical_fixpoint_02(&mut raised, commit_wcoj)`.
  * The shared helper includes `ReduceReduction`, so the standalone `reduce_reduction` post-pass is subsumed.
  * If `raise::reduce_reduction` becomes unused, remove it and migrate `tests/eqsat_reduce_reduction.rs` to assert the mixed-reduce capability via `optimize_logical` (which now runs the full fixpoint_02). Keep its doc rationale by folding it into `logical_fixpoint_02`'s comment.

### Dependencies between changes
1. Extract the helper in lib.rs first (it is the import target).
2. `raise.rs` imports `crate::{fixpoint_logical_02, Fixpoint?, Transform, TransformCtx}` and `crate::typecheck::empty_typechecking_context`.
   `Transform` brings the `.transform()` method into scope.
3. Wire into `optimize_inner` last.

### Verify after each step
* `cargo check -p mz-transform` after lib.rs extraction (confirm `logical_optimizer` still builds).
* `cargo check -p mz-transform` after raise.rs + eqsat.rs.
* `cargo nextest run -p mz-transform` (or `bin/cargo-test -p mz-transform`); rewrite datadriven `.spec` goldens with `REWRITE=1`.
  Review the `.spec` diff: expect fused / literal-lifted / semijoin-stripped output (benign-equivalent).
* `cargo fmt`, `cargo clippy -p mz-transform`.

## Phase 2: SLT golden shift at current placement (controller, serial)

The post-pass re-normalizes eqsat's output, so SLT goldens shift even before the reorder.
Run representative files with `bin/sqllogictest --optimized`, rewrite, eyeball benign:
* `test/sqllogictest/joins.slt`
* `test/sqllogictest/subquery.slt`
One at a time (shared CockroachDB on localhost:26257).

## Phase 3: cutover (follow-up change)

Validated by the `early_no02` experiment (65 benign diffs, crash-free).
* Reorder `EqSatTransform` before `fixpoint_logical_01` in `logical_optimizer`.
* Delete the `Box::new(fixpoint_logical_02())` call from `logical_optimizer` (keep the helper, now used only by eqsat).
* Corpus-wide SLT + `.spec` golden rewrite for CI green.
