# Task 3b report: flag + callback wiring into canonicalize_predicates

## Status

DONE. Whole-workspace `cargo check --workspace --all-targets` passes; touched
crates (mz-transform, mz-expr, mz-repr, mz-sql) clippy-clean; `bin/fmt` applied.

## Commit

`af9dc879a5` — eqsat: flag-gated wiring of the scalar canonicalizer into
canonicalize_predicates (Phase 3). Branch `claude/mir-equality-optimizer-sodbej`.

## Feature flag + test coverage

`enable_eqsat_scalar_canonicalize`, `default: false`, `enable_for_item_parsing:
false` (mirrors `enable_eqsat_optimizer`). Defined in
`src/sql/src/session/vars/definitions.rs`; bound into `OptimizerFeatures` in the
macro list (`src/repr/src/optimize.rs`) and at the three SystemVars sites in
definitions.rs (`From<&SystemVars>`, the test destructure, and the `set_var!`
list). Exhaustive `OptimizerFeatureOverrides` sites updated: ddl.rs:4938
destructure, dml.rs:620 construction.

Production default is OFF. `SystemVars::enable_all_feature_flags_by_default()`
(definitions.rs) iterates ALL flags and sets them on; it is called from
`src/adapter/src/catalog/open.rs:129` under `config.all_features`, which is the
slt/testdrive `--all-features` path. So the new path gets CI coverage there with
the flag ON, while every default-feature build keeps it OFF.

## Injection mechanism

mz-expr `canonicalize.rs`: `canonicalize_predicates` is now a thin wrapper over a
new `canonicalize_predicates_with(predicates, repr_column_types, scalar_canon:
Option<&dyn Fn(&mut MirScalarExpr, &[ReprColumnType])>)`. Step 1 matches on
`scalar_canon`: `Some` runs the callback per predicate, `None` runs
`p.reduce(...)`. The `None` path is the exact current `reduce` path.

mz-transform `eqsat/scalar.rs`: new `pub(crate) fn canonicalize_predicates(preds,
col_types, enable_eqsat_scalar: bool)` is the single bridge. When the flag is
set it calls `canonicalize_predicates_with(.., Some(&|e, ct| *e =
canonicalize(e, ct)))`; otherwise it calls the plain mz-expr
`canonicalize_predicates`.

## Call sites wired (flag threaded from the owning TransformCtx)

- `fusion/filter.rs` `Filter::action` (CLU-137 primary path via CanonicalizeMfp
  -> rebuild_mfp -> Filter::action). `Filter::action` gains a `bool`; threaded
  from `Filter::transform`, `Fusion::action`/`Fusion::transform`,
  `CanonicalizeMfp::{action,rebuild_mfp}` + `CanonicalizeMfp::transform`.
- `predicate_pushdown.rs:846` (push_into_let_binding). `PredicatePushdown::action`
  + `push_into_let_binding` gain a `bool`, threaded from
  `PredicatePushdown::transform`, from `fusion/join.rs` re-normalization, and
  from `dataflow.rs` (`optimize_dataflow_filters[_inner]`, threaded from
  `optimize_dataflow`'s ctx).
- `literal_constraints.rs:628` (`LiteralConstraints::canonicalize_predicates`),
  threaded through `undo_preparation` from `LiteralConstraints::action`'s ctx;
  its `rebuild_mfp` call also forwards the flag.
- `normalize_ops.rs` `Fusion::action` call forwards the flag from ctx.

Not wired (passed `false`): `eqsat/raise.rs:444` `rebuild_mfp` (predicates there
come from the saturated e-graph; re-running eqsat would be redundant), and the
test_runner "filter" datadriven harness (keeps existing goldens flag-off).

## Evidence the flag-off path is unchanged

`cargo nextest run -p mz-transform -p mz-expr`: 351 tests pass (1 skipped),
including the datadriven optimizer goldens (`test_runner::tests::run`,
`test_transforms::run_tests`) and `compare_real::compare_egraph_vs_real`. These
run with default features, i.e. the flag OFF, so the `None`/reduce path is
exercised and produces today's output. `mz-sql`
`optimizer_features_no_enable_for_item_parsing` passes (binding correctness). No
goldens were regenerated.

## Flag-on evidence

- mz-expr `canonicalize_predicates_with_invokes_injected_canonicalizer`: proves
  the `Some` path runs the injected callback (counter-based, deterministic).
- mz-expr `canonicalize_predicates_none_matches_wrapper`: wrapper == explicit
  `None`.
- mz-transform `fusion::filter::tests::clu137_temporal_factors_to_top_level_with_flag`:
  builds the CLU-137 DNF Filter, runs `Filter::action(rel, true)`, asserts the
  temporal `mz_now() < cast(ts)` predicate ends up as a top-level Filter
  conjunct (through-the-transform companion to the e-graph-level
  `test_clu137_temporal_factors_to_top_level`).

## Deferred

- Golden/slt/td regeneration for the all-features (flag-on) path: Phase 3c.
- The other `reduce` calls in `canonicalize.rs` (the `replace_subexpr_and_reduce`
  reduce ~:437 and the `canonicalize_equivalences` reduce ~:87), and the
  `predicate.reduce` inside `PredicatePushdown::action`'s Filter handling
  (predicate_pushdown.rs ~:175): possible later widening, intentionally not
  wired.

## Concerns

- A simple Filter input is not a reduce-vs-eqsat differential: `reduce` already
  factors the CLU-137 DNF here, so a "flag-off leaves it buried" assertion is
  false. The flag-on test asserts correct output; routing is proven separately
  by the mz-expr callback test. The genuine soundness differential (could_error
  gating) is exercised end-to-end in 3c.
- With `--all-features`, existing slt/td goldens will diff once this flag turns
  on. That is the expected 3c work and is why 3b leaves the default OFF and does
  not regenerate goldens.
