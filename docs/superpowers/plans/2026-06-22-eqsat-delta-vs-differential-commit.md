# Eqsat delta-vs-differential commit decision

> Implementation plan. Read "Corrected diagnosis" first; it supersedes the
> memory-accounting framing in `2026-06-22-p4-cardinality-join-planning.md`.

## Corrected diagnosis

The enshrinement flipped many cyclic joins from differential to delta
(differential lines 37 -> 54; full-scan markers 88 -> 200; reused-index markers
23 -> 7), losing index reuse.
The cause is **not** the cost model's arrangement-accounting bugs.
It is that the eqsat physical pass commits *every* extracted `WcoJoin` to a
`DeltaQuery` unconditionally (`raise.rs:236`, `plan_as_delta_query`), and that
commit passes `available = empty`, so the delta plan reuses no existing index.

Empirically, even with the real single-column indexes available, the eqsat
cost model ranks delta over differential because the memory axis is lexicographic
on the maximum arrangement degree (`cost.rs:153`): the differential plan has a
genuine quadratic (degree 2.0) intermediate, delta has none, so delta wins on the
leading term regardless of how the inputs are counted or credited.
Fixing the accounting (free differential inputs, count delta's per-path
arrangements) cannot flip this: `[2.0]` still beats `[1.0, 1.0, ...]` at
position 0.
So the decision must move out of the peak-degree cost model.

## Goal

At the WcoJoin commit point, choose delta only when it needs no more new
arrangements than the differential plan would: commit delta iff
`delta_new_arrangements <= differential_new_arrangements`, reuse-aware, using
production's exact planners and counts.
This is production's `enable_eager_delta_joins = true` rule, applied always and
independently of the flag (the flag field is private to `OptimizerFeatures`, and
the user's directive is to not consult it).

## Architecture

`delta_queries::plan` and `differential::plan` (`join_implementation.rs`) both
return `(plan, new_arrangements: usize)`, counting new arrangements reuse-aware
through their `available` argument.
A new `pub` helper runs both and returns the cheaper-by-arrangement-count plan.
The eqsat raise step threads the index-availability map (already built in
`transform.rs::build_availability` and held by `optimize_inner`) down to the
WcoJoin arm, builds per-input arrangement availability from the raised join's
inputs, and calls the helper instead of the unconditional `plan_as_delta_query`.

No cost-model changes: the model still extracts `WcoJoin` as the cyclic-join
candidate, but the final delta-vs-differential decision is made at commit time
with production-faithful counts.
The cost model's "errors 1+2" become moot for this decision and are left as
separately-tracked latent inaccuracies.

## Global constraints

* All changes in `mz_transform::eqsat`, plus one additive `pub` helper in
  `join_implementation.rs` (precedent: `plan_as_delta_query` exists solely for
  eqsat).  No rewrite of existing production-transform behavior.
* No `as` conversions; no `unsafe`; comments per house style (no em-dash, no
  structuring semicolons, doc states the contract, reasoning inline, no vendor
  names); never drop existing comments.

## Task 1: `plan_join_min_arrangements` helper

**Files:** `src/transform/src/join_implementation.rs`

Add next to `plan_as_delta_query`:

```rust
/// Plan `join`, choosing the delta strategy iff it needs no more new
/// arrangements than the differential strategy would (reuse-aware, via
/// `available`). Binary joins (<= 2 inputs) are always differential. This is the
/// eager delta rule applied unconditionally, independent of the feature flag.
pub fn plan_join_min_arrangements(
    join: &MirRelationExpr,
    available: &[Vec<Vec<MirScalarExpr>>],
    optimizer_features: &OptimizerFeatures,
) -> Result<MirRelationExpr, TransformError> {
    let MirRelationExpr::Join { inputs, .. } = join else {
        return Err(TransformError::Internal(
            "plan_join_min_arrangements called on a non-join expression".into(),
        ));
    };
    let input_mapper = JoinInputMapper::new(inputs);
    let n = inputs.len();
    let unique_keys: Vec<Vec<Vec<usize>>> = vec![Vec::new(); n];
    let cardinalities: Vec<Option<usize>> = vec![None; n];
    let filters: Vec<FilterCharacteristics> = vec![FilterCharacteristics::none(); n];
    let (diff_plan, diff_new) = differential::plan(
        join, &input_mapper, available, &unique_keys, &cardinalities, &filters,
        optimizer_features,
    )?;
    // Binary joins must be differential (a delta join's advantage of avoiding
    // intermediate arrangements does not apply); mirror JoinImplementation.
    if n <= 2 {
        return Ok(diff_plan);
    }
    match delta_queries::plan(
        join, &input_mapper, available, &unique_keys, &cardinalities, &filters,
        optimizer_features,
    ) {
        Ok((delta_plan, delta_new)) if delta_new <= diff_new => Ok(delta_plan),
        _ => Ok(diff_plan),
    }
}
```

**Verify:** `cargo check -p mz-transform`. Unit test (in `join_implementation.rs`
tests or a new eqsat test): a 3-way triangle over global Gets, `available` empty
-> differential (delta needs more); `available` covering all delta keys ->
delta.

## Task 2: thread availability to `raise` and apply the rule

**Files:** `src/transform/src/eqsat/raise.rs`, `src/transform/src/eqsat.rs`

* `eqsat.rs::optimize_inner`: clone `available` before it is moved into the cost
  model, and pass `&available` to `raise::raise`.
* `raise.rs`: change `raise(rel, commit_wcoj)` and `raise_inner(..)` to carry
  `available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>` (thread through the
  recursion closure).
* WcoJoin arm (`raise.rs:211-240`): when `commit_wcoj`, build per-input
  arrangement availability from the raised `inputs` (collect `ArrangeBy` keys,
  `Reduce` group-key prefix, and global-`Get` index keys from `available`,
  stripping leading non-projecting `Filter`/`Map`), then call
  `crate::join_implementation::plan_join_min_arrangements(&join, &per_input, &features)`.
  On `Err`, fall back to the plain `join`.

**Verify:** `cargo check`; existing `raise.rs` tests pass.

## Task 3: golden + regression validation

**Files:** none (test runs only)

* `bin/sqllogictest --optimized -- test/sqllogictest/explain/optimized_plan_as_text.slt`
  with eqsat flags ON; the triangle test (line 770) must return to
  `type=differential` reusing `t_a_idx`/`u_c_idx`.
* Re-run the previously-enshrined goldens; confirm the differential->delta flips
  revert toward production parity (full-scan markers drop back, reused-index
  markers return).  Rewrite goldens only where the new plan matches production.
* `cargo test -p mz-transform` lib + `eqsat.spec`.

## Decision record

If a golden's eqsat plan still differs from production after this change, that is
a separate issue (extraction structure, MFP), not the delta decision; report it
rather than masking by rewriting the golden to a non-production plan.
