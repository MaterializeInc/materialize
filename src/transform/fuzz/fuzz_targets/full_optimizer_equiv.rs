// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the full logical optimizer must preserve results over plans
//! rooted at literal `Constant`s. We build a random, well-typed plan over the
//! bug-rich relational operators (`Join`, `Reduce`, `TopK`, `Threshold`,
//! `Union`, and the map/filter/project/negate/distinct set), run the entire
//! `Optimizer::logical_optimizer` pipeline, and check the optimized plan folds
//! to the same `(row, diff)` multiset as the input.
//!
//! # Why this target as well as `optimizer_symbolic_equiv`
//!
//! On a `Constant`-rooted plan the optimizer's first constant folding (at the
//! tail of the first `fuse_and_collapse_fixpoint`, roughly the sixth transform)
//! collapses the whole plan to a single `Constant`. Every later stage
//! (predicate/equivalence propagation, reduction pushdown, join
//! ordering/implementation, CSE, literal lifting) then runs on that bare
//! `Constant` and is a no-op. So for those stages this target contributes
//! nothing and `optimizer_symbolic_equiv`, which keeps its `Get`s opaque so real
//! relational planning happens, strictly dominates.
//!
//! What this target uniquely covers is the *other* side of that coin: the
//! transforms that DO run here see literal `Constant` subtrees, which carry
//! exact information a symbolic `Get` withholds:
//!
//!  * refined nullability (`Constant::typ` marks a column non-nullable when no
//!    row is null in it), and
//!  * exact keys and cardinality inferred from the actual rows.
//!
//! Two transforms actually see that: `NonNullRequirements`, which runs before the
//! first `fuse_and_collapse_fixpoint`, and `RedundantJoin`, which runs inside
//! `FuseAndCollapse` ahead of that fixpoint's trailing constant folding. Both
//! take code paths off exact nullability and keys that opaque, all-nullable
//! `Get`s never trigger, as do the `NormalizeOps`/`FuseAndCollapse`
//! canonicalizations. And they run here in real pipeline order with their
//! interactions, which the per-transform `mir_relation_transforms` target
//! (transforms in isolation) cannot reach.
//!
//! NOTE: `Demand`, `ReduceElision` and `SemijoinIdempotence` do *not* belong on
//! that list, though they branch on the same exact information. `Demand` sits in
//! `fixpoint_logical_01` and the other two in `fixpoint_logical_02`, both after
//! step 2's trailing `fold_constants_fixpoint`, and every operator this generator
//! emits has a `FoldConstants` arm, so by then the plan is a single `Constant` and
//! they are no-ops. The only executions that reach them with real relational
//! structure are the ones where folding bails at `FOLD_CONSTANTS_LIMIT`, which
//! needs roughly two nested 4-way joins over 4-row leaves. Nothing in the suite
//! covers those three against exact key/cardinality facts: the symbolic target's
//! `Get` deliberately carries the constant's stored all-nullable, keyless type.
//! Closing that would mean a leaf whose `Get` declares `constant.typ()`, which
//! belongs in `gen_get` there rather than here.
//!
//! Oracle: fold the input to its `(row, diff)` multiset, run the optimizer, fold
//! the result. When both fold to a constant, the multisets must be equal. A
//! divergence is a miscompile. The comparison is conservative (we only assert
//! when both sides fold, and skip a plan matching the open bug CLU-137 via
//! `hits_non_strict_error_fold`), so a surviving assertion failure or a panic
//! inside the optimizer is a genuine finding. An optimizer *error* is not a skip:
//! a plan shape the typechecker rejects panics inside `Typecheck` rather than
//! becoming an `Err`, so every `TransformError` reaching us is an invariant
//! violation. See `mz_transform_fuzz::optimize`.

#![no_main]

use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use mz_transform_fuzz::{
    Collapse, collapse, fold_to_multiset, gen_constant, gen_rel, hits_non_strict_error_fold,
    optimize,
};

fn run(u: &mut Unstructured) -> libfuzzer_sys::arbitrary::Result<()> {
    let mut leaf = gen_constant;
    let (rel, _schema, _nn) = gen_rel(u, 4, &mut leaf)?;

    // Skip the shape of the open bug CLU-137, which the optimizer gets wrong for
    // reasons unrelated to whatever else the plan exercises.
    if hits_non_strict_error_fold(&rel) {
        return Ok(());
    }

    // The input must fold to actual rows for there to be anything to compare.
    let Some(baseline) = fold_to_multiset(rel.clone()) else {
        return Ok(());
    };

    let optimized = optimize(rel.clone());

    // The optimizer is semantics-preserving: the optimized plan must fold to the
    // same multiset.
    //
    // Fold the optimized side with `collapse`, not a single `FoldConstants` pass.
    // `RelationCSE` can bind a repeated subexpression to a `Let`, and this
    // generator hands it perfect candidates because the `Union` arm clones `inner`
    // into both branches. `FoldConstants` does not propagate constants through
    // `Let`/`Get`, so a single pass leaves such a plan unfolded and the assertion
    // is skipped. That skip lands on exactly the executions worth checking: a
    // `Let` survives only when the plan was still relational after step 2, i.e.
    // when folding bailed at the row limit, which is also the only window in which
    // the post-collapse stages ran on real structure at all.
    match collapse(optimized) {
        Collapse::Const(after) => assert_eq!(
            baseline, after,
            "the optimizer changed the result multiset\n{rel:?}"
        ),
        // A genuine fold limitation, or still simplifying at the budget.
        Collapse::StuckFixpoint | Collapse::BudgetExhausted => {}
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
