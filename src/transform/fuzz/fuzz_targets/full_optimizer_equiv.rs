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
//! `NonNullRequirements`, `Demand`, `ReduceElision` (group key provably unique),
//! `RedundantJoin`, and `SemijoinIdempotence` all take code paths off that exact
//! information that opaque, all-nullable `Get`s never trigger. And they run here
//! in real pipeline order with their interactions, which the per-transform
//! `mir_relation_transforms` target (transforms in isolation) also cannot reach.
//! So this target sits in a real gap between the other two.
//!
//! Oracle: fold the input to its `(row, diff)` multiset, run the optimizer, fold
//! the result. When both fold to a constant, the multisets must be equal. A
//! divergence is a miscompile. The comparison is conservative (we only assert
//! when both sides fold, and skip when the optimizer returns an error, e.g. the
//! `Typecheck` pass rejecting a plan shape), so a surviving assertion failure or
//! a panic inside the optimizer is a genuine finding.

#![no_main]

use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use mz_transform_fuzz::{fold_to_multiset, gen_constant, gen_rel, optimize};

fn run(u: &mut Unstructured) -> libfuzzer_sys::arbitrary::Result<()> {
    let mut leaf = gen_constant;
    let (rel, _schema, _nn) = gen_rel(u, 4, &mut leaf)?;

    // The input must fold to actual rows for there to be anything to compare.
    let Some(baseline) = fold_to_multiset(rel.clone()) else {
        return Ok(());
    };

    let Some(optimized) = optimize(rel.clone()) else {
        return Ok(());
    };

    // The optimizer is semantics-preserving: the optimized plan must fold to the
    // same multiset. We only assert when the optimized plan also folds (it should,
    // since all leaves are constant), staying conservative about fold limitations.
    if let Some(after) = fold_to_multiset(optimized) {
        assert_eq!(
            baseline, after,
            "the optimizer changed the result multiset\n{rel:?}"
        );
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
