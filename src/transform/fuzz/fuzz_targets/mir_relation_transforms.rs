// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: optimizer transforms on `MirRelationExpr` must preserve a
//! relation's shape and results. We build a random, well-typed plan rooted at
//! `Constant` collections (so it is fully constant-foldable and every scalar
//! subexpression is well-typed by construction), then for each transform check:
//!
//!  1. Shape preservation: arity and per-column scalar types are unchanged
//!     (nullability and keys may be refined).
//!  2. Result equivalence: because the plan is fully constant, `FoldConstants`
//!     evaluates it to a `Constant`, giving the actual result rows. Folding the
//!     transformed plan must yield the same consolidated `(row, diff)` multiset
//!     as folding the original plan, a genuine correctness check.
//!
//! Transforms exercised: `FoldConstants` itself, `CanonicalizeMfp` (Map/Filter/
//! Project chains), `UnionBranchCancellation`, the structural fusions
//! (`Filter`/`Project`/`Map`/`Negate`/`Union`) and `ProjectionExtraction`, plus
//! a hand-written semantics-preserving structural rewrite. Where
//! `full_optimizer_equiv` runs the whole pipeline over constant-rooted plans,
//! this target checks each transform in isolation, so a divergence points at a
//! single transform rather than an interaction.
//!
//! The scalar and constant generators ([`gen_scalar`], [`gen_constant`]) are
//! shared with the other targets. What is specific here is a `Union` generator
//! whose branches are NOT simply `x ∪ -x`: a branch is unioned with a
//! `Map`/`Filter`/`Project`-wrapped negation of an *equal* branch (so the real
//! `compare_branches` recursion through interleaved structural ops must run to
//! detect the `Inverse`), interspersed with a genuinely distinct extra branch
//! that must NOT cancel. This drives `UnionBranchCancellation`'s matching logic
//! instead of only its top-level `x ∪ -x` fast path.

#![no_main]

use std::collections::BTreeMap;

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_repr::{Diff, ReprRelationType, Row};
use mz_transform::canonicalization::ProjectionExtraction;
use mz_transform::canonicalize_mfp::CanonicalizeMfp;
use mz_transform::fold_constants::FoldConstants;
use mz_transform::fusion;
use mz_transform::union_cancel::UnionBranchCancellation;
use mz_transform_fuzz::{
    Ty, apply_recursively, fold_to_multiset, gen_constant, gen_scalar, rand_ty,
};

fn gen_rel(u: &mut Unstructured, depth: u32) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)> {
    if depth == 0 || u.ratio(2u8, 5u8)? {
        return gen_constant(u);
    }
    let (inner, schema) = gen_rel(u, depth - 1)?;
    Ok(match u.int_in_range(0u8..=5)? {
        // Filter: 1-2 boolean predicates over the input columns, shape unchanged.
        0 => {
            let n = u.int_in_range(1usize..=2)?;
            let preds = (0..n)
                .map(|_| gen_scalar(u, Ty::Bool, &schema, 2))
                .collect::<arbitrary::Result<Vec<_>>>()?;
            (inner.filter(preds), schema)
        }
        // Map: append one computed column.
        1 => {
            let ty = rand_ty(u)?;
            let e = gen_scalar(u, ty, &schema, 2)?;
            let mut s = schema.clone();
            s.push(ty);
            (inner.map(vec![e]), s)
        }
        // Project: pick a (possibly reordered/duplicated) subset of columns.
        2 => {
            let len = schema.len();
            let k = u.int_in_range(1usize..=len)?;
            let mut outputs = Vec::with_capacity(k);
            for _ in 0..k {
                outputs.push(u.int_in_range(0..=len - 1)?);
            }
            let s = outputs.iter().map(|&i| schema[i]).collect();
            (inner.project(outputs), s)
        }
        3 => (inner.negate(), schema),
        4 => (inner.distinct(), schema),
        // Union `inner` with a cancelling counterpart. Instead of the trivial
        // `inner ∪ -inner`, the counterpart is `inner` wrapped in a random chain
        // of Map/Filter/Project/Negate carrying an *odd* number of Negates, with
        // the *same* scalars on both sides, so `UnionBranchCancellation`'s
        // `compare_branches` recursion through interleaved structural ops must run
        // to recognize the `Inverse`. A genuinely distinct extra branch (with a
        // fresh predicate) is interspersed and must NOT cancel. The schema is
        // unchanged and the cancelling pair sums to zero, so the result is just
        // `distinct`'s contribution, unaffected by whether the transform fires.
        _ => {
            // Build a Map/Filter/Project wrapper applied identically to two clones
            // of `inner`, with an extra Negate on exactly one of them so the pair
            // cancels. `compare_branches` requires the structural ops to appear in
            // the same order with equal arguments, which this construction
            // guarantees by replaying the same recorded steps on both sides.
            enum Step {
                Map(MirScalarExpr),
                Filter(MirScalarExpr),
                Negate,
            }
            let n_steps = u.int_in_range(0usize..=3)?;
            let mut steps = Vec::with_capacity(n_steps);
            let mut wrapped_schema = schema.clone();
            for _ in 0..n_steps {
                match u.int_in_range(0u8..=2)? {
                    0 => {
                        let ty = rand_ty(u)?;
                        let e = gen_scalar(u, ty, &wrapped_schema, 2)?;
                        wrapped_schema.push(ty);
                        steps.push(Step::Map(e));
                    }
                    1 => steps.push(Step::Filter(gen_scalar(u, Ty::Bool, &wrapped_schema, 2)?)),
                    _ => steps.push(Step::Negate),
                }
            }
            let replay = |mut rel: MirRelationExpr, extra_negate: bool| {
                if extra_negate {
                    rel = rel.negate();
                }
                for step in &steps {
                    rel = match step {
                        Step::Map(e) => rel.map(vec![e.clone()]),
                        Step::Filter(p) => rel.filter(vec![p.clone()]),
                        Step::Negate => rel.negate(),
                    };
                }
                rel
            };
            // The wrapped pair shares the wrapped schema. Project both back to the
            // original arity so the whole union keeps `inner`'s schema.
            let proj: Vec<usize> = (0..schema.len()).collect();
            let left = replay(inner.clone(), false).project(proj.clone());
            let right = replay(inner.clone(), true).project(proj.clone());
            // An extra, genuinely-distinct branch that must survive cancellation:
            // `inner` filtered by a fresh predicate.
            let distinct_pred = gen_scalar(u, Ty::Bool, &schema, 2)?;
            let extra = inner.clone().filter(vec![distinct_pred]);
            // Randomize branch order so the matcher's position search is exercised
            // (`.union` flattens, so this yields a single 3-input `Union`).
            let [b0, b1, b2] = match u.int_in_range(0u8..=2)? {
                0 => [right, extra, left],
                1 => [extra, left, right],
                _ => [left, right, extra],
            };
            (b0.union(b1).union(b2), schema)
        }
    })
}

/// Wrap `rel` in a transformation that preserves its `(row, diff)` multiset.
fn wrap_preserving(
    u: &mut Unstructured,
    rel: MirRelationExpr,
    arity: usize,
) -> arbitrary::Result<MirRelationExpr> {
    let identity = || (0..arity).collect::<Vec<_>>();
    Ok(match u.int_in_range(0u8..=3)? {
        0 => rel.project(identity()),
        1 => rel.filter(vec![MirScalarExpr::literal_true()]),
        2 => rel.negate().negate(),
        _ => rel
            .map(vec![MirScalarExpr::literal_true()])
            .project(identity()),
    })
}

fn assert_shape(
    before: &ReprRelationType,
    after: &ReprRelationType,
    who: &str,
    rel: &MirRelationExpr,
) {
    assert_eq!(
        before.column_types.len(),
        after.column_types.len(),
        "{who} changed the number of columns:\n{rel:?}"
    );
    for (b, a) in before.column_types.iter().zip(after.column_types.iter()) {
        assert_eq!(
            b.scalar_type, a.scalar_type,
            "{who} changed a column's scalar type:\n{rel:?}"
        );
    }
}

/// If both the baseline and the transformed plan fold to constants, require the
/// result multisets to match.
fn assert_same_rows(
    baseline: &Option<BTreeMap<Row, Diff>>,
    transformed: MirRelationExpr,
    who: &str,
    orig: &MirRelationExpr,
) {
    if let (Some(b), Some(t)) = (baseline.as_ref(), fold_to_multiset(transformed)) {
        assert_eq!(*b, t, "{who} changed the fold result:\n{orig:?}");
    }
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let (rel, schema) = gen_rel(u, 5)?;
    let baseline = fold_to_multiset(rel.clone());

    // A hand-written semantics-preserving structural rewrite.
    let rewrite = wrap_preserving(u, rel.clone(), schema.len())?;
    assert_same_rows(&baseline, rewrite, "structural rewrite", &rel);

    // CanonicalizeMfp: canonicalizes Map/Filter/Project chains.
    {
        let mut r = rel.clone();
        let before = r.typ();
        if CanonicalizeMfp.action(&mut r).is_ok() {
            assert_shape(&before, &r.typ(), "CanonicalizeMfp", &rel);
            assert_same_rows(&baseline, r, "CanonicalizeMfp", &rel);
        }
    }

    // UnionBranchCancellation: cancels a branch unioned with its negation.
    {
        let mut r = rel.clone();
        let before = r.typ();
        if apply_recursively(UnionBranchCancellation, &mut r).is_ok() {
            assert_shape(&before, &r.typ(), "UnionBranchCancellation", &rel);
            assert_same_rows(&baseline, r, "UnionBranchCancellation", &rel);
        }
    }

    // Structural fusions. Each is a purely local, semantics-preserving rewrite
    // applied across the whole tree (pre-order, matching their real drivers).
    // None changes the result multiset or the output shape, on any input.
    for (who, action) in [
        (
            "FilterFusion",
            fusion::filter::Filter::action as fn(&mut MirRelationExpr),
        ),
        ("ProjectFusion", fusion::project::Project::action),
        ("MapFusion", fusion::map::Map::action),
        ("NegateFusion", fusion::negate::Negate::action),
        ("UnionFusion", fusion::union::Union::action),
        ("ProjectionExtraction", ProjectionExtraction::action),
    ] {
        let mut r = rel.clone();
        let before = r.typ();
        r.visit_pre_mut(action);
        assert_shape(&before, &r.typ(), who, &rel);
        assert_same_rows(&baseline, r, who, &rel);
    }

    // FoldConstants: the evaluator itself must at least preserve shape.
    {
        let mut r = rel;
        let before = r.typ();
        if apply_recursively(FoldConstants { limit: None }, &mut r).is_ok() {
            assert_shape(&before, &r.typ(), "FoldConstants", &r);
        }
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
