// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Cost-driven join-key spelling selection for eqsat extraction.
//!
//! Given a join's resolved equivalences and the alternative column spellings
//! the eqsat `Equivalences` analysis proves equal, choose the spelling that
//! minimizes the delta-aware cost `(crosses, degrees)` — so a variable-outer
//! join emits a local key spelling and avoids a cross product. Column-pure
//! remaps only (`EScalar::permute_cols`), which preserve the `lit` fact and
//! sidestep the analysis reducer's canonical-direction bias.

use std::collections::BTreeMap;

use mz_expr::{Columns, MirScalarExpr};

use crate::eqsat::cost::{delta_score_lt, CostModel};
use crate::eqsat::ir::{EScalar, Rel};

/// Maximum number of multi-spelling classes to enumerate jointly, and the
/// maximum size of the joint candidate space. Above either bound the selector
/// keeps the canonical spelling (logged) so a wide join with many aliased
/// classes cannot blow up extraction time.
const CAP_CLASSES: usize = 4;
const CAP_PRODUCT: usize = 64;

/// Choose the join-key spelling minimizing the delta-aware cost.
///
/// `base` is the join's resolved equivalences today (the canonical spelling).
/// `analysis_classes` are the column-equivalence classes the eqsat
/// `Equivalences` analysis proves for this join's e-class. The selector tries,
/// per analysis class, remapping every column member to one chosen
/// representative (column-pure, via `EScalar::permute_cols`), scores each joint
/// assignment with `delta_join_terms`, and returns the cheapest by
/// `delta_score_lt`. Returns `base` unchanged when it already has no forced
/// cross, or when the candidate space exceeds the caps.
pub(crate) fn select_join_spelling(
    model: &CostModel,
    inputs: &[Rel],
    base: &[Vec<EScalar>],
    analysis_classes: &[Vec<MirScalarExpr>],
) -> Vec<Vec<EScalar>> {
    let base_vec = base.to_vec();
    let base_score = model.delta_join_terms(inputs, base);
    tracing::debug!(
        n_inputs = inputs.len(),
        base_crosses = base_score.0,
        n_analysis_classes = analysis_classes.len(),
        ?analysis_classes,
        "select_join_spelling: invoked"
    );
    if base_score.0 == 0 {
        // No forced cross to fix: keep today's spelling (byte-identical).
        return base_vec;
    }

    // Column members per analysis class; only classes with >= 2 distinct columns
    // offer a spelling choice.
    let choice_classes: Vec<Vec<usize>> = analysis_classes
        .iter()
        .filter_map(|class| {
            let distinct: std::collections::BTreeSet<usize> =
                class.iter().filter_map(|e| e.as_column()).collect();
            (distinct.len() >= 2).then(|| distinct.into_iter().collect())
        })
        .collect();

    // Empty or too many choice classes: keep the canonical spelling. The
    // empty case also avoids a redundant single-candidate enumeration below.
    if choice_classes.is_empty() || choice_classes.len() > CAP_CLASSES {
        return base_vec;
    }
    let product: usize = choice_classes
        .iter()
        .map(|c| c.len())
        .try_fold(1usize, |acc, n| acc.checked_mul(n))
        .unwrap_or(usize::MAX);
    if product > CAP_PRODUCT {
        tracing::debug!(
            classes = choice_classes.len(),
            product,
            "select_join_spelling: candidate space over cap; keeping canonical spelling"
        );
        return base_vec;
    }

    // Enumerate one target column per choice class (cartesian product) via an
    // odometer over `idx`.
    let mut best = (base_score, base_vec);
    let mut idx = vec![0usize; choice_classes.len()];
    loop {
        // Build the column-pure remap for this assignment: every column in a
        // choice class maps to that class's chosen target.
        let mut colmap: BTreeMap<usize, usize> = BTreeMap::new();
        for (ci, class) in choice_classes.iter().enumerate() {
            let target = class[idx[ci]];
            for &c in class {
                if c != target {
                    // Real Equivalences classes are column-disjoint: a column
                    // never appears in two distinct choice classes, so this
                    // insert always replaces a missing entry.
                    debug_assert!(
                        colmap.insert(c, target).is_none(),
                        "analysis classes must be column-disjoint"
                    );
                }
            }
        }
        let candidate: Vec<Vec<EScalar>> = base
            .iter()
            .map(|cls| {
                cls.iter()
                    .map(|e| {
                        if e.is_col().is_some() {
                            // Leave bare-column equivalence members intact: they are
                            // the connectivity backbone (e.g. {#0,#2} proves
                            // t1.f0 = t2.f2). Remapping them collapses the class and
                            // orphans an input. Only re-spell expression keys.
                            e.clone()
                        } else {
                            e.permute_cols(|c| *colmap.get(&c).unwrap_or(&c) as i64)
                                .expect("column remap stays non-negative")
                        }
                    })
                    .collect()
            })
            .collect();
        let score = model.delta_join_terms(inputs, &candidate);
        if delta_score_lt(&score, &best.0) {
            best = (score, candidate);
        }
        if !advance(&mut idx, &choice_classes) {
            break;
        }
    }
    tracing::debug!(
        chosen_crosses = best.0.0,
        "select_join_spelling: returning best"
    );
    best.1
}

/// Odometer increment over per-class choice indices; returns false when it wraps
/// past the last assignment.
fn advance(idx: &mut [usize], classes: &[Vec<usize>]) -> bool {
    for i in 0..idx.len() {
        idx[i] += 1;
        if idx[i] < classes[i].len() {
            return true;
        }
        idx[i] = 0;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }
    fn col(c: usize) -> EScalar {
        EScalar::plain(MirScalarExpr::column(c))
    }
    fn eq_expr(a: usize, b: usize) -> EScalar {
        use mz_expr::{func, BinaryFunc};
        EScalar::plain(
            MirScalarExpr::column(a)
                .call_binary(MirScalarExpr::column(b), BinaryFunc::Eq(func::Eq)),
        )
    }

    #[mz_ore::test]
    fn selector_picks_local_spelling_to_avoid_cross() {
        let model = CostModel::new();
        let inputs = vec![get("t1", 2), get("t2", 3), get("t3", 3)];
        // Foreign base: the t3 null-pad key `#0 = #4` references #0 (t1), so its
        // class {#5, #0=#4} is a 3-input class -> a forced cross. The connector
        // class {#0, #2} (t1.f0 = t2.f2) SHARES column #0 with that key — the
        // real (overlapping) VOJ shape.
        let base = vec![vec![col(5), eq_expr(0, 4)], vec![col(0), col(2)]];
        // The analysis proves {#0, #2} equal, offering #0 -> #2 for expr keys.
        let analysis_classes =
            vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(2)]];

        let chosen = select_join_spelling(&model, &inputs, &base, &analysis_classes);

        // Re-spells only the expression key (#0 -> #2); the connector class
        // {#0,#2} stays intact, so there is no forced cross.
        assert_eq!(model.delta_join_terms(&inputs, &chosen).0, 0);
        assert!(delta_score_lt(
            &model.delta_join_terms(&inputs, &chosen),
            &model.delta_join_terms(&inputs, &base),
        ));
    }

    #[mz_ore::test]
    fn selector_keeps_base_when_no_cross() {
        let model = CostModel::new();
        let inputs = vec![get("t1", 2), get("t2", 3), get("t3", 3)];
        // Already-local base: no cross. The selector must return it unchanged
        // (byte-identical), regardless of available spellings.
        let base = vec![vec![col(5), eq_expr(2, 4)], vec![col(0), col(2)]];
        let analysis_classes = vec![vec![
            MirScalarExpr::column(0),
            MirScalarExpr::column(2),
        ]];

        let chosen = select_join_spelling(&model, &inputs, &base, &analysis_classes);
        assert_eq!(chosen, base);
    }
}
