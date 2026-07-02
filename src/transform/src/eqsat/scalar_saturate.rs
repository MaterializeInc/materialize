// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Scalar equality-saturation over the combined e-graph.
//!
//! The scalar canonicalizer runs its rules through the one CombinedLang
//! machinery. The bounds are copied from the standalone `scalar/egraph.rs`
//! (600 / 1_000 / 100) so the fixpoint reached here is identical to the old
//! engine's, which the differential test gates on. Scalar rules run only here,
//! never in the relational `EGraph::saturate` pass.

// The driver and its `canonicalize_combined` entry are the SP2b end-state scalar
// path; production still routes through the old engine until a later slice wires
// this in, so the items are exercised only by the inline test until then.
#![allow(dead_code)]

use mz_expr::MirScalarExpr;
use mz_repr::ReprColumnType;

use crate::eqsat::core::Id;
use crate::eqsat::egraph::view::BaseView;
use crate::eqsat::egraph::{Analyses, CNode, EBindings, EGraph, Index};
use crate::eqsat::rules;
use crate::eqsat::scalar::analysis::{ClassAnalysis, make, merge};
use crate::eqsat::scalar_extract;

/// E-node budget for the saturation loop. Copied from `scalar/egraph.rs` so the
/// scalar fixpoint matches the standalone engine's (parity).
const MAX_ENODES: usize = 600;

/// Per-round match cap per rule. Copied from `scalar/egraph.rs`.
const MATCH_LIMIT: usize = 1_000;

/// Maximum saturation iterations. Copied from `scalar/egraph.rs`.
const MAX_ITERS: usize = 100;

/// Recompute the scalar per-class analysis as a monotone least-fixpoint over the
/// current class layout. The CombinedLang port of `scalar/egraph.rs::recompute_analysis`.
///
/// Operates only on scalar classes (a class holding at least one `CNode::Scalar`).
/// A purely relational class carries no scalar analysis entry, and a scalar
/// node's children are always scalar classes, so restricting to scalar classes
/// keeps `make`'s bottom-up child-lookup invariant intact.
///
/// Seed every scalar class to the merge identity, then repeatedly recompute each
/// class as the merge over its scalar nodes' `make` contributions (reading the
/// current, possibly still-seed, child values) until a pass changes nothing.
/// Pre-seeding makes self-referential classes (from constant folding) sound:
/// `make` always finds its children present, and a self-referential child reads
/// the class's current value rather than being dropped. The lattice is finite
/// and `make`/`merge` are monotone (`could_error` only rises, `literal` only
/// goes None -> Some), so this converges to the conservative upper bound.
///
/// This must run after each `rebuild` (see [`saturate`]): the core's `rebuild`
/// only restores congruence and fires the incremental `on_union` hook, so without
/// this pass a class formed by constant folding could carry an under-approximated
/// `could_error`, the one unsound direction for a guard that blocks error-unsafe
/// rewrites.
fn recompute_analysis(eg: &mut EGraph) {
    let scalar_classes: Vec<Id> = eg
        .class_ids()
        .into_iter()
        .filter(|&id| eg.nodes(id).iter().any(|n| matches!(n, CNode::Scalar(_))))
        .collect();

    eg.data_mut().scalar.analysis.clear();
    for &id in &scalar_classes {
        eg.data_mut().scalar.analysis.insert(
            id,
            ClassAnalysis {
                could_error: false,
                literal: None,
            },
        );
    }
    loop {
        let mut changed = false;
        for &id in &scalar_classes {
            let mut acc = ClassAnalysis {
                could_error: false,
                literal: None,
            };
            for node in eg.nodes(id) {
                let CNode::Scalar(s) = node else { continue };
                let node_a = {
                    let store = &eg.data().scalar.analysis;
                    let find = |c| eg.find(c);
                    make(&s, store, &find)
                };
                acc = merge(acc, node_a);
            }
            // Read the current value into locals so the immutable borrow ends
            // before the mutable `data_mut()` insert below.
            let (cur_err, cur_has_lit) = {
                let cur = eg
                    .data()
                    .scalar
                    .analysis
                    .get(&id)
                    .expect("class seeded above");
                (cur.could_error, cur.literal.is_some())
            };
            // Both fields are monotone, so an inequality is always an increase.
            // Comparing `literal` by presence suffices: equal classes carry the
            // same literal.
            if cur_err != acc.could_error || cur_has_lit != acc.literal.is_some() {
                changed = true;
                eg.data_mut().scalar.analysis.insert(id, acc);
            }
        }
        if !changed {
            break;
        }
    }
}

/// Saturate the scalar rules over `eg`, returning the iteration count.
///
/// The relational `EGraph::saturate` must NOT be reused: it recomputes the
/// relational analyses and reads relational bounds, which would break scalar
/// parity. This loop mirrors the standalone scalar driver instead. Unlike the
/// relational loop it has no per-rule backoff (the old scalar driver had none).
pub(crate) fn saturate(eg: &mut EGraph) -> usize {
    let ruleset = rules::scalar_all();
    let compiled = ruleset.rules();
    let mut iters = 0;
    for _ in 0..MAX_ITERS {
        iters += 1;
        eg.rebuild();
        recompute_analysis(eg);
        if eg.node_count() > MAX_ENODES {
            break;
        }

        // Phase 1 (read-only): collect matches. Scalar rules read neither the
        // relational analyses nor the relational index, so an empty `Analyses`
        // and empty relational `Index` are passed; the scalar index is the live
        // match surface.
        let scalar_index = eg.scalar_index();
        let index = Index::new();
        let analyses = Analyses::default();
        let mut pending: Vec<(usize, EBindings)> = Vec::new();
        {
            let view = BaseView {
                eg,
                index: &index,
                scalar_index: &scalar_index,
                an: &analyses,
            };
            for (qi, rule) in compiled.iter().enumerate() {
                let (matches, _hit) = (rule.find)(&view, &analyses, MATCH_LIMIT + 1);
                for b in matches.into_iter().take(MATCH_LIMIT) {
                    pending.push((qi, b));
                }
            }
        }

        // Phase 2 (mutate): apply and union.
        let mut changed = false;
        for (qi, b) in pending {
            if let Ok(new_id) = (compiled[qi].apply)(eg, &b) {
                if eg.union(new_id, b.root) {
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    iters
}

/// Canonicalize a scalar expression through the combined machinery: lower into a
/// fresh e-graph, saturate the scalar rules, then extract the cheapest form.
///
/// `col_types` is stored for the typed-literal rules to read (unused by the
/// slice-1 rule set, required by the entry's contract for later slices). With an
/// empty scalar rule set this is the identity.
pub(crate) fn canonicalize_combined(
    expr: &MirScalarExpr,
    col_types: &[ReprColumnType],
) -> MirScalarExpr {
    let mut eg = EGraph::new();
    eg.data_mut().scalar.col_types = col_types.to_vec();
    let root = crate::eqsat::scalar::lower::lower_into(&mut eg, expr);
    saturate(&mut eg);
    scalar_extract::raise(&eg, root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{MirScalarExpr, UnaryFunc};

    #[mz_ore::test]
    fn canonicalize_combined_is_identity_without_rules() {
        // A single `Not` matches no compiled scalar rule (`not_not` needs a
        // double negation), so this is the identity.
        let e = MirScalarExpr::column(0).call_unary(UnaryFunc::Not(mz_expr::func::Not));
        assert_eq!(canonicalize_combined(&e, &[]), e);
    }

    #[mz_ore::test]
    fn not_not_rewrites_via_combined() {
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let x = MirScalarExpr::column(0);
        let e = not(not(x.clone()));
        assert_eq!(canonicalize_combined(&e, &[]), x);
    }

    // Differential parity harness (SP2b Slice 1): asserts the combined path
    // equals the old standalone scalar engine (`crate::eqsat::scalar::canonicalize`)
    // on the expressions the ported rules cover. Both entry points are
    // `pub(crate)`, so this lives in-crate rather than as an external
    // integration test; the committed corpus fixture is read via `include_str!`
    // relative to this file.
    //
    // Slice 1 ports only `not_not`, so the corpus is restricted to
    // `not(not(...))`-shaped expressions over a bare column: no literals, no
    // type context, nothing that would trigger one of the old engine's other
    // 20 rules the new path lacks. A failure here is the slice-1 go/no-go
    // trigger, not something to paper over by adjusting the assertion.

    /// Committed corpus fixture (see the file for the format and slice-1 scope).
    const CORPUS: &str = include_str!("../../tests/testdata/eqsat_scalar_corpus");

    #[mz_ore::test]
    fn scalar_parity_not_not() {
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let x = MirScalarExpr::column(0);
        let cases = vec![not(not(x.clone())), not(not(not(x.clone()))), x.clone()];
        for e in cases {
            let new = canonicalize_combined(&e, &[]);
            let old = crate::eqsat::scalar::canonicalize(&e, &[]);
            assert_eq!(new, old, "parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice1() {
        // Slice 1 requires at least one double-negation case. Later slices add
        // tie / could_error / type-context coverage assertions here.
        assert!(CORPUS.contains("not(not("), "corpus must exercise not_not");
    }

    // Differential parity harness (SP2b Slice 2): extends slice 1 to the
    // variadic rules (`and_single`, `or_single`, `not_demorgan_and`,
    // `not_demorgan_or`). Same corpus-shaping constraint as slice 1: distinct
    // bare boolean columns only, so none of the old engine's unported rules
    // (const_fold, dedup, flatten_assoc, not_binary_negate, ...) fire and
    // create a divergence unrelated to the rules under test.
    #[mz_ore::test]
    fn scalar_parity_variadic() {
        use mz_expr::{MirScalarExpr, UnaryFunc, VariadicFunc};
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let c = MirScalarExpr::column;

        let cases = vec![
            and(vec![c(0)]),
            or(vec![c(0)]),
            not(and(vec![c(0), c(1), c(2)])),
            not(or(vec![c(0), c(1), c(2), c(3)])),
            not(and(vec![c(0)])),
            // slice-1 shapes still hold under the grown rule set:
            not(not(c(0))),
        ];
        for e in cases {
            // Boolean, type-agnostic rules: `&[]` col_types is sufficient (the
            // rules ported here never read a column type).
            let new = canonicalize_combined(&e, &[]);
            let old = crate::eqsat::scalar::canonicalize(&e, &[]);
            assert_eq!(new, old, "parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice2() {
        assert!(
            CORPUS.contains("and(#0)"),
            "corpus must exercise and/or single"
        );
        assert!(
            CORPUS.contains("not(and(#0, #1, #2))"),
            "corpus must exercise multi-operand de Morgan"
        );
    }
}
