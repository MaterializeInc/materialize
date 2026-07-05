// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The map-split rewrite: peel the first scalar off a `Map` into an upstream
//! shared `Map`, the reverse of `fuse_maps`, so a shared Map prefix hash-conses
//! across consumers. Peel-first only: the first appended scalar reads only input
//! columns, and a position-1 prefix split preserves the exact column layout, so
//! no independence guard and no index rewrite are needed. Iterated peel-first
//! plus `fuse_maps` reaches all common POSITIONAL, in-order prefixes. Hand-written
//! (the DSL cannot destructure the scalar list). Lean theorem in MapSplit.lean is
//! a sorry symmetric to `rule_fuse_maps` (Map acts on row/column structure, not
//! bag-modeled).
//!
//! LIMITATION (order-sensitivity): a share is MISSED when the shared scalar is
//! not a positional prefix of the extending Map. `Map[g, s](r)` alongside a
//! sibling `Map[s](r)` does not share, because `s` is not first. Maps are
//! order-sensitive, so unlike filter-split there is no reordering to canonicalize
//! this away. The follow-up for that reach is an any-input-only-peel with a
//! column-index rewrite (a corrective Project for a middle peel). Deferred.

use crate::eqsat::dsl::Phase;
use crate::eqsat::egraph::view::{self, MatchGraph};
use crate::eqsat::egraph::{Analyses, CNode, EBindings, EGraph, ENode, Id, Sym};
use crate::eqsat::matcher::Payload;
use crate::eqsat::rules::{AnalysisNeeds, CompiledRule};

/// Scalar-list length cap. Bounds the peel chain, matching filter_split's role.
/// A `Map` with more than `K` scalars does not share.
pub(crate) const K: usize = 4;

/// How often the cap was hit (a `Map` with more than `K` scalars was left
/// un-split). Lets a no-share outcome be told apart from a cap decline.
pub(crate) static DECLINED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Build the split `CompiledRule`. The scalar-list cap is the module [`K`], read
/// directly by the fn-pointer `find` (a fn pointer cannot carry a runtime cap).
/// A `const fn` so the `&'static` [`SPLIT_RULE`] can be initialized from it.
pub(crate) const fn map_split_rule() -> CompiledRule {
    CompiledRule {
        name: "map_split",
        phase: Phase::Physical,
        needs: AnalysisNeeds {
            nonneg: false,
            keys: false,
            monotonic: false,
        },
        colored: false,
        find: find_map_split,
        apply: apply_map_split,
    }
}

/// The registered split rule. Appended to the physical rule set by
/// `CompiledRuleSet::with_extra_rule` when `scalar_sharing` is on.
pub(crate) static SPLIT_RULE: CompiledRule = map_split_rule();

/// One match per `Map` with `2..=K` scalars: peel the first scalar. A wider Map
/// is skipped and recorded in [`DECLINED`]. A one-scalar Map has no non-trivial
/// prefix to expose, so it is skipped (its `Map[s0](r)` is already a shareable
/// node without splitting).
fn find_map_split(g: &view::BaseView, _an: &Analyses, limit: usize) -> (Vec<EBindings>, bool) {
    let mut out: Vec<EBindings> = Vec::new();
    for (root_id, root_node) in g.nodes_by_sym(Sym::Map) {
        let ENode::Map { input, scalars } = &root_node else {
            continue;
        };
        let n = scalars.len();
        if n > K {
            DECLINED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        }
        if n < 2 {
            continue;
        }
        let mut b = EBindings::default();
        b.root = root_id;
        b.rels.insert("input", *input);
        b.payloads
            .insert("scalars", Payload::Scalars(scalars.clone()));
        out.push(b);
        if out.len() >= limit {
            return (out, true);
        }
    }
    (out, false)
}

/// Instantiate `Map[rest](Map[[s0]](input))` for the first-scalar peel, returning
/// the outer class. `rest` keeps its original order because Map scalars are
/// order-sensitive, unlike filter-split which canonicalizes by sorting. The
/// saturation loop unions the result into `b.root`, so this never calls `union`
/// itself.
fn apply_map_split(g: &mut EGraph, b: &EBindings) -> Result<Id, String> {
    let input = b
        .rels
        .get("input")
        .copied()
        .ok_or_else(|| "map_split: unbound relation metavariable input".to_string())?;
    let scalars = b
        .payloads
        .get("scalars")
        .cloned()
        .ok_or_else(|| "map_split: unbound payload metavariable scalars".to_string())?
        .into_scalars()?;
    let (first, rest) = scalars
        .split_first()
        .ok_or_else(|| "map_split: empty scalar list".to_string())?;
    // `rest` keeps its original order: Map appends columns positionally, so
    // reordering scalars would change the output layout. This is the opposite of
    // filter-split, which sorts to canonicalize an order-insensitive predicate set.
    let inner = g.add(CNode::Rel(ENode::Map {
        input,
        scalars: vec![*first],
    }));
    let outer = g.add(CNode::Rel(ENode::Map {
        input: inner,
        scalars: rest.to_vec(),
    }));
    Ok(outer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::analysis::LocalFacts;
    use crate::eqsat::egraph::EGraph;
    use crate::eqsat::ir::{EScalar, Rel};
    use crate::eqsat::rules::CompiledRuleSet;
    use mz_expr::MirScalarExpr;

    /// Saturate `eg` with a rule set containing only the split rule.
    fn saturate_with_split_for_test(eg: &mut EGraph) {
        let rules = CompiledRuleSet::of(vec![&SPLIT_RULE]);
        eg.saturate(&rules, 4, &LocalFacts::default());
    }

    #[mz_ore::test]
    fn split_exposes_inner_single_scalar_map() {
        // Map[s, g](r): peeling the first scalar s outward must add Map[s](r) as a
        // sub-node, so a sibling Map[s](r) (consumer 1) hash-conses with it.
        let get = Rel::Get {
            name: "r".into(),
            arity: 2,
        };
        let s = EScalar::plain(MirScalarExpr::column(0));
        let g_scalar = EScalar::plain(MirScalarExpr::column(1));
        let extending = Rel::Map {
            scalars: vec![s.clone(), g_scalar.clone()],
            input: Box::new(get.clone()),
        };
        let consumer1 = Rel::Map {
            scalars: vec![s.clone()],
            input: Box::new(get.clone()),
        };

        let mut eg = EGraph::new();
        let m = eg.add_rel(&extending);
        let c1 = eg.add_rel(&consumer1);
        eg.rebuild();

        saturate_with_split_for_test(&mut eg);

        // `m`'s class must now hold a single-scalar Map whose scalar is `s` and
        // whose input class is consumer1's Map[s](r) class.
        let c1_root = eg.find(c1);
        let found = eg.class_ids().into_iter().any(|cls| {
            eg.find(cls) == eg.find(m)
                && eg.nodes(cls).iter().any(|n| {
                    matches!(n,
                        CNode::Rel(ENode::Map { input, scalars })
                            if scalars.len() == 1 && eg.find(*input) == c1_root)
                })
        });
        assert!(
            found,
            "split did not expose Map[g](Map[s](r)) sharing consumer1"
        );
    }

    #[mz_ore::test]
    fn cap_declines_wide_map() {
        // A Map with K+1 scalars is left un-split and increments DECLINED.
        let get = Rel::Get {
            name: "r".into(),
            arity: 2,
        };
        let scalars: Vec<EScalar> = (0..(K + 1))
            .map(|i| EScalar::plain(MirScalarExpr::column(i % 2)))
            .collect();
        let wide = Rel::Map {
            scalars,
            input: Box::new(get),
        };

        let mut eg = EGraph::new();
        let w = eg.add_rel(&wide);
        eg.rebuild();

        let before = DECLINED.load(std::sync::atomic::Ordering::Relaxed);
        saturate_with_split_for_test(&mut eg);
        let after = DECLINED.load(std::sync::atomic::Ordering::Relaxed);
        assert!(after > before, "cap decline not recorded for |scalars| > K");

        // No split node appeared: no single-scalar Map is in `w`'s class.
        let w_root = eg.find(w);
        let split_appeared = eg.class_ids().into_iter().any(|cls| {
            eg.find(cls) == w_root
                && eg.nodes(cls).iter().any(|n| {
                    matches!(n,
                        CNode::Rel(ENode::Map { scalars, .. }) if scalars.len() == 1)
                })
        });
        assert!(!split_appeared, "wide map must not split");
    }

    /// Drift tripwire pinning the emitted shape. `apply_map_split` on
    /// `Map[s0, s1](input)` must produce `Map[[s1]](Map[[s0]](input))`, the exact
    /// shape the Lean theorem `mapB (catRows s1 s2) r = mapB s2 (mapB s1 r)`
    /// states. A change to the rule's emitted structure breaks this test and
    /// forces re-review of the hand-authored theorem in MapSplit.lean.
    #[mz_ore::test]
    fn emitted_shape_matches_lean_theorem() {
        let get = Rel::Get {
            name: "r".into(),
            arity: 2,
        };
        let s0 = EScalar::plain(MirScalarExpr::column(0));
        let s1 = EScalar::plain(MirScalarExpr::column(1));
        let extending = Rel::Map {
            scalars: vec![s0.clone(), s1.clone()],
            input: Box::new(get.clone()),
        };

        let mut eg = EGraph::new();
        let m = eg.add_rel(&extending);
        // Intern the input relation and the two scalars separately so the
        // assertion pins ids, not just shape.
        let input_id = eg.add_rel(&get);
        let s0_id = eg.intern_scalar(&s0);
        let s1_id = eg.intern_scalar(&s1);
        eg.rebuild();

        saturate_with_split_for_test(&mut eg);

        // The outer node is Map[[s1]](inner) where inner is Map[[s0]](input).
        let m_root = eg.find(m);
        let outer_ok = eg.class_ids().into_iter().any(|cls| {
            if eg.find(cls) != m_root {
                return false;
            }
            eg.nodes(cls).iter().any(|n| {
                let CNode::Rel(ENode::Map { input, scalars }) = n else {
                    return false;
                };
                // Outer node is Map[[s1]](inner).
                if scalars.len() != 1 || eg.find(scalars[0]) != eg.find(s1_id) {
                    return false;
                }
                // Inner class holds Map[[s0]](input).
                eg.nodes(eg.find(*input)).iter().any(|inner| {
                    matches!(inner,
                        CNode::Rel(ENode::Map { input: inner_in, scalars: inner_sc })
                            if inner_sc.len() == 1
                                && eg.find(*inner_in) == eg.find(input_id)
                                && eg.find(inner_sc[0]) == eg.find(s0_id))
                })
            })
        });
        assert!(
            outer_ok,
            "emitted shape drifted from Map[[s1]](Map[[s0]](input))"
        );
    }

    #[mz_ore::test]
    fn rule_metadata_is_stable() {
        let r = map_split_rule();
        assert_eq!(r.name, "map_split");
        assert_eq!(r.phase, Phase::Physical);
    }
}
