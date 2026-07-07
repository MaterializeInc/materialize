// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The filter-split rewrite: peel one predicate off a `Filter` outward so a
//! shared sub-filter can hash-cons across consumers. Hand-written because the
//! DSL deliberately treats predicate lists as opaque whole lists and cannot
//! destructure them, and `Tmpl::Builtin` is scalar-only. The rewrite is the
//! inverse of `merge_filters`. Its Lean theorem is hand-authored in
//! lean/MirRewrite/FilterSplit.lean (this rule never reaches gen-lean).
//!
//! The rule is registered as a `&'static` (`SPLIT_RULE`), appended to the
//! physical rule set by `CompiledRuleSet::with_extra_rule` only when the
//! `filter_sharing` gate is set. A fn-pointer `find` cannot carry a runtime cap,
//! so the predicate-list cap lives in the module `const K` that the matcher
//! reads directly.

use crate::eqsat::dsl::Phase;
use crate::eqsat::egraph::view::{self, MatchGraph};
use crate::eqsat::egraph::{Analyses, CNode, EBindings, EGraph, ENode, Id, Sym};
use crate::eqsat::matcher::Payload;
use crate::eqsat::rules::{AnalysisNeeds, CompiledRule};

/// Predicate-list length cap. A `Filter` with more than `K` predicates is left
/// alone: the reachable subset set is bounded by `2^K`, so the cap keeps
/// saturation finite when the split reverses `merge_filters`. Wider filters do
/// not share. Counter [`DECLINED`] records how often the cap was hit, so a
/// no-share outcome is distinguishable from a cap decline.
pub(crate) const K: usize = 4;

/// How often the cap was hit (a `Filter` with more than `K` predicates was left
/// un-split). Lets a no-share outcome be told apart from a cap decline.
pub(crate) static DECLINED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Build the split `CompiledRule`. The predicate-list cap is the module [`K`],
/// read directly by the fn-pointer `find` (a fn pointer cannot carry a runtime
/// cap). A `const fn` so the `&'static` [`SPLIT_RULE`] can be initialized from it.
pub(crate) const fn filter_split_rule() -> CompiledRule {
    CompiledRule {
        name: "filter_split",
        phase: Phase::Physical,
        needs: AnalysisNeeds {
            nonneg: false,
            keys: false,
            monotonic: false,
        },
        colored: false,
        find: find_filter_split,
        apply: apply_filter_split,
    }
}

/// The registered split rule. Appended to the physical rule set by
/// `CompiledRuleSet::with_extra_rule` when `filter_sharing` is on.
pub(crate) static SPLIT_RULE: CompiledRule = filter_split_rule();

/// Enumerate one match per predicate to peel outward, for every `Filter` with
/// `2..=K` predicates. Every single-predicate peel is emitted as its own match
/// (try-all/union-all), never one chosen via `find_map`, so the result is
/// order-independent. A `Filter` wider than `K` is skipped and recorded in
/// [`DECLINED`].
fn find_filter_split(g: &view::BaseView, _an: &Analyses, limit: usize) -> (Vec<EBindings>, bool) {
    let mut out: Vec<EBindings> = Vec::new();
    for (root_id, root_node) in g.nodes_by_sym(Sym::Filter) {
        let ENode::Filter { input, predicates } = &root_node else {
            continue;
        };
        let n = predicates.len();
        if n > K {
            DECLINED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        }
        if n < 2 {
            continue;
        }
        for i in 0..n {
            let mut b = EBindings::default();
            b.root = root_id;
            b.rels.insert("input", *input);
            b.payloads
                .insert("preds", Payload::Predicates(predicates.clone()));
            b.payloads
                .insert("peel", Payload::Predicates(vec![predicates[i]]));
            out.push(b);
            if out.len() >= limit {
                return (out, true);
            }
        }
    }
    (out, false)
}

/// Instantiate `Filter[peel](Filter[rest](input))` for one peel, returning the
/// outer class. The saturation loop unions it into `b.root`, so this never calls
/// `union` itself.
fn apply_filter_split(g: &mut EGraph, b: &EBindings) -> Result<Id, String> {
    let input = b
        .rels
        .get("input")
        .copied()
        .ok_or_else(|| "filter_split: unbound relation metavariable input".to_string())?;
    let all = b
        .payloads
        .get("preds")
        .cloned()
        .ok_or_else(|| "filter_split: unbound payload metavariable preds".to_string())?
        .into_predicates()?;
    let peel = b
        .payloads
        .get("peel")
        .cloned()
        .ok_or_else(|| "filter_split: unbound payload metavariable peel".to_string())?
        .into_predicates()?;
    let peel = *peel
        .first()
        .ok_or_else(|| "filter_split: empty peel payload".to_string())?;

    // Removing the peeled predicate by value drops every copy of it. Predicate
    // conjunction is idempotent, so a duplicate peeled out is semantically the
    // same filter, and the degenerate all-copies case falls to the empty-rest
    // guard below.
    let mut rest: Vec<Id> = all.into_iter().filter(|&p| p != peel).collect();
    if rest.is_empty() {
        return Err("filter_split: nothing to peel".into());
    }
    // Canonical order by class rep (`find`), not raw `Id`: two ids for the same
    // scalar class must sort to the same slot so the inner `Filter` dedups to one
    // node per subset regardless of which peel path built it, keeping the `2^K`
    // canonical-node dedup intact. `g.add`'s canonicalization remaps each id
    // through `find`, so sorting by `find` aligns the stored order with the
    // canonicalized values. No canonical predicate order exists in the engine, so
    // the rule imposes one here.
    rest.sort_by_key(|&p| g.find(p));
    let inner = g.add(CNode::Rel(ENode::Filter {
        input,
        predicates: rest,
    }));
    let outer = g.add(CNode::Rel(ENode::Filter {
        input: inner,
        predicates: vec![peel],
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
    fn saturate_with_split_for_test(eg: &mut EGraph, _cap: usize) {
        let rules = CompiledRuleSet::of(vec![&SPLIT_RULE]);
        eg.saturate(&rules, 4, &LocalFacts::default());
    }

    #[mz_ore::test]
    fn split_exposes_inner_single_predicate_filter() {
        // Filter[a,b](r): peeling a outward must add Filter[a](r) as a sub-node,
        // so a sibling Filter[a](r) hash-conses with it.
        let get = Rel::Get {
            name: "r".into(),
            arity: 2,
        };
        let a = EScalar::plain(MirScalarExpr::column(0).call_is_null().not());
        let b = EScalar::plain(MirScalarExpr::column(1).call_is_null().not());
        let fused = Rel::Filter {
            predicates: vec![a.clone(), b.clone()],
            input: Box::new(get.clone()),
        };
        let consumer1 = Rel::Filter {
            predicates: vec![a.clone()],
            input: Box::new(get.clone()),
        };

        let mut eg = EGraph::new();
        let f = eg.add_rel(&fused);
        let c1 = eg.add_rel(&consumer1);
        eg.rebuild();

        saturate_with_split_for_test(&mut eg, 4);

        // `f`'s class must now hold a single-predicate Filter whose input class is
        // consumer1's class.
        let c1_root = eg.find(c1);
        let found = eg.class_ids().into_iter().any(|cls| {
            eg.find(cls) == eg.find(f)
                && eg.nodes(cls).iter().any(|n| {
                    matches!(n,
                        CNode::Rel(ENode::Filter { input, predicates })
                            if predicates.len() == 1 && eg.find(*input) == c1_root)
                })
        });
        assert!(
            found,
            "split did not expose Filter[b](Filter[a](r)) sharing consumer1"
        );
    }

    #[mz_ore::test]
    fn cap_declines_wide_filter() {
        // A Filter with K+1 predicates is left un-split and increments DECLINED.
        let get = Rel::Get {
            name: "r".into(),
            arity: K + 2,
        };
        let preds: Vec<EScalar> = (0..(K + 1))
            .map(|i| EScalar::plain(MirScalarExpr::column(i).call_is_null().not()))
            .collect();
        let wide = Rel::Filter {
            predicates: preds,
            input: Box::new(get),
        };

        let mut eg = EGraph::new();
        let w = eg.add_rel(&wide);
        eg.rebuild();

        let before = DECLINED.load(std::sync::atomic::Ordering::Relaxed);
        saturate_with_split_for_test(&mut eg, 4);
        let after = DECLINED.load(std::sync::atomic::Ordering::Relaxed);
        assert!(after > before, "cap decline not recorded for |S| > K");

        // No split node appeared: no single-predicate Filter is in `w`'s class.
        let w_root = eg.find(w);
        let split_appeared = eg.class_ids().into_iter().any(|cls| {
            eg.find(cls) == w_root
                && eg.nodes(cls).iter().any(|n| {
                    matches!(n,
                        CNode::Rel(ENode::Filter { predicates, .. }) if predicates.len() == 1)
                })
        });
        assert!(!split_appeared, "wide filter must not split");
    }

    #[mz_ore::test]
    fn rule_metadata_is_stable() {
        let r = filter_split_rule();
        assert_eq!(r.name, "filter_split");
        assert_eq!(r.phase, Phase::Physical);
    }
}
