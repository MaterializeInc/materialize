// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Color-aware extraction: a generic least-cost extractor parameterized by a
//! pluggable [`CostModel`]. Resolves children through `find(c,·)` and considers,
//! per colored class, the base e-nodes plus the colored delta nodes (siblings),
//! so a colored equality can expose a cheaper representative. SP3b ships a toy
//! additive cost; SP4 supplies the real relational/scalar cost.

use std::collections::HashMap;

use crate::eqsat::colored::{ColorId, ColoredEGraph};
use crate::eqsat::core::{Id, Language, MAX_ANALYSIS_ITERS};
use crate::eqsat::egraph::{CNode, CombinedLang};

/// A pluggable cost model over language `L`.
pub(crate) trait CostModel<L: Language> {
    type Cost: Clone + Ord;
    /// Cost of `node` given the current best cost of each child class. `child`
    /// is the child id as it appears in `node`; the extractor resolves it.
    fn cost(&self, node: &L::Node, child_cost: &dyn Fn(Id) -> Self::Cost) -> Self::Cost;
}

impl<'b, L: Language> ColoredEGraph<'b, L> {
    /// Extract the lowest-cost e-node per class under color `c`. Children resolve
    /// through `find(c,·)`; each class's candidate set is its base e-nodes plus
    /// its colored delta nodes (siblings). A bounded least-fixpoint.
    pub(crate) fn extract_colored<C: CostModel<L>>(
        &mut self,
        c: ColorId,
        model: &C,
    ) -> HashMap<Id, (L::Node, C::Cost)> {
        // Gather visible nodes (base + c/ancestor delta), grouped by colored
        // class. For each node, precompute the resolution child_id -> rep so the
        // fixpoint never needs `&mut self`.
        let mut by_class: HashMap<Id, Vec<(L::Node, HashMap<Id, Id>)>> = HashMap::new();
        let visible = self.visible_nodes(c);
        for (owner, n) in visible {
            let rep = self.find(c, owner);
            let cmap: HashMap<Id, Id> = L::children(&n)
                .into_iter()
                .map(|ch| (ch, self.find(c, ch)))
                .collect();
            by_class.entry(rep).or_default().push((n, cmap));
        }

        let mut best: HashMap<Id, C::Cost> = HashMap::new();
        let mut best_node: HashMap<Id, L::Node> = HashMap::new();
        for _ in 0..MAX_ANALYSIS_ITERS {
            let mut changed = false;
            for (&cls, nodes) in &by_class {
                for (n, cmap) in nodes {
                    // Skip until every child class has a cost.
                    if !cmap.values().all(|r| best.contains_key(r)) {
                        continue;
                    }
                    let cost = model.cost(n, &|ch| {
                        let r = cmap.get(&ch).copied().unwrap_or(ch);
                        best.get(&r).cloned().expect("child cost present")
                    });
                    if best.get(&cls).map_or(true, |c0| &cost < c0) {
                        best.insert(cls, cost);
                        best_node.insert(cls, n.clone());
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        by_class
            .keys()
            .filter_map(|&cls| {
                Some((cls, (best_node.get(&cls)?.clone(), best.get(&cls)?.clone())))
            })
            .collect()
    }
}

/// A structural tree-size [`CostModel`] over the combined relational+scalar
/// language, used by the relational extractor (`egraph::build`) to pick the
/// cheapest colored relational *conclusion* per class (SP4d T10).
///
/// Each node costs `1 + sum(child costs)`, so a node's cost is the total node
/// count of its whole (relational + scalar payload) subtree — the colored
/// analogue of `scalar_expr_cost` lifted to [`CNode`]. This drives ONLY the
/// per-class colored-conclusion selection (the structural simplification); the
/// final base-vs-colored extraction decision is made by the real
/// [`crate::eqsat::cost::CostModel`] on the built `Rel`, so a colored conclusion
/// is taken only when it genuinely wins.
pub(crate) struct TreeSizeColoredCost;

impl CostModel<CombinedLang> for TreeSizeColoredCost {
    type Cost = usize;
    fn cost(&self, node: &CNode, child_cost: &dyn Fn(Id) -> usize) -> usize {
        1 + CombinedLang::children(node)
            .into_iter()
            .map(child_cost)
            .sum::<usize>()
    }
}

/// True when color `c` has at least one relational (`CNode::Rel`) delta node.
///
/// When false, `extract_colored` on this color can produce no relational
/// conclusion that differs from the base (every visible relational e-node is
/// already a base node), so building the per-color conclusion table is provably
/// empty-for-relational and can be skipped — a byte-identical early-out in
/// `EGraph::extract_with` (SP4d).
///
/// `delta_nodes` stores all colored-conclusion nodes (both relational and
/// scalar); we need to distinguish relational ones because `extract_with` only
/// records `CNode::Rel` conclusions (it skips scalar deltas) and skipping the
/// per-color table is only sound when there are none.
impl ColoredEGraph<'_, CombinedLang> {
    pub(crate) fn has_rel_delta_nodes(&self, c: ColorId) -> bool {
        self.colors[c.0]
            .delta_nodes
            .keys()
            .any(|n| matches!(n, CNode::Rel(_)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::colored::oracle::{oracle_close, oracle_extract};
    use crate::eqsat::colored::toy::{ToyCost, ToyLang, ToyNode};
    use crate::eqsat::core::EGraph;

    #[mz_ore::test]
    fn extract_matches_oracle() {
        // Base: a=Leaf, b=Leaf, fa=Op(0,[a]), fb=Op(0,[b]). Color: a≅b.
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        let _fa = eg.add(ToyNode::Op(0, vec![a]));
        let _fb = eg.add(ToyNode::Op(0, vec![b]));
        eg.rebuild();

        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        ceg.close(c, &[(a, b)]);

        let got = ceg.extract_colored(c, &ToyCost);
        let oracle = oracle_close(&eg, &[(a, b)], &[]);
        let want = oracle_extract(&oracle, &ToyCost);

        // Compare costs per class (keyed by the mechanism's reps, mapped through
        // the oracle's reps for the same ids).
        for (&cls, (_node, cost)) in &got {
            let oracle_cost = want.get(&oracle.rep(cls)).expect("oracle has class");
            assert_eq!(cost, oracle_cost, "cost mismatch for class {cls}");
        }
    }

    /// Early-out #2 accessor: `has_rel_delta_nodes` must return false for a fresh
    /// color (no rules have fired yet) and true after a relational delta node is
    /// added via `add_colored`. Tests both branches of the predicate.
    #[mz_ore::test]
    fn has_rel_delta_nodes_reflects_delta_node_presence() {
        use crate::eqsat::egraph::{CNode, EGraph, ENode};
        // Build a minimal base graph (one Constant leaf).
        let mut eg = EGraph::new();
        let _leaf = eg.add(CNode::Rel(ENode::Constant {
            card: 1,
            arity: 1,
            col_types: None,
        }));
        eg.rebuild();

        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);

        // Fresh color: no delta nodes of any kind yet.
        assert!(
            !ceg.has_rel_delta_nodes(c),
            "fresh color has no Rel delta nodes",
        );

        // Add a relational node that is NOT already in the base (different card/arity
        // ensures it is genuinely new and goes into delta_nodes).
        ceg.add_colored(
            c,
            CNode::Rel(ENode::Constant {
                card: 99,
                arity: 2,
                col_types: None,
            }),
        );
        assert!(
            ceg.has_rel_delta_nodes(c),
            "color must have a Rel delta node after add_colored(Rel(...))",
        );
    }

    #[mz_ore::test]
    fn colored_equality_lowers_cost() {
        // Base has a cheap Leaf(0) and an expensive Op tree both reachable from a
        // class only after a colored equality. With c: target≅cheap, the
        // extracted cost of `target`'s class drops to the leaf cost.
        let mut eg = EGraph::<ToyLang>::new();
        let cheap = eg.add(ToyNode::Leaf(0));
        let m = eg.add(ToyNode::Leaf(1));
        let expensive = eg.add(ToyNode::Op(0, vec![m, m])); // cost 1 + 1 + 1 = 3
        eg.rebuild();

        // Base extraction: `expensive`'s class costs 3.
        let mut base_ceg = ColoredEGraph::new(&eg);
        let bc = base_ceg.new_color(None);
        let base = base_ceg.extract_colored(bc, &ToyCost);
        assert_eq!(base[&eg.find(expensive)].1, 3, "base cost of expensive tree");

        // Colored: expensive ≅ cheap ⇒ the class can pick the Leaf, cost 1.
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        ceg.close(c, &[(expensive, cheap)]);
        let colored = ceg.extract_colored(c, &ToyCost);
        let rep = ceg.find(c, expensive);
        assert_eq!(colored[&rep].1, 1, "colored equality exposes the cheap Leaf");
    }
}
