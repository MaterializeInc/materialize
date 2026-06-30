// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Colored conclusions: a per-color canonical hash-cons for e-nodes that exist
//! only in a color (what colored rewrite *conclusions* will produce in SP4).
//! Stored as a delta, never inserted into the base — the ≤2× bound. Driven
//! synthetically here via [`ColoredEGraph::add_colored`]; SP4 supplies a rule
//! driver.

use crate::eqsat::core::{Id, Language};
use crate::eqsat::colored::{ColorId, ColoredEGraph};

impl<'b, L: Language> ColoredEGraph<'b, L> {
    /// Insert `node` as a colored e-node in `c`. Children are canonicalized
    /// through `find(c,·)`. If the canonical form is already a base e-node,
    /// returns the base class (no colored storage). If it is already in `c`'s or
    /// an ancestor's delta store, returns that id. Otherwise allocates a fresh
    /// colored id (> `base.uf_len()`) and stores it canonically in `c`.
    pub(crate) fn add_colored(&mut self, c: ColorId, node: L::Node) -> Id {
        let cn = self.canon(c, &node);

        // Already represented in black? Only meaningful when every child is a
        // base id (a colored-id child cannot exist in the base).
        let all_base_children = L::children(&cn).iter().all(|&ch| ch < self.base.uf_len());
        if all_base_children {
            if let Some(base_id) = self.base.lookup(&cn) {
                return self.find(c, base_id);
            }
        }

        // Already in c's or an ancestor's delta store?
        for cid in self.chain_top_down(c) {
            if let Some(&id) = self.colors[cid.0].delta_nodes.get(&cn) {
                return id;
            }
        }

        // Allocate a fresh colored id and store canonically in c.
        let id = self.next_colored_id;
        self.next_colored_id += 1;
        self.colors[c.0].delta_nodes.insert(cn, id);
        id
    }
}

#[cfg(test)]
mod tests {
    use crate::eqsat::core::{EGraph, Id};
    use crate::eqsat::colored::ColoredEGraph;
    use crate::eqsat::colored::toy::{ToyLang, ToyNode};

    fn base_with_two_leaves() -> (EGraph<ToyLang>, Id, Id) {
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        eg.rebuild();
        (eg, a, b)
    }

    #[mz_ore::test]
    fn add_colored_dedups_canonically() {
        let (eg, a, _b) = base_with_two_leaves();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let n1 = ceg.add_colored(c, ToyNode::Op(7, vec![a]));
        let n2 = ceg.add_colored(c, ToyNode::Op(7, vec![a]));
        assert_eq!(n1, n2, "identical colored conclusions hash-cons to one id");
        assert!(n1 >= eg.uf_len(), "colored id is allocated above the base id space");
    }

    #[mz_ore::test]
    fn add_colored_hits_base_when_present() {
        // Op(0,[a]) already exists in base ⇒ add_colored returns the base class,
        // not a fresh colored id.
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let fa = eg.add(ToyNode::Op(0, vec![a]));
        eg.rebuild();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let got = ceg.add_colored(c, ToyNode::Op(0, vec![a]));
        assert_eq!(got, eg.find(fa), "form present in base returns the base class");
        assert!(got < eg.uf_len(), "no colored id allocated");
    }

    #[mz_ore::test]
    fn add_colored_respects_color_equalities() {
        // Under c, a≅b ⇒ Op(7,[a]) and Op(7,[b]) canonicalize to the same form
        // and dedup to one colored id.
        let (eg, a, b) = base_with_two_leaves();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        ceg.union(c, a, b);
        let na = ceg.add_colored(c, ToyNode::Op(7, vec![a]));
        let nb = ceg.add_colored(c, ToyNode::Op(7, vec![b]));
        assert_eq!(na, nb, "a≅b ⇒ Op(7,[a]) and Op(7,[b]) are the same colored node");
    }

    #[mz_ore::test]
    fn at_most_two_x_storage_bound() {
        // For N distinct colored Op nodes over base leaves, storage is N colored
        // ids (no duplication): the ≤2× bound (one base form + one colored form
        // per e-node) is respected — here there is no base form, so exactly N.
        let mut eg = EGraph::<ToyLang>::new();
        let leaves: Vec<Id> = (0..10).map(|k| eg.add(ToyNode::Leaf(k))).collect();
        eg.rebuild();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let mut ids = std::collections::HashSet::new();
        for &leaf in &leaves {
            ids.insert(ceg.add_colored(c, ToyNode::Op(9, vec![leaf])));
            // Re-adding the same node must not allocate again.
            ids.insert(ceg.add_colored(c, ToyNode::Op(9, vec![leaf])));
        }
        assert_eq!(ids.len(), leaves.len(), "no duplicate colored storage");
    }

    #[mz_ore::test]
    fn close_sees_delta_nodes() {
        // Two colored conclusions Op(5,[a]), Op(5,[b]); asserting a≅b must make
        // close() merge the two colored nodes by congruence.
        let (eg, a, b) = base_with_two_leaves();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let na = ceg.add_colored(c, ToyNode::Op(5, vec![a]));
        let nb = ceg.add_colored(c, ToyNode::Op(5, vec![b]));
        assert_ne!(ceg.find(c, na), ceg.find(c, nb), "distinct before a≅b");
        ceg.close(c, &[(a, b)]);
        assert_eq!(ceg.find(c, na), ceg.find(c, nb), "congruence merges colored nodes");
    }
}
