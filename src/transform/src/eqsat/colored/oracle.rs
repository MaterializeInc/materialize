// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Independent "clones" oracle: an all-pairs colored congruence over the base
//! id space + visible delta nodes, sharing no code with the sparse-UF /
//! layered-find mechanism under test. Ground truth for partition and (Task 5)
//! extraction. O(n²·iters); test-sized graphs only. Realizes the same semantic
//! model as SP3a's `close_color_by_copy`.

use std::collections::HashMap;

use crate::eqsat::colored::extract::CostModel;
use crate::eqsat::colored::{ColorId, ColoredEGraph};
use crate::eqsat::core::{EGraph, Id, Language};

/// The result of an oracle closure: a flat representative map over the relevant
/// id space, plus the visible `(owner, node)` pairs (for extraction in Task 5).
pub(crate) struct OracleColor<L: Language> {
    parent: HashMap<Id, Id>,
    pub(crate) nodes: Vec<(Id, L::Node)>,
}

impl<L: Language> OracleColor<L> {
    /// Canonical representative of `id` (non-compressing read).
    pub(crate) fn rep(&self, mut id: Id) -> Id {
        while let Some(&p) = self.parent.get(&id) {
            if p == id {
                break;
            }
            id = p;
        }
        id
    }

    fn union(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.rep(a), self.rep(b));
        if ra == rb {
            return false;
        }
        self.parent.insert(ra, rb);
        self.parent.insert(rb, rb);
        true
    }
}

/// Ground truth for a color: apply `equalities` and all-pairs congruence over the
/// base e-nodes + `delta_nodes`. `equalities` must be the union of the asserted
/// equalities along the color's chain (the test supplies them).
pub(crate) fn oracle_close<L: Language>(
    base: &EGraph<L>,
    equalities: &[(Id, Id)],
    delta_nodes: &[(Id, L::Node)],
) -> OracleColor<L> {
    let base_roots: Vec<Id> = base
        .class_ids()
        .into_iter()
        .filter(|&id| base.find(id) == id)
        .collect();
    let mut nodes: Vec<(Id, L::Node)> = Vec::new();
    for &id in &base_roots {
        for n in base.nodes(id) {
            nodes.push((id, n));
        }
    }
    nodes.extend(delta_nodes.iter().cloned());

    // Seed the rep map: base ids resolve to their base root; delta ids are their
    // own roots.
    let mut parent: HashMap<Id, Id> = HashMap::new();
    for id in 0..base.uf_len() {
        parent.insert(id, base.find(id));
    }
    for &(id, _) in delta_nodes {
        parent.entry(id).or_insert(id);
    }
    let mut oracle = OracleColor { parent, nodes };

    for &(a, b) in equalities {
        oracle.union(a, b);
    }
    // All-pairs congruence to a fixpoint.
    loop {
        let mut merged = false;
        let n = oracle.nodes.len();
        for i in 0..n {
            for j in (i + 1)..n {
                let fi = L::map_children(&oracle.nodes[i].1, |c| oracle.rep(c));
                let fj = L::map_children(&oracle.nodes[j].1, |c| oracle.rep(c));
                if fi == fj {
                    let (oi, oj) = (oracle.nodes[i].0, oracle.nodes[j].0);
                    if oracle.union(oi, oj) {
                        merged = true;
                    }
                }
            }
        }
        if !merged {
            break;
        }
    }
    oracle
}

/// Whether the colored mechanism and the oracle induce the same equivalence on
/// `ids` under color `c`.
pub(crate) fn same_partition<L: Language>(
    ceg: &mut ColoredEGraph<L>,
    c: ColorId,
    oracle: &OracleColor<L>,
    ids: &[Id],
) -> bool {
    for i in 0..ids.len() {
        for j in 0..ids.len() {
            let mech = ceg.find(c, ids[i]) == ceg.find(c, ids[j]);
            let orac = oracle.rep(ids[i]) == oracle.rep(ids[j]);
            if mech != orac {
                return false;
            }
        }
    }
    true
}

/// Independent least-cost extraction over the oracle's partition + visible nodes,
/// mirroring `extract_colored` with a from-scratch grouping. Returns cost per
/// oracle representative.
pub(crate) fn oracle_extract<L: Language, C: CostModel<L>>(
    oracle: &OracleColor<L>,
    model: &C,
) -> HashMap<Id, C::Cost> {
    let mut by_class: HashMap<Id, Vec<(L::Node, HashMap<Id, Id>)>> = HashMap::new();
    for (owner, n) in &oracle.nodes {
        let rep = oracle.rep(*owner);
        let cmap: HashMap<Id, Id> = L::children(n)
            .into_iter()
            .map(|ch| (ch, oracle.rep(ch)))
            .collect();
        by_class.entry(rep).or_default().push((n.clone(), cmap));
    }
    let mut best: HashMap<Id, C::Cost> = HashMap::new();
    loop {
        let mut changed = false;
        for (&cls, nodes) in &by_class {
            for (n, cmap) in nodes {
                if !cmap.values().all(|r| best.contains_key(r)) {
                    continue;
                }
                let cost = model.cost(n, &|ch| {
                    let r = cmap.get(&ch).copied().unwrap_or(ch);
                    best.get(&r).cloned().expect("child cost present")
                });
                if best.get(&cls).map_or(true, |c0| &cost < c0) {
                    best.insert(cls, cost);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::colored::toy::{gen_base, gen_colors, GenParams, Locality, ToyLang, ToyNode};

    fn fixed_base() -> (EGraph<ToyLang>, [Id; 5]) {
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        let x = eg.add(ToyNode::Leaf(2));
        let fa = eg.add(ToyNode::Op(0, vec![a]));
        let fb = eg.add(ToyNode::Op(0, vec![b]));
        eg.rebuild();
        (eg, [a, b, x, fa, fb])
    }

    #[mz_ore::test]
    fn close_no_equalities_is_base() {
        let (eg, ids) = fixed_base();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let m = ceg.close(c, &[]);
        for &id in &ids {
            assert_eq!(ceg.find(c, id), eg.find(id));
        }
        assert_eq!(m.applied_equalities, 0);
        assert_eq!(m.induced_merges, 0);
        assert_eq!(m.delta_classes, 0);
        assert_eq!(m.delta_nodes, 0);
    }

    #[mz_ore::test]
    fn close_leaf_equality_no_cascade() {
        let (eg, ids) = fixed_base();
        let [a, _b, x, ..] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let m = ceg.close(c, &[(a, x)]);
        assert_eq!(m.applied_equalities, 1);
        assert_eq!(m.induced_merges, 0, "leaf equality induces no congruence");
    }

    #[mz_ore::test]
    fn close_propagates_congruence() {
        let (eg, ids) = fixed_base();
        let [a, b, _x, fa, fb] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let m = ceg.close(c, &[(a, b)]);
        assert_eq!(ceg.find(c, fa), ceg.find(c, fb), "Op(a)≅Op(b) after a≅b");
        assert!(m.induced_merges >= 1);
        assert!(m.delta_nodes >= 1);
    }

    #[mz_ore::test]
    fn close_matches_oracle_fixed() {
        let (eg, ids) = fixed_base();
        let [a, b, x, fa, fb] = ids;
        let cases: &[&[(Id, Id)]] = &[&[], &[(a, b)], &[(a, x)], &[(a, b), (fa, fb)]];
        for eqs in cases {
            let mut ceg = ColoredEGraph::new(&eg);
            let c = ceg.new_color(None);
            ceg.close(c, eqs);
            let oracle = oracle_close(&eg, eqs, &[]);
            assert!(same_partition(&mut ceg, c, &oracle, &ids), "disagree on {eqs:?}");
        }
    }

    #[mz_ore::test]
    fn close_matches_oracle_random() {
        let p = GenParams {
            base_size: 60,
            fan_out: 2,
            depth: 3,
            n_colors: 8,
            eqs_per_color: 3,
            locality: Locality::Mixed,
        };
        for seed in 0..8u64 {
            let base = gen_base(&p, seed);
            let ids = base.class_ids();
            let colors = gen_colors(&p, &base, seed + 100);
            for eqs in &colors {
                let mut ceg = ColoredEGraph::new(&base);
                let c = ceg.new_color(None);
                ceg.close(c, eqs);
                let oracle = oracle_close(&base, eqs, &[]);
                assert!(
                    same_partition(&mut ceg, c, &oracle, &ids),
                    "disagree: seed {seed}, eqs {eqs:?}"
                );
            }
        }
    }

    #[mz_ore::test]
    fn close_matches_oracle_hierarchy() {
        // Two-level chain: parent gets eqs_p, child gets eqs_c. The oracle for
        // the child uses the UNION of both equality sets (the chain).
        let p = GenParams {
            base_size: 60,
            fan_out: 2,
            depth: 3,
            n_colors: 2,
            eqs_per_color: 3,
            locality: Locality::Mixed,
        };
        for seed in 0..8u64 {
            let base = gen_base(&p, seed);
            let ids = base.class_ids();
            let colors = gen_colors(&p, &base, seed + 100);
            let (eqs_p, eqs_c) = (&colors[0], &colors[1]);

            let mut ceg = ColoredEGraph::new(&base);
            let parent = ceg.new_color(None);
            let child = ceg.new_color(Some(parent));
            ceg.close(parent, eqs_p);
            ceg.close(child, eqs_c);

            let mut chain: Vec<(Id, Id)> = eqs_p.clone();
            chain.extend(eqs_c.iter().copied());
            let oracle = oracle_close(&base, &chain, &[]);
            assert!(
                same_partition(&mut ceg, child, &oracle, &ids),
                "hierarchy disagree: seed {seed}"
            );
        }
    }
}
