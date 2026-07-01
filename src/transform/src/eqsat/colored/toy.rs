// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Toy k-ary expression language and seeded synthetic-workload generators for
//! the colored e-graph tests and the measurement harness. Carried from the SP3a
//! spike unchanged.

use std::collections::HashMap;

use crate::eqsat::colored::extract::CostModel;
use crate::eqsat::core::{EGraph, Id, Language};

/// A toy k-ary expression language for the spike. Leaves carry a distinct tag;
/// operators carry an op-code and child ids.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub(crate) enum ToyNode {
    Leaf(u32),
    Op(u16, Vec<Id>),
}

/// Operator-symbol buckets for [`ToyNode`].
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum ToySym {
    Leaf,
    Op(u16),
}

pub(crate) struct ToyLang;

impl Language for ToyLang {
    type Node = ToyNode;
    type Sym = ToySym;
    type GraphData = ();

    fn children(node: &ToyNode) -> Vec<Id> {
        match node {
            ToyNode::Leaf(_) => vec![],
            ToyNode::Op(_, ch) => ch.clone(),
        }
    }

    fn map_children(node: &ToyNode, f: impl Fn(Id) -> Id) -> ToyNode {
        match node {
            ToyNode::Leaf(t) => ToyNode::Leaf(*t),
            ToyNode::Op(c, ch) => ToyNode::Op(*c, ch.iter().map(|&x| f(x)).collect()),
        }
    }

    fn symbol(node: &ToyNode) -> ToySym {
        match node {
            ToyNode::Leaf(_) => ToySym::Leaf,
            ToyNode::Op(c, _) => ToySym::Op(*c),
        }
    }
}

/// A deterministic SplitMix64 PRNG (no `rand` dependency; reproducible).
#[derive(Debug)]
pub(crate) struct Lcg {
    state: u64,
}

impl Lcg {
    pub(crate) fn new(seed: u64) -> Self {
        Lcg { state: seed }
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n` (n must be > 0).
    pub(crate) fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % (n as u64)) as usize
    }
}

/// Where a color's asserted equalities land: best case (leaves) → relational
/// worst case (high-fan-out shared classes).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Locality {
    LeafOnly,
    Mixed,
    SharedHot,
}

/// Synthetic-workload knobs for the spike.
#[derive(Clone, Debug)]
pub(crate) struct GenParams {
    pub base_size: usize,
    pub fan_out: usize,
    pub depth: usize,
    pub n_colors: usize,
    pub eqs_per_color: usize,
    pub locality: Locality,
}

/// In-degree (number of e-nodes referencing each class as a child) per base root.
pub(crate) fn indegree(base: &EGraph<ToyLang>) -> HashMap<Id, usize> {
    let mut deg: HashMap<Id, usize> = HashMap::new();
    for id in base.class_ids() {
        for n in base.nodes(id) {
            for c in ToyLang::children(&n) {
                *deg.entry(base.find(c)).or_insert(0) += 1;
            }
        }
    }
    deg
}

/// Build a base e-graph with the requested shape (deterministic given `seed`).
pub(crate) fn gen_base(p: &GenParams, seed: u64) -> EGraph<ToyLang> {
    let mut eg = EGraph::<ToyLang>::new();
    let mut rng = Lcg::new(seed);

    let n_leaves = (p.base_size / (p.depth + 1)).max(p.fan_out + 1);
    let mut all: Vec<Id> = Vec::new();
    for k in 0..n_leaves {
        all.push(eg.add(ToyNode::Leaf(k as u32)));
    }
    // Recent ids, to bias new operators toward forming depth.
    let mut frontier: Vec<Id> = all.clone();

    // Guard against pathological non-growth (all-duplicate hash-cons hits).
    let max_attempts = p.base_size.saturating_mul(20).max(1000);
    let mut attempts = 0;
    while eg.node_count() < p.base_size && attempts < max_attempts {
        attempts += 1;
        let mut children = Vec::with_capacity(p.fan_out.max(1));
        for _ in 0..p.fan_out.max(1) {
            let use_frontier = rng.below(2) == 0 && !frontier.is_empty();
            let pool = if use_frontier { &frontier } else { &all };
            children.push(pool[rng.below(pool.len())]);
        }
        let id = eg.add(ToyNode::Op(0, children));
        all.push(id);
        frontier.push(id);
        if frontier.len() > n_leaves {
            frontier.remove(0);
        }
    }
    eg.rebuild();
    eg
}

/// Generate `n_colors` equality sets over `base`'s classes (deterministic).
pub(crate) fn gen_colors(p: &GenParams, base: &EGraph<ToyLang>, seed: u64) -> Vec<Vec<(Id, Id)>> {
    let mut rng = Lcg::new(seed ^ 0x00C0_FFEE);
    let mut roots: Vec<Id> = base
        .class_ids()
        .into_iter()
        .filter(|&id| base.find(id) == id)
        .collect();
    roots.sort_unstable(); // Ensure deterministic order regardless of HashMap iteration order.

    // Classes that contain a leaf e-node.
    let leaves: Vec<Id> = roots
        .iter()
        .copied()
        .filter(|&id| base.nodes(id).iter().any(|n| matches!(n, ToyNode::Leaf(_))))
        .collect();

    // Classes ranked by in-degree, descending; take the top quartile as "hot".
    let deg = indegree(base);
    let mut ranked: Vec<Id> = roots.clone();
    ranked.sort_by(|a, b| {
        deg.get(b)
            .unwrap_or(&0)
            .cmp(deg.get(a).unwrap_or(&0))
            .then(a.cmp(b))
    });
    let hot_n = (ranked.len() / 4).max(1);
    let hot: Vec<Id> = ranked.into_iter().take(hot_n).collect();

    // Pick the candidate pool for this locality; fall back to all roots if empty.
    let pool: &[Id] = match p.locality {
        Locality::LeafOnly if !leaves.is_empty() => &leaves,
        Locality::SharedHot if !hot.is_empty() => &hot,
        _ => &roots,
    };

    let mut colors = Vec::with_capacity(p.n_colors);
    for _ in 0..p.n_colors {
        let mut eqs = Vec::with_capacity(p.eqs_per_color);
        for _ in 0..p.eqs_per_color {
            let a = pool[rng.below(pool.len())];
            let b = pool[rng.below(pool.len())];
            eqs.push((a, b));
        }
        colors.push(eqs);
    }
    colors
}

/// Tree-size cost over [`ToyLang`]: a leaf costs 1; an operator costs 1 + the
/// sum of its children's costs. The toy cost used to validate extraction.
#[derive(Debug)]
pub(crate) struct ToyCost;

impl CostModel<ToyLang> for ToyCost {
    type Cost = u64;

    fn cost(&self, node: &ToyNode, child_cost: &dyn Fn(Id) -> u64) -> u64 {
        match node {
            ToyNode::Leaf(_) => 1,
            ToyNode::Op(_, children) => 1 + children.iter().map(|&c| child_cost(c)).sum::<u64>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_params() -> GenParams {
        GenParams {
            base_size: 80,
            fan_out: 3,
            depth: 3,
            n_colors: 5,
            eqs_per_color: 3,
            locality: Locality::Mixed,
        }
    }

    #[mz_ore::test]
    fn gen_base_is_deterministic() {
        let p = small_params();
        let a = gen_base(&p, 7);
        let b = gen_base(&p, 7);
        assert_eq!(
            a.node_count(),
            b.node_count(),
            "same seed ⇒ same node count"
        );
        // Same seed ⇒ same class structure: compare sorted node sets per partition size.
        assert_eq!(
            a.class_ids().len(),
            b.class_ids().len(),
            "same seed ⇒ same class count"
        );
    }

    #[mz_ore::test]
    fn gen_base_reaches_target_size() {
        let p = small_params();
        let eg = gen_base(&p, 1);
        assert!(
            eg.node_count() >= p.base_size,
            "base reaches target node count"
        );
    }

    #[mz_ore::test]
    fn gen_colors_shape() {
        let p = small_params();
        let base = gen_base(&p, 1);
        let colors = gen_colors(&p, &base, 2);
        assert_eq!(colors.len(), p.n_colors, "one equality set per color");
        for eqs in &colors {
            assert_eq!(eqs.len(), p.eqs_per_color, "eqs_per_color pairs per color");
            for &(a, b) in eqs {
                assert!(a < base.uf_len() && b < base.uf_len(), "ids in range");
            }
        }
    }

    #[mz_ore::test]
    fn gen_colors_is_deterministic() {
        // gen_colors must produce the same output on every call with the same seed,
        // regardless of HashMap iteration order. This test must FAIL before the
        // roots.sort_unstable() fix and PASS after.
        let p = small_params();
        let base = gen_base(&p, 42);
        let colors_a = gen_colors(&p, &base, 99);
        let colors_b = gen_colors(&p, &base, 99);
        assert_eq!(
            colors_a, colors_b,
            "gen_colors must be deterministic given the same seed"
        );
    }

    #[mz_ore::test]
    fn locality_steers_toward_shared_classes() {
        // SharedHot should, on average, touch classes of higher in-degree than
        // LeafOnly on the same base.
        let base_p = GenParams {
            n_colors: 30,
            eqs_per_color: 4,
            ..small_params()
        };
        let base = gen_base(&base_p, 1);
        let deg = super::indegree(&base);
        let avg_deg = |colors: &Vec<Vec<(Id, Id)>>| -> f64 {
            let mut sum = 0usize;
            let mut n = 0usize;
            for eqs in colors {
                for &(a, b) in eqs {
                    sum += *deg.get(&base.find(a)).unwrap_or(&0);
                    sum += *deg.get(&base.find(b)).unwrap_or(&0);
                    n += 2;
                }
            }
            sum as f64 / n.max(1) as f64
        };
        let leaf = gen_colors(
            &GenParams {
                locality: Locality::LeafOnly,
                ..base_p.clone()
            },
            &base,
            2,
        );
        let hot = gen_colors(
            &GenParams {
                locality: Locality::SharedHot,
                ..base_p.clone()
            },
            &base,
            2,
        );
        assert!(
            avg_deg(&hot) > avg_deg(&leaf),
            "SharedHot ({}) must touch higher-degree classes than LeafOnly ({})",
            avg_deg(&hot),
            avg_deg(&leaf)
        );
    }
}
