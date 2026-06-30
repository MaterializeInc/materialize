// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

#![allow(dead_code)] // Some items are test-only (toy module, parent_of introspection helper).

//! Production colored e-graph mechanism (Singher–Itzhaky; arXiv:2305.19203,
//! FMCAD'24 "Easter Egg") layered over the shared, immutable base
//! `core::EGraph<L>`. A color is a coarsening of the base congruence
//! (`≅ ⊆ ≅_c`): colors only add edges. Colors form a tree; a layered `find`
//! canonicalizes through the ancestor chain to black, then applies each color's
//! own delta. See `doc/developer/design/20260627_eqsat_colored_full_sp3b.md`.
//!
//! Components: [`union_find`] (sparse delta UF), the color tree + layered find
//! (this file, Task 2), `congruence` (Task 3), `conclusions` (Task 4),
//! `extract` (Task 5). [`toy`] carries the SP3a toy language + workload
//! generators; `oracle` (test-only) is the independent clones oracle.

pub(crate) mod toy;
pub(crate) mod union_find;

mod conclusions;
mod congruence;
mod extract;
pub(crate) mod saturate;
mod view;

// Re-export so the generated colored rule functions (in `crate::eqsat::rules`,
// which is not a descendant of `colored`) can name the view by a `pub(crate)`
// path.
pub(crate) use view::ColoredView;
// The relational extractor (`egraph::build`) picks the cheapest colored
// relational conclusion per class with this structural cost (SP4d T10).
pub(crate) use extract::TreeSizeColoredCost;
// The colored rule driver: referenced by test code via the short name, and by
// the engine (SP4d T11) via the full path `saturate::colored_saturate`.
#[cfg(test)]
pub(crate) use saturate::colored_saturate;
#[cfg(test)]
mod measure;
#[cfg(test)]
mod oracle;

use std::collections::HashMap;

use crate::eqsat::core::{EGraph, Language};
use crate::eqsat::colored::union_find::ColoredUnionFind;

// Re-export Id so the test's `use super::*` picks it up.
pub(crate) use crate::eqsat::core::Id;

/// Index into a [`ColoredEGraph`]'s color tree. Black (the base congruence) is
/// the implicit root every color chain bottoms out at; it is not a `ColorId`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColorId(pub(crate) usize);

/// One color's state: its parent in the tree, its own delta union-find over the
/// parent's partition, and its colored-conclusion hash-cons (Task 4).
#[derive(Debug)]
struct ColorData<L: Language> {
    /// Parent color; `None` ⇒ this color's parent is black (the base).
    parent: Option<ColorId>,
    /// This color's own delta over its parent's partition.
    uf: ColoredUnionFind,
    /// Colored e-nodes that exist only in this color, kept canonical (Task 4).
    delta_nodes: HashMap<L::Node, Id>,
}

/// A colored e-graph: one shared, immutable base + a tree of colors. Generic
/// over `L`; built on `core::EGraph<L>`. The base is borrowed, never copied.
pub(crate) struct ColoredEGraph<'b, L: Language> {
    base: &'b EGraph<L>,
    colors: Vec<ColorData<L>>,
    /// Allocator for colored e-node ids; starts at `base.uf_len()` so colored
    /// ids never alias base ids.
    next_colored_id: Id,
}

impl<'b, L: Language> ColoredEGraph<'b, L> {
    pub(crate) fn new(base: &'b EGraph<L>) -> Self {
        let next_colored_id = base.uf_len();
        ColoredEGraph {
            base,
            colors: Vec::new(),
            next_colored_id,
        }
    }

    /// The number of colors in the tree. Color ids are `ColorId(0..num_colors())`
    /// in parent-first (creation) order, the order the colored rule driver
    /// ([`crate::eqsat::colored::saturate`]) saturates them in.
    pub(crate) fn num_colors(&self) -> usize {
        self.colors.len()
    }

    /// The number of colored delta e-nodes owned directly by color `c` (its own
    /// conclusions, not its ancestors'). The colored rule driver uses this as a
    /// per-color analogue of the base `MAX_ENODES` budget.
    pub(crate) fn colored_node_count(&self, c: ColorId) -> usize {
        self.colors[c.0].delta_nodes.len()
    }

    /// Create a new color whose parent is `parent` (`None` ⇒ child of black).
    /// Colors are created parent-first, so the `colors` index order is a valid
    /// topological (pre-)order.
    pub(crate) fn new_color(&mut self, parent: Option<ColorId>) -> ColorId {
        let id = ColorId(self.colors.len());
        self.colors.push(ColorData {
            parent,
            uf: ColoredUnionFind::new(),
            delta_nodes: HashMap::new(),
        });
        id
    }

    /// The ancestor chain of `c`, top-down: the color whose parent is black
    /// first, `c` last.
    fn chain_top_down(&self, c: ColorId) -> Vec<ColorId> {
        let mut chain = Vec::new();
        let mut cur = Some(c);
        while let Some(cid) = cur {
            chain.push(cid);
            cur = self.colors[cid.0].parent;
        }
        chain.reverse();
        chain
    }

    /// Layered canonical id of `x` under color `c`: canonicalize through black,
    /// then apply each ancestor's and `c`'s own delta, top-down.
    pub(crate) fn find(&mut self, c: ColorId, x: Id) -> Id {
        // Black step: a base id canonicalizes through the base; a colored id is
        // its own black-level root.
        let mut r = if x < self.base.uf_len() {
            self.base.find(x)
        } else {
            x
        };
        for cid in self.chain_top_down(c) {
            r = self.colors[cid.0].uf.find_local(r);
        }
        r
    }

    /// The immediate parent color of `c`, or `None` if `c` is a direct child of
    /// black. Used to navigate the color hierarchy in tests and in the colored
    /// layer builder (which verifies the parent-first `ColorId` invariant).
    pub(crate) fn parent_of(&self, c: ColorId) -> Option<ColorId> {
        self.colors[c.0].parent
    }

    /// Assert `x ≅_c y` in color `c` (inherited by all descendants). Returns
    /// whether the two were distinct under `c`.
    pub(crate) fn union(&mut self, c: ColorId, x: Id, y: Id) -> bool {
        let rx = self.find(c, x);
        let ry = self.find(c, y);
        self.colors[c.0].uf.union_local(rx, ry)
    }

    /// True when color `c`'s own delta union-find holds no tracked entries.
    /// Used in tests to verify that `close` pruned all redundant local edges.
    #[cfg(test)]
    pub(crate) fn color_delta_is_empty(&self, c: ColorId) -> bool {
        self.colors[c.0].uf.local_delta_is_empty()
    }

    /// Number of ids currently tracked in color `c`'s own delta union-find.
    /// Used in tests to confirm a prune actually removed entries.
    #[cfg(test)]
    pub(crate) fn color_delta_tracked_count(&self, c: ColorId) -> usize {
        self.colors[c.0].uf.tracked_ids().count()
    }

    /// All base class ids in the same colored class as `x` under color `c`
    /// (i.e. layered `find(c, ·)` equal to `find(c, x)`). Returns canonical
    /// base ids, sorted ascending. Colored delta ids (≥ `base.uf_len()`) are not
    /// returned — SP4b's candidates are always base terms.
    pub(crate) fn colored_class_members(&mut self, c: ColorId, x: Id) -> Vec<Id> {
        let rep = self.find(c, x);
        // `class_ids()` returns only canonical class keys (the `classes` map is
        // keyed by representative after `rebuild()`), so a `base.find(y) == y`
        // filter would be a no-op. (M2)
        let mut out: Vec<Id> = self
            .base
            .class_ids()
            .into_iter()
            .filter(|&y| self.find(c, y) == rep)
            .collect();
        out.sort_unstable();
        out
    }
}

#[cfg(test)]
mod color_tree_tests {
    use super::*;
    use crate::eqsat::core::EGraph;
    use crate::eqsat::colored::toy::{ToyLang, ToyNode};

    /// a=Leaf(0), b=Leaf(1), x=Leaf(2), fa=Op(0,[a]), fb=Op(0,[b]).
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
    fn fresh_color_equals_base_partition() {
        let (eg, ids) = fixed_base();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        for &id in &ids {
            assert_eq!(ceg.find(c, id), eg.find(id), "fresh color matches base for {id}");
        }
    }

    #[mz_ore::test]
    fn union_merges_within_color() {
        let (eg, ids) = fixed_base();
        let [a, b, ..] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        assert_ne!(ceg.find(c, a), ceg.find(c, b));
        assert!(ceg.union(c, a, b));
        assert_eq!(ceg.find(c, a), ceg.find(c, b), "a≅b within color c");
    }

    #[mz_ore::test]
    fn child_inherits_parent_unions() {
        let (eg, ids) = fixed_base();
        let [a, b, x, ..] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let parent = ceg.new_color(None);
        ceg.union(parent, a, b);
        let child = ceg.new_color(Some(parent));
        // Child sees parent's a≅b for free...
        assert_eq!(ceg.find(child, a), ceg.find(child, b), "child inherits parent union");
        // ...and can add its own without affecting the parent.
        ceg.union(child, a, x);
        assert_eq!(ceg.find(child, a), ceg.find(child, x), "child has its own union");
        assert_ne!(ceg.find(parent, a), ceg.find(parent, x), "parent unaffected by child");
    }

    #[mz_ore::test]
    fn siblings_are_independent() {
        let (eg, ids) = fixed_base();
        let [a, b, x, ..] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let s1 = ceg.new_color(None);
        let s2 = ceg.new_color(None);
        ceg.union(s1, a, b);
        ceg.union(s2, a, x);
        assert_eq!(ceg.find(s1, a), ceg.find(s1, b));
        assert_ne!(ceg.find(s1, a), ceg.find(s1, x), "s1 does not see s2's union");
        assert_eq!(ceg.find(s2, a), ceg.find(s2, x));
        assert_ne!(ceg.find(s2, a), ceg.find(s2, b), "s2 does not see s1's union");
    }

    #[mz_ore::test]
    fn colored_class_members_groups_base_ids() {
        let (eg, ids) = fixed_base();
        let [a, b, x, ..] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        // Before any union, each base id is alone in its colored class.
        assert_eq!(ceg.colored_class_members(c, a), vec![a]);
        // After a≅b, both appear (sorted), and x is unaffected.
        ceg.union(c, a, b);
        let mut ab = vec![a, b];
        ab.sort();
        assert_eq!(ceg.colored_class_members(c, a), ab);
        assert_eq!(ceg.colored_class_members(c, b), ab);
        assert_eq!(ceg.colored_class_members(c, x), vec![x]);
    }
}
