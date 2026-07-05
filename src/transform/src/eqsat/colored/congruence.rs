// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Colored congruence closure: SP3a's `close_color` kernel generalized to the
//! sparse delta union-find, the color hierarchy, and colored delta nodes.
//! Congruence is closed incrementally (egg-style dirty-tracking, SP4d Task 9):
//! one pass builds a parent index + memo and seeds a worklist; thereafter only
//! the parents of merged classes are re-examined, to a fixpoint. Colors are
//! closed in tree pre-order so each child inherits its ancestors' induced
//! merges. In `debug`/`test` builds the result is checked against the old
//! full-rescan congruence (`close_congruence_fullpass`) for partition identity.

use std::collections::{BTreeMap, VecDeque};

use mz_ore::collections::HashMap as OreHashMap;

use crate::eqsat::colored::{ColorId, ColoredEGraph};
use crate::eqsat::core::{Id, Language};

/// Metrics recorded by one [`ColoredEGraph::close`] run (carried from SP3a).
#[derive(Clone, Debug, Default)]
pub(crate) struct ColorMetrics {
    /// Input equalities that actually merged two distinct colored classes.
    pub applied_equalities: usize,
    /// Congruence-cascade unions beyond `applied_equalities`.
    pub induced_merges: usize,
    /// Base roots whose colored root differs from their base root.
    pub delta_classes: usize,
    /// Base e-nodes whose colored canonical form differs from their base form.
    pub delta_nodes: usize,
    /// Closure fixpoint rounds.
    pub iters: usize,
}

impl<'b, L: Language> ColoredEGraph<'b, L> {
    /// Colored canonical form of `n` under color `c`: `n` with each child id
    /// resolved through `find(c, ·)`. Precomputes child reps so the closure
    /// handed to `map_children` is `Fn` (find is `&mut self`).
    pub(crate) fn canon(&mut self, c: ColorId, n: &L::Node) -> L::Node {
        let rep_of: OreHashMap<Id, Id> = L::children(n)
            .into_iter()
            .map(|ch| (ch, self.find(c, ch)))
            .collect();
        L::map_children(n, |ch| rep_of[&ch])
    }

    /// All e-nodes visible to color `c`: every base e-node (paired with its base
    /// class) plus every colored delta node of `c` and its ancestors (paired
    /// with its colored id).
    pub(crate) fn visible_nodes(&self, c: ColorId) -> Vec<(Id, L::Node)> {
        let mut out: Vec<(Id, L::Node)> = Vec::new();
        let base_roots = self.base_roots();
        for &id in &base_roots {
            for n in self.base.nodes(id) {
                out.push((id, n));
            }
        }
        for cid in self.chain_top_down(c) {
            for (n, &nid) in &self.colors[cid.0].delta_nodes {
                out.push((nid, n.clone()));
            }
        }
        out
    }

    /// Canonical base roots (a base id equal to its own base representative).
    fn base_roots(&self) -> Vec<Id> {
        self.base
            .class_ids()
            .into_iter()
            .filter(|&id| self.base.find(id) == id)
            .collect()
    }

    /// Re-key color `c`'s delta_nodes against the current colored partition
    /// (the ≤2× minimization). A no-op until colored conclusions exist (Task 4).
    fn recanonicalize_delta_nodes(&mut self, c: ColorId) {
        let old = std::mem::take(&mut self.colors[c.0].delta_nodes);
        let mut new: BTreeMap<L::Node, Id> = BTreeMap::new();
        for (n, id) in old {
            let cn = self.canon(c, &n);
            new.entry(cn).or_insert(id);
        }
        self.colors[c.0].delta_nodes = new;
    }

    /// Apply `equalities` in color `c`, then close congruence to a fixpoint over
    /// every e-node visible to `c`. Assumes `c`'s ancestors are already closed.
    pub(crate) fn close(&mut self, c: ColorId, equalities: &[(Id, Id)]) -> ColorMetrics {
        let mut applied_equalities = 0;
        for &(a, b) in equalities {
            if self.union(c, a, b) {
                applied_equalities += 1;
            }
        }

        // Snapshot `c`'s delta UF immediately after applying the equalities, so
        // the debug parity check can re-run the full-pass congruence from the
        // exact pre-closure state. (Ancestor UFs are only path-compressed by
        // `find`, which preserves their partitions, so they need no snapshot.)
        #[cfg(any(test, debug_assertions))]
        let pre_uf = self.colors[c.0].uf.clone();

        // Incremental dirty-tracking congruence closure (replaces the old
        // full-rescan fixpoint).
        let (induced_merges, iters) = self.close_congruence_incremental(c);

        // Behavior-identity safety net: in debug/test builds, independently run
        // the OLD full-pass congruence from the same pre-closure state and
        // assert it yields the same partition AND the same induced-merge count.
        // Release builds run only the incremental path.
        #[cfg(any(test, debug_assertions))]
        {
            let ids = self.parity_id_set(c);
            let inc_part: Vec<Id> = ids.iter().map(|&id| self.find(c, id)).collect();
            // Swap the pre-closure UF in, run the full pass, capture its
            // partition, then restore the incremental result.
            let inc_uf = std::mem::replace(&mut self.colors[c.0].uf, pre_uf);
            let fp_induced = self.close_congruence_fullpass(c);
            let fp_part: Vec<Id> = ids.iter().map(|&id| self.find(c, id)).collect();
            self.colors[c.0].uf = inc_uf;
            debug_assert!(
                partition_labels_equal(&inc_part, &fp_part),
                "incremental close partition differs from full-pass for color {c:?}"
            );
            debug_assert_eq!(
                induced_merges, fp_induced,
                "incremental induced_merges differ from full-pass for color {c:?}"
            );
        }

        // Delta metrics over the base, comparing colored vs base canonical forms.
        let base_roots = self.base_roots();
        let delta_classes = base_roots
            .iter()
            .filter(|&&id| self.find(c, id) != id)
            .count();
        let mut delta_nodes = 0;
        for &id in &base_roots {
            for n in self.base.nodes(id) {
                let base_form = L::map_children(&n, |ch| self.base.find(ch));
                let colored_form = self.canon(c, &n);
                if base_form != colored_form {
                    delta_nodes += 1;
                }
            }
        }

        // Pruning pass: drop local edges in `c`'s delta subsumed by the ancestor
        // chain, then remove any solo roots left behind.
        //
        // A tracked id `x` is "dead code" when the ancestor chain moves it
        // away from itself (`find_in_ancestors(c, x) != x`): the layered
        // `find(c, y)` reaches `c`'s UF only after the ancestor step, so `x`
        // is never looked up directly there — removing it is safe.
        //
        // After phase 1, any root whose class has been fully emptied (no
        // non-root still points to it) is a solo root: `find_local_c(r) == r`
        // whether or not `r` is tracked, so removing it is also safe.
        //
        // `find_in_ancestors` and `find_local_ref` both take `&self` so there
        // are no borrow conflicts with `c`'s own UF.
        {
            // Phase 1: remove ids the ancestor chain moves away from themselves.
            let tracked: Vec<Id> = self.colors[c.0].uf.tracked_ids().collect();
            let to_remove_p1: Vec<Id> = tracked
                .iter()
                .copied()
                .filter(|&x| self.find_in_ancestors(c, x) != x)
                .collect();
            for x in to_remove_p1 {
                self.colors[c.0].uf.remove(x);
            }

            // Phase 2: remove solo roots (roots with no remaining non-root
            // member pointing to them).
            let remaining: Vec<Id> = self.colors[c.0].uf.tracked_ids().collect();
            let solo_roots: Vec<Id> = remaining
                .iter()
                .copied()
                .filter(|&r| {
                    let uf = &self.colors[c.0].uf;
                    uf.find_local_ref(r) == r
                        && !remaining
                            .iter()
                            .any(|&other| other != r && uf.find_local_ref(other) == r)
                })
                .collect();
            for r in solo_roots {
                self.colors[c.0].uf.remove(r);
            }
        }

        // Recanonicalize delta-node keys AFTER pruning so they are keyed against
        // the final partition. Pruning can reassign representatives (dead-root /
        // live-child or solo-root cases); running recanonicalization before the
        // prune would leave keys stale once colored-conclusion delta nodes are
        // present (Task 10+). The prune logic reads only the union-find (via
        // `find_in_ancestors` / `find_local_ref`), not delta-node keys, so the
        // reorder is safe.
        self.recanonicalize_delta_nodes(c);

        ColorMetrics {
            applied_equalities,
            induced_merges,
            delta_classes,
            delta_nodes,
            iters,
        }
    }

    /// Incremental (egg-style) congruence closure over every e-node visible to
    /// `c`, assuming the equalities have already been applied to `c`'s delta UF.
    /// Returns `(induced_merges, iters)`.
    ///
    /// Algorithm:
    /// * **Pass A** builds a *parent index* once: for each visible node `p` and
    ///   each canonical child class `k` of `p`, record `p ∈ parents[k]` — the
    ///   nodes to re-examine when `k`'s class merges.
    /// * **Pass B** builds the global `memo: canon(c, node) → owner rep` over all
    ///   visible nodes, merging on every collision. This single pass detects all
    ///   *current* congruence violations — both those induced by the just-applied
    ///   equalities and any left by pre-`close` unions (e.g. remove-in-close),
    ///   so we need not assume the pre-state was already closed.
    /// * **Pass C** drains the dirty worklist (classes that just merged): for
    ///   each parent of a popped class it recomputes `canon`, re-hashconses into
    ///   `memo`, and merges (pushing the new class) on collision — to a fixpoint.
    ///
    /// On every successful union the loser's parent list is folded into the
    /// winner's, keeping `parents[k]` complete for the live representative `k`.
    /// Stale `memo` keys/values are harmless: no live node ever recomputes a key
    /// containing a non-representative child, and `merge` re-`find`s its operands
    /// so a stale value still resolves correctly.
    fn close_congruence_incremental(&mut self, c: ColorId) -> (usize, usize) {
        let visible = self.visible_nodes(c);
        let n = visible.len();
        let mut memo: OreHashMap<L::Node, Id> = OreHashMap::with_capacity(n);
        let mut parents: OreHashMap<Id, Vec<usize>> = OreHashMap::new();
        let mut dirty: VecDeque<Id> = VecDeque::new();
        let mut induced_merges = 0;

        // Pass A: parent index over canonical children.
        for (p, (_owner, node)) in visible.iter().enumerate() {
            let cn = self.canon(c, node);
            for ch in L::children(&cn) {
                parents.entry(ch).or_default().push(p);
            }
        }

        // Pass B: build memo, merging on every initial collision.
        for p in 0..n {
            let owner = visible[p].0;
            self.process_node(
                c,
                owner,
                &visible[p].1,
                &mut memo,
                &mut parents,
                &mut dirty,
                &mut induced_merges,
            );
        }

        // Pass C: propagate merges through parents to a fixpoint.
        let mut iters = 0;
        while let Some(d) = dirty.pop_front() {
            iters += 1;
            let plist = match parents.get(&d) {
                Some(v) => v.clone(),
                None => continue,
            };
            for p in plist {
                let owner = visible[p].0;
                self.process_node(
                    c,
                    owner,
                    &visible[p].1,
                    &mut memo,
                    &mut parents,
                    &mut dirty,
                    &mut induced_merges,
                );
            }
        }

        // `iters` counts the build pass plus every worklist round, a meaningful
        // measure of closure work (the old code counted full rescans).
        (induced_merges, iters + 1)
    }

    /// Re-hashcons a single visible node into `memo`: recompute its canonical
    /// form and owner rep, and either record it or (on collision with a
    /// different class) merge the two owners via [`Self::merge_in_close`].
    #[allow(clippy::too_many_arguments)]
    fn process_node(
        &mut self,
        c: ColorId,
        owner: Id,
        node: &L::Node,
        memo: &mut OreHashMap<L::Node, Id>,
        parents: &mut OreHashMap<Id, Vec<usize>>,
        dirty: &mut VecDeque<Id>,
        induced: &mut usize,
    ) {
        let cn = self.canon(c, node);
        let rep = self.find(c, owner);
        match memo.get(&cn) {
            Some(&other) => {
                self.merge_in_close(c, other, rep, parents, dirty, induced);
                let w = self.find(c, rep);
                memo.insert(cn, w);
            }
            None => {
                memo.insert(cn, rep);
            }
        }
    }

    /// Union the classes of `a` and `b` in color `c`, maintaining the dirty
    /// worklist and the parent index: on a successful merge the loser's parent
    /// list folds into the winner's and the winner is enqueued. Counts the merge
    /// in `induced`. No-op (uncounted) when the two are already in one class.
    fn merge_in_close(
        &mut self,
        c: ColorId,
        a: Id,
        b: Id,
        parents: &mut OreHashMap<Id, Vec<usize>>,
        dirty: &mut VecDeque<Id>,
        induced: &mut usize,
    ) {
        let ra = self.find(c, a);
        let rb = self.find(c, b);
        if ra == rb {
            return;
        }
        let did = self.union(c, ra, rb);
        debug_assert!(did, "distinct roots must union");
        *induced += 1;
        let w = self.find(c, ra);
        let l = if w == ra { rb } else { ra };
        if let Some(lp) = parents.remove(&l) {
            parents.entry(w).or_default().extend(lp);
        }
        dirty.push_back(w);
    }

    /// The old full-rescan congruence closure, retained as the debug/test parity
    /// oracle for [`Self::close_congruence_incremental`]. Assumes the equalities
    /// are already applied. Returns the induced-merge count.
    #[cfg(any(test, debug_assertions))]
    fn close_congruence_fullpass(&mut self, c: ColorId) -> usize {
        let visible = self.visible_nodes(c);
        let mut induced_merges = 0;
        loop {
            let mut memo: OreHashMap<L::Node, Id> = OreHashMap::new();
            let mut merged = false;
            for (owner, n) in &visible {
                let cn = self.canon(c, n);
                let rep = self.find(c, *owner);
                if let Some(&other) = memo.get(&cn) {
                    if self.union(c, other, rep) {
                        induced_merges += 1;
                        merged = true;
                    }
                } else {
                    memo.insert(cn, rep);
                }
            }
            if !merged {
                break;
            }
        }
        induced_merges
    }

    /// The set of ids whose colored partition the debug parity check compares:
    /// every visible-node owner plus every id tracked in `c`'s delta UF. Sorted
    /// and deduplicated.
    #[cfg(any(test, debug_assertions))]
    fn parity_id_set(&self, c: ColorId) -> Vec<Id> {
        let mut set: std::collections::BTreeSet<Id> = std::collections::BTreeSet::new();
        for (owner, _) in self.visible_nodes(c) {
            set.insert(owner);
        }
        for id in self.colors[c.0].uf.tracked_ids() {
            set.insert(id);
        }
        set.into_iter().collect()
    }

    /// Close colors in tree pre-order (ascending `ColorId`, which is parent-first
    /// because colors are created parent-first), so each child inherits its
    /// ancestors' induced merges. The base must be `rebuild()`-ed by the caller.
    pub(crate) fn close_all(
        &mut self,
        per_color: &[(ColorId, Vec<(Id, Id)>)],
    ) -> Vec<(ColorId, ColorMetrics)> {
        let mut order: Vec<&(ColorId, Vec<(Id, Id)>)> = per_color.iter().collect();
        order.sort_by_key(|(c, _)| c.0);
        let mut out = Vec::with_capacity(order.len());
        for (c, eqs) in order {
            let m = self.close(*c, eqs);
            out.push((*c, m));
        }
        out
    }

    /// Layered representative of `x` through all ancestors of `c` (stopping
    /// before `c` itself). Used to check whether `c`'s local edges are already
    /// provided by the ancestor chain. Takes `&self` (uses non-compressing
    /// find) to avoid borrow conflicts when `c`'s own UF is also in scope.
    fn find_in_ancestors(&self, c: ColorId, x: Id) -> Id {
        let mut r = if x < self.base.uf_len() {
            self.base.find(x)
        } else {
            x
        };
        for cid in self.chain_top_down(c) {
            if cid == c {
                break;
            }
            r = self.colors[cid.0].uf.find_local_ref(r);
        }
        r
    }
}

/// Whether two partition labelings over the same aligned id list induce the
/// same equivalence: a consistent bijection between the two label spaces. O(n).
#[cfg(any(test, debug_assertions))]
fn partition_labels_equal(a: &[Id], b: &[Id]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut fwd: OreHashMap<Id, Id> = OreHashMap::new();
    let mut bwd: OreHashMap<Id, Id> = OreHashMap::new();
    for (&x, &y) in a.iter().zip(b) {
        if *fwd.entry(x).or_insert(y) != y {
            return false;
        }
        if *bwd.entry(y).or_insert(x) != x {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::colored::oracle::{oracle_close, same_partition};
    use crate::eqsat::colored::toy::{ToyLang, ToyNode};
    use crate::eqsat::core::EGraph;

    /// The incremental `close` must reach the SAME partition as the old
    /// full-rescan congruence (`close_congruence_fullpass`) and as the
    /// independent differential oracle, on a congruence that requires MULTI-STEP
    /// propagation: with `f(f(x))` and `f(f(y))` present, merging `x ≅ y` must
    /// cascade through `f(x) ≅ f(y)` to `f(f(x)) ≅ f(f(y))`. The test is
    /// non-vacuous: it asserts those two derived merges actually happen, so a
    /// `close` that did nothing would fail.
    #[mz_ore::test]
    fn incremental_close_equals_full_pass() {
        let mut eg = EGraph::<ToyLang>::new();
        let x = eg.add(ToyNode::Leaf(0));
        let y = eg.add(ToyNode::Leaf(1));
        let fx = eg.add(ToyNode::Op(0, vec![x]));
        let fy = eg.add(ToyNode::Op(0, vec![y]));
        let ffx = eg.add(ToyNode::Op(0, vec![fx]));
        let ffy = eg.add(ToyNode::Op(0, vec![fy]));
        eg.rebuild();
        let ids = [x, y, fx, fy, ffx, ffy];

        // Incremental path (the production `close`).
        let mut inc = ColoredEGraph::new(&eg);
        let c = inc.new_color(None);
        let m = inc.close(c, &[(eg.find(x), eg.find(y))]);

        // Non-vacuous: the multi-step congruence merges actually happened.
        assert_eq!(
            inc.find(c, fx),
            inc.find(c, fy),
            "f(x)≅f(y) must be induced"
        );
        assert_eq!(
            inc.find(c, ffx),
            inc.find(c, ffy),
            "f(f(x))≅f(f(y)) must be induced (multi-step cascade)"
        );
        assert_ne!(inc.find(c, x), inc.find(c, fx), "x and f(x) stay distinct");
        assert_eq!(
            m.induced_merges, 2,
            "exactly two congruence merges: the f-level and the ff-level"
        );

        // Full-pass path on an independent colored e-graph: apply x≅y, then run
        // the OLD full-rescan congruence directly (not the incremental `close`).
        let mut fp = ColoredEGraph::new(&eg);
        let cf = fp.new_color(None);
        assert!(fp.union(cf, eg.find(x), eg.find(y)));
        let fp_induced = fp.close_congruence_fullpass(cf);
        assert_eq!(fp_induced, 2, "full-pass also induces two merges");

        // Independent differential oracle.
        let oracle = oracle_close(&eg, &[(eg.find(x), eg.find(y))], &[]);

        // All three agree pairwise over the full id set.
        for &i in &ids {
            for &j in &ids {
                let inc_same = inc.find(c, i) == inc.find(c, j);
                let fp_same = fp.find(cf, i) == fp.find(cf, j);
                let or_same = oracle.rep(i) == oracle.rep(j);
                assert_eq!(inc_same, fp_same, "incremental vs full-pass on ({i},{j})");
                assert_eq!(inc_same, or_same, "incremental vs oracle on ({i},{j})");
            }
        }
    }

    fn two_leaves() -> (EGraph<ToyLang>, Id, Id) {
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        eg.rebuild();
        (eg, a, b)
    }

    /// When a child color carries a local edge that an ancestor already provides,
    /// `close` must prune it.  We manufacture the redundancy explicitly: add the
    /// child edge BEFORE the parent is closed, then close the parent (making
    /// the child edge redundant), then call `close(child, [])` to trigger
    /// pruning.  Membership must be preserved and the child delta must be empty.
    #[mz_ore::test]
    fn remove_in_close_prunes_redundant_child_edge() {
        let (eg, a, b) = two_leaves();
        let mut ceg = ColoredEGraph::new(&eg);
        let parent = ceg.new_color(None);
        let child = ceg.new_color(Some(parent));
        // At this point parent has not closed a≅b, so the union actually adds
        // a local edge to child's delta.
        assert!(
            ceg.union(child, a, b),
            "child edge must be added before parent closes"
        );
        // Close the parent with the same equality — now parent provides a≅b.
        ceg.close(parent, &[(eg.find(a), eg.find(b))]);
        // The child's local edge is now redundant.  close(child) should prune it.
        ceg.close(child, &[]);
        // Membership is preserved (ancestor provides the merge).
        assert_eq!(
            ceg.find(child, eg.find(a)),
            ceg.find(child, eg.find(b)),
            "child membership must be preserved after pruning"
        );
        // The child's own delta carries no local edges after pruning.
        assert!(
            ceg.color_delta_is_empty(child),
            "child delta must be empty after redundant edge is pruned"
        );
    }

    /// When a child color asserts an equality the parent does NOT provide,
    /// `close` must keep that local edge (it is not redundant).
    #[mz_ore::test]
    fn close_keeps_non_redundant_child_edge() {
        let (eg, a, b) = two_leaves();
        let mut ceg = ColoredEGraph::new(&eg);
        let parent = ceg.new_color(None);
        ceg.close(parent, &[]); // parent provides no equalities
        let child = ceg.new_color(Some(parent));
        ceg.close(child, &[(eg.find(a), eg.find(b))]);
        // Membership is correct.
        assert_eq!(
            ceg.find(child, eg.find(a)),
            ceg.find(child, eg.find(b)),
            "child membership must hold"
        );
        // The child's delta is NOT empty — the edge belongs to child alone.
        assert!(
            !ceg.color_delta_is_empty(child),
            "non-redundant child edge must not be pruned"
        );
    }

    /// Pruning a redundant child-edge while a THIRD id is independently merged
    /// in the child must NOT split the 3-way class.
    ///
    /// Setup: three leaves `a`, `b`, `d`.
    ///   Parent provides `a ≅ b`.
    ///   Child (before parent closes) adds `a ≅ b` (redundant) AND `d ≅ a`
    ///   (child-only).  This gives child's local UF 3 tracked ids.
    ///
    /// After closing the parent and then the child:
    ///   Phase-1 pruning removes `b` (ancestor chain already maps b → a ≠ b).
    ///   `d ≅ a` is child-only and survives.
    ///   Child's delta shrinks from 3 → 2 tracked ids.
    ///
    /// The full 3-way partition {a, b, d} is verified via the differential oracle.
    #[mz_ore::test]
    fn remove_in_close_3id_preserves_3way_class() {
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        let d = eg.add(ToyNode::Leaf(2));
        eg.rebuild();

        let mut ceg = ColoredEGraph::new(&eg);
        let parent = ceg.new_color(None);
        let child = ceg.new_color(Some(parent));

        // Add both edges to child's local UF BEFORE the parent is closed, so
        // the child genuinely has entries for a, b, and d.
        assert!(
            ceg.union(child, a, b),
            "a≅b must be added before parent closes"
        );
        assert!(
            ceg.union(child, d, a),
            "d≅a is child-only and must also be added before parent closes"
        );

        // Confirm 3 ids are tracked in child's delta at this point.
        assert_eq!(
            ceg.color_delta_tracked_count(child),
            3,
            "child must track 3 ids (a, b, d) before the prune"
        );

        // Close parent with a≅b — this makes child's a≅b entry redundant.
        ceg.close(parent, &[(eg.find(a), eg.find(b))]);

        // Close child with no new equalities — Phase 1 prunes b (redundant),
        // keeps d≅a (child-only).
        ceg.close(child, &[]);

        // Confirm removal: child's delta shrank from 3 to 2.
        assert_eq!(
            ceg.color_delta_tracked_count(child),
            2,
            "b must have been pruned; child delta must shrink to 2 tracked ids"
        );

        // Child delta is NOT empty — d≅a survived.
        assert!(
            !ceg.color_delta_is_empty(child),
            "child delta must not be empty — the child-only d≅a edge must survive"
        );

        // Full 3-way differential oracle check: parent gives a≅b; child gave
        // both a≅b (now pruned) and d≅a.  Oracle receives the union of all
        // asserted equalities across the chain.
        let oracle = oracle_close(
            &eg,
            &[(eg.find(a), eg.find(b)), (eg.find(d), eg.find(a))],
            &[],
        );
        assert!(
            same_partition(&mut ceg, child, &oracle, &[a, b, d]),
            "3-way class {{a, b, d}} must be preserved after pruning b (oracle differential)"
        );
    }
}
