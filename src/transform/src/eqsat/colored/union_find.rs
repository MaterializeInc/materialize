// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A sparse, path-compressed, union-by-size union-find storing only the
//! black-roots a single color has merged. A missing key means "this id is its
//! own representative" — the color inherits the base/parent partition there.
//! SP3b replaces SP3a's dense `Vec`-backed `ColoredUf` with this.

use std::collections::HashMap;

use crate::eqsat::core::Id;

#[derive(Clone, Debug, Default)]
pub(crate) struct ColoredUnionFind {
    parent: HashMap<Id, Id>,
    size: HashMap<Id, usize>,
}

impl ColoredUnionFind {
    pub(crate) fn new() -> Self {
        ColoredUnionFind::default()
    }

    /// Local representative of `id` within this color's delta. Path-compresses.
    /// `id` must already be canonicalized through any ancestor chain.
    pub(crate) fn find_local(&mut self, id: Id) -> Id {
        // Find the root (a self-loop or an absent key).
        let mut root = id;
        while let Some(&p) = self.parent.get(&root) {
            if p == root {
                break;
            }
            root = p;
        }
        // Path-compress: point everything on the path directly at the root.
        let mut cur = id;
        while let Some(&p) = self.parent.get(&cur) {
            if p == cur {
                break;
            }
            self.parent.insert(cur, root);
            cur = p;
        }
        root
    }

    /// Union the classes of `a` and `b` (union-by-size). Returns whether they
    /// were distinct. Roots are stored as explicit self-loops.
    pub(crate) fn union_local(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.find_local(a), self.find_local(b));
        if ra == rb {
            return false;
        }
        let sa = *self.size.get(&ra).unwrap_or(&1);
        let sb = *self.size.get(&rb).unwrap_or(&1);
        let (win, lose) = if sa >= sb { (ra, rb) } else { (rb, ra) };
        self.parent.insert(win, win);
        self.parent.insert(lose, win);
        self.size.insert(win, sa + sb);
        self.size.remove(&lose);
        true
    }

    /// Non-compressing representative of `id` within this color's delta.
    /// Equivalent to [`find_local`] but takes `&self` so it can be called while
    /// another field of the same `ColoredEGraph` is already borrowed `&mut`.
    /// Used in the `close` pruning pass to check the ancestor chain without
    /// path-compressing it.
    pub(crate) fn find_local_ref(&self, id: Id) -> Id {
        let mut cur = id;
        loop {
            match self.parent.get(&cur) {
                Some(&p) if p != cur => cur = p,
                _ => break,
            }
        }
        cur
    }

    /// Iterator over all ids currently tracked in this color's delta (ids that
    /// have an explicit entry in the parent map).
    pub(crate) fn tracked_ids(&self) -> impl Iterator<Item = Id> + '_ {
        self.parent.keys().copied()
    }

    /// True when no ids are tracked (the color's local delta is empty).
    /// Used by `ColoredEGraph::color_delta_is_empty` in tests.
    #[cfg(test)]
    pub(crate) fn local_delta_is_empty(&self) -> bool {
        self.parent.is_empty()
    }

    /// Remove `id` as a tracked element: its direct children reattach so the
    /// remaining members stay together, and `id` reverts to its own rep. Used to
    /// drop a color-local edge a coarser layer (black/parent) already provides.
    pub(crate) fn remove(&mut self, id: Id) {
        let kids: Vec<Id> = self
            .parent
            .iter()
            .filter(|&(&k, &v)| v == id && k != id)
            .map(|(&k, _)| k)
            .collect();
        // Does `id` have a real parent (i.e. is it a non-root)?
        let parent = self.parent.get(&id).copied().filter(|&p| p != id);
        match parent {
            Some(p) => {
                // Non-root: reattach children above `id` to `id`'s parent's root.
                let target = self.find_local(p);
                for k in kids {
                    self.parent.insert(k, target);
                }
            }
            None => {
                // Root (or untracked): promote the first child as the new root.
                if let Some((&first, rest)) = kids.split_first() {
                    self.parent.insert(first, first);
                    for &k in rest {
                        self.parent.insert(k, first);
                    }
                    if let Some(s) = self.size.remove(&id) {
                        self.size.insert(first, s.saturating_sub(1).max(1));
                    }
                }
            }
        }
        self.parent.remove(&id);
        self.size.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn fresh_is_identity() {
        let mut uf = ColoredUnionFind::new();
        assert_eq!(uf.find_local(7), 7, "untracked id is its own rep");
    }

    #[mz_ore::test]
    fn union_then_find_agree() {
        let mut uf = ColoredUnionFind::new();
        assert!(uf.union_local(1, 2), "union of distinct returns true");
        assert_eq!(uf.find_local(1), uf.find_local(2), "1 and 2 share a rep");
        assert!(!uf.union_local(1, 2), "re-union returns false");
    }

    #[mz_ore::test]
    fn union_by_size_attaches_smaller_under_larger() {
        let mut uf = ColoredUnionFind::new();
        // Build a 3-element class {1,2,3}, then union the singleton {4}.
        uf.union_local(1, 2);
        uf.union_local(1, 3);
        let big_root = uf.find_local(1);
        uf.union_local(4, 1);
        assert_eq!(
            uf.find_local(4),
            big_root,
            "singleton joins the larger class's root"
        );
    }

    #[mz_ore::test]
    fn remove_detaches_element() {
        let mut uf = ColoredUnionFind::new();
        uf.union_local(1, 2);
        uf.remove(2);
        assert_eq!(
            uf.find_local(2),
            2,
            "removed element reverts to its own rep"
        );
        assert_eq!(uf.find_local(1), 1, "remaining element keeps a valid rep");
    }

    #[mz_ore::test]
    fn remove_keeps_other_members_together() {
        let mut uf = ColoredUnionFind::new();
        // {1,2,3}; remove 1 (which may be the root): 2 and 3 must stay together.
        uf.union_local(1, 2);
        uf.union_local(1, 3);
        uf.remove(1);
        assert_eq!(
            uf.find_local(2),
            uf.find_local(3),
            "remaining members stay merged"
        );
        assert_eq!(
            uf.find_local(1),
            1,
            "removed element reverts to its own rep"
        );
    }
}
