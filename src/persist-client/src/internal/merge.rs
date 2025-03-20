// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::task::{JoinHandle, JoinHandleExt};
use std::fmt::{Debug, Formatter};
use std::mem;

/// A merge tree.
///
/// Invariants and guarantees:
/// - This structure preserves the order in which elements are `push`ed.
/// - Merging also preserves order: only adjacent elements will be merged together,
///   and the result will have the same place in the ordering as the input did.
/// - The tree will store at most `O(K log N)` elements at once, where `K` is the provided max len
///   and `N` is the number of elements pushed.
/// - `finish` will return at most `K` elements.
/// - The "depth" of the merge tree - the number of merges any particular element may undergo -
///   is `O(log N)`.
pub struct MergeTree<T> {
    pub(crate) max_len: usize,
    pub(crate) levels: Vec<Vec<T>>,
    merge_fn: Box<dyn Fn(Vec<T>) -> T + Sync + Send>,
}

impl<T: Debug> Debug for MergeTree<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self {
            max_len,
            levels,
            merge_fn: _,
        } = self;
        f.debug_struct("MergeTree")
            .field("max_len", max_len)
            .field("levels", levels)
            .finish_non_exhaustive()
    }
}

impl<T> MergeTree<T> {
    /// Create a new merge tree. `max_len` limits both the number of parts to keep at each level of
    /// the tree, and the number of parts that `Self::finish` will return... and if we exceed that
    /// limit, the provided `merge_fn` is used to combine adjacent elements together.
    pub fn new(max_len: usize, merge_fn: impl Fn(Vec<T>) -> T + Send + Sync + 'static) -> Self {
        let new = Self {
            max_len,
            levels: vec![vec![]],
            merge_fn: Box::new(merge_fn),
        };
        new.assert_invariants();
        new
    }

    /// Iterate over (references to) the parts in this tree in first-to-latest order.
    #[allow(unused)]
    pub fn iter(&self) -> impl Iterator<Item = &T> + DoubleEndedIterator {
        self.levels.iter().rev().flat_map(|l| l.iter())
    }

    /// Iterate over (mutable references to) the parts in this tree in first-to-latest order.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> + DoubleEndedIterator {
        self.levels.iter_mut().rev().flat_map(|l| l.iter_mut())
    }

    /// Push a new part onto the end of this tree, possibly triggering a merge.
    pub fn push(&mut self, mut part: T) {
        // Normally, all levels have strictly less than max_len elements.
        // However, the _deepest_ level is allowed to have exactly max_len elements,
        // since that can save us an unnecessary merge in some cases.
        // (For example, when precisely max_len elements are added.)
        if let Some(last) = self.levels.last_mut() {
            if last.len() == self.max_len {
                let merged = (self.merge_fn)(mem::take(last));
                self.levels.push(vec![merged]);
            }
        }

        // At this point, all levels have room. Add our new part, then continue
        // merging up the tree until either there's still room in the current level
        // or we've reached the top.
        let max_level = self.levels.len() - 1;
        for depth in 0..=max_level {
            let level = &mut self.levels[depth];
            level.push(part);

            if level.len() < self.max_len || depth == max_level {
                break;
            }

            part = (self.merge_fn)(mem::take(level));
        }
    }

    /// Return the contents of this merge tree, flattened into at most `max_len` parts.
    pub fn finish(self) -> Vec<T> {
        self.levels
            .into_iter()
            .reduce(|mut shallower, mut deeper| {
                if shallower.len() + deeper.len() <= self.max_len {
                    // Optimization: if there's enough room in the next level for everything at the
                    // current level, add it directly.
                    deeper.append(&mut shallower);
                } else {
                    // Otherwise, merge this up as if it were a full level.
                    let merged = (self.merge_fn)(shallower);
                    deeper.push(merged);
                }
                deeper
            })
            .expect("non-empty level array")
    }

    pub(crate) fn assert_invariants(&self) {
        assert!(self.max_len >= 2, "max_len must be at least 2");

        let (deepest, shallow) = self.levels.split_last().expect("non-empty level array");
        for (depth, level) in shallow.iter().enumerate() {
            assert!(
                level.len() < self.max_len,
                "strictly less than max elements at level {depth}"
            );
        }
        assert!(
            deepest.len() <= self.max_len,
            "at most max elements at deepest level"
        );
    }
}

/// Either a handle to a task that returns a value or the value itself.
#[derive(Debug)]
pub enum Pending<T> {
    Writing(JoinHandle<T>),
    Blocking,
    Finished(T),
}

impl<T: Send + 'static> Pending<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self::Writing(handle)
    }

    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Finished(_))
    }

    pub async fn into_result(self) -> T {
        match self {
            Pending::Writing(h) => h.wait_and_assert_finished().await,
            Pending::Blocking => panic!("block_until_ready cancelled?"),
            Pending::Finished(t) => t,
        }
    }

    pub async fn block_until_ready(&mut self) {
        let pending = mem::replace(self, Self::Blocking);
        let value = pending.into_result().await;
        *self = Pending::Finished(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn test_merge_tree() {
        // Exhaustively test the merge tree for small sizes.
        for max_len in 2..8 {
            for items in 0..100 {
                let mut merge_tree = MergeTree::new(max_len, |vals: Vec<Vec<usize>>| {
                    // Merge sequences by concatenation.
                    vals.into_iter().flatten().collect()
                });
                for i in 0..items {
                    merge_tree.push(vec![i]);
                    assert!(
                        merge_tree.iter().flatten().copied().eq(0..=i),
                        "no parts should be lost"
                    );
                    merge_tree.assert_invariants();
                }
                let parts = merge_tree.finish();
                assert!(
                    parts.len() <= max_len,
                    "no more than {max_len} finished parts"
                );
                assert!(parts.into_iter().flatten().eq(0..items), "no parts lost");
            }
        }
    }
}
