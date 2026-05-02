// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::task::JoinHandle;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::{Deref, DerefMut};

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
    /// Configuration: the largest any level in the tree is allowed to grow.
    max_level_len: usize,
    /// The length of each level in the tree, stored in order from shallowest to deepest.
    level_lens: Vec<usize>,
    /// A flattened representation of the contents of the tree, stored in order from earliest /
    /// deepest to newest / shallowest.
    data: Vec<T>,
    merge_fn: Box<dyn Fn(Vec<T>) -> T + Sync + Send>,
}

impl<T: Debug> Debug for MergeTree<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self {
            max_level_len,
            level_lens,
            data,
            merge_fn: _,
        } = self;
        f.debug_struct("MergeTree")
            .field("max_level_len", max_level_len)
            .field("level_lens", level_lens)
            .field("data", data)
            .finish_non_exhaustive()
    }
}

impl<T> MergeTree<T> {
    /// Create a new merge tree. `max_len` limits both the number of parts to keep at each level of
    /// the tree, and the number of parts that `Self::finish` will return... and if we exceed that
    /// limit, the provided `merge_fn` is used to combine adjacent elements together.
    pub fn new(max_len: usize, merge_fn: impl Fn(Vec<T>) -> T + Send + Sync + 'static) -> Self {
        let new = Self {
            max_level_len: max_len,
            level_lens: vec![0],
            data: vec![],
            merge_fn: Box::new(merge_fn),
        };
        new.assert_invariants();
        new
    }

    fn merge_last(&mut self, level_len: usize) {
        let offset = self.data.len() - level_len;
        let split = self.data.split_off(offset);
        let merged = (self.merge_fn)(split);
        self.data.push(merged);
    }

    /// Push a new part onto the end of this tree, possibly triggering a merge.
    pub fn push(&mut self, part: T) {
        // Normally, all levels have strictly less than max_len elements.
        // However, the _deepest_ level is allowed to have exactly max_len elements,
        // since that can save us an unnecessary merge in some cases.
        // (For example, when precisely max_len elements are added.)
        if let Some(last_len) = self.level_lens.last_mut() {
            if *last_len == self.max_level_len {
                let len = mem::take(last_len);
                self.merge_last(len);
                self.level_lens.push(1);
            }
        }

        // At this point, all levels have room. Add our new part, then continue
        // merging up the tree until either there's still room in the current level
        // or we've reached the top.
        self.data.push(part);

        let max_level = self.level_lens.len() - 1;
        for depth in 0..=max_level {
            let level_len = &mut self.level_lens[depth];
            *level_len += 1;

            if *level_len < self.max_level_len || depth == max_level {
                break;
            }

            let len = mem::take(level_len);
            self.merge_last(len);
        }
    }

    /// Return the contents of this merge tree, flattened into at most `max_len` parts.
    pub fn finish(mut self) -> Vec<T> {
        let mut tail_len = 0;
        for level_len in mem::take(&mut self.level_lens) {
            if tail_len + level_len <= self.max_level_len {
                // Optimization: we can combine the current level with the last level without
                // going over our limit.
                tail_len += level_len;
            } else {
                // Otherwise, perform the merge and start a new tail.
                self.merge_last(tail_len);
                tail_len = level_len + 1
            }
        }
        assert!(self.data.len() <= self.max_level_len);
        self.data
    }

    pub(crate) fn assert_invariants(&self) {
        assert!(self.max_level_len >= 2, "max_len must be at least 2");

        assert_eq!(
            self.data.len(),
            self.level_lens.iter().copied().sum::<usize>(),
            "level sizes should sum to overall len"
        );
        let (deepest_len, shallow) = self.level_lens.split_last().expect("non-empty level array");
        for (depth, level_len) in shallow.iter().enumerate() {
            assert!(
                *level_len < self.max_level_len,
                "strictly less than max elements at level {depth}"
            );
        }
        assert!(
            *deepest_len <= self.max_level_len,
            "at most max elements at deepest level"
        );
    }
}

impl<T> Deref for MergeTree<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &*self.data
    }
}

impl<T> DerefMut for MergeTree<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.data
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
            Pending::Writing(h) => h.await,
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
    use mz_ore::cast::CastLossy;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn test_merge_tree() {
        // Exhaustively test the merge tree for small sizes.
        struct Value {
            merge_depth: usize,
            elements: Vec<i64>,
        }

        for max_len in 2..8 {
            for items in 0..100 {
                let mut merge_tree = MergeTree::new(max_len, |vals: Vec<Value>| {
                    // Merge sequences by concatenation.
                    Value {
                        merge_depth: vals.iter().map(|v| v.merge_depth).max().unwrap_or(0) + 1,
                        elements: vals.into_iter().flat_map(|e| e.elements).collect(),
                    }
                });
                for i in 0..items {
                    merge_tree.push(Value {
                        merge_depth: 0,
                        elements: vec![i],
                    });
                    assert!(
                        merge_tree
                            .iter()
                            .flat_map(|v| v.elements.iter())
                            .copied()
                            .eq(0..=i),
                        "no parts should be lost"
                    );
                    merge_tree.assert_invariants();
                }
                let parts = merge_tree.finish();
                assert!(
                    parts.len() <= max_len,
                    "no more than {max_len} finished parts"
                );

                // We want our merged tree to be "balanced".
                // If we have 2^N elements in a binary tree, we want the depth to be N;
                // and more generally, we want a depth of N for a K-ary tree with K^N elements...
                // which is to say, a depth of log_K N for a tree with N elements.
                let expected_merge_depth =
                    usize::cast_lossy(f64::cast_lossy(items).log(f64::cast_lossy(max_len)).floor());
                for part in &parts {
                    assert!(
                        part.merge_depth <= expected_merge_depth,
                        "expected at most {expected_merge_depth} merges for a tree \
                        with max len {max_len} and {items} elements, but got {}",
                        part.merge_depth
                    );
                }
                assert!(
                    parts
                        .iter()
                        .flat_map(|v| v.elements.iter())
                        .copied()
                        .eq(0..items),
                    "no parts lost"
                );
            }
        }
    }
}
