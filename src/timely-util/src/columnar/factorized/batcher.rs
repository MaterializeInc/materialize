// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A chunk-level merger that preserves factorization across merge-batcher stages.
//!
//! [`FactTrieInternalMerger`] implements
//! [`differential_dataflow::trace::implementations::merge_batcher::Merger`]
//! with [`KVUpdates`] as the chunk type. It merges two sorted chains of trie
//! chunks pairwise at the key/val granularity, producing new trie chunks that
//! are themselves key/val-deduplicated. [`extract`] partitions merged chunks
//! into ready/kept halves based on the seal frontier.
//!
//! The merger does **not** advance update times; time compaction is a batch-level
//! concern applied inside [`crate::columnar::factorized::batch::FactMerger`].

use std::marker::PhantomData;
use std::ops::Range;

use columnar::{Borrow, Columnar, Index, Len};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::trace::implementations::merge_batcher::Merger;
use mz_ore::cast::CastFrom;
use timely::PartialOrder;
use timely::progress::frontier::{Antichain, AntichainRef};

use super::{KVUpdates, child_range};

/// Compute the val index range for `key_idx` within a `KVUpdates` chunk.
///
/// Free function rather than a method because [`KVUpdates`] is a type alias
/// and cannot carry inherent impls.
#[inline]
fn val_range_in_chunk<K: Columnar, V: Columnar, T: Columnar, R: Columnar>(
    chunk: &KVUpdates<K, V, T, R>,
    key_idx: usize,
) -> Range<usize> {
    child_range(chunk.rest.lists.bounds.borrow(), key_idx)
}

/// Target leaf count for an emitted chunk — byte-based: 64 KiB / sizeof::<(T, R)>().
///
/// Replaces the old fixed `CHUNK_TARGET = 1024`. Matches DD's
/// `ColumnationChunker` 64-KiB heuristic so per-batcher RAM stays bounded
/// when many concurrent arrangements share the worker (e.g. one batcher per
/// source / cluster). Merge passes through `MergeBatcher` still amortize over
/// uniformly sized chunks.
#[inline]
fn chunk_target_leaves<T, R>() -> usize {
    const TARGET_BYTES: usize = 64 * 1024;
    let size = std::mem::size_of::<(T, R)>();
    if size == 0 {
        TARGET_BYTES
    } else {
        std::cmp::max(1, TARGET_BYTES / size)
    }
}

/// A [`Merger`] over factorized trie chunks.
///
/// `merge` streams over two sorted chains of `KVUpdates` chunks, deduplicating
/// keys/values and consolidating diffs for equal `(k, v, t)` triples. `extract`
/// splits chunks into `ship` (times not beyond `upper`) and `kept` (times beyond
/// `upper`), rebuilding factorized tries on both sides.
pub struct FactTrieInternalMerger<K, V, T, R> {
    _marker: PhantomData<(K, V, T, R)>,
}

impl<K, V, T, R> Default for FactTrieInternalMerger<K, V, T, R> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<K, V, T, R> Merger for FactTrieInternalMerger<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + PartialOrder + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Semigroup + Clone + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    type Chunk = KVUpdates<K, V, T, R>;
    type Time = T;

    fn merge(
        &mut self,
        list1: Vec<Self::Chunk>,
        list2: Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
        _stash: &mut Vec<Self::Chunk>,
    ) {
        let mut c1 = ChainCursor::<'_, K, V, T, R>::new(&list1);
        let mut c2 = ChainCursor::<'_, K, V, T, R>::new(&list2);
        let mut builder = TrieMergeBuilder::<K, V, T, R>::new(chunk_target_leaves::<T, R>());

        while let (Some(cur1), Some(cur2)) = (c1.peek(), c2.peek()) {
            let (src1, key1_ref, key1_idx) = cur1;
            let (src2, key2_ref, key2_idx) = cur2;

            use std::cmp::Ordering;
            match K::reborrow(key1_ref).cmp(&K::reborrow(key2_ref)) {
                Ordering::Less => {
                    let r1 = val_range_in_chunk::<K, V, T, R>(src1, key1_idx);
                    builder.copy_key_vals(src1, r1);
                    builder.finish_key(key1_ref);
                    c1.step_key();
                }
                Ordering::Greater => {
                    let r2 = val_range_in_chunk::<K, V, T, R>(src2, key2_idx);
                    builder.copy_key_vals(src2, r2);
                    builder.finish_key(key2_ref);
                    c2.step_key();
                }
                Ordering::Equal => {
                    let r1 = val_range_in_chunk::<K, V, T, R>(src1, key1_idx);
                    let r2 = val_range_in_chunk::<K, V, T, R>(src2, key2_idx);
                    builder.merge_key_vals(src1, r1, src2, r2);
                    builder.finish_key(key1_ref);
                    c1.step_key();
                    c2.step_key();
                }
            }
            builder.maybe_emit(output);
        }

        while let Some((src, key_ref, key_idx)) = c1.peek() {
            let r = val_range_in_chunk::<K, V, T, R>(src, key_idx);
            builder.copy_key_vals(src, r);
            builder.finish_key(key_ref);
            c1.step_key();
            builder.maybe_emit(output);
        }
        while let Some((src, key_ref, key_idx)) = c2.peek() {
            let r = val_range_in_chunk::<K, V, T, R>(src, key_idx);
            builder.copy_key_vals(src, r);
            builder.finish_key(key_ref);
            c2.step_key();
            builder.maybe_emit(output);
        }

        builder.finish(output);
    }

    fn extract(
        &mut self,
        merged: Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        readied: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        _stash: &mut Vec<Self::Chunk>,
    ) {
        let mut ready_builder = TrieMergeBuilder::<K, V, T, R>::new(chunk_target_leaves::<T, R>());
        let mut keep_builder = TrieMergeBuilder::<K, V, T, R>::new(chunk_target_leaves::<T, R>());

        for chunk in &merged {
            let key_count = Len::len(&chunk.lists.values.borrow());
            for key_idx in 0..key_count {
                let v_range = val_range_in_chunk::<K, V, T, R>(chunk, key_idx);
                let key_ref = chunk.lists.values.borrow().get(key_idx);

                // Split each val's update range by upper.
                for val_idx in v_range {
                    let val_ref = chunk.rest.lists.values.borrow().get(val_idx);
                    let upd_range = child_range(chunk.rest.rest.bounds.borrow(), val_idx);
                    let times = chunk.rest.rest.values.0.borrow();
                    let diffs = chunk.rest.rest.values.1.borrow();

                    // Partition into ready / keep. No time-wise compaction here;
                    // seal will produce a consolidated batch via FactBuilder.
                    let start_r = ready_builder.staging.len();
                    let start_k = keep_builder.staging.len();
                    for i in upd_range {
                        let t = T::into_owned(times.get(i));
                        let r = R::into_owned(diffs.get(i));
                        if upper.less_equal(&t) {
                            frontier.insert_with(&t, |t| t.clone());
                            keep_builder.staging.push((t, r));
                        } else {
                            ready_builder.staging.push((t, r));
                        }
                    }
                    if ready_builder.staging.len() > start_r {
                        ready_builder.seal_val_from_staging(val_ref);
                    }
                    if keep_builder.staging.len() > start_k {
                        keep_builder.seal_val_from_staging(val_ref);
                    }
                }
                ready_builder.finish_key(key_ref);
                keep_builder.finish_key(key_ref);
            }
            ready_builder.maybe_emit(readied);
            keep_builder.maybe_emit(kept);
        }

        ready_builder.finish(readied);
        keep_builder.finish(kept);
    }

    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
        (chunk.len(), 0, 0, 0)
    }
}

// -----------------------------------------------------------------------------
// ChainCursor
// -----------------------------------------------------------------------------

/// A cursor over a chain of `KVUpdates` chunks at key granularity.
///
/// Yields `(source_chunk, key_ref, key_idx_in_chunk)` triples in sorted key
/// order across all chunks in the chain. Advances chunk boundaries transparently.
struct ChainCursor<'a, K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    chunks: &'a [KVUpdates<K, V, T, R>],
    chunk_idx: usize,
    key_idx: usize,
}

impl<'a, K, V, T, R> ChainCursor<'a, K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'b> columnar::Ref<'b, K>: Ord + Copy,
    V: Columnar,
    T: Columnar,
    R: Columnar,
{
    fn new(chunks: &'a [KVUpdates<K, V, T, R>]) -> Self {
        let mut cursor = Self {
            chunks,
            chunk_idx: 0,
            key_idx: 0,
        };
        cursor.skip_empty();
        cursor
    }

    /// Skip past any empty leading chunks.
    fn skip_empty(&mut self) {
        while self.chunk_idx < self.chunks.len()
            && self.key_idx >= Len::len(&self.chunks[self.chunk_idx].lists.values.borrow())
        {
            self.chunk_idx += 1;
            self.key_idx = 0;
        }
    }

    /// Peek at the current `(chunk, key_ref, key_idx)`; `None` if drained.
    fn peek(&self) -> Option<(&'a KVUpdates<K, V, T, R>, columnar::Ref<'a, K>, usize)> {
        if self.chunk_idx >= self.chunks.len() {
            return None;
        }
        let chunk = &self.chunks[self.chunk_idx];
        if self.key_idx >= Len::len(&chunk.lists.values.borrow()) {
            return None;
        }
        let key_ref = chunk.lists.values.borrow().get(self.key_idx);
        Some((chunk, key_ref, self.key_idx))
    }

    fn step_key(&mut self) {
        self.key_idx += 1;
        self.skip_empty();
    }
}

// -----------------------------------------------------------------------------
// TrieMergeBuilder
// -----------------------------------------------------------------------------

/// Accumulates a sequence of `(k, v, updates)` items into trie chunks.
///
/// The caller drives the builder by repeatedly calling `copy_key_vals` or
/// `merge_key_vals` for each key, then `finish_key`, and finally
/// `maybe_emit` / `finish`. Chunks are emitted at key boundaries when the
/// leaf count exceeds the configured target.
struct TrieMergeBuilder<K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// The in-progress chunk.
    result: KVUpdates<K, V, T, R>,
    /// Staging buffer for consolidation (per-val).
    staging: Vec<(T, R)>,
    /// Number of vals pushed for the in-progress key (0 = no key open).
    vals_this_key: usize,
    /// Number of keys sealed into the in-progress chunk.
    keys_in_chunk: usize,
    /// Number of leaves sealed into the in-progress chunk.
    leaves_in_chunk: usize,
    /// Target leaf count before emitting a chunk.
    target: usize,
}

impl<K, V, T, R> TrieMergeBuilder<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Semigroup + Clone + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    fn new(target: usize) -> Self {
        Self {
            result: Default::default(),
            staging: Vec::new(),
            vals_this_key: 0,
            keys_in_chunk: 0,
            leaves_in_chunk: 0,
            target,
        }
    }

    /// Stash one val's `(t, r)` entries into `self.staging`.
    #[inline]
    fn stash_one(&mut self, source: &KVUpdates<K, V, T, R>, val_idx: usize) {
        let range = child_range(source.rest.rest.bounds.borrow(), val_idx);
        let times = source.rest.rest.values.0.borrow();
        let diffs = source.rest.rest.values.1.borrow();
        self.staging.reserve(range.len());
        self.staging
            .extend(range.map(|i| (T::into_owned(times.get(i)), R::into_owned(diffs.get(i)))));
    }

    /// Consolidate `self.staging` and, if non-empty, push a val entry
    /// `(v_ref, staged_updates)` to `self.result`. Clears staging.
    #[inline]
    fn seal_val_from_staging(&mut self, v_ref: columnar::Ref<'_, V>) -> bool {
        differential_dataflow::consolidation::consolidate(&mut self.staging);
        if self.staging.is_empty() {
            return false;
        }
        self.leaves_in_chunk += self.staging.len();
        self.vals_this_key += 1;
        for (t, r) in self.staging.drain(..) {
            columnar::Push::push(&mut self.result.rest.rest.values.0, &t);
            columnar::Push::push(&mut self.result.rest.rest.values.1, &r);
        }
        columnar::Push::push(
            &mut self.result.rest.rest.bounds,
            u64::cast_from(Len::len(&self.result.rest.rest.values.0.borrow())),
        );
        columnar::Push::push(&mut self.result.rest.lists.values, v_ref);
        true
    }

    /// Copy one key's vals from `source`'s val range into the builder.
    fn copy_key_vals(&mut self, source: &KVUpdates<K, V, T, R>, v_range: Range<usize>) {
        for val_idx in v_range {
            self.stash_one(source, val_idx);
            let v_ref = source.rest.lists.values.borrow().get(val_idx);
            self.seal_val_from_staging(v_ref);
        }
    }

    /// Merge two val ranges for matching keys. Equal `v` stashes both sources
    /// together before seal; unequal emits each independently.
    fn merge_key_vals(
        &mut self,
        src1: &KVUpdates<K, V, T, R>,
        r1: Range<usize>,
        src2: &KVUpdates<K, V, T, R>,
        r2: Range<usize>,
    ) {
        let mut i1 = r1.start;
        let mut i2 = r2.start;
        let e1 = r1.end;
        let e2 = r2.end;
        let vals1 = src1.rest.lists.values.borrow();
        let vals2 = src2.rest.lists.values.borrow();

        while i1 < e1 && i2 < e2 {
            let v1 = vals1.get(i1);
            let v2 = vals2.get(i2);
            use std::cmp::Ordering;
            match V::reborrow(v1).cmp(&V::reborrow(v2)) {
                Ordering::Less => {
                    self.stash_one(src1, i1);
                    self.seal_val_from_staging(v1);
                    i1 += 1;
                }
                Ordering::Greater => {
                    self.stash_one(src2, i2);
                    self.seal_val_from_staging(v2);
                    i2 += 1;
                }
                Ordering::Equal => {
                    self.stash_one(src1, i1);
                    self.stash_one(src2, i2);
                    self.seal_val_from_staging(v1);
                    i1 += 1;
                    i2 += 1;
                }
            }
        }
        while i1 < e1 {
            self.stash_one(src1, i1);
            self.seal_val_from_staging(vals1.get(i1));
            i1 += 1;
        }
        while i2 < e2 {
            self.stash_one(src2, i2);
            self.seal_val_from_staging(vals2.get(i2));
            i2 += 1;
        }
    }

    /// Seal the in-progress key: if any vals survived, push the key and its
    /// val-bounds to `self.result`. Otherwise this is a no-op.
    fn finish_key(&mut self, k_ref: columnar::Ref<'_, K>) {
        if self.vals_this_key == 0 {
            return;
        }
        columnar::Push::push(&mut self.result.lists.values, k_ref);
        columnar::Push::push(
            &mut self.result.rest.lists.bounds,
            u64::cast_from(Len::len(&self.result.rest.lists.values.borrow())),
        );
        self.keys_in_chunk += 1;
        self.vals_this_key = 0;
    }

    /// If enough leaves have accumulated, seal the chunk and push to `output`.
    fn maybe_emit(&mut self, output: &mut Vec<KVUpdates<K, V, T, R>>) {
        if self.leaves_in_chunk >= self.target {
            self.emit(output);
        }
    }

    /// Finalize current chunk (sealing outer bounds) and move it to `output`.
    fn emit(&mut self, output: &mut Vec<KVUpdates<K, V, T, R>>) {
        if self.keys_in_chunk == 0 {
            return;
        }
        columnar::Push::push(
            &mut self.result.lists.bounds,
            u64::cast_from(Len::len(&self.result.lists.values.borrow())),
        );
        let chunk = std::mem::take(&mut self.result);
        output.push(chunk);
        self.keys_in_chunk = 0;
        self.leaves_in_chunk = 0;
    }

    /// Flush any remaining accumulated state into `output`.
    fn finish(mut self, output: &mut Vec<KVUpdates<K, V, T, R>>) {
        // Any in-progress key should already have been finished via finish_key.
        debug_assert_eq!(
            self.vals_this_key, 0,
            "finish called with open key; caller must finish_key first"
        );
        self.emit(output);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::factorized::batch::FactBuilder;
    use differential_dataflow::trace::{
        Batch, BatchReader, Builder, Cursor, Description, Merger as _,
    };
    use timely::progress::Antichain;

    type TestMerger = FactTrieInternalMerger<u64, u64, u64, i64>;
    type KV = KVUpdates<u64, u64, u64, i64>;

    fn mk_chunk(data: &[(u64, u64, u64, i64)]) -> KV {
        KV::form(data.iter().map(|(k, v, t, d)| (k, v, (t, d))))
    }

    fn collect(chunk: &KV) -> Vec<(u64, u64, u64, i64)> {
        let mut out = Vec::new();
        chunk.for_each_cursor(|k, v, (t, d)| out.push((*k, *v, *t, *d)));
        out
    }

    fn collect_chain(chain: &[KV]) -> Vec<(u64, u64, u64, i64)> {
        chain.iter().flat_map(collect).collect()
    }

    #[mz_ore::test]
    fn test_merge_single_chunks_non_overlapping() {
        let list1 = vec![mk_chunk(&[(1, 10, 100, 1), (3, 30, 300, 1)])];
        let list2 = vec![mk_chunk(&[(5, 50, 400, 1), (7, 70, 500, 1)])];
        let mut out = Vec::new();
        let mut stash = Vec::new();
        TestMerger::default().merge(list1, list2, &mut out, &mut stash);
        assert_eq!(
            collect_chain(&out),
            vec![
                (1, 10, 100, 1),
                (3, 30, 300, 1),
                (5, 50, 400, 1),
                (7, 70, 500, 1),
            ]
        );
    }

    #[mz_ore::test]
    fn test_merge_overlapping_keys() {
        let list1 = vec![mk_chunk(&[(1, 10, 100, 1), (2, 20, 200, 1)])];
        let list2 = vec![mk_chunk(&[(1, 10, 300, 2), (3, 30, 400, 1)])];
        let mut out = Vec::new();
        let mut stash = Vec::new();
        TestMerger::default().merge(list1, list2, &mut out, &mut stash);
        assert_eq!(
            collect_chain(&out),
            vec![
                (1, 10, 100, 1),
                (1, 10, 300, 2),
                (2, 20, 200, 1),
                (3, 30, 400, 1),
            ]
        );
    }

    #[mz_ore::test]
    fn test_merge_consolidates_duplicates() {
        let list1 = vec![mk_chunk(&[(1, 10, 100, 3)])];
        let list2 = vec![mk_chunk(&[(1, 10, 100, -3)])];
        let mut out = Vec::new();
        let mut stash = Vec::new();
        TestMerger::default().merge(list1, list2, &mut out, &mut stash);
        assert!(collect_chain(&out).is_empty(), "diffs cancelled → empty");
    }

    #[mz_ore::test]
    fn test_merge_multichunk_chain() {
        let list1 = vec![mk_chunk(&[(1, 10, 100, 1)]), mk_chunk(&[(3, 30, 300, 1)])];
        let list2 = vec![mk_chunk(&[(2, 20, 200, 1)]), mk_chunk(&[(4, 40, 400, 1)])];
        let mut out = Vec::new();
        let mut stash = Vec::new();
        TestMerger::default().merge(list1, list2, &mut out, &mut stash);
        assert_eq!(
            collect_chain(&out),
            vec![
                (1, 10, 100, 1),
                (2, 20, 200, 1),
                (3, 30, 300, 1),
                (4, 40, 400, 1),
            ]
        );
    }

    /// Compare `FactTrieInternalMerger::merge` against `FactMerger::work` on the
    /// same data. Both must yield identical (k, v, t, d) sequences.
    #[mz_ore::test]
    fn test_merge_matches_fact_merger() {
        use crate::columnar::factorized::batch::{FactBatch, FactMerger};
        use timely::progress::frontier::AntichainRef;

        // Data for two chains.
        let a = [
            (1u64, 10u64, 100u64, 1i64),
            (1, 10, 200, 1),
            (2, 20, 100, 1),
            (3, 30, 300, 1),
        ];
        let b = [
            (1u64, 10u64, 150u64, 1i64),
            (1, 20, 100, -1),
            (2, 20, 100, 1),
            (4, 40, 400, 2),
        ];

        // Chain-merge.
        let list_a = vec![mk_chunk(&a)];
        let list_b = vec![mk_chunk(&b)];
        let mut out = Vec::new();
        TestMerger::default().merge(list_a, list_b, &mut out, &mut Vec::new());
        let chain_result = collect_chain(&out);

        // Reference via FactMerger.
        let batch_a = FactBatch {
            storage: mk_chunk(&a),
            description: Description::new(
                Antichain::from_elem(0u64),
                Antichain::from_elem(500u64),
                Antichain::from_elem(0u64),
            ),
            updates: a.len(),
        };
        let batch_b = FactBatch {
            storage: mk_chunk(&b),
            description: Description::new(
                Antichain::from_elem(500u64),
                Antichain::from_elem(1000u64),
                Antichain::from_elem(0u64),
            ),
            updates: b.len(),
        };
        let mut merger: FactMerger<u64, u64, u64, i64> =
            batch_a.begin_merge(&batch_b, AntichainRef::new(&[]));
        merger.work(&batch_a, &batch_b, &mut 1_000_000);
        let merged = merger.done();
        let mut expected = Vec::new();
        let mut cursor = merged.cursor();
        while cursor.key_valid(&merged) {
            while cursor.val_valid(&merged) {
                let k = *cursor.key(&merged);
                let v = *cursor.val(&merged);
                cursor.map_times(&merged, |t, d| expected.push((k, v, *t, *d)));
                cursor.step_val(&merged);
            }
            cursor.step_key(&merged);
        }

        assert_eq!(chain_result, expected);
    }

    /// `extract` splits updates by `upper`: times ≥ upper go to `kept`, rest to `readied`.
    #[mz_ore::test]
    fn test_extract_split_by_frontier() {
        let chunk = mk_chunk(&[
            (1, 10, 50, 1),
            (1, 10, 150, 1),
            (2, 20, 100, 1),
            (3, 30, 200, 1),
        ]);
        let mut merger = TestMerger::default();
        let upper = Antichain::from_elem(150u64);
        let mut frontier = Antichain::new();
        let mut readied = Vec::new();
        let mut kept = Vec::new();
        merger.extract(
            vec![chunk],
            upper.borrow(),
            &mut frontier,
            &mut readied,
            &mut kept,
            &mut Vec::new(),
        );

        assert_eq!(
            collect_chain(&readied),
            vec![(1, 10, 50, 1), (2, 20, 100, 1)]
        );
        assert_eq!(collect_chain(&kept), vec![(1, 10, 150, 1), (3, 30, 200, 1)]);
        assert!(frontier.less_equal(&150u64));
        assert!(frontier.less_equal(&200u64));
    }

    /// Merged output wired through `FactBuilder::seal` yields a correct batch.
    #[mz_ore::test]
    fn test_merger_output_goes_through_builder() {
        let list1 = vec![mk_chunk(&[(1, 10, 100, 1), (2, 20, 200, 1)])];
        let list2 = vec![mk_chunk(&[(1, 10, 300, 2)])];
        let mut out = Vec::new();
        TestMerger::default().merge(list1, list2, &mut out, &mut Vec::new());

        let batch = <FactBuilder<u64, u64, u64, i64> as Builder>::seal(
            &mut out,
            Description::new(
                Antichain::from_elem(0u64),
                Antichain::from_elem(1000u64),
                Antichain::from_elem(0u64),
            ),
        );
        let mut result = Vec::new();
        let mut cursor = batch.cursor();
        while cursor.key_valid(&batch) {
            while cursor.val_valid(&batch) {
                let k = *cursor.key(&batch);
                let v = *cursor.val(&batch);
                cursor.map_times(&batch, |t, d| result.push((k, v, *t, *d)));
                cursor.step_val(&batch);
            }
            cursor.step_key(&batch);
        }
        assert_eq!(
            result,
            vec![(1, 10, 100, 1), (1, 10, 300, 2), (2, 20, 200, 1),]
        );
    }
}
