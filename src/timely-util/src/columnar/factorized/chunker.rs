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

//! A container builder that produces factorized trie chunks.
//!
//! [`FactTrieChunker`] accepts unsorted `Vec<((K, V), T, R)>` input, consolidates
//! updates (sort + accumulate diffs + drop zeros), and emits [`KVUpdates`] chunks
//! built via [`KVUpdates::form`]. The factorization (key/value deduplication)
//! happens at chunk production time, not downstream in the builder.

use std::collections::VecDeque;

use columnar::Columnar;
use differential_dataflow::difference::Semigroup;
use timely::container::{ContainerBuilder, PushInto};

use super::KVUpdates;

/// Target leaf count for an emitted chunk.
///
/// Picked to match the default power-of-two capacity DD uses for `Vec<(D,T,R)>`,
/// so allocation behavior resembles the baseline `ColumnationChunker` pipeline.
const CHUNK_TARGET: usize = 1024;

/// A [`ContainerBuilder`] that assembles sorted, consolidated [`KVUpdates`] chunks
/// from unsorted `Vec<((K, V), T, R)>` inputs.
///
/// The chunker buffers tuples in `pending`. When enough tuples accumulate, it
/// sorts and consolidates them, then calls [`KVUpdates::form`] to produce a
/// fully-deduplicated trie chunk. Each emitted chunk is a self-contained sorted
/// `KVUpdates` — successive chunks within a chain are also globally sorted by
/// `(K, V, T)`.
pub struct FactTrieChunker<K, V, T, R>
where
    K: Columnar + Ord + Clone,
    V: Columnar + Ord + Clone,
    T: Columnar + Ord + Clone,
    R: Columnar + Semigroup + Clone,
{
    /// Unsorted staging buffer.
    pending: Vec<((K, V), T, R)>,
    /// Consolidated trie chunks waiting to be extracted.
    ready: VecDeque<KVUpdates<K, V, T, R>>,
    /// Parking spot for the most recently-returned chunk (owned by the caller
    /// via `std::mem::take`, but must live behind `&mut` until then).
    empty: KVUpdates<K, V, T, R>,
}

impl<K, V, T, R> Default for FactTrieChunker<K, V, T, R>
where
    K: Columnar + Ord + Clone,
    V: Columnar + Ord + Clone,
    T: Columnar + Ord + Clone,
    R: Columnar + Semigroup + Clone,
{
    fn default() -> Self {
        Self {
            pending: Vec::new(),
            ready: VecDeque::new(),
            empty: Default::default(),
        }
    }
}

impl<K, V, T, R> FactTrieChunker<K, V, T, R>
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
    /// Sort+consolidate `pending` and emit a single trie chunk to `ready`.
    ///
    /// After this call `pending` is empty. Does nothing if `pending` is empty.
    fn flush_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        differential_dataflow::consolidation::consolidate_updates(&mut self.pending);
        if self.pending.is_empty() {
            return;
        }
        let storage =
            KVUpdates::<K, V, T, R>::form(self.pending.iter().map(|((k, v), t, r)| (k, v, (t, r))));
        self.pending.clear();
        self.ready.push_back(storage);
    }
}

impl<K, V, T, R> PushInto<&mut Vec<((K, V), T, R)>> for FactTrieChunker<K, V, T, R>
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
    fn push_into(&mut self, input: &mut Vec<((K, V), T, R)>) {
        self.pending.append(input);
        if self.pending.len() >= 2 * CHUNK_TARGET {
            self.flush_pending();
        }
    }
}

impl<K, V, T, R> ContainerBuilder for FactTrieChunker<K, V, T, R>
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
    type Container = KVUpdates<K, V, T, R>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        let ready = self.ready.pop_front()?;
        self.empty = ready;
        Some(&mut self.empty)
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.flush_pending();
        let ready = self.ready.pop_front()?;
        self.empty = ready;
        Some(&mut self.empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use columnar::{Borrow, Len};

    type TestChunker = FactTrieChunker<u64, u64, u64, i64>;

    fn collect_chunk(chunk: &KVUpdates<u64, u64, u64, i64>) -> Vec<(u64, u64, u64, i64)> {
        let mut out = Vec::new();
        chunk.for_each_cursor(|k, v, (t, d)| {
            out.push((*k, *v, *t, *d));
        });
        out
    }

    #[mz_ore::test]
    fn test_chunker_single_push_flush() {
        let mut chunker = TestChunker::default();
        chunker.push_into(&mut vec![
            ((2u64, 20u64), 200u64, 1i64),
            ((1, 10), 100, 1),
            ((1, 10), 100, 2),
        ]);
        // pending < 2*CHUNK_TARGET, nothing extracted yet.
        assert!(chunker.extract().is_none());

        let chunk = chunker.finish().expect("finish yields chunk");
        let collected = collect_chunk(chunk);
        // Consolidated: (1,10,100,3) + (2,20,200,1).
        assert_eq!(collected, vec![(1, 10, 100, 3), (2, 20, 200, 1)]);
        assert!(chunker.finish().is_none());
    }

    #[mz_ore::test]
    fn test_chunker_consolidation_drops_zero() {
        let mut chunker = TestChunker::default();
        chunker.push_into(&mut vec![((1u64, 10u64), 100u64, 1i64), ((1, 10), 100, -1)]);
        // Both entries cancel out → no chunk.
        assert!(chunker.finish().is_none());
    }

    #[mz_ore::test]
    fn test_chunker_key_val_dedup() {
        // High dedup: one key, one val, many times.
        let mut input: Vec<((u64, u64), u64, i64)> =
            (0u64..500).map(|t| ((1u64, 10u64), t, 1i64)).collect();
        let mut chunker = TestChunker::default();
        chunker.push_into(&mut input);
        let chunk = chunker.finish().expect("finish yields chunk");

        // One key, one val, 500 updates.
        assert_eq!(Len::len(&chunk.lists.values.borrow()), 1);
        assert_eq!(Len::len(&chunk.rest.lists.values.borrow()), 1);
        assert_eq!(chunk.len(), 500);
    }

    #[mz_ore::test]
    fn test_chunker_overflow_emits_early() {
        // Push enough to trigger an early flush.
        let mut input: Vec<((u64, u64), u64, i64)> = (0..(2 * CHUNK_TARGET + 10))
            .map(|i| ((i as u64, 0u64), 0u64, 1i64))
            .collect();
        let mut chunker = TestChunker::default();
        chunker.push_into(&mut input);
        // Should have one chunk ready before finish.
        let first = chunker.extract().map(|c| collect_chunk(c));
        assert!(first.is_some(), "expected an early-flushed chunk");
        // finish() yields nothing else (flush emits a single chunk for all pending).
        assert!(chunker.finish().is_none());
    }
}
