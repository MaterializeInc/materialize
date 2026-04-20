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

use columnar::{Borrow, Columnar};
use differential_dataflow::difference::Semigroup;
use timely::container::{ContainerBuilder, PushInto};

use super::KVUpdates;
use super::batcher::freeze_into_aligned;
use super::column::FactColumn;
use crate::columnar::Column;

/// Flush threshold, in flat `((K, V), T, R)` tuples, before the chunker
/// sorts+consolidates `pending` and emits a trie chunk.
///
/// Computed from [`super::batcher::TARGET_WORDS`] (2 MiB) so a flush produces
/// roughly one "target-sized" chunk after dedup; we size the pending buffer at
/// 2× that to amortize the sort cost. For small types this yields tens of
/// thousands of tuples per flush; for Row-sized types, a few thousand.
#[inline]
fn pending_flush_target<K, V, T, R>() -> usize {
    const TARGET_BYTES: usize = super::batcher::TARGET_WORDS * 8;
    let size = std::mem::size_of::<((K, V), T, R)>();
    let per_chunk = if size == 0 {
        TARGET_BYTES
    } else {
        std::cmp::max(1, TARGET_BYTES / size)
    };
    2 * per_chunk
}

/// A [`ContainerBuilder`] that assembles sorted, consolidated [`FactColumn`]
/// chunks from unsorted `Vec<((K, V), T, R)>` (or [`Column`]) inputs.
///
/// Each `flush_pending` sorts+consolidates the staging buffer, forms a
/// deduplicated trie in the reusable `work` buffer, then freezes it to a
/// [`FactColumn::Align`] (serialized aligned bytes) — mirroring
/// [`crate::columnar::ColumnBuilder`]'s aligned-freeze pattern. `work` is
/// cleared (not replaced) after each freeze to preserve its backing
/// allocations for the next flush.
pub struct FactTrieChunker<K, V, T, R>
where
    K: Columnar + Ord + Clone,
    V: Columnar + Ord + Clone,
    T: Columnar + Ord + Clone,
    R: Columnar + Semigroup + Clone,
{
    /// Unsorted staging buffer for incoming tuples.
    pending: Vec<((K, V), T, R)>,
    /// Reusable trie buffer — `form`ed into then frozen out each flush.
    work: KVUpdates<K, V, T, R>,
    /// Frozen chunks waiting to be extracted.
    ready: VecDeque<FactColumn<K, V, T, R>>,
    /// Parking spot for the most recently-returned chunk (owned by the caller
    /// via `std::mem::take`, but must live behind `&mut` until then).
    empty: FactColumn<K, V, T, R>,
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
            work: Default::default(),
            ready: VecDeque::new(),
            empty: FactColumn::Typed(Default::default()),
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
    /// Sort+consolidate `pending`, form a trie, freeze to a
    /// [`FactColumn::Align`], and push to `ready`.
    ///
    /// After this call `pending` is empty and `work` is cleared (allocations
    /// reused). Does nothing if `pending` is empty or fully consolidates away.
    fn flush_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        differential_dataflow::consolidation::consolidate_updates(&mut self.pending);
        if self.pending.is_empty() {
            return;
        }
        self.work.clear();
        form_into::<K, V, T, R, _, _, _, _>(
            &mut self.work,
            self.pending.iter().map(|((k, v), t, r)| (k, v, (t, r))),
        );
        self.pending.clear();
        self.ready.push_back(freeze_into_aligned(&mut self.work));
    }
}

/// Build a [`KVUpdates`] trie in-place from a sorted iterator.
///
/// Like [`KVUpdates::form`] but writes into an existing (possibly dirty) trie
/// that has just been `clear()`ed, so the backing `Vec`s retain their
/// capacities. `output` must be empty on entry (post-clear).
fn form_into<K, V, T, R, AR, BR, TR, DR>(
    output: &mut KVUpdates<K, V, T, R>,
    sorted: impl Iterator<Item = (AR, BR, (TR, DR))>,
) where
    K: Columnar,
    V: Columnar,
    T: Columnar,
    R: Columnar,
    AR: Copy + Eq,
    BR: Copy + Eq,
    TR: Copy,
    DR: Copy,
    columnar::ContainerOf<K>: columnar::Push<AR>,
    columnar::ContainerOf<V>: columnar::Push<BR>,
    columnar::ContainerOf<T>: columnar::Push<TR>,
    columnar::ContainerOf<R>: columnar::Push<DR>,
{
    use columnar::Push;
    use mz_ore::cast::CastFrom;

    let mut sorted = sorted.peekable();
    if let Some((a, b, (t, r))) = sorted.next() {
        let mut prev_a = a;
        let mut prev_b = b;
        Push::push(&mut output.lists.values, a);
        Push::push(&mut output.rest.lists.values, b);
        Push::push(&mut output.rest.rest.values.0, t);
        Push::push(&mut output.rest.rest.values.1, r);

        for (a, b, (t, r)) in sorted {
            if a != prev_a {
                Push::push(
                    &mut output.rest.rest.bounds,
                    u64::cast_from(columnar::Len::len(&output.rest.rest.values.0.borrow())),
                );
                Push::push(
                    &mut output.rest.lists.bounds,
                    u64::cast_from(columnar::Len::len(&output.rest.lists.values.borrow())),
                );
                Push::push(&mut output.lists.values, a);
                Push::push(&mut output.rest.lists.values, b);
            } else if b != prev_b {
                Push::push(
                    &mut output.rest.rest.bounds,
                    u64::cast_from(columnar::Len::len(&output.rest.rest.values.0.borrow())),
                );
                Push::push(&mut output.rest.lists.values, b);
            }
            Push::push(&mut output.rest.rest.values.0, t);
            Push::push(&mut output.rest.rest.values.1, r);
            prev_a = a;
            prev_b = b;
        }

        // Seal all open bounds.
        Push::push(
            &mut output.rest.rest.bounds,
            u64::cast_from(columnar::Len::len(&output.rest.rest.values.0.borrow())),
        );
        Push::push(
            &mut output.rest.lists.bounds,
            u64::cast_from(columnar::Len::len(&output.rest.lists.values.borrow())),
        );
        Push::push(
            &mut output.lists.bounds,
            u64::cast_from(columnar::Len::len(&output.lists.values.borrow())),
        );
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
        if self.pending.len() >= pending_flush_target::<K, V, T, R>() {
            self.flush_pending();
        }
    }
}

/// Accept a columnar `Column` wire container and drain it into `pending`.
///
/// Each columnar item is decoded into an owned `((K, V), T, R)` tuple and
/// appended to `pending`. The ensuing consolidation (sort + dedup + zero-drop)
/// is handled by [`FactTrieChunker::flush_pending`], matching the `Vec` path.
///
/// This impl lets the factorized batcher pipeline consume the same columnar
/// wire format used by the existing `Col2ValBatcher`, avoiding the need to
/// materialize a `Vec<((K, V), T, R)>` before exchange.
impl<K, V, T, R> PushInto<&mut Column<((K, V), T, R)>> for FactTrieChunker<K, V, T, R>
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
    fn push_into(&mut self, input: &mut Column<((K, V), T, R)>) {
        use columnar::Len;
        let borrowed = input.borrow();
        let len = Len::len(&borrowed);
        self.pending.reserve(len);
        for idx in 0..len {
            let ((k_ref, v_ref), t_ref, r_ref) = columnar::Index::get(&borrowed, idx);
            let k = K::into_owned(k_ref);
            let v = V::into_owned(v_ref);
            let t = T::into_owned(t_ref);
            let r = R::into_owned(r_ref);
            self.pending.push(((k, v), t, r));
        }
        if self.pending.len() >= pending_flush_target::<K, V, T, R>() {
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
    type Container = FactColumn<K, V, T, R>;

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
    use columnar::{Index, Len};

    use super::super::child_range;

    type TestChunker = FactTrieChunker<u64, u64, u64, i64>;

    fn collect_chunk(chunk: &FactColumn<u64, u64, u64, i64>) -> Vec<(u64, u64, u64, i64)> {
        let mut out = Vec::new();
        let b = chunk.borrow();
        for outer in 0..Len::len(&b.lists) {
            for key_idx in child_range(b.lists.bounds, outer) {
                let k = *Index::get(&b.lists.values, key_idx);
                for val_idx in child_range(b.rest.lists.bounds, key_idx) {
                    let v = *Index::get(&b.rest.lists.values, val_idx);
                    for l_idx in child_range(b.rest.rest.bounds, val_idx) {
                        let t = *Index::get(&b.rest.rest.values.0, l_idx);
                        let d = *Index::get(&b.rest.rest.values.1, l_idx);
                        out.push((k, v, t, d));
                    }
                }
            }
        }
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
        // pending below flush threshold, nothing extracted yet.
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
        let b = chunk.borrow();
        assert_eq!(Len::len(&b.lists), 1);
        assert_eq!(Len::len(&b.rest.lists), 1);
        assert_eq!(Len::len(&b.rest.rest.values.0), 500);
    }

    #[mz_ore::test]
    fn test_chunker_overflow_emits_early() {
        // Push enough to trigger an early flush.
        let threshold = pending_flush_target::<u64, u64, u64, i64>();
        let mut input: Vec<((u64, u64), u64, i64)> = (0..(threshold + 10))
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
