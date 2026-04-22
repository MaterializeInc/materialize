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

use std::cmp::Ordering;
use std::collections::VecDeque;

use columnar::{Borrow, Columnar};
use differential_dataflow::difference::Semigroup;
use timely::container::{ContainerBuilder, PushInto};

use super::KVUpdates;
use super::batcher::freeze_into_aligned;
use super::column::FactColumn;
use crate::columnar::Column;

/// A fixed-width 128-bit sort prefix whose lexicographic order is monotone with
/// `Self::cmp`. When two values share a prefix, the full `cmp` is used to break
/// the tie.
///
/// Implementations must satisfy the monotonicity contract:
/// * `a.cmp(b) == Ordering::Less  ==> a.sort_prefix() <= b.sort_prefix()`.
/// * `a.cmp(b) == Ordering::Greater ==> a.sort_prefix() >= b.sort_prefix()`.
///
/// Put differently: if `a.sort_prefix() < b.sort_prefix()` then `a < b`, and
/// equal prefixes require breaking the tie via `cmp`.
pub trait SortPrefix {
    /// Return the 128-bit sort prefix for `self`.
    fn sort_prefix(&self) -> u128;
}

impl SortPrefix for u64 {
    #[inline(always)]
    fn sort_prefix(&self) -> u128 {
        u128::from(*self)
    }
}

impl SortPrefix for u32 {
    #[inline(always)]
    fn sort_prefix(&self) -> u128 {
        u128::from(*self)
    }
}

impl SortPrefix for i64 {
    #[inline(always)]
    fn sort_prefix(&self) -> u128 {
        // Flip the sign bit so two's-complement order matches unsigned order.
        u128::from((*self as u64) ^ (1u64 << 63))
    }
}

impl SortPrefix for i32 {
    #[inline(always)]
    fn sort_prefix(&self) -> u128 {
        u128::from((*self as u32) ^ (1u32 << 31))
    }
}

impl SortPrefix for () {
    #[inline(always)]
    fn sort_prefix(&self) -> u128 {
        0
    }
}

/// Sort and consolidate `pending` with the K sort prefix augmented inline.
///
/// The tuples are temporarily moved into `Vec<(u128, ((K, V), T, R))>` so the
/// sort sees the prefix as a direct-access field. The comparator uses the
/// 128-bit prefix as fast path and falls back to `((K, V), T)` on ties. After
/// sorting, the prefix is dropped and tuples move back into `pending` in order,
/// followed by the usual run consolidation.
///
/// The resulting order is identical to
/// `differential_dataflow::consolidation::consolidate_updates`.
fn prefix_sort_and_consolidate<K, V, T, R>(pending: &mut Vec<((K, V), T, R)>)
where
    K: SortPrefix + Ord,
    V: Ord,
    T: Ord,
    R: Semigroup + Clone,
{
    let n = pending.len();
    if n <= 1 {
        pending.retain(|(_, _, r)| !r.is_zero());
        return;
    }

    // Move tuples into an augmented vector (prefix, tuple) for sorting.
    // `std::mem::take` is a no-op move that reuses the allocation for the empty
    // state and avoids reallocating when we refill `pending` below.
    let input: Vec<((K, V), T, R)> = std::mem::take(pending);
    let mut augmented: Vec<(u128, ((K, V), T, R))> = Vec::with_capacity(n);
    for tuple in input {
        let prefix = tuple.0.0.sort_prefix();
        augmented.push((prefix, tuple));
    }

    augmented.sort_unstable_by(|a, b| match a.0.cmp(&b.0) {
        Ordering::Equal => {
            let ((ak, av), at, _) = &a.1;
            let ((bk, bv), bt, _) = &b.1;
            match ak.cmp(bk) {
                Ordering::Equal => match av.cmp(bv) {
                    Ordering::Equal => at.cmp(bt),
                    other => other,
                },
                other => other,
            }
        }
        other => other,
    });

    // Move tuples back into pending, dropping the prefix.
    pending.reserve(n);
    for (_, tuple) in augmented {
        pending.push(tuple);
    }

    // Accumulate runs sharing (K, V, T); drop zero-sum runs.
    let mut offset = 0usize;
    let mut accum = pending[0].2.clone();
    for index in 1..n {
        let (left, right) = pending.split_at_mut(index);
        let prev = &left[index - 1];
        let curr = &right[0];
        if prev.0 == curr.0 && prev.1 == curr.1 {
            accum.plus_equals(&curr.2);
        } else {
            if !accum.is_zero() {
                pending.swap(offset, index - 1);
                pending[offset].2.clone_from(&accum);
                offset += 1;
            }
            accum.clone_from(&pending[index].2);
        }
    }
    if !accum.is_zero() {
        pending.swap(offset, n - 1);
        pending[offset].2 = accum;
        offset += 1;
    }

    pending.truncate(offset);
}

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
    K: Columnar + SortPrefix + Ord + Clone,
    V: Columnar + SortPrefix + Ord + Clone,
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
    K: Columnar + SortPrefix + Ord + Clone,
    V: Columnar + SortPrefix + Ord + Clone,
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
    K: Columnar + SortPrefix + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + SortPrefix + Ord + Clone + 'static,
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
        // Sort+consolidate the pending staging buffer with a K-prefix
        // accelerated comparator. For types that can expose a monotone 128-bit
        // sort prefix (e.g. `Row`, `Timestamp`, numeric primitives), most
        // tuple comparisons short-circuit via a branch-predictable integer
        // compare, avoiding the full `Row::cmp` memcmp path that dominates
        // arrangement profiles. See `prefix_sort_and_consolidate` for details
        // and `SortPrefix` for the correctness contract.
        prefix_sort_and_consolidate(&mut self.pending);
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
    K: Columnar + SortPrefix + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + SortPrefix + Ord + Clone + 'static,
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
    K: Columnar + SortPrefix + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + SortPrefix + Ord + Clone + 'static,
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
    K: Columnar + SortPrefix + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + SortPrefix + Ord + Clone + 'static,
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

    /// Compare `prefix_sort_and_consolidate` against differential's
    /// `consolidate_updates` for a handful of deliberately adversarial inputs.
    #[mz_ore::test]
    fn test_prefix_sort_matches_consolidate_updates() {
        let cases: Vec<Vec<((u64, u64), u64, i64)>> = vec![
            // Empty.
            vec![],
            // Single element.
            vec![((1u64, 2u64), 3u64, 5i64)],
            // Already sorted, distinct.
            (0u64..64).map(|i| ((i, i + 1), i + 2, 1i64)).collect(),
            // Reversed.
            (0u64..64)
                .rev()
                .map(|i| ((i, i + 1), i + 2, 1i64))
                .collect(),
            // Heavy dedup — all identical.
            (0u64..64).map(|_| ((7u64, 11u64), 13u64, 1i64)).collect(),
            // Dup with cancelling diffs.
            vec![
                ((1, 2), 3, 1),
                ((1, 2), 3, -1),
                ((4, 5), 6, 2),
                ((4, 5), 6, -2),
            ],
            // Mixed — same K prefix, different V.
            (0u64..100)
                .map(|i| ((i % 4, i), i % 3, (i as i64) - 50))
                .collect(),
        ];

        for (idx, case) in cases.into_iter().enumerate() {
            let mut expected = case.clone();
            differential_dataflow::consolidation::consolidate_updates(&mut expected);
            let mut actual = case.clone();
            prefix_sort_and_consolidate(&mut actual);
            assert_eq!(
                expected, actual,
                "prefix_sort_and_consolidate diverged from consolidate_updates on case {idx}"
            );
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(5000))]

        /// For arbitrary inputs, `prefix_sort_and_consolidate` must produce the
        /// exact same output as differential's `consolidate_updates`. This is the
        /// contract upheld by [`FactTrieChunker::flush_pending`].
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn prefix_sort_matches_consolidate_prop(
            input in proptest::collection::vec(
                ((0u64..8, 0u64..8), 0u64..4, -3i64..=3i64),
                0..512,
            )
        ) {
            let mut expected = input.clone();
            differential_dataflow::consolidation::consolidate_updates(&mut expected);
            let mut actual = input;
            prefix_sort_and_consolidate(&mut actual);
            proptest::prop_assert_eq!(expected, actual);
        }
    }
}
