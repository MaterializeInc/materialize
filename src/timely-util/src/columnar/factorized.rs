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

//! Factorized (trie-structured) columnar storage.
//!
//! Stores hierarchical data where each level maps indices to ranges in the next level,
//! deduplicating repeated values. For example, `(Data, Time, Diff)` tuples where many
//! rows share the same `Data` value store each distinct `Data` once, with bounds pointing
//! to the corresponding `Time` range.
//!
//! The building blocks are:
//! * [`Lists`] — a `Vecs<C, Strides>` pairing values with strided offset bounds.
//! * [`Level`] — one level of the trie, holding a [`Lists`] and a child level.
//! * [`FactorizedColumns`] — type alias for a 3-level `Level<A, Level<B, Lists<ContainerOf<C>>>>`.
//!
//! Two construction modes:
//! * [`FactorizedColumns::push_flat`] — stride-1 insertion (no dedup), for accumulating unsorted data.
//! * [`FactorizedColumns::form`] — build a trie from a sorted iterator, deduplicating at each level.

pub mod batch;
pub mod batcher;
pub mod chunker;
pub mod column;
pub mod container;
pub mod layout;
#[cfg(test)]
mod tests_prop;

use std::rc::Rc;

use columnar::primitive::offsets::Strides;
use columnar::{
    AsBytes, Borrow, ContainerOf, FromBytes, Index, IndexAs, Len, Lookbacks, Push, Repeats, Vecs,
};
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
use mz_ore::cast::CastFrom;

use batch::{FactBatch, FactBuilder};
use batcher::FactTrieInternalMerger;
use chunker::FactTrieChunker;
pub use chunker::SortPrefix;

/// A spine of factorized columnar batches.
pub type FactValSpine<K, V, T, R> = Spine<Rc<FactBatch<K, V, T, R>>>;

/// A batcher that consolidates `Vec<((K, V), T, R)>` input and merges sorted
/// chains as factorized [`KVUpdates`] trie chunks. Key/value dedup happens
/// inside the batcher pipeline, not only at final-batch time.
pub type FactValBatcher<K, V, T, R> = MergeBatcher<
    Vec<((K, V), T, R)>,
    FactTrieChunker<K, V, T, R>,
    FactTrieInternalMerger<K, V, T, R>,
>;

/// Columnar-input counterpart to [`FactValBatcher`].
///
/// Accepts `Column<((K, V), T, R)>` (the wire format used by the existing
/// `Col2ValBatcher` pipeline), decodes each update into `pending` inside the
/// chunker, and emits factorized [`KVUpdates`] chunks. This lets renderers
/// that already produce columnar pre-exchange streams (e.g.,
/// `FormArrangementKey` via `ColumnBuilder`) feed directly into a factorized
/// spine without materializing an intermediate `Vec<((K, V), T, R)>`.
pub type FactColValBatcher<K, V, T, R> = MergeBatcher<
    crate::columnar::Column<((K, V), T, R)>,
    FactTrieChunker<K, V, T, R>,
    FactTrieInternalMerger<K, V, T, R>,
>;

/// A builder producing `Rc<FactBatch>` for use with [`FactValSpine`].
pub type FactValBuilder<K, V, T, R> = RcBuilder<FactBuilder<K, V, T, R>>;

/// Column-input builder producing `Rc<FactBatch>`.
///
/// Alias for [`FactValBuilder`]; retained for callsite naming clarity in
/// `reduce_abelian` / `threshold_arrangement` paths. [`FactBuilder`] accepts
/// [`column::FactColumn`] on both the batcher and reduce paths.
pub type FactColValBuilder<K, V, T, R> = RcBuilder<FactBuilder<K, V, T, R>>;

/// A [`Vecs`] using [`Strides`] for offset bounds.
///
/// When all child ranges have constant fan-out (e.g., stride-1 for flat data),
/// `Strides` compresses the bounds to 16 bytes total. Varying fan-out falls back
/// to `Vec<u64>`.
pub type Lists<C> = Vecs<C, Strides>;

/// A single level in a factorized column structure.
///
/// Generic over the lists type `L` and the child level `Rest`. When `L` is
/// `Lists<ContainerOf<C>>`, this level stores values of type `C` with bounds
/// mapping into `Rest`. The same struct is used for both owned and borrowed forms,
/// enabling natural `Borrow`/`AsBytes`/`FromBytes` implementations.
///
/// For example, if `lists.bounds = [3, 5]`, then value 0 maps to children 0..3
/// in `rest`, and value 1 maps to children 3..5.
#[derive(Copy)]
pub struct Level<L, Rest> {
    /// Values at this level, with cumulative bounds into `rest`.
    pub lists: L,
    /// The next level of the factorized structure.
    pub rest: Rest,
}

/// A factorized 3-level column store for `(A, B, C)` data.
///
/// Trie structure: A values → B values → C values.
/// * `level.lists` — A values + bounds mapping outer groups to A ranges.
/// * `level.rest.lists` — B values + bounds mapping A indices to B ranges.
/// * `level.rest.rest` — C values (leaf) + bounds mapping B indices to C ranges.
pub type FactorizedColumns<A, B, C> =
    Level<Lists<ContainerOf<A>>, Level<Lists<ContainerOf<B>>, Lists<ContainerOf<C>>>>;

/// A factorized 4-level column store for `((K, V), Time, Diff)` data.
///
/// Trie structure: K values → V values → (Time, Diff) leaf pairs.
/// The leaf is a tuple of two parallel columns sharing the same bounds.
pub type KVUpdates<K, V, T, R> = Level<
    Lists<ContainerOf<K>>,
    Level<Lists<ContainerOf<V>>, Lists<(ContainerOf<T>, ContainerOf<R>)>>,
>;

/// Like [`KVUpdates`] but with [`Repeats`] on both leaf columns.
///
/// [`Repeats`] encodes consecutive identical values as 1-bit `None` markers,
/// compressing runs of repeated timestamps or diffs (e.g., many `+1` diffs).
pub type KVUpdatesRepeats<K, V, T, R> = Level<
    Lists<ContainerOf<K>>,
    Level<Lists<ContainerOf<V>>, Lists<(Repeats<ContainerOf<T>>, Repeats<ContainerOf<R>>)>>,
>;

/// Like [`KVUpdates`] but with [`Lookbacks`] on both leaf columns.
///
/// [`Lookbacks`] scans up to N previous distinct values for matches, encoding
/// hits as 1-byte back-references. More powerful than [`Repeats`] for non-consecutive
/// repetition, at the cost of O(N) per push.
pub type KVUpdatesLookbacks<K, V, T, R> = Level<
    Lists<ContainerOf<K>>,
    Level<Lists<ContainerOf<V>>, Lists<(Lookbacks<ContainerOf<T>>, Lookbacks<ContainerOf<R>>)>>,
>;

impl<L: Default, Rest: Default> Default for Level<L, Rest> {
    fn default() -> Self {
        Level {
            lists: Default::default(),
            rest: Default::default(),
        }
    }
}

impl<L: Clone, Rest: Clone> Clone for Level<L, Rest> {
    fn clone(&self) -> Self {
        Level {
            lists: self.lists.clone(),
            rest: self.rest.clone(),
        }
    }
}

impl<L: std::fmt::Debug, Rest: std::fmt::Debug> std::fmt::Debug for Level<L, Rest> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Level")
            .field("lists", &self.lists)
            .field("rest", &self.rest)
            .finish()
    }
}

impl<L: PartialEq, Rest: PartialEq> PartialEq for Level<L, Rest> {
    fn eq(&self, other: &Self) -> bool {
        self.lists == other.lists && self.rest == other.rest
    }
}

// --- Serialization support ---
//
// `Level` does not implement `Borrow` (which requires `Borrowed: Index`, unsuited
// for a trie). Instead, it provides a `borrowed()` helper and implements
// `AsBytes`/`FromBytes` directly, enabling serialization as one contiguous
// indexed blob via `indexed::encode`/`indexed::decode`.

impl<L: Borrow, RL: Borrow, Leaf: Borrow> Level<L, Level<RL, Leaf>> {
    /// Borrow all levels recursively, producing a `Level` with borrowed inner types.
    #[inline(always)]
    pub fn borrowed(&self) -> Level<L::Borrowed<'_>, Level<RL::Borrowed<'_>, Leaf::Borrowed<'_>>> {
        Level {
            lists: self.lists.borrow(),
            rest: Level {
                lists: self.rest.lists.borrow(),
                rest: self.rest.rest.borrow(),
            },
        }
    }
}

impl<'a, L: AsBytes<'a>, Rest: AsBytes<'a>> AsBytes<'a> for Level<L, Rest> {
    const SLICE_COUNT: usize = L::SLICE_COUNT + Rest::SLICE_COUNT;

    #[inline]
    fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
        debug_assert!(index < Self::SLICE_COUNT);
        if index < L::SLICE_COUNT {
            self.lists.get_byte_slice(index)
        } else {
            self.rest.get_byte_slice(index - L::SLICE_COUNT)
        }
    }
}

impl<'a, L: FromBytes<'a>, Rest: FromBytes<'a>> FromBytes<'a> for Level<L, Rest> {
    const SLICE_COUNT: usize = L::SLICE_COUNT + Rest::SLICE_COUNT;

    #[inline(always)]
    fn from_bytes(bytes: &mut impl Iterator<Item = &'a [u8]>) -> Self {
        Self {
            lists: FromBytes::from_bytes(bytes),
            rest: FromBytes::from_bytes(bytes),
        }
    }
    #[inline(always)]
    fn from_store(store: &columnar::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
        Self {
            lists: L::from_store(store, offset),
            rest: Rest::from_store(store, offset),
        }
    }
    fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
        L::element_sizes(sizes)?;
        Rest::element_sizes(sizes)?;
        Ok(())
    }
}

/// Returns the child index range for element `i` given cumulative bounds.
///
/// `bounds` stores cumulative end positions: element `i` owns children
/// `bounds[i-1]..bounds[i]` (with `bounds[-1]` defined as 0).
#[inline]
pub fn child_range<B: IndexAs<u64>>(bounds: B, i: usize) -> std::ops::Range<usize> {
    let lower = if i == 0 {
        0
    } else {
        usize::cast_from(bounds.index_as(i - 1))
    };
    let upper = usize::cast_from(bounds.index_as(i));
    lower..upper
}

impl<AV, BV, CC> Level<Vecs<AV, Strides>, Level<Vecs<BV, Strides>, Vecs<CC, Strides>>>
where
    AV: columnar::Container,
    BV: columnar::Container,
    CC: columnar::Container,
{
    /// Push a single `(a, b, c)` element as a stride-1 entry at every level.
    ///
    /// Each element gets its own group at every level (no deduplication).
    /// Used for accumulating unsorted data before form.
    pub fn push_flat<AP, BP, CP>(&mut self, a: AP, b: BP, c: CP)
    where
        AV: Push<AP>,
        BV: Push<BP>,
        CC: Push<CP>,
    {
        self.lists.values.push(a);
        Push::push(
            &mut self.lists.bounds,
            u64::cast_from(self.lists.values.len()),
        );
        self.rest.lists.values.push(b);
        Push::push(
            &mut self.rest.lists.bounds,
            u64::cast_from(self.rest.lists.values.len()),
        );
        self.rest.rest.values.push(c);
        Push::push(
            &mut self.rest.rest.bounds,
            u64::cast_from(self.rest.rest.values.len()),
        );
    }

    /// Iterate all `(A, B, C)` tuples as columnar refs.
    ///
    /// Traverses the trie: for each outer group, for each A value, for each B value
    /// in A's range, for each C value in B's range, yield `(ref_a, ref_b, ref_c)`.
    pub fn iter(
        &self,
    ) -> impl Iterator<
        Item = (
            <AV as Borrow>::Ref<'_>,
            <BV as Borrow>::Ref<'_>,
            <CC as Borrow>::Ref<'_>,
        ),
    > {
        let a_lists = self.lists.borrow();
        let b_lists = self.rest.lists.borrow();
        let c_lists = self.rest.rest.borrow();

        (0..Len::len(&a_lists))
            .flat_map(move |outer| child_range(a_lists.bounds, outer))
            .flat_map(move |a_idx| {
                let a_val = a_lists.values.get(a_idx);
                child_range(b_lists.bounds, a_idx).map(move |b_idx| (a_val, b_idx))
            })
            .flat_map(move |(a_val, b_idx)| {
                let b_val = b_lists.values.get(b_idx);
                child_range(c_lists.bounds, b_idx)
                    .map(move |c_idx| (a_val, b_val, c_lists.values.get(c_idx)))
            })
    }

    /// Visit all `(A, B, C)` tuples using cursor-based leaf iteration.
    ///
    /// Like [`Self::iter`] but uses [`Index::cursor`] for the leaf level instead of
    /// per-element `get()` calls. For containers like [`Repeats`] where `get()` involves
    /// expensive `rank()` operations, cursor-based iteration maintains sequential state
    /// and is significantly faster.
    ///
    /// Uses `for_each` style because cursor iterators borrow the container, which is
    /// incompatible with `flat_map`'s `FnMut` closure requirement.
    pub fn for_each_cursor(&self, mut f: impl FnMut(AV::Ref<'_>, BV::Ref<'_>, CC::Ref<'_>)) {
        let a_lists = self.lists.borrow();
        let b_lists = self.rest.lists.borrow();
        let c_lists = self.rest.rest.borrow();

        for outer in 0..Len::len(&a_lists) {
            for a_idx in child_range(a_lists.bounds, outer) {
                let a_val = a_lists.values.get(a_idx);
                for b_idx in child_range(b_lists.bounds, a_idx) {
                    let b_val = b_lists.values.get(b_idx);
                    let range = child_range(c_lists.bounds, b_idx);
                    for c_val in c_lists.values.cursor(range) {
                        f(a_val, b_val, c_val);
                    }
                }
            }
        }
    }

    /// Build a factorized trie from a sorted iterator of `(A, B, C)` refs.
    ///
    /// The input **must** be sorted by `(A, B, C)` order. Equal A values are
    /// deduplicated into a single entry mapping to a range of B values; likewise
    /// for B→C. No consolidation of C values is performed — the caller is
    /// responsible for deduplication or diff accumulation if needed.
    ///
    /// Produces a single outer group containing all A values.
    pub fn form<AR, BR, CR>(sorted: impl Iterator<Item = (AR, BR, CR)>) -> Self
    where
        AR: Copy + Eq,
        BR: Copy + Eq,
        CR: Copy,
        AV: Push<AR>,
        BV: Push<BR>,
        CC: Push<CR>,
    {
        let mut output = Self::default();
        let mut sorted = sorted.peekable();

        if let Some((a, b, c)) = sorted.next() {
            let mut prev_a = a;
            let mut prev_b = b;
            output.lists.values.push(a);
            output.rest.lists.values.push(b);
            output.rest.rest.values.push(c);

            for (a, b, c) in sorted {
                if a != prev_a {
                    // New A: seal C bounds (for prev B) and B bounds (for prev A).
                    Push::push(
                        &mut output.rest.rest.bounds,
                        u64::cast_from(output.rest.rest.values.len()),
                    );
                    Push::push(
                        &mut output.rest.lists.bounds,
                        u64::cast_from(output.rest.lists.values.len()),
                    );
                    output.lists.values.push(a);
                    output.rest.lists.values.push(b);
                } else if b != prev_b {
                    // Same A, new B: seal C bounds (for prev B).
                    Push::push(
                        &mut output.rest.rest.bounds,
                        u64::cast_from(output.rest.rest.values.len()),
                    );
                    output.rest.lists.values.push(b);
                }
                // Always push C.
                output.rest.rest.values.push(c);

                prev_a = a;
                prev_b = b;
            }

            // Seal all open bounds.
            Push::push(
                &mut output.rest.rest.bounds,
                u64::cast_from(output.rest.rest.values.len()),
            );
            Push::push(
                &mut output.rest.lists.bounds,
                u64::cast_from(output.rest.lists.values.len()),
            );
            Push::push(
                &mut output.lists.bounds,
                u64::cast_from(output.lists.values.len()),
            );
        }

        output
    }

    /// Number of leaf (C-level) entries, equal to the total number of tuples.
    pub fn len(&self) -> usize {
        Len::len(&self.rest.rest.values)
    }

    /// Whether the structure contains no tuples.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all levels, resetting to an empty state.
    pub fn clear(&mut self) {
        columnar::Clear::clear(&mut self.lists.values);
        columnar::Clear::clear(&mut self.lists.bounds);
        columnar::Clear::clear(&mut self.rest.lists.values);
        columnar::Clear::clear(&mut self.rest.lists.bounds);
        columnar::Clear::clear(&mut self.rest.rest.values);
        columnar::Clear::clear(&mut self.rest.rest.bounds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
    use timely::progress::Antichain;
    use timely::progress::frontier::AntichainRef;

    #[mz_ore::test]
    fn test_push_flat_compiles() {
        let mut fc: FactorizedColumns<u64, u64, i64> = Default::default();
        fc.push_flat(&1u64, &10u64, &100i64);
        fc.push_flat(&2u64, &20u64, &200i64);
        // Verify structure: 2 entries at each level
        assert_eq!(Len::len(&fc.lists.values), 2);
        assert_eq!(Len::len(&fc.rest.lists.values), 2);
        assert_eq!(Len::len(&fc.rest.rest.values), 2);
    }

    #[mz_ore::test]
    fn test_push_flat_iter_roundtrip() {
        let mut fc: FactorizedColumns<u64, u64, i64> = Default::default();
        fc.push_flat(&1u64, &10u64, &100i64);
        fc.push_flat(&2u64, &20u64, &200i64);
        fc.push_flat(&3u64, &30u64, &300i64);

        let items: Vec<_> = fc.iter().map(|(a, b, c)| (*a, *b, *c)).collect();
        assert_eq!(items, vec![(1, 10, 100), (2, 20, 200), (3, 30, 300)]);
    }

    #[mz_ore::test]
    fn test_form_deduplication() {
        let input: Vec<(u64, u64, i64)> =
            vec![(1, 10, 100), (1, 10, 200), (1, 20, 300), (2, 30, 400)];
        let refs: Vec<_> = input.iter().map(|(a, b, c)| (a, b, c)).collect();

        let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());

        let result: Vec<_> = fc.iter().map(|(a, b, c)| (*a, *b, *c)).collect();
        assert_eq!(
            result,
            vec![(1, 10, 100), (1, 10, 200), (1, 20, 300), (2, 30, 400)]
        );

        // Verify deduplication.
        assert_eq!(Len::len(&fc.lists.values), 2); // A: [1, 2]
        assert_eq!(Len::len(&fc.rest.lists.values), 3); // B: [10, 20, 30]
        assert_eq!(Len::len(&fc.rest.rest.values), 4); // C: [100, 200, 300, 400]
    }

    #[mz_ore::test]
    fn test_len_clear() {
        let mut fc: FactorizedColumns<u64, u64, i64> = Default::default();
        assert_eq!(fc.len(), 0);
        assert!(fc.is_empty());

        fc.push_flat(&1u64, &10u64, &100i64);
        fc.push_flat(&2u64, &20u64, &200i64);
        assert_eq!(fc.len(), 2);
        assert!(!fc.is_empty());

        fc.clear();
        assert_eq!(fc.len(), 0);
        assert!(fc.is_empty());

        fc.push_flat(&5u64, &50u64, &500i64);
        let items: Vec<_> = fc.iter().map(|(a, b, c)| (*a, *b, *c)).collect();
        assert_eq!(items, vec![(5, 50, 500)]);
    }

    #[mz_ore::test]
    fn test_strides_compression_flat() {
        // Flat push: all bounds should use constant stride (no Vec<u64> allocation).
        let mut fc: FactorizedColumns<u64, u64, i64> = Default::default();
        for i in 0..100u64 {
            fc.push_flat(&i, &(i * 10), &(i as i64 * 100));
        }

        // All strides should be 1 (each element maps to exactly 1 child).
        assert_eq!(fc.lists.bounds.strided(), Some(1));
        assert_eq!(fc.rest.lists.bounds.strided(), Some(1));
        assert_eq!(fc.rest.rest.bounds.strided(), Some(1));
    }

    #[mz_ore::test]
    fn test_strides_compression_form() {
        // Form with varying fan-out: strides should NOT all be constant.
        let input: Vec<(u64, u64, i64)> = vec![
            (1, 10, 100),
            (1, 10, 200), // A=1 has 2 C values under B=10
            (1, 20, 300), // A=1 has 2 B values
            (2, 30, 400), // A=2 has 1 B value
        ];
        let refs: Vec<_> = input.iter().map(|(a, b, c)| (a, b, c)).collect();
        let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());

        // A bounds: single outer group.
        assert!(fc.lists.bounds.strided().is_some());

        // B bounds: A[0]=1 has 2 B children, A[1]=2 has 1 B child → varying.
        assert_eq!(fc.rest.lists.bounds.strided(), None);

        // C bounds: B[0]=10 has 2 children, B[1]=20 has 1, B[2]=30 has 1 → varying.
        assert_eq!(fc.rest.rest.bounds.strided(), None);
    }

    #[mz_ore::test]
    fn test_form_sort_roundtrip() {
        let data = vec![
            (3u64, 30u64, 300i64),
            (1, 10, 100),
            (1, 10, 200),
            (2, 20, 200),
            (1, 20, 300),
            (2, 30, 400),
        ];

        let mut flat: FactorizedColumns<u64, u64, i64> = Default::default();
        for (a, b, c) in &data {
            flat.push_flat(a, b, c);
        }

        let mut refs: Vec<_> = flat.iter().collect();
        refs.sort();
        let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());

        let result: Vec<_> = fc.iter().map(|(a, b, c)| (*a, *b, *c)).collect();
        let mut expected = data;
        expected.sort();
        assert_eq!(result, expected);
    }

    #[mz_ore::test]
    fn test_form_empty() {
        let empty: Vec<(&u64, &u64, &i64)> = vec![];
        let fc = FactorizedColumns::<u64, u64, i64>::form(empty.into_iter());
        assert!(fc.is_empty());
        assert_eq!(fc.len(), 0);
        let items: Vec<_> = fc.iter().collect();
        assert!(items.is_empty());
    }

    #[mz_ore::test]
    fn test_form_single_element() {
        let input = vec![(42u64, 7u64, -1i64)];
        let refs: Vec<_> = input.iter().map(|(a, b, c)| (a, b, c)).collect();
        let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());

        assert_eq!(fc.len(), 1);
        let items: Vec<_> = fc.iter().map(|(a, b, c)| (*a, *b, *c)).collect();
        assert_eq!(items, vec![(42, 7, -1)]);
    }

    #[mz_ore::test]
    fn test_form_all_same_a() {
        let input: Vec<(u64, u64, i64)> =
            vec![(1, 10, 100), (1, 10, 200), (1, 20, 300), (1, 20, 400)];
        let refs: Vec<_> = input.iter().map(|(a, b, c)| (a, b, c)).collect();
        let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());

        let result: Vec<_> = fc.iter().map(|(a, b, c)| (*a, *b, *c)).collect();
        assert_eq!(result, input);

        assert_eq!(Len::len(&fc.lists.values), 1);
        assert_eq!(Len::len(&fc.rest.lists.values), 2);
        assert_eq!(Len::len(&fc.rest.rest.values), 4);
    }

    /// Test KVUpdates with tuple leaf: K → V → (Time, Diff).
    #[mz_ore::test]
    fn test_kv_updates_tuple_leaf() {
        // Sorted ((K, V), Time, Diff) data stored as K → V → (Time, Diff).
        // K=1, V=10: times [100, 200], diffs [1, 1]
        // K=1, V=20: times [100], diffs [1]
        // K=2, V=30: times [100], diffs [-1]
        let input: Vec<(u64, u64, (u64, i64))> = vec![
            (1, 10, (100, 1)),
            (1, 10, (200, 1)),
            (1, 20, (100, 1)),
            (2, 30, (100, -1)),
        ];
        let refs: Vec<_> = input.iter().map(|(k, v, (t, d))| (k, v, (t, d))).collect();
        let fc = KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());

        let result: Vec<_> = fc.iter().map(|(k, v, (t, d))| (*k, *v, (*t, *d))).collect();
        assert_eq!(
            result,
            vec![
                (1, 10, (100, 1)),
                (1, 10, (200, 1)),
                (1, 20, (100, 1)),
                (2, 30, (100, -1)),
            ]
        );

        // K dedup: 2 distinct keys.
        assert_eq!(Len::len(&fc.lists.values), 2);
        // V dedup: 3 distinct vals.
        assert_eq!(Len::len(&fc.rest.lists.values), 3);
        // Leaf: 4 (time, diff) pairs.
        assert_eq!(fc.len(), 4);
    }

    /// Test KVUpdatesRepeats: Repeats compresses consecutive identical leaf values.
    #[mz_ore::test]
    fn test_kv_updates_repeats_compression() {
        // Simulate real pattern: few distinct times, mostly +1 diffs.
        let n = 1000usize;
        let n_keys = 10usize;
        let n_vals = 100usize;
        let n_times = 5usize;

        let mut data: Vec<(u64, u64, (u64, i64))> = Vec::with_capacity(n);
        for i in 0..n {
            data.push((
                (i % n_keys) as u64,
                (i % n_vals) as u64,
                ((i % n_times) as u64, 1i64), // few times, all +1 diffs
            ));
        }
        data.sort();

        // Plain KVUpdates.
        let mut plain_flat: KVUpdates<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &data {
            plain_flat.push_flat(k, v, (&td.0, &td.1));
        }
        let refs: Vec<_> = plain_flat.iter().collect();
        let plain = KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());

        // KVUpdatesRepeats.
        let mut repeat_flat: KVUpdatesRepeats<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &data {
            repeat_flat.push_flat(k, v, (&td.0, &td.1));
        }
        let refs: Vec<_> = repeat_flat.iter().collect();
        let repeat = KVUpdatesRepeats::<u64, u64, u64, i64>::form(refs.into_iter());

        // Both should produce same logical data.
        let plain_result: Vec<_> = plain
            .iter()
            .map(|(k, v, (t, d))| (*k, *v, (*t, *d)))
            .collect();
        let repeat_result: Vec<_> = repeat
            .iter()
            .map(|(k, v, (t, d))| (*k, *v, (*t, *d)))
            .collect();
        assert_eq!(plain_result, repeat_result);

        // Repeats should compress the leaf columns.
        // Time: n_times distinct values cycling → many consecutive repeats after sort.
        // Diff: all +1 → first is Some, rest are None.
        let plain_time_len = Len::len(&plain.rest.rest.values.0);
        let plain_diff_len = Len::len(&plain.rest.rest.values.1);
        let repeat_time_somes = Len::len(&repeat.rest.rest.values.0.inner.somes);
        let repeat_diff_somes = Len::len(&repeat.rest.rest.values.1.inner.somes);

        println!("--- KVUpdatesRepeats compression ---");
        println!("Plain leaf:   time={plain_time_len}, diff={plain_diff_len}");
        println!("Repeat somes: time={repeat_time_somes}, diff={repeat_diff_somes}");
        println!(
            "Compression:  time {:.1}x, diff {:.1}x",
            plain_time_len as f64 / repeat_time_somes as f64,
            plain_diff_len as f64 / repeat_diff_somes as f64,
        );

        // Diff should compress massively (all +1).
        assert!(repeat_diff_somes < plain_diff_len / 2);
    }

    /// Test serialization roundtrip: encode to bytes, decode, iterate, verify same data.
    #[mz_ore::test]
    fn test_serialization_roundtrip() {
        use columnar::bytes::indexed;

        let input: Vec<(u64, u64, i64)> =
            vec![(1, 10, 100), (1, 10, 200), (1, 20, 300), (2, 30, 400)];
        let refs: Vec<_> = input.iter().map(|(a, b, c)| (a, b, c)).collect();
        let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());

        // Encode the entire trie as one indexed blob.
        let borrowed = fc.borrowed();
        let mut store = Vec::new();
        indexed::encode(&mut store, &borrowed);

        // Decode borrowed view from the store.
        let ds = indexed::DecodedStore::new(&store);
        type BorrowedFC<'a> = Level<
            Vecs<&'a [u64], Strides<&'a [u64], &'a [u64]>>,
            Level<
                Vecs<&'a [u64], Strides<&'a [u64], &'a [u64]>>,
                Vecs<&'a [i64], Strides<&'a [u64], &'a [u64]>>,
            >,
        >;
        let decoded: BorrowedFC<'_> = FromBytes::from_store(&ds, &mut 0);

        // Iterate decoded data using child_range.
        let mut result = Vec::new();
        for outer in 0..Len::len(&decoded.lists) {
            for a_idx in child_range(decoded.lists.bounds, outer) {
                let a_val = *Index::get(&decoded.lists.values, a_idx);
                for b_idx in child_range(decoded.rest.lists.bounds, a_idx) {
                    let b_val = *Index::get(&decoded.rest.lists.values, b_idx);
                    for c_idx in child_range(decoded.rest.rest.bounds, b_idx) {
                        let c_val = *Index::get(&decoded.rest.rest.values, c_idx);
                        result.push((a_val, b_val, c_val));
                    }
                }
            }
        }

        assert_eq!(
            result,
            vec![(1, 10, 100), (1, 10, 200), (1, 20, 300), (2, 30, 400)]
        );
    }

    #[mz_ore::test]
    fn bench_memory_savings() {
        let n = 100_000usize;
        let n_distinct_a = 100usize;
        let n_distinct_b = 1_000usize;

        let mut data: Vec<(u64, u64, i64)> = Vec::with_capacity(n);
        for i in 0..n {
            data.push((
                (i % n_distinct_a) as u64,
                (i % n_distinct_b) as u64,
                i as i64,
            ));
        }
        data.sort();

        let mut flat: FactorizedColumns<u64, u64, i64> = Default::default();
        for (a, b, c) in &data {
            flat.push_flat(a, b, c);
        }
        let refs: Vec<_> = flat.iter().collect();
        let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());

        // Verify correctness.
        assert_eq!(fc.len(), n);
        let result: Vec<_> = fc.iter().map(|(a, b, c)| (*a, *b, *c)).collect();
        assert_eq!(result, data);

        // Report.
        println!("--- Factorized columns memory benchmark ---");
        println!(
            "Input: {} tuples, {} distinct A, {} distinct B",
            n, n_distinct_a, n_distinct_b
        );
        println!(
            "Flat:        A={}, B={}, C={} values",
            Len::len(&flat.lists.values),
            Len::len(&flat.rest.lists.values),
            Len::len(&flat.rest.rest.values),
        );
        println!(
            "Factorized:  A={}, B={}, C={} values",
            Len::len(&fc.lists.values),
            Len::len(&fc.rest.lists.values),
            Len::len(&fc.rest.rest.values),
        );
        println!(
            "Reduction: A {:.0}x, B {:.0}x",
            n as f64 / Len::len(&fc.lists.values) as f64,
            n as f64 / Len::len(&fc.rest.lists.values) as f64,
        );
    }

    /// Integration test: build multiple batches, merge them through the full
    /// Batch/Merger/Builder stack, verify cursor output.
    #[mz_ore::test]
    fn test_spine_stack_integration() {
        use batch::{FactBuilder, FactMerger};

        // Build two abutting batches.
        let tuples1: Vec<(u64, u64, u64, i64)> =
            vec![(1, 10, 100, 1), (1, 20, 100, 1), (2, 30, 100, 1)];
        let mut chunk1 = column::FactColumn::Typed(KVUpdates::<u64, u64, u64, i64>::form(
            tuples1.iter().map(|(k, v, t, d)| (k, v, (t, d))),
        ));
        let mut builder1 = FactBuilder::<u64, u64, u64, i64>::with_capacity(0, 0, 0);
        builder1.push(&mut chunk1);
        let batch1 = builder1.done(Description::new(
            Antichain::from_elem(0u64),
            Antichain::from_elem(200u64),
            Antichain::from_elem(0u64),
        ));

        let tuples2: Vec<(u64, u64, u64, i64)> =
            vec![(1, 10, 300, 2), (2, 30, 300, -1), (3, 40, 300, 1)];
        let mut chunk2 = column::FactColumn::Typed(KVUpdates::<u64, u64, u64, i64>::form(
            tuples2.iter().map(|(k, v, t, d)| (k, v, (t, d))),
        ));
        let mut builder2 = FactBuilder::<u64, u64, u64, i64>::with_capacity(0, 0, 0);
        builder2.push(&mut chunk2);
        let batch2 = builder2.done(Description::new(
            Antichain::from_elem(200u64),
            Antichain::from_elem(400u64),
            Antichain::from_elem(0u64),
        ));

        // Merge without compaction.
        let mut merger: FactMerger<u64, u64, u64, i64> =
            batch1.begin_merge(&batch2, AntichainRef::new(&[]));
        merger.work(&batch1, &batch2, &mut 10000);
        let merged = merger.done();

        // Verify merged batch has all updates.
        assert_eq!(merged.len(), 6);

        // Collect and verify.
        let mut result = Vec::new();
        let mut cursor = merged.cursor();
        while cursor.key_valid(&merged) {
            while cursor.val_valid(&merged) {
                let k = *cursor.key(&merged);
                let v = *cursor.val(&merged);
                cursor.map_times(&merged, |t, d| {
                    result.push((k, v, *t, *d));
                });
                cursor.step_val(&merged);
            }
            cursor.step_key(&merged);
        }

        assert_eq!(
            result,
            vec![
                (1, 10, 100, 1),
                (1, 10, 300, 2),
                (1, 20, 100, 1),
                (2, 30, 100, 1),
                (2, 30, 300, -1),
                (3, 40, 300, 1),
            ]
        );

        // Now merge with compaction at time 400 — all times advance to 400.
        let mut merger2: FactMerger<u64, u64, u64, i64> =
            batch1.begin_merge(&batch2, Antichain::from_elem(400u64).borrow());
        merger2.work(&batch1, &batch2, &mut 10000);
        let compacted = merger2.done();

        let mut compacted_result = Vec::new();
        let mut cursor = compacted.cursor();
        while cursor.key_valid(&compacted) {
            while cursor.val_valid(&compacted) {
                let k = *cursor.key(&compacted);
                let v = *cursor.val(&compacted);
                cursor.map_times(&compacted, |t, d| {
                    compacted_result.push((k, v, *t, *d));
                });
                cursor.step_val(&compacted);
            }
            cursor.step_key(&compacted);
        }

        // Key 1, Val 10: times 100→400, 300→400, diffs 1+2=3.
        // Key 1, Val 20: time 100→400, diff 1.
        // Key 2, Val 30: times 100→400, 300→400, diffs 1+(-1)=0 → gone!
        // Key 3, Val 40: time 300→400, diff 1.
        assert_eq!(
            compacted_result,
            vec![(1, 10, 400, 3), (1, 20, 400, 1), (3, 40, 400, 1),]
        );
    }
}
