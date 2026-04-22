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

//! Factorized columnar batch, cursor, and merger.
//!
//! [`FactBatch`] wraps a [`KVUpdates`] trie with a [`Description`] to form an
//! immutable batch of updates. [`FactCursor`] navigates the trie using two indices
//! (`key_cursor`, `val_cursor`) and [`child_range`] for bounds at each level.
//!
//! [`FactMerger`] performs key-by-key trie merge with time compaction via
//! `advance_by` and update consolidation (accumulate diffs, drop zeros).

use std::marker::PhantomData;

use columnar::{Borrow, Columnar, Index, Len};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::WithLayout;
use differential_dataflow::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
use mz_ore::cast::CastFrom;
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

use super::column::FactColumn;
use super::layout::FactLayout;
use super::{KVUpdates, child_range};

/// An immutable collection of update tuples backed by a factorized trie.
///
/// The trie structure `K → V → (T, R)` deduplicates shared keys and values.
/// Navigation uses [`child_range`] on the trie bounds at each level.
pub struct FactBatch<K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// The factorized trie storage.
    pub storage: KVUpdates<K, V, T, R>,
    /// Description of the update times this batch represents.
    pub description: Description<T>,
    /// Total number of `(time, diff)` updates in the batch.
    pub updates: usize,
}

impl<K, V, T, R> FactBatch<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    /// Number of distinct keys in this batch.
    #[inline]
    pub fn key_count(&self) -> usize {
        Len::len(&self.storage.lists.values.borrow())
    }

    /// Value index range for key at `key_idx`.
    #[inline]
    pub fn val_range(&self, key_idx: usize) -> std::ops::Range<usize> {
        child_range(self.storage.rest.lists.bounds.borrow(), key_idx)
    }

    /// Update index range for val at `val_idx`.
    #[inline]
    pub fn upd_range(&self, val_idx: usize) -> std::ops::Range<usize> {
        child_range(self.storage.rest.rest.bounds.borrow(), val_idx)
    }
}

impl<K, V, T, R> WithLayout for FactBatch<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    type Layout = FactLayout<K, V, T, R>;
}

impl<K, V, T, R> BatchReader for FactBatch<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    type Cursor = FactCursor<K, V, T, R>;

    fn cursor(&self) -> Self::Cursor {
        let mut cursor = FactCursor {
            key_cursor: 0,
            val_cursor: 0,
            phantom: PhantomData,
        };
        if cursor.key_valid(self) {
            cursor.rewind_vals(self);
        }
        cursor
    }

    fn len(&self) -> usize {
        self.updates
    }

    fn description(&self) -> &Description<T> {
        &self.description
    }
}

impl<K, V, T, R> Batch for FactBatch<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    type Merger = FactMerger<K, V, T, R>;

    fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<T>) -> Self::Merger {
        FactMerger::new(self, other, compaction_frontier)
    }

    fn empty(lower: Antichain<T>, upper: Antichain<T>) -> Self {
        Self {
            storage: Default::default(),
            description: Description::new(
                lower,
                upper,
                Antichain::from_elem(timely::progress::Timestamp::minimum()),
            ),
            updates: 0,
        }
    }
}

/// Galloping search over `[start..end)`.
///
/// Returns the count of indices from `start` where `pred(i)` is true, assuming
/// `pred` is monotone (all true values come before all false ones). Doubles step
/// size until `pred` flips, then binary-searches within the last interval.
///
/// For small ranges (< 8), falls back to linear scan.
fn galloping_advance(start: usize, end: usize, pred: impl Fn(usize) -> bool) -> usize {
    const SMALL: usize = 16;
    if end - start < SMALL {
        let mut i = start;
        while i < end && pred(i) {
            i += 1;
        }
        return i - start;
    }
    // Gallop: double step until predicate flips to false.
    let mut step = 1usize;
    let mut i = start;
    while i + step < end && pred(i + step) {
        i += step;
        step <<= 1;
    }
    // Binary search within [i, min(i + step, end)).
    step = step.min(end - i);
    while step > 1 {
        let half = step / 2;
        if i + half < end && pred(i + half) {
            i += half;
        }
        step -= half;
    }
    // Advance past last matching element.
    if i < end && pred(i) {
        i += 1;
    }
    i - start
}

// --- Cursor ---

/// A cursor for navigating a [`FactBatch`].
///
/// Holds two absolute indices: `key_cursor` into the key level, and `val_cursor`
/// into the value level. Update iteration is done within [`map_times`](Cursor::map_times)
/// using [`child_range`] on the leaf bounds.
pub struct FactCursor<K, V, T, R> {
    /// Absolute position in the key level.
    key_cursor: usize,
    /// Absolute position in the value level.
    val_cursor: usize,
    phantom: PhantomData<(K, V, T, R)>,
}

impl<K, V, T, R> WithLayout for FactCursor<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    type Layout = FactLayout<K, V, T, R>;
}

impl<K, V, T, R> Cursor for FactCursor<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    type Storage = FactBatch<K, V, T, R>;

    #[inline]
    fn key_valid(&self, storage: &Self::Storage) -> bool {
        self.key_cursor < storage.key_count()
    }

    #[inline]
    fn val_valid(&self, storage: &Self::Storage) -> bool {
        self.val_cursor < storage.val_range(self.key_cursor).end
    }

    #[inline]
    fn key<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, K> {
        storage.storage.lists.values.borrow().get(self.key_cursor)
    }

    #[inline]
    fn val<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, V> {
        storage
            .storage
            .rest
            .lists
            .values
            .borrow()
            .get(self.val_cursor)
    }

    fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, K>> {
        if self.key_valid(storage) {
            Some(self.key(storage))
        } else {
            None
        }
    }

    fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, V>> {
        if self.val_valid(storage) {
            Some(self.val(storage))
        } else {
            None
        }
    }

    fn map_times<L: FnMut(columnar::Ref<'_, T>, columnar::Ref<'_, R>)>(
        &mut self,
        storage: &Self::Storage,
        mut logic: L,
    ) {
        let range = storage.upd_range(self.val_cursor);
        let times = storage.storage.rest.rest.values.0.borrow();
        let diffs = storage.storage.rest.rest.values.1.borrow();
        for i in range {
            logic(times.get(i), diffs.get(i));
        }
    }

    fn step_key(&mut self, storage: &Self::Storage) {
        self.key_cursor += 1;
        if self.key_valid(storage) {
            self.rewind_vals(storage);
        } else {
            self.key_cursor = storage.key_count();
        }
    }

    #[inline]
    fn seek_key(&mut self, storage: &Self::Storage, key: columnar::Ref<'_, K>) {
        let end = storage.key_count();
        let keys = storage.storage.lists.values.borrow();
        let count = galloping_advance(self.key_cursor, end, |i| {
            K::reborrow(keys.get(i)).lt(&K::reborrow(key))
        });
        self.key_cursor += count;
        if self.key_valid(storage) {
            self.rewind_vals(storage);
        }
    }

    fn step_val(&mut self, storage: &Self::Storage) {
        self.val_cursor += 1;
        if !self.val_valid(storage) {
            self.val_cursor = storage.val_range(self.key_cursor).end;
        }
    }

    fn seek_val(&mut self, storage: &Self::Storage, val: columnar::Ref<'_, V>) {
        let end = storage.val_range(self.key_cursor).end;
        let vals = storage.storage.rest.lists.values.borrow();
        let count = galloping_advance(self.val_cursor, end, |i| {
            V::reborrow(vals.get(i)).lt(&V::reborrow(val))
        });
        self.val_cursor += count;
    }

    fn rewind_keys(&mut self, storage: &Self::Storage) {
        self.key_cursor = 0;
        if self.key_valid(storage) {
            self.rewind_vals(storage);
        }
    }

    fn rewind_vals(&mut self, storage: &Self::Storage) {
        self.val_cursor = storage.val_range(self.key_cursor).start;
    }
}

// --- Merger ---

/// Key-by-key trie merger for [`FactBatch`].
///
/// Merges two batches by interleaving keys, merging val subtrees for matching
/// keys, applying time compaction via `advance_by`, and consolidating updates
/// (accumulating diffs and dropping zeros). Fuel-based interruption at key level.
pub struct FactMerger<K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// Key cursor into the first source batch.
    key_cursor1: usize,
    /// Key cursor into the second source batch.
    key_cursor2: usize,
    /// Result being assembled.
    result: KVUpdates<K, V, T, R>,
    /// Description for the merged batch.
    description: Description<T>,
    /// Staging area for update consolidation during merge.
    staging: Vec<(T, R)>,
    /// Total updates sealed into result.
    update_count: usize,
}

impl<K, V, T, R> Merger<FactBatch<K, V, T, R>> for FactMerger<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    fn new(
        batch1: &FactBatch<K, V, T, R>,
        batch2: &FactBatch<K, V, T, R>,
        compaction_frontier: AntichainRef<T>,
    ) -> Self {
        assert!(batch1.upper() == batch2.lower());
        let mut since = batch1
            .description()
            .since()
            .join(batch2.description().since());
        since = since.join(&compaction_frontier.to_owned());

        let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

        FactMerger {
            key_cursor1: 0,
            key_cursor2: 0,
            result: Default::default(),
            description,
            staging: Vec::new(),
            update_count: 0,
        }
    }

    fn work(
        &mut self,
        source1: &FactBatch<K, V, T, R>,
        source2: &FactBatch<K, V, T, R>,
        fuel: &mut isize,
    ) {
        let starting_updates = self.update_count;
        let mut effort = 0isize;

        let key_count1 = source1.key_count();
        let key_count2 = source2.key_count();

        // Merge keys from both sources.
        while self.key_cursor1 < key_count1 && self.key_cursor2 < key_count2 && effort < *fuel {
            self.merge_key(source1, source2);
            effort = isize::try_from(self.update_count - starting_updates)
                .expect("update count fits isize");
        }

        // Copy remaining keys from source1.
        while self.key_cursor1 < key_count1 && effort < *fuel {
            self.copy_key(&source1.storage, self.key_cursor1);
            self.key_cursor1 += 1;
            effort = isize::try_from(self.update_count - starting_updates)
                .expect("update count fits isize");
        }

        // Copy remaining keys from source2.
        while self.key_cursor2 < key_count2 && effort < *fuel {
            self.copy_key(&source2.storage, self.key_cursor2);
            self.key_cursor2 += 1;
            effort = isize::try_from(self.update_count - starting_updates)
                .expect("update count fits isize");
        }

        *fuel -= effort;
    }

    fn done(mut self) -> FactBatch<K, V, T, R> {
        // Seal the outer group bounds if we pushed any keys.
        let key_len = Len::len(&self.result.lists.values.borrow());
        if key_len > 0 {
            columnar::Push::push(&mut self.result.lists.bounds, u64::cast_from(key_len));
        }
        FactBatch {
            updates: self.update_count,
            storage: self.result,
            description: self.description,
        }
    }
}

impl<K, V, T, R> FactMerger<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    /// Compare and merge the next key from both sources.
    fn merge_key(&mut self, source1: &FactBatch<K, V, T, R>, source2: &FactBatch<K, V, T, R>) {
        let key1 = source1.storage.lists.values.borrow().get(self.key_cursor1);
        let key2 = source2.storage.lists.values.borrow().get(self.key_cursor2);

        use std::cmp::Ordering;
        match K::reborrow(key1).cmp(&K::reborrow(key2)) {
            Ordering::Less => {
                self.copy_key(&source1.storage, self.key_cursor1);
                self.key_cursor1 += 1;
            }
            Ordering::Equal => {
                let vr1 = source1.val_range(self.key_cursor1);
                let vr2 = source2.val_range(self.key_cursor2);
                let init_vals = Len::len(&self.result.rest.lists.values.borrow());

                self.merge_vals(
                    &source1.storage,
                    vr1.start,
                    vr1.end,
                    &source2.storage,
                    vr2.start,
                    vr2.end,
                );

                // Only push key if any vals survived consolidation.
                if Len::len(&self.result.rest.lists.values.borrow()) > init_vals {
                    columnar::Push::push(
                        &mut self.result.lists.values,
                        source1.storage.lists.values.borrow().get(self.key_cursor1),
                    );
                    columnar::Push::push(
                        &mut self.result.rest.lists.bounds,
                        u64::cast_from(Len::len(&self.result.rest.lists.values.borrow())),
                    );
                }

                self.key_cursor1 += 1;
                self.key_cursor2 += 1;
            }
            Ordering::Greater => {
                self.copy_key(&source2.storage, self.key_cursor2);
                self.key_cursor2 += 1;
            }
        }
    }

    /// Copy one key's subtree from `source` with time compaction + consolidation.
    #[inline]
    fn copy_key(&mut self, source: &KVUpdates<K, V, T, R>, key_idx: usize) {
        let init_vals = Len::len(&self.result.rest.lists.values.borrow());
        let vr = child_range(source.rest.lists.bounds.borrow(), key_idx);

        for val_idx in vr {
            self.stash_updates(source, val_idx);
            if self.seal_updates() {
                columnar::Push::push(
                    &mut self.result.rest.lists.values,
                    source.rest.lists.values.borrow().get(val_idx),
                );
            }
        }

        // Only push key if any vals survived.
        if Len::len(&self.result.rest.lists.values.borrow()) > init_vals {
            columnar::Push::push(
                &mut self.result.lists.values,
                source.lists.values.borrow().get(key_idx),
            );
            columnar::Push::push(
                &mut self.result.rest.lists.bounds,
                u64::cast_from(Len::len(&self.result.rest.lists.values.borrow())),
            );
        }
    }

    /// Merge val ranges from both sources.
    fn merge_vals(
        &mut self,
        source1: &KVUpdates<K, V, T, R>,
        mut lower1: usize,
        upper1: usize,
        source2: &KVUpdates<K, V, T, R>,
        mut lower2: usize,
        upper2: usize,
    ) {
        while lower1 < upper1 && lower2 < upper2 {
            let v1 = source1.rest.lists.values.borrow().get(lower1);
            let v2 = source2.rest.lists.values.borrow().get(lower2);

            use std::cmp::Ordering;
            match V::reborrow(v1).cmp(&V::reborrow(v2)) {
                Ordering::Less => {
                    self.stash_updates(source1, lower1);
                    if self.seal_updates() {
                        columnar::Push::push(
                            &mut self.result.rest.lists.values,
                            source1.rest.lists.values.borrow().get(lower1),
                        );
                    }
                    lower1 += 1;
                }
                Ordering::Equal => {
                    self.stash_updates(source1, lower1);
                    self.stash_updates(source2, lower2);
                    if self.seal_updates() {
                        columnar::Push::push(
                            &mut self.result.rest.lists.values,
                            source1.rest.lists.values.borrow().get(lower1),
                        );
                    }
                    lower1 += 1;
                    lower2 += 1;
                }
                Ordering::Greater => {
                    self.stash_updates(source2, lower2);
                    if self.seal_updates() {
                        columnar::Push::push(
                            &mut self.result.rest.lists.values,
                            source2.rest.lists.values.borrow().get(lower2),
                        );
                    }
                    lower2 += 1;
                }
            }
        }

        // Remaining vals from source1.
        while lower1 < upper1 {
            self.stash_updates(source1, lower1);
            if self.seal_updates() {
                columnar::Push::push(
                    &mut self.result.rest.lists.values,
                    source1.rest.lists.values.borrow().get(lower1),
                );
            }
            lower1 += 1;
        }

        // Remaining vals from source2.
        while lower2 < upper2 {
            self.stash_updates(source2, lower2);
            if self.seal_updates() {
                columnar::Push::push(
                    &mut self.result.rest.lists.values,
                    source2.rest.lists.values.borrow().get(lower2),
                );
            }
            lower2 += 1;
        }
    }

    /// Collect `(time, diff)` pairs for a val, applying time compaction.
    #[inline]
    fn stash_updates(&mut self, source: &KVUpdates<K, V, T, R>, val_idx: usize) {
        let range = child_range(source.rest.rest.bounds.borrow(), val_idx);
        let times = source.rest.rest.values.0.borrow();
        let diffs = source.rest.rest.values.1.borrow();
        let since = self.description.since().borrow();
        self.staging.reserve(range.len());
        self.staging.extend(range.map(|i| {
            let mut time = T::into_owned(times.get(i));
            time.advance_by(since);
            let diff = R::into_owned(diffs.get(i));
            (time, diff)
        }));
    }

    /// Consolidate staged updates, push non-zero results. Returns true if non-empty.
    fn seal_updates(&mut self) -> bool {
        differential_dataflow::consolidation::consolidate(&mut self.staging);
        if self.staging.is_empty() {
            return false;
        }
        self.update_count += self.staging.len();
        for (time, diff) in self.staging.drain(..) {
            columnar::Push::push(&mut self.result.rest.rest.values.0, &time);
            columnar::Push::push(&mut self.result.rest.rest.values.1, &diff);
        }
        columnar::Push::push(
            &mut self.result.rest.rest.bounds,
            u64::cast_from(Len::len(&self.result.rest.rest.values.0.borrow())),
        );
        true
    }
}

// --- Builder ---

/// A builder for [`FactBatch`] that receives sorted factorized trie chunks.
///
/// Chunks arrive pre-sorted and pre-consolidated from the batcher pipeline, with
/// consecutive chunks holding disjoint key ranges. [`done`](Builder::done) flattens
/// the chain through [`KVUpdates::form`] to produce a single fully-deduplicated trie.
pub struct FactBuilder<K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// Accumulated trie chunks; flattened in `done()`.
    chunks: Vec<KVUpdates<K, V, T, R>>,
}

impl<K, V, T, R> Builder for FactBuilder<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    type Input = KVUpdates<K, V, T, R>;
    type Time = T;
    type Output = FactBatch<K, V, T, R>;

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        FactBuilder { chunks: Vec::new() }
    }

    fn push(&mut self, chunk: &mut Self::Input) {
        self.chunks.push(std::mem::take(chunk));
    }

    fn done(self, description: Description<T>) -> FactBatch<K, V, T, R> {
        let updates: usize = self.chunks.iter().map(|c| c.len()).sum();
        let storage = KVUpdates::<K, V, T, R>::form(self.chunks.iter().flat_map(|c| c.iter()));
        FactBatch {
            storage,
            description,
            updates,
        }
    }

    fn seal(chain: &mut Vec<Self::Input>, description: Description<T>) -> FactBatch<K, V, T, R> {
        let mut builder = Self::with_capacity(0, 0, 0);
        for mut chunk in chain.drain(..) {
            builder.push(&mut chunk);
        }
        builder.done(description)
    }
}

// --- Column-input Builder ---

/// A builder for [`FactBatch`] that accepts [`FactColumn`] chunks.
///
/// Parallel to [`FactBuilder`]. `FactBuilder::Input` is `KVUpdates` (fed by
/// the batcher pipeline); `FactColBuilder::Input` is `FactColumn` (fed by
/// `reduce_abelian` / `threshold_arrangement`, which need a `Bu::Input` that
/// implements `Container + Default + ClearContainer + PushInto<((K, V), T, R)>`).
///
/// On `push`, the builder unwraps the `Typed` variant and takes ownership of
/// its inner `KVUpdates`. `Bytes`/`Align` variants are not expected in this
/// path (they arise from deserialization, not from reduce output) and panic.
/// `done` flattens the chain through [`KVUpdates::form`] exactly like
/// [`FactBuilder::done`].
pub struct FactColBuilder<K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// Accumulated trie chunks; flattened in `done()`.
    chunks: Vec<KVUpdates<K, V, T, R>>,
}

impl<K, V, T, R> Builder for FactColBuilder<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    V: Columnar + Ord + Clone + 'static,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    type Input = FactColumn<K, V, T, R>;
    type Time = T;
    type Output = FactBatch<K, V, T, R>;

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        FactColBuilder { chunks: Vec::new() }
    }

    fn push(&mut self, chunk: &mut Self::Input) {
        match std::mem::take(chunk) {
            FactColumn::Typed(storage) => self.chunks.push(storage),
            FactColumn::Bytes(_) | FactColumn::Align(_) => {
                panic!("FactColBuilder::push received a non-Typed FactColumn variant");
            }
        }
    }

    fn done(self, description: Description<T>) -> FactBatch<K, V, T, R> {
        let updates: usize = self.chunks.iter().map(|c| c.len()).sum();
        let storage = KVUpdates::<K, V, T, R>::form(self.chunks.iter().flat_map(|c| c.iter()));
        FactBatch {
            storage,
            description,
            updates,
        }
    }

    fn seal(chain: &mut Vec<Self::Input>, description: Description<T>) -> FactBatch<K, V, T, R> {
        let mut builder = Self::with_capacity(0, 0, 0);
        for mut chunk in chain.drain(..) {
            builder.push(&mut chunk);
        }
        builder.done(description)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a FactBatch from sorted `(K, V, T, R)` tuples.
    fn make_batch(data: &[(u64, u64, u64, i64)]) -> FactBatch<u64, u64, u64, i64> {
        let refs: Vec<_> = data.iter().map(|(k, v, t, d)| (k, v, (t, d))).collect();
        let storage = KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());
        let updates = data.len();
        FactBatch {
            storage,
            description: Description::new(
                Antichain::from_elem(0u64),
                Antichain::from_elem(1000u64),
                Antichain::from_elem(0u64),
            ),
            updates,
        }
    }

    /// Build a FactBatch with custom description.
    fn make_batch_desc(
        data: &[(u64, u64, u64, i64)],
        lower: u64,
        upper: u64,
    ) -> FactBatch<u64, u64, u64, i64> {
        let refs: Vec<_> = data.iter().map(|(k, v, t, d)| (k, v, (t, d))).collect();
        let storage = KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());
        let updates = data.len();
        FactBatch {
            storage,
            description: Description::new(
                Antichain::from_elem(lower),
                Antichain::from_elem(upper),
                Antichain::from_elem(0u64),
            ),
            updates,
        }
    }

    /// Collect all `(K, V, Vec<(T, R)>)` from a cursor traversal.
    fn collect_cursor(batch: &FactBatch<u64, u64, u64, i64>) -> Vec<(u64, u64, Vec<(u64, i64)>)> {
        let mut result = Vec::new();
        let mut cursor = batch.cursor();
        while cursor.key_valid(batch) {
            while cursor.val_valid(batch) {
                let k = *cursor.key(batch);
                let v = *cursor.val(batch);
                let mut times = Vec::new();
                cursor.map_times(batch, |t, d| {
                    times.push((*t, *d));
                });
                result.push((k, v, times));
                cursor.step_val(batch);
            }
            cursor.step_key(batch);
        }
        result
    }

    #[mz_ore::test]
    fn test_cursor_traversal() {
        let batch = make_batch(&[
            (1, 10, 100, 1),
            (1, 10, 200, 1),
            (1, 20, 300, -1),
            (2, 30, 400, 1),
        ]);

        let result = collect_cursor(&batch);
        assert_eq!(
            result,
            vec![
                (1, 10, vec![(100, 1), (200, 1)]),
                (1, 20, vec![(300, -1)]),
                (2, 30, vec![(400, 1)]),
            ]
        );
    }

    #[mz_ore::test]
    fn test_cursor_empty_batch() {
        let batch = make_batch(&[]);
        let result = collect_cursor(&batch);
        assert!(result.is_empty());
    }

    #[mz_ore::test]
    fn test_cursor_single_element() {
        let batch = make_batch(&[(42, 7, 100, 1)]);
        let result = collect_cursor(&batch);
        assert_eq!(result, vec![(42, 7, vec![(100, 1)])]);
    }

    #[mz_ore::test]
    fn test_cursor_seek_key() {
        let batch = make_batch(&[
            (1, 10, 100, 1),
            (3, 20, 200, 1),
            (5, 30, 300, 1),
            (7, 40, 400, 1),
        ]);

        let mut cursor = batch.cursor();

        // Seek to existing key.
        cursor.seek_key(&batch, &3u64);
        assert!(cursor.key_valid(&batch));
        assert_eq!(*cursor.key(&batch), 3);

        // Seek to non-existing key — lands on next.
        cursor.rewind_keys(&batch);
        cursor.seek_key(&batch, &4u64);
        assert!(cursor.key_valid(&batch));
        assert_eq!(*cursor.key(&batch), 5);

        // Seek past all keys.
        cursor.seek_key(&batch, &100u64);
        assert!(!cursor.key_valid(&batch));
    }

    #[mz_ore::test]
    fn test_cursor_seek_val() {
        let batch = make_batch(&[(1, 10, 100, 1), (1, 20, 200, 1), (1, 30, 300, 1)]);

        let mut cursor = batch.cursor();
        assert!(cursor.key_valid(&batch));

        // Seek to existing val.
        cursor.seek_val(&batch, &20u64);
        assert!(cursor.val_valid(&batch));
        assert_eq!(*cursor.val(&batch), 20);

        // Seek to non-existing val — lands on next.
        cursor.rewind_vals(&batch);
        cursor.seek_val(&batch, &15u64);
        assert!(cursor.val_valid(&batch));
        assert_eq!(*cursor.val(&batch), 20);

        // Seek past all vals.
        cursor.seek_val(&batch, &100u64);
        assert!(!cursor.val_valid(&batch));
    }

    #[mz_ore::test]
    fn test_cursor_rewind_keys() {
        let batch = make_batch(&[(1, 10, 100, 1), (2, 20, 200, 1)]);

        let mut cursor = batch.cursor();
        cursor.step_key(&batch);
        assert_eq!(*cursor.key(&batch), 2);
        cursor.rewind_keys(&batch);
        assert_eq!(*cursor.key(&batch), 1);
    }

    #[mz_ore::test]
    fn test_cursor_get_key_val() {
        let batch = make_batch(&[(1, 10, 100, 1)]);
        let mut cursor = batch.cursor();

        assert_eq!(cursor.get_key(&batch), Some(&1u64));
        assert_eq!(cursor.get_val(&batch), Some(&10u64));

        cursor.step_key(&batch);
        assert_eq!(cursor.get_key(&batch), None);
    }

    #[mz_ore::test]
    fn test_merge_non_overlapping_keys() {
        let batch1 = make_batch_desc(&[(1, 10, 100, 1), (3, 30, 300, 1)], 0, 500);
        let batch2 = make_batch_desc(&[(5, 50, 400, 1), (7, 70, 500, 1)], 500, 1000);

        let mut merger = batch1.begin_merge(&batch2, AntichainRef::new(&[]));
        merger.work(&batch1, &batch2, &mut 1000);
        let merged = merger.done();

        let result = collect_cursor(&merged);
        assert_eq!(
            result,
            vec![
                (1, 10, vec![(100, 1)]),
                (3, 30, vec![(300, 1)]),
                (5, 50, vec![(400, 1)]),
                (7, 70, vec![(500, 1)]),
            ]
        );
    }

    #[mz_ore::test]
    fn test_merge_overlapping_keys() {
        let batch1 = make_batch_desc(&[(1, 10, 100, 1), (2, 20, 200, 1)], 0, 500);
        let batch2 = make_batch_desc(&[(1, 10, 300, 2), (3, 30, 400, 1)], 500, 1000);

        let mut merger = batch1.begin_merge(&batch2, AntichainRef::new(&[]));
        merger.work(&batch1, &batch2, &mut 1000);
        let merged = merger.done();

        let result = collect_cursor(&merged);
        assert_eq!(
            result,
            vec![
                (1, 10, vec![(100, 1), (300, 2)]),
                (2, 20, vec![(200, 1)]),
                (3, 30, vec![(400, 1)]),
            ]
        );
    }

    #[mz_ore::test]
    fn test_merge_consolidation() {
        // Same key, same val, same time — diffs should consolidate to zero.
        let batch1 = make_batch_desc(&[(1, 10, 100, 3)], 0, 500);
        let batch2 = make_batch_desc(&[(1, 10, 100, -3)], 500, 1000);

        let mut merger = batch1.begin_merge(&batch2, AntichainRef::new(&[]));
        merger.work(&batch1, &batch2, &mut 1000);
        let merged = merger.done();

        // +3 and -3 consolidate to zero → no output.
        assert_eq!(merged.len(), 0);
        let result = collect_cursor(&merged);
        assert!(result.is_empty());
    }

    #[mz_ore::test]
    fn test_merge_time_compaction() {
        // Two different times compact to one when compaction frontier covers both.
        let batch1 = make_batch_desc(&[(1, 10, 100, 1)], 0, 500);
        let batch2 = make_batch_desc(&[(1, 10, 200, 1)], 500, 1000);

        // Compact to time 500: both 100 and 200 advance to 500.
        let frontier = Antichain::from_elem(500u64);
        let mut merger = batch1.begin_merge(&batch2, frontier.borrow());
        merger.work(&batch1, &batch2, &mut 1000);
        let merged = merger.done();

        let result = collect_cursor(&merged);
        // Both updates compact to time 500 and consolidate: diff 1+1=2.
        assert_eq!(result, vec![(1, 10, vec![(500, 2)])]);
    }

    /// Helper: build a sorted `KVUpdates` chunk from `(K, V, T, R)` tuples.
    fn make_chunk_u64(data: &[(u64, u64, u64, i64)]) -> KVUpdates<u64, u64, u64, i64> {
        KVUpdates::<u64, u64, u64, i64>::form(data.iter().map(|(k, v, t, d)| (k, v, (t, d))))
    }

    #[mz_ore::test]
    fn test_builder_single_chunk() {
        let mut chunk = make_chunk_u64(&[
            (1, 10, 100, 1),
            (1, 10, 200, 1),
            (1, 20, 300, -1),
            (2, 30, 400, 1),
        ]);
        let mut builder = FactBuilder::<u64, u64, u64, i64>::with_capacity(0, 0, 0);
        builder.push(&mut chunk);
        let batch = builder.done(Description::new(
            Antichain::from_elem(0u64),
            Antichain::from_elem(1000u64),
            Antichain::from_elem(0u64),
        ));

        let result = collect_cursor(&batch);
        assert_eq!(
            result,
            vec![
                (1, 10, vec![(100, 1), (200, 1)]),
                (1, 20, vec![(300, -1)]),
                (2, 30, vec![(400, 1)]),
            ]
        );
    }

    #[mz_ore::test]
    fn test_builder_multiple_chunks() {
        // Chunks must hold disjoint key ranges, matching the batcher invariant.
        let mut chunk1 = make_chunk_u64(&[(1, 10, 100, 1), (1, 20, 200, 1)]);
        let mut chunk2 = make_chunk_u64(&[(2, 30, 300, 1), (3, 40, 400, -1)]);

        let mut builder = FactBuilder::<u64, u64, u64, i64>::with_capacity(0, 0, 0);
        builder.push(&mut chunk1);
        builder.push(&mut chunk2);
        let batch = builder.done(Description::new(
            Antichain::from_elem(0u64),
            Antichain::from_elem(1000u64),
            Antichain::from_elem(0u64),
        ));

        let result = collect_cursor(&batch);
        assert_eq!(
            result,
            vec![
                (1, 10, vec![(100, 1)]),
                (1, 20, vec![(200, 1)]),
                (2, 30, vec![(300, 1)]),
                (3, 40, vec![(400, -1)]),
            ]
        );
    }

    #[mz_ore::test]
    fn test_builder_empty() {
        let mut builder = FactBuilder::<u64, u64, u64, i64>::with_capacity(0, 0, 0);
        builder.push(&mut KVUpdates::<u64, u64, u64, i64>::default());
        let batch = builder.done(Description::new(
            Antichain::from_elem(0u64),
            Antichain::from_elem(1000u64),
            Antichain::from_elem(0u64),
        ));
        assert_eq!(batch.len(), 0);
        let result = collect_cursor(&batch);
        assert!(result.is_empty());
    }

    #[mz_ore::test]
    fn test_builder_seal() {
        let mut chain = vec![
            make_chunk_u64(&[(1, 10, 100, 1)]),
            make_chunk_u64(&[(2, 20, 200, 1)]),
        ];
        let batch = FactBuilder::<u64, u64, u64, i64>::seal(
            &mut chain,
            Description::new(
                Antichain::from_elem(0u64),
                Antichain::from_elem(1000u64),
                Antichain::from_elem(0u64),
            ),
        );
        let result = collect_cursor(&batch);
        assert_eq!(
            result,
            vec![(1, 10, vec![(100, 1)]), (2, 20, vec![(200, 1)]),]
        );
    }

    /// Validate bounds for variable-length byte-vector keys/values.
    ///
    /// `Vec<u8>` has the same columnar shape as `mz_repr::Row`: owned type
    /// is a heap-allocated sequence, `Ref<'a>` is a borrowed slice view
    /// (`Slice<&[u8]>` here; `&RowRef` for Row). If bounds satisfy for
    /// `Vec<u8>`, they satisfy for `Row`.
    #[mz_ore::test]
    fn test_byte_vec_keyed_batch() {
        let mut tuples: Vec<((Vec<u8>, Vec<u8>), u64, i64)> = vec![
            ((b"a".to_vec(), b"x".to_vec()), 100, 1),
            ((b"a".to_vec(), b"x".to_vec()), 200, 1),
            ((b"a".to_vec(), b"y".to_vec()), 100, -1),
            ((b"b".to_vec(), b"z".to_vec()), 100, 2),
        ];
        tuples.sort();
        let mut chunk = KVUpdates::<Vec<u8>, Vec<u8>, u64, i64>::form(
            tuples.iter().map(|((k, v), t, r)| (k, v, (t, r))),
        );

        let mut builder = FactBuilder::<Vec<u8>, Vec<u8>, u64, i64>::with_capacity(0, 0, 0);
        builder.push(&mut chunk);
        let batch = builder.done(Description::new(
            Antichain::from_elem(0u64),
            Antichain::from_elem(1000u64),
            Antichain::from_elem(0u64),
        ));

        // Count entries via cursor traversal (avoids Slice equality).
        let mut key_count = 0usize;
        let mut val_count = 0usize;
        let mut update_count = 0usize;
        let mut cursor = batch.cursor();
        while cursor.key_valid(&batch) {
            key_count += 1;
            while cursor.val_valid(&batch) {
                val_count += 1;
                cursor.map_times(&batch, |_, _| update_count += 1);
                cursor.step_val(&batch);
            }
            cursor.step_key(&batch);
        }
        assert_eq!(key_count, 2, "keys: a, b");
        assert_eq!(val_count, 3, "vals: (a,x), (a,y), (b,z)");
        assert_eq!(update_count, 4);

        // Merge two Vec<u8>-keyed batches.
        let tuples2: Vec<((Vec<u8>, Vec<u8>), u64, i64)> =
            vec![((b"a".to_vec(), b"x".to_vec()), 300, 1)];
        let mut chunk2 = KVUpdates::<Vec<u8>, Vec<u8>, u64, i64>::form(
            tuples2.iter().map(|((k, v), t, r)| (k, v, (t, r))),
        );
        let mut b2 = FactBuilder::<Vec<u8>, Vec<u8>, u64, i64>::with_capacity(0, 0, 0);
        b2.push(&mut chunk2);
        let batch2 = b2.done(Description::new(
            Antichain::from_elem(1000u64),
            Antichain::from_elem(2000u64),
            Antichain::from_elem(0u64),
        ));
        let mut merger = batch.begin_merge(&batch2, AntichainRef::new(&[]));
        merger.work(&batch, &batch2, &mut 1_000_000);
        let merged = merger.done();
        assert_eq!(merged.len(), 5);
    }
}
