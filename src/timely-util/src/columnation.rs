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

//! Columnation-based containers vendored from differential-dataflow.
//!
//! This module provides [`ColumnationStack`], a columnar container that stores data using the
//! columnation library, along with [`ColumnationChunker`] for organizing streams into sorted
//! chunks, and the [`InternalMerge`] implementation needed by the merge batcher.

use std::collections::VecDeque;
use std::iter::FromIterator;

use columnation::{Columnation, Region};
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::merge_batcher::container::InternalMerge;
use differential_dataflow::trace::implementations::{BatchContainer, BuilderInput};
use timely::container::{ContainerBuilder, DrainContainer, PushInto, SizableContainer};
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::{Accountable, PartialOrder};

// ---------------------------------------------------------------------------
// ColumnationStack
// ---------------------------------------------------------------------------

/// An append-only vector that stores records as columns.
///
/// This container maintains elements that might conventionally own
/// memory allocations, but instead the pointers to those allocations
/// reference larger regions of memory shared with multiple instances
/// of the type. Elements can be retrieved as references, and care is
/// taken when this type is dropped to ensure that the correct memory
/// is returned (rather than the incorrect memory, from running the
/// elements `Drop` implementations).
pub struct ColumnationStack<T: Columnation> {
    local: Vec<T>,
    inner: T::InnerRegion,
}

impl<T: Columnation> ColumnationStack<T> {
    /// Construct a [`ColumnationStack`], reserving space for `capacity` elements.
    ///
    /// Note that the associated region is not initialized to a specific capacity
    /// because we can't generally know how much space would be required.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            local: Vec::with_capacity(capacity),
            inner: T::InnerRegion::default(),
        }
    }

    /// Ensures `Self` can absorb `items` without further allocations.
    ///
    /// The argument `items` may be cloned and iterated multiple times.
    /// Please be careful if it contains side effects.
    #[inline(always)]
    pub fn reserve_items<'a, I>(&mut self, items: I)
    where
        I: Iterator<Item = &'a T> + Clone,
        T: 'a,
    {
        self.local.reserve(items.clone().count());
        self.inner.reserve_items(items);
    }

    /// Ensures `Self` can absorb `items` without further allocations.
    ///
    /// The argument `items` may be cloned and iterated multiple times.
    /// Please be careful if it contains side effects.
    #[inline(always)]
    pub fn reserve_regions<'a, I>(&mut self, regions: I)
    where
        Self: 'a,
        I: Iterator<Item = &'a Self> + Clone,
    {
        self.local
            .reserve(regions.clone().map(|cs| cs.local.len()).sum());
        self.inner.reserve_regions(regions.map(|cs| &cs.inner));
    }

    /// Copies an element into the region.
    ///
    /// The element can be read by indexing.
    pub fn copy(&mut self, item: &T) {
        unsafe {
            self.local.push(self.inner.copy(item));
        }
    }

    /// Empties the collection.
    pub fn clear(&mut self) {
        unsafe {
            self.local.set_len(0);
            self.inner.clear();
        }
    }

    /// Retain elements that pass a predicate, from a specified offset.
    ///
    /// This method may or may not reclaim memory in the inner region.
    pub fn retain_from<P: FnMut(&T) -> bool>(&mut self, index: usize, mut predicate: P) {
        let mut write_position = index;
        for position in index..self.local.len() {
            if predicate(&self[position]) {
                self.local.swap(position, write_position);
                write_position += 1;
            }
        }
        unsafe {
            // Unsafety justified in that `write_position` is no greater than
            // `self.local.len()` and so this exposes no invalid data.
            self.local.set_len(write_position);
        }
    }

    /// Unsafe access to `local` data. The slices store data that is backed by a region
    /// allocation. Therefore, it is undefined behavior to mutate elements of the `local` slice.
    ///
    /// # Safety
    /// Elements within `local` can be reordered, but not mutated, removed and/or dropped.
    pub unsafe fn local(&mut self) -> &mut [T] {
        &mut self.local[..]
    }

    /// Estimate the memory capacity in bytes.
    #[inline]
    pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
        let size_of = std::mem::size_of::<T>();
        callback(self.local.len() * size_of, self.local.capacity() * size_of);
        self.inner.heap_size(callback);
    }

    /// Estimate the consumed memory capacity in bytes, summing both used and total capacity.
    #[inline]
    pub fn summed_heap_size(&self) -> (usize, usize) {
        let (mut length, mut capacity) = (0, 0);
        self.heap_size(|len, cap| {
            length += len;
            capacity += cap
        });
        (length, capacity)
    }

    /// The length in items.
    #[inline]
    pub fn len(&self) -> usize {
        self.local.len()
    }

    /// Returns `true` if the stack is empty.
    pub fn is_empty(&self) -> bool {
        self.local.is_empty()
    }

    /// The capacity of the local vector.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.local.capacity()
    }

    /// Reserve space for `additional` elements.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.local.reserve(additional)
    }
}

impl<A: Columnation, B: Columnation> ColumnationStack<(A, B)> {
    /// Copies a destructured tuple `(A, B)` into this column stack.
    pub fn copy_destructured(&mut self, t1: &A, t2: &B) {
        unsafe {
            self.local.push(self.inner.copy_destructured(t1, t2));
        }
    }
}

impl<A: Columnation, B: Columnation, C: Columnation> ColumnationStack<(A, B, C)> {
    /// Copies a destructured tuple `(A, B, C)` into this column stack.
    pub fn copy_destructured(&mut self, r0: &A, r1: &B, r2: &C) {
        unsafe {
            self.local.push(self.inner.copy_destructured(r0, r1, r2));
        }
    }
}

impl<T: Columnation> std::ops::Deref for ColumnationStack<T> {
    type Target = [T];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.local[..]
    }
}

impl<T: Columnation> Drop for ColumnationStack<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<T: Columnation> Default for ColumnationStack<T> {
    fn default() -> Self {
        Self {
            local: Vec::new(),
            inner: T::InnerRegion::default(),
        }
    }
}

impl<'a, A: 'a + Columnation> FromIterator<&'a A> for ColumnationStack<A> {
    fn from_iter<I: IntoIterator<Item = &'a A>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut c = ColumnationStack::<A>::with_capacity(iter.size_hint().0);
        for element in iter {
            c.copy(element);
        }
        c
    }
}

impl<T: Columnation + PartialEq> PartialEq for ColumnationStack<T> {
    fn eq(&self, other: &Self) -> bool {
        PartialEq::eq(&self[..], &other[..])
    }
}

impl<T: Columnation + Eq> Eq for ColumnationStack<T> {}

impl<T: Columnation + std::fmt::Debug> std::fmt::Debug for ColumnationStack<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self[..].fmt(f)
    }
}

impl<T: Columnation> Clone for ColumnationStack<T> {
    fn clone(&self) -> Self {
        let mut new: Self = Default::default();
        for item in &self[..] {
            new.copy(item);
        }
        new
    }

    fn clone_from(&mut self, source: &Self) {
        self.clear();
        for item in &source[..] {
            self.copy(item);
        }
    }
}

impl<T: Columnation> PushInto<T> for ColumnationStack<T> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.copy(&item);
    }
}

impl<T: Columnation> PushInto<&T> for ColumnationStack<T> {
    #[inline]
    fn push_into(&mut self, item: &T) {
        self.copy(item);
    }
}

impl<T: Columnation> PushInto<&&T> for ColumnationStack<T> {
    #[inline]
    fn push_into(&mut self, item: &&T) {
        self.copy(*item);
    }
}

// Container trait impls

impl<T: Columnation> Accountable for ColumnationStack<T> {
    #[inline]
    fn record_count(&self) -> i64 {
        i64::try_from(self.local.len()).unwrap()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.local.is_empty()
    }
}

impl<T: Columnation> DrainContainer for ColumnationStack<T> {
    type Item<'a>
        = &'a T
    where
        Self: 'a;
    type DrainIter<'a>
        = std::slice::Iter<'a, T>
    where
        Self: 'a;
    #[inline]
    fn drain(&mut self) -> Self::DrainIter<'_> {
        (*self).iter()
    }
}

impl<T: Columnation> SizableContainer for ColumnationStack<T> {
    fn at_capacity(&self) -> bool {
        self.len() == self.capacity()
    }
    fn ensure_capacity(&mut self, stash: &mut Option<Self>) {
        if self.capacity() == 0 {
            *self = stash.take().unwrap_or_default();
            self.clear();
        }
        let preferred = timely::container::buffer::default_capacity::<T>();
        if self.capacity() < preferred {
            self.reserve(preferred - self.capacity());
        }
    }
}

impl<T: Clone + Ord + Columnation + 'static> BatchContainer for ColumnationStack<T> {
    type Owned = T;
    type ReadItem<'a> = &'a T;

    #[inline(always)]
    fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned {
        item.clone()
    }
    #[inline(always)]
    fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) {
        other.clone_from(item);
    }

    fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
        item
    }

    fn push_ref(&mut self, item: Self::ReadItem<'_>) {
        self.push_into(item)
    }
    fn push_own(&mut self, item: &Self::Owned) {
        self.push_into(item)
    }

    fn clear(&mut self) {
        self.clear()
    }

    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }
    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        let mut new = Self::default();
        new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
        new
    }
    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        &self[index]
    }
    fn len(&self) -> usize {
        self[..].len()
    }
}

impl<K, V, T, R> BuilderInput<K, V> for ColumnationStack<((K::Owned, V::Owned), T, R)>
where
    K: for<'a> BatchContainer<
            ReadItem<'a>: PartialEq<&'a K::Owned>,
            Owned: Ord + Columnation + Clone + 'static,
        >,
    V: for<'a> BatchContainer<
            ReadItem<'a>: PartialEq<&'a V::Owned>,
            Owned: Ord + Columnation + Clone + 'static,
        >,
    T: Timestamp + Lattice + Columnation + Clone + 'static,
    R: Ord + Clone + Semigroup + Columnation + 'static,
{
    type Key<'a> = &'a K::Owned;
    type Val<'a> = &'a V::Owned;
    type Time = T;
    type Diff = R;

    fn into_parts<'a>(
        ((key, val), time, diff): Self::Item<'a>,
    ) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff) {
        (key, val, time.clone(), diff.clone())
    }

    fn key_eq(this: &&K::Owned, other: K::ReadItem<'_>) -> bool {
        K::reborrow(other) == *this
    }

    fn val_eq(this: &&V::Owned, other: V::ReadItem<'_>) -> bool {
        V::reborrow(other) == *this
    }

    fn key_val_upd_counts(chain: &[Self]) -> (usize, usize, usize) {
        let mut keys = 0;
        let mut vals = 0;
        let mut upds = 0;
        let mut prev_keyval = None;
        for link in chain.iter() {
            for ((key, val), _, _) in link.iter() {
                if let Some((p_key, p_val)) = prev_keyval {
                    if p_key != key {
                        keys += 1;
                        vals += 1;
                    } else if p_val != val {
                        vals += 1;
                    }
                } else {
                    keys += 1;
                    vals += 1;
                }
                upds += 1;
                prev_keyval = Some((key, val));
            }
        }
        (keys, vals, upds)
    }
}

// ---------------------------------------------------------------------------
// ColumnationChunker
// ---------------------------------------------------------------------------

/// Chunk a stream of vectors into chains of columnation stacks.
///
/// This chunker accumulates into a `Vec` (not a `ColumnationStack`) for efficient
/// in-place sorting and consolidation, then copies the consolidated results
/// into `ColumnationStack` chunks. This avoids the cost of sorting through
/// columnation indirection.
pub struct ColumnationChunker<T: Columnation> {
    pending: Vec<T>,
    ready: VecDeque<ColumnationStack<T>>,
    empty: Option<ColumnationStack<T>>,
}

impl<T: Columnation> Default for ColumnationChunker<T> {
    fn default() -> Self {
        Self {
            pending: Vec::default(),
            ready: VecDeque::default(),
            empty: None,
        }
    }
}

impl<D, T, R> ColumnationChunker<(D, T, R)>
where
    D: Columnation + Ord,
    T: Columnation + Ord,
    R: Columnation + Semigroup,
{
    const BUFFER_SIZE_BYTES: usize = 64 << 10;

    fn chunk_capacity() -> usize {
        let size = std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    fn form_chunk(&mut self) {
        consolidate_updates(&mut self.pending);
        if self.pending.len() >= Self::chunk_capacity() {
            while self.pending.len() > Self::chunk_capacity() {
                let mut chunk = ColumnationStack::with_capacity(Self::chunk_capacity());
                for item in self.pending.drain(..chunk.capacity()) {
                    chunk.copy(&item);
                }
                self.ready.push_back(chunk);
            }
        }
    }
}

impl<'a, D, T, R> PushInto<&'a mut Vec<(D, T, R)>> for ColumnationChunker<(D, T, R)>
where
    D: Columnation + Ord + Clone,
    T: Columnation + Ord + Clone,
    R: Columnation + Semigroup + Clone,
{
    fn push_into(&mut self, container: &'a mut Vec<(D, T, R)>) {
        if self.pending.capacity() < Self::chunk_capacity() * 2 {
            self.pending
                .reserve(Self::chunk_capacity() * 2 - self.pending.len());
        }

        let mut drain = container.drain(..).peekable();
        while drain.peek().is_some() {
            self.pending
                .extend((&mut drain).take(self.pending.capacity() - self.pending.len()));
            if self.pending.len() == self.pending.capacity() {
                self.form_chunk();
            }
        }
    }
}

impl<D, T, R> ContainerBuilder for ColumnationChunker<(D, T, R)>
where
    D: Columnation + Ord + Clone + 'static,
    T: Columnation + Ord + Clone + 'static,
    R: Columnation + Semigroup + Clone + 'static,
{
    type Container = ColumnationStack<(D, T, R)>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.empty = Some(ready);
            self.empty.as_mut()
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        consolidate_updates(&mut self.pending);
        while !self.pending.is_empty() {
            let mut chunk = ColumnationStack::with_capacity(Self::chunk_capacity());
            for item in self
                .pending
                .drain(..std::cmp::min(self.pending.len(), chunk.capacity()))
            {
                chunk.copy(&item);
            }
            self.ready.push_back(chunk);
        }
        self.empty = self.ready.pop_front();
        self.empty.as_mut()
    }
}

// ---------------------------------------------------------------------------
// InternalMerge impl for ColumnationStack
// ---------------------------------------------------------------------------

impl<D, T, R> InternalMerge for ColumnationStack<(D, T, R)>
where
    D: Ord + Columnation + Clone + 'static,
    T: Ord + Columnation + Clone + PartialOrder + 'static,
    R: Default + Semigroup + Columnation + Clone + 'static,
{
    type TimeOwned = T;

    fn len(&self) -> usize {
        self[..].len()
    }

    fn clear(&mut self) {
        ColumnationStack::clear(self)
    }

    fn account(&self) -> (usize, usize, usize, usize) {
        let (mut size, mut capacity, mut allocations) = (0, 0, 0);
        let cb = |siz, cap| {
            size += siz;
            capacity += cap;
            allocations += 1;
        };
        self.heap_size(cb);
        (self.len(), size, capacity, allocations)
    }

    fn merge_from(&mut self, others: &mut [Self], positions: &mut [usize]) {
        use std::cmp::Ordering;
        match others.len() {
            0 => {}
            1 => {
                let other = &mut others[0];
                let pos = &mut positions[0];
                if self[..].is_empty() && *pos == 0 {
                    std::mem::swap(self, other);
                    return;
                }
                for i in *pos..other[..].len() {
                    self.copy(&other[i]);
                }
                *pos = other[..].len();
            }
            2 => {
                let (left, right) = others.split_at_mut(1);
                let other1 = &left[0];
                let other2 = &right[0];

                let mut stash = R::default();

                while positions[0] < other1[..].len()
                    && positions[1] < other2[..].len()
                    && !self.at_capacity()
                {
                    let (d1, t1, _) = &other1[positions[0]];
                    let (d2, t2, _) = &other2[positions[1]];
                    match (d1, t1).cmp(&(d2, t2)) {
                        Ordering::Less => {
                            self.copy(&other1[positions[0]]);
                            positions[0] += 1;
                        }
                        Ordering::Greater => {
                            self.copy(&other2[positions[1]]);
                            positions[1] += 1;
                        }
                        Ordering::Equal => {
                            let (_, _, r1) = &other1[positions[0]];
                            let (_, _, r2) = &other2[positions[1]];
                            stash.clone_from(r1);
                            stash.plus_equals(r2);
                            if !stash.is_zero() {
                                let (d, t, _) = &other1[positions[0]];
                                self.copy_destructured(d, t, &stash);
                            }
                            positions[0] += 1;
                            positions[1] += 1;
                        }
                    }
                }
            }
            n => unimplemented!("{n}-way merge not yet supported"),
        }
    }

    fn extract(
        &mut self,
        position: &mut usize,
        upper: AntichainRef<T>,
        frontier: &mut Antichain<T>,
        keep: &mut Self,
        ship: &mut Self,
    ) {
        let len = self[..].len();
        while *position < len && !keep.at_capacity() && !ship.at_capacity() {
            let (data, time, diff) = &self[*position];
            if upper.less_equal(time) {
                frontier.insert_with(time, |time| time.clone());
                keep.copy_destructured(data, time, diff);
            } else {
                ship.copy_destructured(data, time, diff);
            }
            *position += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// ColInternalMerger type alias
// ---------------------------------------------------------------------------

/// A `Merger` using internal iteration for `ColumnationStack` containers.
pub type ColInternalMerger<D, T, R> =
    differential_dataflow::trace::implementations::merge_batcher::InternalMerger<
        ColumnationStack<(D, T, R)>,
    >;
