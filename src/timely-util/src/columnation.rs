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
//! chunks, and the [`ColInternalMerger`] [`Merger`] implementation needed by the merge batcher.

use std::collections::VecDeque;
use std::iter::FromIterator;

use columnation::{Columnation, Region};
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::merge_batcher::Merger;
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
// ColInternalMerger: a `Merger` for `ColumnationStack` chunks
// ---------------------------------------------------------------------------

/// A [`Merger`] using internal iteration over [`ColumnationStack`] chunks.
///
/// Implements differential-dataflow's chunk-list merge-batcher interface by
/// merging and splitting chains of sorted, consolidated `ColumnationStack`
/// chunks. Elements are copied through `copy` / `copy_destructured` rather than
/// moved out of their backing regions, which is why this operates by internal
/// iteration rather than draining owned tuples like the stock `VecMerger`.
pub struct ColInternalMerger<D, T, R> {
    _marker: std::marker::PhantomData<(D, T, R)>,
}

impl<D, T, R> Default for ColInternalMerger<D, T, R> {
    fn default() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<D, T, R> ColInternalMerger<D, T, R>
where
    D: Ord + Columnation + Clone + 'static,
    T: Ord + Columnation + Clone + PartialOrder + 'static,
    R: Default + Semigroup + Columnation + Clone + 'static,
{
    /// Target chunk capacity in elements, mirroring [`ColumnationChunker`].
    fn chunk_capacity() -> usize {
        const BUFFER_SIZE_BYTES: usize = 64 << 10;
        let size = std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            BUFFER_SIZE_BYTES
        } else if size <= BUFFER_SIZE_BYTES {
            BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    /// Acquire an empty chunk with the target capacity, recycling from `stash`.
    fn empty(stash: &mut Vec<ColumnationStack<(D, T, R)>>) -> ColumnationStack<(D, T, R)> {
        let target = Self::chunk_capacity();
        match stash.pop() {
            Some(mut chunk) if chunk.capacity() >= target => {
                chunk.clear();
                chunk
            }
            _ => ColumnationStack::with_capacity(target),
        }
    }

    /// Recycle a consumed chunk into `stash`, retaining its allocations.
    fn recycle(
        mut chunk: ColumnationStack<(D, T, R)>,
        stash: &mut Vec<ColumnationStack<(D, T, R)>>,
    ) {
        chunk.clear();
        stash.push(chunk);
    }

    /// Drain remaining items from one side into `result`/`output`.
    ///
    /// Copies the partially-consumed head into `result` (swapping wholesale
    /// when `result` is empty and the head untouched), then appends remaining
    /// full chunks directly to `output` without copying.
    fn drain_side(
        head: &mut ColumnationStack<(D, T, R)>,
        pos: &mut usize,
        list: &mut std::vec::IntoIter<ColumnationStack<(D, T, R)>>,
        result: &mut ColumnationStack<(D, T, R)>,
        output: &mut Vec<ColumnationStack<(D, T, R)>>,
        stash: &mut Vec<ColumnationStack<(D, T, R)>>,
    ) {
        // Copy the partially-consumed head into result.
        if *pos < head[..].len() {
            if result.is_empty() && *pos == 0 {
                std::mem::swap(result, head);
            } else {
                for i in *pos..head[..].len() {
                    result.copy(&head[i]);
                }
            }
            *pos = head[..].len();
        }
        // Flush result before appending full chunks.
        if !result.is_empty() {
            output.push(std::mem::replace(result, Self::empty(stash)));
        }
        // Remaining full chunks go directly to output.
        output.extend(list);
    }
}

impl<D, T, R> Merger for ColInternalMerger<D, T, R>
where
    D: Ord + Columnation + Clone + 'static,
    T: Ord + Columnation + Clone + PartialOrder + 'static,
    R: Default + Semigroup + Columnation + Clone + 'static,
{
    type Chunk = ColumnationStack<(D, T, R)>;
    type Time = T;

    fn merge(
        &mut self,
        list1: Vec<Self::Chunk>,
        list2: Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    ) {
        use std::cmp::Ordering;

        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();
        let mut head1 = list1.next().unwrap_or_default();
        let mut head2 = list2.next().unwrap_or_default();
        let mut pos1 = 0;
        let mut pos2 = 0;
        let mut result = Self::empty(stash);
        let mut diff = R::default();

        // Main merge loop: both sides have data.
        while pos1 < head1[..].len() && pos2 < head2[..].len() {
            // Tight inner loop bounded by the current heads and result capacity.
            while pos1 < head1[..].len() && pos2 < head2[..].len() && !result.at_capacity() {
                let (d1, t1, r1) = &head1[pos1];
                let (d2, t2, r2) = &head2[pos2];
                match (d1, t1).cmp(&(d2, t2)) {
                    Ordering::Less => {
                        result.copy(&head1[pos1]);
                        pos1 += 1;
                    }
                    Ordering::Greater => {
                        result.copy(&head2[pos2]);
                        pos2 += 1;
                    }
                    Ordering::Equal => {
                        diff.clone_from(r1);
                        diff.plus_equals(r2);
                        if !diff.is_zero() {
                            result.copy_destructured(d1, t1, &diff);
                        }
                        pos1 += 1;
                        pos2 += 1;
                    }
                }
            }
            if result.at_capacity() {
                output.push(std::mem::replace(&mut result, Self::empty(stash)));
            }
            if pos1 >= head1[..].len() {
                let old = std::mem::replace(&mut head1, list1.next().unwrap_or_default());
                Self::recycle(old, stash);
                pos1 = 0;
            }
            if pos2 >= head2[..].len() {
                let old = std::mem::replace(&mut head2, list2.next().unwrap_or_default());
                Self::recycle(old, stash);
                pos2 = 0;
            }
        }

        // After the loop at least one side is exhausted; draining it is a
        // no-op, and the other contributes its sorted tail in order — the
        // partial head by copy, remaining full chunks by passthrough.
        Self::drain_side(
            &mut head1,
            &mut pos1,
            &mut list1,
            &mut result,
            output,
            stash,
        );
        Self::drain_side(
            &mut head2,
            &mut pos2,
            &mut list2,
            &mut result,
            output,
            stash,
        );

        if !result.is_empty() {
            output.push(result);
        }
    }

    fn extract(
        &mut self,
        merged: Vec<Self::Chunk>,
        upper: AntichainRef<T>,
        frontier: &mut Antichain<T>,
        readied: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    ) {
        let mut keep = Self::empty(stash);
        let mut ready = Self::empty(stash);

        for chunk in merged {
            let len = chunk[..].len();
            for i in 0..len {
                let (data, time, diff) = &chunk[i];
                if upper.less_equal(time) {
                    frontier.insert_with(time, |time| time.clone());
                    keep.copy_destructured(data, time, diff);
                } else {
                    ready.copy_destructured(data, time, diff);
                }
                if keep.at_capacity() {
                    kept.push(std::mem::replace(&mut keep, Self::empty(stash)));
                }
                if ready.at_capacity() {
                    readied.push(std::mem::replace(&mut ready, Self::empty(stash)));
                }
            }
            // Chunk fully consumed; recycle its allocations.
            Self::recycle(chunk, stash);
        }

        if !keep.is_empty() {
            kept.push(keep);
        }
        if !ready.is_empty() {
            readied.push(ready);
        }
    }

    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
        let (mut size, mut capacity, mut allocations) = (0, 0, 0);
        chunk.heap_size(|siz, cap| {
            size += siz;
            capacity += cap;
            allocations += 1;
        });
        (chunk[..].len(), size, capacity, allocations)
    }
}

use crate::merge_batcher::{TemporalMerger, TimePartitioned, update_bounds};

/// The columnation-based [`TemporalMerger`]: [`ColInternalMerger`] plus the
/// recycled-chunk stash that differential's merge batcher would otherwise
/// carry. Chunks pass through unchanged, there is no offloaded
/// representation.
pub struct ColTemporalMerger<D, T, R>
where
    D: Columnation,
    T: Columnation,
    R: Columnation,
{
    inner: ColInternalMerger<D, T, R>,
    stash: Vec<ColumnationStack<(D, T, R)>>,
}

impl<D, T, R> Default for ColTemporalMerger<D, T, R>
where
    D: Columnation,
    T: Columnation,
    R: Columnation,
{
    fn default() -> Self {
        Self {
            inner: ColInternalMerger::default(),
            stash: Vec::new(),
        }
    }
}

impl<D, T, R> TemporalMerger for ColTemporalMerger<D, T, R>
where
    D: Ord + Columnation + Clone + 'static,
    T: Ord + Columnation + Clone + PartialOrder + 'static,
    R: Default + Semigroup + Columnation + Clone + 'static,
{
    type Time = T;
    type Chunk = ColumnationStack<(D, T, R)>;
    type Output = ColumnationStack<(D, T, R)>;

    fn absorb(&mut self, input: Self::Output) -> Self::Chunk {
        input
    }

    fn materialize(&mut self, chunk: Self::Chunk) -> Self::Output {
        chunk
    }

    fn merge(
        &mut self,
        list1: Vec<Self::Chunk>,
        list2: Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
    ) {
        Merger::merge(&mut self.inner, list1, list2, output, &mut self.stash);
    }

    fn chunk_len(chunk: &Self::Chunk) -> usize {
        chunk[..].len()
    }

    fn chunk_time_bounds(chunk: &Self::Chunk) -> Option<(T, T)> {
        let mut bounds = None;
        for (_, time, _) in &chunk[..] {
            update_bounds(&mut bounds, time);
        }
        bounds
    }

    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
        <ColInternalMerger<D, T, R> as Merger>::account(chunk)
    }

    fn seal_done(&mut self) {
        self.stash.clear();
    }

    fn extract_time_partitioned(
        &mut self,
        merged: Vec<Self::Chunk>,
        split_lo: AntichainRef<'_, T>,
        split_hi: AntichainRef<'_, T>,
        track_before: bool,
    ) -> TimePartitioned<Self::Chunk, T> {
        let stash = &mut self.stash;
        let mut result = TimePartitioned {
            before: Vec::new(),
            before_bounds: None,
            within: Vec::new(),
            within_bounds: None,
            beyond: Vec::new(),
            beyond_bounds: None,
            records: 0,
        };
        let mut before = ColInternalMerger::<D, T, R>::empty(stash);
        // The `within` and `beyond` parts are often empty (any seal without
        // future updates); allocate their builders lazily so such extracts
        // pay exactly the plain extract's allocations.
        let mut within: Option<Self::Chunk> = None;
        let mut beyond: Option<Self::Chunk> = None;

        for chunk in merged {
            let len = chunk[..].len();
            for i in 0..len {
                let (data, time, diff) = &chunk[i];
                result.records += 1;
                // NOTE: chunks are sorted data-major, so times arrive in no
                // particular order and every record updates the bounds.
                // Testing `before` first keeps the dominant case (ready
                // data) at the plain extract's single comparison.
                if !split_lo.less_equal(time) {
                    if track_before {
                        update_bounds(&mut result.before_bounds, time);
                    }
                    before.copy_destructured(data, time, diff);
                    if before.at_capacity() {
                        result.before.push(std::mem::replace(
                            &mut before,
                            ColInternalMerger::<D, T, R>::empty(stash),
                        ));
                    }
                } else if split_hi.less_equal(time) {
                    update_bounds(&mut result.beyond_bounds, time);
                    let builder =
                        beyond.get_or_insert_with(|| ColInternalMerger::<D, T, R>::empty(stash));
                    builder.copy_destructured(data, time, diff);
                    if builder.at_capacity() {
                        result.beyond.push(std::mem::replace(
                            builder,
                            ColInternalMerger::<D, T, R>::empty(stash),
                        ));
                    }
                } else {
                    update_bounds(&mut result.within_bounds, time);
                    let builder =
                        within.get_or_insert_with(|| ColInternalMerger::<D, T, R>::empty(stash));
                    builder.copy_destructured(data, time, diff);
                    if builder.at_capacity() {
                        result.within.push(std::mem::replace(
                            builder,
                            ColInternalMerger::<D, T, R>::empty(stash),
                        ));
                    }
                }
            }
            ColInternalMerger::<D, T, R>::recycle(chunk, stash);
        }

        for (builder, out) in [
            (Some(before), &mut result.before),
            (within, &mut result.within),
            (beyond, &mut result.beyond),
        ] {
            if let Some(builder) = builder {
                if builder[..].is_empty() {
                    ColInternalMerger::<D, T, R>::recycle(builder, stash);
                } else {
                    out.push(builder);
                }
            }
        }
        result
    }
}
