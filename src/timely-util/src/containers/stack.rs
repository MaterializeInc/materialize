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

//! A chunked columnar container based on the columnation library. It stores the local
//! portion in region-allocated data, too, which is different to the `TimelyStack` type.

use std::cell::Cell;
use std::collections::Bound;
use std::ops::{Index, RangeBounds};
use std::sync::atomic::AtomicBool;

use differential_dataflow::trace::implementations::BatchContainer;
use either::Either;
use timely::container::columnation::{Columnation, Region, TimelyStack};
use timely::container::{Container, ContainerBuilder, PushInto, SizableContainer};

use crate::containers::array::Array;

static ENABLE_CHUNKED_STACK: AtomicBool = AtomicBool::new(false);

/// A runtime-configurable wrapper around timely stacks and chunked stacks.
#[derive(Clone)]
pub enum StackWrapper<T: Columnation> {
    Legacy(TimelyStack<T>),
    Chunked(ChunkedStack<T>),
}

/// Runtime switch to select the stack implementation. `true` to use [`ChunkedStack`],
/// `false` to select [`TimelyStack`].
pub fn use_chunked_stack(enable: bool) {
    ENABLE_CHUNKED_STACK.store(enable, std::sync::atomic::Ordering::Relaxed);
}

impl<T: Columnation> StackWrapper<T> {
    #[inline]
    fn with_capacity(size: usize) -> Self {
        if ENABLE_CHUNKED_STACK.load(std::sync::atomic::Ordering::Relaxed) {
            Self::Chunked(ChunkedStack::with_capacity(size))
        } else {
            Self::Legacy(TimelyStack::with_capacity(size))
        }
    }

    /// Return the capacity of the local buffer.
    pub fn capacity(&self) -> usize {
        match self {
            Self::Legacy(stack) => stack.capacity(),
            Self::Chunked(stack) => stack.capacity(),
        }
    }

    /// Estimate the memory capacity in bytes.
    #[inline]
    pub fn heap_size(&self, callback: impl FnMut(usize, usize)) {
        match self {
            StackWrapper::Legacy(stack) => stack.heap_size(callback),
            StackWrapper::Chunked(stack) => stack.heap_size(callback),
        }
    }
}

// The `ToOwned` requirement exists to satisfy `self.reserve_items`, who must for now
// be presented with the actual contained type, rather than a type that borrows into it.
impl<T: Ord + Columnation + Clone + 'static> BatchContainer for StackWrapper<T> {
    type Owned = T;
    type ReadItem<'a> = &'a Self::Owned;

    #[inline]
    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        use StackWrapper::*;
        match (cont1, cont2) {
            (Legacy(cont1), Legacy(cont2)) => {
                let mut new = TimelyStack::with_capacity(cont1.len() + cont2.len());
                new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
                Self::Legacy(new)
            }
            (Chunked(cont1), Chunked(cont2)) => {
                let mut new = ChunkedStack::with_capacity(cont1.len() + cont2.len());
                new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
                Self::Chunked(new)
            }
            (cont1, cont2) => {
                // We don't have a good way to estimate the result region size
                // if the two inputs are different. This should only happen after
                // toggling the flag at runtime, which mh@ assumes to be a rare
                // event.
                Self::with_capacity(BatchContainer::len(cont1) + BatchContainer::len(cont2))
            }
        }
    }

    fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
        item
    }

    #[inline]
    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        match self {
            StackWrapper::Legacy(stack) => stack.index(index),
            StackWrapper::Chunked(stack) => stack.index(index),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            StackWrapper::Legacy(stack) => stack.len(),
            StackWrapper::Chunked(stack) => stack.length,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            StackWrapper::Legacy(stack) => BatchContainer::is_empty(stack),
            StackWrapper::Chunked(stack) => stack.is_empty(),
        }
    }
}

impl<T: Clone + Columnation + 'static> Container for StackWrapper<T> {
    type ItemRef<'a>
        = &'a T
    where
        Self: 'a;
    type Item<'a>
        = &'a T
    where
        Self: 'a;

    fn len(&self) -> usize {
        match self {
            StackWrapper::Legacy(legacy) => legacy.len(),
            StackWrapper::Chunked(chunked) => chunked.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            StackWrapper::Legacy(legacy) => legacy.is_empty(),
            StackWrapper::Chunked(chunked) => chunked.is_empty(),
        }
    }

    fn clear(&mut self) {
        match self {
            StackWrapper::Legacy(legacy) => legacy.clear(),
            StackWrapper::Chunked(chunked) => chunked.clear(),
        }
    }

    type Iter<'a> =
        Either<<TimelyStack<T> as Container>::Iter<'a>, <ChunkedStack<T> as Container>::Iter<'a>>;

    fn iter(&self) -> Self::Iter<'_> {
        match self {
            StackWrapper::Legacy(legacy) => Either::Left(legacy.iter()),
            StackWrapper::Chunked(chunked) => Either::Right(chunked.iter()),
        }
    }

    type DrainIter<'a> = Either<
        <TimelyStack<T> as Container>::DrainIter<'a>,
        <ChunkedStack<T> as Container>::DrainIter<'a>,
    >;

    fn drain(&mut self) -> Self::DrainIter<'_> {
        match self {
            StackWrapper::Legacy(legacy) => Either::Left(legacy.drain()),
            StackWrapper::Chunked(chunked) => Either::Right(chunked.drain()),
        }
    }
}

impl<T: Clone + Columnation + 'static> SizableContainer for StackWrapper<T> {
    fn at_capacity(&self) -> bool {
        match self {
            StackWrapper::Legacy(ts) => ts.at_capacity(),
            StackWrapper::Chunked(chunked) => chunked.len() == chunked.capacity(),
        }
    }

    fn ensure_capacity(&mut self, stash: &mut Option<Self>) {
        let (mut stash_ts, mut stash_chunked) = match stash.take() {
            Some(StackWrapper::Legacy(ts)) => (Some(ts), None),
            Some(StackWrapper::Chunked(chunked)) => (None, Some(chunked)),
            None => (None, None),
        };
        match self {
            StackWrapper::Legacy(ts) => ts.ensure_capacity(&mut stash_ts),
            StackWrapper::Chunked(chunked) => {
                if chunked.capacity() == 0 {
                    *chunked = stash_chunked.take().unwrap_or_default();
                    chunked.clear();
                }
                let preferred = timely::container::buffer::default_capacity::<T>();
                if chunked.capacity() < preferred {
                    chunked.reserve(preferred - chunked.capacity());
                }
            }
        }
    }
}

impl<T: Columnation + ToOwned<Owned = T> + 'static> PushInto<T> for StackWrapper<T> {
    fn push_into(&mut self, item: T) {
        match self {
            StackWrapper::Legacy(stack) => stack.copy(&item),
            StackWrapper::Chunked(stack) => stack.push_into(&item),
        }
    }
}

impl<T: Columnation + ToOwned<Owned = T> + 'static> PushInto<&T> for StackWrapper<T> {
    fn push_into(&mut self, item: &T) {
        match self {
            StackWrapper::Legacy(stack) => stack.copy(item),
            StackWrapper::Chunked(stack) => stack.push_into(item),
        }
    }
}

impl<T: Columnation> Default for StackWrapper<T> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl<A: Columnation, B: Columnation, C: Columnation> StackWrapper<(A, B, C)> {
    /// Copies a destructured tuple `(A, B, C)` into this column stack.
    ///
    /// This serves situations where a tuple should be constructed from its constituents but not
    /// not all elements are available as owned data.
    pub fn copy_destructured(&mut self, a: &A, b: &B, c: &C) {
        match self {
            Self::Legacy(stack) => stack.copy_destructured(a, b, c),
            Self::Chunked(stack) => stack.copy_destructured(a, b, c),
        }
    }
}

/// A Stacked container builder that keep track of container memory usage.
#[derive(Default)]
pub struct AccountedStackBuilder<CB> {
    pub bytes: Cell<usize>,
    pub builder: CB,
}

impl<T, CB> ContainerBuilder for AccountedStackBuilder<CB>
where
    T: Clone + Columnation + 'static,
    CB: ContainerBuilder<Container = StackWrapper<T>>,
{
    type Container = StackWrapper<T>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        let container = self.builder.extract()?;
        let mut new_bytes = 0;
        container.heap_size(|_, cap| new_bytes += cap);
        self.bytes.set(self.bytes.get() + new_bytes);
        Some(container)
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        let container = self.builder.finish()?;
        let mut new_bytes = 0;
        container.heap_size(|_, cap| new_bytes += cap);
        self.bytes.set(self.bytes.get() + new_bytes);
        Some(container)
    }
}

impl<T, CB: PushInto<T>> PushInto<T> for AccountedStackBuilder<CB> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.builder.push_into(item);
    }
}

/// An append-only vector that store records as columns.
///
/// This container maintains elements that might conventionally own
/// memory allocations, but instead the pointers to those allocations
/// reference larger regions of memory shared with multiple instances
/// of the type. Elements can be retrieved as references, and care is
/// taken when this type is dropped to ensure that the correct memory
/// is returned (rather than the incorrect memory, from running the
/// elements `Drop` implementations).
pub struct ChunkedStack<T: Columnation> {
    local: Vec<Array<T>>,
    inner: T::InnerRegion,
    length: usize,
}

impl<T: Columnation> ChunkedStack<T> {
    /// The capacity of each individual chunk, in number of elements. Should be a power of two.
    const CHUNK: usize = 64 << 10;

    /// Construct a [`ChunkedStack`], reserving space for `capacity` elements
    ///
    /// Note that the associated region is not initialized to a specific capacity
    /// because we can't generally know how much space would be required.
    pub fn with_capacity(capacity: usize) -> Self {
        let local = Vec::with_capacity((capacity + Self::CHUNK - 1) / Self::CHUNK);
        Self {
            local,
            inner: T::InnerRegion::default(),
            length: 0,
        }
    }

    /// The capacity of the local array.
    pub fn capacity(&self) -> usize {
        self.local.capacity() * Self::CHUNK
    }

    /// Ensures `Self` can absorb `items` without further allocations.
    ///
    /// The argument `items` may be cloned and iterated multiple times.
    /// Please be careful if it contains side effects.
    #[inline(always)]
    pub fn reserve_items<'a, I>(&'a mut self, items: I)
    where
        I: Iterator<Item = &'a T> + Clone,
    {
        self.inner.reserve_items(items);
    }

    /// Ensures `Self` can absorb `regions` without further allocations.
    ///
    /// The argument `regions` may be cloned and iterated multiple times.
    /// Please be careful if it contains side effects.
    #[inline(always)]
    pub fn reserve_regions<'a, I>(&mut self, regions: I)
    where
        Self: 'a,
        I: Iterator<Item = &'a Self> + Clone,
    {
        self.inner.reserve_regions(regions.map(|cs| &cs.inner));
    }

    /// Copies an element in to the region.
    ///
    /// The element can be read by indexing
    #[inline(always)]
    pub fn copy(&mut self, item: &T) {
        // SAFETY: We never drop the `T` returned from `copy`, satisfying its invariant.
        let copy = unsafe { self.inner.copy(item) };
        self.push(copy);
    }

    /// Internal helper to push a copied item onto the local storage. The `item` must be allocated
    /// in the region, because it will not be dropped.
    fn push(&mut self, item: T) {
        if Some(true) != self.local.last().map(|last| last.len() < Self::CHUNK) {
            self.local.push(Array::with_capacity(Self::CHUNK));
        }
        let chunk = self.local.last_mut().unwrap();
        chunk.push(item);
        self.length += 1;
    }

    /// Estimate the memory capacity in bytes.
    #[inline]
    pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
        let size_of = std::mem::size_of::<Array<T>>();
        callback(self.local.len() * size_of, self.local.capacity() * size_of);
        for local in &self.local {
            local.heap_size(&mut callback);
        }
        self.inner.heap_size(callback);
    }

    /// Iterate over a range of elements. Panics if the range mentions non-existent elements,
    /// i.e., its end is past the last element of this container.
    #[inline(always)]
    pub fn range(&self, r: impl RangeBounds<usize> + std::fmt::Debug) -> Iter<'_, T> {
        let offset = match r.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => x.checked_add(1).unwrap(),
            Bound::Unbounded => 0,
        };
        let limit = match r.end_bound() {
            Bound::Included(x) => x.checked_sub(1).unwrap(),
            Bound::Excluded(x) => *x,
            Bound::Unbounded => self.length,
        };
        debug_assert!(offset <= limit, "Incorrect range bounds: {r:?}");
        debug_assert!(
            limit <= self.length,
            "Limit {limit} exceeds length {}",
            self.length
        );

        Iter {
            stack: self,
            offset,
            limit,
        }
    }

    /// Lookup a specific element.
    #[inline(always)]
    fn index(&self, index: usize) -> &T {
        let chunk = index / Self::CHUNK;
        let offset = index & (Self::CHUNK - 1);
        &self.local[chunk][offset]
    }

    /// The number of elements we store.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Test if this container is empty.
    pub fn is_empty(&self) -> bool {
        self.local.is_empty()
    }

    /// Reserve space for `additional` elements.
    pub fn reserve(&mut self, additional: usize) {
        let additional_chunks = (additional + Self::CHUNK - 1) / Self::CHUNK;
        self.local.reserve(additional_chunks);
    }

    /// Empties the collection.
    pub fn clear(&mut self) {
        for array in &mut self.local {
            // SAFETY: All elements in `array` have their allocations in a region. We drop the
            // region and forget the immediate values. We would try to drop region-allocated
            // data through the objects in `array`, which is UB.
            unsafe {
                array.set_len(0);
            }
        }
        // Important: Clear the region before dropping it to avoid double frees.
        // Regions in columnation do not necessarily have a `Drop` implementation, so we need to
        // make sure they release their contents before dropping.
        self.inner.clear();
        self.length = 0;
    }
}

// The `ToOwned` requirement exists to satisfy `self.reserve_items`, who must for now
// be presented with the actual contained type, rather than a type that borrows into it.
impl<T: Ord + Columnation + ToOwned<Owned = T> + 'static> BatchContainer for ChunkedStack<T> {
    type Owned = T;
    type ReadItem<'a> = &'a Self::Owned;

    #[inline]
    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        let mut new = Self::with_capacity(cont1.length + cont2.length);
        new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
        new
    }

    fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
        item
    }

    #[inline]
    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        self.index(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

impl<T: Columnation + 'static> Container for ChunkedStack<T> {
    type ItemRef<'a>
        = &'a T
    where
        Self: 'a;
    type Item<'a>
        = &'a T
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn clear(&mut self) {
        self.clear()
    }

    type Iter<'a> = Iter<'a, T>;

    fn iter(&self) -> Self::Iter<'_> {
        self.range(..)
    }

    type DrainIter<'a> = Iter<'a, T>;

    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.range(..)
    }
}

impl<T: Columnation> PushInto<&T> for ChunkedStack<T> {
    fn push_into(&mut self, item: &T) {
        self.copy(item);
    }
}

impl<T: Columnation> Index<usize> for ChunkedStack<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        ChunkedStack::index(self, index)
    }
}

impl<T: Columnation> Default for ChunkedStack<T> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl<T: Columnation> Clone for ChunkedStack<T> {
    fn clone(&self) -> Self {
        let mut new: Self = Default::default();
        for item in self.range(..) {
            new.copy(item);
        }
        new
    }

    fn clone_from(&mut self, source: &Self) {
        self.clear();
        for item in source.range(..) {
            self.copy(item);
        }
    }
}

impl<A: Columnation, B: Columnation, C: Columnation> ChunkedStack<(A, B, C)> {
    /// Copies a destructured tuple `(A, B, C)` into this column stack.
    ///
    /// This serves situations where a tuple should be constructed from its constituents but not
    /// not all elements are available as owned data.
    pub fn copy_destructured(&mut self, a: &A, b: &B, c: &C) {
        // SAFETY: We never drop the `T` returned from `copy_destructured`, satisfying its
        // invariant.
        let copy = unsafe { self.inner.copy_destructured(a, b, c) };
        self.push(copy);
    }
}

/// An iterator of a [`ChunkedStack`].
pub struct Iter<'a, T: Columnation> {
    stack: &'a ChunkedStack<T>,
    offset: usize,
    limit: usize,
}

impl<'a, T: Columnation> Clone for Iter<'a, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T: Columnation> Copy for Iter<'a, T> {}

impl<'a, T: Columnation> Iterator for Iter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.limit {
            None
        } else {
            let next = self.stack.index(self.offset);
            self.offset = self.offset.saturating_add(1);
            Some(next)
        }
    }
}

impl<T: Columnation> Drop for ChunkedStack<T> {
    fn drop(&mut self) {
        self.clear();
    }
}
