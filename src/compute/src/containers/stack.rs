// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A chunked columnar container based on the columnation library. It stores the local
//! portion in region-allocated data, too, which is different to the `TimelyStack` type.

use std::collections::Bound;
use std::ops::{Index, RangeBounds};
use std::rc::Rc;

use differential_dataflow::trace::implementations::merge_batcher_col::ColumnatedMergeBatcher;
use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder};
use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::{BatchContainer, Layout, Update};
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
use timely::container::columnation::{Columnation, Region};

use crate::containers::array::Array;
use crate::row_spine::OffsetOptimized;

pub type ColValSpine<K, V, T, R> = Spine<
    Rc<OrdValBatch<MzStack<((K, V), T, R)>>>,
    ColumnatedMergeBatcher<K, V, T, R>,
    RcBuilder<OrdValBuilder<MzStack<((K, V), T, R)>>>,
>;

pub type ColKeySpine<K, T, R> = Spine<
    Rc<OrdKeyBatch<MzStack<((K, ()), T, R)>>>,
    ColumnatedMergeBatcher<K, (), T, R>,
    RcBuilder<OrdKeyBuilder<MzStack<((K, ()), T, R)>>>,
>;

/// A layout based on chunked timely stacks
pub struct MzStack<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

impl<U: Update> Layout for MzStack<U>
where
    U::Key: Columnation + 'static,
    U::Val: Columnation + 'static,
    U::Time: Columnation,
    U::Diff: Columnation,
{
    type Target = U;
    type KeyContainer = ChunkedStack<U::Key>;
    type ValContainer = ChunkedStack<U::Val>;
    type UpdContainer = ChunkedStack<(U::Time, U::Diff)>;
    type OffsetContainer = OffsetOptimized;
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
        self.inner.reserve_regions(regions.map(|cs| &cs.inner));
    }

    /// Copies an element in to the region.
    ///
    /// The element can be read by indexing
    #[inline(always)]
    pub fn copy(&mut self, item: &T) {
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
        let size_of = std::mem::size_of::<T>();
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
}

// The `ToOwned` requirement exists to satisfy `self.reserve_items`, who must for now
// be presented with the actual contained type, rather than a type that borrows into it.
impl<T: Ord + Columnation + ToOwned<Owned = T> + 'static> BatchContainer for ChunkedStack<T> {
    type PushItem = T;
    type ReadItem<'a> = &'a Self::PushItem;

    #[inline]
    fn copy(&mut self, item: &T) {
        self.copy(item);
    }
    #[inline]
    fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
        let range = other.range(start..end);
        self.reserve_items(range);
        for item in range {
            self.copy(item);
        }
    }

    #[inline]
    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        let mut new = Self::with_capacity(cont1.length + cont2.length);
        new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
        new
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

impl<A: Columnation, B: Columnation, C: Columnation> ChunkedStack<(A, B, C)> {
    /// Copies a destructured tuple `(A, B, C)` into this column stack.
    ///
    /// This serves situations where a tuple should be constructed from its constituents but
    /// not all elements are available as owned data.
    ///
    /// The element can be read by indexing
    pub fn copy_destructured(&mut self, r0: &A, r1: &B, r2: &C) {
        let copy = unsafe { self.inner.copy_destructured(r0, r1, r2) };
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
        for array in &mut self.local {
            // SAFETY: All elements in `array` have their allocations in a region. We drop the
            // region and forget the immediate values.
            unsafe {
                array.set_len(0);
            }
        }
        // Important: Clear the region before dropping it to avoid double frees.
        self.inner.clear();
    }
}
