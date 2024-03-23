// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A fixed-length columnar stack.

use std::ops::Deref;

use timely::container::columnation::{Columnation, Region};

use crate::containers::array::Array;

/// A fixed-length columnar stack.
///
/// Clients can push a fixed number of times until it reaches capacity. At this point, any further
/// push will result in a panic.
pub struct FixedStack<T: Columnation> {
    /// The local elements, stored in a region.
    elements: Array<T>,
    /// Spill space.
    region: T::InnerRegion,
}

impl<T: Columnation> FixedStack<T> {
    /// Construct a new [`FixedStack`] with a given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            elements: Array::with_capacity(capacity),
            region: T::InnerRegion::default(),
        }
    }

    /// The number of elements this stack can absorb.
    pub fn capacity(&self) -> usize {
        self.elements.capacity()
    }

    /// Drop all elements from this stack.
    pub fn clear(&mut self) {
        // SAFETY: Forget local elements
        unsafe {
            self.elements.set_len(0);
        }
        self.region.clear()
    }

    /// Copy an element onto this stack. Panics if there is no capacity left.
    ///
    /// Note that the panics are unrecoverable and can lead to memory leaks/corruption.
    pub fn copy(&mut self, item: &T) {
        let copy = unsafe { self.region.copy(item) };
        self.elements.push(copy);
    }

    /// Visit contained allocations to determine their size and capacity.
    pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
        self.region.heap_size(&mut callback);
        self.elements.heap_size(callback);
    }

    /// Ensures `Self` can absorb `items` without further allocations.
    ///
    /// The argument `items` may be cloned and iterated multiple times.
    /// Please be careful if it contains side effects.
    pub fn reserve_items<'a, I>(&'a mut self, items: I)
    where
        I: Iterator<Item = &'a T> + Clone,
    {
        self.region.reserve_items(items);
    }
}

impl<A: Columnation, B: Columnation, C: Columnation> FixedStack<(A, B, C)> {
    /// Similarly to `copy`, copies the individual components of a tree-tuple.
    pub fn copy_destructured(&mut self, r0: &A, r1: &B, r2: &C) {
        let copy = unsafe { self.region.copy_destructured(r0, r1, r2) };
        self.elements.push(copy)
    }
}

impl<T: Columnation> Default for FixedStack<T> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl<T: Columnation> Deref for FixedStack<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &*self.elements
    }
}

impl<T: Columnation> Drop for FixedStack<T> {
    fn drop(&mut self) {
        // Forget elements, drop region.
        unsafe {
            self.elements.set_len(0);
        }
        self.region.clear();
    }
}
