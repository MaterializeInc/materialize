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

//! Region-allocated data utilities.

use std::fmt::{Debug, Formatter};

/// A region allocator which holds items at stable memory locations.
///
/// Items once inserted will not be moved, and their locations in memory
/// can be relied on by others, until the region is cleared.
///
/// This type accepts owned data, rather than references, and does not
/// itself intend to implement `Region`. Rather, it is a useful building
/// block for other less-safe code that wants allocated data to remain at
/// fixed memory locations.
pub struct LgAllocRegion<T> {
    /// The active allocation into which we are writing.
    local: lgalloc::Region<T>,
    /// All previously active allocations.
    stash: Vec<lgalloc::Region<T>>,
    /// The maximum allocation size
    limit: usize,
}

// Manually implement `Default` as `T` may not implement it.
impl<T> Default for LgAllocRegion<T> {
    fn default() -> Self {
        Self {
            local: Default::default(),
            stash: Vec::new(),
            limit: usize::MAX,
        }
    }
}

impl<T> Debug for LgAllocRegion<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LgAllocRegion")
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl<T> LgAllocRegion<T> {
    /// Construct a [LgAllocRegion] with a allocation size limit.
    pub fn with_limit(limit: usize) -> Self {
        Self {
            local: Default::default(),
            stash: Default::default(),
            limit,
        }
    }

    /// Clears the contents without dropping any elements.
    #[inline]
    pub fn clear(&mut self) {
        unsafe {
            // Unsafety justified in that setting the length to zero exposes
            // no invalid data.
            self.local.clear();
            // Release allocations in `stash` without dropping their elements.
            self.stash.clear()
        }
    }
    /// Copies an iterator of items into the region.
    #[inline]
    pub fn copy_iter<I>(&mut self, items: I) -> &mut [T]
    where
        I: Iterator<Item = T> + std::iter::ExactSizeIterator,
    {
        self.reserve(items.len());
        let initial_len = self.local.len();
        self.local.extend(items);
        &mut self.local[initial_len..]
    }
    /// Copies a slice of cloneable items into the region.
    #[inline]
    pub fn copy_slice(&mut self, items: &[T]) -> &mut [T]
    where
        T: Clone,
    {
        self.reserve(items.len());
        let initial_len = self.local.len();
        self.local.extend_from_slice(items);
        &mut self.local[initial_len..]
    }

    /// Ensures that there is space in `self.local` to copy at least `count` items.
    #[inline(always)]
    pub fn reserve(&mut self, count: usize) {
        // Check if `item` fits into `self.local` without reallocation.
        // If not, stash `self.local` and increase the allocation.
        if count > self.local.capacity() - self.local.len() {
            // Increase allocated capacity in powers of two.
            // We could choose a different rule here if we wanted to be
            // more conservative with memory (e.g. page size allocations).
            let mut next_len = (self.local.capacity() + 1).next_power_of_two();
            next_len = std::cmp::min(next_len, self.limit);
            next_len = std::cmp::max(count, next_len);
            let new_local = lgalloc::Region::new_auto(next_len);
            if !self.local.is_empty() {
                self.stash.push(std::mem::take(&mut self.local));
            }
            self.local = new_local;
        }
    }

    /// Allocates a new `Self` that can accept `count` items without reallocation.
    pub fn with_capacity(count: usize) -> Self {
        let mut region = Self::default();
        region.reserve(count);
        region
    }

    /// The number of items current held in the region.
    pub fn len(&self) -> usize {
        self.local.len() + self.stash.iter().map(|r| r.len()).sum::<usize>()
    }

    /// Visit contained allocations to determine their size and capacity.
    #[inline]
    pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
        // Calculate heap size for local, stash, and stash entries
        let size_of_t = std::mem::size_of::<T>();
        callback(
            self.local.len() * size_of_t,
            self.local.capacity() * size_of_t,
        );
        callback(
            self.stash.len() * std::mem::size_of::<Vec<T>>(),
            self.stash.capacity() * std::mem::size_of::<Vec<T>>(),
        );
        for stash in &self.stash {
            callback(stash.len() * size_of_t, stash.capacity() * size_of_t);
        }
    }
}
