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
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

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
    local: Region<T>,
    /// All previously active allocations.
    stash: Vec<Region<T>>,
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
        #[cold]
        fn reserve_inner<T>(this: &mut LgAllocRegion<T>, count: usize) {
            // Increase allocated capacity in powers of two.
            // We could choose a different rule here if we wanted to be
            // more conservative with memory (e.g. page size allocations).
            let mut next_len = (this.local.capacity() + 1).next_power_of_two();
            next_len = std::cmp::min(next_len, this.limit);
            next_len = std::cmp::max(count, next_len);
            let new_local = Region::new_auto(next_len);
            if !this.local.is_empty() {
                this.stash.push(std::mem::take(&mut this.local));
            }
            this.local = new_local;
        }

        // Check if `item` fits into `self.local` without reallocation.
        // If not, stash `self.local` and increase the allocation.
        if count > self.local.capacity() - self.local.len() {
            reserve_inner(self, count);
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

/// An abstraction over different kinds of allocated regions.
///
/// # WARNING
///
/// The implementation does not drop its elements, but forgets them instead. Do not use where
/// this is not intended, i.e., outside `Copy` types or columnation regions.
///
/// NOTE: We plan to deprecate this type soon. Users should switch to different types or the raw
/// `lgalloc` API instead.
#[derive(Debug)]
pub enum Region<T> {
    /// A possibly empty heap-allocated region, represented as a vector.
    Heap(Vec<T>),
    /// A mmaped region, represented by a vector and its backing memory mapping.
    MMap(MMapRegion<T>),
}

/// Type encapsulating private data for memory-mapped regions.
pub struct MMapRegion<T> {
    /// Vector-representation of the underlying memory. Must not be dropped.
    inner: ManuallyDrop<Vec<T>>,
    /// Opaque handle to lgalloc.
    handle: Option<lgalloc::Handle>,
}

impl<T> MMapRegion<T> {
    /// Clear the contents of this region without dropping elements.
    unsafe fn clear(&mut self) {
        self.inner.set_len(0);
    }
}

impl<T: Debug> Debug for MMapRegion<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MMapRegion")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<T> Deref for MMapRegion<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Default for Region<T> {
    #[inline]
    fn default() -> Self {
        Self::new_empty()
    }
}

impl<T> Region<T> {
    /// Create a new empty region.
    #[inline]
    #[must_use]
    pub fn new_empty() -> Region<T> {
        Region::Heap(Vec::new())
    }

    /// Create a new heap-allocated region of a specific capacity.
    #[inline]
    #[must_use]
    pub fn new_heap(capacity: usize) -> Region<T> {
        Region::Heap(Vec::with_capacity(capacity))
    }

    /// Create a new file-based mapped region of a specific capacity. The capacity of the
    /// returned region can be larger than requested to accommodate page sizes.
    ///
    /// # Errors
    ///
    /// Returns an error if the memory allocation fails.
    #[inline(always)]
    pub fn new_mmap(capacity: usize) -> Result<Region<T>, lgalloc::AllocError> {
        lgalloc::allocate(capacity).map(|(ptr, capacity, handle)| {
            // SAFETY: `ptr` points to suitable memory.
            // It is UB to call `from_raw_parts` with a pointer not allocated from the global
            // allocator, but we accept this here because we promise never to reallocate the vector.
            let inner =
                ManuallyDrop::new(unsafe { Vec::from_raw_parts(ptr.as_ptr(), 0, capacity) });
            let handle = Some(handle);
            Region::MMap(MMapRegion { inner, handle })
        })
    }

    /// Create a region depending on the capacity.
    ///
    /// The capacity of the returned region must be at least as large as the requested capacity,
    /// but can be larger if the implementation requires it.
    ///
    /// Returns a [`Region::MMap`] if possible, and falls back to [`Region::Heap`] otherwise.
    #[must_use]
    pub fn new_auto(capacity: usize) -> Region<T> {
        match Region::new_mmap(capacity) {
            Ok(r) => return r,
            Err(lgalloc::AllocError::Disabled) | Err(lgalloc::AllocError::InvalidSizeClass(_)) => {}
            Err(e) => {
                eprintln!("lgalloc error: {e}, falling back to heap");
            }
        }
        // Fall-through
        Region::new_heap(capacity)
    }

    /// Clears the contents of the region, without dropping its elements.
    ///
    /// # Safety
    ///
    /// Discards all contends. Elements are not dropped.
    #[inline]
    pub unsafe fn clear(&mut self) {
        match self {
            Region::Heap(vec) => vec.set_len(0),
            Region::MMap(inner) => inner.clear(),
        }
    }

    /// Returns the capacity of the underlying allocation.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        match self {
            Region::Heap(vec) => vec.capacity(),
            Region::MMap(inner) => inner.inner.capacity(),
        }
    }

    /// Returns the number of elements in the allocation.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Region::Heap(vec) => vec.len(),
            Region::MMap(inner) => inner.len(),
        }
    }

    /// Returns true if the region does not contain any elements.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Region::Heap(vec) => vec.is_empty(),
            Region::MMap(inner) => inner.is_empty(),
        }
    }

    /// Dereference to the contained vector
    #[inline]
    #[must_use]
    pub fn as_vec(&self) -> &Vec<T> {
        match self {
            Region::Heap(vec) => vec,
            Region::MMap(inner) => &inner.inner,
        }
    }

    /// Extend the underlying region from the iterator.
    ///
    /// Care must be taken to not re-allocate the inner vector representation.
    #[inline]
    pub fn extend<I: IntoIterator<Item = T> + ExactSizeIterator>(&mut self, iter: I) {
        assert!(self.capacity() - self.len() >= iter.len());
        // SAFETY: We just asserted that we have sufficient capacity.
        unsafe { self.as_mut_vec().extend(iter) };
    }

    /// Obtain a mutable reference to the inner vector representation.
    ///
    /// Unsafe because the caller has to make sure that the vector will not reallocate.
    /// Otherwise, the vector representation could try to reallocate the underlying memory
    /// using the global allocator, which would cause problems because the memory might not
    /// have originated from it. This is undefined behavior.
    #[inline]
    unsafe fn as_mut_vec(&mut self) -> &mut Vec<T> {
        match self {
            Region::Heap(vec) => vec,
            Region::MMap(inner) => &mut inner.inner,
        }
    }
}

impl<T: Clone> Region<T> {
    /// Extend the region from a slice.
    ///
    /// Panics if the region does not have sufficient capacity.
    #[inline]
    pub fn extend_from_slice(&mut self, slice: &[T]) {
        assert!(self.capacity() - self.len() >= slice.len());
        // SAFETY: We just asserted that we have enough capacity.
        unsafe { self.as_mut_vec() }.extend_from_slice(slice);
    }
}

impl<T> Deref for Region<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_vec()
    }
}

impl<T> DerefMut for Region<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: We're dereferencing to `&mut [T]`, which does not allow reallocating the
        // underlying allocation, which makes it safe.
        unsafe { self.as_mut_vec().as_mut_slice() }
    }
}

impl<T> Drop for Region<T> {
    #[inline]
    fn drop(&mut self) {
        match self {
            Region::Heap(vec) => {
                // SAFETY: Don't drop the elements, drop the vec, in line with the documentation
                // of the `Region` type.
                unsafe { vec.set_len(0) }
            }
            Region::MMap(_) => {}
        }
    }
}

impl<T> Drop for MMapRegion<T> {
    fn drop(&mut self) {
        // Similar to dropping Region: Drop the allocation, don't drop the `inner` vector.
        lgalloc::deallocate(self.handle.take().unwrap());
    }
}
