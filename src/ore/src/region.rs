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

pub use vec::LgAllocVec;

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

mod vec {
    use std::fmt::{Debug, Formatter};
    use std::mem::{ManuallyDrop, MaybeUninit};
    use std::ops::Deref;
    use std::ptr;

    /// A fixed-length region in memory, which is either allocated from heap or lgalloc.
    pub struct LgAllocVec<T> {
        /// A handle to lgalloc. None for heap allocations, Some if the memory comes from lgalloc.
        handle: Option<lgalloc::Handle>,
        /// Slice representation of the memory. Elements 0..self.length are valid.
        elements: ManuallyDrop<Box<[MaybeUninit<T>]>>,
        /// The number of valid elements in `elements`
        length: usize,
    }

    impl<T> LgAllocVec<T> {
        /// Create a new [`LgAllocVec`] with the specified capacity. The actual capacity of the returned
        /// array is at least as big as the requested capacity.
        pub fn with_capacity(capacity: usize) -> Self {
            // Allocate memory, fall-back to regular heap allocations if we cannot acquire memory through
            // lgalloc.
            let (handle, boxed) = if let Ok((ptr, actual_capacity, handle)) =
                lgalloc::allocate::<MaybeUninit<T>>(capacity)
            {
                // We allocated sucessfully through lgalloc.
                let handle = Some(handle);
                // SAFETY: `ptr` is valid for constructing a slice:
                // 1. Valid for reading and writing, and enough capacity.
                // 2. Properly initialized (left for writing).
                // 3. Not aliased.
                // 4. Total size not longer than isize::MAX because lgalloc has a capacity limit.
                let slice =
                    unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr(), actual_capacity) };
                // SAFETY: slice is valid, and we deallocate it usinge lgalloc.
                (handle, unsafe { Box::from_raw(slice) })
            } else {
                // We failed to allocate through lgalloc, fall back to heap.
                let mut vec = Vec::with_capacity(capacity);
                // SAFETY: We treat all elements as uninitialized and track initialized elements
                // through `self.length`.
                unsafe {
                    vec.set_len(vec.capacity());
                }
                (None, vec.into_boxed_slice())
            };

            let elements = ManuallyDrop::new(boxed);
            Self {
                handle,
                elements,
                length: 0,
            }
        }

        /// Visit contained allocations to determine their size and capacity.
        pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
            let size_of_t = std::mem::size_of::<T>();
            callback(self.len() * size_of_t, self.capacity() * size_of_t)
        }

        /// Move an element on the array. Panics if there is no more capacity.
        pub fn push(&mut self, item: T) {
            if self.len() == self.capacity() {
                self.reserve(1);
            }
            self.elements[self.length].write(item);
            self.length += 1;
        }

        /// Extend the array from a slice. Increases the capacity if required.
        pub fn extend_from_slice(&mut self, slice: &[T])
        where
            T: Clone,
        {
            self.reserve(slice.len());
            let mut iterator = slice.iter().cloned();
            while let Some(element) = iterator.next() {
                let len = self.len();
                if len == self.capacity() {
                    let (lower, _) = iterator.size_hint();
                    self.reserve(lower.saturating_add(1));
                }
                unsafe {
                    ptr::write(
                        self.elements.as_mut_ptr().add(len),
                        MaybeUninit::new(element),
                    );
                    self.set_len(len + 1);
                }
            }
        }

        /// Extend the array from a slice of copyable elements. Increases the capacity if required.
        pub fn extend_from_copy_slice(&mut self, slice: &[T])
        where
            T: Copy,
        {
            let count = slice.len();
            self.reserve(count);
            let len = self.len();
            unsafe {
                ptr::copy_nonoverlapping(
                    slice.as_ptr(),
                    self.elements.as_mut_ptr().add(len) as *const MaybeUninit<T> as *mut T,
                    count,
                );
                self.set_len(len + count);
            }
        }

        /// Move elements from a vector to the array. Increases the capacity if required.
        pub fn append(&mut self, data: &mut Vec<T>) {
            let count = data.len();
            self.reserve(count);
            let len = self.len();
            unsafe {
                data.set_len(0);
                ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    self.elements.as_mut_ptr().add(len) as *const MaybeUninit<T> as *mut T,
                    count,
                );
                self.set_len(len + count);
            }
        }

        /// Update the length. Highly unsafe because it doesn't drop elements when reducing the length,
        /// and doesn't initialize elements when increasing the length.
        #[inline]
        pub unsafe fn set_len(&mut self, length: usize) {
            debug_assert!(length <= self.capacity());
            self.length = length;
        }

        /// The number of elements this array can absorb.
        pub fn capacity(&self) -> usize {
            self.elements.len()
        }

        /// Remove all elements. Drops the contents, but leaves the allocation untouched.
        pub fn clear(&mut self) {
            let elems = &mut self.elements[..self.length];
            // We are about to run the type's destructor, which may panic. Therefore we set the length
            // of the array to zero so that if we have to unwind the stack we don't end up re-dropping
            // in valid memory through the Drop impl of Array itself.
            self.length = 0;
            for e in elems {
                // SAFETY: We know elements up to `length` are initialized.
                unsafe {
                    e.assume_init_drop();
                }
            }
        }

        /// Grow the array to at least `new_len` elements. Reallocates the underlying storage.
        fn grow(&mut self, new_len: usize) {
            let new_capacity = std::cmp::max(self.capacity() * 2, new_len);
            let mut new_vec = LgAllocVec::with_capacity(new_capacity);

            let src_ptr = self.elements.as_ptr();
            let dst_ptr = new_vec.elements.as_mut_ptr();
            let len = self.len();

            unsafe {
                self.set_len(0);
                std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, len);
                new_vec.set_len(len);
            }

            std::mem::swap(&mut new_vec, self);
        }

        /// Reserve space for at least `additional` elements. The capacity is increased if necessary.
        pub fn reserve(&mut self, additional: usize) {
            let new_len = self.len() + additional;
            if new_len > self.capacity() {
                self.grow(new_len);
            }
        }
    }

    impl<T: Clone> Clone for LgAllocVec<T> {
        fn clone(&self) -> Self {
            let mut new_vec = LgAllocVec::with_capacity(self.len());
            new_vec.extend_from_slice(self);
            new_vec
        }

        fn clone_from(&mut self, source: &Self) {
            // TODO: Optimize for reuse of existing elements.
            self.clear();
            self.extend_from_slice(source);
        }
    }

    impl<T> Default for LgAllocVec<T> {
        fn default() -> Self {
            Self::with_capacity(0)
        }
    }

    impl<T> Deref for LgAllocVec<T> {
        type Target = [T];

        fn deref(&self) -> &Self::Target {
            // TODO: Use `slice_assume_init_ref` once stable.
            // Context: https://doc.rust-lang.org/std/mem/union.MaybeUninit.html#method.slice_assume_init_ref
            // The following safety argument is adapted from the source.
            // SAFETY: casting `elements` to a `*const [T]` is safe since the caller guarantees that
            // `slice` is initialized, and `MaybeUninit` is guaranteed to have the same layout as `T`.
            // The pointer obtained is valid since it refers to memory owned by `elements` which is a
            // reference and thus guaranteed to be valid for reads.
            #[allow(clippy::as_conversions)]
            unsafe {
                &*(&self.elements[..self.length] as *const [MaybeUninit<T>] as *const [T])
            }
        }
    }

    impl<T> Drop for LgAllocVec<T> {
        fn drop(&mut self) {
            self.clear();
            if let Some(handle) = self.handle.take() {
                // Memory allocated through lgalloc
                lgalloc::deallocate(handle);
            } else {
                // Regular allocation
                // SAFETY: `elements` is a sliced box allocated from the global allocator, drop it.
                unsafe {
                    ManuallyDrop::drop(&mut self.elements);
                }
            }
        }
    }

    impl<T: Debug> Debug for LgAllocVec<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.deref().fmt(f)
        }
    }

    #[cfg(test)]
    mod test {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use super::*;

        #[mz_ore::test]
        fn double_drop() {
            static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
            struct DropGuard;

            impl Drop for DropGuard {
                fn drop(&mut self) {
                    let drops = DROP_COUNT.fetch_add(1, Ordering::Relaxed);
                    // If this is the first time we're being dropped, panic.
                    if drops == 0 {
                        panic!();
                    }
                }
            }

            let mut array = LgAllocVec::with_capacity(1);
            array.push(DropGuard);
            let _ = mz_ore::panic::catch_unwind(move || array.clear());
            assert_eq!(DROP_COUNT.load(Ordering::Relaxed), 1);
        }
    }
}
