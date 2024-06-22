// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! An array of fixed length, allocated from lgalloc if possible.

use std::mem::{ManuallyDrop, MaybeUninit};
use std::ops::Deref;

/// A fixed-length region in memory, which is either allocated from heap or lgalloc.
pub struct Array<T> {
    /// A handle to lgalloc. None for heap allocations, Some if the memory comes from lgalloc.
    handle: Option<lgalloc::Handle>,
    /// Slice representation of the memory. Elements 0..self.length are valid.
    elements: ManuallyDrop<Box<[MaybeUninit<T>]>>,
    /// The number of valid elements in `elements`
    length: usize,
}

impl<T> Array<T> {
    /// Create a new [`Array`] with the specified capacity. The actual capacity of the returned
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
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr(), actual_capacity) };
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
        debug_assert!(
            self.length < self.elements.len(),
            "Failed to push: length {} not less than {} capacity",
            self.length,
            self.elements.len()
        );
        self.elements[self.length].write(item);
        self.length += 1;
    }

    /// Update the length. Highly unsafe because it doesn't drop elements when reducing the length,
    /// and doesn't initialize elements when increasing the length.
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
}

impl<T> Deref for Array<T> {
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

impl<T> Drop for Array<T> {
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

        let mut array = Array::with_capacity(1);
        array.push(DropGuard);
        let _ = mz_ore::panic::catch_unwind(move || array.clear());
        assert_eq!(DROP_COUNT.load(Ordering::Relaxed), 1);
    }
}
