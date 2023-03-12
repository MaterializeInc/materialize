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

//! Vector utilities.

use std::mem::{align_of, size_of};

#[cfg(feature = "smallvec")]
use smallvec::SmallVec;

/// Create a new vector that re-uses the same allocation as an old one.
/// The element types must have the same size and alignment.
pub fn repurpose_allocation<T1, T2>(mut v: Vec<T1>) -> Vec<T2> {
    assert_eq!(size_of::<T1>(), size_of::<T2>(), "same size");
    assert_eq!(align_of::<T1>(), align_of::<T2>(), "same alignment");

    v.clear();
    let cap = v.capacity();
    let p = v.as_mut_ptr().cast();
    std::mem::forget(v);
    // This is safe because `T1` and `T2` have the same size and alignment,
    // `p`'s allocation is no longer owned by `v` (since that has been forgotten),
    // and `p` was previously allocated with capacity `cap`.
    unsafe { Vec::from_raw_parts(p, 0, cap) }
}

/// A trait for objects that behave like vectors.
pub trait Vector<T> {
    /// Appends an element to the vector.
    fn push(&mut self, value: T);

    /// Copies and appends all elements in a slice to the vector.
    fn extend_from_slice(&mut self, other: &[T])
    where
        T: Copy;
}

impl<T> Vector<T> for Vec<T> {
    #[inline]
    fn push(&mut self, value: T) {
        Vec::push(self, value)
    }

    #[inline]
    fn extend_from_slice(&mut self, other: &[T])
    where
        T: Copy,
    {
        Vec::extend_from_slice(self, other)
    }
}

#[cfg(feature = "smallvec")]
impl<A> Vector<A::Item> for SmallVec<A>
where
    A: smallvec::Array,
{
    #[inline]
    fn push(&mut self, value: A::Item) {
        SmallVec::push(self, value)
    }

    #[inline]
    fn extend_from_slice(&mut self, other: &[A::Item])
    where
        A::Item: Copy,
    {
        SmallVec::extend_from_slice(self, other)
    }
}

/// Extention methods for `std::vec::Vec`
pub trait VecExt<T> {
    /// Creates an iterator which uses a closure to determine if an element should be removed.
    ///
    /// If the closure returns true, then the element is removed and yielded.
    /// If the closure returns false, the element will remain in the vector and will not be yielded
    /// by the iterator.
    ///
    /// Using this method is equivalent to the following code:
    ///
    /// ```
    /// # let some_predicate = |x: &mut i32| { *x == 2 || *x == 3 || *x == 6 };
    /// # let mut vec = vec![1, 2, 3, 4, 5, 6];
    /// let mut i = 0;
    /// while i < vec.len() {
    ///     if some_predicate(&mut vec[i]) {
    ///         let val = vec.swap_remove(i);
    ///         // your code here
    ///     } else {
    ///         i += 1;
    ///     }
    /// }
    ///
    /// # assert_eq!(vec, vec![1, 5, 4]);
    /// ```
    ///
    /// But `drain_filter_swapping` is easier to use.
    ///
    /// Note that `drain_filter_swapping` also lets you mutate every element in the filter closure,
    /// regardless of whether you choose to keep or remove it.
    ///
    /// # Note
    ///
    /// Because the elements are removed using [`Vec::swap_remove`] the order of elements yielded
    /// by the iterator and the order of the remaining elements in the original vector is **not**
    /// maintained.
    ///
    /// # Examples
    ///
    /// Splitting an array into evens and odds, reusing the original allocation. Notice how the
    /// order is not preserved in either results:
    ///
    /// ```
    /// use mz_ore::vec::VecExt;
    ///
    /// let mut numbers = vec![1, 2, 3, 4, 5, 6, 8, 9, 11, 13, 14, 15];
    ///
    /// let evens = numbers.drain_filter_swapping(|x| *x % 2 == 0).collect::<Vec<_>>();
    /// let odds = numbers;
    ///
    /// assert_eq!(evens, vec![2, 4, 14, 6, 8]);
    /// assert_eq!(odds, vec![1, 15, 3, 13, 5, 11, 9]);
    /// ```
    fn drain_filter_swapping<F>(&mut self, filter: F) -> DrainFilterSwapping<'_, T, F>
    where
        F: FnMut(&mut T) -> bool;
}

impl<T> VecExt<T> for Vec<T> {
    fn drain_filter_swapping<F>(&mut self, filter: F) -> DrainFilterSwapping<'_, T, F>
    where
        F: FnMut(&mut T) -> bool,
    {
        DrainFilterSwapping {
            vec: self,
            idx: 0,
            pred: filter,
        }
    }
}

/// An iterator which uses a closure to determine if an element should be removed.
///
/// This struct is created by [`VecExt::drain_filter_swapping`].
/// See its documentation for more.
///
/// # Example
///
/// ```
/// use mz_ore::vec::VecExt;
///
/// let mut v = vec![0, 1, 2];
/// let iter: mz_ore::vec::DrainFilterSwapping<_, _> = v.drain_filter_swapping(|x| *x % 2 == 0);
/// ```
#[derive(Debug)]
pub struct DrainFilterSwapping<'a, T, F> {
    vec: &'a mut Vec<T>,
    /// The index of the item that will be inspected by the next call to `next`.
    idx: usize,
    /// The filter test predicate.
    pred: F,
}

impl<'a, T, F> Iterator for DrainFilterSwapping<'a, T, F>
where
    F: FnMut(&mut T) -> bool,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.vec.get_mut(self.idx)?;
            if (self.pred)(item) {
                return Some(self.vec.swap_remove(self.idx));
            } else {
                self.idx += 1;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.vec.len() - self.idx))
    }
}

/// Remove the elements from `v` at the positions indicated by `indexes`, and return the removed
/// elements in a new vector.
///
/// `indexes` shouldn't have duplicates. (Might panic or behave incorrectly in case of
/// duplicates.)
pub fn swap_remove_multiple<T>(v: &mut Vec<T>, mut indexes: Vec<usize>) -> Vec<T> {
    indexes.sort();
    indexes.reverse();
    let mut result = Vec::new();
    for r in indexes {
        result.push(v.swap_remove(r));
    }
    result
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn miri_test_repurpose() {
        let v: Vec<usize> = vec![0, 1, 2];

        let mut other: Vec<isize> = repurpose_allocation(v);

        assert!(other.is_empty());
        other.push(-1);
        assert_eq!(other[0], -1);

        struct Gus1 {
            s: String,
        }
        impl Drop for Gus1 {
            fn drop(&mut self) {
                println!("happy {}", self.s);
            }
        }

        struct Gus2 {
            s: String,
        }
        impl Drop for Gus2 {
            fn drop(&mut self) {
                println!("happy {}", self.s);
            }
        }

        // also exercise non-`Copy`, `Drop`-impling values as well
        let v: Vec<Gus1> = vec![Gus1 {
            s: "hmm".to_string(),
        }];

        let mut other: Vec<Gus2> = repurpose_allocation(v);

        assert!(other.is_empty());
        other.push(Gus2 {
            s: "hmm2".to_string(),
        });
        assert_eq!(other[0].s, "hmm2");
    }

    #[test]
    #[should_panic(expected = "same size")]
    fn miri_test_wrong_size() {
        let v: Vec<usize> = vec![0, 1, 2];
        let _: Vec<()> = repurpose_allocation(v);
    }

    #[test]
    #[should_panic(expected = "same alignment")]
    fn miri_test_wrong_align() {
        #[repr(align(8))]
        #[derive(Default)]
        struct Gus1 {
            _i: [u8; 16],
        }

        #[repr(align(16))]
        #[derive(Default)]
        struct Gus2 {
            _i: [u8; 8],
        }

        use std::mem::size_of;
        assert_eq!(size_of::<Gus1>(), size_of::<Gus2>(), "same size in test");

        // You need a value in here to have miri catch the problem, if we remove
        // the alignment check
        let v: Vec<Gus1> = vec![Default::default()];
        let _: Vec<Gus2> = repurpose_allocation(v);
    }
}
