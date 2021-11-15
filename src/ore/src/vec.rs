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
    assert!(size_of::<T1>() == size_of::<T2>());
    assert!(align_of::<T1>() == align_of::<T2>());
    v.clear();
    let cap = v.capacity();
    let p = v.as_mut_ptr().cast();
    std::mem::forget(v);
    // This is sound because `T1` and `T2` have the same size and alignment,
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
    fn push(&mut self, value: T) {
        Vec::push(self, value)
    }

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
    fn push(&mut self, value: A::Item) {
        SmallVec::push(self, value)
    }

    fn extend_from_slice(&mut self, other: &[A::Item])
    where
        A::Item: Copy,
    {
        SmallVec::extend_from_slice(self, other)
    }
}
