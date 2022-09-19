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
