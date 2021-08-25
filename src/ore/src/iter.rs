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

//! Iterator utilities.

use std::cmp::Ordering;
use std::iter::{self, Chain, Once};

/// Extension methods for iterators.
pub trait IteratorExt
where
    Self: Iterator + Sized,
{
    /// Checks if the elements of this iterator are sorted.
    ///
    /// That is, for each element `a` and its following element `b`, `a <= b`
    /// must hold. If the iterator yields exactly zero or one element, `true` is
    /// returned.
    ///
    /// Note that if `Self::Item` is only `PartialOrd`, but not `Ord`, the above
    /// definition implies that this function returns `false` if any two
    /// consecutive items are not comparable.
    ///
    /// **Note:** this is a Materialize forward-port of a forthcoming feature
    /// to the standard library.
    /// See [rust-lang/rust#53485](https://github.com/rust-lang/rust/issues/53485).
    ///
    /// # Examples
    ///
    /// ```
    /// use ore::iter::IteratorExt;
    /// assert!([1, 2, 2, 9].iter().mz_is_sorted());
    /// assert!(![1, 3, 2, 4].iter().mz_is_sorted());
    /// assert!([0].iter().mz_is_sorted());
    /// assert!(std::iter::empty::<i32>().mz_is_sorted());
    /// assert!(![0.0, 1.0, f32::NAN].iter().mz_is_sorted());
    /// ```
    fn mz_is_sorted(self) -> bool
    where
        Self: Sized,
        Self::Item: PartialOrd,
    {
        self.mz_is_sorted_by(PartialOrd::partial_cmp)
    }

    /// Checks if the elements of this iterator are sorted using the given comparator function.
    ///
    /// Instead of using `PartialOrd::partial_cmp`, this function uses the given `compare`
    /// function to determine the ordering of two elements. Apart from that, it's equivalent to
    /// [`Iterator::is_sorted`]; see its documentation for more information.
    ///
    /// **Note:** this is a Materialize forward-port of a forthcoming feature
    /// to the standard library.
    /// See [rust-lang/rust#53485](https://github.com/rust-lang/rust/issues/53485).
    ///
    /// # Examples
    ///
    /// ```
    /// use ore::iter::IteratorExt;
    /// assert!([1, 2, 2, 9].iter().mz_is_sorted_by(|a, b| a.partial_cmp(b)));
    /// assert!(![1, 3, 2, 4].iter().mz_is_sorted_by(|a, b| a.partial_cmp(b)));
    /// assert!([0].iter().mz_is_sorted_by(|a, b| a.partial_cmp(b)));
    /// assert!(std::iter::empty::<i32>().mz_is_sorted_by(|a, b| a.partial_cmp(b)));
    /// assert!(![0.0, 1.0, f32::NAN].iter().mz_is_sorted_by(|a, b| a.partial_cmp(b)));
    /// ```
    ///
    /// [`std::iter::Iterator::is_sorted`]: IteratorExt::is_sorted
    fn mz_is_sorted_by<F>(mut self, mut compare: F) -> bool
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> Option<Ordering>,
    {
        let mut last = match self.next() {
            Some(e) => e,
            None => return true,
        };

        for curr in self {
            if let Some(Ordering::Greater) | None = compare(&last, &curr) {
                return false;
            }
            last = curr;
        }

        true
    }

    /// Chains a single `item` onto the end of this iterator.
    ///
    /// Equivalent to `self.chain(iter::once(item))`.
    fn chain_one(self, item: Self::Item) -> Chain<Self, Once<Self::Item>> {
        self.chain(iter::once(item))
    }
}

impl<I> IteratorExt for I where I: Iterator {}
