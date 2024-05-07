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

use std::iter::{self, Chain, Once};

/// Extension methods for iterators.
pub trait IteratorExt
where
    Self: Iterator + Sized,
{
    /// Chains a single `item` onto the end of this iterator.
    ///
    /// Equivalent to `self.chain(iter::once(item))`.
    fn chain_one(self, item: Self::Item) -> Chain<Self, Once<Self::Item>> {
        self.chain(iter::once(item))
    }

    /// Reports whether all the elements of the iterator are the same.
    ///
    /// This condition is trivially true for iterators with zero or one elements.
    fn all_equal(mut self) -> bool
    where
        Self::Item: PartialEq,
    {
        match self.next() {
            None => true,
            Some(v1) => self.all(|v2| v1 == v2),
        }
    }

    /// Converts the the iterator into an `ExactSizeIterator` reporting the given size.
    ///
    /// The caller is responsible for providing the correct size of the iterator! Providing an
    /// incorrect size value will lead to panics and/or incorrect responses to size queries.
    ///
    /// # Panics
    ///
    /// Panics if the given length is not consistent with this iterator's `size_hint`.
    fn exact_size(self, len: usize) -> ExactSize<Self> {
        let (lower, upper) = self.size_hint();
        assert!(
            lower <= len && upper.map_or(true, |upper| upper >= len),
            "provided length {len} inconsistent with `size_hint`: {:?}",
            (lower, upper)
        );

        ExactSize { inner: self, len }
    }
}

impl<I> IteratorExt for I where I: Iterator {}

/// Iterator type returned by [`IteratorExt::exact_size`].
#[derive(Debug)]
pub struct ExactSize<I> {
    inner: I,
    len: usize,
}

impl<I: Iterator> Iterator for ExactSize<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.len = self.len.saturating_sub(1);
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<I: Iterator> ExactSizeIterator for ExactSize<I> {}

#[cfg(test)]
mod tests {
    use crate::iter::IteratorExt;

    #[crate::test]
    fn test_all_equal() {
        let empty: [i64; 0] = [];
        assert!(empty.iter().all_equal());
        assert!([1].iter().all_equal());
        assert!([1, 1].iter().all_equal());
        assert!(![1, 2].iter().all_equal());
    }
}
