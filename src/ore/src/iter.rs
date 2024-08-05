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

    /// Wrap this iterator with one that yields a tuple of the iterator element and the extra
    /// value on each iteration. The extra value is cloned for each but the last `Some` element
    /// returned.
    ///
    /// This is useful to provide an owned extra value to each iteration, but only clone it
    /// when necessary.
    ///
    /// NOTE: this will immediately consume the next element of the iterator
    /// internally to determine if the extra value should be cloned.
    ///
    /// NOTE: Once the iterator starts producing `None` values, the extra value will be consumed
    /// and no longer be available. This should not be used for iterators that may produce
    /// `Some` values after producing `None`.
    fn repeat_clone<A: Clone>(mut self, extra_val: A) -> RepeatClone<Self, A> {
        RepeatClone {
            next: self.next(),
            iter: self,
            extra_val: Some(extra_val),
        }
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

/// Iterator type returned by [`IteratorExt::repeat_clone`].
#[derive(Debug)]
pub struct RepeatClone<I: Iterator, A> {
    iter: I,
    next: Option<I::Item>,
    extra_val: Option<A>,
}

impl<I: Iterator, A: Clone> Iterator for RepeatClone<I, A> {
    type Item = (I::Item, A);

    fn next(&mut self) -> Option<Self::Item> {
        // Store the next element and retrieve the current one to return.
        let cur_next = std::mem::replace(&mut self.next, self.iter.next());

        // Clone the extra_val only if there is an item to return on the next call to `next`.
        let val = if self.next.is_some() {
            self.extra_val.as_ref().cloned()
        } else {
            self.extra_val.take()
        };

        // We should always return a value if there is a current element.
        match (cur_next, val) {
            (Some(cur_next), Some(val)) => Some((cur_next, val)),
            (None, _) => None,
            (Some(_), None) => unreachable!("RepeatClone invariant violated"),
        }
    }
}

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
