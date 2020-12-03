// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Iterator utilities.

use std::collections::HashMap;
use std::hash::Hash;
use std::iter::{self, Chain, Once};

pub use fallible_iterator::FallibleIterator;

/// An iterator adapter that yields each element that appears more than once.
#[derive(Debug)]
pub struct Duplicates<I>
where
    I: Iterator,
{
    iter: I,
    seen: HashMap<I::Item, bool>,
}

impl<I> Duplicates<I>
where
    I: Iterator,
    I::Item: Hash + Eq,
{
    fn new(iter: I) -> Self {
        let (lower, _) = iter.size_hint();
        Self {
            iter,
            seen: HashMap::with_capacity(lower),
        }
    }
}

impl<I> Iterator for Duplicates<I>
where
    I: Iterator,
    I::Item: Hash + Eq,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(v) = self.iter.next() {
            match self.seen.get_mut(&v) {
                // New element. Record it as seen but unyielded.
                None => {
                    self.seen.insert(v, false);
                }
                // Duplicate element that we've not yielded. Yield it.
                Some(yielded @ false) => {
                    *yielded = true;
                    return Some(v);
                }
                // Duplicate element that we've already yielded. Suppress it.
                Some(true) => (),
            }
        }
        None
    }
}

/// Extension methods for iterators.
pub trait IteratorExt
where
    Self: Iterator + Sized,
{
    /// Determines whether the iterator yields any duplicates.
    ///
    /// The algorithm requires O(n) time and O(n) space.
    fn has_duplicates(self) -> bool
    where
        Self::Item: Hash + Eq,
    {
        self.duplicates().next().is_some()
    }

    /// Creates an iterator that produces each duplicate item exactly once.
    /// Duplicates are detected using the item's hash.
    ///
    /// The algorithm requires O(n) space.
    fn duplicates(self) -> Duplicates<Self>
    where
        Self::Item: Hash + Eq,
    {
        Duplicates::new(self)
    }

    /// Chains a single `item` onto the end of this iterator.
    ///
    /// Equivalent to `self.chain(iter::once(item))`.
    fn chain_one(self, item: Self::Item) -> Chain<Self, Once<Self::Item>> {
        self.chain(iter::once(item))
    }

    /// Converts this iterator to a [`FallibleIterator`].
    fn fallible<T, E>(self) -> fallible_iterator::Convert<Self>
    where
        Self: Iterator<Item = Result<T, E>>,
    {
        fallible_iterator::convert(self)
    }
}

impl<I> IteratorExt for I where I: Iterator {}

/// Extension methods for fallible iterators.
pub trait FallibleIteratorExt
where
    Self: FallibleIterator,
{
    /// [`Iterator::fold`] without a base case. The closure will only be invoked
    /// if the iterator produces at least one element, in which case the
    /// function returns the result of the fold operation wrapped in a `Some`;
    /// otherwise, it returns `None`.
    fn fold1<F>(&mut self, f: F) -> Option<Result<Self::Item, Self::Error>>
    where
        F: FnMut(Self::Item, Self::Item) -> Result<Self::Item, Self::Error>,
    {
        match self.next() {
            Ok(x) => x.map(|x| self.fold(x, f)),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<I> FallibleIteratorExt for I where I: FallibleIterator {}

#[cfg(test)]
mod tests {
    use super::IteratorExt;

    #[test]
    fn test_duplicates() {
        let a = vec![1, 2, 3, 4, 5];
        assert_eq!(a.iter().duplicates().next(), None);

        let a = vec![1, 2, 1, 4, 5, 2, 1, 5];
        assert_eq!(
            a.into_iter().duplicates().collect::<Vec<_>>(),
            vec![1, 2, 5]
        );
    }

    #[test]
    fn test_has_duplicates() {
        let a = vec![1, 2, 3, 4, 5];
        assert_eq!(a.iter().has_duplicates(), false);

        let a = vec![1, 2, 1, 4, 5];
        assert_eq!(a.iter().has_duplicates(), true);
    }
}
