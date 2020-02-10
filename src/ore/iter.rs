// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Iterator utilities.

use std::iter::{self, Chain, Once};

pub use fallible_iterator::FallibleIterator;

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
