// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Iterator utilities.

pub use fallible_iterator::FallibleIterator;

/// Extension methods for iterators.
pub trait IteratorExt<T, E>
where
    Self: Iterator<Item = Result<T, E>> + Sized,
{
    /// Converts this iterator to a [`FallibleIterator`].
    fn fallible(self) -> fallible_iterator::Convert<Self> {
        fallible_iterator::convert(self)
    }
}

impl<I, T, E> IteratorExt<T, E> for I where I: Iterator<Item = Result<T, E>> {}

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
