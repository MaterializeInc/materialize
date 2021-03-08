// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! Iterator utilities.

use std::cmp::Ordering;
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
