// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides fixed-length representations for data composed of `Datum`s of fixed-length types.
//! These representations are aimed at being more efficient in memory usage than `Row` by
//! relying on statically selected container lengths. Traits are provided that allow these
//! representations to be made into instances of `Row` or created from `Row`s. The traits are
//! trivially implemented for `Row` itself, providing a uniform interface to describe `Row`s
//! or fixed-length containers standing in for them.

use std::borrow::Borrow;
use std::iter::{empty, Empty};

use crate::row::DatumListIter;
use crate::{Datum, Row};

/// A helper trait to get references to `Row.
pub trait IntoRowByTypes: Sized {
    /// An iterator type for use in `into_datum_iter`.
    type DatumIter<'a>: IntoIterator<Item = Datum<'a>>
    where
        Self: 'a;

    /// Obtains an iterator of datums out of an instance of `Self`, given a schema provided
    /// by `types`.
    fn into_datum_iter<'a>(&'a self) -> Self::DatumIter<'a>;
}

impl<'b, T: IntoRowByTypes> IntoRowByTypes for &'b T {
    type DatumIter<'a> = T::DatumIter<'a> where T: 'a, Self: 'a;
    fn into_datum_iter<'a>(&'a self) -> Self::DatumIter<'a> {
        (**self).into_datum_iter()
    }
}

// Blanket identity implementation for Row.
impl IntoRowByTypes for Row {
    /// Datum iterator for `Row`.
    type DatumIter<'a> = DatumListIter<'a>;

    /// Borrows `self` and gets an iterator from it.
    #[inline]
    fn into_datum_iter<'a>(&'a self) -> Self::DatumIter<'a> {
        self.iter()
    }
}

/// A helper trait to construct target values from input `Row` instances.
pub trait FromRowByTypes: Sized + Default {
    /// Obtains an instance of `Self' given an iterator of borrowed datums and a schema
    /// provided by `types`.
    fn from_datum_iter<'a, I, D>(&mut self, datum_iter: I) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>;

    /// Obtains an instance of `Self' given an iterator of results of borrowed datums and a schema
    /// provided by `types`.
    ///
    /// In the case the iterator produces an error, the pushing of datums is terminated and the
    /// error returned.
    fn try_from_datum_iter<'a, I, D, E>(&mut self, datum_iter: I) -> Result<Self, E>
    where
        I: IntoIterator<Item = Result<D, E>>,
        D: Borrow<Datum<'a>>;
}

impl FromRowByTypes for Row {
    /// Packs into `self` the given iterator of datums and returns a clone.
    #[inline]
    fn from_datum_iter<'a, I, D>(&mut self, datum_iter: I) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        self.packer().extend(datum_iter);
        self.clone()
    }

    /// Packs into `self` by using the packer's `try_extend` method on the given iterator
    /// and returns a clone.
    #[inline]
    fn try_from_datum_iter<'a, I, D, E>(&mut self, datum_iter: I) -> Result<Self, E>
    where
        I: IntoIterator<Item = Result<D, E>>,
        D: Borrow<Datum<'a>>,
    {
        self.packer().try_extend(datum_iter)?;
        Ok(self.clone())
    }
}

impl IntoRowByTypes for () {
    /// Empty iterator for unit.
    type DatumIter<'a> = Empty<Datum<'a>>;

    /// Returns an empty iterator.
    #[inline]
    fn into_datum_iter<'a>(&'a self) -> Self::DatumIter<'a> {
        empty()
    }
}

impl FromRowByTypes for () {
    /// Obtains a unit value from an empty datum iterator.
    #[inline]
    fn from_datum_iter<'a, I, D>(&mut self, datum_iter: I) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        debug_assert!(datum_iter.into_iter().next().is_none());
        ()
    }

    /// Obtains a unit value from an empty iterator of results of datums.
    #[inline]
    fn try_from_datum_iter<'a, I, D, E>(&mut self, datum_iter: I) -> Result<Self, E>
    where
        I: IntoIterator<Item = Result<D, E>>,
        D: Borrow<Datum<'a>>,
    {
        debug_assert!(datum_iter.into_iter().next().is_none());
        Ok(())
    }
}
