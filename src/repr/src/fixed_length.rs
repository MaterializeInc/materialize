// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides abstractions for types that can be converted from and into `Datum` iterators.
//! `Row` is the most obvious implementor, but other trace types that may use more advanced
//! representations only need to commit to implementing these traits.

use std::borrow::Borrow;

use crate::row::DatumListIter;
use crate::{Datum, Row};

/// A helper trait to get references to `Row.
pub trait ToDatumIter: Sized {
    /// An iterator type for use in `to_datum_iter`.
    type DatumIter<'a>: IntoIterator<Item = Datum<'a>>
    where
        Self: 'a;

    /// Obtains an iterator of datums out of an instance of `Self`, given a schema provided
    /// by `types`.
    fn to_datum_iter<'a>(&'a self) -> Self::DatumIter<'a>;
}

impl<'b, T: ToDatumIter> ToDatumIter for &'b T {
    type DatumIter<'a> = T::DatumIter<'a> where T: 'a, Self: 'a;
    fn to_datum_iter<'a>(&'a self) -> Self::DatumIter<'a> {
        (**self).to_datum_iter()
    }
}

// Blanket identity implementation for Row.
impl ToDatumIter for Row {
    /// Datum iterator for `Row`.
    type DatumIter<'a> = DatumListIter<'a>;

    /// Borrows `self` and gets an iterator from it.
    #[inline]
    fn to_datum_iter<'a>(&'a self) -> Self::DatumIter<'a> {
        self.iter()
    }
}

/// A helper trait to construct target values from input `Row` instances.
pub trait FromDatumIter: Sized + Default {
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

impl FromDatumIter for Row {
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
