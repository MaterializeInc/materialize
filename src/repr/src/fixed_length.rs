// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides abstractions for types that can be converted into `Datum` iterators.
//! `Row` is the most obvious implementor, but other trace types that may use more advanced
//! representations only need to commit to implementing these traits.

use crate::row::DatumListIter;
use crate::{Datum, Row};

/// A helper trait to turn a type into an iterator of datums.
pub trait ToDatumIter: Sized {
    /// An iterator type for use in `to_datum_iter`.
    type DatumIter<'a>: IntoIterator<Item = Datum<'a>>
    where
        Self: 'a;

    /// Obtains an iterator of datums out of an instance of `&Self`.
    fn to_datum_iter(&self) -> Self::DatumIter<'_>;
}

impl<'b, T: ToDatumIter> ToDatumIter for &'b T {
    type DatumIter<'a>
        = T::DatumIter<'a>
    where
        Self: 'a;
    fn to_datum_iter(&self) -> Self::DatumIter<'_> {
        (**self).to_datum_iter()
    }
}

// Blanket identity implementation for Row.
impl ToDatumIter for Row {
    /// Datum iterator for `Row`.
    type DatumIter<'a> = DatumListIter<'a>;

    /// Borrows `self` and gets an iterator from it.
    #[inline]
    fn to_datum_iter(&self) -> Self::DatumIter<'_> {
        self.iter()
    }
}
