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

    /// Append datums to `target` — all of them (`max == None`) or at most
    /// `max` (`Some`) — branching on representation ONCE rather than once per
    /// datum.
    ///
    /// The default extends via `to_datum_iter`, identical to the prior
    /// `target.extend(x.to_datum_iter()[.take(max)])` call sites. Implementors
    /// backed by packed bytes (e.g. row-spine's `DatumSeq`) should override this
    /// with a tight loop that pushes directly, avoiding the `Iterator::extend` /
    /// `take` per-datum machinery.
    #[inline]
    fn extend_datums<'a>(&'a self, target: &mut Vec<Datum<'a>>, max: Option<usize>) {
        match max {
            Some(max) => target.extend(self.to_datum_iter().into_iter().take(max)),
            None => target.extend(self.to_datum_iter()),
        }
    }
}

impl<'b, T: ToDatumIter> ToDatumIter for &'b T {
    type DatumIter<'a>
        = T::DatumIter<'a>
    where
        Self: 'a;
    fn to_datum_iter(&self) -> Self::DatumIter<'_> {
        (**self).to_datum_iter()
    }
    #[inline]
    fn extend_datums<'a>(&'a self, target: &mut Vec<Datum<'a>>, max: Option<usize>) {
        // Forward to T's impl so an override isn't lost behind a reference.
        (**self).extend_datums(target, max)
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
