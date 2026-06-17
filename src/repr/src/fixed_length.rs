// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides abstractions for types that can append their datums to a vector.
//! `Row` is the most obvious implementor, but other trace types that may use more advanced
//! representations only need to commit to implementing this trait.

use crate::{Datum, Row, RowArena};

/// A helper trait for types that can append their datums to a `Vec<Datum>`.
pub trait ExtendDatums {
    /// Append datums to `target` — all of them (`max == None`) or at most
    /// `max` (`Some`) — branching on representation ONCE rather than once per
    /// datum.
    ///
    /// `arena` provides storage for data that the appended datums borrow from
    /// but that does not already live in `self`. Representations backed directly
    /// by packed bytes (`Row`, row-spine's `DatumSeq`) ignore it, while
    /// compressed representations can decode into it so the resulting
    /// `Datum<'a>`s borrow from the arena rather than from `self`. Implementors
    /// should push directly into `target` in a tight loop, branching on their
    /// representation once rather than once per datum.
    fn extend_datums<'a>(
        &'a self,
        arena: &'a RowArena,
        target: &mut Vec<Datum<'a>>,
        max: Option<usize>,
    );
}

impl<T: ExtendDatums + ?Sized> ExtendDatums for &T {
    #[inline]
    fn extend_datums<'a>(
        &'a self,
        arena: &'a RowArena,
        target: &mut Vec<Datum<'a>>,
        max: Option<usize>,
    ) {
        // Forward to T's impl so an override isn't lost behind a reference.
        (**self).extend_datums(arena, target, max)
    }
}

// Identity implementation for Row, whose datums borrow directly from its bytes.
impl ExtendDatums for Row {
    #[inline]
    fn extend_datums<'a>(
        &'a self,
        _arena: &'a RowArena,
        target: &mut Vec<Datum<'a>>,
        max: Option<usize>,
    ) {
        match max {
            Some(max) => target.extend(self.iter().take(max)),
            None => target.extend(self.iter()),
        }
    }
}
