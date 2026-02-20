// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A re-useable vector of `Datum` with varying lifetimes.
//!
//! This type is meant to allow us to recycle an underlying allocation with
//! a specific lifetime, under the condition that the vector is emptied before
//! this happens (to prevent leaking of invalid references).
//!
//! It uses `ore::vec::repurpose_allocation` to accomplish this, which contains
//! unsafe code.

use crate::{Datum, RowRef};

/// A re-useable vector of `Datum` with no particular lifetime.
#[derive(Debug, Default, Clone)]
pub struct DatumVec {
    outer: Vec<Datum<'static>>,
}

impl DatumVec {
    /// Allocate a new instance.
    pub const fn new() -> Self {
        Self { outer: Vec::new() }
    }
    /// Borrow an instance with a specific lifetime.
    ///
    /// When the result is dropped, its allocation will be returned to `self`.
    #[inline]
    pub fn borrow<'a>(&'a mut self) -> DatumVecBorrow<'a> {
        let inner = std::mem::take(&mut self.outer);
        DatumVecBorrow {
            outer: &mut self.outer,
            inner,
        }
    }

    /// Borrow an instance with a specific lifetime, and pre-populate with a `Row`.
    #[inline]
    pub fn borrow_with<'a>(&'a mut self, row: &'a RowRef) -> DatumVecBorrow<'a> {
        let mut borrow = self.borrow();
        borrow.extend(row.iter());
        borrow
    }

    /// Borrow an instance with a specific lifetime, and pre-populate with a `Row`, but
    /// only fully decode columns marked `true` in `needed`. Unneeded columns are skipped
    /// using fast pointer arithmetic (`skip_datum`) and filled with `Datum::Null`.
    ///
    /// This is faster than `borrow_with` when only a subset of columns are needed,
    /// because `skip_datum` avoids expensive type-specific construction (chrono DateTime
    /// for timestamps, Decimal for numerics, etc.).
    ///
    /// The caller must ensure that only columns marked `true` in `needed` are subsequently
    /// read from the returned borrow.
    #[inline]
    pub fn borrow_with_selective<'a>(
        &'a mut self,
        row: &'a RowRef,
        needed: &[bool],
    ) -> DatumVecBorrow<'a> {
        let mut borrow = self.borrow();
        row.decode_selective(needed, &mut borrow);
        borrow
    }

    /// Borrow an instance with a specific lifetime, and pre-populate with a `Row` with up to
    /// `limit` elements. If `limit` is greater than the number of elements in `row`, the borrow
    /// will contain all elements of `row`.
    pub fn borrow_with_limit<'a>(
        &'a mut self,
        row: &'a RowRef,
        limit: usize,
    ) -> DatumVecBorrow<'a> {
        let mut borrow = self.borrow();
        borrow.extend(row.iter().take(limit));
        borrow
    }
}

/// A borrowed allocation of `Datum` with a specific lifetime.
///
/// When an instance is dropped, its allocation is returned to the vector from
/// which it was extracted.
#[derive(Debug)]
pub struct DatumVecBorrow<'outer> {
    outer: &'outer mut Vec<Datum<'static>>,
    inner: Vec<Datum<'outer>>,
}

impl<'outer> Drop for DatumVecBorrow<'outer> {
    fn drop(&mut self) {
        *self.outer = mz_ore::vec::repurpose_allocation(std::mem::take(&mut self.inner));
    }
}

impl<'outer> std::ops::Deref for DatumVecBorrow<'outer> {
    type Target = Vec<Datum<'outer>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'outer> std::ops::DerefMut for DatumVecBorrow<'outer> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Row;

    #[mz_ore::test]
    fn miri_test_datum_vec() {
        let mut d = DatumVec::new();

        assert_eq!(d.borrow().len(), 0);

        let r = Row::pack_slice(&[Datum::String("first"), Datum::Dummy]);

        {
            let borrow = d.borrow_with(&r);
            assert_eq!(borrow.len(), 2);
            assert_eq!(borrow[0], Datum::String("first"));
        }

        {
            // different lifetime, so that rust is happy with the reference lifetimes
            let r2 = Row::pack_slice(&[Datum::String("second")]);
            let mut borrow = d.borrow();
            borrow.extend(&r);
            borrow.extend(&r2);
            assert_eq!(borrow.len(), 3);
            assert_eq!(borrow[2], Datum::String("second"));
        }
    }

    #[mz_ore::test]
    fn test_selective_decode() {
        use chrono::NaiveDate;
        use crate::adt::timestamp::CheckedTimestamp;

        let ts = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_micro_opt(14, 30, 0, 123456)
            .unwrap();
        let ts = CheckedTimestamp::from_timestamplike(ts).unwrap();

        let row = Row::pack_slice(&[
            Datum::Int64(42),
            Datum::String("hello"),
            Datum::Float64(3.14.into()),
            Datum::Timestamp(ts),
            Datum::True,
        ]);

        let mut dv = DatumVec::new();

        // Full decode for reference
        let full = dv.borrow_with(&row);
        assert_eq!(full.len(), 5);
        assert_eq!(full[0], Datum::Int64(42));
        assert_eq!(full[1], Datum::String("hello"));
        assert_eq!(full[4], Datum::True);
        drop(full);

        // Selective: need only columns 0, 2, 4
        let needed = vec![true, false, true, false, true];
        let selective = dv.borrow_with_selective(&row, &needed);
        assert_eq!(selective.len(), 5);
        // Needed columns should match full decode
        assert_eq!(selective[0], Datum::Int64(42));
        assert_eq!(selective[2], Datum::Float64(3.14.into()));
        assert_eq!(selective[4], Datum::True);
        // Unneeded columns should be Null
        assert_eq!(selective[1], Datum::Null);
        assert_eq!(selective[3], Datum::Null);
        drop(selective);

        // Selective: need only column 3 (timestamp)
        let needed = vec![false, false, false, true, false];
        let selective = dv.borrow_with_selective(&row, &needed);
        assert_eq!(selective.len(), 5);
        assert_eq!(selective[3], Datum::Timestamp(ts));
        assert_eq!(selective[0], Datum::Null);
        assert_eq!(selective[1], Datum::Null);
        assert_eq!(selective[2], Datum::Null);
        assert_eq!(selective[4], Datum::Null);
        drop(selective);

        // All columns needed should match full decode
        let needed = vec![true; 5];
        let selective = dv.borrow_with_selective(&row, &needed);
        assert_eq!(selective.len(), 5);
        assert_eq!(selective[0], Datum::Int64(42));
        assert_eq!(selective[1], Datum::String("hello"));
        assert_eq!(selective[3], Datum::Timestamp(ts));
        drop(selective);

        // Empty needed mask (shorter than row) - extra columns decoded normally
        let needed = vec![];
        let selective = dv.borrow_with_selective(&row, &needed);
        assert_eq!(selective.len(), 5);
        // All columns should be decoded since needed is shorter than row
        assert_eq!(selective[0], Datum::Int64(42));
        assert_eq!(selective[4], Datum::True);
    }
}
