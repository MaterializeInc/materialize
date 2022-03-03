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

use crate::{Datum, Row};

/// A re-useable vector of `Datum` with no particular lifetime.
#[derive(Debug)]
pub struct DatumVec {
    outer: Vec<Datum<'static>>,
}

impl DatumVec {
    /// Allocate a new instance.
    pub fn new() -> Self {
        Self { outer: Vec::new() }
    }
    /// Borrow an instance with a specific lifetime.
    ///
    /// When the result is dropped, its allocation will be returned to `self`.
    pub fn borrow<'a>(&'a mut self) -> DatumVecBorrow<'a> {
        let inner = std::mem::take(&mut self.outer);
        DatumVecBorrow {
            outer: &mut self.outer,
            inner,
        }
    }
    /// Borrow an instance with a specific lifetime, and pre-populate with a `Row`.
    pub fn borrow_with<'a>(&'a mut self, row: &'a Row) -> DatumVecBorrow<'a> {
        let mut borrow = self.borrow();
        borrow.extend(row.iter());
        borrow
    }

    /// Borrow an instance with a specific lifetime, and pre-populate with `Row`s, for example
    /// first adding a key followed by its values.
    pub fn borrow_with_many<'a, 'b, D: ::std::borrow::Borrow<Row> + 'a>(
        &'a mut self,
        rows: &'b [&'a D],
    ) -> DatumVecBorrow<'a> {
        let mut borrow = self.borrow();
        for row in rows {
            borrow.extend(row.borrow().iter());
        }
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

    #[test]
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
            let borrow = d.borrow_with_many(&[&r, &r2]);
            assert_eq!(borrow.len(), 3);
            assert_eq!(borrow[2], Datum::String("second"));
        }
    }
}
