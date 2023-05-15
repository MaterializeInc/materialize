// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Type erased columnar structs.

use std::collections::BTreeMap;

use crate::columnar::Data;
use crate::dyn_col::{DynColumnMut, DynColumnRef};
use crate::stats::{DynStats, StatsFn};

/// A set of shared references to named columns.
///
/// This type implements a "builder"-esque pattern to help
/// [crate::columnar::Schema::decoder] impls. All columns should be removed via
/// [Self::col] and the [Self::finish] called to verify that all columns have
/// been accounted for.
#[derive(Debug)]
pub struct ColumnsRef<'a> {
    pub(crate) cols: BTreeMap<&'a str, &'a DynColumnRef>,
}

impl<'a> ColumnsRef<'a> {
    /// Removes the named typed column from the set.
    pub fn col<T: Data>(&mut self, name: &str) -> Result<&'a T::Col, String> {
        self.dyn_col(name)?.downcast::<T>()
    }

    /// Computes statistics for the named column and removes it from the set.
    pub fn stats(&mut self, name: &str, stats_fn: StatsFn) -> Result<Box<dyn DynStats>, String> {
        let col = self.dyn_col(name)?;
        match stats_fn {
            StatsFn::Default => Ok(col.stats_default()),
            StatsFn::Custom(stats_fn) => stats_fn(col),
        }
    }

    /// Removes the named dynamic column from the set.
    fn dyn_col(&mut self, name: &str) -> Result<&'a DynColumnRef, String> {
        self.cols
            .remove(name)
            .ok_or_else(|| format!("no col named {}", name))
    }

    /// Verifies that all columns in the set have been removed.
    pub fn finish(self) -> Result<(), String> {
        if self.cols.is_empty() {
            Ok(())
        } else {
            let names = self.cols.iter().map(|(x, _)| *x).collect::<Vec<_>>();
            Err(format!("unused cols: {}", names.join(" ")))
        }
    }
}

/// A set of exclusive references to named columns.
///
/// This type implements a "builder"-esque pattern to help
/// [crate::columnar::Schema::encoder] impls. All columns should be removed via
/// [Self::col] and the [Self::finish] called to verify that all columns have
/// been accounted for.
#[derive(Debug)]
pub struct ColumnsMut<'a> {
    pub(crate) cols: BTreeMap<&'a str, &'a mut DynColumnMut>,
}

impl<'a> ColumnsMut<'a> {
    /// Removes the named column from the set.
    pub fn col<T: Data>(&mut self, name: &str) -> Result<&'a mut T::Mut, String> {
        let col = self
            .cols
            .remove(name)
            .ok_or_else(|| format!("no col named {}", name))?;
        col.downcast::<T>()
    }

    /// Verifies that all columns in the set have been removed.
    pub fn finish(self) -> Result<(), String> {
        if self.cols.is_empty() {
            Ok(())
        } else {
            let names = self.cols.iter().map(|(x, _)| *x).collect::<Vec<_>>();
            Err(format!("unused cols: {}", names.join(" ")))
        }
    }
}
