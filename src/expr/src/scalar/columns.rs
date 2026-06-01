// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generic interface for scalar expressions that hold columns.

use std::collections::{BTreeMap, BTreeSet};

pub trait Columns: Sized {
    /// True when the outermost structure is a column.
    fn is_column(&self) -> bool;

    /// If self is a column, return the column index, otherwise `None`.
    fn as_column(&self) -> Option<usize>;

    /// The support of the given set, i.e., the columns that are actually used.
    ///
    /// You can use `BTreeSet::last()` to extract the maximum column.
    fn support(&self) -> BTreeSet<usize> {
        let mut support = BTreeSet::new();
        self.support_into(&mut support);
        support
    }

    /// Adds the support of the given set, i.e., the columns that are actually used,
    /// to the given set.
    fn support_into(&self, support: &mut BTreeSet<usize>);

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    fn permute(&mut self, permutation: &[usize]) {
        self.visit_columns(|c| *c = permutation[*c]);
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    fn permute_map(&mut self, permutation: &BTreeMap<usize, usize>) {
        self.visit_columns(|c| *c = permutation[c]);
    }

    /// Visits each column reference and applies `action` to the column.
    ///
    /// Useful for remapping columns, or for collecting expression support.
    fn visit_columns<F>(&mut self, action: F)
    where
        F: FnMut(&mut usize);
}
