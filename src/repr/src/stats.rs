// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{Datum, Row, RowArena};

/// Provides access to statistics about SourceData stored in a Persist part (S3
/// data blob).
///
/// Statistics are best-effort, and individual stats may be omitted at any time,
/// e.g. if persist cannot determine them accurately, if the values are too
/// large to store in Consensus, if the statistics data is larger than the part,
/// if we wrote the data before we started collecting statistics, etc.
pub trait PersistSourceDataStats: std::fmt::Debug {
    /// The number of updates (Rows + errors) in the part.
    fn len(&self) -> Option<usize> {
        None
    }

    /// The number of errors in the part.
    fn err_count(&self) -> Option<usize> {
        None
    }

    /// The part's minimum value for the named column, if available.
    /// A return value of `None` indicates that Persist did not / was
    /// not able to calculate a minimum for this column.
    fn col_min<'a>(&'a self, _idx: usize, _arena: &'a RowArena) -> Option<Datum<'a>> {
        None
    }

    /// (ditto above, but for the maximum column value)
    fn col_max<'a>(&'a self, _idx: usize, _arena: &'a RowArena) -> Option<Datum<'a>> {
        None
    }

    /// The part's null count for the named column, if available. A
    /// return value of `None` indicates that Persist did not / was
    /// not able to calculate the null count for this column.
    fn col_null_count(&self, _idx: usize) -> Option<usize> {
        None
    }

    /// A prefix of column values for the minimum Row in the part. A
    /// return of `None` indicates that Persist did not / was not able
    /// to calculate the minimum row. A `Some(usize)` indicates how many
    /// columns are in the prefix. The prefix may be less than the full
    /// row if persist cannot determine/store an individual column, for
    /// the same reasons that `col_min`/`col_max` may omit values.
    ///
    /// TODO: If persist adds more "indexes" than the "primary" one (the order
    /// of columns returned by Schema), we'll want to generalize this to support
    /// other subsets of columns.
    fn row_min(&self, _row: &mut Row) -> Option<usize> {
        None
    }

    /// (ditto above, but for the maximum row)
    fn row_max(&self, _row: &mut Row) -> Option<usize> {
        None
    }
}
