// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{Datum, Row, RowArena};

/// Provides access to statistics stored for each Persist part (S3 data blob).
///
/// Statistics are best-effort, and individual stats may be omitted at any
/// time, e.g. if persist cannot determine them accurately, if the values are
/// too large to store in Consensus, if the statistics data is larger than
/// the part, etc.
pub trait PersistPartStats: std::fmt::Debug {
    /// The number of updates (Rows + errors) in the part.
    fn len(&self) -> Option<usize>;

    /// The number of errors in the part.
    fn err_count(&self) -> Option<usize>;

    /// The part's minimum value for the named column, if available.
    /// A return value of `None` indicates that Persist did not / was
    /// not able to calculate a minimum for this column.
    fn col_min<'a>(&self, name: &str, arena: &'a RowArena) -> Option<Datum<'a>>;

    /// (ditto above, but for the maximum column value)
    fn col_max<'a>(&self, name: &str, arena: &'a RowArena) -> Option<Datum<'a>>;

    /// The part's null count for the named column, if available. A
    /// return value of `None` indicates that Persist did not / was
    /// not able to calculate the null count for this column.
    fn col_null_count(&self, _name: &str) -> Option<usize>;

    /// A prefix of column values for the minimum Row in the part. A
    /// return of `None` indicates that Persist did not / was not able
    /// to calculate the minimum row. A `Some(usize)` indicates how many
    /// columns are in the prefix. The prefix may be less than the full
    /// row if persist cannot determine/store an individual column, for
    /// the same reasons that `col_min`/`col_max` may omit values.
    fn row_min(&self, _row: &mut Row) -> Option<usize>;

    /// (ditto above, but for the maximum row)
    fn row_max(&self, _row: &mut Row) -> Option<usize>;
}

/// WIP
///
/// Logic for filtering parts in a read as uninteresting purely from stats about
/// the data held in them.
pub trait PersistPartStatsFilter {
    /// WIP
    fn should_fetch<S: PersistPartStats>(&mut self, stats: S) -> bool;
}
