// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Aggregate statistics about data stored in persist.

use std::fmt::Debug;

use arrow::array::{
    BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};

use crate::stats::primitive::{
    truncate_bytes, truncate_string, PrimitiveStats, TruncateBound, TRUNCATE_LEN,
};
use crate::stats::{DynStats, NoneStats};

/// A type that can incrementally collect stats from a sequence of values.
pub trait ColumnarStatsBuilder<T>: Debug {
    /// Type of [`arrow`] column these statistics can be derived from.
    type ArrowColumn: arrow::array::Array + 'static;
    /// The type of statistics the collector finalizes into.
    type FinishedStats: DynStats;

    /// Create a new instance of `Self` that can be used to collect statistics.
    fn new() -> Self
    where
        Self: Sized;

    /// Derive statistics from a column of data.
    fn from_column(col: &Self::ArrowColumn) -> Self
    where
        Self: Sized;
    /// Derive statistics from an opaque column of data.
    ///
    /// Returns `None` if the opaque column is not the same type as
    /// [`Self::ArrowColumn`].
    fn from_column_dyn(col: &dyn arrow::array::Array) -> Option<Self>
    where
        Self: Sized,
    {
        let col = col.as_any().downcast_ref::<Self::ArrowColumn>()?;
        Some(Self::from_column(col))
    }

    /// Include the provided value in the current statistics.
    fn include(&mut self, val: T);

    /// Finish this collector returning the final aggregated statistics.
    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized;
}

/// We collect stats for all primitive types in exactly the same way. This
/// macro de-duplicates some of that logic.
///
/// Note: If at any point someone finds this macro too complext, they should
/// feel free to refactor it away!
macro_rules! primitive_stats {
    ($native:ty, $arrow_col:ty, $min_fn:path, $max_fn:path) => {
        impl ColumnarStatsBuilder<$native> for PrimitiveStats<$native> {
            type FinishedStats = Self;
            type ArrowColumn = $arrow_col;

            fn new() -> Self
            where
                Self: Sized,
            {
                PrimitiveStats {
                    lower: <$native>::default(),
                    upper: <$native>::default(),
                }
            }

            fn from_column(col: &Self::ArrowColumn) -> Self
            where
                Self: Sized,
            {
                let lower = $min_fn(col).unwrap_or_default();
                let upper = $max_fn(col).unwrap_or_default();

                PrimitiveStats { lower, upper }
            }

            fn include(&mut self, val: $native) {
                self.lower = val.min(self.lower);
                self.upper = val.max(self.upper);
            }

            fn finish(self) -> Self::FinishedStats {
                self
            }
        }
    };
}

primitive_stats!(
    bool,
    BooleanArray,
    arrow::compute::min_boolean,
    arrow::compute::max_boolean
);
primitive_stats!(u8, UInt8Array, arrow::compute::min, arrow::compute::max);
primitive_stats!(u16, UInt16Array, arrow::compute::min, arrow::compute::max);
primitive_stats!(u32, UInt32Array, arrow::compute::min, arrow::compute::max);
primitive_stats!(u64, UInt64Array, arrow::compute::min, arrow::compute::max);
primitive_stats!(i8, Int8Array, arrow::compute::min, arrow::compute::max);
primitive_stats!(i16, Int16Array, arrow::compute::min, arrow::compute::max);
primitive_stats!(i32, Int32Array, arrow::compute::min, arrow::compute::max);
primitive_stats!(i64, Int64Array, arrow::compute::min, arrow::compute::max);
primitive_stats!(f32, Float32Array, arrow::compute::min, arrow::compute::max);
primitive_stats!(f64, Float64Array, arrow::compute::min, arrow::compute::max);

impl ColumnarStatsBuilder<&str> for PrimitiveStats<String> {
    type ArrowColumn = StringArray;
    type FinishedStats = Self;

    fn new() -> Self
    where
        Self: Sized,
    {
        PrimitiveStats {
            lower: String::default(),
            upper: String::default(),
        }
    }

    fn from_column(col: &Self::ArrowColumn) -> Self
    where
        Self: Sized,
    {
        let lower = arrow::compute::min_string(col).unwrap_or_default();
        let lower = truncate_string(lower, TRUNCATE_LEN, TruncateBound::Lower)
            .expect("lower bound should always truncate");
        let upper = arrow::compute::max_string(col).unwrap_or_default();
        let upper = truncate_string(upper, TRUNCATE_LEN, TruncateBound::Upper)
            // NB: The cost+trim stuff will remove the column entirely if
            // it's still too big (also this should be extremely rare in
            // practice).
            .unwrap_or_else(|| upper.to_owned());

        PrimitiveStats { lower, upper }
    }

    fn include(&mut self, val: &str) {
        let lower_candidate = truncate_string(val, TRUNCATE_LEN, TruncateBound::Lower)
            .expect("lower bound should always truncate");
        if lower_candidate < self.lower {
            self.lower = lower_candidate
        }

        let upper_candidate = truncate_string(val, TRUNCATE_LEN, TruncateBound::Upper)
            // NB: The cost+trim stuff will remove the column entirely if
            // it's still too big (also this should be extremely rare in
            // practice).
            .unwrap_or_else(|| val.to_owned());
        if upper_candidate < self.upper {
            self.upper = upper_candidate
        }
    }

    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized,
    {
        self
    }
}

impl ColumnarStatsBuilder<&[u8]> for PrimitiveStats<Vec<u8>> {
    type ArrowColumn = BinaryArray;
    type FinishedStats = Self;

    fn new() -> Self
    where
        Self: Sized,
    {
        PrimitiveStats {
            lower: Vec::default(),
            upper: Vec::default(),
        }
    }

    fn from_column(col: &Self::ArrowColumn) -> Self
    where
        Self: Sized,
    {
        let lower = arrow::compute::min_binary(col).unwrap_or_default();
        let lower = truncate_bytes(lower, TRUNCATE_LEN, TruncateBound::Lower)
            .expect("lower bound should always truncate");
        let upper = arrow::compute::max_binary(col).unwrap_or_default();
        let upper = truncate_bytes(upper, TRUNCATE_LEN, TruncateBound::Upper)
            // NB: The cost+trim stuff will remove the column entirely if
            // it's still too big (also this should be extremely rare in
            // practice).
            .unwrap_or_else(|| upper.to_owned());

        PrimitiveStats { lower, upper }
    }

    fn include(&mut self, val: &[u8]) {
        let lower_candidate = truncate_bytes(val, TRUNCATE_LEN, TruncateBound::Lower)
            .expect("lower bound should always truncate");
        if lower_candidate < self.lower {
            self.lower = lower_candidate
        }

        let upper_candidate = truncate_bytes(val, TRUNCATE_LEN, TruncateBound::Upper)
            // NB: The cost+trim stuff will remove the column entirely if
            // it's still too big (also this should be extremely rare in
            // practice).
            .unwrap_or_else(|| val.to_owned());
        if upper_candidate < self.upper {
            self.upper = upper_candidate
        }
    }

    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized,
    {
        self
    }
}

/// Empty statistics for a column of data.
#[derive(Debug)]
pub struct NoneStatsBuilder<A>(std::marker::PhantomData<A>);

impl<T, A: arrow::array::Array + 'static> ColumnarStatsBuilder<T> for NoneStatsBuilder<A> {
    type ArrowColumn = A;
    type FinishedStats = NoneStats;

    fn new() -> Self
    where
        Self: Sized,
    {
        NoneStatsBuilder(std::marker::PhantomData)
    }

    fn from_column(_col: &Self::ArrowColumn) -> Self
    where
        Self: Sized,
    {
        NoneStatsBuilder(std::marker::PhantomData)
    }

    fn include(&mut self, _val: T) {}

    fn finish(self) -> Self::FinishedStats
    where
        Self::FinishedStats: Sized,
    {
        NoneStats
    }
}
