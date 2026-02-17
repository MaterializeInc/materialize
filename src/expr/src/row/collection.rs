// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines types for working with collections of [`Row`].

use std::cell::RefCell;
use std::num::NonZeroUsize;

use itertools::Itertools;
use mz_repr::{
    DatumVec, IntoRowIterator, Row, RowIterator, RowRef, Rows, RowsBuilder, SharedSlice,
    UpdateCollection,
};
use serde::{Deserialize, Serialize};

use crate::ColumnOrder;

/// Collection of runs of sorted [`Row`]s represented as a single blob.
///
/// Note: the encoding format we use to represent [`Row`]s in this struct is
/// not stable, and thus should never be persisted durably.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowCollection {
    /// Contiguous blob of encoded Rows.
    rows: Rows,
    /// Metadata about an individual Row in the blob.
    diffs: SharedSlice<NonZeroUsize>,
}

#[derive(Debug)]
pub struct RowCollectionBuilder {
    rows: RowsBuilder,
    diffs: Vec<NonZeroUsize>,
}

impl RowCollectionBuilder {
    pub fn push(&mut self, row: &RowRef, diff: NonZeroUsize) {
        self.rows.push(row);
        self.diffs.push(diff);
    }

    pub fn build(self) -> RowCollection {
        RowCollection {
            rows: self.rows.build(),
            diffs: self.diffs.into(),
        }
    }
}

impl RowCollection {
    /// Create a new [`RowCollection`] from a collection of [`Row`]s.
    ///
    /// The order in which elements are pushed will be preserved.
    pub fn builder(byte_len_hint: usize, len_hint: usize) -> RowCollectionBuilder {
        RowCollectionBuilder {
            rows: Rows::builder(byte_len_hint, len_hint),
            diffs: Vec::with_capacity(len_hint),
        }
    }

    /// Create a new row collection from a update collection. If any update has a non-positive
    /// multiplicity, this will return an error.
    pub fn from_updates(updates: UpdateCollection) -> Result<RowCollection, String> {
        let rows = updates.rows().clone();
        let mut diffs = Vec::with_capacity(rows.len());
        for (row, time, diff) in updates.iter() {
            let Ok(count) = usize::try_from(diff.into_inner()) else {
                tracing::error!(
                    ?row,
                    ?time,
                    ?diff,
                    "encountered negative multiplicities in ok trace"
                );
                return Err(format!(
                    "Invalid data in source, saw retractions ({count}) for row that does not exist: {row:?}",
                    count = -diff,
                ));
            };
            diffs.push(NonZeroUsize::new(count).expect("diffs should be consolidated"));
        }
        Ok(Self {
            rows,
            diffs: diffs.into(),
        })
    }

    /// Create a new [`RowCollection`] from a collection of [`Row`]s. Sorts data by `order_by`.
    ///
    /// Note that all row collections to be merged must be constructed with the same `order_by`
    /// to ensure a consistent sort order. Anything else is undefined behavior.
    // TODO: Remember the `order_by` and assert that it is the same for all collections.
    pub fn new(mut rows: Vec<(Row, NonZeroUsize)>, order_by: &[ColumnOrder]) -> Self {
        // Sort data to maintain sortedness invariants.
        if order_by.is_empty() {
            // Skip row decoding if not required.
            rows.sort();
        } else {
            let (mut datum_vec1, mut datum_vec2) = (DatumVec::new(), DatumVec::new());
            rows.sort_by(|(row1, _diff1), (row2, _diff2)| {
                let borrow1 = datum_vec1.borrow_with(row1);
                let borrow2 = datum_vec2.borrow_with(row2);
                crate::compare_columns(order_by, &borrow1, &borrow2, || row1.cmp(row2))
            });
        }

        // Pre-sizing our buffer should allow us to make just 1 allocation, and
        // use the perfect amount of memory.
        //
        // Note(parkmycar): I didn't do any benchmarking to determine if this
        // is faster, so feel free to change this if you'd like.
        let encoded_size = rows.iter().map(|(row, _diff)| row.data_len()).sum();

        let mut builder = Self::builder(encoded_size, rows.len());
        for (row, diff) in rows {
            builder.push(row.as_row_ref(), diff);
        }
        builder.build()
    }

    fn iter(&self) -> impl Iterator<Item = (&RowRef, NonZeroUsize)> {
        self.rows.iter().zip_eq(self.diffs.iter().copied())
    }

    /// Concatenate another [`RowCollection`] onto `self`, copying and reallocating both sets of rows.
    ///
    /// This does not reorder the rows; the output will be sorted only if the inputs are.
    pub fn concat(&mut self, other: &RowCollection) {
        if other.count() == 0 {
            return;
        }

        // TODO(parkmycar): Using SegmentedBytes here would be nice.
        let byte_len = self.rows.byte_len() + other.rows.byte_len();
        let len = self.rows.len() + other.rows.len();
        let mut builder = Self::builder(byte_len, len);
        for (row, diff) in self.iter().chain(other.iter()) {
            builder.push(row, diff);
        }
        *self = builder.build();
    }

    /// Adjust a row count for the provided offset and limit.
    ///
    /// This is only marginally related to row collections, but many callers need to make
    /// this adjustment.
    pub fn offset_limit(mut total: usize, offset: usize, limit: Option<usize>) -> usize {
        // Consider a possible OFFSET.
        total = total.saturating_sub(offset);

        // Consider a possible LIMIT.
        if let Some(limit) = limit {
            total = std::cmp::min(limit, total);
        }

        total
    }

    /// Total count of [`Row`]s represented by this collection.
    pub fn count(&self) -> usize {
        self.diffs.iter().map(|u| u.get()).sum()
    }

    /// Total count of ([`Row`], `EncodedRowMetadata`) pairs in this collection.
    pub fn entries(&self) -> usize {
        self.rows.len()
    }

    /// Returns the number of bytes this [`RowCollection`] uses.
    pub fn byte_len(&self) -> usize {
        // Count both the bytes in the byte array and the size of the offsets themselves.
        let row_data_size = self
            .rows
            .byte_len()
            .saturating_add(self.rows.len().saturating_mul(size_of::<usize>()));
        let diff_size = self.diffs.len().saturating_mul(size_of::<u64>());
        row_data_size.saturating_add(diff_size)
    }

    /// Returns a [`RowRef`] for the entry at `idx`, if one exists.
    pub fn get(&self, idx: usize) -> Option<(&RowRef, &NonZeroUsize)> {
        Some((self.rows.get(idx)?, self.diffs.get(idx)?))
    }

    /// "Sorts" the [`RowCollection`] by the column order in `order_by`. The output will be sorted
    /// if the inputs were all sorted by the given order; otherwise, the order is unspecified.
    /// In either case, the output will be a [RowCollection] that contains the full contents of all
    /// the input collections.
    pub fn merge_sorted(runs: &[Self], order_by: &[ColumnOrder]) -> RowCollection {
        if order_by.is_empty() {
            Self::merge_sorted_inner(runs, &Ord::cmp)
        } else {
            let left_datum_vec = RefCell::new(mz_repr::DatumVec::new());
            let right_datum_vec = RefCell::new(mz_repr::DatumVec::new());

            let cmp = &|left: &RowRef, right: &RowRef| {
                let (mut left_datum_vec, mut right_datum_vec) =
                    (left_datum_vec.borrow_mut(), right_datum_vec.borrow_mut());
                let left_datums = left_datum_vec.borrow_with(left);
                let right_datums = right_datum_vec.borrow_with(right);
                crate::compare_columns(order_by, &left_datums, &right_datums, || left.cmp(right))
            };
            Self::merge_sorted_inner(runs, cmp)
        }
    }

    fn merge_sorted_inner<F>(runs: &[Self], cmp: &F) -> RowCollection
    where
        F: Fn(&RowRef, &RowRef) -> std::cmp::Ordering,
    {
        let mut metadata_len = 0;
        let mut encoded_len = 0;
        for collection in runs.iter() {
            metadata_len += collection.rows.len();
            encoded_len += collection.rows.byte_len();
        }

        let mut builder = Self::builder(encoded_len, metadata_len);

        for (row, diff) in
            mz_ore::iter::merge_iters_by(runs.iter().map(|r| r.iter()), |(r0, _), (r1, _)| {
                cmp(r0, r1)
            })
        {
            builder.push(row, diff);
        }
        builder.build()
    }
}

#[derive(Debug, Clone)]
pub struct RowCollectionIter {
    /// Collection we're iterating over.
    collection: RowCollection,

    /// Index for the row we're currently referencing.
    row_idx: usize,
    /// Number of diffs we've emitted for the current row.
    diff_idx: usize,

    /// Maximum number of rows this iterator will yield.
    limit: Option<usize>,
    /// Number of rows we're offset by.
    ///
    /// Note: We eagerly apply an offset, but we track it here so we can
    /// accurately report [`RowIterator::count`].
    offset: usize,

    /// Columns to underlying rows to include.
    projection: Option<Vec<usize>>,
    /// Allocations that we reuse for every iteration to project columns.
    projection_buf: (DatumVec, Row),
}

impl RowCollectionIter {
    /// Returns the inner `RowCollection`.
    pub fn into_inner(self) -> RowCollection {
        self.collection
    }

    /// Immediately applies an offset to this iterator.
    pub fn apply_offset(mut self, offset: usize) -> RowCollectionIter {
        Self::advance_by(
            &self.collection,
            &mut self.row_idx,
            &mut self.diff_idx,
            offset,
        );

        // Keep track of how many rows we've offset by.
        self.offset = self.offset.saturating_add(offset);

        self
    }

    /// Sets the limit for this iterator.
    pub fn with_limit(mut self, limit: usize) -> RowCollectionIter {
        self.limit = Some(limit);
        self
    }

    /// Specify the columns that should be yielded.
    pub fn with_projection(mut self, projection: Vec<usize>) -> RowCollectionIter {
        // Omit the projection if it would be a no-op to avoid a relatively expensive memcpy.
        if let Some((row, _)) = self.collection.get(0) {
            let cols = row.into_iter().enumerate().map(|(idx, _datum)| idx);
            if projection.iter().copied().eq(cols) {
                return self;
            }
        }

        self.projection = Some(projection);
        self
    }

    /// Helper method for implementing [`RowIterator`].
    ///
    /// Advances the internal pointers by the specified amount.
    fn advance_by(
        collection: &RowCollection,
        row_idx: &mut usize,
        diff_idx: &mut usize,
        mut count: usize,
    ) {
        while count > 0 {
            let Some((_, row_meta)) = collection.get(*row_idx) else {
                return;
            };

            let remaining_diff = row_meta.get() - *diff_idx;
            if remaining_diff <= count {
                *diff_idx = 0;
                *row_idx += 1;
                count -= remaining_diff;
            } else {
                *diff_idx += count;
                count = 0;
            }
        }
    }

    /// Helper method for implementing [`RowIterator`].
    ///
    /// Projects columns for the provided `row`.
    fn project<'a>(
        projection: Option<&[usize]>,
        row: &'a RowRef,
        datum_buf: &'a mut DatumVec,
        row_buf: &'a mut Row,
    ) -> &'a RowRef {
        if let Some(projection) = projection {
            // Copy the required columns into our reusable buffer.
            {
                let datums = datum_buf.borrow_with(row);
                row_buf
                    .packer()
                    .extend(projection.iter().map(|i| &datums[*i]));
            }

            row_buf
        } else {
            row
        }
    }
}

impl RowIterator for RowCollectionIter {
    fn next(&mut self) -> Option<&RowRef> {
        // Bail if we've reached our limit.
        if let Some(0) = self.limit {
            return None;
        }

        let row = self.collection.get(self.row_idx).map(|(r, _)| r)?;

        // If we're about to yield a row, then subtract from our limit.
        if let Some(limit) = &mut self.limit {
            *limit = limit.saturating_sub(1);
        }

        // Advance to the next row.
        Self::advance_by(&self.collection, &mut self.row_idx, &mut self.diff_idx, 1);

        // Project away and/or re-order any columns.
        let (datum_buf, row_buf) = &mut self.projection_buf;
        Some(Self::project(
            self.projection.as_deref(),
            row,
            datum_buf,
            row_buf,
        ))
    }

    fn peek(&mut self) -> Option<&RowRef> {
        // Bail if we've reached our limit.
        if let Some(0) = self.limit {
            return None;
        }

        let row = self.collection.get(self.row_idx).map(|(r, _)| r)?;

        // Note: Unlike `next()` we do not subtract from our limit, nor advance
        // the internal pointers.

        // Project away and/or re-order any columns.
        let (datum_buf, row_buf) = &mut self.projection_buf;
        Some(Self::project(
            self.projection.as_deref(),
            row,
            datum_buf,
            row_buf,
        ))
    }

    fn count(&self) -> usize {
        RowCollection::offset_limit(self.collection.count(), self.offset, self.limit)
    }

    fn box_clone(&self) -> Box<dyn RowIterator> {
        Box::new(self.clone())
    }
}

impl IntoRowIterator for RowCollection {
    type Iter = RowCollectionIter;

    fn into_row_iter(self) -> Self::Iter {
        RowCollectionIter {
            collection: self,
            row_idx: 0,
            diff_idx: 0,
            limit: None,
            offset: 0,
            projection: None,
            // Note: Neither of these types allocate until elements are pushed in.
            projection_buf: (DatumVec::new(), Row::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use mz_ore::assert_none;
    use mz_repr::Datum;
    use proptest::prelude::*;
    use proptest::test_runner::Config;

    use super::*;

    impl<'a, T: IntoIterator<Item = &'a Row>> From<T> for RowCollection {
        fn from(rows: T) -> Self {
            let mut encoded = Rows::builder(0, 0);
            let mut diffs = vec![];

            for row in rows {
                encoded.push(row.as_row_ref());
                diffs.push(NonZeroUsize::MIN);
            }

            RowCollection {
                rows: encoded.build(),
                diffs: diffs.into(),
            }
        }
    }

    #[mz_ore::test]
    fn test_row_collection() {
        let a = Row::pack_slice(&[Datum::False, Datum::String("hello world"), Datum::Int16(42)]);
        let b = Row::pack_slice(&[Datum::MzTimestamp(mz_repr::Timestamp::new(10))]);

        let collection = RowCollection::from([&a, &b]);

        let (a_rnd, _) = collection.get(0).unwrap();
        assert_eq!(a_rnd, a.borrow());

        let (b_rnd, _) = collection.get(1).unwrap();
        assert_eq!(b_rnd, b.borrow());
    }

    #[mz_ore::test]
    fn test_merge() {
        let a = Row::pack_slice(&[Datum::False, Datum::String("hello world"), Datum::Int16(42)]);
        let b = Row::pack_slice(&[Datum::MzTimestamp(mz_repr::Timestamp::new(10))]);

        let mut a_col = RowCollection::from([&a]);
        let b_col = RowCollection::from([&b]);

        a_col.concat(&b_col);

        assert_eq!(a_col.count(), 2);
        assert_eq!(a_col.get(0).map(|(r, _)| r), Some(a.borrow()));
        assert_eq!(a_col.get(1).map(|(r, _)| r), Some(b.borrow()));
    }

    #[mz_ore::test]
    fn test_sort() {
        let a = Row::pack_slice(&[Datum::False, Datum::String("hello world"), Datum::Int16(42)]);
        let b = Row::pack_slice(&[Datum::MzTimestamp(mz_repr::Timestamp::new(10))]);
        let c = Row::pack_slice(&[Datum::True, Datum::String("hello world"), Datum::Int16(42)]);
        let d = Row::pack_slice(&[Datum::MzTimestamp(mz_repr::Timestamp::new(9))]);

        let cols = {
            let mut part = [&a, &b];
            part.sort_by(|a, b| a.cmp(b));
            let part1 = RowCollection::from(part);
            let mut part = [&c, &d];
            part.sort_by(|a, b| a.cmp(b));
            let part2 = RowCollection::from(part);
            vec![part1, part2]
        };
        let mut rows = [a, b, c, d];

        let sorted_view = RowCollection::merge_sorted(&cols, &[]);
        rows.sort_by(|a, b| a.cmp(b));

        for i in 0..rows.len() {
            let (row_x, _) = sorted_view.get(i).unwrap();
            let row_y = rows.get(i).unwrap();

            assert_eq!(row_x, row_y.borrow());
        }
    }

    #[mz_ore::test]
    fn test_sorted_iter() {
        let a = Row::pack_slice(&[Datum::String("hello world")]);
        let b = Row::pack_slice(&[Datum::UInt32(42)]);
        let col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(3).unwrap())], &[]);
        let col = RowCollection::merge_sorted(
            &[
                col,
                RowCollection::new(vec![(b.clone(), NonZeroUsize::new(2).unwrap())], &[]),
            ],
            &[],
        );
        let mut iter = col.into_row_iter();

        // Peek shouldn't advance the iterator.
        assert_eq!(iter.peek(), Some(b.borrow()));

        assert_eq!(iter.next(), Some(b.borrow()));
        assert_eq!(iter.next(), Some(b.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), None);

        // For good measure make sure we don't panic.
        assert_eq!(iter.next(), None);
        assert_eq!(iter.peek(), None);
    }

    #[mz_ore::test]
    fn test_sorted_iter_offset() {
        let a = Row::pack_slice(&[Datum::String("hello world")]);
        let b = Row::pack_slice(&[Datum::UInt32(42)]);
        let col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(3).unwrap())], &[]);
        let col = RowCollection::merge_sorted(
            &[
                col,
                RowCollection::new(vec![(b.clone(), NonZeroUsize::new(2).unwrap())], &[]),
            ],
            &[],
        );

        // Test with a reasonable offset that does not span rows.
        let mut iter = col.into_row_iter().apply_offset(1);
        assert_eq!(iter.next(), Some(b.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let col = iter.into_inner();

        // Test with an offset that spans the first row.
        let mut iter = col.into_row_iter().apply_offset(3);

        assert_eq!(iter.peek(), Some(a.borrow()));

        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let col = iter.into_inner();

        // Test with an offset that passes the entire collection.
        let mut iter = col.into_row_iter().apply_offset(100);
        assert_eq!(iter.peek(), None);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.peek(), None);
        assert_eq!(iter.next(), None);
    }

    #[mz_ore::test]
    fn test_sorted_iter_limit() {
        let a = Row::pack_slice(&[Datum::String("hello world")]);
        let b = Row::pack_slice(&[Datum::UInt32(42)]);
        let col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(3).unwrap())], &[]);
        let col = RowCollection::merge_sorted(
            &[
                col,
                RowCollection::new(vec![(b.clone(), NonZeroUsize::new(2).unwrap())], &[]),
            ],
            &[],
        );

        // Test with a limit that spans only the first row.
        let mut iter = col.into_row_iter().with_limit(1);
        assert_eq!(iter.next(), Some(b.borrow()));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let col = iter.into_inner();

        // Test with a limit that spans both rows.
        let mut iter = col.into_row_iter().with_limit(4);
        assert_eq!(iter.peek(), Some(b.borrow()));
        assert_eq!(iter.next(), Some(b.borrow()));
        assert_eq!(iter.next(), Some(b.borrow()));

        assert_eq!(iter.peek(), Some(a.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));

        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let col = iter.into_inner();

        // Test with a limit that is more rows than we have.
        let mut iter = col.into_row_iter().with_limit(1000);
        assert_eq!(iter.next(), Some(b.borrow()));
        assert_eq!(iter.next(), Some(b.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), Some(a.borrow()));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let col = iter.into_inner();

        // Test with a limit of 0.
        let mut iter = col.into_row_iter().with_limit(0);
        assert_eq!(iter.peek(), None);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[mz_ore::test]
    fn test_mapped_row_iterator() {
        let a = Row::pack_slice(&[Datum::String("hello world")]);
        let col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(3).unwrap())], &[]);

        // Make sure we can call `.map` on a `dyn RowIterator`.
        let iter: Box<dyn RowIterator> = Box::new(col.into_row_iter());

        let mut mapped = iter.map(|f| f.to_owned());
        assert!(mapped.next().is_some());
        assert!(mapped.next().is_some());
        assert!(mapped.next().is_some());
        assert_none!(mapped.next());
        assert_none!(mapped.next());
    }

    #[mz_ore::test]
    fn test_projected_row_iterator() {
        let a = Row::pack_slice(&[Datum::String("hello world"), Datum::Int16(42)]);
        let col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(2).unwrap())], &[]);

        // Project away the first column.
        let mut iter = col.into_row_iter().with_projection(vec![1]);

        let projected_a = Row::pack_slice(&[Datum::Int16(42)]);
        assert_eq!(iter.next(), Some(projected_a.as_ref()));
        assert_eq!(iter.next(), Some(projected_a.as_ref()));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let col = iter.into_inner();

        // Project away all columns.
        let mut iter = col.into_row_iter().with_projection(vec![]);

        let projected_a = Row::default();
        assert_eq!(iter.next(), Some(projected_a.as_ref()));
        assert_eq!(iter.next(), Some(projected_a.as_ref()));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let col = iter.into_inner();

        // Include all columns.
        let mut iter = col.into_row_iter().with_projection(vec![0, 1]);

        assert_eq!(iter.next(), Some(a.as_ref()));
        assert_eq!(iter.next(), Some(a.as_ref()));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let col = iter.into_inner();

        // Swap the order of columns.
        let mut iter = col.into_row_iter().with_projection(vec![1, 0]);

        let projected_a = Row::pack_slice(&[Datum::Int16(42), Datum::String("hello world")]);
        assert_eq!(iter.next(), Some(projected_a.as_ref()));
        assert_eq!(iter.next(), Some(projected_a.as_ref()));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[mz_ore::test]
    fn test_count_respects_limit_and_offset() {
        let a = Row::pack_slice(&[Datum::String("hello world")]);
        let b = Row::pack_slice(&[Datum::UInt32(42)]);
        let col = RowCollection::new(
            vec![
                (a.clone(), NonZeroUsize::new(3).unwrap()),
                (b.clone(), NonZeroUsize::new(2).unwrap()),
            ],
            &[],
        );

        // How many total rows there are.
        let iter = col.into_row_iter();
        assert_eq!(iter.count(), 5);

        let col = iter.into_inner();

        // With a LIMIT.
        let iter = col.into_row_iter().with_limit(1);
        assert_eq!(iter.count(), 1);

        let col = iter.into_inner();

        // With a LIMIT larger than the total number of rows.
        let iter = col.into_row_iter().with_limit(100);
        assert_eq!(iter.count(), 5);

        let col = iter.into_inner();

        // With an OFFSET.
        let iter = col.into_row_iter().apply_offset(3);
        assert_eq!(iter.count(), 2);

        let col = iter.into_inner();

        // With an OFFSET greater than the total number of rows.
        let iter = col.into_row_iter().apply_offset(100);
        assert_eq!(iter.count(), 0);

        let col = iter.into_inner();

        // With a LIMIT and an OFFSET.
        let iter = col.into_row_iter().with_limit(2).apply_offset(4);
        assert_eq!(iter.count(), 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn proptest_row_collection() {
        fn row_collection_roundtrips(rows: Vec<Row>) {
            let collection = RowCollection::from(&rows);

            for i in 0..rows.len() {
                let (a, _) = collection.get(i).unwrap();
                let b = rows.get(i).unwrap().borrow();

                assert_eq!(a, b);
            }
        }

        // This test is slow, so we limit the default number of test cases.
        proptest!(
            Config { cases: 5, ..Default::default() },
            |(rows in any::<Vec<Row>>())| {
                // The proptest! macro interferes with rustfmt.
                row_collection_roundtrips(rows)
            }
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn proptest_merge() {
        fn row_collection_merge(a: Vec<Row>, b: Vec<Row>) {
            let mut a_col = RowCollection::from(&a);
            let b_col = RowCollection::from(&b);

            a_col.concat(&b_col);

            let all_rows = a.iter().chain(b.iter());
            for (idx, row) in all_rows.enumerate() {
                let (col_row, _) = a_col.get(idx).unwrap();
                assert_eq!(col_row, row.borrow());
            }
        }

        // This test is slow, so we limit the default number of test cases.
        proptest!(
            Config { cases: 3, ..Default::default() },
            |(a in any::<Vec<Row>>(), b in any::<Vec<Row>>())| {
                // The proptest! macro interferes with rustfmt.
                row_collection_merge(a, b)
            }
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn proptest_sort() {
        fn row_collection_sort(mut a: Vec<Row>, mut b: Vec<Row>) {
            a.sort_by(|a, b| a.cmp(b));
            b.sort_by(|a, b| a.cmp(b));

            let sorted_view = RowCollection::merge_sorted(
                &[RowCollection::from(&a), RowCollection::from(&b)],
                &[],
            );

            a.append(&mut b);
            a.sort_by(|a, b| a.cmp(b));

            for i in 0..a.len() {
                let (row_x, _) = sorted_view.get(i).unwrap();
                let row_y = a.get(i).unwrap();

                assert_eq!(row_x, row_y.borrow());
            }
        }

        // This test is slow, so we limit the default number of test cases.
        proptest!(
            Config { cases: 5, ..Default::default() },
            |(a in any::<Vec<Row>>(), b in any::<Vec<Row>>())| {
                // The proptest! macro interferes with rustfmt.
                row_collection_sort(a, b)
            }
        );
    }
}
