// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines types for working with collections of [`Row`].

use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use mz_ore::cast::CastFrom;
use mz_proto::RustType;
use serde::{Deserialize, Serialize};

use crate::row::iter::{IntoRowIterator, RowIterator};
use crate::row::{Row, RowRef};
use crate::DatumVec;

include!(concat!(env!("OUT_DIR"), "/mz_repr.row.collection.rs"));

/// Collection of [`Row`]s represented as a single blob.
///
/// Note: the encoding format we use to represent [`Row`]s in this struct is
/// not stable, and thus should never be persisted durably.
#[derive(Default, Debug, Clone, PartialEq)]
pub struct RowCollection {
    /// Contiguous blob of encoded Rows.
    encoded: Bytes,
    /// Metadata about an individual Row in the blob.
    metadata: Vec<EncodedRowMetadata>,
}

impl RowCollection {
    /// Create a new [`RowCollection`] from a collection of [`Row`]s.
    pub fn new(rows: &[(Row, NonZeroUsize)]) -> Self {
        // Pre-sizing our buffer should allow us to make just 1 allocation, and
        // use the perfect amount of memory.
        //
        // Note(parkmycar): I didn't do any benchmarking to determine if this
        // is faster, so feel free to change this if you'd like.
        let encoded_size = rows.iter().map(|(row, _diff)| row.data.len()).sum();

        let mut encoded = Vec::<u8>::with_capacity(encoded_size);
        let mut metadata = Vec::<EncodedRowMetadata>::with_capacity(rows.len());

        for (row, diff) in rows {
            encoded.extend(row.data());
            metadata.push(EncodedRowMetadata {
                offset: encoded.len(),
                diff: *diff,
            });
        }

        RowCollection {
            encoded: Bytes::from(encoded),
            metadata,
        }
    }

    /// Merge another [`RowCollection`] into `self`.
    pub fn merge(&mut self, other: &RowCollection) {
        if other.count(0, None) == 0 {
            return;
        }

        // TODO(parkmycar): Using SegmentedBytes here would be nice.
        let mut new_bytes = vec![0; self.encoded.len() + other.encoded.len()];
        new_bytes[..self.encoded.len()].copy_from_slice(&self.encoded[..]);
        new_bytes[self.encoded.len()..].copy_from_slice(&other.encoded[..]);

        let mapped_metas = other.metadata.iter().map(|meta| EncodedRowMetadata {
            offset: meta.offset + self.encoded.len(),
            diff: meta.diff,
        });

        self.metadata.extend(mapped_metas);
        self.encoded = Bytes::from(new_bytes);
    }

    /// Total count of [`Row`]s represented by this collection, considering a
    /// possible `OFFSET` and `LIMIT`.
    pub fn count(&self, offset: usize, limit: Option<usize>) -> usize {
        let mut total: usize = self.metadata.iter().map(|meta| meta.diff.get()).sum();

        // Consider a possible OFFSET.
        total = total.saturating_sub(offset);

        // Consider a possible LIMIT.
        if let Some(limit) = limit {
            total = std::cmp::min(limit, total);
        }

        total
    }

    /// Total count of ([`Row`], `EncodedRowMetadata`) pairs in this collection.
    pub fn entries(&self) -> usize {
        self.metadata.len()
    }

    /// Returns the number of bytes this [`RowCollection`] uses.
    pub fn byte_len(&self) -> usize {
        let row_data_size = self.encoded.len();
        let metadata_size = self
            .metadata
            .len()
            .saturating_mul(std::mem::size_of::<EncodedRowMetadata>());

        row_data_size.saturating_add(metadata_size)
    }

    /// Returns a [`RowRef`] for the entry at `idx`, if one exists.
    pub fn get(&self, idx: usize) -> Option<(&RowRef, &EncodedRowMetadata)> {
        let (lower_offset, upper) = match idx {
            0 => (0, self.metadata.get(idx)?),
            _ => {
                let lower = self.metadata.get(idx - 1).map(|m| m.offset)?;
                let upper = self.metadata.get(idx)?;
                (lower, upper)
            }
        };

        let slice = &self.encoded[lower_offset..upper.offset];
        let row = RowRef::from_slice(slice);

        Some((row, upper))
    }

    /// "Sorts" the [`RowCollection`] by returning a sorted view over the collection.
    pub fn sorted_view<F>(self, mut f: F) -> SortedRowCollection
    where
        F: FnMut(&RowRef, &RowRef) -> std::cmp::Ordering,
    {
        let mut view: Vec<_> = (0..self.metadata.len()).collect();
        view.sort_by(|a, b| {
            let (a_row, _) = self.get(*a).expect("index invalid?");
            let (b_row, _) = self.get(*b).expect("index invalid?");
            f(a_row, b_row)
        });

        SortedRowCollection {
            collection: self,
            sorted_view: view.into(),
        }
    }
}

impl<'a, T: IntoIterator<Item = &'a Row>> From<T> for RowCollection {
    fn from(rows: T) -> Self {
        let mut encoded = Vec::<u8>::new();
        let mut metadata = Vec::<EncodedRowMetadata>::new();

        for row in rows {
            encoded.extend(row.data());
            metadata.push(EncodedRowMetadata {
                offset: encoded.len(),
                diff: unsafe { NonZeroUsize::new_unchecked(1) },
            });
        }

        RowCollection {
            encoded: Bytes::from(encoded),
            metadata,
        }
    }
}

impl RustType<ProtoRowCollection> for RowCollection {
    fn into_proto(&self) -> ProtoRowCollection {
        ProtoRowCollection {
            encoded: Bytes::clone(&self.encoded),
            metadata: self
                .metadata
                .iter()
                .map(EncodedRowMetadata::into_proto)
                .collect(),
        }
    }

    fn from_proto(proto: ProtoRowCollection) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(RowCollection {
            encoded: proto.encoded,
            metadata: proto
                .metadata
                .into_iter()
                .map(EncodedRowMetadata::from_proto)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Inner type of [`RowCollection`], describes a single Row.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct EncodedRowMetadata {
    /// Offset into the binary blob of encoded rows.
    ///
    /// TODO(parkmycar): Consider making this a `u32`.
    offset: usize,
    /// Diff for the Row.
    ///
    /// TODO(parkmycar): Consider making this a smaller type, note that some compute introspection
    /// collections, e.g. `mz_scheduling_elapsed_raw`, encodes nano seconds in the diff field which
    /// requires a u64.
    diff: NonZeroUsize,
}

impl RustType<ProtoEncodedRowMetadata> for EncodedRowMetadata {
    fn into_proto(&self) -> ProtoEncodedRowMetadata {
        ProtoEncodedRowMetadata {
            offset: u64::cast_from(self.offset),
            diff: self.diff.into_proto(),
        }
    }

    fn from_proto(proto: ProtoEncodedRowMetadata) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(EncodedRowMetadata {
            offset: usize::cast_from(proto.offset),
            diff: NonZeroUsize::from_proto(proto.diff)?,
        })
    }
}

/// Provides a sorted view of a [`RowCollection`].
#[derive(Debug, Clone)]
pub struct SortedRowCollection {
    /// The inner [`RowCollection`].
    collection: RowCollection,
    /// Indexes into the inner collection that represent the sorted order.
    sorted_view: Arc<[usize]>,
}

impl SortedRowCollection {
    pub fn get(&self, idx: usize) -> Option<(&RowRef, &EncodedRowMetadata)> {
        self.sorted_view
            .get(idx)
            .and_then(|inner_idx| self.collection.get(*inner_idx))
    }
}

#[derive(Debug, Clone)]
pub struct SortedRowCollectionIter {
    /// Collection we're iterating over.
    collection: SortedRowCollection,

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

impl SortedRowCollectionIter {
    /// Returns the inner `SortedRowCollection`.
    pub fn into_inner(self) -> SortedRowCollection {
        self.collection
    }

    /// Immediately applies an offset to this iterator.
    pub fn apply_offset(mut self, offset: usize) -> SortedRowCollectionIter {
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
    pub fn with_limit(mut self, limit: usize) -> SortedRowCollectionIter {
        self.limit = Some(limit);
        self
    }

    /// Specify the columns that should be yielded.
    pub fn with_projection(mut self, projection: Vec<usize>) -> SortedRowCollectionIter {
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
        collection: &SortedRowCollection,
        row_idx: &mut usize,
        diff_idx: &mut usize,
        mut count: usize,
    ) {
        while count > 0 {
            let Some((_, row_meta)) = collection.get(*row_idx) else {
                return;
            };

            let remaining_diff = row_meta.diff.get() - *diff_idx;
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
        projection: Option<&'a Vec<usize>>,
        row: &'a RowRef,
        datum_buf: &'a mut DatumVec,
        row_buf: &'a mut Row,
    ) -> Option<&'a RowRef> {
        if let Some(projection) = projection.as_ref() {
            // Copy the required columns into our reusable buffer.
            {
                let datums = datum_buf.borrow_with(row);
                row_buf
                    .packer()
                    .extend(projection.iter().map(|i| &datums[*i]));
            }

            Some(row_buf)
        } else {
            Some(row)
        }
    }
}

impl RowIterator for SortedRowCollectionIter {
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
        let row = Self::project(self.projection.as_ref(), row, datum_buf, row_buf)?;

        Some(row)
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
        let row = Self::project(self.projection.as_ref(), row, datum_buf, row_buf)?;

        Some(row)
    }

    fn count(&self) -> usize {
        self.collection.collection.count(self.offset, self.limit)
    }
}

impl IntoRowIterator for SortedRowCollection {
    type Iter = SortedRowCollectionIter;

    fn into_row_iter(self) -> Self::Iter {
        SortedRowCollectionIter {
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
    use proptest::prelude::*;
    use proptest::test_runner::Config;

    use super::*;
    use crate::Datum;

    #[mz_ore::test]
    fn test_row_collection() {
        let a = Row::pack_slice(&[Datum::False, Datum::String("hello world"), Datum::Int16(42)]);
        let b = Row::pack_slice(&[Datum::MzTimestamp(crate::Timestamp::new(10))]);

        let collection = RowCollection::from([&a, &b]);

        let (a_rnd, _) = collection.get(0).unwrap();
        assert_eq!(a_rnd, a.borrow());

        let (b_rnd, _) = collection.get(1).unwrap();
        assert_eq!(b_rnd, b.borrow());
    }

    #[mz_ore::test]
    fn test_merge() {
        let a = Row::pack_slice(&[Datum::False, Datum::String("hello world"), Datum::Int16(42)]);
        let b = Row::pack_slice(&[Datum::MzTimestamp(crate::Timestamp::new(10))]);

        let mut a_col = RowCollection::from([&a]);
        let b_col = RowCollection::from([&b]);

        a_col.merge(&b_col);

        assert_eq!(a_col.count(0, None), 2);
        assert_eq!(a_col.get(0).map(|(r, _)| r), Some(a.borrow()));
        assert_eq!(a_col.get(1).map(|(r, _)| r), Some(b.borrow()));
    }

    #[mz_ore::test]
    fn test_sort() {
        let a = Row::pack_slice(&[Datum::False, Datum::String("hello world"), Datum::Int16(42)]);
        let b = Row::pack_slice(&[Datum::MzTimestamp(crate::Timestamp::new(10))]);

        let col = RowCollection::from([&a, &b]);
        let mut rows = [a, b];

        let sorted_view = col.sorted_view(|a, b| a.cmp(b));
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
        let col = RowCollection::new(&[
            (a.clone(), NonZeroUsize::new(3).unwrap()),
            (b.clone(), NonZeroUsize::new(2).unwrap()),
        ]);
        let col = col.sorted_view(|a, b| a.cmp(b));
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
        let col = RowCollection::new(&[
            (a.clone(), NonZeroUsize::new(3).unwrap()),
            (b.clone(), NonZeroUsize::new(2).unwrap()),
        ]);
        let col = col.sorted_view(|a, b| a.cmp(b));

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
        let col = RowCollection::new(&[
            (a.clone(), NonZeroUsize::new(3).unwrap()),
            (b.clone(), NonZeroUsize::new(2).unwrap()),
        ]);
        let col = col.sorted_view(|a, b| a.cmp(b));

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
        let col = RowCollection::new(&[(a.clone(), NonZeroUsize::new(3).unwrap())]);
        let col = col.sorted_view(|a, b| a.cmp(b));

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
        let col = RowCollection::new(&[(a.clone(), NonZeroUsize::new(2).unwrap())]);
        let col = col.sorted_view(|a, b| a.cmp(b));

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
        let col = RowCollection::new(&[
            (a.clone(), NonZeroUsize::new(3).unwrap()),
            (b.clone(), NonZeroUsize::new(2).unwrap()),
        ]);
        let col = col.sorted_view(|a, b| a.cmp(b));

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
            Config { cases: 10, ..Default::default() },
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

            a_col.merge(&b_col);

            let all_rows = a.iter().chain(b.iter());
            for (idx, row) in all_rows.enumerate() {
                let (col_row, _) = a_col.get(idx).unwrap();
                assert_eq!(col_row, row.borrow());
            }
        }

        // This test is slow, so we limit the default number of test cases.
        proptest!(
            Config { cases: 5, ..Default::default() },
            |(a in any::<Vec<Row>>(), b in any::<Vec<Row>>())| {
                // The proptest! macro interferes with rustfmt.
                row_collection_merge(a, b)
            }
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn proptest_sort() {
        fn row_collection_sort(mut a: Vec<Row>) {
            let a_col = RowCollection::from(&a);

            let sorted_view = a_col.sorted_view(|a, b| a.cmp(b));
            a.sort_by(|a, b| a.cmp(b));

            for i in 0..a.len() {
                let (row_x, _) = sorted_view.get(i).unwrap();
                let row_y = a.get(i).unwrap();

                assert_eq!(row_x, row_y.borrow());
            }
        }

        // This test is slow, so we limit the default number of test cases.
        proptest!(
            Config { cases: 10, ..Default::default() },
            |(a in any::<Vec<Row>>())| {
                // The proptest! macro interferes with rustfmt.
                row_collection_sort(a)
            }
        );
    }
}
