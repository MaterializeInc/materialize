// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines types for working with collections of [`Row`].

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use mz_ore::cast::CastFrom;
use mz_proto::RustType;
use mz_repr::{DatumVec, IntoRowIterator, Row, RowIterator, RowRef};
use serde::{Deserialize, Serialize};

use crate::ColumnOrder;

include!(concat!(env!("OUT_DIR"), "/mz_expr.row.collection.rs"));

/// Collection of sorted [`Row`]s represented as a single blob.
///
/// Note: the encoding format we use to represent [`Row`]s in this struct is
/// not stable, and thus should never be persisted durably.
#[derive(Default, Debug, Clone, PartialEq)]
pub struct RowCollection {
    /// Contiguous blob of encoded Rows.
    encoded: Bytes,
    /// Metadata about an individual Row in the blob.
    metadata: Vec<EncodedRowMetadata>,
    /// End of non-empty, sorted runs of rows in number of rows.
    runs: Vec<usize>,
}

impl RowCollection {
    /// Create a new [`RowCollection`] from a collection of [`Row`]s. Sorts data by `order_by`.
    ///
    /// Note that all row collections must be constructed with the same `order_by` to ensure that
    /// the sort order is consistent. Anything else is undefined behavior.
    // TODO: Remember the `order_by` and assert that it is the same for all collections.
    pub fn new(mut rows: Vec<(Row, NonZeroUsize)>, order_by: &[ColumnOrder]) -> Self {
        // Sort data to maintain sortedness invariants.
        let (mut datum_vec1, mut datum_vec2) = (DatumVec::new(), DatumVec::new());
        rows.sort_by(|(row1, _diff1), (row2, _diff2)| {
            let borrow1 = datum_vec1.borrow_with(row1);
            let borrow2 = datum_vec2.borrow_with(row2);
            crate::compare_columns(order_by, &borrow1, &borrow2, || row1.cmp(row2))
        });

        // Pre-sizing our buffer should allow us to make just 1 allocation, and
        // use the perfect amount of memory.
        //
        // Note(parkmycar): I didn't do any benchmarking to determine if this
        // is faster, so feel free to change this if you'd like.
        let encoded_size = rows.iter().map(|(row, _diff)| row.data_len()).sum();

        let mut encoded = Vec::<u8>::with_capacity(encoded_size);
        let mut metadata = Vec::<EncodedRowMetadata>::with_capacity(rows.len());
        let runs = if rows.is_empty() {
            vec![]
        } else {
            vec![rows.len()]
        };

        for (row, diff) in rows {
            encoded.extend(row.data());
            metadata.push(EncodedRowMetadata {
                offset: encoded.len(),
                diff,
            });
        }

        RowCollection {
            encoded: Bytes::from(encoded),
            metadata,
            runs,
        }
    }

    /// Merge another [`RowCollection`] into `self`.
    pub fn merge(&mut self, other: &RowCollection) {
        if other.is_empty() {
            return;
        } else if self.is_empty() {
            *self = other.clone();
            return;
        }

        let self_len = self.metadata.len();

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
        self.runs.extend(other.runs.iter().map(|f| f + self_len));
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

    /// Returns true iff this collection is empty. Not `pub` because it doesn't take offset/limit
    /// into account.
    fn is_empty(&self) -> bool {
        self.metadata.is_empty()
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
    pub fn sorted_view<F>(self, cmp: F) -> SortedRowCollection
    where
        F: Fn(&RowRef, &RowRef) -> std::cmp::Ordering,
    {
        let mut heap = BinaryHeap::new();

        for index in 0..self.runs.len() {
            let start = (index == 0)
                .then_some(0)
                .unwrap_or_else(|| self.runs[index - 1]);
            let end = self.runs[index];

            heap.push(Reverse(RunIter {
                collection: &self,
                cmp: &cmp,
                start,
                end,
            }));
        }

        let mut view = Vec::with_capacity(self.metadata.len());

        while let Some(Reverse(mut run)) = heap.pop() {
            view.push(run.start);
            run.start += 1;
            if run.start < run.end {
                heap.push(Reverse(run));
            }
        }

        assert_eq!(view.len(), self.metadata.len());

        SortedRowCollection {
            collection: self,
            sorted_view: view.into(),
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
            runs: self.runs.iter().copied().map(u64::cast_from).collect(),
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
            runs: proto.runs.into_iter().map(usize::cast_from).collect(),
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

struct RunIter<'a, F> {
    collection: &'a RowCollection,
    cmp: &'a F,
    start: usize,
    end: usize,
}

impl<'a, F> PartialOrd for RunIter<'a, F>
where
    F: Fn(&RowRef, &RowRef) -> std::cmp::Ordering,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, F> Ord for RunIter<'a, F>
where
    F: Fn(&RowRef, &RowRef) -> std::cmp::Ordering,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.cmp)(
            self.collection.get(self.start).unwrap().0,
            self.collection.get(other.start).unwrap().0,
        )
    }
}

impl<'a, F> PartialEq for RunIter<'a, F>
where
    F: Fn(&RowRef, &RowRef) -> std::cmp::Ordering,
{
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other)
            .map(|ordering| ordering == std::cmp::Ordering::Equal)
            .unwrap_or(false)
    }
}

impl<'a, F> Eq for RunIter<'a, F> where F: Fn(&RowRef, &RowRef) -> std::cmp::Ordering {}

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
            let mut encoded = Vec::<u8>::new();
            let mut metadata = Vec::<EncodedRowMetadata>::new();

            for row in rows {
                encoded.extend(row.data());
                metadata.push(EncodedRowMetadata {
                    offset: encoded.len(),
                    diff: NonZeroUsize::MIN,
                });
            }
            let runs = vec![metadata.len()];

            RowCollection {
                encoded: Bytes::from(encoded),
                metadata,
                runs,
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

        a_col.merge(&b_col);

        assert_eq!(a_col.count(0, None), 2);
        assert_eq!(a_col.get(0).map(|(r, _)| r), Some(a.borrow()));
        assert_eq!(a_col.get(1).map(|(r, _)| r), Some(b.borrow()));
    }

    #[mz_ore::test]
    fn test_sort() {
        let a = Row::pack_slice(&[Datum::False, Datum::String("hello world"), Datum::Int16(42)]);
        let b = Row::pack_slice(&[Datum::MzTimestamp(mz_repr::Timestamp::new(10))]);
        let c = Row::pack_slice(&[Datum::True, Datum::String("hello world"), Datum::Int16(42)]);
        let d = Row::pack_slice(&[Datum::MzTimestamp(mz_repr::Timestamp::new(9))]);

        let col = {
            let mut part = [&a, &b];
            part.sort_by(|a, b| a.cmp(b));
            let mut part1 = RowCollection::from(part);
            let mut part = [&c, &d];
            part.sort_by(|a, b| a.cmp(b));
            let part2 = RowCollection::from(part);
            part1.merge(&part2);
            part1
        };
        let mut rows = [a, b, c, d];

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
        let mut col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(3).unwrap())], &[]);
        col.merge(&RowCollection::new(
            vec![(b.clone(), NonZeroUsize::new(2).unwrap())],
            &[],
        ));
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
        let mut col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(3).unwrap())], &[]);
        col.merge(&RowCollection::new(
            vec![(b.clone(), NonZeroUsize::new(2).unwrap())],
            &[],
        ));
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
        let mut col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(3).unwrap())], &[]);
        col.merge(&RowCollection::new(
            vec![(b.clone(), NonZeroUsize::new(2).unwrap())],
            &[],
        ));
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
        let col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(3).unwrap())], &[]);
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
        let col = RowCollection::new(vec![(a.clone(), NonZeroUsize::new(2).unwrap())], &[]);
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
        let col = RowCollection::new(
            vec![
                (a.clone(), NonZeroUsize::new(3).unwrap()),
                (b.clone(), NonZeroUsize::new(2).unwrap()),
            ],
            &[],
        );
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
        fn row_collection_sort(mut a: Vec<Row>, mut b: Vec<Row>) {
            a.sort_by(|a, b| a.cmp(b));
            b.sort_by(|a, b| a.cmp(b));
            let mut col = RowCollection::from(&a);
            col.merge(&RowCollection::from(&b));

            let sorted_view = col.sorted_view(|a, b| a.cmp(b));

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
            Config { cases: 10, ..Default::default() },
            |(a in any::<Vec<Row>>(), b in any::<Vec<Row>>())| {
                // The proptest! macro interferes with rustfmt.
                row_collection_sort(a, b)
            }
        );
    }
}
