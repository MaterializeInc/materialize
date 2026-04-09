// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{Diff, RowRef};
use bytes::Bytes;
use itertools::Itertools;
use mz_ore::vec::PartialOrdVecExt;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Debug;
use std::ops::{Deref, Range};
use std::sync::Arc;
use timely::progress::Timestamp;

/// An immutable, shared slice. Morally, this is [bytes::Bytes] but with fewer features
/// and supporting arbitrary types.
pub struct SharedSlice<T> {
    /// The range of offsets in the backing data that are present in the slice.
    /// (This allows us to subset the slice without reallocating.)
    range: Range<usize>,
    data: Arc<[T]>,
}

impl<T: Debug> Debug for SharedSlice<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedSlice").field(&&self[..]).finish()
    }
}

impl<T> Clone for SharedSlice<T> {
    fn clone(&self) -> Self {
        Self {
            range: self.range.clone(),
            data: Arc::clone(&self.data),
        }
    }
}

impl<T> SharedSlice<T> {
    /// Split this slice in half at the provided offset.
    pub fn split_at(self, offset: usize) -> (Self, Self) {
        let Self { range, data } = self;
        assert!(
            offset <= range.len(),
            "offset out of bounds: {offset} !<= {len}",
            len = range.len()
        );
        let offset = range.start + offset;
        (
            Self {
                range: range.start..offset,
                data: Arc::clone(&data),
            },
            Self {
                range: offset..range.end,
                data,
            },
        )
    }
}

impl<T> Deref for SharedSlice<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.data[self.range.clone()]
    }
}

impl<T: Serialize> Serialize for SharedSlice<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (**self).serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SharedSlice<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<T> = Deserialize::deserialize(deserializer)?;
        Ok(vec.into())
    }
}

impl<T: PartialEq> PartialEq for SharedSlice<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<T: Eq> Eq for SharedSlice<T> {}

impl<T> Default for SharedSlice<T> {
    fn default() -> Self {
        vec![].into()
    }
}

impl<T> From<Vec<T>> for SharedSlice<T> {
    fn from(data: Vec<T>) -> Self {
        Self {
            range: 0..data.len(),
            data: data.into(),
        }
    }
}

/// A shared slice of increasing offsets, used to track runs of data in some other sequence.
///
/// One hassle with using a raw [SharedSlice] for offsets is that when the
/// slice is split:
/// - if the split point is in the middle of a run, we have a new beginning and end offset that
///   aren't present in the slice;
/// - the offsets are now relative to a new start value, not 0.
/// This struct bottles up the necessary math for this.
#[derive(Clone, Debug)]
struct Offsets {
    /// The range of offsets in the backing data that are present in the slice.
    /// (This allows us to subset the slice without reallocating.)
    range: Range<usize>,
    data: SharedSlice<usize>,
}

impl Offsets {
    pub fn new(data: Vec<usize>) -> Self {
        debug_assert!(data.is_sorted());
        Self {
            range: 0..data.last().copied().unwrap_or(0),
            data: data.into(),
        }
    }

    fn adjust_end(&self, data_offset: usize) -> usize {
        self.range.end.min(data_offset) - self.range.start
    }

    /// The number of runs represented by these offsets.
    fn run_len(&self) -> usize {
        self.data.len()
    }

    /// The total range of offsets covered by this instance; ie. the upper bound of the last run.
    pub fn offset_len(&self) -> usize {
        self.range.len()
    }

    /// Given the index of a particular run, return the end of that run.
    fn run_end(&self, run_index: usize) -> Option<usize> {
        let end_offset = *self.data.get(run_index)?;
        Some(self.adjust_end(end_offset))
    }

    /// Given the index of a particular run, return the start of that run.
    fn run_start(&self, run_index: usize) -> Option<usize> {
        if run_index == 0 {
            return Some(0);
        }
        self.run_end(run_index - 1)
    }

    fn run_for_offset(&self, offset: usize) -> usize {
        let offset = self.range.start + offset;
        // If we're past the end of the data, return one past the index of the last run.
        if offset >= self.range.end {
            return self.data.len();
        }
        // Otherwise, return the index of the first run that does not end before our offset.
        self.data.partition_point(|i| *i <= offset)
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> {
        self.data.iter().copied().map(|i| self.adjust_end(i))
    }

    pub fn split_at_run(self, index: usize) -> (Self, Self) {
        let (before, after) = self.data.split_at(index);
        let split_at_offset = before
            .last()
            .copied()
            .unwrap_or(self.range.start)
            .min(self.range.end);
        (
            Self {
                range: self.range.start..split_at_offset,
                data: before,
            },
            Self {
                range: split_at_offset..self.range.end,
                data: after,
            },
        )
    }

    /// Split this slice in half at the provided offset.
    pub fn split_at_offset(self, offset: usize) -> (Self, Self) {
        assert!(
            offset <= self.offset_len(),
            "index out of bounds: {offset} > {len}",
            len = self.offset_len()
        );
        let offset = self.range.start + offset;
        let run_index = if offset == self.range.start {
            Ok(0)
        } else if offset == self.range.end {
            Ok(self.data.len())
        } else {
            self.data.binary_search(&offset).map(|n| n + 1)
        };
        let (before_data, after_data) = match run_index {
            Ok(endpoint) => {
                // Great news: we're splitting at one of the existing offsets.
                self.data.split_at(endpoint)
            }
            Err(midpoint) => {
                // Less good news: we're splitting between offsets.
                let after_split = midpoint;
                let before_split = midpoint + 1;
                let (before_data, _) = self.data.clone().split_at(before_split);
                let (_, after_data) = self.data.split_at(after_split);
                (before_data, after_data)
            }
        };
        let before = Self {
            data: before_data,
            range: self.range.start..offset,
        };
        let after = Self {
            data: after_data,
            range: offset..self.range.end,
        };
        (before, after)
    }
}

impl Serialize for Offsets {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for Offsets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<usize> = Deserialize::deserialize(deserializer)?;
        if vec.is_sorted() {
            Ok(Offsets::new(vec))
        } else {
            Err(D::Error::custom("offsets out of order"))
        }
    }
}

impl PartialEq for Offsets {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for Offsets {}

impl Default for Offsets {
    fn default() -> Self {
        Self {
            range: 0..0,
            data: Default::default(),
        }
    }
}

/// See [Rows].
#[derive(Debug)]
pub struct RowsBuilder {
    bytes: Vec<u8>,
    run_ends: Vec<usize>,
}

impl RowsBuilder {
    pub fn push(&mut self, row: &RowRef) {
        self.bytes.extend(row.data());
        self.run_ends.push(self.bytes.len());
    }

    pub fn build(self) -> Rows {
        Rows {
            bytes: self.bytes.into(),
            run_ends: Offsets::new(self.run_ends),
        }
    }
}

/// A packed representation of a set of rows.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rows {
    bytes: Bytes,
    run_ends: Offsets,
}

impl Rows {
    pub fn builder(byte_size_hint: usize, row_size_hint: usize) -> RowsBuilder {
        RowsBuilder {
            bytes: Vec::with_capacity(byte_size_hint),
            run_ends: Vec::with_capacity(row_size_hint),
        }
    }
    pub fn get(&self, index: usize) -> Option<&RowRef> {
        let lo = self.run_ends.run_start(index)?;
        let hi = self.run_ends.run_end(index)?;
        // SAFETY: endpoints and data taken from a pushed encoded run.
        Some(unsafe { RowRef::from_slice(&self.bytes[lo..hi]) })
    }

    pub fn iter(&self) -> impl Iterator<Item = &RowRef> {
        [0].into_iter()
            .chain(self.run_ends.iter())
            .tuple_windows()
            .map(|(lo, hi)| {
                // SAFETY: endpoints and data taken from a pushed encoded run.
                unsafe { RowRef::from_slice(&self.bytes[lo..hi]) }
            })
    }

    pub fn split_at(mut self, mid: usize) -> (Self, Self) {
        let (run_ends_a, run_ends_b) = self.run_ends.split_at_run(mid);
        let byte_mid = run_ends_a.offset_len();
        let bytes_b = self.bytes.split_off(byte_mid);
        (
            Self {
                bytes: self.bytes,
                run_ends: run_ends_a,
            },
            Self {
                bytes: bytes_b,
                run_ends: run_ends_b,
            },
        )
    }

    pub fn len(&self) -> usize {
        self.run_ends.run_len()
    }

    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEncoded<T> {
    /// Actual data, one per run.
    data: SharedSlice<T>,
    /// Endpoints of individual runs of data. Should be equal length with the data itself.
    run_ends: Offsets,
}

impl<T> Default for RunEncoded<T> {
    fn default() -> Self {
        Self {
            data: Default::default(),
            run_ends: Default::default(),
        }
    }
}

#[derive(Debug)]
struct RunEncodedBuilder<T> {
    data: Vec<T>,
    run_ends: Vec<usize>,
}

impl<T: PartialEq> RunEncodedBuilder<T> {
    fn len(&self) -> usize {
        self.run_ends.last().copied().unwrap_or(0)
    }

    fn push(&mut self, value: T) {
        if self.data.last().is_some_and(|t| *t == value) {
            *self.run_ends.last_mut().expect("same length") += 1
        } else {
            self.data.push(value);
            self.run_ends.push(self.len() + 1);
        }
    }

    fn build(self) -> RunEncoded<T> {
        RunEncoded {
            data: self.data.into(),
            run_ends: Offsets::new(self.run_ends),
        }
    }
}

impl<T> RunEncoded<T> {
    fn builder() -> RunEncodedBuilder<T> {
        RunEncodedBuilder {
            data: Vec::new(),
            run_ends: Vec::new(),
        }
    }

    /// Similar to [slice::partition_point], but searching over the run ends directly.
    pub fn partition_point(&self, partition_by: impl Fn(&T) -> bool) -> usize {
        let index = self.data.partition_point(partition_by);
        self.run_ends.run_start(index).expect("index out of bounds")
    }

    pub fn len(&self) -> usize {
        self.run_ends.offset_len()
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        let index = self.run_ends.run_for_offset(index);
        self.data.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        [0].into_iter()
            .chain(self.run_ends.iter())
            .tuple_windows()
            .enumerate()
            .flat_map(|(idx, (lo, hi))| {
                let value = &self.data[idx];
                (lo..hi).map(move |_| value)
            })
    }

    pub fn split_at(self, index: usize) -> (Self, Self) {
        assert!(
            index <= self.len(),
            "index out of bounds: {index} > {len}",
            len = self.len()
        );

        // Note: it's possible that the "middle" run might be split into two runs.
        let len = self.run_ends.run_len();
        let (before_ends, after_ends) = self.run_ends.split_at_offset(index);
        let (before_data, _) = self.data.clone().split_at(before_ends.run_len());
        let (_, after_data) = self.data.clone().split_at(len - after_ends.run_len());
        let before = Self {
            data: before_data,
            run_ends: before_ends,
        };
        let after = Self {
            data: after_data,
            run_ends: after_ends,
        };
        (before, after)
    }
}

#[derive(Debug)]
pub struct UpdateCollectionBuilder<T = crate::Timestamp> {
    rows: RowsBuilder,
    times: RunEncodedBuilder<T>,
    diffs: Vec<Diff>,
}

impl<T: Timestamp> UpdateCollectionBuilder<T> {
    pub fn push(&mut self, (row, time, diff): (&RowRef, &T, Diff)) {
        self.rows.push(row);
        self.times.push(time.clone());
        self.diffs.push(diff);
    }

    pub fn build(self) -> UpdateCollection<T> {
        UpdateCollection {
            rows: self.rows.build(),
            times: self.times.build(),
            diffs: self.diffs.into(),
        }
    }
}

/// A collection of row-time-diff updates in a columnar format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCollection<T = crate::Timestamp> {
    rows: Rows,
    times: RunEncoded<T>,
    diffs: SharedSlice<Diff>,
}

impl<T> Default for UpdateCollection<T> {
    fn default() -> Self {
        Self {
            rows: Default::default(),
            times: Default::default(),
            diffs: Default::default(),
        }
    }
}

impl<'a, T: Timestamp> FromIterator<(&'a RowRef, &'a T, Diff)> for UpdateCollection<T> {
    fn from_iter<I: IntoIterator<Item = (&'a RowRef, &'a T, Diff)>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let len_hint = iter.size_hint().0;
        let bytes_hint = len_hint * 8;
        let mut builder = UpdateCollection::builder(bytes_hint, len_hint);
        for row in iter {
            builder.push(row);
        }
        builder.build()
    }
}

impl<T> UpdateCollection<T> {
    pub fn builder(byte_size_hint: usize, row_size_hint: usize) -> UpdateCollectionBuilder<T> {
        UpdateCollectionBuilder {
            rows: Rows::builder(byte_size_hint, row_size_hint),
            times: RunEncoded::builder(),
            diffs: Vec::with_capacity(row_size_hint),
        }
    }

    pub fn get(&self, index: usize) -> Option<(&RowRef, &T, Diff)> {
        Some((
            self.rows.get(index)?,
            self.times.get(index)?,
            *self.diffs.get(index)?,
        ))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RowRef, &T, Diff)> {
        itertools::multizip((
            self.rows.iter(),
            self.times.iter(),
            self.diffs.iter().cloned(),
        ))
    }

    pub fn split_at(self, index: usize) -> (Self, Self) {
        let (rows_a, rows_b) = self.rows.split_at(index);
        let (times_a, times_b) = self.times.split_at(index);
        let (diffs_a, diffs_b) = self.diffs.split_at(index);
        (
            Self {
                rows: rows_a,
                times: times_a,
                diffs: diffs_a,
            },
            Self {
                rows: rows_b,
                times: times_b,
                diffs: diffs_b,
            },
        )
    }

    pub fn rows(&self) -> &Rows {
        &self.rows
    }

    pub fn times(&self) -> &RunEncoded<T> {
        &self.times
    }

    pub fn diffs(&self) -> &[Diff] {
        &self.diffs
    }

    pub fn count(&self) -> Result<usize, std::num::TryFromIntError> {
        let mut sum = 0usize;
        for diff in self.diffs.iter() {
            sum += usize::try_from(diff.into_inner())?;
        }
        Ok(sum)
    }

    pub fn byte_len(&self) -> usize {
        // Count both the bytes in the byte array and the size of the offsets themselves.
        let row_data_size = self
            .rows
            .byte_len()
            .saturating_add(self.rows.len().saturating_mul(size_of::<usize>()));
        let time_size = self.times.data.len().saturating_mul(size_of::<T>());
        let diff_size = self.diffs.len().saturating_mul(size_of::<Diff>());
        row_data_size
            .saturating_add(time_size)
            .saturating_add(diff_size)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Row;
    use proptest::array::uniform;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::strategy::Union;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_slice_splits() {
        proptest!(|(data in vec(0u64..1000u64, 0..20), [a, b] in uniform(0usize..20))| {
            let sliceable: SharedSlice<u64> = data.clone().into();
            let mid = a.clamp(0, data.len());
            let data = data.split_at(mid);
            let sliceable = sliceable.split_at(mid);
            assert_eq!(data.0, &*sliceable.0);
            assert_eq!(data.1, &*sliceable.1);
            let mid = b.clamp(0, data.0.len());
            let data = data.0.split_at(mid);
            let sliceable = sliceable.0.split_at(mid);
            assert_eq!(data.0, &*sliceable.0);
            assert_eq!(data.1, &*sliceable.1);
        });
    }

    #[derive(Debug, Clone)]
    enum Op {
        SplitAt { offset: usize, keep_first: bool },
        Reserialize,
    }

    fn op_gen() -> impl Strategy<Value = Op> {
        Union::new([
            Just(Op::Reserialize).boxed(),
            (0usize..10, any::<bool>())
                .prop_map(|(offset, keep_first)| Op::SplitAt { keep_first, offset })
                .boxed(),
        ])
    }
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_row_ops() {
        fn check(mut data: Vec<Row>, ops: Vec<Op>) {
            let mut rows = Rows::builder(0, 0);
            for row in &data {
                rows.push(row.as_row_ref());
            }
            let mut rows = rows.build();
            let mut history = vec![rows.clone()];

            for op in ops {
                match op {
                    Op::SplitAt { keep_first, offset } => {
                        let offset = offset.clamp(0, rows.len());
                        let (rows_0, rows_1) = rows.split_at(offset);
                        let (data_0, data_1) = data.split_at(offset);
                        if keep_first {
                            data = data_0.to_vec();
                            rows = rows_0;
                        } else {
                            data = data_1.to_vec();
                            rows = rows_1;
                        }
                    }
                    Op::Reserialize => {
                        let encoded = bincode::serialize(&rows).unwrap();
                        rows = bincode::deserialize(&encoded).unwrap();
                    }
                }

                history.push(rows.clone());

                assert_eq!(data.len(), rows.len());
                assert!(
                    data.iter().map(|r| r.as_row_ref()).eq(rows.iter()),
                    "{history:?}"
                );
                for i in 0..=data.len() {
                    assert_eq!(data.get(i).map(|r| r.as_row_ref()), rows.get(i));
                }

                assert!(rows.run_ends.iter().is_sorted());
                assert_eq!(rows.bytes.len(), rows.run_ends.offset_len(), "{history:?}");
            }
        }

        proptest!(|(data in vec(any::<crate::Row>(), 0..8), ops in vec(op_gen(), 0usize..8))| {
            check(data, ops)
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_run_ops() {
        fn check(mut data: Vec<u64>, ops: Vec<Op>) {
            data.sort();
            let mut builder = RunEncoded::builder();
            for value in &data {
                builder.push(*value);
            }
            let mut runs = builder.build();

            let mut history = vec![runs.clone()];

            for op in ops {
                match op {
                    Op::SplitAt { keep_first, offset } => {
                        let offset = offset.clamp(0, runs.len());
                        let (runs_0, runs_1) = runs.split_at(offset);
                        let (data_0, data_1) = data.split_at(offset);
                        if keep_first {
                            data = data_0.to_vec();
                            runs = runs_0;
                        } else {
                            data = data_1.to_vec();
                            runs = runs_1;
                        }
                    }
                    Op::Reserialize => {
                        let encoded = bincode::serialize(&runs).unwrap();
                        runs = bincode::deserialize(&encoded).unwrap();
                    }
                }

                history.push(runs.clone());

                assert_eq!(data.len(), runs.len());
                assert!(data.iter().eq(runs.iter()), "{history:?}");
                for i in 0..=data.len() {
                    assert_eq!(data.get(i), runs.get(i));
                }
                for needle in 0u64..16 {
                    assert_eq!(
                        data.partition_point(|x| *x < needle),
                        runs.partition_point(|x| *x < needle),
                    );
                }

                assert!(runs.run_ends.iter().is_sorted());
                assert_eq!(runs.data.len(), runs.run_ends.run_len());
            }
        }

        proptest!(|(data in vec(0u64..16, 0..8), ops in vec(op_gen(), 0usize..8))| {
            check(data, ops)
        });
    }
}
