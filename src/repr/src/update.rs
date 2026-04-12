// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::{Deref, Range};
use std::sync::Arc;
use crate::{Diff, RowRef};

/// An immutable, shared slice. Morally, this is [bytes::Bytes] but with fewer features
/// and supporting arbitrary types.
#[derive(Debug, Clone)]
pub struct SharedSlice<T> {
    /// The range of offsets in the backing data that are present in the slice.
    /// (This allows us to subset the slice without reallocating.)
    range: Range<usize>,
    data: Arc<[T]>,
}

impl<T> SharedSlice<T> {
    /// Split this slice in half at the provided offset.
    pub fn split_at(self, offset: usize) -> (Self, Self) {
        let Self { range, data } = self;
        assert!(offset <= range.len());
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
            run_start: 0,
            run_ends: self.run_ends.into(),
        }
    }
}

/// A packed representation of a set of rows.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rows {
    bytes: Bytes,
    /// After a split, the bytes above will only cover the range of bytes currently contained in this
    /// [Rows], but the shared slice ends contain offsets into the original un-split buffer.
    /// That means we need to track the offset that corresponds to the first byte in the bytes buffer,
    /// to offset the rest of the values with. That's what this value is!
    run_start: usize,
    run_ends: SharedSlice<usize>,
}

impl Rows {
    pub fn builder(byte_size_hint: usize, row_size_hint: usize) -> RowsBuilder {
        RowsBuilder {
            bytes: Vec::with_capacity(byte_size_hint),
            run_ends: Vec::with_capacity(row_size_hint),
        }
    }
    pub fn get(&self, index: usize) -> Option<&RowRef> {
        if index >= self.run_ends.len() {
            return None;
        }
        let lo = if index == 0 {
            0
        } else {
            self.run_ends[index - 1] - self.run_start
        };
        let hi = self.run_ends[index] - self.run_start;
        // SAFETY: endpoints and data taken from a pushed encoded run.
        Some(unsafe { RowRef::from_slice(&self.bytes[lo..hi]) })
    }

    pub fn iter(&self) -> impl Iterator<Item = &RowRef> {
        [0].into_iter()
            .chain(self.run_ends.iter().map(|i| *i - self.run_start))
            .tuple_windows()
            .map(|(lo, hi)| {
                // SAFETY: endpoints and data taken from a pushed encoded run.
                unsafe { RowRef::from_slice(&self.bytes[lo..hi]) }
            })
    }

    pub fn split_at(mut self, mid: usize) -> (Self, Self) {
        let (run_ends_a, run_ends_b) = self.run_ends.split_at(mid);
        let byte_mid = run_ends_a.last().map_or(0, |e| *e - self.run_start);
        let bytes_b = self.bytes.split_off(byte_mid);
        (
            Self {
                bytes: self.bytes,
                run_start: self.run_start,
                run_ends: run_ends_a,
            },
            Self {
                bytes: bytes_b,
                run_start: self.run_start + byte_mid,
                run_ends: run_ends_b,
            },
        )
    }

    pub fn len(&self) -> usize {
        self.run_ends.len()
    }

    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }
}

#[derive(Debug)]
pub struct UpdateCollectionBuilder<T = crate::Timestamp> {
    rows: RowsBuilder,
    times: Vec<T>,
    diffs: Vec<Diff>,
}

impl<T: Clone> UpdateCollectionBuilder<T> {
    pub fn push(&mut self, (row, time, diff): (&RowRef, &T, Diff)) {
        self.rows.push(row);
        self.times.push(time.clone());
        self.diffs.push(diff);
    }

    pub fn build(self) -> UpdateCollection<T> {
        UpdateCollection {
            rows: self.rows.build(),
            times: self.times.into(),
            diffs: self.diffs.into(),
        }
    }
}

/// A collection of row-time-diff updates in a columnar format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCollection<T = crate::Timestamp> {
    rows: Rows,
    times: SharedSlice<T>,
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

impl<'a, T: Clone> FromIterator<(&'a RowRef, &'a T, Diff)> for UpdateCollection<T> {
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
            times: Vec::with_capacity(row_size_hint),
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

    pub fn times(&self) -> &[T] {
        &*self.times
    }

    pub fn byte_len(&self) -> usize {
        self.rows.byte_len()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::array::uniform;
    use proptest::collection::vec;
    use proptest::prelude::*;

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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_rows_splits() {
        proptest!(|(data in vec(any::<crate::Row>(), 0..8), [a, b] in uniform(0usize..8))| {
            let mut rows = Rows::builder(0, 0);
            for row in &data {
                rows.push(row.as_row_ref());
            }
            let rows = rows.build();

            let mid = a.clamp(0, data.len());
            let data = data.split_at(mid);
            let rows = rows.split_at(mid);
            assert!(data.0.iter().map(|r| r.as_row_ref()).eq(rows.0.iter()));
            assert_eq!(rows.0.len(), mid);

            let mid = b.clamp(0, data.0.len());
            let data_0 = data.0.split_at(mid);
            let rows_0 = rows.0.split_at(mid);
            assert!(data_0.0.iter().map(|r| r.as_row_ref()).eq(rows_0.0.iter()));
            assert_eq!(rows_0.0.len(), mid);
            // assert_eq!(rows_0.0.len() + rows_0.1.len(), rows.0.len());

            let mid = b.clamp(0, data.1.len());
            let data_1 = data.1.split_at(mid);
            let rows_1 = rows.1.split_at(mid);
            assert!(data_1.0.iter().map(|r| r.as_row_ref()).eq(rows_1.0.iter()));
            assert_eq!(rows_1.0.len(), mid);
        });
    }
}
