// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A columnar representation of ((Key, Val), Time, Diff) data suitable for in-memory
//! reads and persistent storage.

use std::fmt;
use std::iter::FromIterator;

use serde::{Deserialize, Serialize};

/// A set of ((Key, Val), Time, Diff) records stored in a columnar representation.
///
/// Note that the data are unsorted, and unconsolidated (so there may be multiple
/// instances of the same ((Key, Val), Time), and some Diffs might be zero, or
/// add up to zero).
#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColumnarRecords {
    len: usize,
    key_data: Vec<u8>,
    key_offsets: Vec<usize>,
    val_data: Vec<u8>,
    val_offsets: Vec<usize>,
    timestamps: Vec<u64>,
    diffs: Vec<isize>,
}

impl fmt::Debug for ColumnarRecords {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.borrow(), fmt)
    }
}

impl ColumnarRecords {
    /// The number of (potentially duplicated) ((Key, Val), Time, Diff) records
    /// stored in Self.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Borrow Self as a [ColumnarRecordsRef].
    fn borrow<'a>(&'a self) -> ColumnarRecordsRef<'a> {
        ColumnarRecordsRef {
            len: self.len,
            key_data: self.key_data.as_slice(),
            key_offsets: self.key_offsets.as_slice(),
            val_data: self.val_data.as_slice(),
            val_offsets: self.val_offsets.as_slice(),
            timestamps: self.timestamps.as_slice(),
            diffs: self.diffs.as_slice(),
        }
    }

    /// Convert an instance of Self into a [ColumnarRecordsBuilder] by reusing
    /// and clearing out the underlying memory.
    pub fn into_builder(mut self) -> ColumnarRecordsBuilder {
        self.len = 0;
        self.key_data.clear();
        self.key_offsets.clear();
        self.val_data.clear();
        self.val_offsets.clear();
        self.timestamps.clear();
        self.diffs.clear();
        ColumnarRecordsBuilder { records: self }
    }

    /// Iterate through the records in Self.
    pub fn iter<'a>(&'a self) -> ColumnarRecordsIter<'a> {
        self.borrow().iter()
    }
}

// TODO: deduplicate this with the other FromIterator implementation.
impl<'a, K, V> FromIterator<&'a ((K, V), u64, isize)> for ColumnarRecords
where
    K: AsRef<[u8]> + 'a,
    V: AsRef<[u8]> + 'a,
{
    fn from_iter<T: IntoIterator<Item = &'a ((K, V), u64, isize)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint();

        let mut builder = ColumnarRecordsBuilder::default();
        for record in iter.into_iter() {
            let ((key, val), ts, diff) = record;
            let (key, val) = (key.as_ref(), val.as_ref());
            if builder.records.len() == 0 {
                // Use the first record to attempt to pre-size the builder
                // allocations. This uses the iter's size_hint's lower+1 to
                // match the logic in Vec.
                let (lower, _) = size_hint;
                let additional = usize::saturating_add(lower, 1);
                builder.reserve(additional, key.len(), val.len());
            }
            builder.push(((key, val), *ts, *diff));
        }
        builder.finish()
    }
}

impl<K, V> FromIterator<((K, V), u64, isize)> for ColumnarRecords
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    fn from_iter<T: IntoIterator<Item = ((K, V), u64, isize)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint();

        let mut builder = ColumnarRecordsBuilder::default();
        for record in iter.into_iter() {
            let ((key, val), ts, diff) = record;
            let (key, val) = (key.as_ref(), val.as_ref());
            if builder.records.len() == 0 {
                // Use the first record to attempt to pre-size the builder
                // allocations. This uses the iter's size_hint's lower+1 to
                // match the logic in Vec.
                let (lower, _) = size_hint;
                let additional = usize::saturating_add(lower, 1);
                builder.reserve(additional, key.len(), val.len());
            }
            builder.push(((key, val), ts, diff));
        }
        builder.finish()
    }
}

/// A reference to a [ColumnarRecords].
#[derive(Clone)]
struct ColumnarRecordsRef<'a> {
    len: usize,
    key_data: &'a [u8],
    key_offsets: &'a [usize],
    val_data: &'a [u8],
    val_offsets: &'a [usize],
    timestamps: &'a [u64],
    diffs: &'a [isize],
}

impl<'a> fmt::Debug for ColumnarRecordsRef<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> ColumnarRecordsRef<'a> {
    /// Read the record at `idx`, if there is one.
    ///
    /// Returns None if `idx >= self.len()`.
    fn get(&self, idx: usize) -> Option<((&'a [u8], &'a [u8]), u64, isize)> {
        if idx >= self.len {
            return None;
        }
        debug_assert_eq!(self.key_offsets.len(), self.len + 1);
        debug_assert_eq!(self.val_offsets.len(), self.len + 1);
        debug_assert_eq!(self.timestamps.len(), self.len);
        debug_assert_eq!(self.diffs.len(), self.len);
        let key = &self.key_data[self.key_offsets[idx]..self.key_offsets[idx + 1]];
        let val = &self.val_data[self.val_offsets[idx]..self.val_offsets[idx + 1]];
        let ts = self.timestamps[idx];
        let diff = self.diffs[idx];
        Some(((key, val), ts, diff))
    }

    /// Iterate through the records in Self.
    fn iter(&self) -> ColumnarRecordsIter<'a> {
        ColumnarRecordsIter {
            idx: 0,
            records: self.clone(),
        }
    }
}

/// An [Iterator] over the records in a [ColumnarRecords].
#[derive(Clone, Debug)]
pub struct ColumnarRecordsIter<'a> {
    idx: usize,
    records: ColumnarRecordsRef<'a>,
}

impl<'a> Iterator for ColumnarRecordsIter<'a> {
    type Item = ((&'a [u8], &'a [u8]), u64, isize);

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.records.len, Some(self.records.len))
    }

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.records.get(self.idx);
        self.idx += 1;
        ret
    }
}

impl<'a> ExactSizeIterator for ColumnarRecordsIter<'a> {}

/// An abstraction to incrementally add ((Key, Value), Time, Diff) records
/// in a columnar representation, and eventually get back a [ColumnarRecords].
#[derive(Debug, Default)]
pub struct ColumnarRecordsBuilder {
    records: ColumnarRecords,
}

impl ColumnarRecordsBuilder {
    /// The number of (potentially duplicated) ((Key, Val), Time, Diff) records
    /// stored in Self.
    pub fn len(&self) -> usize {
        self.records.len
    }

    /// Reserve space for `additional` more records, based on `key_size_guess` and
    /// `val_size_guess`.
    ///
    /// The guesses for key and val sizes are best effort, and if they end up being
    /// too small, the underlying buffers will be resized.
    pub fn reserve(&mut self, additional: usize, key_size_guess: usize, val_size_guess: usize) {
        self.records.key_offsets.reserve(additional);
        self.records.key_data.reserve(additional * key_size_guess);
        self.records.val_offsets.reserve(additional);
        self.records.val_data.reserve(additional * val_size_guess);
        self.records.timestamps.reserve(additional);
        self.records.diffs.reserve(additional);
    }

    /// Add a record to Self.
    pub fn push(&mut self, record: ((&[u8], &[u8]), u64, isize)) {
        let ((key, val), ts, diff) = record;
        self.records.key_offsets.push(self.records.key_data.len());
        self.records.key_data.extend_from_slice(key);
        self.records.val_offsets.push(self.records.val_data.len());
        self.records.val_data.extend_from_slice(val);
        self.records.timestamps.push(ts);
        self.records.diffs.push(diff);
        self.records.len += 1;
        // The checks on `key_offsets` and `val_offsets` are different than in the
        // rest of the file because those arrays have not received their final
        // value yet in finish().
        debug_assert_eq!(self.records.key_offsets.len(), self.records.len);
        debug_assert_eq!(self.records.val_offsets.len(), self.records.len);
        debug_assert_eq!(self.records.timestamps.len(), self.records.len);
        debug_assert_eq!(self.records.diffs.len(), self.records.len);
    }

    /// Finalize constructing a [ColumnarRecords].
    pub fn finish(mut self) -> ColumnarRecords {
        self.records.key_offsets.push(self.records.key_data.len());
        self.records.val_offsets.push(self.records.val_data.len());
        debug_assert_eq!(self.records.key_offsets.len(), self.records.len + 1);
        debug_assert_eq!(self.records.val_offsets.len(), self.records.len + 1);
        debug_assert_eq!(self.records.timestamps.len(), self.records.len);
        debug_assert_eq!(self.records.diffs.len(), self.records.len);
        self.records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test some edge cases around empty sets of records and empty keys/vals
    ///
    /// Most of this functionality is also well-exercised in other unit tests as well.
    #[test]
    fn columnar_records() {
        let builder = ColumnarRecordsBuilder::default();

        // Empty builder.
        let records = builder.finish();
        let reads: Vec<_> = records.iter().collect();
        assert_eq!(reads, vec![]);

        // Empty key and val.
        let updates: Vec<((Vec<u8>, Vec<u8>), _, _)> = vec![
            (("".into(), "".into()), 0, 0),
            (("".into(), "".into()), 1, 1),
        ];
        let mut builder = ColumnarRecordsBuilder::default();
        for ((key, val), time, diff) in updates.iter() {
            builder.push(((key, val), *time, *diff));
        }

        let records = builder.finish();
        let reads: Vec<_> = records
            .iter()
            .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), t, d))
            .collect();
        assert_eq!(reads, updates);

        // Recycling a [ColumnarRecords] into a [ColumnarRecordsBuilder]
        let builder = records.into_builder();
        let records = builder.finish();
        let reads: Vec<_> = records.iter().collect();
        assert_eq!(reads, vec![]);
    }
}
