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

use arrow2::buffer::{Buffer, MutableBuffer};
use arrow2::types::Index;
use ore::cast::CastFrom;

pub mod arrow;
pub mod parquet;

/// A set of ((Key, Val), Time, Diff) records stored in a columnar
/// representation.
///
/// Note that the data are unsorted, and unconsolidated (so there may be
/// multiple instances of the same ((Key, Val), Time), and some Diffs might be
/// zero, or add up to zero).
///
/// The i'th key's data is stored in
/// `key_data[key_offsets[i]..key_offsets[i+1]]`. Similarly for val.
///
/// Invariants:
/// - key_offsets.len() == len + 1
/// - key_offsets are non-decreasing
/// - Each key_offset is <= key_data.len()
/// - key_offsets.first().unwrap() == 0
/// - key_offsets.last().unwrap() == key_data.len()
/// - val_offsets.len() == len + 1
/// - val_offsets are non-decreasing
/// - Each val_offset is <= val_data.len()
/// - val_offsets.first().unwrap() == 0
/// - val_offsets.last().unwrap() == val_data.len()
/// - timestamps.len() == len
/// - diffs.len() == len
///
/// NB: The `first().unwrap() == 0` could be `>= 0` if necessary. Ditto for
/// `last().unwrap()` and `<= key_data.len()`.
#[derive(Clone, PartialEq)]
pub struct ColumnarRecords {
    len: usize,
    key_data: Buffer<u8>,
    key_offsets: Buffer<i32>,
    val_data: Buffer<u8>,
    val_offsets: Buffer<i32>,
    timestamps: Buffer<u64>,
    diffs: Buffer<i64>,
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
            if builder.len() == 0 {
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
            if builder.len() == 0 {
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
    key_offsets: &'a [i32],
    val_data: &'a [u8],
    val_offsets: &'a [i32],
    timestamps: &'a [u64],
    diffs: &'a [i64],
}

impl<'a> fmt::Debug for ColumnarRecordsRef<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> ColumnarRecordsRef<'a> {
    fn validate(&self) -> Result<(), String> {
        if self.key_offsets.len() != self.len + 1 {
            return Err(format!(
                "expected {} key_offsets got {}",
                self.len + 1,
                self.key_offsets.len()
            ));
        }
        if let Some(first_key_offset) = self.key_offsets.first() {
            if first_key_offset.to_usize() != 0 {
                return Err(format!(
                    "expected first key offset to be 0 got {}",
                    first_key_offset.to_usize()
                ));
            }
        }
        if let Some(last_key_offset) = self.key_offsets.last() {
            if last_key_offset.to_usize() != self.key_data.len() {
                return Err(format!(
                    "expected {} bytes of key data got {}",
                    last_key_offset,
                    self.key_data.len()
                ));
            }
        }
        if self.val_offsets.len() != self.len + 1 {
            return Err(format!(
                "expected {} val_offsets got {}",
                self.len + 1,
                self.val_offsets.len()
            ));
        }
        if let Some(first_val_offset) = self.val_offsets.first() {
            if first_val_offset.to_usize() != 0 {
                return Err(format!(
                    "expected first val offset to be 0 got {}",
                    first_val_offset.to_usize()
                ));
            }
        }
        if let Some(last_val_offset) = self.val_offsets.last() {
            if last_val_offset.to_usize() != self.val_data.len() {
                return Err(format!(
                    "expected {} bytes of val data got {}",
                    last_val_offset,
                    self.val_data.len()
                ));
            }
        }
        if self.diffs.len() != self.len {
            return Err(format!(
                "expected {} diffs got {}",
                self.len,
                self.diffs.len()
            ));
        }
        if self.timestamps.len() != self.len {
            return Err(format!(
                "expected {} timestamps got {}",
                self.len,
                self.timestamps.len()
            ));
        }

        // Unlike most of our Validate methods, this one is called in a
        // production code path: when decoding a columnar batch. Only check the
        // more expensive assertions in debug.
        #[cfg(debug_assertions)]
        {
            let (mut prev_key, mut prev_val) = (0, 0);
            for i in 0..=self.len {
                let (key, val) = (self.key_offsets[i], self.val_offsets[i]);
                if key < prev_key {
                    return Err(format!(
                        "expected non-decreasing key offsets got {} followed by {}",
                        prev_key, key
                    ));
                }
                if val < prev_val {
                    return Err(format!(
                        "expected non-decreasing val offsets got {} followed by {}",
                        prev_val, val
                    ));
                }
                prev_key = key;
                prev_val = val;
            }
        }

        Ok(())
    }

    /// Read the record at `idx`, if there is one.
    ///
    /// Returns None if `idx >= self.len()`.
    fn get(&self, idx: usize) -> Option<((&'a [u8], &'a [u8]), u64, isize)> {
        if idx >= self.len {
            return None;
        }
        debug_assert_eq!(self.validate(), Ok(()));
        let key_range = self.key_offsets[idx].to_usize()..self.key_offsets[idx + 1].to_usize();
        let val_range = self.val_offsets[idx].to_usize()..self.val_offsets[idx + 1].to_usize();
        let key = &self.key_data[key_range];
        let val = &self.val_data[val_range];
        let ts = self.timestamps[idx];
        let diff = isize::cast_from(self.diffs[idx]);
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
pub struct ColumnarRecordsBuilder {
    len: usize,
    key_data: MutableBuffer<u8>,
    key_offsets: MutableBuffer<i32>,
    val_data: MutableBuffer<u8>,
    val_offsets: MutableBuffer<i32>,
    timestamps: MutableBuffer<u64>,
    diffs: MutableBuffer<i64>,
}

impl fmt::Debug for ColumnarRecordsBuilder {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.borrow(), fmt)
    }
}

impl Default for ColumnarRecordsBuilder {
    fn default() -> Self {
        let mut ret = ColumnarRecordsBuilder {
            len: 0,
            key_data: MutableBuffer::new(),
            key_offsets: MutableBuffer::new(),
            val_data: MutableBuffer::new(),
            val_offsets: MutableBuffer::new(),
            timestamps: MutableBuffer::new(),
            diffs: MutableBuffer::new(),
        };
        // Push initial 0 offsets to maintain our invariants, even as we build.
        ret.key_offsets.push(0);
        ret.val_offsets.push(0);
        debug_assert_eq!(ret.borrow().validate(), Ok(()));
        ret
    }
}

impl ColumnarRecordsBuilder {
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

    /// Reserve space for `additional` more records, based on `key_size_guess` and
    /// `val_size_guess`.
    ///
    /// The guesses for key and val sizes are best effort, and if they end up being
    /// too small, the underlying buffers will be resized.
    pub fn reserve(&mut self, additional: usize, key_size_guess: usize, val_size_guess: usize) {
        self.key_offsets.reserve(additional);
        self.key_data.reserve(additional * key_size_guess);
        self.val_offsets.reserve(additional);
        self.val_data.reserve(additional * val_size_guess);
        self.timestamps.reserve(additional);
        self.diffs.reserve(additional);
        debug_assert_eq!(self.borrow().validate(), Ok(()));
    }

    /// Add a record to Self.
    pub fn push(&mut self, record: ((&[u8], &[u8]), u64, isize)) {
        let ((key, val), ts, diff) = record;
        self.key_data.extend_from_slice(key);
        self.key_offsets
            .push(i32::try_from(self.key_data.len()).expect("batch is smaller than 2GB"));
        self.val_data.extend_from_slice(val);
        self.val_offsets
            .push(i32::try_from(self.val_data.len()).expect("batch is smaller than 2GB"));
        self.timestamps.push(ts);
        self.diffs.push(i64::cast_from(diff));
        self.len += 1;
        debug_assert_eq!(self.borrow().validate(), Ok(()));
    }

    /// Finalize constructing a [ColumnarRecords].
    pub fn finish(self) -> ColumnarRecords {
        let ret = ColumnarRecords {
            len: self.len,
            key_data: Buffer::from(self.key_data),
            key_offsets: Buffer::from(self.key_offsets),
            val_data: Buffer::from(self.val_data),
            val_offsets: Buffer::from(self.val_offsets),
            timestamps: Buffer::from(self.timestamps),
            diffs: Buffer::from(self.diffs),
        };
        debug_assert_eq!(ret.borrow().validate(), Ok(()));
        ret
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
    }
}
