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

use std::collections::HashMap;
use std::fmt;
use std::iter::FromIterator;
use std::sync::Arc;

use arrow2::array::{
    Array, ArrayRef, Int64Array, ListArray, PrimitiveArray, UInt64Array, UInt8Array,
};
use arrow2::buffer::{Buffer, MutableBuffer};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::record_batch::RecordBatch;
use ore::cast::CastFrom;

use crate::error::Error;

/// A set of ((Key, Val), Time, Diff) records stored in a columnar representation.
///
/// Note that the data are unsorted, and unconsolidated (so there may be multiple
/// instances of the same ((Key, Val), Time), and some Diffs might be zero, or
/// add up to zero).
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

    /// Converts these records into the [arrow2] crate's format for a batch of
    /// columnar data.
    ///
    /// This isn't free but it doesn't copy the data.
    pub fn to_record_batch(&self, metadata: HashMap<String, String>) -> RecordBatch {
        let (mut fields, mut columns) = (Vec::with_capacity(4), Vec::with_capacity(4));

        let keys = ListArray::<i32>::from_data(
            ListArray::<i32>::default_datatype(DataType::UInt8),
            self.key_offsets.clone(),
            Arc::new(PrimitiveArray::<u8>::from_data(
                DataType::UInt8,
                self.key_data.clone(),
                None,
            )),
            None,
        );
        fields.push(Field::new(
            "key",
            keys.data_type().clone(),
            keys.validity().is_some(),
        ));
        columns.push(Arc::new(keys) as ArrayRef);

        let vals = ListArray::<i32>::from_data(
            ListArray::<i32>::default_datatype(DataType::UInt8),
            self.val_offsets.clone(),
            Arc::new(PrimitiveArray::<u8>::from_data(
                DataType::UInt8,
                self.val_data.clone(),
                None,
            )),
            None,
        );
        fields.push(Field::new(
            "val",
            vals.data_type().clone(),
            vals.validity().is_some(),
        ));
        columns.push(Arc::new(vals) as ArrayRef);

        let timestamps =
            PrimitiveArray::<u64>::from_data(DataType::UInt64, self.timestamps.clone(), None);
        fields.push(Field::new(
            "ts",
            timestamps.data_type().clone(),
            timestamps.validity().is_some(),
        ));
        columns.push(Arc::new(timestamps) as ArrayRef);

        let diffs = PrimitiveArray::<i64>::from_data(DataType::Int64, self.diffs.clone(), None);
        fields.push(Field::new(
            "diff",
            diffs.data_type().clone(),
            diffs.validity().is_some(),
        ));
        columns.push(Arc::new(diffs) as ArrayRef);

        let schema = Schema::new_from(fields, metadata);
        RecordBatch::try_new(Arc::new(schema), columns).expect("internal error")
    }
}

impl<'a> FromIterator<&'a ((Vec<u8>, Vec<u8>), u64, isize)> for ColumnarRecords {
    fn from_iter<T: IntoIterator<Item = &'a ((Vec<u8>, Vec<u8>), u64, isize)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint();

        let mut builder = ColumnarRecordsBuilder::default();
        for record in iter.into_iter() {
            let ((key, val), ts, diff) = record;
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

impl TryFrom<RecordBatch> for ColumnarRecords {
    type Error = Error;

    fn try_from(x: RecordBatch) -> Result<Self, Self::Error> {
        let len = x.num_rows();
        if x.num_columns() != 4 {
            return Err(format!("expected 4 columns got {}", x.num_columns()).into());
        }
        let key_col = x.column(0);
        let val_col = x.column(1);
        let ts_col = x.column(2);
        let diff_col = x.column(3);

        let key_array = key_col
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .expect("WIP")
            .clone();
        let key_offsets = key_array.offsets().clone();
        let key_data = key_array
            .values()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("WIP")
            .values()
            .clone();
        let val_array = val_col
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .expect("WIP")
            .clone();
        let val_offsets = val_array.offsets().clone();
        let val_data = val_array
            .values()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("WIP")
            .values()
            .clone();
        let timestamps = ts_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("WIP")
            .values()
            .clone();
        let diffs = diff_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("WIP")
            .values()
            .clone();

        Ok(ColumnarRecords {
            len,
            key_data,
            key_offsets,
            val_data,
            val_offsets,
            timestamps,
            diffs,
        })
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
        // WIP
        let key_range = self.key_offsets[idx] as usize..self.key_offsets[idx + 1] as usize;
        let val_range = self.val_offsets[idx] as usize..self.val_offsets[idx + 1] as usize;
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
#[derive(Debug)]
pub struct ColumnarRecordsBuilder {
    len: usize,
    key_data: MutableBuffer<u8>,
    key_offsets: MutableBuffer<i32>,
    val_data: MutableBuffer<u8>,
    val_offsets: MutableBuffer<i32>,
    timestamps: MutableBuffer<u64>,
    diffs: MutableBuffer<i64>,
}

impl Default for ColumnarRecordsBuilder {
    fn default() -> Self {
        ColumnarRecordsBuilder {
            len: 0,
            key_data: MutableBuffer::new(),
            key_offsets: MutableBuffer::new(),
            val_data: MutableBuffer::new(),
            val_offsets: MutableBuffer::new(),
            timestamps: MutableBuffer::new(),
            diffs: MutableBuffer::new(),
        }
    }
}

impl ColumnarRecordsBuilder {
    /// The number of (potentially duplicated) ((Key, Val), Time, Diff) records
    /// stored in Self.
    pub fn len(&self) -> usize {
        self.len
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
        self.key_data.reserve(additional * val_size_guess);
        self.timestamps.reserve(additional);
        self.diffs.reserve(additional);
    }

    /// Add a record to Self.
    pub fn push(&mut self, record: ((&[u8], &[u8]), u64, isize)) {
        let ((key, val), ts, diff) = record;
        self.key_offsets.push(self.key_data.len() as i32);
        self.key_data.extend_from_slice(key);
        self.val_offsets.push(self.val_data.len() as i32);
        self.val_data.extend_from_slice(val);
        self.timestamps.push(ts);
        self.diffs.push(i64::cast_from(diff));
        self.len += 1;
        // The checks on `key_offsets` and `val_offsets` are different than in the
        // rest of the file because those arrays have not received their final
        // value yet in finish().
        debug_assert_eq!(self.key_offsets.len(), self.len);
        debug_assert_eq!(self.val_offsets.len(), self.len);
        debug_assert_eq!(self.timestamps.len(), self.len);
        debug_assert_eq!(self.diffs.len(), self.len);
    }

    /// Finalize constructing a [ColumnarRecords].
    pub fn finish(mut self) -> ColumnarRecords {
        self.key_offsets.push(self.key_data.len() as i32);
        self.val_offsets.push(self.val_data.len() as i32);
        debug_assert_eq!(self.key_offsets.len(), self.len + 1);
        debug_assert_eq!(self.val_offsets.len(), self.len + 1);
        debug_assert_eq!(self.timestamps.len(), self.len);
        debug_assert_eq!(self.diffs.len(), self.len);
        ColumnarRecords {
            len: self.len,
            key_data: Buffer::from(self.key_data),
            key_offsets: Buffer::from(self.key_offsets),
            val_data: Buffer::from(self.val_data),
            val_offsets: Buffer::from(self.val_offsets),
            timestamps: Buffer::from(self.timestamps),
            diffs: Buffer::from(self.diffs),
        }
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
