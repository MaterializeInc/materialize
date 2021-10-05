// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// WIP
#![allow(missing_docs)]

use std::iter::FromIterator;

// WIP note there are no guarantees about ordering or consolidation
#[derive(Clone)]
pub struct ColumnarRecords {
    len: usize,
    key_data: Vec<u8>,
    key_offsets: Vec<usize>,
    val_data: Vec<u8>,
    val_offsets: Vec<usize>,
    timestamps: Vec<u64>,
    diffs: Vec<isize>,
}

impl ColumnarRecords {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn borrow<'a>(&'a self) -> ColumnarRecordsRef<'a> {
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

    // WIP mention this also clears
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

    pub fn iter<'a>(&'a self) -> ColumnarRecordsIter<'a> {
        self.borrow().iter()
    }
}

impl<'a> FromIterator<&'a ((Vec<u8>, Vec<u8>), u64, isize)> for ColumnarRecords {
    fn from_iter<T: IntoIterator<Item = &'a ((Vec<u8>, Vec<u8>), u64, isize)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint();

        let mut builder = ColumnarRecordsBuilder::default();
        for record in iter.into_iter() {
            let ((key, val), ts, diff) = record;
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

#[derive(Clone)]
pub struct ColumnarRecordsRef<'a> {
    len: usize,
    key_data: &'a [u8],
    key_offsets: &'a [usize],
    val_data: &'a [u8],
    val_offsets: &'a [usize],
    timestamps: &'a [u64],
    diffs: &'a [isize],
}

impl<'a> ColumnarRecordsRef<'a> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, idx: usize) -> Option<((&'a [u8], &'a [u8]), u64, isize)> {
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

    pub fn iter(&self) -> ColumnarRecordsIter<'a> {
        ColumnarRecordsIter {
            idx: 0,
            records: self.clone(),
        }
    }
}

#[derive(Clone)]
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

pub struct ColumnarRecordsBuilder {
    records: ColumnarRecords,
}

impl Default for ColumnarRecordsBuilder {
    fn default() -> Self {
        Self {
            records: ColumnarRecords {
                len: 0,
                key_offsets: Vec::new(),
                key_data: Vec::new(),
                val_offsets: Vec::new(),
                val_data: Vec::new(),
                timestamps: Vec::new(),
                diffs: Vec::new(),
            },
        }
    }
}

impl ColumnarRecordsBuilder {
    pub fn len(&self) -> usize {
        self.records.len
    }

    pub fn reserve(&mut self, additional: usize, key_size_guess: usize, val_size_guess: usize) {
        self.records.key_offsets.reserve(additional);
        self.records.key_data.reserve(additional * key_size_guess);
        self.records.val_offsets.reserve(additional);
        self.records.key_data.reserve(additional * val_size_guess);
        self.records.timestamps.reserve(additional);
        self.records.diffs.reserve(additional);
    }

    pub fn push(&mut self, record: ((&[u8], &[u8]), u64, isize)) {
        let ((key, val), ts, diff) = record;
        self.records.key_offsets.push(self.records.key_data.len());
        self.records.key_data.extend_from_slice(key);
        self.records.val_offsets.push(self.records.val_data.len());
        self.records.val_data.extend_from_slice(val);
        self.records.timestamps.push(ts);
        self.records.diffs.push(diff);
        self.records.len += 1;
        debug_assert_eq!(self.records.key_offsets.len(), self.records.len);
        debug_assert_eq!(self.records.val_offsets.len(), self.records.len);
        debug_assert_eq!(self.records.timestamps.len(), self.records.len);
        debug_assert_eq!(self.records.diffs.len(), self.records.len);
    }

    pub fn mut_diff(&mut self, idx: usize) -> Option<&mut isize> {
        if idx >= self.records.len {
            return None;
        }
        debug_assert_eq!(self.records.diffs.len(), self.records.len);
        Some(&mut self.records.diffs[idx])
    }

    pub fn finish(mut self) -> ColumnarRecords {
        self.records.key_offsets.push(self.records.key_data.len());
        self.records.val_offsets.push(self.records.val_data.len());
        self.records
    }
}
