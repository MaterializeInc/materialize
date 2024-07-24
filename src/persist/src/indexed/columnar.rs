// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A columnar representation of ((Key, Val), Time, i64) data suitable for in-memory
//! reads and persistent storage.

use std::fmt;
use std::mem::size_of;
use std::sync::Arc;

use ::arrow::array::{Array, AsArray, BinaryArray, BinaryBuilder, Int64Array};
use ::arrow::buffer::OffsetBuffer;
use ::arrow::datatypes::ToByteSlice;
use bytes::Bytes;
use mz_persist_types::columnar::{ColumnEncoder, Schema2};
use mz_persist_types::Codec;
use mz_proto::{ProtoType, RustType, TryFromProtoError};

use crate::gen::persist::ProtoColumnarRecords;
use crate::indexed::columnar::arrow::realloc_array;
use crate::metrics::ColumnarMetrics;

pub mod arrow;
pub mod parquet;

/// The maximum allowed amount of total key data (similarly val data) in a
/// single ColumnarBatch.
///
/// Note that somewhat counter-intuitively, this also includes offsets (counting
/// as 4 bytes each) in the definition of "key/val data".
///
/// TODO: The limit on the amount of {key,val} data is because we use i32
/// offsets in parquet; this won't change. However, we include the offsets in
/// the size because the parquet library we use currently maps each Array 1:1
/// with a parquet "page" (so for a "binary" column this is both the offsets and
/// the data). The parquet format internally stores the size of a page in an
/// i32, so if this gets too big, our library overflows it and writes bad data.
/// There's no reason it needs to map an Array 1:1 to a page (it could instead
/// be 1:1 with a "column chunk", which contains 1 or more pages). For now, we
/// work around it.
// TODO(benesch): find a way to express this without `as`.
#[allow(clippy::as_conversions)]
pub const KEY_VAL_DATA_MAX_LEN: usize = i32::MAX as usize;

const BYTES_PER_KEY_VAL_OFFSET: usize = 4;

/// A set of ((Key, Val), Time, Diff) records stored in a columnar
/// representation.
///
/// Note that the data are unsorted, and unconsolidated (so there may be
/// multiple instances of the same ((Key, Val), Time), and some Diffs might be
/// zero, or add up to zero).
///
/// Both Time and Diff are presented externally to persist users as a type
/// parameter that implements [mz_persist_types::Codec64]. Our columnar format
/// intentionally stores them both as i64 columns (as opposed to something like
/// a fixed width binary column) because this allows us additional compression
/// options.
///
/// Also note that we intentionally use an i64 over a u64 for Time. Over the
/// range `[0, i64::MAX]`, the bytes are the same and we've talked at various
/// times about changing Time in mz to an i64. Both millis since unix epoch and
/// nanos since unix epoch easily fit into this range (the latter until some
/// time after year 2200). Using a i64 might be a pessimization for a
/// non-realtime mz source with u64 timestamps in the range `(i64::MAX,
/// u64::MAX]`, but realtime sources are overwhelmingly the common case.
///
/// Invariants:
/// - len <= u32::MAX (since we use arrow's `BinaryArray` for our binary data)
/// - the length of all arrays should == len
/// - all entries should be non-null
#[derive(Clone, PartialEq)]
pub struct ColumnarRecords {
    len: usize,
    key_data: BinaryArray,
    val_data: BinaryArray,
    timestamps: Int64Array,
    diffs: Int64Array,
}

impl Default for ColumnarRecords {
    fn default() -> Self {
        Self {
            len: 0,
            key_data: BinaryArray::from_vec(vec![]),
            val_data: BinaryArray::from_vec(vec![]),
            timestamps: Int64Array::from_iter_values([]),
            diffs: Int64Array::from_iter_values([]),
        }
    }
}

impl fmt::Debug for ColumnarRecords {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.borrow(), fmt)
    }
}

impl ColumnarRecords {
    /// The number of (potentially duplicated) ((Key, Val), Time, i64) records
    /// stored in Self.
    pub fn len(&self) -> usize {
        self.len
    }

    /// The keys in this columnar records as an array.
    pub fn keys(&self) -> &BinaryArray {
        &self.key_data
    }

    /// The number of logical bytes in the represented data, excluding offsets
    /// and lengths.
    pub fn goodbytes(&self) -> usize {
        self.key_data.values().len()
            + self.val_data.values().len()
            + self.timestamps.values().inner().len()
            + self.diffs.values().inner().len()
    }

    /// Read the record at `idx`, if there is one.
    ///
    /// Returns None if `idx >= self.len()`.
    pub fn get<'a>(&'a self, idx: usize) -> Option<((&'a [u8], &'a [u8]), [u8; 8], [u8; 8])> {
        self.borrow().get(idx)
    }

    /// Borrow Self as a [ColumnarRecordsRef].
    fn borrow<'a>(&'a self) -> ColumnarRecordsRef<'a> {
        // The ColumnarRecords constructor already validates, so don't bother
        // doing it again.
        //
        // TODO: Forcing everything through a `fn new` would make this more
        // obvious.
        ColumnarRecordsRef {
            len: self.len,
            key_data: &self.key_data,
            val_data: &self.val_data,
            timestamps: self.timestamps.values(),
            diffs: self.diffs.values(),
        }
    }

    /// Iterate through the records in Self.
    pub fn iter<'a>(&'a self) -> ColumnarRecordsIter<'a> {
        self.borrow().iter()
    }

    /// Concatenate the given records together, column-by-column.
    pub fn concat(records: &[ColumnarRecords], metrics: &ColumnarMetrics) -> ColumnarRecords {
        match records.len() {
            0 => return ColumnarRecords::default(),
            1 => return records[0].clone(),
            _ => {}
        }

        let mut concat_array = vec![];
        let mut concat = |get: fn(&ColumnarRecords) -> &dyn Array| {
            concat_array.extend(records.iter().map(get));
            let res = ::arrow::compute::concat(&concat_array).expect("same type");
            concat_array.clear();
            res
        };

        Self {
            len: records.iter().map(|c| c.len).sum(),
            key_data: realloc_array(concat(|c| &c.key_data).as_binary(), metrics),
            val_data: realloc_array(concat(|c| &c.val_data).as_binary(), metrics),
            timestamps: realloc_array(concat(|c| &c.timestamps).as_primitive(), metrics),
            diffs: realloc_array(concat(|c| &c.diffs).as_primitive(), metrics),
        }
    }
}

/// A reference to a [ColumnarRecords].
#[derive(Clone)]
struct ColumnarRecordsRef<'a> {
    len: usize,
    key_data: &'a BinaryArray,
    val_data: &'a BinaryArray,
    timestamps: &'a [i64],
    diffs: &'a [i64],
}

impl<'a> fmt::Debug for ColumnarRecordsRef<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> ColumnarRecordsRef<'a> {
    fn validate(&self) -> Result<(), String> {
        let validate_array = |name: &str, array: &dyn Array| {
            let len = array.len();
            if len != self.len {
                return Err(format!("expected {} {name} got {len}", self.len));
            }
            let null_count = array.null_count();
            if null_count > 0 {
                return Err(format!("{null_count} unexpected nulls in {name} array"));
            }
            Ok(())
        };

        let key_data_size =
            self.key_data.values().len() + self.key_data.offsets().inner().inner().len();
        if key_data_size > KEY_VAL_DATA_MAX_LEN {
            return Err(format!(
                "expected encoded key offsets and data size to be less than or equal to {} got {}",
                KEY_VAL_DATA_MAX_LEN, key_data_size
            ));
        }
        validate_array("keys", &self.key_data)?;

        let val_data_size =
            self.val_data.values().len() + self.val_data.offsets().inner().inner().len();
        if val_data_size > KEY_VAL_DATA_MAX_LEN {
            return Err(format!(
                "expected encoded val offsets and data size to be less than or equal to {} got {}",
                KEY_VAL_DATA_MAX_LEN, val_data_size
            ));
        }
        validate_array("vals", &self.val_data)?;

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

        Ok(())
    }

    /// Read the record at `idx`, if there is one.
    ///
    /// Returns None if `idx >= self.len()`.
    fn get(&self, idx: usize) -> Option<((&'a [u8], &'a [u8]), [u8; 8], [u8; 8])> {
        if idx >= self.len {
            return None;
        }

        // There used to be `debug_assert_eq!(self.validate(), Ok(()))`, but it
        // resulted in accidentally O(n^2) behavior in debug mode. Instead, we
        // push that responsibility to the ColumnarRecordsRef constructor.
        let key = self.key_data.value(idx);
        let val = self.val_data.value(idx);
        let ts = i64::to_le_bytes(self.timestamps[idx]);
        let diff = i64::to_le_bytes(self.diffs[idx]);
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
    type Item = ((&'a [u8], &'a [u8]), [u8; 8], [u8; 8]);

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

/// An abstraction to incrementally add ((Key, Value), Time, i64) records
/// in a columnar representation, and eventually get back a [ColumnarRecords].
#[derive(Debug)]
pub struct ColumnarRecordsBuilder<K: Codec, V: Codec> {
    len: usize,

    /// [`Codec`] key data.
    key_data: BinaryBuilder,
    /// Structured key data.
    key_encoder: <K::Schema as Schema2<K>>::Encoder,
    /// [`Codec`] val data.
    val_data: BinaryBuilder,
    /// Structured val data.
    val_encoder: <V::Schema as Schema2<V>>::Encoder,

    timestamps: Vec<i64>,
    diffs: Vec<i64>,
}

impl<K: Codec, V: Codec> ColumnarRecordsBuilder<K, V> {
    /// Create a new [`ColumnarRecordsBuilder`].
    ///
    /// Note: If you know how many records you need to store, prefer [`ColumnarRecordsBuilder::with_capacity`].
    pub fn new(key_schema: &K::Schema, val_schema: &V::Schema) -> Self {
        let key_data = BinaryBuilder::new();
        let key_encoder = key_schema.encoder().expect("TODO");
        let val_data = BinaryBuilder::new();
        let val_encoder = val_schema.encoder().expect("TODO");

        Self {
            len: 0,
            key_data,
            key_encoder,
            val_data,
            val_encoder,
            timestamps: Vec::new(),
            diffs: Vec::new(),
        }
    }

    /// Reserve space for the given number of items with the given sizes in bytes.
    /// If they end up being too small, the underlying buffers will be resized as usual.
    pub fn with_capacity(
        items: usize,
        key_bytes: usize,
        key_schema: &K::Schema,
        val_bytes: usize,
        val_schema: &V::Schema,
    ) -> Self {
        let key_data = BinaryBuilder::with_capacity(items, key_bytes);
        let key_encoder = key_schema.encoder().expect("TODO");
        let val_data = BinaryBuilder::with_capacity(items, val_bytes);
        let val_encoder = val_schema.encoder().expect("TODO");
        let timestamps = Vec::with_capacity(items);
        let diffs = Vec::with_capacity(items);

        Self {
            len: 0,
            key_data,
            key_encoder,
            val_data,
            val_encoder,
            timestamps,
            diffs,
        }
    }

    /// The number of (potentially duplicated) ((Key, Val), Time, i64) records
    /// stored in Self.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns if the given key_offsets+key_data or val_offsets+val_data fits
    /// in the limits imposed by ColumnarRecords.
    ///
    /// Note that limit is always [KEY_VAL_DATA_MAX_LEN] in production. It's
    /// only override-able here for testing.
    pub fn can_fit(&self, key: &[u8], val: &[u8], limit: usize) -> bool {
        let key_data_size = self.key_data.values_slice().len()
            + self.key_data.offsets_slice().to_byte_slice().len()
            + key.len();
        let val_data_size = self.val_data.values_slice().len()
            + self.val_data.offsets_slice().to_byte_slice().len()
            + val.len();
        key_data_size <= limit && val_data_size <= limit
    }

    /// The current size of the columnar records data, useful for bounding batches at a
    /// target size.
    pub fn total_bytes(&self) -> usize {
        self.key_data.values_slice().len()
            + self.key_data.offsets_slice().to_byte_slice().len()
            + self.val_data.values_slice().len()
            + self.val_data.offsets_slice().to_byte_slice().len()
            + self.timestamps.to_byte_slice().len()
            + self.diffs.to_byte_slice().len()
    }

    /// Add a record to Self.
    ///
    /// Returns whether the record was successfully added. A record will not a
    /// added if it exceeds the size limitations of ColumnarBatch. This method
    /// is atomic, if it fails, no partial data will have been added.
    #[must_use]
    pub fn push(&mut self, record: ((&K, &V), [u8; 8], [u8; 8])) -> bool {
        let ((key, val), ts, diff) = record;

        let mut k_encoded = vec![];
        K::encode(key, &mut k_encoded);

        let mut v_encoded = vec![];
        V::encode(val, &mut v_encoded);

        // Check size invariants ahead of time so we stay atomic when we can't
        // add the record.
        if !self.can_fit(&k_encoded, &v_encoded, KEY_VAL_DATA_MAX_LEN) {
            return false;
        }

        self.key_data.append_value(k_encoded);
        self.key_encoder.append(key);
        self.val_data.append_value(v_encoded);
        self.val_encoder.append(val);

        self.timestamps.push(i64::from_le_bytes(ts));
        self.diffs.push(i64::from_le_bytes(diff));
        self.len += 1;

        true
    }

    /// Finalize constructing a [ColumnarRecords].
    pub fn finish(mut self, _metrics: &ColumnarMetrics) -> ColumnarRecords {
        // We're almost certainly going to immediately encode this and drop it,
        // so don't bother actually copying the data into lgalloc.
        // Revisit if that changes.
        let ret = ColumnarRecords {
            len: self.len,
            key_data: BinaryBuilder::finish(&mut self.key_data),
            val_data: BinaryBuilder::finish(&mut self.val_data),
            timestamps: self.timestamps.into(),
            diffs: self.diffs.into(),
        };
        debug_assert_eq!(ret.borrow().validate(), Ok(()));
        ret
    }

    /// Size of an update record as stored in the columnar representation
    pub fn columnar_record_size(key_bytes_len: usize, value_bytes_len: usize) -> usize {
        (key_bytes_len + BYTES_PER_KEY_VAL_OFFSET)
            + (value_bytes_len + BYTES_PER_KEY_VAL_OFFSET)
            + (2 * size_of::<u64>()) // T and D
    }
}

impl ColumnarRecords {
    /// See [RustType::into_proto].
    pub fn into_proto(&self) -> ProtoColumnarRecords {
        ProtoColumnarRecords {
            len: self.len.into_proto(),
            key_offsets: self.key_data.offsets().to_vec(),
            key_data: Bytes::copy_from_slice(self.key_data.value_data()),
            val_offsets: self.val_data.offsets().to_vec(),
            val_data: Bytes::copy_from_slice(self.val_data.value_data()),
            timestamps: self.timestamps.values().to_vec(),
            diffs: self.diffs.values().to_vec(),
        }
    }

    /// See [RustType::from_proto].
    pub fn from_proto(
        lgbytes: &ColumnarMetrics,
        proto: ProtoColumnarRecords,
    ) -> Result<Self, TryFromProtoError> {
        let binary_array = |data: Bytes, offsets: Vec<i32>| match BinaryArray::try_new(
            OffsetBuffer::new(offsets.into()),
            data.into(),
            None,
        ) {
            Ok(data) => Ok(realloc_array(&data, lgbytes)),
            Err(e) => Err(TryFromProtoError::InvalidFieldError(format!(
                "Unable to decode binary array from repeated proto fields: {e:?}"
            ))),
        };

        let ret = ColumnarRecords {
            len: proto.len.into_rust()?,
            key_data: binary_array(proto.key_data, proto.key_offsets)?,
            val_data: binary_array(proto.val_data, proto.val_offsets)?,
            timestamps: realloc_array(&proto.timestamps.into(), lgbytes),
            diffs: realloc_array(&proto.diffs.into(), lgbytes),
        };
        let () = ret
            .borrow()
            .validate()
            .map_err(TryFromProtoError::InvalidPersistState)?;
        Ok(ret)
    }
}

/// An "extension" to [`ColumnarRecords`] that duplicates the "key" (`K`) and "val" (`V`) columns
/// as structured Arrow data.
///
/// [`ColumnarRecords`] stores the key and value columns as binary blobs encoded with the [`Codec`]
/// trait. We're migrating to instead store the key and value columns as structured Parquet data,
/// which we interface with via Arrow.
///
/// [`Codec`]: mz_persist_types::Codec
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnarRecordsStructuredExt {
    /// The structured `k` column.
    ///
    /// [`arrow`] does not allow empty [`StructArray`]s so we model an empty `key` column as None.
    ///
    /// [`StructArray`]: ::arrow::array::StructArray
    pub key: Option<Arc<dyn Array>>,
    /// The structured `v` column.
    ///
    /// [`arrow`] does not allow empty [`StructArray`]s so we model an empty `val` column as None.
    ///
    /// [`StructArray`]: ::arrow::array::StructArray
    pub val: Option<Arc<dyn Array>>,
}

#[cfg(test)]
mod tests {
    use mz_persist_types::codec_impls::VecU8Schema;
    use mz_persist_types::Codec64;

    use super::*;

    /// Smoke test some edge cases around empty sets of records and empty keys/vals
    ///
    /// Most of this functionality is also well-exercised in other unit tests as well.
    #[mz_ore::test]
    fn columnar_records() {
        let metrics = ColumnarMetrics::disconnected();
        let builder = ColumnarRecordsBuilder::<Vec<u8>, Vec<u8>>::new(&VecU8Schema, &VecU8Schema);

        // Empty builder.
        let records = builder.finish(&metrics);
        let reads: Vec<_> = records.iter().collect();
        assert_eq!(reads, vec![]);

        // Empty key and val.
        let updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)> = vec![
            (("".into(), "".into()), 0, 0),
            (("".into(), "".into()), 1, 1),
        ];
        let mut builder = ColumnarRecordsBuilder::new(&VecU8Schema, &VecU8Schema);
        for ((key, val), time, diff) in updates.iter() {
            assert!(builder.push(((key, val), u64::encode(time), i64::encode(diff))));
        }

        let records = builder.finish(&metrics);
        let reads: Vec<_> = records
            .iter()
            .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), u64::decode(t), i64::decode(d)))
            .collect();
        assert_eq!(reads, updates);
    }
}
