// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data structures stored in Blobs and Logs in serialized form.

// NB: Everything starting with Blob* is directly serialized as a Blob value.
// Ditto for Log* and the Log. The others are used internally in these top-level
// structs.

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::BufMut;
use differential_dataflow::trace::Description;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::soft_panic_or_log;
use mz_persist_types::columnar::{codec_to_schema2, data_type};
use mz_persist_types::parquet::EncodingConfig;
use mz_persist_types::schema::backward_compatible;
use mz_persist_types::{Codec, Codec64};
use proptest::arbitrary::Arbitrary;
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Just};
use prost::Message;
use serde::Serialize;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::error;

use crate::error::Error;
use crate::gen::persist::proto_batch_part_inline::FormatMetadata as ProtoFormatMetadata;
use crate::gen::persist::{
    ProtoBatchFormat, ProtoBatchPartInline, ProtoU64Antichain, ProtoU64Description,
};
use crate::indexed::columnar::parquet::{decode_trace_parquet, encode_trace_parquet};
use crate::indexed::columnar::{ColumnarRecords, ColumnarRecordsStructuredExt};
use crate::location::Blob;
use crate::metrics::ColumnarMetrics;

/// Column format of a batch.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum BatchColumnarFormat {
    /// Rows are encoded to `ProtoRow` and then a batch is written down as a Parquet with a schema
    /// of `(k, v, t, d)`, where `k` are the serialized bytes.
    Row,
    /// Rows are encoded to `ProtoRow` and a columnar struct. The batch is written down as Parquet
    /// with a schema of `(k, k_c, v, v_c, t, d)`, where `k` are the serialized bytes and `k_c` is
    /// nested columnar data.
    Both(usize),
}

impl BatchColumnarFormat {
    /// Returns a default value for [`BatchColumnarFormat`].
    pub const fn default() -> Self {
        // IMPORTANT: Default to only writing Row data and not our newer structured format.
        BatchColumnarFormat::Row
    }

    /// Returns a [`BatchColumnarFormat`] for a given `&str`, falling back to a default value if
    /// provided `&str` is invalid.
    pub fn from_str(s: &str) -> Self {
        match s {
            "row" => BatchColumnarFormat::Row,
            "both" => BatchColumnarFormat::Both(0),
            "both_v2" => BatchColumnarFormat::Both(2),
            x => {
                let default = BatchColumnarFormat::default();
                soft_panic_or_log!("Invalid batch columnar type: {x}, falling back to {default}");
                default
            }
        }
    }

    /// Returns a string representation for the [`BatchColumnarFormat`].
    pub const fn as_str(&self) -> &'static str {
        match self {
            BatchColumnarFormat::Row => "row",
            BatchColumnarFormat::Both(0 | 1) => "both",
            BatchColumnarFormat::Both(2) => "both_v2",
            _ => panic!("unknown batch columnar format"),
        }
    }

    /// Returns if we should encode a Batch in a structured format.
    pub const fn is_structured(&self) -> bool {
        match self {
            BatchColumnarFormat::Row => false,
            // The V0 format has been deprecated and we ignore its structured columns.
            BatchColumnarFormat::Both(0 | 1) => false,
            BatchColumnarFormat::Both(_) => true,
        }
    }
}

impl fmt::Display for BatchColumnarFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Arbitrary for BatchColumnarFormat {
    type Parameters = ();
    type Strategy = BoxedStrategy<BatchColumnarFormat>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        proptest::strategy::Union::new(vec![
            Just(BatchColumnarFormat::Row).boxed(),
            Just(BatchColumnarFormat::Both(0)).boxed(),
            Just(BatchColumnarFormat::Both(1)).boxed(),
        ])
        .boxed()
    }
}

/// The metadata necessary to reconstruct a list of [BlobTraceBatchPart]s.
///
/// Invariants:
/// - The Description's time interval is non-empty.
/// - Keys for all trace batch parts are unique.
/// - Keys for all trace batch parts are stored in index order.
/// - The data in all of the trace batch parts is sorted and consolidated.
/// - All of the trace batch parts have the same desc as the metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceBatchMeta {
    /// The keys to retrieve the batch's data from the blob store.
    ///
    /// The set of keys can be empty to denote an empty batch.
    pub keys: Vec<String>,
    /// The format of the stored batch data.
    pub format: ProtoBatchFormat,
    /// The half-open time interval `[lower, upper)` this batch contains data
    /// for.
    pub desc: Description<u64>,
    /// The compaction level of each batch.
    pub level: u64,
    /// Size of the encoded batch.
    pub size_bytes: u64,
}

/// The structure serialized and stored as a value in
/// [crate::location::Blob] storage for data keys corresponding to trace
/// data.
///
/// This batch represents the data that was originally written at some time in
/// [lower, upper) (more precisely !< lower and < upper). The individual record
/// times may have later been advanced by compaction to something <= since. This
/// means the ability to reconstruct the state of the collection at times <
/// since has been lost. However, there may still be records present in the
/// batch whose times are < since. Users iterating through updates must take
/// care to advance records with times < since to since in order to correctly
/// answer queries at times >= since.
///
/// Invariants:
/// - The [lower, upper) interval of times in desc is non-empty.
/// - The timestamp of each update is >= to desc.lower().
/// - The timestamp of each update is < desc.upper() iff desc.upper() >
///   desc.since(). Otherwise the timestamp of each update is <= desc.since().
/// - The values in updates are sorted by (key, value, time).
/// - The values in updates are "consolidated", i.e. (key, value, time) is
///   unique.
/// - All entries have a non-zero diff.
///
/// TODO: disallow empty trace batch parts in the future so there is one unique
/// way to represent an empty trace batch.
#[derive(Clone, Debug, PartialEq)]
pub struct BlobTraceBatchPart<T> {
    /// Which updates are included in this batch.
    ///
    /// There may be other parts for the batch that also contain updates within
    /// the specified [lower, upper) range.
    pub desc: Description<T>,
    /// Index of this part in the list of parts that form the batch.
    pub index: u64,
    /// The updates themselves.
    pub updates: BlobTraceUpdates,
}

/// The set of updates that are part of a [`BlobTraceBatchPart`].
#[derive(Clone, Debug, PartialEq)]
pub enum BlobTraceUpdates {
    /// Legacy format. Keys and Values are encoded into bytes via [`Codec`], then stored in our own
    /// columnar-esque struct.
    ///
    /// [`Codec`]: mz_persist_types::Codec
    Row(ColumnarRecords),
    /// Migration format. Keys and Values are encoded into bytes via [`Codec`] and structured into
    /// an Apache Arrow columnar format.
    ///
    /// [`Codec`]: mz_persist_types::Codec
    Both(ColumnarRecords, ColumnarRecordsStructuredExt),
    // TODO(parkmycar): Write only columnar/Arrow data.
}

impl BlobTraceUpdates {
    /// The number of updates.
    pub fn len(&self) -> usize {
        self.records().len()
    }

    /// Return the [`ColumnarRecords`] of the blob.
    pub fn records(&self) -> &ColumnarRecords {
        match self {
            BlobTraceUpdates::Row(c) => c,
            BlobTraceUpdates::Both(c, _structured) => c,
        }
    }

    /// Return the [`ColumnarRecordsStructuredExt`] of the blob.
    pub fn structured(&self) -> Option<&ColumnarRecordsStructuredExt> {
        match self {
            BlobTraceUpdates::Row(_) => None,
            BlobTraceUpdates::Both(_, structured) => Some(structured),
        }
    }

    /// Return the estimated memory usage of the raw data.
    pub fn goodbytes(&self) -> usize {
        self.records().goodbytes()
            + self.structured().map_or(0, |e| {
                e.key.get_buffer_memory_size() + e.val.get_buffer_memory_size()
            })
    }

    /// Return the [`ColumnarRecordsStructuredExt`] of the blob.
    pub fn get_or_make_structured<K: Codec, V: Codec>(
        &mut self,
        key_schema: &K::Schema,
        val_schema: &V::Schema,
    ) -> &ColumnarRecordsStructuredExt {
        match self {
            BlobTraceUpdates::Row(records) => {
                let key = codec_to_schema2::<K>(key_schema, records.keys()).expect("valid keys");
                let val = codec_to_schema2::<V>(val_schema, records.vals()).expect("valid values");
                *self = BlobTraceUpdates::Both(
                    records.clone(),
                    ColumnarRecordsStructuredExt { key, val },
                );
                // Recurse at most once, since this data is now structured.
                self.get_or_make_structured::<K, V>(key_schema, val_schema)
            }
            BlobTraceUpdates::Both(_, structured) => {
                // If the types don't match, attempt to migrate the array to the new type.
                // We expect this to succeed, since this should only be called with backwards-
                // compatible schemas... but if it fails we only log, and let some higher-level
                // code signal the error if it cares.
                let migrate = |array: &mut ArrayRef, to_type: DataType| {
                    // TODO: Plumb down the SchemaCache and use it here for the array migrations.
                    let from_type = array.data_type().clone();
                    if from_type != to_type {
                        if let Some(migration) = backward_compatible(&from_type, &to_type) {
                            *array = migration.migrate(Arc::clone(array));
                        } else {
                            error!(
                                ?from_type,
                                ?to_type,
                                "failed to migrate array type; backwards-incompatible schema migration?"
                            );
                        }
                    }
                };
                migrate(
                    &mut structured.key,
                    data_type::<K>(key_schema).expect("valid key schema"),
                );
                migrate(
                    &mut structured.val,
                    data_type::<V>(val_schema).expect("valid value schema"),
                );

                structured
            }
        }
    }

    /// Concatenate the given records together, column-by-column.
    pub fn concat<K: Codec, V: Codec>(
        mut updates: Vec<BlobTraceUpdates>,
        key_schema: &K::Schema,
        val_schema: &V::Schema,
        metrics: &ColumnarMetrics,
    ) -> anyhow::Result<BlobTraceUpdates> {
        match updates.len() {
            0 => return Ok(BlobTraceUpdates::Row(ColumnarRecords::default())),
            1 => return Ok(updates.into_iter().into_element()),
            _ => {}
        }

        let columnar: Vec<_> = updates.iter().map(|u| u.records().clone()).collect();
        let records = ColumnarRecords::concat(&columnar, metrics);

        let mut keys = Vec::with_capacity(records.len());
        let mut vals = Vec::with_capacity(records.len());
        for updates in &mut updates {
            let structured = updates.get_or_make_structured::<K, V>(key_schema, val_schema);
            keys.push(structured.key.as_ref());
            vals.push(structured.val.as_ref());
        }
        let ext = ColumnarRecordsStructuredExt {
            key: ::arrow::compute::concat(&keys)?,
            val: ::arrow::compute::concat(&vals)?,
        };

        let out = Self::Both(records, ext);
        metrics
            .arrow
            .concat_bytes
            .inc_by(u64::cast_from(out.goodbytes()));
        Ok(out)
    }
}

impl TraceBatchMeta {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if the equal case (an empty desc) is
        // useful/harmful. Feel free to make this a less_than if empty descs end
        // up making sense.
        if PartialOrder::less_equal(self.desc.upper(), self.desc.lower()) {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }

        Ok(())
    }

    /// Assert that all of the [BlobTraceBatchPart]'s obey the required invariants.
    pub async fn validate_data(
        &self,
        blob: &dyn Blob,
        metrics: &ColumnarMetrics,
    ) -> Result<(), Error> {
        let mut batches = vec![];
        for (idx, key) in self.keys.iter().enumerate() {
            let value = blob
                .get(key)
                .await?
                .ok_or_else(|| Error::from(format!("no blob for trace batch at key: {}", key)))?;
            let batch = BlobTraceBatchPart::decode(&value, metrics)?;
            if batch.desc != self.desc {
                return Err(format!(
                    "invalid trace batch part desc expected {:?} got {:?}",
                    &self.desc, &batch.desc
                )
                .into());
            }

            if batch.index != u64::cast_from(idx) {
                return Err(format!(
                    "invalid index for blob trace batch part at key {} expected {} got {}",
                    key, idx, batch.index
                )
                .into());
            }

            batch.validate()?;
            batches.push(batch);
        }

        for update in batches
            .iter()
            .flat_map(|batch| batch.updates.records().iter())
        {
            let ((_key, _val), _ts, diff) = update;

            // TODO: Don't assume diff is an i64, take a D type param instead.
            let diff: u64 = Codec64::decode(diff);

            // Check data invariants.
            if diff == 0 {
                return Err(format!(
                    "update with 0 diff: {:?}",
                    PrettyRecord::<u64, i64>(update, PhantomData)
                )
                .into());
            }
        }

        Ok(())
    }
}

impl<T: Timestamp + Codec64> BlobTraceBatchPart<T> {
    /// Asserts the documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if the equal case (an empty desc) is
        // useful/harmful. Feel free to make this a less_than if empty descs end
        // up making sense.
        if PartialOrder::less_equal(self.desc.upper(), self.desc.lower()) {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }

        let uncompacted = PartialOrder::less_equal(self.desc.since(), self.desc.lower());

        for update in self.updates.records().iter() {
            let ((_key, _val), ts, diff) = update;
            // TODO: Don't assume diff is an i64, take a D type param instead.
            let ts = T::decode(ts);
            let diff: i64 = Codec64::decode(diff);

            // Check ts against desc.
            if !self.desc.lower().less_equal(&ts) {
                return Err(format!(
                    "timestamp {:?} is less than the batch lower: {:?}",
                    ts, self.desc
                )
                .into());
            }

            // when since is less than or equal to lower, the upper is a strict bound on the updates'
            // timestamp because no compaction has been performed. Because user batches are always
            // uncompacted, this ensures that new updates are recorded with valid timestamps.
            // Otherwise, we can make no assumptions about the timestamps
            if uncompacted && self.desc.upper().less_equal(&ts) {
                return Err(format!(
                    "timestamp {:?} is greater than or equal to the batch upper: {:?}",
                    ts, self.desc
                )
                .into());
            }

            // Check data invariants.
            if diff == 0 {
                return Err(format!(
                    "update with 0 diff: {:?}",
                    PrettyRecord::<u64, i64>(update, PhantomData)
                )
                .into());
            }
        }
        Ok(())
    }

    /// Encodes an BlobTraceBatchPart into the Parquet format.
    pub fn encode<B>(&self, buf: &mut B, metrics: &ColumnarMetrics, cfg: &EncodingConfig)
    where
        B: BufMut + Send,
    {
        encode_trace_parquet(&mut buf.writer(), self, metrics, cfg).expect("batch was invalid");
    }

    /// Decodes a BlobTraceBatchPart from the Parquet format.
    pub fn decode(buf: &SegmentedBytes, metrics: &ColumnarMetrics) -> Result<Self, Error> {
        decode_trace_parquet(buf.clone(), metrics)
    }

    /// Scans the part and returns a lower bound on the contained keys.
    pub fn key_lower(&self) -> &[u8] {
        self.updates
            .records()
            .iter()
            .map(|((key, _), _, _)| key)
            .min()
            .unwrap_or(&[])
    }
}

#[derive(PartialOrd, Ord, PartialEq, Eq)]
struct PrettyBytes<'a>(&'a [u8]);

impl fmt::Debug for PrettyBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(self.0) {
            Ok(x) => fmt::Debug::fmt(x, f),
            Err(_) => fmt::Debug::fmt(self.0, f),
        }
    }
}

struct PrettyRecord<'a, T, D>(
    ((&'a [u8], &'a [u8]), [u8; 8], [u8; 8]),
    PhantomData<(T, D)>,
);

impl<T, D> fmt::Debug for PrettyRecord<'_, T, D>
where
    T: Debug + Codec64,
    D: Debug + Codec64,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ((k, v), ts, diff) = &self.0;
        fmt::Debug::fmt(
            &(
                (PrettyBytes(k), PrettyBytes(v)),
                T::decode(*ts),
                D::decode(*diff),
            ),
            f,
        )
    }
}

impl<T: Timestamp + Codec64> From<ProtoU64Description> for Description<T> {
    fn from(x: ProtoU64Description) -> Self {
        Description::new(
            x.lower
                .map_or_else(|| Antichain::from_elem(T::minimum()), |x| x.into()),
            x.upper
                .map_or_else(|| Antichain::from_elem(T::minimum()), |x| x.into()),
            x.since
                .map_or_else(|| Antichain::from_elem(T::minimum()), |x| x.into()),
        )
    }
}

impl<T: Timestamp + Codec64> From<ProtoU64Antichain> for Antichain<T> {
    fn from(x: ProtoU64Antichain) -> Self {
        Antichain::from(
            x.elements
                .into_iter()
                .map(|x| T::decode(u64::to_le_bytes(x)))
                .collect::<Vec<_>>(),
        )
    }
}

impl<T: Timestamp + Codec64> From<&Antichain<T>> for ProtoU64Antichain {
    fn from(x: &Antichain<T>) -> Self {
        ProtoU64Antichain {
            elements: x
                .elements()
                .iter()
                .map(|x| u64::from_le_bytes(T::encode(x)))
                .collect(),
        }
    }
}

impl<T: Timestamp + Codec64> From<&Description<T>> for ProtoU64Description {
    fn from(x: &Description<T>) -> Self {
        ProtoU64Description {
            lower: Some(x.lower().into()),
            upper: Some(x.upper().into()),
            since: Some(x.since().into()),
        }
    }
}

/// Encodes the inline metadata for a trace batch into a base64 string.
pub fn encode_trace_inline_meta<T: Timestamp + Codec64>(batch: &BlobTraceBatchPart<T>) -> String {
    let (format, format_metadata) = match &batch.updates {
        // For the legacy Row format, we only write it as `ParquetKvtd`.
        BlobTraceUpdates::Row(_) => (ProtoBatchFormat::ParquetKvtd, None),
        // For the newer structured format we track some metadata about the version of the format.
        BlobTraceUpdates::Both { .. } => {
            let metadata = ProtoFormatMetadata::StructuredMigration(2);
            (ProtoBatchFormat::ParquetStructured, Some(metadata))
        }
    };

    let inline = ProtoBatchPartInline {
        format: format.into(),
        desc: Some((&batch.desc).into()),
        index: batch.index,
        format_metadata,
    };
    let inline_encoded = inline.encode_to_vec();
    base64::encode(inline_encoded)
}

/// Decodes the inline metadata for a trace batch from a base64 string.
pub fn decode_trace_inline_meta(
    inline_base64: Option<&String>,
) -> Result<(ProtoBatchFormat, ProtoBatchPartInline), Error> {
    let inline_base64 = inline_base64.ok_or("missing batch metadata")?;
    let inline_encoded = base64::decode(inline_base64).map_err(|err| err.to_string())?;
    let inline = ProtoBatchPartInline::decode(&*inline_encoded).map_err(|err| err.to_string())?;
    let format = ProtoBatchFormat::try_from(inline.format)
        .map_err(|_| Error::from(format!("unknown format: {}", inline.format)))?;
    Ok((format, inline))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::error::Error;
    use crate::indexed::columnar::ColumnarRecordsBuilder;
    use crate::mem::{MemBlob, MemBlobConfig};
    use crate::metrics::ColumnarMetrics;
    use crate::workload::DataGenerator;

    use super::*;

    fn update_with_key(ts: u64, key: &'static str) -> ((Vec<u8>, Vec<u8>), u64, i64) {
        ((key.into(), "".into()), ts, 1)
    }

    fn u64_desc(lower: u64, upper: u64) -> Description<u64> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(0),
        )
    }

    fn batch_meta(lower: u64, upper: u64) -> TraceBatchMeta {
        TraceBatchMeta {
            keys: vec![],
            format: ProtoBatchFormat::Unknown,
            desc: u64_desc(lower, upper),
            level: 1,
            size_bytes: 0,
        }
    }

    fn u64_desc_since(lower: u64, upper: u64, since: u64) -> Description<u64> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(since),
        )
    }

    fn columnar_records(updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> BlobTraceUpdates {
        let mut builder = ColumnarRecordsBuilder::default();
        for ((k, v), t, d) in updates {
            assert!(builder.push(((&k, &v), Codec64::encode(&t), Codec64::encode(&d))));
        }
        let updates = builder.finish(&ColumnarMetrics::disconnected());
        BlobTraceUpdates::Row(updates)
    }

    #[mz_ore::test]
    fn trace_batch_validate() {
        // Normal case
        let b = BlobTraceBatchPart {
            desc: u64_desc(0, 2),
            index: 0,
            updates: columnar_records(vec![update_with_key(0, "0"), update_with_key(1, "1")]),
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b = BlobTraceBatchPart {
            desc: u64_desc(0, 2),
            index: 0,
            updates: columnar_records(vec![]),
        };
        assert_eq!(b.validate(), Ok(()));

        // Invalid desc
        let b = BlobTraceBatchPart {
            desc: u64_desc(2, 0),
            index: 0,
            updates: columnar_records(vec![]),
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid desc: Description { lower: Antichain { elements: [2] }, upper: Antichain { elements: [0] }, since: Antichain { elements: [0] } }"
            ))
        );

        // Empty desc
        let b = BlobTraceBatchPart {
            desc: u64_desc(0, 0),
            index: 0,
            updates: columnar_records(vec![]),
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [0] }, since: Antichain { elements: [0] } }"
            ))
        );

        // Update "before" desc
        let b = BlobTraceBatchPart {
            desc: u64_desc(1, 2),
            index: 0,
            updates: columnar_records(vec![update_with_key(0, "0")]),
        };
        assert_eq!(b.validate(), Err(Error::from("timestamp 0 is less than the batch lower: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }")));

        // Update "after" desc
        let b = BlobTraceBatchPart {
            desc: u64_desc(1, 2),
            index: 0,
            updates: columnar_records(vec![update_with_key(2, "0")]),
        };
        assert_eq!(b.validate(), Err(Error::from("timestamp 2 is greater than or equal to the batch upper: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }")));

        // Normal case: update "after" desc and within since
        let b = BlobTraceBatchPart {
            desc: u64_desc_since(1, 2, 4),
            index: 0,
            updates: columnar_records(vec![update_with_key(2, "0")]),
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case: update "after" desc and at since
        let b = BlobTraceBatchPart {
            desc: u64_desc_since(1, 2, 4),
            index: 0,
            updates: columnar_records(vec![update_with_key(4, "0")]),
        };
        assert_eq!(b.validate(), Ok(()));

        // Update "after" desc since
        let b = BlobTraceBatchPart {
            desc: u64_desc_since(1, 2, 4),
            index: 0,
            updates: columnar_records(vec![update_with_key(5, "0")]),
        };
        assert_eq!(b.validate(), Ok(()));

        // Invalid update
        let b = BlobTraceBatchPart {
            desc: u64_desc(0, 1),
            index: 0,
            updates: columnar_records(vec![(("0".into(), "0".into()), 0, 0)]),
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("update with 0 diff: ((\"0\", \"0\"), 0, 0)"))
        );
    }

    #[mz_ore::test]
    fn trace_batch_meta_validate() {
        // Normal case
        let b = batch_meta(0, 1);
        assert_eq!(b.validate(), Ok(()));

        // Empty interval
        let b = batch_meta(0, 0);
        assert_eq!(b.validate(),
            Err(Error::from(
                "invalid desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [0] }, since: Antichain { elements: [0] } }"
            )),
        );

        // Invalid interval
        let b = batch_meta(2, 0);
        assert_eq!(b.validate(),
            Err(Error::from(
                "invalid desc: Description { lower: Antichain { elements: [2] }, upper: Antichain { elements: [0] }, since: Antichain { elements: [0] } }"
            )),
        );
    }

    async fn expect_set_trace_batch<T: Timestamp + Codec64>(
        blob: &dyn Blob,
        key: &str,
        batch: &BlobTraceBatchPart<T>,
    ) -> u64 {
        let mut val = Vec::new();
        let metrics = ColumnarMetrics::disconnected();
        let config = EncodingConfig::default();
        batch.encode(&mut val, &metrics, &config);
        let val = Bytes::from(val);
        let val_len = u64::cast_from(val.len());
        blob.set(key, val).await.expect("failed to set trace batch");
        val_len
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn trace_batch_meta_validate_data() -> Result<(), Error> {
        let metrics = ColumnarMetrics::disconnected();
        let blob = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let format = ProtoBatchFormat::ParquetKvtd;

        let batch_desc = u64_desc_since(0, 3, 0);
        let batch0 = BlobTraceBatchPart {
            desc: batch_desc.clone(),
            index: 0,
            updates: columnar_records(vec![
                (("k".as_bytes().to_vec(), "v".as_bytes().to_vec()), 2, 1),
                (("k3".as_bytes().to_vec(), "v3".as_bytes().to_vec()), 2, 1),
            ]),
        };
        let batch1 = BlobTraceBatchPart {
            desc: batch_desc.clone(),
            index: 1,
            updates: columnar_records(vec![
                (("k4".as_bytes().to_vec(), "v4".as_bytes().to_vec()), 2, 1),
                (("k5".as_bytes().to_vec(), "v5".as_bytes().to_vec()), 2, 1),
            ]),
        };

        let batch0_size_bytes = expect_set_trace_batch(blob.as_ref(), "b0", &batch0).await;
        let batch1_size_bytes = expect_set_trace_batch(blob.as_ref(), "b1", &batch1).await;
        let size_bytes = batch0_size_bytes + batch1_size_bytes;
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1".into()],
            format,
            desc: batch_desc.clone(),
            level: 0,
            size_bytes,
        };

        // Normal case:
        assert_eq!(
            batch_meta.validate_data(blob.as_ref(), &metrics).await,
            Ok(())
        );

        // Incorrect desc
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1".into()],
            format,
            desc: u64_desc_since(1, 3, 0),
            level: 0,
            size_bytes,
        };
        assert_eq!(batch_meta.validate_data(blob.as_ref(), &metrics).await, Err(Error::from("invalid trace batch part desc expected Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } } got Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));
        // Key with no corresponding batch part
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1".into(), "b2".into()],
            format,
            desc: batch_desc.clone(),
            level: 0,
            size_bytes,
        };
        assert_eq!(
            batch_meta.validate_data(blob.as_ref(), &metrics).await,
            Err(Error::from("no blob for trace batch at key: b2"))
        );
        // Batch parts not in index order
        let batch_meta = TraceBatchMeta {
            keys: vec!["b1".into(), "b0".into()],
            format,
            desc: batch_desc,
            level: 0,
            size_bytes,
        };
        assert_eq!(
            batch_meta.validate_data(blob.as_ref(), &metrics).await,
            Err(Error::from(
                "invalid index for blob trace batch part at key b1 expected 0 got 1"
            ))
        );

        Ok(())
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn encoded_batch_sizes() {
        fn sizes(data: DataGenerator) -> usize {
            let metrics = ColumnarMetrics::disconnected();
            let config = EncodingConfig::default();
            let updates: Vec<_> = data.batches().collect();
            let updates = BlobTraceUpdates::Row(ColumnarRecords::concat(&updates, &metrics));
            let trace = BlobTraceBatchPart {
                desc: Description::new(
                    Antichain::from_elem(0u64),
                    Antichain::new(),
                    Antichain::from_elem(0u64),
                ),
                index: 0,
                updates,
            };
            let mut trace_buf = Vec::new();
            trace.encode(&mut trace_buf, &metrics, &config);
            trace_buf.len()
        }

        let record_size_bytes = DataGenerator::default().record_size_bytes;
        // Print all the sizes into one assert so we only have to update one
        // place if sizes change.
        assert_eq!(
            format!(
                "1/1={:?} 25/1={:?} 1000/1={:?} 1000/100={:?}",
                sizes(DataGenerator::new(1, record_size_bytes, 1)),
                sizes(DataGenerator::new(25, record_size_bytes, 25)),
                sizes(DataGenerator::new(1_000, record_size_bytes, 1_000)),
                sizes(DataGenerator::new(1_000, record_size_bytes, 1_000 / 100)),
            ),
            "1/1=951 25/1=2702 1000/1=72943 1000/100=72943"
        );
    }
}
