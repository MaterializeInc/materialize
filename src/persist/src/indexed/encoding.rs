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

use std::fmt::{self, Debug};
use std::io::Cursor;
use std::marker::PhantomData;

use bytes::BufMut;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_persist_types::{Codec, Codec64};
use prost::Message;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::Error;
use crate::gen::persist::{
    proto_batch_inline, ProtoBatchFormat, ProtoBatchInline, ProtoTraceBatchMeta,
    ProtoTraceBatchPartInline, ProtoU64Antichain, ProtoU64Description,
};
use crate::indexed::cache::{BlobCache, CacheHint};
use crate::indexed::columnar::parquet::{decode_trace_parquet, encode_trace_parquet};
use crate::indexed::columnar::ColumnarRecords;
use crate::location::Blob;

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
#[derive(Clone, Debug)]
pub struct BlobTraceBatchPart {
    /// Which updates are included in this batch.
    ///
    /// There may be other parts for the batch that also contain updates within
    /// the specified [lower, upper) range.
    pub desc: Description<u64>,
    /// Index of this part in the list of parts that form the batch.
    pub index: u64,
    /// The updates themselves.
    pub updates: Vec<ColumnarRecords>,
}

impl TraceBatchMeta {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if the equal case (an empty desc) is
        // useful/harmful. Feel free to make this a less_than if empty descs end
        // up making sense.
        if PartialOrder::less_equal(self.desc.upper(), &self.desc.lower()) {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }

        Ok(())
    }

    /// Assert that all of the [BlobTraceBatchPart]'s obey the required invariants.
    pub async fn validate_data<B: Blob + Send + Sync + 'static>(
        &self,
        cache: &BlobCache<B>,
    ) -> Result<(), Error> {
        let mut batches = vec![];
        for (idx, key) in self.keys.iter().enumerate() {
            let batch = cache
                .get_trace_batch_async(key, CacheHint::NeverAdd)
                .await?;
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
            .flat_map(|batch| batch.updates.iter().flat_map(|u| u.iter()))
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

impl BlobTraceBatchPart {
    /// Asserts the documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if the equal case (an empty desc) is
        // useful/harmful. Feel free to make this a less_than if empty descs end
        // up making sense.
        if PartialOrder::less_equal(self.desc.upper(), &self.desc.lower()) {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }

        for update in self.updates.iter().flat_map(|u| u.iter()) {
            let ((_key, _val), ts, diff) = update;
            // TODO: Don't assume ts and diff are a u64 and a i64, take T and D
            // type params instead.
            let ts: u64 = Codec64::decode(ts);
            let diff: i64 = Codec64::decode(diff);

            // Check ts against desc.
            if !self.desc.lower().less_equal(&ts) {
                return Err(format!(
                    "timestamp {} is less than the batch lower: {:?}",
                    ts, self.desc
                )
                .into());
            }

            if PartialOrder::less_than(self.desc.since(), self.desc.upper()) {
                if self.desc.upper().less_equal(&ts) {
                    return Err(format!(
                        "timestamp {} is greater than or equal to the batch upper: {:?}",
                        ts, self.desc
                    )
                    .into());
                }
            } else if self.desc.since().less_than(&ts) {
                return Err(format!(
                    "timestamp {} is greater than the batch since: {:?}",
                    ts, self.desc,
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
}

// BlobTraceBatchPart doesn't really need to implement Codec (it's never stored as a
// key or value in a persisted record) but it's nice to have a common interface
// for this.
impl Codec for BlobTraceBatchPart {
    fn codec_name() -> String {
        "parquet[TraceBatch]".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        encode_trace_parquet(&mut buf.writer(), self).expect("writes to BufMut are infallible");
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        decode_trace_parquet(&mut Cursor::new(&buf)).map_err(|err| err.to_string())
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
                (PrettyBytes(&k), PrettyBytes(&v)),
                T::decode(*ts),
                D::decode(*diff),
            ),
            f,
        )
    }
}

impl From<ProtoTraceBatchMeta> for TraceBatchMeta {
    fn from(x: ProtoTraceBatchMeta) -> Self {
        TraceBatchMeta {
            format: x.format(),
            keys: x.keys.clone(),
            desc: x.desc.map_or_else(
                || {
                    Description::new(
                        Antichain::from_elem(u64::minimum()),
                        Antichain::from_elem(u64::minimum()),
                        Antichain::from_elem(u64::minimum()),
                    )
                },
                |x| x.into(),
            ),
            level: x.level,
            size_bytes: x.size_bytes,
        }
    }
}

impl From<ProtoU64Description> for Description<u64> {
    fn from(x: ProtoU64Description) -> Self {
        Description::new(
            x.lower
                .map_or_else(|| Antichain::from_elem(u64::minimum()), |x| x.into()),
            x.upper
                .map_or_else(|| Antichain::from_elem(u64::minimum()), |x| x.into()),
            x.since
                .map_or_else(|| Antichain::from_elem(u64::minimum()), |x| x.into()),
        )
    }
}

impl From<ProtoU64Antichain> for Antichain<u64> {
    fn from(x: ProtoU64Antichain) -> Self {
        Antichain::from(x.elements)
    }
}

impl From<&TraceBatchMeta> for ProtoTraceBatchMeta {
    fn from(x: &TraceBatchMeta) -> Self {
        ProtoTraceBatchMeta {
            keys: x.keys.clone(),
            format: x.format.into(),
            desc: Some((&x.desc).into()),
            level: x.level,
            size_bytes: x.size_bytes,
        }
    }
}

impl From<&Antichain<u64>> for ProtoU64Antichain {
    fn from(x: &Antichain<u64>) -> Self {
        ProtoU64Antichain {
            elements: x.elements().to_vec(),
        }
    }
}

impl From<&Description<u64>> for ProtoU64Description {
    fn from(x: &Description<u64>) -> Self {
        ProtoU64Description {
            lower: Some(x.lower().into()),
            upper: Some(x.upper().into()),
            since: Some(x.since().into()),
        }
    }
}

/// Encodes the inline metadata for a trace batch into a base64 string.
pub fn encode_trace_inline_meta(batch: &BlobTraceBatchPart, format: ProtoBatchFormat) -> String {
    let inline = ProtoBatchInline {
        batch_type: Some(proto_batch_inline::BatchType::Trace(
            ProtoTraceBatchPartInline {
                format: format.into(),
                desc: Some((&batch.desc).into()),
                index: batch.index,
            },
        )),
    };
    let inline_encoded = inline.encode_to_vec();
    base64::encode(inline_encoded)
}

/// Decodes the inline metadata for a trace batch from a base64 string.
pub fn decode_trace_inline_meta(
    inline_base64: Option<&String>,
) -> Result<(ProtoBatchFormat, ProtoTraceBatchPartInline), Error> {
    let inline_base64 = inline_base64.ok_or("missing batch metadata")?;
    let inline_encoded = base64::decode(&inline_base64).map_err(|err| err.to_string())?;
    let inline = ProtoBatchInline::decode(&*inline_encoded).map_err(|err| err.to_string())?;
    match inline.batch_type {
        Some(proto_batch_inline::BatchType::Trace(x)) => {
            let format = ProtoBatchFormat::from_i32(x.format)
                .ok_or_else(|| Error::from(format!("unknown format: {}", x.format)))?;
            Ok((format, x))
        }
        x => return Err(format!("incorrect batch type: {:?}", x).into()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::error::Error;
    use crate::indexed::columnar::ColumnarRecordsVec;
    use crate::indexed::metrics::Metrics;
    use crate::mem::{MemBlob, MemBlobConfig};
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

    fn columnar_records(updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> Vec<ColumnarRecords> {
        updates.iter().collect::<ColumnarRecordsVec>().into_inner()
    }

    #[test]
    fn trace_batch_validate() {
        // Normal case
        let b = BlobTraceBatchPart {
            desc: u64_desc(0, 2),
            index: 0,
            updates: columnar_records(vec![update_with_key(0, "0"), update_with_key(1, "1")]),
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: BlobTraceBatchPart = BlobTraceBatchPart {
            desc: u64_desc(0, 2),
            index: 0,
            updates: columnar_records(vec![]),
        };
        assert_eq!(b.validate(), Ok(()));

        // Invalid desc
        let b: BlobTraceBatchPart = BlobTraceBatchPart {
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
        let b: BlobTraceBatchPart = BlobTraceBatchPart {
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
        assert_eq!(b.validate(), Err(Error::from("timestamp 5 is greater than the batch since: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [4] } }")));

        // Invalid update
        let b: BlobTraceBatchPart = BlobTraceBatchPart {
            desc: u64_desc(0, 1),
            index: 0,
            updates: columnar_records(vec![(("0".into(), "0".into()), 0, 0)]),
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("update with 0 diff: ((\"0\", \"0\"), 0, 0)"))
        );
    }

    #[test]
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

    #[tokio::test]
    async fn trace_batch_meta_validate_data() -> Result<(), Error> {
        let blob = BlobCache::new(
            Arc::new(Metrics::default()),
            MemBlob::open(MemBlobConfig::default()),
            None,
        );
        let format = ProtoBatchFormat::ParquetKvtd;

        let batch_desc = u64_desc_since(0, 3, 0);
        let batch0 = BlobTraceBatchPart {
            desc: batch_desc.clone(),
            index: 0,
            updates: vec![
                (("k".as_bytes(), "v".as_bytes()), 2, 1),
                (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
            ]
            .iter()
            .collect::<ColumnarRecordsVec>()
            .into_inner(),
        };
        let batch1 = BlobTraceBatchPart {
            desc: batch_desc.clone(),
            index: 1,
            updates: vec![
                (("k4".as_bytes(), "v4".as_bytes()), 2, 1),
                (("k5".as_bytes(), "v5".as_bytes()), 2, 1),
            ]
            .iter()
            .collect::<ColumnarRecordsVec>()
            .into_inner(),
        };

        let batch0_size_bytes = blob.set_trace_batch("b0".into(), batch0, format).await?;
        let batch1_size_bytes = blob.set_trace_batch("b1".into(), batch1, format).await?;
        let size_bytes = batch0_size_bytes + batch1_size_bytes;
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1".into()],
            format,
            desc: batch_desc.clone(),
            level: 0,
            size_bytes,
        };

        // Normal case:
        assert_eq!(batch_meta.validate_data(&blob).await, Ok(()));

        // Incorrect desc
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1".into()],
            format,
            desc: u64_desc_since(1, 3, 0),
            level: 0,
            size_bytes,
        };
        assert_eq!(batch_meta.validate_data(&blob).await, Err(Error::from("invalid trace batch part desc expected Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } } got Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));
        // Key with no corresponding batch part
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1".into(), "b2".into()],
            format,
            desc: batch_desc.clone(),
            level: 0,
            size_bytes,
        };
        assert_eq!(
            batch_meta.validate_data(&blob).await,
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
            batch_meta.validate_data(&blob).await,
            Err(Error::from(
                "invalid index for blob trace batch part at key b1 expected 0 got 1"
            ))
        );

        Ok(())
    }

    #[test]
    fn encoded_batch_sizes() {
        fn sizes(data: DataGenerator) -> usize {
            let trace = BlobTraceBatchPart {
                desc: Description::new(
                    Antichain::from_elem(0),
                    Antichain::from_elem(1),
                    Antichain::from_elem(0),
                ),
                index: 0,
                updates: data.batches().collect(),
            };
            let mut trace_buf = Vec::new();
            trace.encode(&mut trace_buf);
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
            "1/1=1031 25/1=2782 1000/1=73026 1000/100=113071"
        );
    }
}
