// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::marker::PhantomData;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist_types::{Codec, Codec64};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use prost::Message;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use uuid::Uuid;

use crate::error::CodecMismatch;
use crate::r#impl::paths::PartialBlobKey;
use crate::r#impl::state::{
    HollowBatch, ProtoHollowBatch, ProtoHollowBatchPart, ProtoReader, ProtoSnapshotSplit,
    ProtoStateRollup, ProtoTrace, ProtoU64Antichain, ProtoU64Description, ProtoWriter,
    ReadCapability, State, StateCollections, WriterState,
};
use crate::r#impl::trace::Trace;
use crate::read::{ReaderId, SnapshotSplit};
use crate::{ShardId, WriterId};

pub(crate) fn parse_id(id_prefix: char, id_type: &str, encoded: &str) -> Result<[u8; 16], String> {
    let uuid_encoded = match encoded.strip_prefix(id_prefix) {
        Some(x) => x,
        None => return Err(format!("invalid {} {}: incorrect prefix", id_type, encoded)),
    };
    let uuid = Uuid::parse_str(&uuid_encoded)
        .map_err(|err| format!("invalid {} {}: {}", id_type, encoded, err))?;
    Ok(*uuid.as_bytes())
}

impl RustType<String> for ShardId {
    fn into_proto(&self) -> String {
        self.to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        match parse_id('s', "ShardId", &proto) {
            Ok(x) => Ok(ShardId(x)),
            Err(_) => Err(TryFromProtoError::InvalidShardId(proto)),
        }
    }
}

impl RustType<String> for ReaderId {
    fn into_proto(&self) -> String {
        self.to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        match parse_id('r', "ReaderId", &proto) {
            Ok(x) => Ok(ReaderId(x)),
            Err(_) => Err(TryFromProtoError::InvalidShardId(proto)),
        }
    }
}

impl RustType<String> for WriterId {
    fn into_proto(&self) -> String {
        self.to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        match parse_id('w', "WriterId", &proto) {
            Ok(x) => Ok(WriterId(x)),
            Err(_) => Err(TryFromProtoError::InvalidShardId(proto)),
        }
    }
}

impl RustType<String> for PartialBlobKey {
    fn into_proto(&self) -> String {
        self.0.clone()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        Ok(PartialBlobKey(proto))
    }
}

impl<K, V, T, D> State<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
    pub fn decode(buf: &[u8]) -> Result<Self, CodecMismatch> {
        let proto = ProtoStateRollup::decode(buf)
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");
        Self::try_from(proto).expect("internal error: invalid encoded state")
    }
}

impl<K, V, T, D> Codec for State<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
    fn codec_name() -> String {
        "proto[State]".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let proto = ProtoStateRollup::decode(buf).map_err(|err| err.to_string())?;
        // This match goes away when we do incremental state.
        match State::try_from(proto) {
            Ok(Ok(x)) => Ok(x),
            Ok(Err(err)) => Err(err.to_string()),
            Err(err) => Err(err.to_string()),
        }
    }
}

impl<K, V, T, D> RustType<ProtoStateRollup> for State<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
    fn into_proto(&self) -> ProtoStateRollup {
        ProtoStateRollup {
            shard_id: self.shard_id.into_proto(),
            seqno: self.seqno.into_proto(),
            key_codec: K::codec_name(),
            val_codec: V::codec_name(),
            ts_codec: T::codec_name(),
            diff_codec: D::codec_name(),
            readers: self
                .collections
                .readers
                .iter()
                .map(|(id, cap)| ProtoReader {
                    reader_id: id.into_proto(),
                    since: Some(cap.since.into_proto()),
                    seqno: cap.seqno.into_proto(),
                })
                .collect(),
            writers: self
                .collections
                .writers
                .iter()
                .map(|(id, writer)| ProtoWriter {
                    writer_id: id.into_proto(),
                    last_heartbeat_timestamp_ms: writer.last_heartbeat_timestamp_ms,
                })
                .collect(),
            trace: Some(self.collections.trace.into_proto()),
        }
    }

    fn from_proto(proto: ProtoStateRollup) -> Result<Self, TryFromProtoError> {
        match State::try_from(proto) {
            Ok(Ok(x)) => Ok(x),
            Ok(Err(err)) => Err(TryFromProtoError::CodecMismatch(err.to_string())),
            Err(err) => Err(err),
        }
    }
}

impl<K, V, T, D> State<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
    fn try_from(x: ProtoStateRollup) -> Result<Result<Self, CodecMismatch>, TryFromProtoError> {
        if K::codec_name() != x.key_codec
            || V::codec_name() != x.val_codec
            || T::codec_name() != x.ts_codec
            || D::codec_name() != x.diff_codec
        {
            return Ok(Err(CodecMismatch {
                requested: (
                    K::codec_name(),
                    V::codec_name(),
                    T::codec_name(),
                    D::codec_name(),
                ),
                actual: (x.key_codec, x.val_codec, x.ts_codec, x.diff_codec),
            }));
        }

        let mut readers = HashMap::with_capacity(x.readers.len());
        for proto in x.readers {
            let reader_id = proto.reader_id.into_rust()?;
            let cap = ReadCapability {
                since: proto.since.into_rust_if_some("since")?,
                seqno: proto.seqno.into_rust()?,
            };
            readers.insert(reader_id, cap);
        }
        let mut writers = HashMap::with_capacity(x.writers.len());
        for proto in x.writers {
            let writer_id = proto.writer_id.into_rust()?;
            writers.insert(
                writer_id,
                WriterState {
                    last_heartbeat_timestamp_ms: proto.last_heartbeat_timestamp_ms,
                },
            );
        }
        let collections = StateCollections {
            readers,
            writers,
            trace: x.trace.into_rust_if_some("trace")?,
        };
        Ok(Ok(State {
            shard_id: x.shard_id.into_rust()?,
            seqno: x.seqno.into_rust()?,
            collections,
            _phantom: PhantomData,
        }))
    }
}

impl<T: Timestamp + Lattice + Codec64> RustType<ProtoTrace> for Trace<T> {
    fn into_proto(&self) -> ProtoTrace {
        let mut spine = Vec::new();
        self.map_batches(|b| {
            spine.push(b.into_proto());
        });
        ProtoTrace {
            since: Some(self.since().into_proto()),
            spine,
        }
    }

    fn from_proto(proto: ProtoTrace) -> Result<Self, TryFromProtoError> {
        let mut ret = Trace::default();
        ret.downgrade_since(proto.since.into_rust_if_some("since")?);
        for batch in proto.spine.into_iter() {
            let batch: HollowBatch<T> = batch.into_rust()?;
            if PartialOrder::less_than(ret.since(), batch.desc.since()) {
                return Err(TryFromProtoError::InvalidPersistState(format!(
                    "invalid ProtoTrace: the spine's since {:?} was less than a batch's since {:?}",
                    ret.since(),
                    batch.desc.since()
                )));
            }
            // We could perhaps more directly serialize and rehydrate the
            // internals of the Spine, but this is nice because it insulates
            // us against changes in the Spine logic. The current logic has
            // turned out to be relatively expensive in practice, but as we
            // tune things (especially when we add inc state) the rate of
            // this deserialization should go down. Revisit as necessary.
            ret.push_batch(batch);
        }
        let _ = ret.take_merge_reqs();
        Ok(ret)
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoHollowBatch> for HollowBatch<T> {
    fn into_proto(&self) -> ProtoHollowBatch {
        ProtoHollowBatch {
            desc: Some(self.desc.into_proto()),
            keys: self.keys.into_proto(),
            len: self.len.into_proto(),
        }
    }

    fn from_proto(proto: ProtoHollowBatch) -> Result<Self, TryFromProtoError> {
        Ok(HollowBatch {
            desc: proto.desc.into_rust_if_some("desc")?,
            keys: proto.keys.into_rust()?,
            len: proto.len.into_rust()?,
        })
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoU64Description> for Description<T> {
    fn into_proto(&self) -> ProtoU64Description {
        ProtoU64Description {
            lower: Some(self.lower().into_proto()),
            upper: Some(self.upper().into_proto()),
            since: Some(self.since().into_proto()),
        }
    }

    fn from_proto(proto: ProtoU64Description) -> Result<Self, TryFromProtoError> {
        Ok(Description::new(
            proto.lower.into_rust_if_some("lower")?,
            proto.upper.into_rust_if_some("upper")?,
            proto.since.into_rust_if_some("since")?,
        ))
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoU64Antichain> for Antichain<T> {
    fn into_proto(&self) -> ProtoU64Antichain {
        ProtoU64Antichain {
            elements: self
                .elements()
                .iter()
                .map(|x| i64::from_le_bytes(T::encode(x)))
                .collect(),
        }
    }

    fn from_proto(proto: ProtoU64Antichain) -> Result<Self, TryFromProtoError> {
        let elements = proto
            .elements
            .iter()
            .map(|x| T::decode(x.to_le_bytes()))
            .collect::<Vec<_>>();
        Ok(Antichain::from(elements))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SerdeSnapshotSplit(Vec<u8>);

impl<T: Timestamp + Codec64> From<SnapshotSplit<T>> for SerdeSnapshotSplit {
    fn from(x: SnapshotSplit<T>) -> Self {
        SerdeSnapshotSplit(x.into_proto().encode_to_vec())
    }
}

impl<T: Timestamp + Codec64> From<SerdeSnapshotSplit> for SnapshotSplit<T> {
    fn from(x: SerdeSnapshotSplit) -> Self {
        let proto = ProtoSnapshotSplit::decode(x.0.as_slice())
            .expect("internal error: invalid snapshot split");
        proto
            .into_rust()
            .expect("internal error: invalid snapshot split")
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoSnapshotSplit> for SnapshotSplit<T> {
    fn into_proto(&self) -> ProtoSnapshotSplit {
        ProtoSnapshotSplit {
            shard_id: self.shard_id.into_proto(),
            as_of: Some(self.as_of.into_proto()),
            batches: self
                .batches
                .iter()
                .map(|(key, desc)| ProtoHollowBatchPart {
                    desc: Some(desc.into_proto()),
                    key: key.into_proto(),
                })
                .collect(),
        }
    }

    fn from_proto(proto: ProtoSnapshotSplit) -> Result<Self, TryFromProtoError> {
        let mut batches = Vec::new();
        for batch in proto.batches.into_iter() {
            let desc = batch.desc.into_rust_if_some("desc")?;
            batches.push((PartialBlobKey(batch.key), desc));
        }
        Ok(SnapshotSplit {
            shard_id: proto.shard_id.into_rust()?,
            as_of: proto.as_of.into_rust_if_some("as_of")?,
            batches,
        })
    }
}
