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
use semver::Version;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use uuid::Uuid;

use crate::error::CodecMismatch;
use crate::r#impl::paths::PartialBlobKey;
use crate::r#impl::state::proto_hollow_batch_reader_metadata;
use crate::r#impl::state::{
    HollowBatch, ProtoHollowBatch, ProtoHollowBatchReaderMetadata, ProtoReadEnrichedHollowBatch,
    ProtoReaderState, ProtoStateRollup, ProtoTrace, ProtoU64Antichain, ProtoU64Description,
    ProtoWriterState, ReaderState, State, StateCollections, WriterState,
};
use crate::r#impl::trace::Trace;
use crate::read::{HollowBatchReaderMetadata, ReaderEnrichedHollowBatch, ReaderId};
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
    pub fn decode(build_version: &Version, buf: &[u8]) -> Result<Self, CodecMismatch> {
        let proto = ProtoStateRollup::decode(buf)
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");
        let state = Self::try_from(proto).expect("internal error: invalid encoded state")?;

        // If persist gets some encoded ProtoState from the future (e.g. two
        // versions of code are running simultaneously against the same shard),
        // it might have a field that the current code doesn't know about. This
        // would be silently discarded at proto decode time. Unknown Fields [1]
        // are a tool we can use in the future to help deal with this, but in
        // the short-term, it's best to keep the persist read-modify-CaS loop
        // simple for as long as we can get away with it (i.e. until we have to
        // offer the ability to do rollbacks).
        //
        // [1]:
        //     https://developers.google.com/protocol-buffers/docs/proto3#unknowns
        //
        // To detect the bad situation and disallow it, we tag every version of
        // state written to consensus with the version of code used to encode
        // it. Then at decode time, we're able to compare the current version
        // against any we receive and assert as necessary.
        //
        // Initially we reject any version from the future (no forward
        // compatibility, most conservative but easiest to reason about) but
        // allow any from the past (permanent backward compatibility). If/when
        // we support deploy rollbacks and rolling upgrades, we can adjust this
        // assert as necessary to reflect the policy (e.g. by adding some window
        // of X allowed versions of forward compatibility, computed by comparing
        // semvers).
        //
        // We could do the same for blob data, but it shouldn't be necessary.
        // Any blob data we read is going to be because we fetched it using a
        // pointer stored in some persist state. If we can handle the state, we
        // can handle the blobs it references, too.
        if build_version < &state.applier_version {
            panic!(
                "{} received persist state from the future {}",
                build_version, state.applier_version
            );
        }

        Ok(state)
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
            applier_version: self.applier_version.to_string(),
            shard_id: self.shard_id.into_proto(),
            seqno: self.seqno.into_proto(),
            key_codec: K::codec_name(),
            val_codec: V::codec_name(),
            ts_codec: T::codec_name(),
            diff_codec: D::codec_name(),
            last_gc_req: self.collections.last_gc_req.into_proto(),
            readers: self
                .collections
                .readers
                .iter()
                .map(|(id, cap)| ProtoReaderState {
                    reader_id: id.into_proto(),
                    since: Some(cap.since.into_proto()),
                    seqno: cap.seqno.into_proto(),
                    last_heartbeat_timestamp_ms: cap.last_heartbeat_timestamp_ms,
                })
                .collect(),
            writers: self
                .collections
                .writers
                .iter()
                .map(|(id, writer)| ProtoWriterState {
                    writer_id: id.into_proto(),
                    last_heartbeat_timestamp_ms: writer.last_heartbeat_timestamp_ms,
                    lease_duration_ms: writer.lease_duration_ms,
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

        let applier_version = if x.applier_version.is_empty() {
            // Backward compatibility with versions of ProtoState before we set
            // this field: if it's missing (empty), assume an infinitely old
            // version.
            semver::Version::new(0, 0, 0)
        } else {
            semver::Version::parse(&x.applier_version).map_err(|err| {
                TryFromProtoError::InvalidSemverVersion(format!(
                    "invalid applier_version {}: {}",
                    x.applier_version, err
                ))
            })?
        };

        let mut readers = HashMap::with_capacity(x.readers.len());
        for proto in x.readers {
            let reader_id = proto.reader_id.into_rust()?;
            let cap = ReaderState {
                since: proto.since.into_rust_if_some("since")?,
                seqno: proto.seqno.into_rust()?,
                last_heartbeat_timestamp_ms: proto.last_heartbeat_timestamp_ms,
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
                    lease_duration_ms: proto.lease_duration_ms,
                },
            );
        }
        let collections = StateCollections {
            last_gc_req: x.last_gc_req.into_rust()?,
            readers,
            writers,
            trace: x.trace.into_rust_if_some("trace")?,
        };
        Ok(Ok(State {
            applier_version,
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
        ret.downgrade_since(&proto.since.into_rust_if_some("since")?);
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

impl<T: Timestamp + Codec64> RustType<ProtoHollowBatchReaderMetadata>
    for HollowBatchReaderMetadata<T>
{
    fn into_proto(&self) -> ProtoHollowBatchReaderMetadata {
        use proto_hollow_batch_reader_metadata::*;
        ProtoHollowBatchReaderMetadata {
            kind: Some(match self {
                HollowBatchReaderMetadata::Snapshot { as_of } => {
                    Kind::Snapshot(ProtoHollowBatchReaderMetadataSnapshot {
                        as_of: Some(as_of.into_proto()),
                    })
                }
                HollowBatchReaderMetadata::Listen {
                    as_of,
                    until,
                    since,
                } => Kind::Listen(ProtoHollowBatchReaderMetadataListen {
                    as_of: Some(as_of.into_proto()),
                    until: Some(until.into_proto()),
                    since: Some(since.into_proto()),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoHollowBatchReaderMetadata) -> Result<Self, TryFromProtoError> {
        use proto_hollow_batch_reader_metadata::Kind::*;
        Ok(match proto.kind {
            Some(Snapshot(snapshot)) => HollowBatchReaderMetadata::Snapshot {
                as_of: snapshot
                    .as_of
                    .into_rust_if_some("ProtoHollowBatchReaderMetadata::Kind::Snapshot::as_of")?,
            },
            Some(Listen(listen)) => HollowBatchReaderMetadata::Listen {
                as_of: listen
                    .as_of
                    .into_rust_if_some("ProtoHollowBatchReaderMetadata::Kind::Listen::as_of")?,
                until: listen
                    .until
                    .into_rust_if_some("ProtoHollowBatchReaderMetadata::Kind::Listen::until")?,
                since: listen
                    .since
                    .into_rust_if_some("ProtoHollowBatchReaderMetadata::Kind::Listen::since")?,
            },
            None => {
                return Err(TryFromProtoError::missing_field(
                    "ProtoHollowBatchReaderMetadata::Kind",
                ))
            }
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SerdeReaderEnrichedHollowBatch(Vec<u8>);

impl<T: Timestamp + Codec64> From<ReaderEnrichedHollowBatch<T>> for SerdeReaderEnrichedHollowBatch {
    fn from(x: ReaderEnrichedHollowBatch<T>) -> Self {
        SerdeReaderEnrichedHollowBatch(x.into_proto().encode_to_vec())
    }
}

impl<T: Timestamp + Codec64> From<SerdeReaderEnrichedHollowBatch> for ReaderEnrichedHollowBatch<T> {
    fn from(x: SerdeReaderEnrichedHollowBatch) -> Self {
        let proto = ProtoReadEnrichedHollowBatch::decode(x.0.as_slice())
            .expect("internal error: invalid snapshot split");
        proto
            .into_rust()
            .expect("internal error: invalid snapshot split")
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoReadEnrichedHollowBatch>
    for ReaderEnrichedHollowBatch<T>
{
    fn into_proto(&self) -> ProtoReadEnrichedHollowBatch {
        ProtoReadEnrichedHollowBatch {
            shard_id: self.shard_id.into_proto(),
            reader_metadata: Some(self.reader_metadata.into_proto()),
            batch: Some(self.batch.into_proto()),
        }
    }

    fn from_proto(proto: ProtoReadEnrichedHollowBatch) -> Result<Self, TryFromProtoError> {
        Ok(ReaderEnrichedHollowBatch {
            shard_id: proto.shard_id.into_rust()?,
            reader_metadata: proto
                .reader_metadata
                .into_rust_if_some("ProtoReadEnrichedHollowBatch::reader_metadata")?,
            batch: proto
                .batch
                .into_rust_if_some("ProtoReadEnrichedHollowBatch::batch")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_types::Codec;

    use crate::r#impl::state::State;
    use crate::ShardId;

    #[test]
    fn applier_version() {
        let v1 = semver::Version::new(1, 0, 0);
        let v2 = semver::Version::new(2, 0, 0);
        let v3 = semver::Version::new(3, 0, 0);

        // Code version v2 evaluates and writes out some State.
        let state = State::<(), (), u64, i64>::new(v2.clone(), ShardId::new());
        let mut buf = Vec::new();
        state.encode(&mut buf);

        // We can read it back using persist code v2 and v3.
        assert_eq!(State::decode(&v2, &buf).as_ref(), Ok(&state));
        assert_eq!(State::decode(&v3, &buf).as_ref(), Ok(&state));

        // But we can't read it back using v1 because v1 might corrupt it by
        // losing or misinterpreting something written out by a future version
        // of code.
        let v1_res = std::panic::catch_unwind(|| State::<(), (), u64, i64>::decode(&v1, &buf));
        assert!(v1_res.is_err());
    }
}
