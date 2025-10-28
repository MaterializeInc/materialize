// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastInto;
use mz_ore::{assert_none, halt, soft_panic_or_log};
use mz_persist::indexed::encoding::{BatchColumnarFormat, BlobTraceBatchPart, BlobTraceUpdates};
use mz_persist::location::{SeqNo, VersionedData};
use mz_persist::metrics::ColumnarMetrics;
use mz_persist_types::schema::SchemaId;
use mz_persist_types::stats::{PartStats, ProtoStructStats};
use mz_persist_types::{Codec, Codec64};
use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use proptest::prelude::Arbitrary;
use proptest::strategy::Strategy;
use prost::Message;
use semver::Version;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use timely::progress::{Antichain, Timestamp};
use uuid::Uuid;

use crate::critical::CriticalReaderId;
use crate::error::{CodecMismatch, CodecMismatchT};
use crate::internal::metrics::Metrics;
use crate::internal::paths::{PartialBatchKey, PartialRollupKey};
use crate::internal::state::{
    ActiveGc, ActiveRollup, BatchPart, CriticalReaderState, EncodedSchemas, HandleDebugState,
    HollowBatch, HollowBatchPart, HollowRollup, HollowRun, HollowRunRef, IdempotencyToken,
    LeasedReaderState, OpaqueState, ProtoActiveGc, ProtoActiveRollup, ProtoCompaction,
    ProtoCriticalReaderState, ProtoEncodedSchemas, ProtoHandleDebugState, ProtoHollowBatch,
    ProtoHollowBatchPart, ProtoHollowRollup, ProtoHollowRun, ProtoHollowRunRef, ProtoIdHollowBatch,
    ProtoIdMerge, ProtoIdSpineBatch, ProtoInlineBatchPart, ProtoInlinedDiffs,
    ProtoLeasedReaderState, ProtoMerge, ProtoRollup, ProtoRunMeta, ProtoRunOrder, ProtoSpineBatch,
    ProtoSpineId, ProtoStateDiff, ProtoStateField, ProtoStateFieldDiffType, ProtoStateFieldDiffs,
    ProtoTrace, ProtoU64Antichain, ProtoU64Description, ProtoVersionedData, ProtoWriterState,
    RunId, RunMeta, RunOrder, RunPart, State, StateCollections, TypedState, WriterState,
    proto_hollow_batch_part,
};
use crate::internal::state_diff::{
    ProtoStateFieldDiff, ProtoStateFieldDiffsWriter, StateDiff, StateFieldDiff, StateFieldValDiff,
};
use crate::internal::trace::{
    ActiveCompaction, FlatTrace, SpineId, ThinMerge, ThinSpineBatch, Trace,
};
use crate::read::{LeasedReaderId, READER_LEASE_DURATION};
use crate::{PersistConfig, ShardId, WriterId, cfg};

/// A key and value `Schema` of data written to a batch or shard.
#[derive(Debug)]
pub struct Schemas<K: Codec, V: Codec> {
    /// Id under which this schema is registered in the shard's schema registry,
    /// if any.
    pub id: Option<SchemaId>,
    /// Key `Schema`.
    pub key: Arc<K::Schema>,
    /// Value `Schema`.
    pub val: Arc<V::Schema>,
}

impl<K: Codec, V: Codec> Clone for Schemas<K, V> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            key: Arc::clone(&self.key),
            val: Arc::clone(&self.val),
        }
    }
}

/// A proto that is decoded on use.
///
/// Because of the way prost works, decoding a large protobuf may result in a
/// number of very short lived allocations in our RustType/ProtoType decode path
/// (e.g. this happens for a repeated embedded message). Not every use of
/// persist State needs every transitive bit of it to be decoded, so we opt
/// certain parts of it (initially stats) to be decoded on use.
///
/// This has the dual benefit of only paying for the short-lived allocs when
/// necessary and also allowing decoding to be gated by a feature flag. The
/// tradeoffs are that we might decode more than once and that we have to handle
/// invalid proto errors in more places.
///
/// Mechanically, this is accomplished by making the field a proto `bytes` types
/// instead of `ProtoFoo`. These bytes then contain the serialization of
/// ProtoFoo. NB: Swapping between the two is actually a forward and backward
/// compatible change.
///
/// > Embedded messages are compatible with bytes if the bytes contain an
/// > encoded version of the message.
///
/// (See <https://protobuf.dev/programming-guides/proto3/#updating>)
#[derive(Clone, Serialize, Deserialize)]
pub struct LazyProto<T> {
    buf: Bytes,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: Message + Default> Debug for LazyProto<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.decode() {
            Ok(proto) => Debug::fmt(&proto, f),
            Err(err) => f
                .debug_struct(&format!("LazyProto<{}>", std::any::type_name::<T>()))
                .field("err", &err)
                .finish(),
        }
    }
}

impl<T> PartialEq for LazyProto<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T> Eq for LazyProto<T> {}

impl<T> PartialOrd for LazyProto<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for LazyProto<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let LazyProto {
            buf: self_buf,
            _phantom: _,
        } = self;
        let LazyProto {
            buf: other_buf,
            _phantom: _,
        } = other;
        self_buf.cmp(other_buf)
    }
}

impl<T> Hash for LazyProto<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let LazyProto { buf, _phantom } = self;
        buf.hash(state);
    }
}

impl<T: Message + Default> From<&T> for LazyProto<T> {
    fn from(value: &T) -> Self {
        let buf = Bytes::from(value.encode_to_vec());
        LazyProto {
            buf,
            _phantom: PhantomData,
        }
    }
}

impl<T: Message + Default> LazyProto<T> {
    pub fn decode(&self) -> Result<T, prost::DecodeError> {
        T::decode(&*self.buf)
    }

    pub fn decode_to<R: RustType<T>>(&self) -> anyhow::Result<R> {
        Ok(T::decode(&*self.buf)?.into_rust()?)
    }
}

impl<T: Message + Default> RustType<Bytes> for LazyProto<T> {
    fn into_proto(&self) -> Bytes {
        self.buf.clone()
    }

    fn from_proto(buf: Bytes) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            buf,
            _phantom: PhantomData,
        })
    }
}

/// Our Proto implementation, Prost, cannot handle unrecognized fields. This means that unexpected
/// data will be dropped at deserialization time, which means that we can't reliably roundtrip data
/// from future versions of the code, which causes trouble during upgrades and at other times.
///
/// This type works around the issue by defining an unstructured metadata map. Keys are expected to
/// be well-known strings defined in the code; values are bytes, expected to be encoded protobuf.
/// (The association between the two is lightly enforced with the affiliated [MetadataKey] type.)
/// It's safe to add new metadata keys in new versions, since even unrecognized keys can be losslessly
/// roundtripped. However, if the metadata is not safe for the old version to ignore -- perhaps it
/// needs to be kept in sync with some other part of the struct -- you will need to use a more
/// heavyweight migration for it.
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct MetadataMap(BTreeMap<String, Bytes>);

/// Associating a field name and an associated Proto message type, for lookup in a metadata map.
///
/// It is an error to reuse key names, or to change the type associated with a particular name.
/// It is polite to choose short names, since they get serialized alongside every struct.
#[allow(unused)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct MetadataKey<V, P = V> {
    name: &'static str,
    type_: PhantomData<(V, P)>,
}

impl<V, P> MetadataKey<V, P> {
    #[allow(unused)]
    pub(crate) const fn new(name: &'static str) -> Self {
        MetadataKey {
            name,
            type_: PhantomData,
        }
    }
}

impl serde::Serialize for MetadataMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_map(self.0.iter())
    }
}

impl MetadataMap {
    /// Returns true iff no metadata keys have been set.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Serialize and insert a new key into the map, replacing any existing value for the key.
    #[allow(unused)]
    pub fn set<V: RustType<P>, P: prost::Message>(&mut self, key: MetadataKey<V, P>, value: V) {
        self.0.insert(
            String::from(key.name),
            Bytes::from(value.into_proto_owned().encode_to_vec()),
        );
    }

    /// Deserialize a key from the map, if it is present.
    #[allow(unused)]
    pub fn get<V: RustType<P>, P: prost::Message + Default>(
        &self,
        key: MetadataKey<V, P>,
    ) -> Option<V> {
        let proto = match P::decode(self.0.get(key.name)?.as_ref()) {
            Ok(decoded) => decoded,
            Err(err) => {
                // This should be impossible unless one of the MetadataKey invariants are broken.
                soft_panic_or_log!(
                    "error when decoding {key}; was it redefined? {err}",
                    key = key.name
                );
                return None;
            }
        };

        match proto.into_rust() {
            Ok(proto) => Some(proto),
            Err(err) => {
                // This should be impossible unless one of the MetadataKey invariants are broken.
                soft_panic_or_log!(
                    "error when decoding {key}; was it redefined? {err}",
                    key = key.name
                );
                None
            }
        }
    }
}
impl RustType<BTreeMap<String, Bytes>> for MetadataMap {
    fn into_proto(&self) -> BTreeMap<String, Bytes> {
        self.0.clone()
    }
    fn from_proto(proto: BTreeMap<String, Bytes>) -> Result<Self, TryFromProtoError> {
        Ok(MetadataMap(proto))
    }
}

pub(crate) fn parse_id(id_prefix: &str, id_type: &str, encoded: &str) -> Result<[u8; 16], String> {
    let uuid_encoded = match encoded.strip_prefix(id_prefix) {
        Some(x) => x,
        None => return Err(format!("invalid {} {}: incorrect prefix", id_type, encoded)),
    };
    let uuid = Uuid::parse_str(uuid_encoded)
        .map_err(|err| format!("invalid {} {}: {}", id_type, encoded, err))?;
    Ok(*uuid.as_bytes())
}

pub(crate) fn assert_code_can_read_data(code_version: &Version, data_version: &Version) {
    if !cfg::code_can_read_data(code_version, data_version) {
        // We can't catch halts, so panic in test, so we can get unit test
        // coverage.
        if cfg!(test) {
            panic!("code at version {code_version} cannot read data with version {data_version}");
        } else {
            halt!("code at version {code_version} cannot read data with version {data_version}");
        }
    }
}

impl RustType<String> for LeasedReaderId {
    fn into_proto(&self) -> String {
        self.to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        match proto.parse() {
            Ok(x) => Ok(x),
            Err(_) => Err(TryFromProtoError::InvalidShardId(proto)),
        }
    }
}

impl RustType<String> for CriticalReaderId {
    fn into_proto(&self) -> String {
        self.to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        match proto.parse() {
            Ok(x) => Ok(x),
            Err(_) => Err(TryFromProtoError::InvalidShardId(proto)),
        }
    }
}

impl RustType<String> for WriterId {
    fn into_proto(&self) -> String {
        self.to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        match proto.parse() {
            Ok(x) => Ok(x),
            Err(_) => Err(TryFromProtoError::InvalidShardId(proto)),
        }
    }
}

impl RustType<ProtoEncodedSchemas> for EncodedSchemas {
    fn into_proto(&self) -> ProtoEncodedSchemas {
        ProtoEncodedSchemas {
            key: self.key.clone(),
            key_data_type: self.key_data_type.clone(),
            val: self.val.clone(),
            val_data_type: self.val_data_type.clone(),
        }
    }

    fn from_proto(proto: ProtoEncodedSchemas) -> Result<Self, TryFromProtoError> {
        Ok(EncodedSchemas {
            key: proto.key,
            key_data_type: proto.key_data_type,
            val: proto.val,
            val_data_type: proto.val_data_type,
        })
    }
}

impl RustType<String> for IdempotencyToken {
    fn into_proto(&self) -> String {
        self.to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        match proto.parse() {
            Ok(x) => Ok(x),
            Err(_) => Err(TryFromProtoError::InvalidShardId(proto)),
        }
    }
}

impl RustType<String> for PartialBatchKey {
    fn into_proto(&self) -> String {
        self.0.clone()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        Ok(PartialBatchKey(proto))
    }
}

impl RustType<String> for PartialRollupKey {
    fn into_proto(&self) -> String {
        self.0.clone()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        Ok(PartialRollupKey(proto))
    }
}

impl<T: Timestamp + Lattice + Codec64> StateDiff<T> {
    pub fn encode<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    pub fn decode(build_version: &Version, buf: Bytes) -> Self {
        let proto = ProtoStateDiff::decode(buf)
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");
        let diff = Self::from_proto(proto).expect("internal error: invalid encoded state");
        assert_code_can_read_data(build_version, &diff.applier_version);
        diff
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoStateDiff> for StateDiff<T> {
    fn into_proto(&self) -> ProtoStateDiff {
        // Deconstruct self so we get a compile failure if new fields are added.
        let StateDiff {
            applier_version,
            seqno_from,
            seqno_to,
            walltime_ms,
            latest_rollup_key,
            rollups,
            active_rollup,
            active_gc,
            hostname,
            last_gc_req,
            leased_readers,
            critical_readers,
            writers,
            schemas,
            since,
            legacy_batches,
            hollow_batches,
            spine_batches,
            merges,
        } = self;

        let proto = ProtoStateFieldDiffs::default();

        // Create a writer so we can efficiently encode our data.
        let mut writer = proto.into_writer();

        field_diffs_into_proto(ProtoStateField::Hostname, hostname, &mut writer);
        field_diffs_into_proto(ProtoStateField::LastGcReq, last_gc_req, &mut writer);
        field_diffs_into_proto(ProtoStateField::Rollups, rollups, &mut writer);
        field_diffs_into_proto(ProtoStateField::ActiveRollup, active_rollup, &mut writer);
        field_diffs_into_proto(ProtoStateField::ActiveGc, active_gc, &mut writer);
        field_diffs_into_proto(ProtoStateField::LeasedReaders, leased_readers, &mut writer);
        field_diffs_into_proto(
            ProtoStateField::CriticalReaders,
            critical_readers,
            &mut writer,
        );
        field_diffs_into_proto(ProtoStateField::Writers, writers, &mut writer);
        field_diffs_into_proto(ProtoStateField::Schemas, schemas, &mut writer);
        field_diffs_into_proto(ProtoStateField::Since, since, &mut writer);
        field_diffs_into_proto(ProtoStateField::LegacyBatches, legacy_batches, &mut writer);
        field_diffs_into_proto(ProtoStateField::HollowBatches, hollow_batches, &mut writer);
        field_diffs_into_proto(ProtoStateField::SpineBatches, spine_batches, &mut writer);
        field_diffs_into_proto(ProtoStateField::SpineMerges, merges, &mut writer);

        // After encoding all of our data, convert back into the proto.
        let field_diffs = writer.into_proto();

        debug_assert_eq!(field_diffs.validate(), Ok(()));
        ProtoStateDiff {
            applier_version: applier_version.to_string(),
            seqno_from: seqno_from.into_proto(),
            seqno_to: seqno_to.into_proto(),
            walltime_ms: walltime_ms.into_proto(),
            latest_rollup_key: latest_rollup_key.into_proto(),
            field_diffs: Some(field_diffs),
        }
    }

    fn from_proto(proto: ProtoStateDiff) -> Result<Self, TryFromProtoError> {
        let applier_version = if proto.applier_version.is_empty() {
            // Backward compatibility with versions of ProtoState before we set
            // this field: if it's missing (empty), assume an infinitely old
            // version.
            semver::Version::new(0, 0, 0)
        } else {
            semver::Version::parse(&proto.applier_version).map_err(|err| {
                TryFromProtoError::InvalidSemverVersion(format!(
                    "invalid applier_version {}: {}",
                    proto.applier_version, err
                ))
            })?
        };
        let mut state_diff = StateDiff::new(
            applier_version,
            proto.seqno_from.into_rust()?,
            proto.seqno_to.into_rust()?,
            proto.walltime_ms,
            proto.latest_rollup_key.into_rust()?,
        );
        if let Some(field_diffs) = proto.field_diffs {
            debug_assert_eq!(field_diffs.validate(), Ok(()));
            for field_diff in field_diffs.iter() {
                let (field, diff) = field_diff?;
                match field {
                    ProtoStateField::Hostname => field_diff_into_rust::<(), String, _, _, _, _>(
                        diff,
                        &mut state_diff.hostname,
                        |()| Ok(()),
                        |v| v.into_rust(),
                    )?,
                    ProtoStateField::LastGcReq => field_diff_into_rust::<(), u64, _, _, _, _>(
                        diff,
                        &mut state_diff.last_gc_req,
                        |()| Ok(()),
                        |v| v.into_rust(),
                    )?,
                    ProtoStateField::ActiveGc => {
                        field_diff_into_rust::<(), ProtoActiveGc, _, _, _, _>(
                            diff,
                            &mut state_diff.active_gc,
                            |()| Ok(()),
                            |v| v.into_rust(),
                        )?
                    }
                    ProtoStateField::ActiveRollup => {
                        field_diff_into_rust::<(), ProtoActiveRollup, _, _, _, _>(
                            diff,
                            &mut state_diff.active_rollup,
                            |()| Ok(()),
                            |v| v.into_rust(),
                        )?
                    }
                    ProtoStateField::Rollups => {
                        field_diff_into_rust::<u64, ProtoHollowRollup, _, _, _, _>(
                            diff,
                            &mut state_diff.rollups,
                            |k| k.into_rust(),
                            |v| v.into_rust(),
                        )?
                    }
                    // MIGRATION: We previously stored rollups as a `SeqNo ->
                    // string Key` map, but now the value is a `struct
                    // HollowRollup`.
                    ProtoStateField::DeprecatedRollups => {
                        field_diff_into_rust::<u64, String, _, _, _, _>(
                            diff,
                            &mut state_diff.rollups,
                            |k| k.into_rust(),
                            |v| {
                                Ok(HollowRollup {
                                    key: v.into_rust()?,
                                    encoded_size_bytes: None,
                                })
                            },
                        )?
                    }
                    ProtoStateField::LeasedReaders => {
                        field_diff_into_rust::<String, ProtoLeasedReaderState, _, _, _, _>(
                            diff,
                            &mut state_diff.leased_readers,
                            |k| k.into_rust(),
                            |v| v.into_rust(),
                        )?
                    }
                    ProtoStateField::CriticalReaders => {
                        field_diff_into_rust::<String, ProtoCriticalReaderState, _, _, _, _>(
                            diff,
                            &mut state_diff.critical_readers,
                            |k| k.into_rust(),
                            |v| v.into_rust(),
                        )?
                    }
                    ProtoStateField::Writers => {
                        field_diff_into_rust::<String, ProtoWriterState, _, _, _, _>(
                            diff,
                            &mut state_diff.writers,
                            |k| k.into_rust(),
                            |v| v.into_rust(),
                        )?
                    }
                    ProtoStateField::Schemas => {
                        field_diff_into_rust::<u64, ProtoEncodedSchemas, _, _, _, _>(
                            diff,
                            &mut state_diff.schemas,
                            |k| k.into_rust(),
                            |v| v.into_rust(),
                        )?
                    }
                    ProtoStateField::Since => {
                        field_diff_into_rust::<(), ProtoU64Antichain, _, _, _, _>(
                            diff,
                            &mut state_diff.since,
                            |()| Ok(()),
                            |v| v.into_rust(),
                        )?
                    }
                    ProtoStateField::LegacyBatches => {
                        field_diff_into_rust::<ProtoHollowBatch, (), _, _, _, _>(
                            diff,
                            &mut state_diff.legacy_batches,
                            |k| k.into_rust(),
                            |()| Ok(()),
                        )?
                    }
                    ProtoStateField::HollowBatches => {
                        field_diff_into_rust::<ProtoSpineId, ProtoHollowBatch, _, _, _, _>(
                            diff,
                            &mut state_diff.hollow_batches,
                            |k| k.into_rust(),
                            |v| v.into_rust(),
                        )?
                    }
                    ProtoStateField::SpineBatches => {
                        field_diff_into_rust::<ProtoSpineId, ProtoSpineBatch, _, _, _, _>(
                            diff,
                            &mut state_diff.spine_batches,
                            |k| k.into_rust(),
                            |v| v.into_rust(),
                        )?
                    }
                    ProtoStateField::SpineMerges => {
                        field_diff_into_rust::<ProtoSpineId, ProtoMerge, _, _, _, _>(
                            diff,
                            &mut state_diff.merges,
                            |k| k.into_rust(),
                            |v| v.into_rust(),
                        )?
                    }
                }
            }
        }
        Ok(state_diff)
    }
}

fn field_diffs_into_proto<K, KP, V, VP>(
    field: ProtoStateField,
    diffs: &[StateFieldDiff<K, V>],
    writer: &mut ProtoStateFieldDiffsWriter,
) where
    KP: prost::Message,
    K: RustType<KP>,
    VP: prost::Message,
    V: RustType<VP>,
{
    for diff in diffs.iter() {
        field_diff_into_proto(field, diff, writer);
    }
}

fn field_diff_into_proto<K, KP, V, VP>(
    field: ProtoStateField,
    diff: &StateFieldDiff<K, V>,
    writer: &mut ProtoStateFieldDiffsWriter,
) where
    KP: prost::Message,
    K: RustType<KP>,
    VP: prost::Message,
    V: RustType<VP>,
{
    writer.push_field(field);
    writer.encode_proto(&diff.key.into_proto());
    match &diff.val {
        StateFieldValDiff::Insert(to) => {
            writer.push_diff_type(ProtoStateFieldDiffType::Insert);
            writer.encode_proto(&to.into_proto());
        }
        StateFieldValDiff::Update(from, to) => {
            writer.push_diff_type(ProtoStateFieldDiffType::Update);
            writer.encode_proto(&from.into_proto());
            writer.encode_proto(&to.into_proto());
        }
        StateFieldValDiff::Delete(from) => {
            writer.push_diff_type(ProtoStateFieldDiffType::Delete);
            writer.encode_proto(&from.into_proto());
        }
    };
}

fn field_diff_into_rust<KP, VP, K, V, KFn, VFn>(
    proto: ProtoStateFieldDiff<'_>,
    diffs: &mut Vec<StateFieldDiff<K, V>>,
    k_fn: KFn,
    v_fn: VFn,
) -> Result<(), TryFromProtoError>
where
    KP: prost::Message + Default,
    VP: prost::Message + Default,
    KFn: Fn(KP) -> Result<K, TryFromProtoError>,
    VFn: Fn(VP) -> Result<V, TryFromProtoError>,
{
    let val = match proto.diff_type {
        ProtoStateFieldDiffType::Insert => {
            let to = VP::decode(proto.to)
                .map_err(|err| TryFromProtoError::InvalidPersistState(err.to_string()))?;
            StateFieldValDiff::Insert(v_fn(to)?)
        }
        ProtoStateFieldDiffType::Update => {
            let from = VP::decode(proto.from)
                .map_err(|err| TryFromProtoError::InvalidPersistState(err.to_string()))?;
            let to = VP::decode(proto.to)
                .map_err(|err| TryFromProtoError::InvalidPersistState(err.to_string()))?;

            StateFieldValDiff::Update(v_fn(from)?, v_fn(to)?)
        }
        ProtoStateFieldDiffType::Delete => {
            let from = VP::decode(proto.from)
                .map_err(|err| TryFromProtoError::InvalidPersistState(err.to_string()))?;
            StateFieldValDiff::Delete(v_fn(from)?)
        }
    };
    let key = KP::decode(proto.key)
        .map_err(|err| TryFromProtoError::InvalidPersistState(err.to_string()))?;
    diffs.push(StateFieldDiff {
        key: k_fn(key)?,
        val,
    });
    Ok(())
}

/// The decoded state of [ProtoRollup] for which we have not yet checked
/// that codecs match the ones in durable state.
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(Clone, PartialEq))]
pub struct UntypedState<T> {
    pub(crate) key_codec: String,
    pub(crate) val_codec: String,
    pub(crate) ts_codec: String,
    pub(crate) diff_codec: String,

    // Important! This T has not been validated, so we can't expose anything in
    // State that references T until one of the check methods have been called.
    // Any field on State that doesn't reference T is fair game.
    state: State<T>,
}

impl<T: Timestamp + Lattice + Codec64> UntypedState<T> {
    pub fn seqno(&self) -> SeqNo {
        self.state.seqno
    }

    pub fn rollups(&self) -> &BTreeMap<SeqNo, HollowRollup> {
        &self.state.collections.rollups
    }

    pub fn latest_rollup(&self) -> (&SeqNo, &HollowRollup) {
        self.state.latest_rollup()
    }

    pub fn apply_encoded_diffs<'a, I: IntoIterator<Item = &'a VersionedData>>(
        &mut self,
        cfg: &PersistConfig,
        metrics: &Metrics,
        diffs: I,
    ) {
        // The apply_encoded_diffs might panic if T is not correct. Making this
        // a silent no-op is far too subtle for my taste, but it's not clear
        // what else we could do instead.
        if T::codec_name() != self.ts_codec {
            return;
        }
        self.state.apply_encoded_diffs(cfg, metrics, diffs);
    }

    pub fn check_codecs<K: Codec, V: Codec, D: Codec64>(
        self,
        shard_id: &ShardId,
    ) -> Result<TypedState<K, V, T, D>, Box<CodecMismatch>> {
        // Also defensively check that the shard_id on the state we fetched
        // matches the shard_id we were trying to fetch.
        assert_eq!(shard_id, &self.state.shard_id);
        if K::codec_name() != self.key_codec
            || V::codec_name() != self.val_codec
            || T::codec_name() != self.ts_codec
            || D::codec_name() != self.diff_codec
        {
            return Err(Box::new(CodecMismatch {
                requested: (
                    K::codec_name(),
                    V::codec_name(),
                    T::codec_name(),
                    D::codec_name(),
                    None,
                ),
                actual: (
                    self.key_codec,
                    self.val_codec,
                    self.ts_codec,
                    self.diff_codec,
                    None,
                ),
            }));
        }
        Ok(TypedState {
            state: self.state,
            _phantom: PhantomData,
        })
    }

    pub(crate) fn check_ts_codec(self, shard_id: &ShardId) -> Result<State<T>, CodecMismatchT> {
        // Also defensively check that the shard_id on the state we fetched
        // matches the shard_id we were trying to fetch.
        assert_eq!(shard_id, &self.state.shard_id);
        if T::codec_name() != self.ts_codec {
            return Err(CodecMismatchT {
                requested: T::codec_name(),
                actual: self.ts_codec,
            });
        }
        Ok(self.state)
    }

    pub fn decode(build_version: &Version, buf: impl Buf) -> Self {
        let proto = ProtoRollup::decode(buf)
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");
        let state = Rollup::from_proto(proto)
            .expect("internal error: invalid encoded state")
            .state;
        assert_code_can_read_data(build_version, &state.state.collections.version);
        state
    }
}

impl<K, V, T, D> From<TypedState<K, V, T, D>> for UntypedState<T>
where
    K: Codec,
    V: Codec,
    T: Codec64,
    D: Codec64,
{
    fn from(typed_state: TypedState<K, V, T, D>) -> Self {
        UntypedState {
            key_codec: K::codec_name(),
            val_codec: V::codec_name(),
            ts_codec: T::codec_name(),
            diff_codec: D::codec_name(),
            state: typed_state.state,
        }
    }
}

/// A struct that maps 1:1 with ProtoRollup.
///
/// Contains State, and optionally the diffs between (state.latest_rollup.seqno, state.seqno]
///
/// `diffs` is always expected to be `Some` when writing new rollups, but may be `None`
/// when deserializing rollups that were persisted before we started inlining diffs.
#[derive(Debug)]
pub struct Rollup<T> {
    pub(crate) state: UntypedState<T>,
    pub(crate) diffs: Option<InlinedDiffs>,
}

impl<T: Timestamp + Lattice + Codec64> Rollup<T> {
    /// Creates a `StateRollup` from a state and diffs from its last rollup.
    ///
    /// The diffs must span the seqno range `(state.last_rollup().seqno, state.seqno]`.
    pub(crate) fn from(state: UntypedState<T>, diffs: Vec<VersionedData>) -> Self {
        let latest_rollup_seqno = *state.latest_rollup().0;
        let mut verify_seqno = latest_rollup_seqno;
        for diff in &diffs {
            assert_eq!(verify_seqno.next(), diff.seqno);
            verify_seqno = diff.seqno;
        }
        assert_eq!(verify_seqno, state.seqno());

        let diffs = Some(InlinedDiffs::from(
            latest_rollup_seqno.next(),
            state.seqno().next(),
            diffs,
        ));

        Self { state, diffs }
    }

    pub(crate) fn from_untyped_state_without_diffs(state: UntypedState<T>) -> Self {
        Self { state, diffs: None }
    }

    pub(crate) fn from_state_without_diffs(
        state: State<T>,
        key_codec: String,
        val_codec: String,
        ts_codec: String,
        diff_codec: String,
    ) -> Self {
        Self::from_untyped_state_without_diffs(UntypedState {
            key_codec,
            val_codec,
            ts_codec,
            diff_codec,
            state,
        })
    }
}

#[derive(Debug)]
pub(crate) struct InlinedDiffs {
    pub(crate) lower: SeqNo,
    pub(crate) upper: SeqNo,
    pub(crate) diffs: Vec<VersionedData>,
}

impl InlinedDiffs {
    pub(crate) fn description(&self) -> Description<SeqNo> {
        Description::new(
            Antichain::from_elem(self.lower),
            Antichain::from_elem(self.upper),
            Antichain::from_elem(SeqNo::minimum()),
        )
    }

    fn from(lower: SeqNo, upper: SeqNo, diffs: Vec<VersionedData>) -> Self {
        for diff in &diffs {
            assert!(diff.seqno >= lower);
            assert!(diff.seqno < upper);
        }
        Self {
            lower,
            upper,
            diffs,
        }
    }
}

impl RustType<ProtoInlinedDiffs> for InlinedDiffs {
    fn into_proto(&self) -> ProtoInlinedDiffs {
        ProtoInlinedDiffs {
            lower: self.lower.into_proto(),
            upper: self.upper.into_proto(),
            diffs: self.diffs.into_proto(),
        }
    }

    fn from_proto(proto: ProtoInlinedDiffs) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            lower: proto.lower.into_rust()?,
            upper: proto.upper.into_rust()?,
            diffs: proto.diffs.into_rust()?,
        })
    }
}

impl<T: Timestamp + Lattice + Codec64> RustType<ProtoRollup> for Rollup<T> {
    fn into_proto(&self) -> ProtoRollup {
        ProtoRollup {
            applier_version: self.state.state.collections.version.to_string(),
            shard_id: self.state.state.shard_id.into_proto(),
            seqno: self.state.state.seqno.into_proto(),
            walltime_ms: self.state.state.walltime_ms.into_proto(),
            hostname: self.state.state.hostname.into_proto(),
            key_codec: self.state.key_codec.into_proto(),
            val_codec: self.state.val_codec.into_proto(),
            ts_codec: T::codec_name(),
            diff_codec: self.state.diff_codec.into_proto(),
            last_gc_req: self.state.state.collections.last_gc_req.into_proto(),
            active_rollup: self.state.state.collections.active_rollup.into_proto(),
            active_gc: self.state.state.collections.active_gc.into_proto(),
            rollups: self
                .state
                .state
                .collections
                .rollups
                .iter()
                .map(|(seqno, key)| (seqno.into_proto(), key.into_proto()))
                .collect(),
            deprecated_rollups: Default::default(),
            leased_readers: self
                .state
                .state
                .collections
                .leased_readers
                .iter()
                .map(|(id, state)| (id.into_proto(), state.into_proto()))
                .collect(),
            critical_readers: self
                .state
                .state
                .collections
                .critical_readers
                .iter()
                .map(|(id, state)| (id.into_proto(), state.into_proto()))
                .collect(),
            writers: self
                .state
                .state
                .collections
                .writers
                .iter()
                .map(|(id, state)| (id.into_proto(), state.into_proto()))
                .collect(),
            schemas: self
                .state
                .state
                .collections
                .schemas
                .iter()
                .map(|(id, schema)| (id.into_proto(), schema.into_proto()))
                .collect(),
            trace: Some(self.state.state.collections.trace.into_proto()),
            diffs: self.diffs.as_ref().map(|x| x.into_proto()),
        }
    }

    fn from_proto(x: ProtoRollup) -> Result<Self, TryFromProtoError> {
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

        let mut rollups = BTreeMap::new();
        for (seqno, rollup) in x.rollups {
            rollups.insert(seqno.into_rust()?, rollup.into_rust()?);
        }
        for (seqno, key) in x.deprecated_rollups {
            rollups.insert(
                seqno.into_rust()?,
                HollowRollup {
                    key: key.into_rust()?,
                    encoded_size_bytes: None,
                },
            );
        }
        let mut leased_readers = BTreeMap::new();
        for (id, state) in x.leased_readers {
            leased_readers.insert(id.into_rust()?, state.into_rust()?);
        }
        let mut critical_readers = BTreeMap::new();
        for (id, state) in x.critical_readers {
            critical_readers.insert(id.into_rust()?, state.into_rust()?);
        }
        let mut writers = BTreeMap::new();
        for (id, state) in x.writers {
            writers.insert(id.into_rust()?, state.into_rust()?);
        }
        let mut schemas = BTreeMap::new();
        for (id, x) in x.schemas {
            schemas.insert(id.into_rust()?, x.into_rust()?);
        }
        let active_rollup = x
            .active_rollup
            .map(|rollup| rollup.into_rust())
            .transpose()?;
        let active_gc = x.active_gc.map(|gc| gc.into_rust()).transpose()?;
        let collections = StateCollections {
            version: applier_version.clone(),
            rollups,
            active_rollup,
            active_gc,
            last_gc_req: x.last_gc_req.into_rust()?,
            leased_readers,
            critical_readers,
            writers,
            schemas,
            trace: x.trace.into_rust_if_some("trace")?,
        };
        let state = State {
            shard_id: x.shard_id.into_rust()?,
            seqno: x.seqno.into_rust()?,
            walltime_ms: x.walltime_ms,
            hostname: x.hostname,
            collections,
        };

        let diffs: Option<InlinedDiffs> = x.diffs.map(|diffs| diffs.into_rust()).transpose()?;
        if let Some(diffs) = &diffs {
            if diffs.lower != state.latest_rollup().0.next() {
                return Err(TryFromProtoError::InvalidPersistState(format!(
                    "diffs lower ({}) should match latest rollup's successor: ({})",
                    diffs.lower,
                    state.latest_rollup().0.next()
                )));
            }
            if diffs.upper != state.seqno.next() {
                return Err(TryFromProtoError::InvalidPersistState(format!(
                    "diffs upper ({}) should match state's successor: ({})",
                    diffs.lower,
                    state.seqno.next()
                )));
            }
        }

        Ok(Rollup {
            state: UntypedState {
                state,
                key_codec: x.key_codec.into_rust()?,
                val_codec: x.val_codec.into_rust()?,
                ts_codec: x.ts_codec.into_rust()?,
                diff_codec: x.diff_codec.into_rust()?,
            },
            diffs,
        })
    }
}

impl RustType<ProtoVersionedData> for VersionedData {
    fn into_proto(&self) -> ProtoVersionedData {
        ProtoVersionedData {
            seqno: self.seqno.into_proto(),
            data: Bytes::clone(&self.data),
        }
    }

    fn from_proto(proto: ProtoVersionedData) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            seqno: proto.seqno.into_rust()?,
            data: proto.data,
        })
    }
}

impl RustType<ProtoSpineId> for SpineId {
    fn into_proto(&self) -> ProtoSpineId {
        ProtoSpineId {
            lo: self.0.into_proto(),
            hi: self.1.into_proto(),
        }
    }

    fn from_proto(proto: ProtoSpineId) -> Result<Self, TryFromProtoError> {
        Ok(SpineId(proto.lo.into_rust()?, proto.hi.into_rust()?))
    }
}

impl<T: Timestamp + Codec64> ProtoMapEntry<SpineId, Arc<HollowBatch<T>>> for ProtoIdHollowBatch {
    fn from_rust<'a>(entry: (&'a SpineId, &'a Arc<HollowBatch<T>>)) -> Self {
        let (id, batch) = entry;
        ProtoIdHollowBatch {
            id: Some(id.into_proto()),
            batch: Some(batch.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(SpineId, Arc<HollowBatch<T>>), TryFromProtoError> {
        let id = self.id.into_rust_if_some("ProtoIdHollowBatch::id")?;
        let batch = Arc::new(self.batch.into_rust_if_some("ProtoIdHollowBatch::batch")?);
        Ok((id, batch))
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoSpineBatch> for ThinSpineBatch<T> {
    fn into_proto(&self) -> ProtoSpineBatch {
        ProtoSpineBatch {
            desc: Some(self.desc.into_proto()),
            parts: self.parts.into_proto(),
            level: self.level.into_proto(),
            descs: self.descs.into_proto(),
        }
    }

    fn from_proto(proto: ProtoSpineBatch) -> Result<Self, TryFromProtoError> {
        let level = proto.level.into_rust()?;
        let desc = proto.desc.into_rust_if_some("ProtoSpineBatch::desc")?;
        let parts = proto.parts.into_rust()?;
        let descs = proto.descs.into_rust()?;
        Ok(ThinSpineBatch {
            level,
            desc,
            parts,
            descs,
        })
    }
}

impl<T: Timestamp + Codec64> ProtoMapEntry<SpineId, ThinSpineBatch<T>> for ProtoIdSpineBatch {
    fn from_rust<'a>(entry: (&'a SpineId, &'a ThinSpineBatch<T>)) -> Self {
        let (id, batch) = entry;
        ProtoIdSpineBatch {
            id: Some(id.into_proto()),
            batch: Some(batch.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(SpineId, ThinSpineBatch<T>), TryFromProtoError> {
        let id = self.id.into_rust_if_some("ProtoHollowBatch::id")?;
        let batch = self.batch.into_rust_if_some("ProtoHollowBatch::batch")?;
        Ok((id, batch))
    }
}

impl RustType<ProtoCompaction> for ActiveCompaction {
    fn into_proto(&self) -> ProtoCompaction {
        ProtoCompaction {
            start_ms: self.start_ms,
        }
    }

    fn from_proto(proto: ProtoCompaction) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            start_ms: proto.start_ms,
        })
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoMerge> for ThinMerge<T> {
    fn into_proto(&self) -> ProtoMerge {
        ProtoMerge {
            since: Some(self.since.into_proto()),
            remaining_work: self.remaining_work.into_proto(),
            active_compaction: self.active_compaction.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMerge) -> Result<Self, TryFromProtoError> {
        let since = proto.since.into_rust_if_some("ProtoMerge::since")?;
        let remaining_work = proto.remaining_work.into_rust()?;
        let active_compaction = proto.active_compaction.into_rust()?;
        Ok(Self {
            since,
            remaining_work,
            active_compaction,
        })
    }
}

impl<T: Timestamp + Codec64> ProtoMapEntry<SpineId, ThinMerge<T>> for ProtoIdMerge {
    fn from_rust<'a>((id, merge): (&'a SpineId, &'a ThinMerge<T>)) -> Self {
        ProtoIdMerge {
            id: Some(id.into_proto()),
            merge: Some(merge.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(SpineId, ThinMerge<T>), TryFromProtoError> {
        let id = self.id.into_rust_if_some("ProtoIdMerge::id")?;
        let merge = self.merge.into_rust_if_some("ProtoIdMerge::merge")?;
        Ok((id, merge))
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoTrace> for FlatTrace<T> {
    fn into_proto(&self) -> ProtoTrace {
        let since = self.since.into_proto();
        let legacy_batches = self
            .legacy_batches
            .iter()
            .map(|(b, _)| b.into_proto())
            .collect();
        let hollow_batches = self.hollow_batches.into_proto();
        let spine_batches = self.spine_batches.into_proto();
        let merges = self.merges.into_proto();
        ProtoTrace {
            since: Some(since),
            legacy_batches,
            hollow_batches,
            spine_batches,
            merges,
        }
    }

    fn from_proto(proto: ProtoTrace) -> Result<Self, TryFromProtoError> {
        let since = proto.since.into_rust_if_some("ProtoTrace::since")?;
        let legacy_batches = proto
            .legacy_batches
            .into_iter()
            .map(|b| b.into_rust().map(|b| (b, ())))
            .collect::<Result<_, _>>()?;
        let hollow_batches = proto.hollow_batches.into_rust()?;
        let spine_batches = proto.spine_batches.into_rust()?;
        let merges = proto.merges.into_rust()?;
        Ok(FlatTrace {
            since,
            legacy_batches,
            hollow_batches,
            spine_batches,
            merges,
        })
    }
}

impl<T: Timestamp + Lattice + Codec64> RustType<ProtoTrace> for Trace<T> {
    fn into_proto(&self) -> ProtoTrace {
        self.flatten().into_proto()
    }

    fn from_proto(proto: ProtoTrace) -> Result<Self, TryFromProtoError> {
        Trace::unflatten(proto.into_rust()?).map_err(TryFromProtoError::InvalidPersistState)
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoLeasedReaderState> for LeasedReaderState<T> {
    fn into_proto(&self) -> ProtoLeasedReaderState {
        ProtoLeasedReaderState {
            seqno: self.seqno.into_proto(),
            since: Some(self.since.into_proto()),
            last_heartbeat_timestamp_ms: self.last_heartbeat_timestamp_ms.into_proto(),
            lease_duration_ms: self.lease_duration_ms.into_proto(),
            debug: Some(self.debug.into_proto()),
        }
    }

    fn from_proto(proto: ProtoLeasedReaderState) -> Result<Self, TryFromProtoError> {
        let mut lease_duration_ms = proto.lease_duration_ms.into_rust()?;
        // MIGRATION: If the lease_duration_ms is empty, then the proto field
        // was missing and we need to fill in a default. This would ideally be
        // based on the actual value in PersistConfig, but it's only here for a
        // short time and this is way easier.
        if lease_duration_ms == 0 {
            lease_duration_ms = u64::try_from(READER_LEASE_DURATION.default().as_millis())
                .expect("lease duration as millis should fit within u64");
        }
        // MIGRATION: If debug is empty, then the proto field was missing and we
        // need to fill in a default.
        let debug = proto.debug.unwrap_or_default().into_rust()?;
        Ok(LeasedReaderState {
            seqno: proto.seqno.into_rust()?,
            since: proto
                .since
                .into_rust_if_some("ProtoLeasedReaderState::since")?,
            last_heartbeat_timestamp_ms: proto.last_heartbeat_timestamp_ms.into_rust()?,
            lease_duration_ms,
            debug,
        })
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoCriticalReaderState> for CriticalReaderState<T> {
    fn into_proto(&self) -> ProtoCriticalReaderState {
        ProtoCriticalReaderState {
            since: Some(self.since.into_proto()),
            opaque: i64::from_le_bytes(self.opaque.0),
            opaque_codec: self.opaque_codec.clone(),
            debug: Some(self.debug.into_proto()),
        }
    }

    fn from_proto(proto: ProtoCriticalReaderState) -> Result<Self, TryFromProtoError> {
        // MIGRATION: If debug is empty, then the proto field was missing and we
        // need to fill in a default.
        let debug = proto.debug.unwrap_or_default().into_rust()?;
        Ok(CriticalReaderState {
            since: proto
                .since
                .into_rust_if_some("ProtoCriticalReaderState::since")?,
            opaque: OpaqueState(i64::to_le_bytes(proto.opaque)),
            opaque_codec: proto.opaque_codec,
            debug,
        })
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoWriterState> for WriterState<T> {
    fn into_proto(&self) -> ProtoWriterState {
        ProtoWriterState {
            last_heartbeat_timestamp_ms: self.last_heartbeat_timestamp_ms.into_proto(),
            lease_duration_ms: self.lease_duration_ms.into_proto(),
            most_recent_write_token: self.most_recent_write_token.into_proto(),
            most_recent_write_upper: Some(self.most_recent_write_upper.into_proto()),
            debug: Some(self.debug.into_proto()),
        }
    }

    fn from_proto(proto: ProtoWriterState) -> Result<Self, TryFromProtoError> {
        // MIGRATION: We didn't originally have most_recent_write_token and
        // most_recent_write_upper. Pick values that aren't going to
        // accidentally match ones in incoming writes and confuse things. We
        // could instead use Option on WriterState but this keeps the backward
        // compatibility logic confined to one place.
        let most_recent_write_token = if proto.most_recent_write_token.is_empty() {
            IdempotencyToken::SENTINEL
        } else {
            proto.most_recent_write_token.into_rust()?
        };
        let most_recent_write_upper = match proto.most_recent_write_upper {
            Some(x) => x.into_rust()?,
            None => Antichain::from_elem(T::minimum()),
        };
        // MIGRATION: If debug is empty, then the proto field was missing and we
        // need to fill in a default.
        let debug = proto.debug.unwrap_or_default().into_rust()?;
        Ok(WriterState {
            last_heartbeat_timestamp_ms: proto.last_heartbeat_timestamp_ms.into_rust()?,
            lease_duration_ms: proto.lease_duration_ms.into_rust()?,
            most_recent_write_token,
            most_recent_write_upper,
            debug,
        })
    }
}

impl RustType<ProtoHandleDebugState> for HandleDebugState {
    fn into_proto(&self) -> ProtoHandleDebugState {
        ProtoHandleDebugState {
            hostname: self.hostname.into_proto(),
            purpose: self.purpose.into_proto(),
        }
    }

    fn from_proto(proto: ProtoHandleDebugState) -> Result<Self, TryFromProtoError> {
        Ok(HandleDebugState {
            hostname: proto.hostname,
            purpose: proto.purpose,
        })
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoHollowRun> for HollowRun<T> {
    fn into_proto(&self) -> ProtoHollowRun {
        ProtoHollowRun {
            parts: self.parts.into_proto(),
        }
    }

    fn from_proto(proto: ProtoHollowRun) -> Result<Self, TryFromProtoError> {
        Ok(HollowRun {
            parts: proto.parts.into_rust()?,
        })
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoHollowBatch> for HollowBatch<T> {
    fn into_proto(&self) -> ProtoHollowBatch {
        let mut run_meta = self.run_meta.into_proto();
        // For backwards compatibility reasons, don't keep default metadata in the proto.
        let run_meta_default = RunMeta::default().into_proto();
        while run_meta.last() == Some(&run_meta_default) {
            run_meta.pop();
        }
        ProtoHollowBatch {
            desc: Some(self.desc.into_proto()),
            parts: self.parts.into_proto(),
            len: self.len.into_proto(),
            runs: self.run_splits.into_proto(),
            run_meta,
            deprecated_keys: vec![],
        }
    }

    fn from_proto(proto: ProtoHollowBatch) -> Result<Self, TryFromProtoError> {
        let mut parts: Vec<RunPart<T>> = proto.parts.into_rust()?;
        // MIGRATION: We used to just have the keys instead of a more structured
        // part.
        parts.extend(proto.deprecated_keys.into_iter().map(|key| {
            RunPart::Single(BatchPart::Hollow(HollowBatchPart {
                key: PartialBatchKey(key),
                encoded_size_bytes: 0,
                key_lower: vec![],
                structured_key_lower: None,
                stats: None,
                ts_rewrite: None,
                diffs_sum: None,
                format: None,
                schema_id: None,
                deprecated_schema_id: None,
            }))
        }));
        // We discard default metadatas from the proto above; re-add them here.
        let run_splits: Vec<usize> = proto.runs.into_rust()?;
        let num_runs = if parts.is_empty() {
            0
        } else {
            run_splits.len() + 1
        };
        let mut run_meta: Vec<RunMeta> = proto.run_meta.into_rust()?;
        run_meta.resize(num_runs, RunMeta::default());
        Ok(HollowBatch {
            desc: proto.desc.into_rust_if_some("desc")?,
            parts,
            len: proto.len.into_rust()?,
            run_splits,
            run_meta,
        })
    }
}

impl RustType<String> for RunId {
    fn into_proto(&self) -> String {
        self.to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        RunId::from_str(&proto).map_err(|_| {
            TryFromProtoError::InvalidPersistState(format!("invalid RunId: {}", proto))
        })
    }
}

impl RustType<ProtoRunMeta> for RunMeta {
    fn into_proto(&self) -> ProtoRunMeta {
        let order = match self.order {
            None => ProtoRunOrder::Unknown,
            Some(RunOrder::Unordered) => ProtoRunOrder::Unordered,
            Some(RunOrder::Codec) => ProtoRunOrder::Codec,
            Some(RunOrder::Structured) => ProtoRunOrder::Structured,
        };
        ProtoRunMeta {
            order: order.into(),
            schema_id: self.schema.into_proto(),
            deprecated_schema_id: self.deprecated_schema.into_proto(),
            id: self.id.into_proto(),
            len: self.len.into_proto(),
        }
    }

    fn from_proto(proto: ProtoRunMeta) -> Result<Self, TryFromProtoError> {
        let order = match ProtoRunOrder::try_from(proto.order)? {
            ProtoRunOrder::Unknown => None,
            ProtoRunOrder::Unordered => Some(RunOrder::Unordered),
            ProtoRunOrder::Codec => Some(RunOrder::Codec),
            ProtoRunOrder::Structured => Some(RunOrder::Structured),
        };
        Ok(Self {
            order,
            schema: proto.schema_id.into_rust()?,
            deprecated_schema: proto.deprecated_schema_id.into_rust()?,
            id: proto.id.into_rust()?,
            len: proto.len.into_rust()?,
        })
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoHollowBatchPart> for RunPart<T> {
    fn into_proto(&self) -> ProtoHollowBatchPart {
        match self {
            RunPart::Single(part) => part.into_proto(),
            RunPart::Many(runs) => runs.into_proto(),
        }
    }

    fn from_proto(proto: ProtoHollowBatchPart) -> Result<Self, TryFromProtoError> {
        let run_part = if let Some(proto_hollow_batch_part::Kind::RunRef(_)) = proto.kind {
            RunPart::Many(proto.into_rust()?)
        } else {
            RunPart::Single(proto.into_rust()?)
        };
        Ok(run_part)
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoHollowBatchPart> for HollowRunRef<T> {
    fn into_proto(&self) -> ProtoHollowBatchPart {
        let part = ProtoHollowBatchPart {
            kind: Some(proto_hollow_batch_part::Kind::RunRef(ProtoHollowRunRef {
                key: self.key.into_proto(),
                max_part_bytes: self.max_part_bytes.into_proto(),
            })),
            encoded_size_bytes: self.hollow_bytes.into_proto(),
            key_lower: Bytes::copy_from_slice(&self.key_lower),
            diffs_sum: self.diffs_sum.map(i64::from_le_bytes),
            key_stats: None,
            ts_rewrite: None,
            format: None,
            schema_id: None,
            structured_key_lower: self.structured_key_lower.into_proto(),
            deprecated_schema_id: None,
        };
        part
    }

    fn from_proto(proto: ProtoHollowBatchPart) -> Result<Self, TryFromProtoError> {
        let run_proto = match proto.kind {
            Some(proto_hollow_batch_part::Kind::RunRef(proto_ref)) => proto_ref,
            _ => Err(TryFromProtoError::UnknownEnumVariant(
                "ProtoHollowBatchPart::kind".to_string(),
            ))?,
        };
        Ok(Self {
            key: run_proto.key.into_rust()?,
            hollow_bytes: proto.encoded_size_bytes.into_rust()?,
            max_part_bytes: run_proto.max_part_bytes.into_rust()?,
            key_lower: proto.key_lower.to_vec(),
            structured_key_lower: proto.structured_key_lower.into_rust()?,
            diffs_sum: proto.diffs_sum.as_ref().map(|x| i64::to_le_bytes(*x)),
            _phantom_data: Default::default(),
        })
    }
}

impl<T: Timestamp + Codec64> RustType<ProtoHollowBatchPart> for BatchPart<T> {
    fn into_proto(&self) -> ProtoHollowBatchPart {
        match self {
            BatchPart::Hollow(x) => ProtoHollowBatchPart {
                kind: Some(proto_hollow_batch_part::Kind::Key(x.key.into_proto())),
                encoded_size_bytes: x.encoded_size_bytes.into_proto(),
                key_lower: Bytes::copy_from_slice(&x.key_lower),
                structured_key_lower: x.structured_key_lower.as_ref().map(|lazy| lazy.buf.clone()),
                key_stats: x.stats.into_proto(),
                ts_rewrite: x.ts_rewrite.as_ref().map(|x| x.into_proto()),
                diffs_sum: x.diffs_sum.as_ref().map(|x| i64::from_le_bytes(*x)),
                format: x.format.map(|f| f.into_proto()),
                schema_id: x.schema_id.into_proto(),
                deprecated_schema_id: x.deprecated_schema_id.into_proto(),
            },
            BatchPart::Inline {
                updates,
                ts_rewrite,
                schema_id,
                deprecated_schema_id,
            } => ProtoHollowBatchPart {
                kind: Some(proto_hollow_batch_part::Kind::Inline(updates.into_proto())),
                encoded_size_bytes: 0,
                key_lower: Bytes::new(),
                structured_key_lower: None,
                key_stats: None,
                ts_rewrite: ts_rewrite.as_ref().map(|x| x.into_proto()),
                diffs_sum: None,
                format: None,
                schema_id: schema_id.into_proto(),
                deprecated_schema_id: deprecated_schema_id.into_proto(),
            },
        }
    }

    fn from_proto(proto: ProtoHollowBatchPart) -> Result<Self, TryFromProtoError> {
        let ts_rewrite = match proto.ts_rewrite {
            Some(ts_rewrite) => Some(ts_rewrite.into_rust()?),
            None => None,
        };
        let schema_id = proto.schema_id.into_rust()?;
        let deprecated_schema_id = proto.deprecated_schema_id.into_rust()?;
        match proto.kind {
            Some(proto_hollow_batch_part::Kind::Key(key)) => {
                Ok(BatchPart::Hollow(HollowBatchPart {
                    key: key.into_rust()?,
                    encoded_size_bytes: proto.encoded_size_bytes.into_rust()?,
                    key_lower: proto.key_lower.into(),
                    structured_key_lower: proto.structured_key_lower.into_rust()?,
                    stats: proto.key_stats.into_rust()?,
                    ts_rewrite,
                    diffs_sum: proto.diffs_sum.map(i64::to_le_bytes),
                    format: proto.format.map(|f| f.into_rust()).transpose()?,
                    schema_id,
                    deprecated_schema_id,
                }))
            }
            Some(proto_hollow_batch_part::Kind::Inline(x)) => {
                assert_eq!(proto.encoded_size_bytes, 0);
                assert_eq!(proto.key_lower.len(), 0);
                assert_none!(proto.key_stats);
                assert_none!(proto.diffs_sum);
                let updates = LazyInlineBatchPart(x.into_rust()?);
                Ok(BatchPart::Inline {
                    updates,
                    ts_rewrite,
                    schema_id,
                    deprecated_schema_id,
                })
            }
            _ => Err(TryFromProtoError::unknown_enum_variant(
                "ProtoHollowBatchPart::kind",
            )),
        }
    }
}

impl RustType<proto_hollow_batch_part::Format> for BatchColumnarFormat {
    fn into_proto(&self) -> proto_hollow_batch_part::Format {
        match self {
            BatchColumnarFormat::Row => proto_hollow_batch_part::Format::Row(()),
            BatchColumnarFormat::Both(version) => {
                proto_hollow_batch_part::Format::RowAndColumnar((*version).cast_into())
            }
            BatchColumnarFormat::Structured => proto_hollow_batch_part::Format::Structured(()),
        }
    }

    fn from_proto(proto: proto_hollow_batch_part::Format) -> Result<Self, TryFromProtoError> {
        let format = match proto {
            proto_hollow_batch_part::Format::Row(_) => BatchColumnarFormat::Row,
            proto_hollow_batch_part::Format::RowAndColumnar(version) => {
                BatchColumnarFormat::Both(version.cast_into())
            }
            proto_hollow_batch_part::Format::Structured(_) => BatchColumnarFormat::Structured,
        };
        Ok(format)
    }
}

/// Aggregate statistics about data contained in a part.
///
/// These are "lazy" in the sense that we don't decode them (or even validate
/// the encoded version) until they're used.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LazyPartStats {
    key: LazyProto<ProtoStructStats>,
}

impl Debug for LazyPartStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LazyPartStats")
            .field(&self.decode())
            .finish()
    }
}

impl LazyPartStats {
    pub(crate) fn encode(x: &PartStats, map_proto: impl FnOnce(&mut ProtoStructStats)) -> Self {
        let PartStats { key } = x;
        let mut proto_stats = ProtoStructStats::from_rust(key);
        map_proto(&mut proto_stats);
        LazyPartStats {
            key: LazyProto::from(&proto_stats),
        }
    }
    /// Decodes and returns PartStats from the encoded representation.
    ///
    /// This does not cache the returned value, it decodes each time it's
    /// called.
    pub fn decode(&self) -> PartStats {
        let key = self.key.decode().expect("valid proto");
        PartStats {
            key: key.into_rust().expect("valid stats"),
        }
    }
}

impl RustType<Bytes> for LazyPartStats {
    fn into_proto(&self) -> Bytes {
        let LazyPartStats { key } = self;
        key.into_proto()
    }

    fn from_proto(proto: Bytes) -> Result<Self, TryFromProtoError> {
        Ok(LazyPartStats {
            key: proto.into_rust()?,
        })
    }
}

#[cfg(test)]
pub(crate) fn any_some_lazy_part_stats() -> impl Strategy<Value = Option<LazyPartStats>> {
    proptest::prelude::any::<LazyPartStats>().prop_map(Some)
}

#[allow(unused_parens)]
impl Arbitrary for LazyPartStats {
    type Parameters = ();
    type Strategy =
        proptest::strategy::Map<(<PartStats as Arbitrary>::Strategy), fn((PartStats)) -> Self>;

    fn arbitrary_with(_: ()) -> Self::Strategy {
        Strategy::prop_map((proptest::prelude::any::<PartStats>()), |(x)| {
            LazyPartStats::encode(&x, |_| {})
        })
    }
}

impl ProtoInlineBatchPart {
    pub(crate) fn into_rust<T: Timestamp + Codec64>(
        lgbytes: &ColumnarMetrics,
        proto: Self,
    ) -> Result<BlobTraceBatchPart<T>, TryFromProtoError> {
        let updates = proto
            .updates
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoInlineBatchPart::updates"))?;
        let updates = BlobTraceUpdates::from_proto(lgbytes, updates)?;

        Ok(BlobTraceBatchPart {
            desc: proto.desc.into_rust_if_some("ProtoInlineBatchPart::desc")?,
            index: proto.index.into_rust()?,
            updates,
        })
    }
}

/// A batch part stored inlined in State.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LazyInlineBatchPart(LazyProto<ProtoInlineBatchPart>);

impl From<&ProtoInlineBatchPart> for LazyInlineBatchPart {
    fn from(value: &ProtoInlineBatchPart) -> Self {
        LazyInlineBatchPart(value.into())
    }
}

impl Serialize for LazyInlineBatchPart {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        // NB: This serialize impl is only used for QA and debugging, so emit a
        // truncated version.
        let proto = self.0.decode().expect("valid proto");
        let mut s = s.serialize_struct("InlineBatchPart", 3)?;
        let () = s.serialize_field("desc", &proto.desc)?;
        let () = s.serialize_field("index", &proto.index)?;
        let () = s.serialize_field("updates[len]", &proto.updates.map_or(0, |x| x.len))?;
        s.end()
    }
}

impl LazyInlineBatchPart {
    pub(crate) fn encoded_size_bytes(&self) -> usize {
        self.0.buf.len()
    }

    /// Decodes and returns a BlobTraceBatchPart from the encoded
    /// representation.
    ///
    /// This does not cache the returned value, it decodes each time it's
    /// called.
    pub fn decode<T: Timestamp + Codec64>(
        &self,
        lgbytes: &ColumnarMetrics,
    ) -> Result<BlobTraceBatchPart<T>, TryFromProtoError> {
        let proto = self.0.decode().expect("valid proto");
        ProtoInlineBatchPart::into_rust(lgbytes, proto)
    }
}

impl RustType<Bytes> for LazyInlineBatchPart {
    fn into_proto(&self) -> Bytes {
        self.0.into_proto()
    }

    fn from_proto(proto: Bytes) -> Result<Self, TryFromProtoError> {
        Ok(LazyInlineBatchPart(proto.into_rust()?))
    }
}

impl RustType<ProtoHollowRollup> for HollowRollup {
    fn into_proto(&self) -> ProtoHollowRollup {
        ProtoHollowRollup {
            key: self.key.into_proto(),
            encoded_size_bytes: self.encoded_size_bytes.into_proto(),
        }
    }

    fn from_proto(proto: ProtoHollowRollup) -> Result<Self, TryFromProtoError> {
        Ok(HollowRollup {
            key: proto.key.into_rust()?,
            encoded_size_bytes: proto.encoded_size_bytes.into_rust()?,
        })
    }
}

impl RustType<ProtoActiveRollup> for ActiveRollup {
    fn into_proto(&self) -> ProtoActiveRollup {
        ProtoActiveRollup {
            start_ms: self.start_ms,
            seqno: self.seqno.into_proto(),
        }
    }

    fn from_proto(proto: ProtoActiveRollup) -> Result<Self, TryFromProtoError> {
        Ok(ActiveRollup {
            start_ms: proto.start_ms,
            seqno: proto.seqno.into_rust()?,
        })
    }
}

impl RustType<ProtoActiveGc> for ActiveGc {
    fn into_proto(&self) -> ProtoActiveGc {
        ProtoActiveGc {
            start_ms: self.start_ms,
            seqno: self.seqno.into_proto(),
        }
    }

    fn from_proto(proto: ProtoActiveGc) -> Result<Self, TryFromProtoError> {
        Ok(ActiveGc {
            start_ms: proto.start_ms,
            seqno: proto.seqno.into_rust()?,
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::assert_err;
    use mz_persist::location::SeqNo;
    use proptest::prelude::*;

    use crate::ShardId;
    use crate::internal::paths::PartialRollupKey;
    use crate::internal::state::tests::any_state;
    use crate::internal::state::{BatchPart, HandleDebugState};
    use crate::internal::state_diff::StateDiff;
    use crate::tests::new_test_client_cache;

    use super::*;

    #[mz_ore::test]
    fn metadata_map() {
        const COUNT: MetadataKey<u64> = MetadataKey::new("count");

        let mut map = MetadataMap::default();
        map.set(COUNT, 100);
        let mut map = MetadataMap::from_proto(map.into_proto()).unwrap();
        assert_eq!(map.get(COUNT), Some(100));

        const ANTICHAIN: MetadataKey<Antichain<u64>, ProtoU64Antichain> =
            MetadataKey::new("antichain");
        assert_none!(map.get(ANTICHAIN));

        map.set(ANTICHAIN, Antichain::from_elem(30));
        let map = MetadataMap::from_proto(map.into_proto()).unwrap();
        assert_eq!(map.get(COUNT), Some(100));
        assert_eq!(map.get(ANTICHAIN), Some(Antichain::from_elem(30)));
    }

    #[mz_ore::test]
    fn applier_version_state() {
        let v1 = semver::Version::new(1, 0, 0);
        let v2 = semver::Version::new(2, 0, 0);
        let v3 = semver::Version::new(3, 0, 0);

        // Code version v2 evaluates and writes out some State.
        let shard_id = ShardId::new();
        let state = TypedState::<(), (), u64, i64>::new(v2.clone(), shard_id, "".to_owned(), 0);
        let rollup =
            Rollup::from_untyped_state_without_diffs(state.clone_for_rollup().into()).into_proto();
        let mut buf = Vec::new();
        rollup.encode(&mut buf).expect("serializable");
        let bytes = Bytes::from(buf);

        // We can read it back using persist code v2 and v3.
        assert_eq!(
            UntypedState::<u64>::decode(&v2, bytes.clone())
                .check_codecs(&shard_id)
                .as_ref(),
            Ok(&state)
        );
        assert_eq!(
            UntypedState::<u64>::decode(&v3, bytes.clone())
                .check_codecs(&shard_id)
                .as_ref(),
            Ok(&state)
        );

        // But we can't read it back using v1 because v1 might corrupt it by
        // losing or misinterpreting something written out by a future version
        // of code.
        #[allow(clippy::disallowed_methods)] // not using enhanced panic handler in tests
        let v1_res = std::panic::catch_unwind(|| UntypedState::<u64>::decode(&v1, bytes.clone()));
        assert_err!(v1_res);
    }

    #[mz_ore::test]
    fn applier_version_state_diff() {
        let v1 = semver::Version::new(1, 0, 0);
        let v2 = semver::Version::new(2, 0, 0);
        let v3 = semver::Version::new(3, 0, 0);

        // Code version v2 evaluates and writes out some State.
        let diff = StateDiff::<u64>::new(
            v2.clone(),
            SeqNo(0),
            SeqNo(1),
            2,
            PartialRollupKey("rollup".into()),
        );
        let mut buf = Vec::new();
        diff.encode(&mut buf);
        let bytes = Bytes::from(buf);

        // We can read it back using persist code v2 and v3.
        assert_eq!(StateDiff::decode(&v2, bytes.clone()), diff);
        assert_eq!(StateDiff::decode(&v3, bytes.clone()), diff);

        // But we can't read it back using v1 because v1 might corrupt it by
        // losing or misinterpreting something written out by a future version
        // of code.
        #[allow(clippy::disallowed_methods)] // not using enhanced panic handler in tests
        let v1_res = std::panic::catch_unwind(|| StateDiff::<u64>::decode(&v1, bytes));
        assert_err!(v1_res);
    }

    #[mz_ore::test]
    fn hollow_batch_migration_keys() {
        let x = HollowBatch::new_run(
            Description::new(
                Antichain::from_elem(1u64),
                Antichain::from_elem(2u64),
                Antichain::from_elem(3u64),
            ),
            vec![RunPart::Single(BatchPart::Hollow(HollowBatchPart {
                key: PartialBatchKey("a".into()),
                encoded_size_bytes: 5,
                key_lower: vec![],
                structured_key_lower: None,
                stats: None,
                ts_rewrite: None,
                diffs_sum: None,
                format: None,
                schema_id: None,
                deprecated_schema_id: None,
            }))],
            4,
        );
        let mut old = x.into_proto();
        // Old ProtoHollowBatch had keys instead of parts.
        old.deprecated_keys = vec!["b".into()];
        // We don't expect to see a ProtoHollowBatch with keys _and_ parts, only
        // one or the other, but we have a defined output, so may as well test
        // it.
        let mut expected = x;
        // We fill in 0 for encoded_size_bytes when we migrate from keys. This
        // will violate bounded memory usage compaction during the transition
        // (short-term issue), but that's better than creating unnecessary runs
        // (longer-term issue).
        expected
            .parts
            .push(RunPart::Single(BatchPart::Hollow(HollowBatchPart {
                key: PartialBatchKey("b".into()),
                encoded_size_bytes: 0,
                key_lower: vec![],
                structured_key_lower: None,
                stats: None,
                ts_rewrite: None,
                diffs_sum: None,
                format: None,
                schema_id: None,
                deprecated_schema_id: None,
            })));
        assert_eq!(<HollowBatch<u64>>::from_proto(old).unwrap(), expected);
    }

    #[mz_ore::test]
    fn reader_state_migration_lease_duration() {
        let x = LeasedReaderState {
            seqno: SeqNo(1),
            since: Antichain::from_elem(2u64),
            last_heartbeat_timestamp_ms: 3,
            debug: HandleDebugState {
                hostname: "host".to_owned(),
                purpose: "purpose".to_owned(),
            },
            // Old ProtoReaderState had no lease_duration_ms field
            lease_duration_ms: 0,
        };
        let old = x.into_proto();
        let mut expected = x;
        // We fill in DEFAULT_READ_LEASE_DURATION for lease_duration_ms when we
        // migrate from unset.
        expected.lease_duration_ms =
            u64::try_from(READER_LEASE_DURATION.default().as_millis()).unwrap();
        assert_eq!(<LeasedReaderState<u64>>::from_proto(old).unwrap(), expected);
    }

    #[mz_ore::test]
    fn writer_state_migration_most_recent_write() {
        let proto = ProtoWriterState {
            last_heartbeat_timestamp_ms: 1,
            lease_duration_ms: 2,
            // Old ProtoWriterState had no most_recent_write_token or
            // most_recent_write_upper.
            most_recent_write_token: "".into(),
            most_recent_write_upper: None,
            debug: Some(ProtoHandleDebugState {
                hostname: "host".to_owned(),
                purpose: "purpose".to_owned(),
            }),
        };
        let expected = WriterState {
            last_heartbeat_timestamp_ms: proto.last_heartbeat_timestamp_ms,
            lease_duration_ms: proto.lease_duration_ms,
            most_recent_write_token: IdempotencyToken::SENTINEL,
            most_recent_write_upper: Antichain::from_elem(0),
            debug: HandleDebugState {
                hostname: "host".to_owned(),
                purpose: "purpose".to_owned(),
            },
        };
        assert_eq!(<WriterState<u64>>::from_proto(proto).unwrap(), expected);
    }

    #[mz_ore::test]
    fn state_migration_rollups() {
        let r1 = HollowRollup {
            key: PartialRollupKey("foo".to_owned()),
            encoded_size_bytes: None,
        };
        let r2 = HollowRollup {
            key: PartialRollupKey("bar".to_owned()),
            encoded_size_bytes: Some(2),
        };
        let shard_id = ShardId::new();
        let mut state = TypedState::<(), (), u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            shard_id,
            "host".to_owned(),
            0,
        );
        state.state.collections.rollups.insert(SeqNo(2), r2.clone());
        let mut proto = Rollup::from_untyped_state_without_diffs(state.into()).into_proto();

        // Manually add the old rollup encoding.
        proto.deprecated_rollups.insert(1, r1.key.0.clone());

        let state: Rollup<u64> = proto.into_rust().unwrap();
        let state = state.state;
        let state = state.check_codecs::<(), (), i64>(&shard_id).unwrap();
        let expected = vec![(SeqNo(1), r1), (SeqNo(2), r2)];
        assert_eq!(
            state
                .state
                .collections
                .rollups
                .into_iter()
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn state_diff_migration_rollups(dyncfgs: ConfigUpdates) {
        let r1_rollup = HollowRollup {
            key: PartialRollupKey("foo".to_owned()),
            encoded_size_bytes: None,
        };
        let r1 = StateFieldDiff {
            key: SeqNo(1),
            val: StateFieldValDiff::Insert(r1_rollup.clone()),
        };
        let r2_rollup = HollowRollup {
            key: PartialRollupKey("bar".to_owned()),
            encoded_size_bytes: Some(2),
        };
        let r2 = StateFieldDiff {
            key: SeqNo(2),
            val: StateFieldValDiff::Insert(r2_rollup.clone()),
        };
        let r3_rollup = HollowRollup {
            key: PartialRollupKey("baz".to_owned()),
            encoded_size_bytes: None,
        };
        let r3 = StateFieldDiff {
            key: SeqNo(3),
            val: StateFieldValDiff::Delete(r3_rollup.clone()),
        };
        let mut diff = StateDiff::<u64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            SeqNo(4),
            SeqNo(5),
            0,
            PartialRollupKey("ignored".to_owned()),
        );
        diff.rollups.push(r2.clone());
        diff.rollups.push(r3.clone());
        let mut diff_proto = diff.into_proto();

        let field_diffs = std::mem::take(&mut diff_proto.field_diffs).unwrap();
        let mut field_diffs_writer = field_diffs.into_writer();

        // Manually add the old rollup encoding.
        field_diffs_into_proto(
            ProtoStateField::DeprecatedRollups,
            &[StateFieldDiff {
                key: r1.key,
                val: StateFieldValDiff::Insert(r1_rollup.key.clone()),
            }],
            &mut field_diffs_writer,
        );

        assert_none!(diff_proto.field_diffs);
        diff_proto.field_diffs = Some(field_diffs_writer.into_proto());

        let diff = StateDiff::<u64>::from_proto(diff_proto.clone()).unwrap();
        assert_eq!(
            diff.rollups.into_iter().collect::<Vec<_>>(),
            vec![r2, r3, r1]
        );

        // Also make sure that a rollup delete in a diff applies cleanly to a
        // state that had it in the deprecated field.
        let shard_id = ShardId::new();
        let mut state = TypedState::<(), (), u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            shard_id,
            "host".to_owned(),
            0,
        );
        state.state.seqno = SeqNo(4);
        let mut rollup = Rollup::from_untyped_state_without_diffs(state.into()).into_proto();
        rollup
            .deprecated_rollups
            .insert(3, r3_rollup.key.into_proto());
        let state: Rollup<u64> = rollup.into_rust().unwrap();
        let state = state.state;
        let mut state = state.check_codecs::<(), (), i64>(&shard_id).unwrap();
        let cache = new_test_client_cache(&dyncfgs);
        let encoded_diff = VersionedData {
            seqno: SeqNo(5),
            data: diff_proto.encode_to_vec().into(),
        };
        state.apply_encoded_diffs(cache.cfg(), &cache.metrics, std::iter::once(&encoded_diff));
        assert_eq!(
            state
                .state
                .collections
                .rollups
                .into_iter()
                .collect::<Vec<_>>(),
            vec![(SeqNo(1), r1_rollup), (SeqNo(2), r2_rollup)]
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn state_proto_roundtrip() {
        fn testcase<T: Timestamp + Lattice + Codec64>(state: State<T>) {
            let before = UntypedState {
                key_codec: <() as Codec>::codec_name(),
                val_codec: <() as Codec>::codec_name(),
                ts_codec: <T as Codec64>::codec_name(),
                diff_codec: <i64 as Codec64>::codec_name(),
                state,
            };
            let proto = Rollup::from_untyped_state_without_diffs(before.clone()).into_proto();
            let after: Rollup<T> = proto.into_rust().unwrap();
            let after = after.state;
            assert_eq!(before, after);
        }

        proptest!(|(state in any_state::<u64>(0..3))| testcase(state));
    }

    #[mz_ore::test]
    fn check_data_versions() {
        #[track_caller]
        fn testcase(code: &str, data: &str, expected: Result<(), ()>) {
            let code = Version::parse(code).unwrap();
            let data = Version::parse(data).unwrap();
            #[allow(clippy::disallowed_methods)]
            let actual = cfg::code_can_write_data(&code, &data)
                .then_some(())
                .ok_or(());
            assert_eq!(actual, expected, "data at {data} read by code {code}");
        }

        testcase("0.160.0-dev", "0.160.0-dev", Ok(()));
        testcase("0.160.0-dev", "0.160.0", Err(()));
        // Note: Probably useful to let tests use two arbitrary shas on main, at
        // the very least for things like git bisect.
        testcase("0.160.0-dev", "0.161.0-dev", Err(()));
        testcase("0.160.0-dev", "0.161.0", Err(()));
        testcase("0.160.0-dev", "0.162.0-dev", Err(()));
        testcase("0.160.0-dev", "0.162.0", Err(()));
        testcase("0.160.0-dev", "0.163.0-dev", Err(()));

        testcase("0.160.0", "0.158.0-dev", Ok(()));
        testcase("0.160.0", "0.158.0", Ok(()));
        testcase("0.160.0", "0.159.0-dev", Ok(()));
        testcase("0.160.0", "0.159.0", Ok(()));
        testcase("0.160.0", "0.160.0-dev", Ok(()));
        testcase("0.160.0", "0.160.0", Ok(()));

        testcase("0.160.0", "0.161.0-dev", Err(()));
        testcase("0.160.0", "0.161.0", Err(()));
        testcase("0.160.0", "0.161.1", Err(()));
        testcase("0.160.0", "0.161.1000000", Err(()));
        testcase("0.160.0", "0.162.0-dev", Err(()));
        testcase("0.160.0", "0.162.0", Err(()));
        testcase("0.160.0", "0.163.0-dev", Err(()));

        testcase("0.160.1", "0.159.0", Ok(()));
        testcase("0.160.1", "0.160.0", Ok(()));
        testcase("0.160.1", "0.161.0", Err(()));
        testcase("0.160.1", "0.161.1", Err(()));
        testcase("0.160.1", "0.161.100", Err(()));
        testcase("0.160.0", "0.160.1", Err(()));

        testcase("0.160.1", "26.0.0", Err(()));
        testcase("26.0.0", "0.160.1", Ok(()));
        testcase("26.2.0", "0.160.1", Ok(()));
        testcase("26.200.200", "0.160.1", Ok(()));

        testcase("27.0.0", "0.160.1", Err(()));
        testcase("27.0.0", "0.16000.1", Err(()));
        testcase("27.0.0", "26.0.1", Ok(()));
        testcase("27.1000.100", "26.0.1", Ok(()));
        testcase("28.0.0", "26.0.1", Err(()));
        testcase("28.0.0", "26.1000.1", Err(()));
        testcase("28.0.0", "27.0.0", Ok(()));
    }
}
