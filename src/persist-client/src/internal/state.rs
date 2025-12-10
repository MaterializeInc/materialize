// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::ensure;
use async_stream::{stream, try_stream};
use differential_dataflow::difference::Monoid;
use mz_persist::metrics::ColumnarMetrics;
use proptest::prelude::{Arbitrary, Strategy};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::ControlFlow::{self, Break, Continue};
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use arrow::array::{Array, ArrayData, make_array};
use arrow::datatypes::DataType;
use bytes::Bytes;
use differential_dataflow::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::implementations::BatchContainer;
use futures::Stream;
use futures_util::StreamExt;
use itertools::Itertools;
use mz_dyncfg::Config;
use mz_ore::cast::CastFrom;
use mz_ore::now::EpochMillis;
use mz_ore::soft_panic_or_log;
use mz_ore::vec::PartialOrdVecExt;
use mz_persist::indexed::encoding::{BatchColumnarFormat, BlobTraceUpdates};
use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::arrow::{ArrayBound, ProtoArrayData};
use mz_persist_types::columnar::{ColumnEncoder, Schema};
use mz_persist_types::schema::{SchemaId, backward_compatible};
use mz_persist_types::{Codec, Codec64, Opaque};
use mz_proto::ProtoType;
use mz_proto::RustType;
use proptest_derive::Arbitrary;
use semver::Version;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use timely::PartialOrder;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::info;
use uuid::Uuid;

use crate::critical::CriticalReaderId;
use crate::error::InvalidUsage;
use crate::internal::encoding::{
    LazyInlineBatchPart, LazyPartStats, LazyProto, MetadataMap, parse_id,
};
use crate::internal::gc::GcReq;
use crate::internal::machine::retry_external;
use crate::internal::paths::{BlobKey, PartId, PartialBatchKey, PartialRollupKey, WriterKey};
use crate::internal::trace::{
    ActiveCompaction, ApplyMergeResult, FueledMergeReq, FueledMergeRes, Trace,
};
use crate::metrics::Metrics;
use crate::read::LeasedReaderId;
use crate::schema::CaESchema;
use crate::write::WriterId;
use crate::{PersistConfig, ShardId};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_persist_client.internal.state.rs"
));

include!(concat!(
    env!("OUT_DIR"),
    "/mz_persist_client.internal.diff.rs"
));

/// Determines how often to write rollups, assigning a maintenance task after
/// `rollup_threshold` seqnos have passed since the last rollup.
///
/// Tuning note: in the absence of a long reader seqno hold, and with
/// incremental GC, this threshold will determine about how many live diffs are
/// held in Consensus. Lowering this value decreases the live diff count at the
/// cost of more maintenance work + blob writes.
pub(crate) const ROLLUP_THRESHOLD: Config<usize> = Config::new(
    "persist_rollup_threshold",
    128,
    "The number of seqnos between rollups.",
);

/// Determines how long to wait before an active rollup is considered
/// "stuck" and a new rollup is started.
pub(crate) const ROLLUP_FALLBACK_THRESHOLD_MS: Config<usize> = Config::new(
    "persist_rollup_fallback_threshold_ms",
    5000,
    "The number of milliseconds before a worker claims an already claimed rollup.",
);

/// Feature flag the new active rollup tracking mechanism.
/// We musn't enable this until we are fully deployed on the new version.
pub(crate) const ROLLUP_USE_ACTIVE_ROLLUP: Config<bool> = Config::new(
    "persist_rollup_use_active_rollup",
    true,
    "Whether to use the new active rollup tracking mechanism.",
);

/// Determines how long to wait before an active GC is considered
/// "stuck" and a new GC is started.
pub(crate) const GC_FALLBACK_THRESHOLD_MS: Config<usize> = Config::new(
    "persist_gc_fallback_threshold_ms",
    900000,
    "The number of milliseconds before a worker claims an already claimed GC.",
);

/// See the config description string.
pub(crate) const GC_MIN_VERSIONS: Config<usize> = Config::new(
    "persist_gc_min_versions",
    32,
    "The number of un-GCd versions that may exist in state before we'll trigger a GC.",
);

/// See the config description string.
pub(crate) const GC_MAX_VERSIONS: Config<usize> = Config::new(
    "persist_gc_max_versions",
    128_000,
    "The maximum number of versions to GC in a single GC run.",
);

/// Feature flag the new active GC tracking mechanism.
/// We musn't enable this until we are fully deployed on the new version.
pub(crate) const GC_USE_ACTIVE_GC: Config<bool> = Config::new(
    "persist_gc_use_active_gc",
    false,
    "Whether to use the new active GC tracking mechanism.",
);

pub(crate) const ENABLE_INCREMENTAL_COMPACTION: Config<bool> = Config::new(
    "persist_enable_incremental_compaction",
    false,
    "Whether to enable incremental compaction.",
);

/// A token to disambiguate state commands that could not otherwise be
/// idempotent.
#[derive(Arbitrary, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(into = "String")]
pub struct IdempotencyToken(pub(crate) [u8; 16]);

impl std::fmt::Display for IdempotencyToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "i{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for IdempotencyToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IdempotencyToken({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for IdempotencyToken {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id("i", "IdempotencyToken", s).map(IdempotencyToken)
    }
}

impl From<IdempotencyToken> for String {
    fn from(x: IdempotencyToken) -> Self {
        x.to_string()
    }
}

impl IdempotencyToken {
    pub(crate) fn new() -> Self {
        IdempotencyToken(*Uuid::new_v4().as_bytes())
    }
    pub(crate) const SENTINEL: IdempotencyToken = IdempotencyToken([17u8; 16]);
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LeasedReaderState<T> {
    /// The seqno capability of this reader.
    pub seqno: SeqNo,
    /// The since capability of this reader.
    pub since: Antichain<T>,
    /// UNIX_EPOCH timestamp (in millis) of this reader's most recent heartbeat
    pub last_heartbeat_timestamp_ms: u64,
    /// Duration (in millis) allowed after [Self::last_heartbeat_timestamp_ms]
    /// after which this reader may be expired
    pub lease_duration_ms: u64,
    /// For debugging.
    pub debug: HandleDebugState,
}

#[derive(Arbitrary, Clone, Debug, PartialEq, Serialize)]
#[serde(into = "u64")]
pub struct OpaqueState(pub [u8; 8]);

impl From<OpaqueState> for u64 {
    fn from(value: OpaqueState) -> Self {
        u64::from_le_bytes(value.0)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CriticalReaderState<T> {
    /// The since capability of this reader.
    pub since: Antichain<T>,
    /// An opaque token matched on by compare_and_downgrade_since.
    pub opaque: OpaqueState,
    /// The [Codec64] used to encode [Self::opaque].
    pub opaque_codec: String,
    /// For debugging.
    pub debug: HandleDebugState,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WriterState<T> {
    /// UNIX_EPOCH timestamp (in millis) of this writer's most recent heartbeat
    pub last_heartbeat_timestamp_ms: u64,
    /// Duration (in millis) allowed after [Self::last_heartbeat_timestamp_ms]
    /// after which this writer may be expired
    pub lease_duration_ms: u64,
    /// The idempotency token of the most recent successful compare_and_append
    /// by this writer.
    pub most_recent_write_token: IdempotencyToken,
    /// The upper of the most recent successful compare_and_append by this
    /// writer.
    pub most_recent_write_upper: Antichain<T>,
    /// For debugging.
    pub debug: HandleDebugState,
}

/// Debugging info for a reader or writer.
#[derive(Arbitrary, Clone, Debug, Default, PartialEq, Serialize)]
pub struct HandleDebugState {
    /// Hostname of the persist user that registered this writer or reader. For
    /// critical readers, this is the _most recent_ registration.
    pub hostname: String,
    /// Plaintext description of this writer or reader's intent.
    pub purpose: String,
}

/// Part of the updates in a Batch.
///
/// Either a pointer to ones stored in Blob or the updates themselves inlined.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum BatchPart<T> {
    Hollow(HollowBatchPart<T>),
    Inline {
        updates: LazyInlineBatchPart,
        ts_rewrite: Option<Antichain<T>>,
        schema_id: Option<SchemaId>,

        /// ID of a schema that has since been deprecated and exists only to cleanly roundtrip.
        deprecated_schema_id: Option<SchemaId>,
    },
}

fn decode_structured_lower(lower: &LazyProto<ProtoArrayData>) -> Option<ArrayBound> {
    let try_decode = |lower: &LazyProto<ProtoArrayData>| {
        let proto = lower.decode()?;
        let data = ArrayData::from_proto(proto)?;
        ensure!(data.len() == 1);
        Ok(ArrayBound::new(make_array(data), 0))
    };

    let decoded: anyhow::Result<ArrayBound> = try_decode(lower);

    match decoded {
        Ok(bound) => Some(bound),
        Err(e) => {
            soft_panic_or_log!("failed to decode bound: {e:#?}");
            None
        }
    }
}

impl<T> BatchPart<T> {
    pub fn hollow_bytes(&self) -> usize {
        match self {
            BatchPart::Hollow(x) => x.encoded_size_bytes,
            BatchPart::Inline { .. } => 0,
        }
    }

    pub fn is_inline(&self) -> bool {
        matches!(self, BatchPart::Inline { .. })
    }

    pub fn inline_bytes(&self) -> usize {
        match self {
            BatchPart::Hollow(_) => 0,
            BatchPart::Inline { updates, .. } => updates.encoded_size_bytes(),
        }
    }

    pub fn writer_key(&self) -> Option<WriterKey> {
        match self {
            BatchPart::Hollow(x) => x.key.split().map(|(writer, _part)| writer),
            BatchPart::Inline { .. } => None,
        }
    }

    pub fn encoded_size_bytes(&self) -> usize {
        match self {
            BatchPart::Hollow(x) => x.encoded_size_bytes,
            BatchPart::Inline { updates, .. } => updates.encoded_size_bytes(),
        }
    }

    // A user-interpretable identifier or description of the part (for logs and
    // such).
    pub fn printable_name(&self) -> &str {
        match self {
            BatchPart::Hollow(x) => x.key.0.as_str(),
            BatchPart::Inline { .. } => "<inline>",
        }
    }

    pub fn stats(&self) -> Option<&LazyPartStats> {
        match self {
            BatchPart::Hollow(x) => x.stats.as_ref(),
            BatchPart::Inline { .. } => None,
        }
    }

    pub fn key_lower(&self) -> &[u8] {
        match self {
            BatchPart::Hollow(x) => x.key_lower.as_slice(),
            // We don't duplicate the lowest key because this can be
            // considerable overhead for small parts.
            //
            // The empty key might not be a tight lower bound, but it is a valid
            // lower bound. If a caller is interested in a tighter lower bound,
            // the data is inline.
            BatchPart::Inline { .. } => &[],
        }
    }

    pub fn structured_key_lower(&self) -> Option<ArrayBound> {
        let part = match self {
            BatchPart::Hollow(part) => part,
            BatchPart::Inline { .. } => return None,
        };

        decode_structured_lower(part.structured_key_lower.as_ref()?)
    }

    pub fn ts_rewrite(&self) -> Option<&Antichain<T>> {
        match self {
            BatchPart::Hollow(x) => x.ts_rewrite.as_ref(),
            BatchPart::Inline { ts_rewrite, .. } => ts_rewrite.as_ref(),
        }
    }

    pub fn schema_id(&self) -> Option<SchemaId> {
        match self {
            BatchPart::Hollow(x) => x.schema_id,
            BatchPart::Inline { schema_id, .. } => *schema_id,
        }
    }

    pub fn deprecated_schema_id(&self) -> Option<SchemaId> {
        match self {
            BatchPart::Hollow(x) => x.deprecated_schema_id,
            BatchPart::Inline {
                deprecated_schema_id,
                ..
            } => *deprecated_schema_id,
        }
    }
}

impl<T: Timestamp + Codec64> BatchPart<T> {
    pub fn is_structured_only(&self, metrics: &ColumnarMetrics) -> bool {
        match self {
            BatchPart::Hollow(x) => matches!(x.format, Some(BatchColumnarFormat::Structured)),
            BatchPart::Inline { updates, .. } => {
                let inline_part = updates.decode::<T>(metrics).expect("valid inline part");
                matches!(inline_part.updates, BlobTraceUpdates::Structured { .. })
            }
        }
    }

    pub fn diffs_sum<D: Codec64 + Monoid>(&self, metrics: &ColumnarMetrics) -> Option<D> {
        match self {
            BatchPart::Hollow(x) => x.diffs_sum.map(D::decode),
            BatchPart::Inline { updates, .. } => Some(
                updates
                    .decode::<T>(metrics)
                    .expect("valid inline part")
                    .updates
                    .diffs_sum(),
            ),
        }
    }
}

/// An ordered list of parts, generally stored as part of a larger run.
#[derive(Debug, Clone)]
pub struct HollowRun<T> {
    /// Pointers usable to retrieve the updates.
    pub(crate) parts: Vec<RunPart<T>>,
}

/// A reference to a [HollowRun], including the key in the blob store and some denormalized
/// metadata.
#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct HollowRunRef<T> {
    pub key: PartialBatchKey,

    /// The size of the referenced run object, plus all of the hollow objects it contains.
    pub hollow_bytes: usize,

    /// The size of the largest individual part in the run; useful for sizing compaction.
    pub max_part_bytes: usize,

    /// The lower bound of the data in this part, ordered by the codec ordering.
    pub key_lower: Vec<u8>,

    /// The lower bound of the data in this part, ordered by the structured ordering.
    pub structured_key_lower: Option<LazyProto<ProtoArrayData>>,

    pub diffs_sum: Option<[u8; 8]>,

    pub(crate) _phantom_data: PhantomData<T>,
}
impl<T: Eq> PartialOrd<Self> for HollowRunRef<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Eq> Ord for HollowRunRef<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<T> HollowRunRef<T> {
    pub fn writer_key(&self) -> Option<WriterKey> {
        Some(self.key.split()?.0)
    }
}

impl<T: Timestamp + Codec64> HollowRunRef<T> {
    /// Stores the given runs and returns a [HollowRunRef] that points to them.
    pub async fn set<D: Codec64 + Monoid>(
        shard_id: ShardId,
        blob: &dyn Blob,
        writer: &WriterKey,
        data: HollowRun<T>,
        metrics: &Metrics,
    ) -> Self {
        let hollow_bytes = data.parts.iter().map(|p| p.hollow_bytes()).sum();
        let max_part_bytes = data
            .parts
            .iter()
            .map(|p| p.max_part_bytes())
            .max()
            .unwrap_or(0);
        let key_lower = data
            .parts
            .first()
            .map_or(vec![], |p| p.key_lower().to_vec());
        let structured_key_lower = match data.parts.first() {
            Some(RunPart::Many(r)) => r.structured_key_lower.clone(),
            Some(RunPart::Single(BatchPart::Hollow(p))) => p.structured_key_lower.clone(),
            Some(RunPart::Single(BatchPart::Inline { .. })) | None => None,
        };
        let diffs_sum = data
            .parts
            .iter()
            .map(|p| {
                p.diffs_sum::<D>(&metrics.columnar)
                    .expect("valid diffs sum")
            })
            .reduce(|mut a, b| {
                a.plus_equals(&b);
                a
            })
            .expect("valid diffs sum")
            .encode();

        let key = PartialBatchKey::new(writer, &PartId::new());
        let blob_key = key.complete(&shard_id);
        let bytes = Bytes::from(prost::Message::encode_to_vec(&data.into_proto()));
        let () = retry_external(&metrics.retries.external.hollow_run_set, || {
            blob.set(&blob_key, bytes.clone())
        })
        .await;
        Self {
            key,
            hollow_bytes,
            max_part_bytes,
            key_lower,
            structured_key_lower,
            diffs_sum: Some(diffs_sum),
            _phantom_data: Default::default(),
        }
    }

    /// Retrieve the [HollowRun] that this reference points to.
    /// The caller is expected to ensure that this ref is the result of calling [HollowRunRef::set]
    /// with the same shard id and backing store.
    pub async fn get(
        &self,
        shard_id: ShardId,
        blob: &dyn Blob,
        metrics: &Metrics,
    ) -> Option<HollowRun<T>> {
        let blob_key = self.key.complete(&shard_id);
        let mut bytes = retry_external(&metrics.retries.external.hollow_run_get, || {
            blob.get(&blob_key)
        })
        .await?;
        let proto_runs: ProtoHollowRun =
            prost::Message::decode(&mut bytes).expect("illegal state: invalid proto bytes");
        let runs = proto_runs
            .into_rust()
            .expect("illegal state: invalid encoded runs proto");
        Some(runs)
    }
}

/// Part of the updates in a run.
///
/// Either a pointer to ones stored in Blob or a single part stored inline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum RunPart<T> {
    Single(BatchPart<T>),
    Many(HollowRunRef<T>),
}

impl<T: Ord> PartialOrd<Self> for RunPart<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for RunPart<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (RunPart::Single(a), RunPart::Single(b)) => a.cmp(b),
            (RunPart::Single(_), RunPart::Many(_)) => Ordering::Less,
            (RunPart::Many(_), RunPart::Single(_)) => Ordering::Greater,
            (RunPart::Many(a), RunPart::Many(b)) => a.cmp(b),
        }
    }
}

impl<T> RunPart<T> {
    #[cfg(test)]
    pub fn expect_hollow_part(&self) -> &HollowBatchPart<T> {
        match self {
            RunPart::Single(BatchPart::Hollow(hollow)) => hollow,
            _ => panic!("expected hollow part!"),
        }
    }

    pub fn hollow_bytes(&self) -> usize {
        match self {
            Self::Single(p) => p.hollow_bytes(),
            Self::Many(r) => r.hollow_bytes,
        }
    }

    pub fn is_inline(&self) -> bool {
        match self {
            Self::Single(p) => p.is_inline(),
            Self::Many(_) => false,
        }
    }

    pub fn inline_bytes(&self) -> usize {
        match self {
            Self::Single(p) => p.inline_bytes(),
            Self::Many(_) => 0,
        }
    }

    pub fn max_part_bytes(&self) -> usize {
        match self {
            Self::Single(p) => p.encoded_size_bytes(),
            Self::Many(r) => r.max_part_bytes,
        }
    }

    pub fn writer_key(&self) -> Option<WriterKey> {
        match self {
            Self::Single(p) => p.writer_key(),
            Self::Many(r) => r.writer_key(),
        }
    }

    pub fn encoded_size_bytes(&self) -> usize {
        match self {
            Self::Single(p) => p.encoded_size_bytes(),
            Self::Many(r) => r.hollow_bytes,
        }
    }

    pub fn schema_id(&self) -> Option<SchemaId> {
        match self {
            Self::Single(p) => p.schema_id(),
            Self::Many(_) => None,
        }
    }

    // A user-interpretable identifier or description of the part (for logs and
    // such).
    pub fn printable_name(&self) -> &str {
        match self {
            Self::Single(p) => p.printable_name(),
            Self::Many(r) => r.key.0.as_str(),
        }
    }

    pub fn stats(&self) -> Option<&LazyPartStats> {
        match self {
            Self::Single(p) => p.stats(),
            // TODO: if we kept stats we could avoid fetching the metadata here.
            Self::Many(_) => None,
        }
    }

    pub fn key_lower(&self) -> &[u8] {
        match self {
            Self::Single(p) => p.key_lower(),
            Self::Many(r) => r.key_lower.as_slice(),
        }
    }

    pub fn structured_key_lower(&self) -> Option<ArrayBound> {
        match self {
            Self::Single(p) => p.structured_key_lower(),
            Self::Many(_) => None,
        }
    }

    pub fn ts_rewrite(&self) -> Option<&Antichain<T>> {
        match self {
            Self::Single(p) => p.ts_rewrite(),
            Self::Many(_) => None,
        }
    }
}

impl<T> RunPart<T>
where
    T: Timestamp + Codec64,
{
    pub fn diffs_sum<D: Codec64 + Monoid>(&self, metrics: &ColumnarMetrics) -> Option<D> {
        match self {
            Self::Single(p) => p.diffs_sum(metrics),
            Self::Many(hollow_run) => hollow_run.diffs_sum.map(D::decode),
        }
    }
}

/// A blob was missing!
#[derive(Clone, Debug)]
pub struct MissingBlob(BlobKey);

impl std::fmt::Display for MissingBlob {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "unexpectedly missing key: {}", self.0)
    }
}

impl std::error::Error for MissingBlob {}

impl<T: Timestamp + Codec64 + Sync> RunPart<T> {
    pub fn part_stream<'a>(
        &'a self,
        shard_id: ShardId,
        blob: &'a dyn Blob,
        metrics: &'a Metrics,
    ) -> impl Stream<Item = Result<Cow<'a, BatchPart<T>>, MissingBlob>> + Send + 'a {
        try_stream! {
            match self {
                RunPart::Single(p) => {
                    yield Cow::Borrowed(p);
                }
                RunPart::Many(r) => {
                    let fetched = r.get(shard_id, blob, metrics).await.ok_or_else(|| MissingBlob(r.key.complete(&shard_id)))?;
                    for run_part in fetched.parts {
                        for await batch_part in run_part.part_stream(shard_id, blob, metrics).boxed() {
                            yield Cow::Owned(batch_part?.into_owned());
                        }
                    }
                }
            }
        }
    }
}

impl<T: Ord> PartialOrd for BatchPart<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for BatchPart<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (BatchPart::Hollow(s), BatchPart::Hollow(o)) => s.cmp(o),
            (
                BatchPart::Inline {
                    updates: s_updates,
                    ts_rewrite: s_ts_rewrite,
                    schema_id: s_schema_id,
                    deprecated_schema_id: s_deprecated_schema_id,
                },
                BatchPart::Inline {
                    updates: o_updates,
                    ts_rewrite: o_ts_rewrite,
                    schema_id: o_schema_id,
                    deprecated_schema_id: o_deprecated_schema_id,
                },
            ) => (
                s_updates,
                s_ts_rewrite.as_ref().map(|x| x.elements()),
                s_schema_id,
                s_deprecated_schema_id,
            )
                .cmp(&(
                    o_updates,
                    o_ts_rewrite.as_ref().map(|x| x.elements()),
                    o_schema_id,
                    o_deprecated_schema_id,
                )),
            (BatchPart::Hollow(_), BatchPart::Inline { .. }) => Ordering::Less,
            (BatchPart::Inline { .. }, BatchPart::Hollow(_)) => Ordering::Greater,
        }
    }
}

/// What order are the parts in this run in?
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize)]
pub(crate) enum RunOrder {
    /// They're in no particular order.
    Unordered,
    /// They're ordered based on the codec-encoded K/V bytes.
    Codec,
    /// They're ordered by the natural ordering of the structured data.
    Structured,
}

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Serialize, Copy, Hash)]
pub struct RunId(pub(crate) [u8; 16]);

impl std::fmt::Display for RunId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ri{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for RunId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RunId({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for RunId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id("ri", "RunId", s).map(RunId)
    }
}

impl From<RunId> for String {
    fn from(x: RunId) -> Self {
        x.to_string()
    }
}

impl RunId {
    pub(crate) fn new() -> Self {
        RunId(*Uuid::new_v4().as_bytes())
    }
}

impl Arbitrary for RunId {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;
    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        Strategy::prop_map(proptest::prelude::any::<u128>(), |n| {
            RunId(*Uuid::from_u128(n).as_bytes())
        })
        .boxed()
    }
}

/// Metadata shared across a run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd, Serialize)]
pub struct RunMeta {
    /// If none, Persist should infer the order based on the proto metadata.
    pub(crate) order: Option<RunOrder>,
    /// All parts in a run should have the same schema.
    pub(crate) schema: Option<SchemaId>,

    /// ID of a schema that has since been deprecated and exists only to cleanly roundtrip.
    pub(crate) deprecated_schema: Option<SchemaId>,

    /// If set, a UUID that uniquely identifies this run.
    pub(crate) id: Option<RunId>,

    /// The number of updates in this run, or `None` if the number is unknown.
    pub(crate) len: Option<usize>,

    /// Additional unstructured metadata.
    #[serde(skip_serializing_if = "MetadataMap::is_empty")]
    pub(crate) meta: MetadataMap,
}

/// A subset of a [HollowBatch] corresponding 1:1 to a blob.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HollowBatchPart<T> {
    /// Pointer usable to retrieve the updates.
    pub key: PartialBatchKey,
    /// Miscellaneous metadata.
    #[serde(skip_serializing_if = "MetadataMap::is_empty")]
    pub meta: MetadataMap,
    /// The encoded size of this part.
    pub encoded_size_bytes: usize,
    /// A lower bound on the keys in the part. (By default, this the minimum
    /// possible key: `vec![]`.)
    #[serde(serialize_with = "serialize_part_bytes")]
    pub key_lower: Vec<u8>,
    /// A lower bound on the keys in the part, stored as structured data.
    #[serde(serialize_with = "serialize_lazy_proto")]
    pub structured_key_lower: Option<LazyProto<ProtoArrayData>>,
    /// Aggregate statistics about data contained in this part.
    #[serde(serialize_with = "serialize_part_stats")]
    pub stats: Option<LazyPartStats>,
    /// A frontier to which timestamps in this part are advanced on read, if
    /// set.
    ///
    /// A value of `Some([T::minimum()])` is functionally the same as `None`,
    /// but we maintain the distinction between the two for some internal sanity
    /// checking of invariants as well as metrics. If this ever becomes an
    /// issue, everything still works with this as just `Antichain<T>`.
    pub ts_rewrite: Option<Antichain<T>>,
    /// A Codec64 encoded sum of all diffs in this part, if known.
    ///
    /// This is `None` if this part was written before we started storing this
    /// information, or if it was written when the dyncfg was off.
    ///
    /// It could also make sense to model this as part of the pushdown stats, if
    /// we later decide that's of some benefit.
    #[serde(serialize_with = "serialize_diffs_sum")]
    pub diffs_sum: Option<[u8; 8]>,
    /// Columnar format that this batch was written in.
    ///
    /// This is `None` if this part was written before we started writing structured
    /// columnar data.
    pub format: Option<BatchColumnarFormat>,
    /// The schemas used to encode the data in this batch part.
    ///
    /// Or None for historical data written before the schema registry was
    /// added.
    pub schema_id: Option<SchemaId>,

    /// ID of a schema that has since been deprecated and exists only to cleanly roundtrip.
    pub deprecated_schema_id: Option<SchemaId>,
}

/// A [Batch] but with the updates themselves stored externally.
///
/// [Batch]: differential_dataflow::trace::BatchReader
#[derive(Clone, PartialEq, Eq)]
pub struct HollowBatch<T> {
    /// Describes the times of the updates in the batch.
    pub desc: Description<T>,
    /// The number of updates in the batch.
    pub len: usize,
    /// Pointers usable to retrieve the updates.
    pub(crate) parts: Vec<RunPart<T>>,
    /// Runs of sequential sorted batch parts, stored as indices into `parts`.
    /// ex.
    /// ```text
    ///     parts=[p1, p2, p3], runs=[]     --> run  is  [p1, p2, p2]
    ///     parts=[p1, p2, p3], runs=[1]    --> runs are [p1] and [p2, p3]
    ///     parts=[p1, p2, p3], runs=[1, 2] --> runs are [p1], [p2], [p3]
    /// ```
    pub(crate) run_splits: Vec<usize>,
    /// Run-level metadata: the first entry has metadata for the first run, and so on.
    /// If there's no corresponding entry for a particular run, it's assumed to be [RunMeta::default()].
    pub(crate) run_meta: Vec<RunMeta>,
}

impl<T: Debug> Debug for HollowBatch<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let HollowBatch {
            desc,
            parts,
            len,
            run_splits: runs,
            run_meta,
        } = self;
        f.debug_struct("HollowBatch")
            .field(
                "desc",
                &(
                    desc.lower().elements(),
                    desc.upper().elements(),
                    desc.since().elements(),
                ),
            )
            .field("parts", &parts)
            .field("len", &len)
            .field("runs", &runs)
            .field("run_meta", &run_meta)
            .finish()
    }
}

impl<T: Serialize> serde::Serialize for HollowBatch<T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let HollowBatch {
            desc,
            len,
            // Both parts and runs are covered by the self.runs call.
            parts: _,
            run_splits: _,
            run_meta: _,
        } = self;
        let mut s = s.serialize_struct("HollowBatch", 5)?;
        let () = s.serialize_field("lower", &desc.lower().elements())?;
        let () = s.serialize_field("upper", &desc.upper().elements())?;
        let () = s.serialize_field("since", &desc.since().elements())?;
        let () = s.serialize_field("len", len)?;
        let () = s.serialize_field("part_runs", &self.runs().collect::<Vec<_>>())?;
        s.end()
    }
}

impl<T: Ord> PartialOrd for HollowBatch<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for HollowBatch<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Deconstruct self and other so we get a compile failure if new fields
        // are added.
        let HollowBatch {
            desc: self_desc,
            parts: self_parts,
            len: self_len,
            run_splits: self_runs,
            run_meta: self_run_meta,
        } = self;
        let HollowBatch {
            desc: other_desc,
            parts: other_parts,
            len: other_len,
            run_splits: other_runs,
            run_meta: other_run_meta,
        } = other;
        (
            self_desc.lower().elements(),
            self_desc.upper().elements(),
            self_desc.since().elements(),
            self_parts,
            self_len,
            self_runs,
            self_run_meta,
        )
            .cmp(&(
                other_desc.lower().elements(),
                other_desc.upper().elements(),
                other_desc.since().elements(),
                other_parts,
                other_len,
                other_runs,
                other_run_meta,
            ))
    }
}

impl<T: Timestamp + Codec64 + Sync> HollowBatch<T> {
    pub(crate) fn part_stream<'a>(
        &'a self,
        shard_id: ShardId,
        blob: &'a dyn Blob,
        metrics: &'a Metrics,
    ) -> impl Stream<Item = Result<Cow<'a, BatchPart<T>>, MissingBlob>> + 'a {
        stream! {
            for part in &self.parts {
                for await part in part.part_stream(shard_id, blob, metrics) {
                    yield part;
                }
            }
        }
    }
}
impl<T> HollowBatch<T> {
    /// Construct an in-memory hollow batch from the given metadata.
    ///
    /// This method checks that `runs` is a sequence of valid indices into `parts`. The caller
    /// is responsible for ensuring that the defined runs are valid.
    ///
    /// `len` should represent the number of valid updates in the referenced parts.
    pub(crate) fn new(
        desc: Description<T>,
        parts: Vec<RunPart<T>>,
        len: usize,
        run_meta: Vec<RunMeta>,
        run_splits: Vec<usize>,
    ) -> Self {
        debug_assert!(
            run_splits.is_strictly_sorted(),
            "run indices should be strictly increasing"
        );
        debug_assert!(
            run_splits.first().map_or(true, |i| *i > 0),
            "run indices should be positive"
        );
        debug_assert!(
            run_splits.last().map_or(true, |i| *i < parts.len()),
            "run indices should be valid indices into parts"
        );
        debug_assert!(
            parts.is_empty() || run_meta.len() == run_splits.len() + 1,
            "all metadata should correspond to a run"
        );

        Self {
            desc,
            len,
            parts,
            run_splits,
            run_meta,
        }
    }

    /// Construct a batch of a single run with default metadata. Mostly interesting for tests.
    pub(crate) fn new_run(desc: Description<T>, parts: Vec<RunPart<T>>, len: usize) -> Self {
        let run_meta = if parts.is_empty() {
            vec![]
        } else {
            vec![RunMeta::default()]
        };
        Self {
            desc,
            len,
            parts,
            run_splits: vec![],
            run_meta,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_run_for_test(
        desc: Description<T>,
        parts: Vec<RunPart<T>>,
        len: usize,
        run_id: RunId,
    ) -> Self {
        let run_meta = if parts.is_empty() {
            vec![]
        } else {
            let mut meta = RunMeta::default();
            meta.id = Some(run_id);
            vec![meta]
        };
        Self {
            desc,
            len,
            parts,
            run_splits: vec![],
            run_meta,
        }
    }

    /// An empty hollow batch, representing no updates over the given desc.
    pub(crate) fn empty(desc: Description<T>) -> Self {
        Self {
            desc,
            len: 0,
            parts: vec![],
            run_splits: vec![],
            run_meta: vec![],
        }
    }

    pub(crate) fn runs(&self) -> impl Iterator<Item = (&RunMeta, &[RunPart<T>])> {
        let run_ends = self
            .run_splits
            .iter()
            .copied()
            .chain(std::iter::once(self.parts.len()));
        let run_metas = self.run_meta.iter();
        let run_parts = run_ends
            .scan(0, |start, end| {
                let range = *start..end;
                *start = end;
                Some(range)
            })
            .filter(|range| !range.is_empty())
            .map(|range| &self.parts[range]);
        run_metas.zip_eq(run_parts)
    }

    pub(crate) fn inline_bytes(&self) -> usize {
        self.parts.iter().map(|x| x.inline_bytes()).sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    pub(crate) fn part_count(&self) -> usize {
        self.parts.len()
    }

    /// The sum of the encoded sizes of all parts in the batch.
    pub fn encoded_size_bytes(&self) -> usize {
        self.parts.iter().map(|p| p.encoded_size_bytes()).sum()
    }
}

// See the comment on [Batch::rewrite_ts] for why this is TotalOrder.
impl<T: Timestamp + TotalOrder> HollowBatch<T> {
    pub(crate) fn rewrite_ts(
        &mut self,
        frontier: &Antichain<T>,
        new_upper: Antichain<T>,
    ) -> Result<(), String> {
        if !PartialOrder::less_than(frontier, &new_upper) {
            return Err(format!(
                "rewrite frontier {:?} !< rewrite upper {:?}",
                frontier.elements(),
                new_upper.elements(),
            ));
        }
        if PartialOrder::less_than(&new_upper, self.desc.upper()) {
            return Err(format!(
                "rewrite upper {:?} < batch upper {:?}",
                new_upper.elements(),
                self.desc.upper().elements(),
            ));
        }

        // The following are things that it seems like we could support, but
        // initially we don't because we don't have a use case for them.
        if PartialOrder::less_than(frontier, self.desc.lower()) {
            return Err(format!(
                "rewrite frontier {:?} < batch lower {:?}",
                frontier.elements(),
                self.desc.lower().elements(),
            ));
        }
        if self.desc.since() != &Antichain::from_elem(T::minimum()) {
            return Err(format!(
                "batch since {:?} != minimum antichain {:?}",
                self.desc.since().elements(),
                &[T::minimum()],
            ));
        }
        for part in self.parts.iter() {
            let Some(ts_rewrite) = part.ts_rewrite() else {
                continue;
            };
            if PartialOrder::less_than(frontier, ts_rewrite) {
                return Err(format!(
                    "rewrite frontier {:?} < batch rewrite {:?}",
                    frontier.elements(),
                    ts_rewrite.elements(),
                ));
            }
        }

        self.desc = Description::new(
            self.desc.lower().clone(),
            new_upper,
            self.desc.since().clone(),
        );
        for part in &mut self.parts {
            match part {
                RunPart::Single(BatchPart::Hollow(part)) => {
                    part.ts_rewrite = Some(frontier.clone())
                }
                RunPart::Single(BatchPart::Inline { ts_rewrite, .. }) => {
                    *ts_rewrite = Some(frontier.clone())
                }
                RunPart::Many(runs) => {
                    // Currently unreachable: we only apply rewrites to user batches, and we don't
                    // ever generate runs of >1 part for those.
                    panic!("unexpected rewrite of a hollow runs ref: {runs:?}");
                }
            }
        }
        Ok(())
    }
}

impl<T: Ord> PartialOrd for HollowBatchPart<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for HollowBatchPart<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Deconstruct self and other so we get a compile failure if new fields
        // are added.
        let HollowBatchPart {
            key: self_key,
            meta: self_meta,
            encoded_size_bytes: self_encoded_size_bytes,
            key_lower: self_key_lower,
            structured_key_lower: self_structured_key_lower,
            stats: self_stats,
            ts_rewrite: self_ts_rewrite,
            diffs_sum: self_diffs_sum,
            format: self_format,
            schema_id: self_schema_id,
            deprecated_schema_id: self_deprecated_schema_id,
        } = self;
        let HollowBatchPart {
            key: other_key,
            meta: other_meta,
            encoded_size_bytes: other_encoded_size_bytes,
            key_lower: other_key_lower,
            structured_key_lower: other_structured_key_lower,
            stats: other_stats,
            ts_rewrite: other_ts_rewrite,
            diffs_sum: other_diffs_sum,
            format: other_format,
            schema_id: other_schema_id,
            deprecated_schema_id: other_deprecated_schema_id,
        } = other;
        (
            self_key,
            self_meta,
            self_encoded_size_bytes,
            self_key_lower,
            self_structured_key_lower,
            self_stats,
            self_ts_rewrite.as_ref().map(|x| x.elements()),
            self_diffs_sum,
            self_format,
            self_schema_id,
            self_deprecated_schema_id,
        )
            .cmp(&(
                other_key,
                other_meta,
                other_encoded_size_bytes,
                other_key_lower,
                other_structured_key_lower,
                other_stats,
                other_ts_rewrite.as_ref().map(|x| x.elements()),
                other_diffs_sum,
                other_format,
                other_schema_id,
                other_deprecated_schema_id,
            ))
    }
}

/// A pointer to a rollup stored externally.
#[derive(Arbitrary, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct HollowRollup {
    /// Pointer usable to retrieve the rollup.
    pub key: PartialRollupKey,
    /// The encoded size of this rollup, if known.
    pub encoded_size_bytes: Option<usize>,
}

/// A pointer to a blob stored externally.
#[derive(Debug)]
pub enum HollowBlobRef<'a, T> {
    Batch(&'a HollowBatch<T>),
    Rollup(&'a HollowRollup),
}

/// A rollup that is currently being computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Arbitrary, Serialize)]
pub struct ActiveRollup {
    pub seqno: SeqNo,
    pub start_ms: u64,
}

/// A garbage collection request that is currently being computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Arbitrary, Serialize)]
pub struct ActiveGc {
    pub seqno: SeqNo,
    pub start_ms: u64,
}

/// A sentinel for a state transition that was a no-op.
///
/// Critically, this also indicates that the no-op state transition was not
/// committed through compare_and_append and thus is _not linearized_.
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct NoOpStateTransition<T>(pub T);

// TODO: Document invariants.
#[derive(Debug, Clone)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct StateCollections<T> {
    /// The version of this state. This is typically identical to the version of the code
    /// that wrote it, but may diverge during 0dt upgrades and similar operations when a
    /// new version of code is intentionally interoperating with an older state format.
    pub(crate) version: Version,

    // - Invariant: `<= all reader.since`
    // - Invariant: Doesn't regress across state versions.
    pub(crate) last_gc_req: SeqNo,

    // - Invariant: There is a rollup with `seqno <= self.seqno_since`.
    pub(crate) rollups: BTreeMap<SeqNo, HollowRollup>,

    /// The rollup that is currently being computed.
    pub(crate) active_rollup: Option<ActiveRollup>,
    /// The gc request that is currently being computed.
    pub(crate) active_gc: Option<ActiveGc>,

    pub(crate) leased_readers: BTreeMap<LeasedReaderId, LeasedReaderState<T>>,
    pub(crate) critical_readers: BTreeMap<CriticalReaderId, CriticalReaderState<T>>,
    pub(crate) writers: BTreeMap<WriterId, WriterState<T>>,
    pub(crate) schemas: BTreeMap<SchemaId, EncodedSchemas>,

    // - Invariant: `trace.since == meet(all reader.since)`
    // - Invariant: `trace.since` doesn't regress across state versions.
    // - Invariant: `trace.upper` doesn't regress across state versions.
    // - Invariant: `trace` upholds its own invariants.
    pub(crate) trace: Trace<T>,
}

/// A key and val [Codec::Schema] encoded via [Codec::encode_schema].
///
/// This strategy of directly serializing the schema objects requires that
/// persist users do the right thing. Specifically, that an encoded schema
/// doesn't in some later version of mz decode to an in-mem object that acts
/// differently. In a sense, the current system (before the introduction of the
/// schema registry) where schemas are passed in unchecked to reader and writer
/// registration calls also has the same defect, so seems fine.
///
/// An alternative is to write down here some persist-specific representation of
/// the schema (e.g. the arrow DataType). This is a lot more work and also has
/// the potential to lead down a similar failure mode to the mz_persist_types
/// `Data` trait, where the boilerplate isn't worth the safety. Given that we
/// can always migrate later by rehydrating these, seems fine to start with the
/// easy thing.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EncodedSchemas {
    /// A full in-mem `K::Schema` impl encoded via [Codec::encode_schema].
    pub key: Bytes,
    /// The arrow `DataType` produced by this `K::Schema` at the time it was
    /// registered, encoded as a `ProtoDataType`.
    pub key_data_type: Bytes,
    /// A full in-mem `V::Schema` impl encoded via [Codec::encode_schema].
    pub val: Bytes,
    /// The arrow `DataType` produced by this `V::Schema` at the time it was
    /// registered, encoded as a `ProtoDataType`.
    pub val_data_type: Bytes,
}

impl EncodedSchemas {
    pub(crate) fn decode_data_type(buf: &[u8]) -> DataType {
        let proto = prost::Message::decode(buf).expect("valid ProtoDataType");
        DataType::from_proto(proto).expect("valid DataType")
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum CompareAndAppendBreak<T> {
    AlreadyCommitted,
    Upper {
        shard_upper: Antichain<T>,
        writer_upper: Antichain<T>,
    },
    InvalidUsage(InvalidUsage<T>),
    InlineBackpressure,
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum SnapshotErr<T> {
    AsOfNotYetAvailable(SeqNo, Upper<T>),
    AsOfHistoricalDistinctionsLost(Since<T>),
}

impl<T> StateCollections<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub fn add_rollup(
        &mut self,
        add_rollup: (SeqNo, &HollowRollup),
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        let (rollup_seqno, rollup) = add_rollup;
        let applied = match self.rollups.get(&rollup_seqno) {
            Some(x) => x.key == rollup.key,
            None => {
                self.active_rollup = None;
                self.rollups.insert(rollup_seqno, rollup.to_owned());
                true
            }
        };
        // This state transition is a no-op if applied is false but we
        // still commit the state change so that this gets linearized
        // (maybe we're looking at old state).
        Continue(applied)
    }

    pub fn remove_rollups(
        &mut self,
        remove_rollups: &[(SeqNo, PartialRollupKey)],
    ) -> ControlFlow<NoOpStateTransition<Vec<SeqNo>>, Vec<SeqNo>> {
        if remove_rollups.is_empty() || self.is_tombstone() {
            return Break(NoOpStateTransition(vec![]));
        }

        //This state transition is called at the end of the GC process, so we
        //need to unset the `active_gc` field.
        self.active_gc = None;

        let mut removed = vec![];
        for (seqno, key) in remove_rollups {
            let removed_key = self.rollups.remove(seqno);
            debug_assert!(
                removed_key.as_ref().map_or(true, |x| &x.key == key),
                "{} vs {:?}",
                key,
                removed_key
            );

            if removed_key.is_some() {
                removed.push(*seqno);
            }
        }

        Continue(removed)
    }

    pub fn register_leased_reader(
        &mut self,
        hostname: &str,
        reader_id: &LeasedReaderId,
        purpose: &str,
        seqno: SeqNo,
        lease_duration: Duration,
        heartbeat_timestamp_ms: u64,
        use_critical_since: bool,
    ) -> ControlFlow<
        NoOpStateTransition<(LeasedReaderState<T>, SeqNo)>,
        (LeasedReaderState<T>, SeqNo),
    > {
        let since = if use_critical_since {
            self.critical_since()
                .unwrap_or_else(|| self.trace.since().clone())
        } else {
            self.trace.since().clone()
        };
        let reader_state = LeasedReaderState {
            debug: HandleDebugState {
                hostname: hostname.to_owned(),
                purpose: purpose.to_owned(),
            },
            seqno,
            since,
            last_heartbeat_timestamp_ms: heartbeat_timestamp_ms,
            lease_duration_ms: u64::try_from(lease_duration.as_millis())
                .expect("lease duration as millis should fit within u64"),
        };

        // If the shard-global upper and since are both the empty antichain,
        // then no further writes can ever commit and no further reads can be
        // served. Optimize this by no-op-ing reader registration so that we can
        // settle the shard into a final unchanging tombstone state.
        if self.is_tombstone() {
            return Break(NoOpStateTransition((reader_state, self.seqno_since(seqno))));
        }

        // TODO: Handle if the reader or writer already exists.
        self.leased_readers
            .insert(reader_id.clone(), reader_state.clone());
        Continue((reader_state, self.seqno_since(seqno)))
    }

    pub fn register_critical_reader<O: Opaque + Codec64>(
        &mut self,
        hostname: &str,
        reader_id: &CriticalReaderId,
        purpose: &str,
    ) -> ControlFlow<NoOpStateTransition<CriticalReaderState<T>>, CriticalReaderState<T>> {
        let state = CriticalReaderState {
            debug: HandleDebugState {
                hostname: hostname.to_owned(),
                purpose: purpose.to_owned(),
            },
            since: self.trace.since().clone(),
            opaque: OpaqueState(Codec64::encode(&O::initial())),
            opaque_codec: O::codec_name(),
        };

        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(state));
        }

        let state = match self.critical_readers.get_mut(reader_id) {
            Some(existing_state) => {
                existing_state.debug = state.debug;
                existing_state.clone()
            }
            None => {
                self.critical_readers
                    .insert(reader_id.clone(), state.clone());
                state
            }
        };
        Continue(state)
    }

    pub fn register_schema<K: Codec, V: Codec>(
        &mut self,
        key_schema: &K::Schema,
        val_schema: &V::Schema,
    ) -> ControlFlow<NoOpStateTransition<Option<SchemaId>>, Option<SchemaId>> {
        fn encode_data_type(data_type: &DataType) -> Bytes {
            let proto = data_type.into_proto();
            prost::Message::encode_to_vec(&proto).into()
        }

        // Look for an existing registered SchemaId for these schemas.
        //
        // The common case is that this should be a recent one, so as a minor
        // optimization, do this search in reverse order.
        //
        // TODO: Note that this impl is `O(schemas)`. Combined with the
        // possibility of cmd retries, it's possible but unlikely for this to
        // get expensive. We could maintain a reverse map to speed this up in
        // necessary. This would either need to work on the encoded
        // representation (which, we'd have to fall back to the linear scan) or
        // we'd need to add a Hash/Ord bound to Schema.
        let existing_id = self.schemas.iter().rev().find(|(_, x)| {
            K::decode_schema(&x.key) == *key_schema && V::decode_schema(&x.val) == *val_schema
        });
        match existing_id {
            Some((schema_id, _)) => {
                // TODO: Validate that the decoded schemas still produce records
                // of the recorded DataType, to detect shenanigans. Probably
                // best to wait until we've turned on Schema2 in prod and thus
                // committed to the current mappings.
                Break(NoOpStateTransition(Some(*schema_id)))
            }
            None if self.is_tombstone() => {
                // TODO: Is this right?
                Break(NoOpStateTransition(None))
            }
            None if self.schemas.is_empty() => {
                // We'll have to do something more sophisticated here to
                // generate the next id if/when we start supporting the removal
                // of schemas.
                let id = SchemaId(self.schemas.len());
                let key_data_type = mz_persist_types::columnar::data_type::<K>(key_schema)
                    .expect("valid key schema");
                let val_data_type = mz_persist_types::columnar::data_type::<V>(val_schema)
                    .expect("valid val schema");
                let prev = self.schemas.insert(
                    id,
                    EncodedSchemas {
                        key: K::encode_schema(key_schema),
                        key_data_type: encode_data_type(&key_data_type),
                        val: V::encode_schema(val_schema),
                        val_data_type: encode_data_type(&val_data_type),
                    },
                );
                assert_eq!(prev, None);
                Continue(Some(id))
            }
            None => {
                info!(
                    "register_schemas got {:?} expected {:?}",
                    key_schema,
                    self.schemas
                        .iter()
                        .map(|(id, x)| (id, K::decode_schema(&x.key)))
                        .collect::<Vec<_>>()
                );
                // Until we implement persist schema changes, only allow at most
                // one registered schema.
                Break(NoOpStateTransition(None))
            }
        }
    }

    pub fn compare_and_evolve_schema<K: Codec, V: Codec>(
        &mut self,
        expected: SchemaId,
        key_schema: &K::Schema,
        val_schema: &V::Schema,
    ) -> ControlFlow<NoOpStateTransition<CaESchema<K, V>>, CaESchema<K, V>> {
        fn data_type<T>(schema: &impl Schema<T>) -> DataType {
            // To be defensive, create an empty batch and inspect the resulting
            // data type (as opposed to something like allowing the `Schema` to
            // declare the DataType).
            let array = Schema::encoder(schema).expect("valid schema").finish();
            Array::data_type(&array).clone()
        }

        let (current_id, current) = self
            .schemas
            .last_key_value()
            .expect("all shards have a schema");
        if *current_id != expected {
            return Break(NoOpStateTransition(CaESchema::ExpectedMismatch {
                schema_id: *current_id,
                key: K::decode_schema(&current.key),
                val: V::decode_schema(&current.val),
            }));
        }

        let current_key = K::decode_schema(&current.key);
        let current_key_dt = EncodedSchemas::decode_data_type(&current.key_data_type);
        let current_val = V::decode_schema(&current.val);
        let current_val_dt = EncodedSchemas::decode_data_type(&current.val_data_type);

        let key_dt = data_type(key_schema);
        let val_dt = data_type(val_schema);

        // If the schema is exactly the same as the current one, no-op.
        if current_key == *key_schema
            && current_key_dt == key_dt
            && current_val == *val_schema
            && current_val_dt == val_dt
        {
            return Break(NoOpStateTransition(CaESchema::Ok(*current_id)));
        }

        let key_fn = backward_compatible(&current_key_dt, &key_dt);
        let val_fn = backward_compatible(&current_val_dt, &val_dt);
        let (Some(key_fn), Some(val_fn)) = (key_fn, val_fn) else {
            return Break(NoOpStateTransition(CaESchema::Incompatible));
        };
        // Persist initially disallows dropping columns. This would require a
        // bunch more work (e.g. not safe to use the latest schema in
        // compaction) and isn't initially necessary in mz.
        if key_fn.contains_drop() || val_fn.contains_drop() {
            return Break(NoOpStateTransition(CaESchema::Incompatible));
        }

        // We'll have to do something more sophisticated here to
        // generate the next id if/when we start supporting the removal
        // of schemas.
        let id = SchemaId(self.schemas.len());
        self.schemas.insert(
            id,
            EncodedSchemas {
                key: K::encode_schema(key_schema),
                key_data_type: prost::Message::encode_to_vec(&key_dt.into_proto()).into(),
                val: V::encode_schema(val_schema),
                val_data_type: prost::Message::encode_to_vec(&val_dt.into_proto()).into(),
            },
        );
        Continue(CaESchema::Ok(id))
    }

    pub fn compare_and_append(
        &mut self,
        batch: &HollowBatch<T>,
        writer_id: &WriterId,
        heartbeat_timestamp_ms: u64,
        lease_duration_ms: u64,
        idempotency_token: &IdempotencyToken,
        debug_info: &HandleDebugState,
        inline_writes_total_max_bytes: usize,
        claim_compaction_percent: usize,
        claim_compaction_min_version: Option<&Version>,
    ) -> ControlFlow<CompareAndAppendBreak<T>, Vec<FueledMergeReq<T>>> {
        // We expire all writers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            assert_eq!(self.trace.upper(), &Antichain::new());
            return Break(CompareAndAppendBreak::Upper {
                shard_upper: Antichain::new(),
                // This writer might have been registered before the shard upper
                // was advanced, which would make this pessimistic in the
                // Indeterminate handling of compare_and_append at the machine
                // level, but that's fine.
                writer_upper: Antichain::new(),
            });
        }

        let writer_state = self
            .writers
            .entry(writer_id.clone())
            .or_insert_with(|| WriterState {
                last_heartbeat_timestamp_ms: heartbeat_timestamp_ms,
                lease_duration_ms,
                most_recent_write_token: IdempotencyToken::SENTINEL,
                most_recent_write_upper: Antichain::from_elem(T::minimum()),
                debug: debug_info.clone(),
            });

        if PartialOrder::less_than(batch.desc.upper(), batch.desc.lower()) {
            return Break(CompareAndAppendBreak::InvalidUsage(
                InvalidUsage::InvalidBounds {
                    lower: batch.desc.lower().clone(),
                    upper: batch.desc.upper().clone(),
                },
            ));
        }

        // If the time interval is empty, the list of updates must also be
        // empty.
        if batch.desc.upper() == batch.desc.lower() && !batch.is_empty() {
            return Break(CompareAndAppendBreak::InvalidUsage(
                InvalidUsage::InvalidEmptyTimeInterval {
                    lower: batch.desc.lower().clone(),
                    upper: batch.desc.upper().clone(),
                    keys: batch
                        .parts
                        .iter()
                        .map(|x| x.printable_name().to_owned())
                        .collect(),
                },
            ));
        }

        if idempotency_token == &writer_state.most_recent_write_token {
            // If the last write had the same idempotency_token, then this must
            // have already committed. Sanity check that the most recent write
            // upper matches and that the shard upper is at least the write
            // upper, if it's not something very suspect is going on.
            assert_eq!(batch.desc.upper(), &writer_state.most_recent_write_upper);
            assert!(
                PartialOrder::less_equal(batch.desc.upper(), self.trace.upper()),
                "{:?} vs {:?}",
                batch.desc.upper(),
                self.trace.upper()
            );
            return Break(CompareAndAppendBreak::AlreadyCommitted);
        }

        let shard_upper = self.trace.upper();
        if shard_upper != batch.desc.lower() {
            return Break(CompareAndAppendBreak::Upper {
                shard_upper: shard_upper.clone(),
                writer_upper: writer_state.most_recent_write_upper.clone(),
            });
        }

        let new_inline_bytes = batch.inline_bytes();
        if new_inline_bytes > 0 {
            let mut existing_inline_bytes = 0;
            self.trace
                .map_batches(|x| existing_inline_bytes += x.inline_bytes());
            // TODO: For very small batches, it may actually _increase_ the size
            // of state to flush them out. Consider another threshold under
            // which an inline part can be appended no matter what.
            if existing_inline_bytes + new_inline_bytes >= inline_writes_total_max_bytes {
                return Break(CompareAndAppendBreak::InlineBackpressure);
            }
        }

        let mut merge_reqs = if batch.desc.upper() != batch.desc.lower() {
            self.trace.push_batch(batch.clone())
        } else {
            Vec::new()
        };

        // NB: we don't claim unclaimed compactions when the recording flag is off, even if we'd
        // otherwise be allowed to, to avoid triggering the same compactions in every writer.
        let all_empty_reqs = merge_reqs
            .iter()
            .all(|req| req.inputs.iter().all(|b| b.batch.is_empty()));
        if all_empty_reqs && !batch.is_empty() {
            let mut reqs_to_take = claim_compaction_percent / 100;
            if (usize::cast_from(idempotency_token.hashed()) % 100)
                < (claim_compaction_percent % 100)
            {
                reqs_to_take += 1;
            }
            let threshold_ms = heartbeat_timestamp_ms.saturating_sub(lease_duration_ms);
            let min_writer = claim_compaction_min_version.map(WriterKey::for_version);
            merge_reqs.extend(
                // We keep the oldest `reqs_to_take` batches, under the theory that they're least
                // likely to be compacted soon for other reasons.
                self.trace
                    .fueled_merge_reqs_before_ms(threshold_ms, min_writer)
                    .take(reqs_to_take),
            )
        }

        for req in &merge_reqs {
            self.trace.claim_compaction(
                req.id,
                ActiveCompaction {
                    start_ms: heartbeat_timestamp_ms,
                },
            )
        }

        debug_assert_eq!(self.trace.upper(), batch.desc.upper());
        writer_state.most_recent_write_token = idempotency_token.clone();
        // The writer's most recent upper should only go forward.
        assert!(
            PartialOrder::less_equal(&writer_state.most_recent_write_upper, batch.desc.upper()),
            "{:?} vs {:?}",
            &writer_state.most_recent_write_upper,
            batch.desc.upper()
        );
        writer_state
            .most_recent_write_upper
            .clone_from(batch.desc.upper());

        // Heartbeat the writer state to keep our idempotency token alive.
        writer_state.last_heartbeat_timestamp_ms = std::cmp::max(
            heartbeat_timestamp_ms,
            writer_state.last_heartbeat_timestamp_ms,
        );

        Continue(merge_reqs)
    }

    pub fn apply_merge_res<D: Codec64 + Monoid + PartialEq>(
        &mut self,
        res: &FueledMergeRes<T>,
        metrics: &ColumnarMetrics,
    ) -> ControlFlow<NoOpStateTransition<ApplyMergeResult>, ApplyMergeResult> {
        // We expire all writers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(ApplyMergeResult::NotAppliedNoMatch));
        }

        let apply_merge_result = self.trace.apply_merge_res_checked::<D>(res, metrics);
        Continue(apply_merge_result)
    }

    pub fn spine_exert(
        &mut self,
        fuel: usize,
    ) -> ControlFlow<NoOpStateTransition<Vec<FueledMergeReq<T>>>, Vec<FueledMergeReq<T>>> {
        let (merge_reqs, did_work) = self.trace.exert(fuel);
        if did_work {
            Continue(merge_reqs)
        } else {
            assert!(merge_reqs.is_empty());
            // Break if we have nothing useful to do to save the seqno (and
            // resulting crdb traffic)
            Break(NoOpStateTransition(Vec::new()))
        }
    }

    pub fn downgrade_since(
        &mut self,
        reader_id: &LeasedReaderId,
        seqno: SeqNo,
        outstanding_seqno: Option<SeqNo>,
        new_since: &Antichain<T>,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<NoOpStateTransition<Since<T>>, Since<T>> {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(Since(Antichain::new())));
        }

        // The only way to have a missing reader in state is if it's been expired... and in that
        // case, we behave the same as though that reader had been downgraded to the empty antichain.
        let Some(reader_state) = self.leased_reader(reader_id) else {
            tracing::warn!(
                "Leased reader {reader_id} was expired due to inactivity. Did the machine go to sleep?",
            );
            return Break(NoOpStateTransition(Since(Antichain::new())));
        };

        // Also use this as an opportunity to heartbeat the reader and downgrade
        // the seqno capability.
        reader_state.last_heartbeat_timestamp_ms = std::cmp::max(
            heartbeat_timestamp_ms,
            reader_state.last_heartbeat_timestamp_ms,
        );

        let seqno = match outstanding_seqno {
            Some(outstanding_seqno) => {
                assert!(
                    outstanding_seqno >= reader_state.seqno,
                    "SeqNos cannot go backward; however, oldest leased SeqNo ({:?}) \
                    is behind current reader_state ({:?})",
                    outstanding_seqno,
                    reader_state.seqno,
                );
                std::cmp::min(outstanding_seqno, seqno)
            }
            None => seqno,
        };

        reader_state.seqno = seqno;

        let reader_current_since = if PartialOrder::less_than(&reader_state.since, new_since) {
            reader_state.since.clone_from(new_since);
            self.update_since();
            new_since.clone()
        } else {
            // No-op, but still commit the state change so that this gets
            // linearized.
            reader_state.since.clone()
        };

        Continue(Since(reader_current_since))
    }

    pub fn compare_and_downgrade_since<O: Opaque + Codec64>(
        &mut self,
        reader_id: &CriticalReaderId,
        expected_opaque: &O,
        (new_opaque, new_since): (&O, &Antichain<T>),
    ) -> ControlFlow<
        NoOpStateTransition<Result<Since<T>, (O, Since<T>)>>,
        Result<Since<T>, (O, Since<T>)>,
    > {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            // Match the idempotence behavior below of ignoring the token if
            // since is already advanced enough (in this case, because it's a
            // tombstone, we know it's the empty antichain).
            return Break(NoOpStateTransition(Ok(Since(Antichain::new()))));
        }

        let reader_state = self.critical_reader(reader_id);
        assert_eq!(reader_state.opaque_codec, O::codec_name());

        if &O::decode(reader_state.opaque.0) != expected_opaque {
            // No-op, but still commit the state change so that this gets
            // linearized.
            return Continue(Err((
                Codec64::decode(reader_state.opaque.0),
                Since(reader_state.since.clone()),
            )));
        }

        reader_state.opaque = OpaqueState(Codec64::encode(new_opaque));
        if PartialOrder::less_equal(&reader_state.since, new_since) {
            reader_state.since.clone_from(new_since);
            self.update_since();
            Continue(Ok(Since(new_since.clone())))
        } else {
            // no work to be done -- the reader state's `since` is already sufficiently
            // advanced. we may someday need to revisit this branch when it's possible
            // for two `since` frontiers to be incomparable.
            Continue(Ok(Since(reader_state.since.clone())))
        }
    }

    pub fn heartbeat_leased_reader(
        &mut self,
        reader_id: &LeasedReaderId,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(false));
        }

        match self.leased_readers.get_mut(reader_id) {
            Some(reader_state) => {
                reader_state.last_heartbeat_timestamp_ms = std::cmp::max(
                    heartbeat_timestamp_ms,
                    reader_state.last_heartbeat_timestamp_ms,
                );
                Continue(true)
            }
            // No-op, but we still commit the state change so that this gets
            // linearized (maybe we're looking at old state).
            None => Continue(false),
        }
    }

    pub fn expire_leased_reader(
        &mut self,
        reader_id: &LeasedReaderId,
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(false));
        }

        let existed = self.leased_readers.remove(reader_id).is_some();
        if existed {
            // TODO(database-issues#6885): Re-enable this
            //
            // Temporarily disabling this because we think it might be the cause
            // of the remap since bug. Specifically, a clusterd process has a
            // ReadHandle for maintaining the once and one inside a Listen. If
            // we crash and stay down for longer than the read lease duration,
            // it's possible that an expiry of them both in quick succession
            // jumps the since forward to the Listen one.
            //
            // Don't forget to update the downgrade_since when this gets
            // switched back on.
            //
            // self.update_since();
        }
        // No-op if existed is false, but still commit the state change so that
        // this gets linearized.
        Continue(existed)
    }

    pub fn expire_critical_reader(
        &mut self,
        reader_id: &CriticalReaderId,
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(false));
        }

        let existed = self.critical_readers.remove(reader_id).is_some();
        if existed {
            // TODO(database-issues#6885): Re-enable this
            //
            // Temporarily disabling this because we think it might be the cause
            // of the remap since bug. Specifically, a clusterd process has a
            // ReadHandle for maintaining the once and one inside a Listen. If
            // we crash and stay down for longer than the read lease duration,
            // it's possible that an expiry of them both in quick succession
            // jumps the since forward to the Listen one.
            //
            // Don't forget to update the downgrade_since when this gets
            // switched back on.
            //
            // self.update_since();
        }
        // This state transition is a no-op if existed is false, but we still
        // commit the state change so that this gets linearized (maybe we're
        // looking at old state).
        Continue(existed)
    }

    pub fn expire_writer(
        &mut self,
        writer_id: &WriterId,
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        // We expire all writers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(false));
        }

        let existed = self.writers.remove(writer_id).is_some();
        // This state transition is a no-op if existed is false, but we still
        // commit the state change so that this gets linearized (maybe we're
        // looking at old state).
        Continue(existed)
    }

    fn leased_reader(&mut self, id: &LeasedReaderId) -> Option<&mut LeasedReaderState<T>> {
        self.leased_readers.get_mut(id)
    }

    fn critical_reader(&mut self, id: &CriticalReaderId) -> &mut CriticalReaderState<T> {
        self.critical_readers
            .get_mut(id)
            .unwrap_or_else(|| {
                panic!(
                    "Unknown CriticalReaderId({}). It was either never registered, or has been manually expired.",
                    id
                )
            })
    }

    fn critical_since(&self) -> Option<Antichain<T>> {
        let mut critical_sinces = self.critical_readers.values().map(|r| &r.since);
        let mut since = critical_sinces.next().cloned()?;
        for s in critical_sinces {
            since.meet_assign(s);
        }
        Some(since)
    }

    fn update_since(&mut self) {
        let mut sinces_iter = self
            .leased_readers
            .values()
            .map(|x| &x.since)
            .chain(self.critical_readers.values().map(|x| &x.since));
        let mut since = match sinces_iter.next() {
            Some(since) => since.clone(),
            None => {
                // If there are no current readers, leave `since` unchanged so
                // it doesn't regress.
                return;
            }
        };
        while let Some(s) = sinces_iter.next() {
            since.meet_assign(s);
        }
        self.trace.downgrade_since(&since);
    }

    fn seqno_since(&self, seqno: SeqNo) -> SeqNo {
        let mut seqno_since = seqno;
        for cap in self.leased_readers.values() {
            seqno_since = std::cmp::min(seqno_since, cap.seqno);
        }
        // critical_readers don't hold a seqno capability.
        seqno_since
    }

    fn tombstone_batch() -> HollowBatch<T> {
        HollowBatch::empty(Description::new(
            Antichain::from_elem(T::minimum()),
            Antichain::new(),
            Antichain::new(),
        ))
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        self.trace.upper().is_empty()
            && self.trace.since().is_empty()
            && self.writers.is_empty()
            && self.leased_readers.is_empty()
            && self.critical_readers.is_empty()
    }

    pub(crate) fn is_single_empty_batch(&self) -> bool {
        let mut batch_count = 0;
        let mut is_empty = true;
        self.trace.map_batches(|b| {
            batch_count += 1;
            is_empty &= b.is_empty()
        });
        batch_count <= 1 && is_empty
    }

    pub fn become_tombstone_and_shrink(&mut self) -> ControlFlow<NoOpStateTransition<()>, ()> {
        assert_eq!(self.trace.upper(), &Antichain::new());
        assert_eq!(self.trace.since(), &Antichain::new());

        // Remember our current state, so we can decide whether we have to
        // record a transition in durable state.
        let was_tombstone = self.is_tombstone();

        // Enter the "tombstone" state, if we're not in it already.
        self.writers.clear();
        self.leased_readers.clear();
        self.critical_readers.clear();

        debug_assert!(self.is_tombstone());

        // Now that we're in a "tombstone" state -- ie. nobody can read the data from a shard or write to
        // it -- the actual contents of our batches no longer matter.
        // This method progressively replaces batches in our state with simpler versions, to allow
        // freeing up resources and to reduce the state size. (Since the state is unreadable, this
        // is not visible to clients.) We do this a little bit at a time to avoid really large state
        // transitions... most operations happen incrementally, and large single writes can overwhelm
        // a backing store. See comments for why we believe the relevant diffs are reasonably small.

        let mut to_replace = None;
        let mut batch_count = 0;
        self.trace.map_batches(|b| {
            batch_count += 1;
            if !b.is_empty() && to_replace.is_none() {
                to_replace = Some(b.desc.clone());
            }
        });
        if let Some(desc) = to_replace {
            // We have a nonempty batch: replace it with an empty batch and return.
            // This should not produce an excessively large diff: if it did, we wouldn't have been
            // able to append that batch in the first place.
            let result = self.trace.apply_tombstone_merge(&desc);
            assert!(
                result.matched(),
                "merge with a matching desc should always match"
            );
            Continue(())
        } else if batch_count > 1 {
            // All our batches are empty, but we have more than one of them. Replace the whole set
            // with a new single-batch trace.
            // This produces a diff with a size proportional to the number of batches, but since
            // Spine keeps a logarithmic number of batches this should never be excessively large.
            let mut new_trace = Trace::default();
            new_trace.downgrade_since(&Antichain::new());
            let merge_reqs = new_trace.push_batch(Self::tombstone_batch());
            assert_eq!(merge_reqs, Vec::new());
            self.trace = new_trace;
            Continue(())
        } else if !was_tombstone {
            // We were not tombstoned before, so have to make sure this state
            // transition is recorded.
            Continue(())
        } else {
            // All our batches are empty, and there's only one... there's no shrinking this
            // tombstone further.
            Break(NoOpStateTransition(()))
        }
    }
}

// TODO: Document invariants.
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(Clone, PartialEq))]
pub struct State<T> {
    pub(crate) shard_id: ShardId,

    pub(crate) seqno: SeqNo,
    /// A strictly increasing wall time of when this state was written, in
    /// milliseconds since the unix epoch.
    pub(crate) walltime_ms: u64,
    /// Hostname of the persist user that created this version of state. For
    /// debugging.
    pub(crate) hostname: String,
    pub(crate) collections: StateCollections<T>,
}

/// A newtype wrapper of State that guarantees the K, V, and D codecs match the
/// ones in durable storage.
pub struct TypedState<K, V, T, D> {
    pub(crate) state: State<T>,

    // According to the docs, PhantomData is to "mark things that act like they
    // own a T". State doesn't actually own K, V, or D, just the ability to
    // produce them. Using the `fn() -> T` pattern gets us the same variance as
    // T [1], but also allows State to correctly derive Send+Sync.
    //
    // [1]:
    //     https://doc.rust-lang.org/nomicon/phantom-data.html#table-of-phantomdata-patterns
    pub(crate) _phantom: PhantomData<fn() -> (K, V, D)>,
}

impl<K, V, T: Clone, D> TypedState<K, V, T, D> {
    #[cfg(any(test, debug_assertions))]
    pub(crate) fn clone(&self, hostname: String) -> Self {
        TypedState {
            state: State {
                shard_id: self.shard_id.clone(),
                seqno: self.seqno.clone(),
                walltime_ms: self.walltime_ms,
                hostname,
                collections: self.collections.clone(),
            },
            _phantom: PhantomData,
        }
    }

    pub(crate) fn clone_for_rollup(&self) -> Self {
        TypedState {
            state: State {
                shard_id: self.shard_id.clone(),
                seqno: self.seqno.clone(),
                walltime_ms: self.walltime_ms,
                hostname: self.hostname.clone(),
                collections: self.collections.clone(),
            },
            _phantom: PhantomData,
        }
    }
}

impl<K, V, T: Debug, D> Debug for TypedState<K, V, T, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Deconstruct self so we get a compile failure if new fields
        // are added.
        let TypedState { state, _phantom } = self;
        f.debug_struct("TypedState").field("state", state).finish()
    }
}

// Impl PartialEq regardless of the type params.
#[cfg(any(test, debug_assertions))]
impl<K, V, T: PartialEq, D> PartialEq for TypedState<K, V, T, D> {
    fn eq(&self, other: &Self) -> bool {
        // Deconstruct self and other so we get a compile failure if new fields
        // are added.
        let TypedState {
            state: self_state,
            _phantom,
        } = self;
        let TypedState {
            state: other_state,
            _phantom,
        } = other;
        self_state == other_state
    }
}

impl<K, V, T, D> Deref for TypedState<K, V, T, D> {
    type Target = State<T>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<K, V, T, D> DerefMut for TypedState<K, V, T, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<K, V, T, D> TypedState<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
    pub fn new(
        applier_version: Version,
        shard_id: ShardId,
        hostname: String,
        walltime_ms: u64,
    ) -> Self {
        let state = State {
            shard_id,
            seqno: SeqNo::minimum(),
            walltime_ms,
            hostname,
            collections: StateCollections {
                version: applier_version,
                last_gc_req: SeqNo::minimum(),
                rollups: BTreeMap::new(),
                active_rollup: None,
                active_gc: None,
                leased_readers: BTreeMap::new(),
                critical_readers: BTreeMap::new(),
                writers: BTreeMap::new(),
                schemas: BTreeMap::new(),
                trace: Trace::default(),
            },
        };
        TypedState {
            state,
            _phantom: PhantomData,
        }
    }

    pub fn clone_apply<R, E, WorkFn>(
        &self,
        cfg: &PersistConfig,
        work_fn: &mut WorkFn,
    ) -> ControlFlow<E, (R, Self)>
    where
        WorkFn: FnMut(SeqNo, &PersistConfig, &mut StateCollections<T>) -> ControlFlow<E, R>,
    {
        // We do not increment the version by default, though work_fn can if it chooses to.
        let mut new_state = State {
            shard_id: self.shard_id,
            seqno: self.seqno.next(),
            walltime_ms: (cfg.now)(),
            hostname: cfg.hostname.clone(),
            collections: self.collections.clone(),
        };

        // Make sure walltime_ms is strictly increasing, in case clocks are
        // offset.
        if new_state.walltime_ms <= self.walltime_ms {
            new_state.walltime_ms = self.walltime_ms + 1;
        }

        let work_ret = work_fn(new_state.seqno, cfg, &mut new_state.collections)?;
        let new_state = TypedState {
            state: new_state,
            _phantom: PhantomData,
        };
        Continue((work_ret, new_state))
    }
}

#[derive(Copy, Clone, Debug)]
pub struct GcConfig {
    pub use_active_gc: bool,
    pub fallback_threshold_ms: u64,
    pub min_versions: usize,
    pub max_versions: usize,
}

impl<T> State<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn seqno(&self) -> SeqNo {
        self.seqno
    }

    pub fn since(&self) -> &Antichain<T> {
        self.collections.trace.since()
    }

    pub fn upper(&self) -> &Antichain<T> {
        self.collections.trace.upper()
    }

    pub fn spine_batch_count(&self) -> usize {
        self.collections.trace.num_spine_batches()
    }

    pub fn size_metrics(&self) -> StateSizeMetrics {
        let mut ret = StateSizeMetrics::default();
        self.blobs().for_each(|x| match x {
            HollowBlobRef::Batch(x) => {
                ret.hollow_batch_count += 1;
                ret.batch_part_count += x.part_count();
                ret.num_updates += x.len;

                let batch_size = x.encoded_size_bytes();
                for x in x.parts.iter() {
                    if x.ts_rewrite().is_some() {
                        ret.rewrite_part_count += 1;
                    }
                    if x.is_inline() {
                        ret.inline_part_count += 1;
                        ret.inline_part_bytes += x.inline_bytes();
                    }
                }
                ret.largest_batch_bytes = std::cmp::max(ret.largest_batch_bytes, batch_size);
                ret.state_batches_bytes += batch_size;
            }
            HollowBlobRef::Rollup(x) => {
                ret.state_rollup_count += 1;
                ret.state_rollups_bytes += x.encoded_size_bytes.unwrap_or_default()
            }
        });
        ret
    }

    pub fn latest_rollup(&self) -> (&SeqNo, &HollowRollup) {
        // We maintain the invariant that every version of state has at least
        // one rollup.
        self.collections
            .rollups
            .iter()
            .rev()
            .next()
            .expect("State should have at least one rollup if seqno > minimum")
    }

    pub(crate) fn seqno_since(&self) -> SeqNo {
        self.collections.seqno_since(self.seqno)
    }

    // Returns whether the cmd proposing this state has been selected to perform
    // background garbage collection work.
    //
    // If it was selected, this information is recorded in the state itself for
    // commit along with the cmd's state transition. This helps us to avoid
    // redundant work.
    //
    // Correctness does not depend on a gc assignment being executed, nor on
    // them being executed in the order they are given. But it is expected that
    // gc assignments are best-effort respected. In practice, cmds like
    // register_foo or expire_foo, where it would be awkward, ignore gc.
    pub fn maybe_gc(&mut self, is_write: bool, now: u64, cfg: GcConfig) -> Option<GcReq> {
        let GcConfig {
            use_active_gc,
            fallback_threshold_ms,
            min_versions,
            max_versions,
        } = cfg;
        // This is an arbitrary-ish threshold that scales with seqno, but never
        // gets particularly big. It probably could be much bigger and certainly
        // could use a tuning pass at some point.
        let gc_threshold = if use_active_gc {
            u64::cast_from(min_versions)
        } else {
            std::cmp::max(
                1,
                u64::cast_from(self.seqno.0.next_power_of_two().trailing_zeros()),
            )
        };
        let new_seqno_since = self.seqno_since();
        // Collect until the new seqno since... or the old since plus the max number of versions,
        // whatever is less.
        let gc_until_seqno = new_seqno_since.min(SeqNo(
            self.collections
                .last_gc_req
                .0
                .saturating_add(u64::cast_from(max_versions)),
        ));
        let should_gc = new_seqno_since
            .0
            .saturating_sub(self.collections.last_gc_req.0)
            >= gc_threshold;

        // If we wouldn't otherwise gc, check if we have an active gc. If we do, and
        // it's been a while since it started, we should gc.
        let should_gc = if use_active_gc && !should_gc {
            match self.collections.active_gc {
                Some(active_gc) => now.saturating_sub(active_gc.start_ms) > fallback_threshold_ms,
                None => false,
            }
        } else {
            should_gc
        };
        // Assign GC traffic preferentially to writers, falling back to anyone
        // generating new state versions if there are no writers.
        let should_gc = should_gc && (is_write || self.collections.writers.is_empty());
        // Always assign GC work to a tombstoned shard to have the chance to
        // clean up any residual blobs. This is safe (won't cause excess gc)
        // as the only allowed command after becoming a tombstone is to write
        // the final rollup.
        let tombstone_needs_gc = self.collections.is_tombstone();
        let should_gc = should_gc || tombstone_needs_gc;
        let should_gc = if use_active_gc {
            // If we have an active gc, we should only gc if the active gc is
            // sufficiently old. This is to avoid doing more gc work than
            // necessary.
            should_gc
                && match self.collections.active_gc {
                    Some(active) => now.saturating_sub(active.start_ms) > fallback_threshold_ms,
                    None => true,
                }
        } else {
            should_gc
        };
        if should_gc {
            self.collections.last_gc_req = gc_until_seqno;
            Some(GcReq {
                shard_id: self.shard_id,
                new_seqno_since: gc_until_seqno,
            })
        } else {
            None
        }
    }

    /// Return the number of gc-ineligible state versions.
    pub fn seqnos_held(&self) -> usize {
        usize::cast_from(self.seqno.0.saturating_sub(self.seqno_since().0))
    }

    /// Expire all readers and writers up to the given walltime_ms.
    pub fn expire_at(&mut self, walltime_ms: EpochMillis) -> ExpiryMetrics {
        let mut metrics = ExpiryMetrics::default();
        let shard_id = self.shard_id();
        self.collections.leased_readers.retain(|id, state| {
            let retain = state.last_heartbeat_timestamp_ms + state.lease_duration_ms >= walltime_ms;
            if !retain {
                info!(
                    "Force expiring reader {id} ({}) of shard {shard_id} due to inactivity",
                    state.debug.purpose
                );
                metrics.readers_expired += 1;
            }
            retain
        });
        // critical_readers don't need forced expiration. (In fact, that's the point!)
        self.collections.writers.retain(|id, state| {
            let retain =
                (state.last_heartbeat_timestamp_ms + state.lease_duration_ms) >= walltime_ms;
            if !retain {
                info!(
                    "Force expiring writer {id} ({}) of shard {shard_id} due to inactivity",
                    state.debug.purpose
                );
                metrics.writers_expired += 1;
            }
            retain
        });
        metrics
    }

    /// Returns the batches that contain updates up to (and including) the given `as_of`. The
    /// result `Vec` contains blob keys, along with a [`Description`] of what updates in the
    /// referenced parts are valid to read.
    pub fn snapshot(&self, as_of: &Antichain<T>) -> Result<Vec<HollowBatch<T>>, SnapshotErr<T>> {
        if PartialOrder::less_than(as_of, self.collections.trace.since()) {
            return Err(SnapshotErr::AsOfHistoricalDistinctionsLost(Since(
                self.collections.trace.since().clone(),
            )));
        }
        let upper = self.collections.trace.upper();
        if PartialOrder::less_equal(upper, as_of) {
            return Err(SnapshotErr::AsOfNotYetAvailable(
                self.seqno,
                Upper(upper.clone()),
            ));
        }

        let batches = self
            .collections
            .trace
            .batches()
            .filter(|b| !PartialOrder::less_than(as_of, b.desc.lower()))
            .cloned()
            .collect();
        Ok(batches)
    }

    // NB: Unlike the other methods here, this one is read-only.
    pub fn verify_listen(&self, as_of: &Antichain<T>) -> Result<(), Since<T>> {
        if PartialOrder::less_than(as_of, self.collections.trace.since()) {
            return Err(Since(self.collections.trace.since().clone()));
        }
        Ok(())
    }

    pub fn next_listen_batch(&self, frontier: &Antichain<T>) -> Result<HollowBatch<T>, SeqNo> {
        // TODO: Avoid the O(n^2) here: `next_listen_batch` is called once per
        // batch and this iterates through all batches to find the next one.
        self.collections
            .trace
            .batches()
            .find(|b| {
                PartialOrder::less_equal(b.desc.lower(), frontier)
                    && PartialOrder::less_than(frontier, b.desc.upper())
            })
            .cloned()
            .ok_or(self.seqno)
    }

    pub fn active_rollup(&self) -> Option<ActiveRollup> {
        self.collections.active_rollup
    }

    pub fn need_rollup(
        &self,
        threshold: usize,
        use_active_rollup: bool,
        fallback_threshold_ms: u64,
        now: u64,
    ) -> Option<SeqNo> {
        let (latest_rollup_seqno, _) = self.latest_rollup();

        // Tombstoned shards require one final rollup. However, because we
        // write a rollup as of SeqNo X and then link it in using a state
        // transition (in this case from X to X+1), the minimum number of
        // live diffs is actually two. Detect when we're in this minimal
        // two diff state and stop the (otherwise) infinite iteration.
        if self.collections.is_tombstone() && latest_rollup_seqno.next() < self.seqno {
            return Some(self.seqno);
        }

        let seqnos_since_last_rollup = self.seqno.0.saturating_sub(latest_rollup_seqno.0);

        if use_active_rollup {
            // If sequnos_since_last_rollup>threshold, and there is no existing rollup in progress,
            // we should start a new rollup.
            // If there is an active rollup, we should check if it has been running too long.
            // If it has, we should start a new rollup.
            // This is to guard against a worker dying/taking too long/etc.
            if seqnos_since_last_rollup > u64::cast_from(threshold) {
                match self.active_rollup() {
                    Some(active_rollup) => {
                        if now.saturating_sub(active_rollup.start_ms) > fallback_threshold_ms {
                            return Some(self.seqno);
                        }
                    }
                    None => {
                        return Some(self.seqno);
                    }
                }
            }
        } else {
            // every `threshold` seqnos since the latest rollup, assign rollup maintenance.
            // we avoid assigning rollups to every seqno past the threshold to avoid handles
            // racing / performing redundant work.
            if seqnos_since_last_rollup > 0
                && seqnos_since_last_rollup % u64::cast_from(threshold) == 0
            {
                return Some(self.seqno);
            }

            // however, since maintenance is best-effort and could fail, do assign rollup
            // work to every seqno after a fallback threshold to ensure one is written.
            if seqnos_since_last_rollup
                > u64::cast_from(
                    threshold * PersistConfig::DEFAULT_FALLBACK_ROLLUP_THRESHOLD_MULTIPLIER,
                )
            {
                return Some(self.seqno);
            }
        }

        None
    }

    pub(crate) fn blobs(&self) -> impl Iterator<Item = HollowBlobRef<'_, T>> {
        let batches = self.collections.trace.batches().map(HollowBlobRef::Batch);
        let rollups = self.collections.rollups.values().map(HollowBlobRef::Rollup);
        batches.chain(rollups)
    }
}

fn serialize_part_bytes<S: Serializer>(val: &[u8], s: S) -> Result<S::Ok, S::Error> {
    let val = hex::encode(val);
    val.serialize(s)
}

fn serialize_lazy_proto<S: Serializer, T: prost::Message + Default>(
    val: &Option<LazyProto<T>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    val.as_ref()
        .map(|lazy| hex::encode(&lazy.into_proto()))
        .serialize(s)
}

fn serialize_part_stats<S: Serializer>(
    val: &Option<LazyPartStats>,
    s: S,
) -> Result<S::Ok, S::Error> {
    let val = val.as_ref().map(|x| x.decode().key);
    val.serialize(s)
}

fn serialize_diffs_sum<S: Serializer>(val: &Option<[u8; 8]>, s: S) -> Result<S::Ok, S::Error> {
    // This is only used for debugging, so hack to assume that D is i64.
    let val = val.map(i64::decode);
    val.serialize(s)
}

// This Serialize impl is used for debugging/testing and exposed via SQL. It's
// intentionally gated from users, so not strictly subject to our backward
// compatibility guarantees, but still probably best to be thoughtful about
// making unnecessary changes. Additionally, it's nice to make the output as
// nice to use as possible without tying our hands for the actual code usages.
impl<T: Serialize + Timestamp + Lattice> Serialize for State<T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let State {
            shard_id,
            seqno,
            walltime_ms,
            hostname,
            collections:
                StateCollections {
                    version: applier_version,
                    last_gc_req,
                    rollups,
                    active_rollup,
                    active_gc,
                    leased_readers,
                    critical_readers,
                    writers,
                    schemas,
                    trace,
                },
        } = self;
        let mut s = s.serialize_struct("State", 13)?;
        let () = s.serialize_field("applier_version", &applier_version.to_string())?;
        let () = s.serialize_field("shard_id", shard_id)?;
        let () = s.serialize_field("seqno", seqno)?;
        let () = s.serialize_field("walltime_ms", walltime_ms)?;
        let () = s.serialize_field("hostname", hostname)?;
        let () = s.serialize_field("last_gc_req", last_gc_req)?;
        let () = s.serialize_field("rollups", rollups)?;
        let () = s.serialize_field("active_rollup", active_rollup)?;
        let () = s.serialize_field("active_gc", active_gc)?;
        let () = s.serialize_field("leased_readers", leased_readers)?;
        let () = s.serialize_field("critical_readers", critical_readers)?;
        let () = s.serialize_field("writers", writers)?;
        let () = s.serialize_field("schemas", schemas)?;
        let () = s.serialize_field("since", &trace.since().elements())?;
        let () = s.serialize_field("upper", &trace.upper().elements())?;
        let trace = trace.flatten();
        let () = s.serialize_field("batches", &trace.legacy_batches.keys().collect::<Vec<_>>())?;
        let () = s.serialize_field("hollow_batches", &trace.hollow_batches)?;
        let () = s.serialize_field("spine_batches", &trace.spine_batches)?;
        let () = s.serialize_field("merges", &trace.merges)?;
        s.end()
    }
}

#[derive(Debug, Default)]
pub struct StateSizeMetrics {
    pub hollow_batch_count: usize,
    pub batch_part_count: usize,
    pub rewrite_part_count: usize,
    pub num_updates: usize,
    pub largest_batch_bytes: usize,
    pub state_batches_bytes: usize,
    pub state_rollups_bytes: usize,
    pub state_rollup_count: usize,
    pub inline_part_count: usize,
    pub inline_part_bytes: usize,
}

#[derive(Default)]
pub struct ExpiryMetrics {
    pub(crate) readers_expired: usize,
    pub(crate) writers_expired: usize,
}

/// Wrapper for Antichain that represents a Since
#[derive(Debug, Clone, PartialEq)]
pub struct Since<T>(pub Antichain<T>);

/// Wrapper for Antichain that represents an Upper
#[derive(Debug, PartialEq)]
pub struct Upper<T>(pub Antichain<T>);

#[cfg(test)]
pub(crate) mod tests {
    use std::ops::Range;
    use std::str::FromStr;

    use bytes::Bytes;
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::now::SYSTEM_TIME;
    use mz_ore::{assert_none, assert_ok};
    use mz_proto::RustType;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;

    use crate::InvalidUsage::{InvalidBounds, InvalidEmptyTimeInterval};
    use crate::cache::PersistClientCache;
    use crate::internal::encoding::any_some_lazy_part_stats;
    use crate::internal::paths::RollupId;
    use crate::internal::trace::tests::any_trace;
    use crate::tests::new_test_client_cache;
    use crate::{Diagnostics, PersistLocation};

    use super::*;

    const LEASE_DURATION_MS: u64 = 900 * 1000;
    fn debug_state() -> HandleDebugState {
        HandleDebugState {
            hostname: "debug".to_owned(),
            purpose: "finding the bugs".to_owned(),
        }
    }

    pub fn any_hollow_batch_with_exact_runs<T: Arbitrary + Timestamp>(
        num_runs: usize,
    ) -> impl Strategy<Value = HollowBatch<T>> {
        (
            any::<T>(),
            any::<T>(),
            any::<T>(),
            proptest::collection::vec(any_run_part::<T>(), num_runs + 1..20),
            any::<usize>(),
        )
            .prop_map(move |(t0, t1, since, parts, len)| {
                let (lower, upper) = if t0 <= t1 {
                    (Antichain::from_elem(t0), Antichain::from_elem(t1))
                } else {
                    (Antichain::from_elem(t1), Antichain::from_elem(t0))
                };
                let since = Antichain::from_elem(since);

                let run_splits = (1..num_runs)
                    .map(|i| i * parts.len() / num_runs)
                    .collect::<Vec<_>>();

                let run_meta = (0..num_runs)
                    .map(|_| {
                        let mut meta = RunMeta::default();
                        meta.id = Some(RunId::new());
                        meta
                    })
                    .collect::<Vec<_>>();

                HollowBatch::new(
                    Description::new(lower, upper, since),
                    parts,
                    len % 10,
                    run_meta,
                    run_splits,
                )
            })
    }

    pub fn any_hollow_batch<T: Arbitrary + Timestamp>() -> impl Strategy<Value = HollowBatch<T>> {
        Strategy::prop_map(
            (
                any::<T>(),
                any::<T>(),
                any::<T>(),
                proptest::collection::vec(any_run_part::<T>(), 0..20),
                any::<usize>(),
                0..=10usize,
                proptest::collection::vec(any::<RunId>(), 10),
            ),
            |(t0, t1, since, parts, len, num_runs, run_ids)| {
                let (lower, upper) = if t0 <= t1 {
                    (Antichain::from_elem(t0), Antichain::from_elem(t1))
                } else {
                    (Antichain::from_elem(t1), Antichain::from_elem(t0))
                };
                let since = Antichain::from_elem(since);
                if num_runs > 0 && parts.len() > 2 && num_runs < parts.len() {
                    let run_splits = (1..num_runs)
                        .map(|i| i * parts.len() / num_runs)
                        .collect::<Vec<_>>();

                    let run_meta = (0..num_runs)
                        .enumerate()
                        .map(|(i, _)| {
                            let mut meta = RunMeta::default();
                            meta.id = Some(run_ids[i]);
                            meta
                        })
                        .collect::<Vec<_>>();

                    HollowBatch::new(
                        Description::new(lower, upper, since),
                        parts,
                        len % 10,
                        run_meta,
                        run_splits,
                    )
                } else {
                    HollowBatch::new_run_for_test(
                        Description::new(lower, upper, since),
                        parts,
                        len % 10,
                        run_ids[0],
                    )
                }
            },
        )
    }

    pub fn any_batch_part<T: Arbitrary + Timestamp>() -> impl Strategy<Value = BatchPart<T>> {
        Strategy::prop_map(
            (
                any::<bool>(),
                any_hollow_batch_part(),
                any::<Option<T>>(),
                any::<Option<SchemaId>>(),
                any::<Option<SchemaId>>(),
            ),
            |(is_hollow, hollow, ts_rewrite, schema_id, deprecated_schema_id)| {
                if is_hollow {
                    BatchPart::Hollow(hollow)
                } else {
                    let updates = LazyInlineBatchPart::from_proto(Bytes::new()).unwrap();
                    let ts_rewrite = ts_rewrite.map(Antichain::from_elem);
                    BatchPart::Inline {
                        updates,
                        ts_rewrite,
                        schema_id,
                        deprecated_schema_id,
                    }
                }
            },
        )
    }

    pub fn any_run_part<T: Arbitrary + Timestamp>() -> impl Strategy<Value = RunPart<T>> {
        Strategy::prop_map(any_batch_part(), |part| RunPart::Single(part))
    }

    pub fn any_hollow_batch_part<T: Arbitrary + Timestamp>()
    -> impl Strategy<Value = HollowBatchPart<T>> {
        Strategy::prop_map(
            (
                any::<PartialBatchKey>(),
                any::<usize>(),
                any::<Vec<u8>>(),
                any_some_lazy_part_stats(),
                any::<Option<T>>(),
                any::<[u8; 8]>(),
                any::<Option<BatchColumnarFormat>>(),
                any::<Option<SchemaId>>(),
                any::<Option<SchemaId>>(),
            ),
            |(
                key,
                encoded_size_bytes,
                key_lower,
                stats,
                ts_rewrite,
                diffs_sum,
                format,
                schema_id,
                deprecated_schema_id,
            )| {
                HollowBatchPart {
                    key,
                    meta: Default::default(),
                    encoded_size_bytes,
                    key_lower,
                    structured_key_lower: None,
                    stats,
                    ts_rewrite: ts_rewrite.map(Antichain::from_elem),
                    diffs_sum: Some(diffs_sum),
                    format,
                    schema_id,
                    deprecated_schema_id,
                }
            },
        )
    }

    pub fn any_leased_reader_state<T: Arbitrary>() -> impl Strategy<Value = LeasedReaderState<T>> {
        Strategy::prop_map(
            (
                any::<SeqNo>(),
                any::<Option<T>>(),
                any::<u64>(),
                any::<u64>(),
                any::<HandleDebugState>(),
            ),
            |(seqno, since, last_heartbeat_timestamp_ms, mut lease_duration_ms, debug)| {
                // lease_duration_ms of 0 means this state was written by an old
                // version of code, which means we'll migrate it in the decode
                // path. Avoid.
                if lease_duration_ms == 0 {
                    lease_duration_ms += 1;
                }
                LeasedReaderState {
                    seqno,
                    since: since.map_or_else(Antichain::new, Antichain::from_elem),
                    last_heartbeat_timestamp_ms,
                    lease_duration_ms,
                    debug,
                }
            },
        )
    }

    pub fn any_critical_reader_state<T: Arbitrary>() -> impl Strategy<Value = CriticalReaderState<T>>
    {
        Strategy::prop_map(
            (
                any::<Option<T>>(),
                any::<OpaqueState>(),
                any::<String>(),
                any::<HandleDebugState>(),
            ),
            |(since, opaque, opaque_codec, debug)| CriticalReaderState {
                since: since.map_or_else(Antichain::new, Antichain::from_elem),
                opaque,
                opaque_codec,
                debug,
            },
        )
    }

    pub fn any_writer_state<T: Arbitrary>() -> impl Strategy<Value = WriterState<T>> {
        Strategy::prop_map(
            (
                any::<u64>(),
                any::<u64>(),
                any::<IdempotencyToken>(),
                any::<Option<T>>(),
                any::<HandleDebugState>(),
            ),
            |(
                last_heartbeat_timestamp_ms,
                lease_duration_ms,
                most_recent_write_token,
                most_recent_write_upper,
                debug,
            )| WriterState {
                last_heartbeat_timestamp_ms,
                lease_duration_ms,
                most_recent_write_token,
                most_recent_write_upper: most_recent_write_upper
                    .map_or_else(Antichain::new, Antichain::from_elem),
                debug,
            },
        )
    }

    pub fn any_encoded_schemas() -> impl Strategy<Value = EncodedSchemas> {
        Strategy::prop_map(
            (
                any::<Vec<u8>>(),
                any::<Vec<u8>>(),
                any::<Vec<u8>>(),
                any::<Vec<u8>>(),
            ),
            |(key, key_data_type, val, val_data_type)| EncodedSchemas {
                key: Bytes::from(key),
                key_data_type: Bytes::from(key_data_type),
                val: Bytes::from(val),
                val_data_type: Bytes::from(val_data_type),
            },
        )
    }

    pub fn any_state<T: Arbitrary + Timestamp + Lattice>(
        num_trace_batches: Range<usize>,
    ) -> impl Strategy<Value = State<T>> {
        let part1 = (
            any::<ShardId>(),
            any::<SeqNo>(),
            any::<u64>(),
            any::<String>(),
            any::<SeqNo>(),
            proptest::collection::btree_map(any::<SeqNo>(), any::<HollowRollup>(), 1..3),
            proptest::option::of(any::<ActiveRollup>()),
        );

        let part2 = (
            proptest::option::of(any::<ActiveGc>()),
            proptest::collection::btree_map(
                any::<LeasedReaderId>(),
                any_leased_reader_state::<T>(),
                1..3,
            ),
            proptest::collection::btree_map(
                any::<CriticalReaderId>(),
                any_critical_reader_state::<T>(),
                1..3,
            ),
            proptest::collection::btree_map(any::<WriterId>(), any_writer_state::<T>(), 0..3),
            proptest::collection::btree_map(any::<SchemaId>(), any_encoded_schemas(), 0..3),
            any_trace::<T>(num_trace_batches),
        );

        (part1, part2).prop_map(
            |(
                (shard_id, seqno, walltime_ms, hostname, last_gc_req, rollups, active_rollup),
                (active_gc, leased_readers, critical_readers, writers, schemas, trace),
            )| State {
                shard_id,
                seqno,
                walltime_ms,
                hostname,
                collections: StateCollections {
                    version: Version::new(1, 2, 3),
                    last_gc_req,
                    rollups,
                    active_rollup,
                    active_gc,
                    leased_readers,
                    critical_readers,
                    writers,
                    schemas,
                    trace,
                },
            },
        )
    }

    pub(crate) fn hollow<T: Timestamp>(
        lower: T,
        upper: T,
        keys: &[&str],
        len: usize,
    ) -> HollowBatch<T> {
        HollowBatch::new_run(
            Description::new(
                Antichain::from_elem(lower),
                Antichain::from_elem(upper),
                Antichain::from_elem(T::minimum()),
            ),
            keys.iter()
                .map(|x| {
                    RunPart::Single(BatchPart::Hollow(HollowBatchPart {
                        key: PartialBatchKey((*x).to_owned()),
                        meta: Default::default(),
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
                })
                .collect(),
            len,
        )
    }

    #[mz_ore::test]
    fn downgrade_since() {
        let mut state = TypedState::<(), (), u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );
        let reader = LeasedReaderId::new();
        let seqno = SeqNo::minimum();
        let now = SYSTEM_TIME.clone();
        let _ = state.collections.register_leased_reader(
            "",
            &reader,
            "",
            seqno,
            Duration::from_secs(10),
            now(),
            false,
        );

        // The shard global since == 0 initially.
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(0));

        // Greater
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                seqno,
                None,
                &Antichain::from_elem(2),
                now()
            ),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Equal (no-op)
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                seqno,
                None,
                &Antichain::from_elem(2),
                now()
            ),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Less (no-op)
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                seqno,
                None,
                &Antichain::from_elem(1),
                now()
            ),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));

        // Create a second reader.
        let reader2 = LeasedReaderId::new();
        let _ = state.collections.register_leased_reader(
            "",
            &reader2,
            "",
            seqno,
            Duration::from_secs(10),
            now(),
            false,
        );

        // Shard since doesn't change until the meet (min) of all reader sinces changes.
        assert_eq!(
            state.collections.downgrade_since(
                &reader2,
                seqno,
                None,
                &Antichain::from_elem(3),
                now()
            ),
            Continue(Since(Antichain::from_elem(3)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Shard since == 3 when all readers have since >= 3.
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                seqno,
                None,
                &Antichain::from_elem(5),
                now()
            ),
            Continue(Since(Antichain::from_elem(5)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Shard since unaffected readers with since > shard since expiring.
        assert_eq!(
            state.collections.expire_leased_reader(&reader),
            Continue(true)
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Create a third reader.
        let reader3 = LeasedReaderId::new();
        let _ = state.collections.register_leased_reader(
            "",
            &reader3,
            "",
            seqno,
            Duration::from_secs(10),
            now(),
            false,
        );

        // Shard since doesn't change until the meet (min) of all reader sinces changes.
        assert_eq!(
            state.collections.downgrade_since(
                &reader3,
                seqno,
                None,
                &Antichain::from_elem(10),
                now()
            ),
            Continue(Since(Antichain::from_elem(10)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Shard since advances when reader with the minimal since expires.
        assert_eq!(
            state.collections.expire_leased_reader(&reader2),
            Continue(true)
        );
        // TODO(database-issues#6885): expiry temporarily doesn't advance since
        // Switch this assertion back when we re-enable this.
        //
        // assert_eq!(state.collections.trace.since(), &Antichain::from_elem(10));
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Shard since unaffected when all readers are expired.
        assert_eq!(
            state.collections.expire_leased_reader(&reader3),
            Continue(true)
        );
        // TODO(database-issues#6885): expiry temporarily doesn't advance since
        // Switch this assertion back when we re-enable this.
        //
        // assert_eq!(state.collections.trace.since(), &Antichain::from_elem(10));
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));
    }

    #[mz_ore::test]
    fn compare_and_downgrade_since() {
        let mut state = TypedState::<(), (), u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );
        let reader = CriticalReaderId::new();
        let _ = state
            .collections
            .register_critical_reader::<u64>("", &reader, "");

        // The shard global since == 0 initially.
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(0));
        // The initial opaque value should be set.
        assert_eq!(
            u64::decode(state.collections.critical_reader(&reader).opaque.0),
            u64::initial()
        );

        // Greater
        assert_eq!(
            state.collections.compare_and_downgrade_since::<u64>(
                &reader,
                &u64::initial(),
                (&1, &Antichain::from_elem(2)),
            ),
            Continue(Ok(Since(Antichain::from_elem(2))))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        assert_eq!(
            u64::decode(state.collections.critical_reader(&reader).opaque.0),
            1
        );
        // Equal (no-op)
        assert_eq!(
            state.collections.compare_and_downgrade_since::<u64>(
                &reader,
                &1,
                (&2, &Antichain::from_elem(2)),
            ),
            Continue(Ok(Since(Antichain::from_elem(2))))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        assert_eq!(
            u64::decode(state.collections.critical_reader(&reader).opaque.0),
            2
        );
        // Less (no-op)
        assert_eq!(
            state.collections.compare_and_downgrade_since::<u64>(
                &reader,
                &2,
                (&3, &Antichain::from_elem(1)),
            ),
            Continue(Ok(Since(Antichain::from_elem(2))))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        assert_eq!(
            u64::decode(state.collections.critical_reader(&reader).opaque.0),
            3
        );
    }

    #[mz_ore::test]
    fn compare_and_append() {
        let state = &mut TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        )
        .collections;

        let writer_id = WriterId::new();
        let now = SYSTEM_TIME.clone();

        // State is initially empty.
        assert_eq!(state.trace.num_spine_batches(), 0);
        assert_eq!(state.trace.num_hollow_batches(), 0);
        assert_eq!(state.trace.num_updates(), 0);

        // Cannot insert a batch with a lower != current shard upper.
        assert_eq!(
            state.compare_and_append(
                &hollow(1, 2, &["key1"], 1),
                &writer_id,
                now(),
                LEASE_DURATION_MS,
                &IdempotencyToken::new(),
                &debug_state(),
                0,
                100,
                None
            ),
            Break(CompareAndAppendBreak::Upper {
                shard_upper: Antichain::from_elem(0),
                writer_upper: Antichain::from_elem(0)
            })
        );

        // Insert an empty batch with an upper > lower..
        assert!(
            state
                .compare_and_append(
                    &hollow(0, 5, &[], 0),
                    &writer_id,
                    now(),
                    LEASE_DURATION_MS,
                    &IdempotencyToken::new(),
                    &debug_state(),
                    0,
                    100,
                    None
                )
                .is_continue()
        );

        // Cannot insert a batch with a upper less than the lower.
        assert_eq!(
            state.compare_and_append(
                &hollow(5, 4, &["key1"], 1),
                &writer_id,
                now(),
                LEASE_DURATION_MS,
                &IdempotencyToken::new(),
                &debug_state(),
                0,
                100,
                None
            ),
            Break(CompareAndAppendBreak::InvalidUsage(InvalidBounds {
                lower: Antichain::from_elem(5),
                upper: Antichain::from_elem(4)
            }))
        );

        // Cannot insert a nonempty batch with an upper equal to lower.
        assert_eq!(
            state.compare_and_append(
                &hollow(5, 5, &["key1"], 1),
                &writer_id,
                now(),
                LEASE_DURATION_MS,
                &IdempotencyToken::new(),
                &debug_state(),
                0,
                100,
                None
            ),
            Break(CompareAndAppendBreak::InvalidUsage(
                InvalidEmptyTimeInterval {
                    lower: Antichain::from_elem(5),
                    upper: Antichain::from_elem(5),
                    keys: vec!["key1".to_owned()],
                }
            ))
        );

        // Can insert an empty batch with an upper equal to lower.
        assert!(
            state
                .compare_and_append(
                    &hollow(5, 5, &[], 0),
                    &writer_id,
                    now(),
                    LEASE_DURATION_MS,
                    &IdempotencyToken::new(),
                    &debug_state(),
                    0,
                    100,
                    None
                )
                .is_continue()
        );
    }

    #[mz_ore::test]
    fn snapshot() {
        let now = SYSTEM_TIME.clone();

        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );
        // Cannot take a snapshot with as_of == shard upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(0)),
            Err(SnapshotErr::AsOfNotYetAvailable(
                SeqNo(0),
                Upper(Antichain::from_elem(0))
            ))
        );

        // Cannot take a snapshot with as_of > shard upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(5)),
            Err(SnapshotErr::AsOfNotYetAvailable(
                SeqNo(0),
                Upper(Antichain::from_elem(0))
            ))
        );

        let writer_id = WriterId::new();

        // Advance upper to 5.
        assert!(
            state
                .collections
                .compare_and_append(
                    &hollow(0, 5, &["key1"], 1),
                    &writer_id,
                    now(),
                    LEASE_DURATION_MS,
                    &IdempotencyToken::new(),
                    &debug_state(),
                    0,
                    100,
                    None
                )
                .is_continue()
        );

        // Can take a snapshot with as_of < upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(0)),
            Ok(vec![hollow(0, 5, &["key1"], 1)])
        );

        // Can take a snapshot with as_of >= shard since, as long as as_of < shard_upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(4)),
            Ok(vec![hollow(0, 5, &["key1"], 1)])
        );

        // Cannot take a snapshot with as_of >= upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(5)),
            Err(SnapshotErr::AsOfNotYetAvailable(
                SeqNo(0),
                Upper(Antichain::from_elem(5))
            ))
        );
        assert_eq!(
            state.snapshot(&Antichain::from_elem(6)),
            Err(SnapshotErr::AsOfNotYetAvailable(
                SeqNo(0),
                Upper(Antichain::from_elem(5))
            ))
        );

        let reader = LeasedReaderId::new();
        // Advance the since to 2.
        let _ = state.collections.register_leased_reader(
            "",
            &reader,
            "",
            SeqNo::minimum(),
            Duration::from_secs(10),
            now(),
            false,
        );
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                SeqNo::minimum(),
                None,
                &Antichain::from_elem(2),
                now()
            ),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Cannot take a snapshot with as_of < shard_since.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(1)),
            Err(SnapshotErr::AsOfHistoricalDistinctionsLost(Since(
                Antichain::from_elem(2)
            )))
        );

        // Advance the upper to 10 via an empty batch.
        assert!(
            state
                .collections
                .compare_and_append(
                    &hollow(5, 10, &[], 0),
                    &writer_id,
                    now(),
                    LEASE_DURATION_MS,
                    &IdempotencyToken::new(),
                    &debug_state(),
                    0,
                    100,
                    None
                )
                .is_continue()
        );

        // Can still take snapshots at times < upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(7)),
            Ok(vec![hollow(0, 5, &["key1"], 1), hollow(5, 10, &[], 0)])
        );

        // Cannot take snapshots with as_of >= upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(10)),
            Err(SnapshotErr::AsOfNotYetAvailable(
                SeqNo(0),
                Upper(Antichain::from_elem(10))
            ))
        );

        // Advance upper to 15.
        assert!(
            state
                .collections
                .compare_and_append(
                    &hollow(10, 15, &["key2"], 1),
                    &writer_id,
                    now(),
                    LEASE_DURATION_MS,
                    &IdempotencyToken::new(),
                    &debug_state(),
                    0,
                    100,
                    None
                )
                .is_continue()
        );

        // Filter out batches whose lowers are less than the requested as of (the
        // batches that are too far in the future for the requested as_of).
        assert_eq!(
            state.snapshot(&Antichain::from_elem(9)),
            Ok(vec![hollow(0, 5, &["key1"], 1), hollow(5, 10, &[], 0)])
        );

        // Don't filter out batches whose lowers are <= the requested as_of.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(10)),
            Ok(vec![
                hollow(0, 5, &["key1"], 1),
                hollow(5, 10, &[], 0),
                hollow(10, 15, &["key2"], 1)
            ])
        );

        assert_eq!(
            state.snapshot(&Antichain::from_elem(11)),
            Ok(vec![
                hollow(0, 5, &["key1"], 1),
                hollow(5, 10, &[], 0),
                hollow(10, 15, &["key2"], 1)
            ])
        );
    }

    #[mz_ore::test]
    fn next_listen_batch() {
        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );

        // Empty collection never has any batches to listen for, regardless of the
        // current frontier.
        assert_eq!(
            state.next_listen_batch(&Antichain::from_elem(0)),
            Err(SeqNo(0))
        );
        assert_eq!(state.next_listen_batch(&Antichain::new()), Err(SeqNo(0)));

        let writer_id = WriterId::new();
        let now = SYSTEM_TIME.clone();

        // Add two batches of data, one from [0, 5) and then another from [5, 10).
        assert!(
            state
                .collections
                .compare_and_append(
                    &hollow(0, 5, &["key1"], 1),
                    &writer_id,
                    now(),
                    LEASE_DURATION_MS,
                    &IdempotencyToken::new(),
                    &debug_state(),
                    0,
                    100,
                    None
                )
                .is_continue()
        );
        assert!(
            state
                .collections
                .compare_and_append(
                    &hollow(5, 10, &["key2"], 1),
                    &writer_id,
                    now(),
                    LEASE_DURATION_MS,
                    &IdempotencyToken::new(),
                    &debug_state(),
                    0,
                    100,
                    None
                )
                .is_continue()
        );

        // All frontiers in [0, 5) return the first batch.
        for t in 0..=4 {
            assert_eq!(
                state.next_listen_batch(&Antichain::from_elem(t)),
                Ok(hollow(0, 5, &["key1"], 1))
            );
        }

        // All frontiers in [5, 10) return the second batch.
        for t in 5..=9 {
            assert_eq!(
                state.next_listen_batch(&Antichain::from_elem(t)),
                Ok(hollow(5, 10, &["key2"], 1))
            );
        }

        // There is no batch currently available for t = 10.
        assert_eq!(
            state.next_listen_batch(&Antichain::from_elem(10)),
            Err(SeqNo(0))
        );

        // By definition, there is no frontier ever at the empty antichain which
        // is the time after all possible times.
        assert_eq!(state.next_listen_batch(&Antichain::new()), Err(SeqNo(0)));
    }

    #[mz_ore::test]
    fn expire_writer() {
        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );
        let now = SYSTEM_TIME.clone();

        let writer_id_one = WriterId::new();

        let writer_id_two = WriterId::new();

        // Writer is eligible to write
        assert!(
            state
                .collections
                .compare_and_append(
                    &hollow(0, 2, &["key1"], 1),
                    &writer_id_one,
                    now(),
                    LEASE_DURATION_MS,
                    &IdempotencyToken::new(),
                    &debug_state(),
                    0,
                    100,
                    None
                )
                .is_continue()
        );

        assert!(
            state
                .collections
                .expire_writer(&writer_id_one)
                .is_continue()
        );

        // Other writers should still be able to write
        assert!(
            state
                .collections
                .compare_and_append(
                    &hollow(2, 5, &["key2"], 1),
                    &writer_id_two,
                    now(),
                    LEASE_DURATION_MS,
                    &IdempotencyToken::new(),
                    &debug_state(),
                    0,
                    100,
                    None
                )
                .is_continue()
        );
    }

    #[mz_ore::test]
    fn maybe_gc_active_gc() {
        const GC_CONFIG: GcConfig = GcConfig {
            use_active_gc: true,
            fallback_threshold_ms: 5000,
            min_versions: 99,
            max_versions: 500,
        };
        let now_fn = SYSTEM_TIME.clone();

        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );

        let now = now_fn();
        // Empty state doesn't need gc, regardless of is_write.
        assert_eq!(state.maybe_gc(true, now, GC_CONFIG), None);
        assert_eq!(state.maybe_gc(false, now, GC_CONFIG), None);

        // Artificially advance the seqno so the seqno_since advances past our
        // internal gc_threshold.
        state.seqno = SeqNo(100);
        assert_eq!(state.seqno_since(), SeqNo(100));

        // When a writer is present, non-writes don't gc.
        let writer_id = WriterId::new();
        let _ = state.collections.compare_and_append(
            &hollow(1, 2, &["key1"], 1),
            &writer_id,
            now,
            LEASE_DURATION_MS,
            &IdempotencyToken::new(),
            &debug_state(),
            0,
            100,
            None,
        );
        assert_eq!(state.maybe_gc(false, now, GC_CONFIG), None);

        // A write will gc though.
        assert_eq!(
            state.maybe_gc(true, now, GC_CONFIG),
            Some(GcReq {
                shard_id: state.shard_id,
                new_seqno_since: SeqNo(100)
            })
        );

        // But if we write down an active gc, we won't gc.
        state.collections.active_gc = Some(ActiveGc {
            seqno: state.seqno,
            start_ms: now,
        });

        state.seqno = SeqNo(200);
        assert_eq!(state.seqno_since(), SeqNo(200));

        assert_eq!(state.maybe_gc(true, now, GC_CONFIG), None);

        state.seqno = SeqNo(300);
        assert_eq!(state.seqno_since(), SeqNo(300));
        // But if we advance the time past the threshold, we will gc.
        let new_now = now + GC_CONFIG.fallback_threshold_ms + 1;
        assert_eq!(
            state.maybe_gc(true, new_now, GC_CONFIG),
            Some(GcReq {
                shard_id: state.shard_id,
                new_seqno_since: SeqNo(300)
            })
        );

        // Even if the sequence number doesn't pass the threshold, if the
        // active gc is expired, we will gc.

        state.seqno = SeqNo(301);
        assert_eq!(state.seqno_since(), SeqNo(301));
        assert_eq!(
            state.maybe_gc(true, new_now, GC_CONFIG),
            Some(GcReq {
                shard_id: state.shard_id,
                new_seqno_since: SeqNo(301)
            })
        );

        state.collections.active_gc = None;

        // Artificially advance the seqno (again) so the seqno_since advances
        // past our internal gc_threshold (again).
        state.seqno = SeqNo(400);
        assert_eq!(state.seqno_since(), SeqNo(400));

        let now = now_fn();

        // If there are no writers, even a non-write will gc.
        let _ = state.collections.expire_writer(&writer_id);
        assert_eq!(
            state.maybe_gc(false, now, GC_CONFIG),
            Some(GcReq {
                shard_id: state.shard_id,
                new_seqno_since: SeqNo(400)
            })
        );

        // Upper-bound the number of seqnos we'll attempt to collect in one go.
        let previous_seqno = state.seqno;
        state.seqno = SeqNo(10_000);
        assert_eq!(state.seqno_since(), SeqNo(10_000));

        let now = now_fn();
        assert_eq!(
            state.maybe_gc(true, now, GC_CONFIG),
            Some(GcReq {
                shard_id: state.shard_id,
                new_seqno_since: SeqNo(previous_seqno.0 + u64::cast_from(GC_CONFIG.max_versions))
            })
        );
    }

    #[mz_ore::test]
    fn maybe_gc_classic() {
        const GC_CONFIG: GcConfig = GcConfig {
            use_active_gc: false,
            fallback_threshold_ms: 5000,
            min_versions: 16,
            max_versions: 128,
        };
        const NOW_MS: u64 = 0;

        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );

        // Empty state doesn't need gc, regardless of is_write.
        assert_eq!(state.maybe_gc(true, NOW_MS, GC_CONFIG), None);
        assert_eq!(state.maybe_gc(false, NOW_MS, GC_CONFIG), None);

        // Artificially advance the seqno so the seqno_since advances past our
        // internal gc_threshold.
        state.seqno = SeqNo(100);
        assert_eq!(state.seqno_since(), SeqNo(100));

        // When a writer is present, non-writes don't gc.
        let writer_id = WriterId::new();
        let now = SYSTEM_TIME.clone();
        let _ = state.collections.compare_and_append(
            &hollow(1, 2, &["key1"], 1),
            &writer_id,
            now(),
            LEASE_DURATION_MS,
            &IdempotencyToken::new(),
            &debug_state(),
            0,
            100,
            None,
        );
        assert_eq!(state.maybe_gc(false, NOW_MS, GC_CONFIG), None);

        // A write will gc though.
        assert_eq!(
            state.maybe_gc(true, NOW_MS, GC_CONFIG),
            Some(GcReq {
                shard_id: state.shard_id,
                new_seqno_since: SeqNo(100)
            })
        );

        // Artificially advance the seqno (again) so the seqno_since advances
        // past our internal gc_threshold (again).
        state.seqno = SeqNo(200);
        assert_eq!(state.seqno_since(), SeqNo(200));

        // If there are no writers, even a non-write will gc.
        let _ = state.collections.expire_writer(&writer_id);
        assert_eq!(
            state.maybe_gc(false, NOW_MS, GC_CONFIG),
            Some(GcReq {
                shard_id: state.shard_id,
                new_seqno_since: SeqNo(200)
            })
        );
    }

    #[mz_ore::test]
    fn need_rollup_active_rollup() {
        const ROLLUP_THRESHOLD: usize = 3;
        const ROLLUP_USE_ACTIVE_ROLLUP: bool = true;
        const ROLLUP_FALLBACK_THRESHOLD_MS: u64 = 5000;
        let now = SYSTEM_TIME.clone();

        mz_ore::test::init_logging();
        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );

        let rollup_seqno = SeqNo(5);
        let rollup = HollowRollup {
            key: PartialRollupKey::new(rollup_seqno, &RollupId::new()),
            encoded_size_bytes: None,
        };

        assert!(
            state
                .collections
                .add_rollup((rollup_seqno, &rollup))
                .is_continue()
        );

        // shouldn't need a rollup at the seqno of the rollup
        state.seqno = SeqNo(5);
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            now()
        ));

        // shouldn't need a rollup at seqnos less than our threshold
        state.seqno = SeqNo(6);
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            now()
        ));
        state.seqno = SeqNo(7);
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            now()
        ));
        state.seqno = SeqNo(8);
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            now()
        ));

        let mut current_time = now();
        // hit our threshold! we should need a rollup
        state.seqno = SeqNo(9);
        assert_eq!(
            state
                .need_rollup(
                    ROLLUP_THRESHOLD,
                    ROLLUP_USE_ACTIVE_ROLLUP,
                    ROLLUP_FALLBACK_THRESHOLD_MS,
                    current_time
                )
                .expect("rollup"),
            SeqNo(9)
        );

        state.collections.active_rollup = Some(ActiveRollup {
            seqno: SeqNo(9),
            start_ms: current_time,
        });

        // There is now an active rollup, so we shouldn't need a rollup.
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            current_time
        ));

        state.seqno = SeqNo(10);
        // We still don't need a rollup, even though the seqno is greater than
        // the rollup threshold.
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            current_time
        ));

        // But if we wait long enough, we should need a rollup again.
        current_time += u64::cast_from(ROLLUP_FALLBACK_THRESHOLD_MS) + 1;
        assert_eq!(
            state
                .need_rollup(
                    ROLLUP_THRESHOLD,
                    ROLLUP_USE_ACTIVE_ROLLUP,
                    ROLLUP_FALLBACK_THRESHOLD_MS,
                    current_time
                )
                .expect("rollup"),
            SeqNo(10)
        );

        state.seqno = SeqNo(9);
        // Clear the active rollup and ensure we need a rollup again.
        state.collections.active_rollup = None;
        let rollup_seqno = SeqNo(9);
        let rollup = HollowRollup {
            key: PartialRollupKey::new(rollup_seqno, &RollupId::new()),
            encoded_size_bytes: None,
        };
        assert!(
            state
                .collections
                .add_rollup((rollup_seqno, &rollup))
                .is_continue()
        );

        state.seqno = SeqNo(11);
        // We shouldn't need a rollup at seqnos less than our threshold
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            current_time
        ));
        // hit our threshold! we should need a rollup
        state.seqno = SeqNo(13);
        assert_eq!(
            state
                .need_rollup(
                    ROLLUP_THRESHOLD,
                    ROLLUP_USE_ACTIVE_ROLLUP,
                    ROLLUP_FALLBACK_THRESHOLD_MS,
                    current_time
                )
                .expect("rollup"),
            SeqNo(13)
        );
    }

    #[mz_ore::test]
    fn need_rollup_classic() {
        const ROLLUP_THRESHOLD: usize = 3;
        const ROLLUP_USE_ACTIVE_ROLLUP: bool = false;
        const ROLLUP_FALLBACK_THRESHOLD_MS: u64 = 0;
        const NOW: u64 = 0;

        mz_ore::test::init_logging();
        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );

        let rollup_seqno = SeqNo(5);
        let rollup = HollowRollup {
            key: PartialRollupKey::new(rollup_seqno, &RollupId::new()),
            encoded_size_bytes: None,
        };

        assert!(
            state
                .collections
                .add_rollup((rollup_seqno, &rollup))
                .is_continue()
        );

        // shouldn't need a rollup at the seqno of the rollup
        state.seqno = SeqNo(5);
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            NOW
        ));

        // shouldn't need a rollup at seqnos less than our threshold
        state.seqno = SeqNo(6);
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            NOW
        ));
        state.seqno = SeqNo(7);
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            NOW
        ));

        // hit our threshold! we should need a rollup
        state.seqno = SeqNo(8);
        assert_eq!(
            state
                .need_rollup(
                    ROLLUP_THRESHOLD,
                    ROLLUP_USE_ACTIVE_ROLLUP,
                    ROLLUP_FALLBACK_THRESHOLD_MS,
                    NOW
                )
                .expect("rollup"),
            SeqNo(8)
        );

        // but we don't need rollups for every seqno > the threshold
        state.seqno = SeqNo(9);
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            NOW
        ));

        // we only need a rollup each `ROLLUP_THRESHOLD` beyond our current seqno
        state.seqno = SeqNo(11);
        assert_eq!(
            state
                .need_rollup(
                    ROLLUP_THRESHOLD,
                    ROLLUP_USE_ACTIVE_ROLLUP,
                    ROLLUP_FALLBACK_THRESHOLD_MS,
                    NOW
                )
                .expect("rollup"),
            SeqNo(11)
        );

        // add another rollup and ensure we're always picking the latest
        let rollup_seqno = SeqNo(6);
        let rollup = HollowRollup {
            key: PartialRollupKey::new(rollup_seqno, &RollupId::new()),
            encoded_size_bytes: None,
        };
        assert!(
            state
                .collections
                .add_rollup((rollup_seqno, &rollup))
                .is_continue()
        );

        state.seqno = SeqNo(8);
        assert_none!(state.need_rollup(
            ROLLUP_THRESHOLD,
            ROLLUP_USE_ACTIVE_ROLLUP,
            ROLLUP_FALLBACK_THRESHOLD_MS,
            NOW
        ));
        state.seqno = SeqNo(9);
        assert_eq!(
            state
                .need_rollup(
                    ROLLUP_THRESHOLD,
                    ROLLUP_USE_ACTIVE_ROLLUP,
                    ROLLUP_FALLBACK_THRESHOLD_MS,
                    NOW
                )
                .expect("rollup"),
            SeqNo(9)
        );

        // and ensure that after a fallback point, we assign every seqno work
        let fallback_seqno = SeqNo(
            rollup_seqno.0
                * u64::cast_from(PersistConfig::DEFAULT_FALLBACK_ROLLUP_THRESHOLD_MULTIPLIER),
        );
        state.seqno = fallback_seqno;
        assert_eq!(
            state
                .need_rollup(
                    ROLLUP_THRESHOLD,
                    ROLLUP_USE_ACTIVE_ROLLUP,
                    ROLLUP_FALLBACK_THRESHOLD_MS,
                    NOW
                )
                .expect("rollup"),
            fallback_seqno
        );
        state.seqno = fallback_seqno.next();
        assert_eq!(
            state
                .need_rollup(
                    ROLLUP_THRESHOLD,
                    ROLLUP_USE_ACTIVE_ROLLUP,
                    ROLLUP_FALLBACK_THRESHOLD_MS,
                    NOW
                )
                .expect("rollup"),
            fallback_seqno.next()
        );
    }

    #[mz_ore::test]
    fn idempotency_token_sentinel() {
        assert_eq!(
            IdempotencyToken::SENTINEL.to_string(),
            "i11111111-1111-1111-1111-111111111111"
        );
    }

    /// This test generates an "arbitrary" State, but uses a fixed seed for the
    /// randomness, so that it's deterministic. This lets us assert the
    /// serialization of that State against a golden file that's committed,
    /// making it easy to see what the serialization (used in an upcoming
    /// INSPECT feature) looks like.
    ///
    /// This golden will have to be updated each time we change State, but
    /// that's a feature, not a bug.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn state_inspect_serde_json() {
        const STATE_SERDE_JSON: &str = include_str!("state_serde.json");
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let tree = any_state::<u64>(6..8).new_tree(&mut runner).unwrap();
        let json = serde_json::to_string_pretty(&tree.current()).unwrap();
        assert_eq!(
            json.trim(),
            STATE_SERDE_JSON.trim(),
            "\n\nNEW GOLDEN\n{}\n",
            json
        );
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn sneaky_downgrades(dyncfgs: ConfigUpdates) {
        let mut clients = new_test_client_cache(&dyncfgs);
        let shard_id = ShardId::new();

        async fn open_and_write(
            clients: &mut PersistClientCache,
            version: semver::Version,
            shard_id: ShardId,
        ) -> Result<(), tokio::task::JoinError> {
            clients.cfg.build_version = version.clone();
            clients.clear_state_cache();
            let client = clients.open(PersistLocation::new_in_mem()).await.unwrap();
            // Run in a task so we can catch the panic.
            mz_ore::task::spawn(|| version.to_string(), async move {
                let () = client
                    .upgrade_version::<String, (), u64, i64>(shard_id, Diagnostics::for_tests())
                    .await
                    .expect("valid usage");
                let (mut write, _) = client.expect_open::<String, (), u64, i64>(shard_id).await;
                let current = *write.upper().as_option().unwrap();
                // Do a write so that we tag the state with the version.
                write
                    .expect_compare_and_append_batch(&mut [], current, current + 1)
                    .await;
            })
            .into_tokio_handle()
            .await
        }

        // Start at v0.10.0.
        let res = open_and_write(&mut clients, Version::new(0, 10, 0), shard_id).await;
        assert_ok!(res);

        // Upgrade to v0.11.0 is allowed.
        let res = open_and_write(&mut clients, Version::new(0, 11, 0), shard_id).await;
        assert_ok!(res);

        // Downgrade to v0.10.0 is no longer allowed.
        let res = open_and_write(&mut clients, Version::new(0, 10, 0), shard_id).await;
        assert!(res.unwrap_err().is_panic());

        // Downgrade to v0.9.0 is _NOT_ allowed.
        let res = open_and_write(&mut clients, Version::new(0, 9, 0), shard_id).await;
        assert!(res.unwrap_err().is_panic());
    }

    #[mz_ore::test]
    fn runid_roundtrip() {
        proptest!(|(runid: RunId)| {
            let runid_str = runid.to_string();
            let parsed = RunId::from_str(&runid_str);
            prop_assert_eq!(parsed, Ok(runid));
        });
    }
}
