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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::Cursor;
use std::ops::Range;

use bytes::BufMut;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_persist_types::Codec;
use prost::Message;
use semver::Version;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::Error;
use crate::gen::persist::{
    proto_batch_inline, ProtoArrangement, ProtoBatchFormat, ProtoBatchInline, ProtoMeta,
    ProtoStreamRegistration, ProtoTraceBatchMeta, ProtoTraceBatchPartInline, ProtoU64Antichain,
    ProtoU64Description, ProtoUnsealedBatchInline, ProtoUnsealedBatchMeta,
};
use crate::indexed::cache::{BlobCache, CacheHint};
use crate::indexed::columnar::parquet::{
    decode_trace_parquet, decode_unsealed_parquet, encode_trace_parquet, encode_unsealed_parquet,
};
use crate::indexed::columnar::ColumnarRecords;
use crate::indexed::snapshot::UnsealedSnapshot;
use crate::storage::{BlobRead, SeqNo};

/// An internally unique id for a persisted stream. External users identify
/// streams with a string, which is then mapped internally to this.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Id(pub u64);

/// The structure serialized and stored as an entry in a
/// [crate::storage::Log].
///
/// Invariants:
/// - The updates field is non-empty.
#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    /// Pairs of stream id and the updates themselves.
    //
    // We could require that each Id is included at most once, but at the
    // moment, there's no particular reason we'd need to.
    pub updates: Vec<(Id, Vec<((Vec<u8>, Vec<u8>), u64, i64)>)>,
}

/// The structure serialized and stored as a value in [crate::storage::Blob]
/// storage for metadata keys.
///
/// Invariants:
/// - All strings in id_mapping are unique.
/// - All ids in id_mapping are unique.
/// - All strings in graveyard are unique.
/// - All ids in graveyard are unique.
/// - None of the strings in graveyard are present in any of the (string, id)
///   tuples in id_mapping.
/// - None of the ids in graveyard are present in any of the (string, id) tuples
///   in id_mapping.
/// - The same set of ids are present in id_mapping, unsealeds, and traces.
/// - For each id, the ts_lower in the unsealed is <= the ts_upper in the
///   corresponding trace. (This is less than equals and not strictly equals
///   because truncating the unnecessary elements out of unsealed is fallible, and
///   is allowed to lag behind the migration of new data into trace)
/// - id_mapping.len() + graveyard.len() is == next_stream_id.
/// - All of the keys for trace and unsealed batches are unique across all persisted
///   streams.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobMeta {
    /// Which mutations are included in the represented state.
    ///
    /// Persist is a state machine, with all mutating requests modeled as input
    /// state changes sequenced into a log. Periodically those state changes are
    /// applied and the resulting state is written out to blob storage. This
    /// field indicates which prefix of the log (`0..=self.seqno`) has been
    /// included in the state represented by this BlobMeta. SeqNo(0) represents
    /// the initial empty state, the first mutation is SeqNo(1).
    ///
    /// Invariant: For each UnsealedMeta in `unsealeds`, this is >= the last
    /// batch's upper. If they are not equal, there is logically an empty batch
    /// between [last batch's upper, self.seqno).
    pub seqno: SeqNo,
    /// Internal stream id indexed by external stream name.
    ///
    /// Invariant: Each stream name and stream id are in here at most once.
    pub id_mapping: Vec<StreamRegistration>,
    /// Set of deleted streams, indexed by external stream name.
    pub graveyard: Vec<StreamRegistration>,
    /// Arrangements indexed by stream id.
    ///
    /// Invariant: Each stream id is in here at most once.
    pub arrangements: Vec<ArrangementMeta>,
}

/// Registration information for a single stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamRegistration {
    /// The external stream name.
    pub name: String,
    /// The internal stream id.
    pub id: Id,
    /// The codec used to encode and decode keys in this stream.
    pub key_codec_name: String,
    /// The codec used to encode and decode values in this stream.
    pub val_codec_name: String,
}

/// The metadata necessary to reconstruct an Arrangement.
///
/// Invariants:
/// - The unsealed_batch SeqNo ranges are sorted and non-overlapping.
/// - The trace_batch Descriptions are sorted, non-overlapping, and contiguous.
/// - Every batch's since frontier is <= the overall trace's since frontier.
/// - The compaction level of trace_batches is weakly decreasing when iterating
///   from oldest to most recent time intervals.
/// - Every trace_batch's upper is <= the overall trace's seal frontier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArrangementMeta {
    /// The stream this unsealed belongs to.
    pub id: Id,
    /// Frontier this trace has been sealed up to.
    pub seal: Antichain<u64>,
    /// Compaction frontier for the batches contained in this trace.
    /// There may still be batches containing updates at times < since, but the
    /// the trace only contains correct answers for times at or in advance of this
    /// of this frontier. Readers are expected to advance any updates < since to
    /// since.
    pub since: Antichain<u64>,
    /// The batches that make up the Unsealed.
    pub unsealed_batches: Vec<UnsealedBatchMeta>,
    /// The batches that make up the Trace.
    pub trace_batches: Vec<TraceBatchMeta>,
}

/// The metadata necessary to reconstruct a [BlobUnsealedBatch].
///
/// Invariants:
/// - The [lower, upper) interval of sequence numbers in desc is non-empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnsealedBatchMeta {
    /// The key to retrieve the [BlobUnsealedBatch] from blob storage.
    pub key: String,
    /// The format of the stored batch data.
    pub format: ProtoBatchFormat,
    /// Half-open interval [lower, upper) of sequence numbers that this batch
    /// contains updates for.
    pub desc: Range<SeqNo>,
    /// The maximum timestamp of any update contained in this batch.
    pub ts_upper: u64,
    /// The minimum timestamp from any update contained in this batch.
    pub ts_lower: u64,
    /// Size of the encoded batch.
    pub size_bytes: u64,
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

/// The metadata necessary to reconstruct an [UnsealedSnapshot].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsealedSnapshotMeta {
    /// A closed lower bound on the times of contained updates.
    pub ts_lower: Antichain<u64>,
    /// An open upper bound on the times of the contained updates.
    pub ts_upper: Antichain<u64>,
    /// The batches making up the snapshot.
    pub batches: Vec<UnsealedBatchMeta>,
}

impl UnsealedSnapshotMeta {
    /// Creates an UnsealedSnapshot by kicking off the necessary blob fetches.
    pub fn fetch<B: BlobRead>(self, blob: &BlobCache<B>) -> UnsealedSnapshot {
        let batches = self
            .batches
            .into_iter()
            .map(|x| blob.get_unsealed_batch_async(&x.key, CacheHint::MaybeAdd))
            .collect();
        UnsealedSnapshot {
            ts_lower: self.ts_lower,
            ts_upper: self.ts_upper,
            batches,
        }
    }
}

/// The structure serialized and stored as a value in [crate::storage::Blob]
/// storage for data keys corresponding to unsealed data.
///
/// Invariants:
/// - The [lower, upper) interval of sequence numbers in desc is non-empty.
/// - The updates field is non-empty.
#[derive(Clone, Debug)]
pub struct BlobUnsealedBatch {
    /// Which updates are included in this batch.
    pub desc: Range<SeqNo>,
    /// The updates themselves.
    pub updates: Vec<ColumnarRecords>,
}

/// The structure serialized and stored as a value in [crate::storage::Blob]
/// storage for data keys corresponding to trace data.
///
/// This batch represents the data that was originally written at some time in
/// [lower, upper) (more precisely !< lower and < upper). The individual record
/// times may have later been advanced by compaction to something <= since.
/// This means the ability to reconstruct the state of the collection at times < since
/// has been lost. However, there may still be records present in the batch whose
/// times are < since. Users iterating through updates must take care to advance
/// records with times < since to since in order to correctly answer queries at
/// times >= since.
///
/// Invariants:
/// - The [lower, upper) interval of times in desc is non-empty.
/// - The timestamp of each update is >= to desc.lower().
/// - The timestamp of each update is < desc.upper() iff desc.upper() > desc.since().
///   Otherwise the timestamp of each update is <= desc.since().
/// - The values in updates are sorted by (key, value, time).
/// - The values in updates are "consolidated", i.e. (key, value, time) is
///   unique.
/// - All entries have a non-zero diff.
///
/// TODO: disallow empty trace batch parts in the future so there is one unique way
/// to represent an empty trace batch.
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

impl LogEntry {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if this invariant is useful/harmful. Feel free to
        // remove it if it ends up not making sense.
        if self.updates.is_empty() {
            return Err("updates is empty".into());
        }
        Ok(())
    }
}

impl Default for BlobMeta {
    fn default() -> Self {
        BlobMeta {
            seqno: SeqNo(0),
            id_mapping: Vec::new(),
            graveyard: Vec::new(),
            arrangements: Vec::new(),
        }
    }
}

impl BlobMeta {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        for r in self.id_mapping.iter() {
            if names.contains(&r.name) {
                return Err(format!("duplicate external stream name: {}", r.name).into());
            }
            names.insert(r.name.clone());
            if ids.contains(&r.id) {
                return Err(format!("duplicate internal stream id: {:?}", r.id).into());
            }
            ids.insert(r.id);
        }

        let mut deleted_ids = HashSet::new();
        let mut deleted_names = HashSet::new();

        for r in self.graveyard.iter() {
            if names.contains(&r.name) {
                return Err(format!(
                    "duplicate external stream name {} across deleted and registered streams",
                    r.name
                )
                .into());
            }

            if ids.contains(&r.id) {
                return Err(format!(
                    "duplicate internal stream id {:?} across deleted and registered streams",
                    r.id
                )
                .into());
            }

            if deleted_names.contains(&r.name) {
                return Err(format!("duplicate deleted external stream name: {}", r.name).into());
            }
            deleted_names.insert(r.name.clone());

            if deleted_ids.contains(&r.id) {
                return Err(format!("duplicate deleted internal stream id: {:?}", r.id).into());
            }
            deleted_ids.insert(r.id);
        }

        let next_stream_id = self.next_stream_id();
        if u64::cast_from(deleted_ids.len() + ids.len()) != next_stream_id.0 {
            return Err(format!(
                "next stream {:?}, but only registered {} ids and deleted {} ids",
                next_stream_id,
                ids.len(),
                deleted_ids.len()
            )
            .into());
        }

        let mut arrangements = HashMap::new();
        for f in self.arrangements.iter() {
            if !ids.contains(&f.id) {
                return Err(format!("arrangements id {:?} not present in id_mapping", f.id).into());
            }

            if arrangements.contains_key(&f.id) {
                return Err(format!("duplicate arrangement: {:?}", f.id).into());
            }
            arrangements.insert(f.id, f);

            f.validate()?;
        }

        for id in ids.iter() {
            let arrangement = arrangements.get(id).ok_or_else(|| {
                Error::from(format!(
                    "id_mapping id {:?} not present in arrangements",
                    id
                ))
            })?;
            let unsealed_seqno_upper = arrangement.unsealed_seqno_upper();
            if !unsealed_seqno_upper.less_equal(&self.seqno) {
                return Err(Error::from(format!(
                    "id {:?} unsealed seqno_upper {:?} is not less or equal to the blob's seqno {:?}",
                    id, unsealed_seqno_upper, self.seqno,
                )));
            }
        }

        let mut batch_keys = HashSet::new();
        for a in self.arrangements.iter() {
            for batch in a.unsealed_batches.iter() {
                if batch_keys.contains(&batch.key) {
                    return Err(
                        format!("duplicate batch key found in unsealed: {}", batch.key).into(),
                    );
                }
                batch_keys.insert(batch.key.clone());
            }
            for batch in a.trace_batches.iter() {
                for key in batch.keys.iter() {
                    if batch_keys.contains(key) {
                        return Err(format!("duplicate batch key found in trace: {}", key).into());
                    }
                    batch_keys.insert(key.clone());
                }
            }
        }

        Ok(())
    }

    /// The next Id to issue for a stream being added to id_mapping.
    pub fn next_stream_id(&self) -> Id {
        let current_highest = self
            .id_mapping
            .iter()
            .chain(self.graveyard.iter())
            .map(|s| s.id)
            .max();
        current_highest.map_or(Id(0), |id| Id(id.0 + 1))
    }
}

impl Default for ArrangementMeta {
    fn default() -> Self {
        ArrangementMeta {
            id: Id(0),
            since: Antichain::from_elem(Timestamp::minimum()),
            seal: Antichain::from_elem(Timestamp::minimum()),
            unsealed_batches: Vec::new(),
            trace_batches: Vec::new(),
        }
    }
}

impl ArrangementMeta {
    /// Create a new [ArrangementMeta] belonging to `id`.
    pub fn new(id: Id) -> Self {
        ArrangementMeta {
            id,
            ..Default::default()
        }
    }

    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let mut unsealed_prev: Option<&UnsealedBatchMeta> = None;
        for meta in self.unsealed_batches.iter() {
            meta.validate()?;
            if let Some(prev) = unsealed_prev {
                if prev.desc.end > meta.desc.start {
                    return Err(format!(
                        "invalid batch sequence: {:?} followed by {:?}",
                        prev.desc, meta.desc
                    )
                    .into());
                }
            }
            unsealed_prev = Some(&meta)
        }

        let mut trace_prev: Option<&TraceBatchMeta> = None;
        for meta in self.trace_batches.iter() {
            if !PartialOrder::less_equal(meta.desc.since(), &self.since) {
                return Err(format!(
                    "invalid batch since: {:?} in advance of trace since {:?}",
                    meta.desc, self.since
                )
                .into());
            }

            if !PartialOrder::less_equal(meta.desc.upper(), &self.seal) {
                return Err(format!(
                    "invalid batch upper: {:?} in advance of trace seal {:?}",
                    meta.desc, self.seal,
                )
                .into());
            }

            meta.validate()?;

            if let Some(prev) = trace_prev {
                if prev.desc.upper() != meta.desc.lower() {
                    return Err(format!(
                        "invalid batch sequence: {:?} followed by {:?}",
                        prev.desc, meta.desc,
                    )
                    .into());
                }

                if prev.level < meta.level {
                    return Err(format!(
                        "invalid batch sequence: compaction level {} followed by {}",
                        prev.level, meta.level
                    )
                    .into());
                }
            }
            trace_prev = Some(&meta)
        }

        Ok(())
    }

    /// Returns an open upper bound on the seqnos contained in this unsealed.
    pub fn unsealed_seqno_upper(&self) -> SeqNo {
        self.unsealed_batches
            .last()
            .map_or_else(|| SeqNo(0), |meta| meta.desc.end)
    }

    /// Returns an open upper bound on the timestamps of data contained in this
    /// trace.
    pub fn trace_ts_upper(&self) -> Antichain<u64> {
        self.trace_batches.last().map_or_else(
            || Antichain::from_elem(Timestamp::minimum()),
            |meta| meta.desc.upper().clone(),
        )
    }
}

impl UnsealedBatchMeta {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if the equal case (an empty desc) is
        // useful/harmful. Feel free to make this a less_than if empty descs end
        // up making sense.
        if self.desc.end <= self.desc.start {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }

        Ok(())
    }
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
    pub fn validate_data<B: BlobRead>(&self, cache: &BlobCache<B>) -> Result<(), Error> {
        let mut prev: Option<(PrettyBytes<'_>, PrettyBytes<'_>, u64)> = None;
        let mut batches = vec![];
        for (idx, key) in self.keys.iter().enumerate() {
            let batch = cache
                .get_trace_batch_async(key, CacheHint::NeverAdd)
                .recv()?;
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
            let ((key, val), ts, diff) = update;
            let this = (PrettyBytes(&key), PrettyBytes(&val), ts);
            if let Some(prev) = prev {
                match prev.cmp(&this) {
                    Ordering::Less => {} // Correct.
                    Ordering::Equal => return Err(format!("unconsolidated: {:?}", this).into()),
                    Ordering::Greater => {
                        return Err(format!("unsorted: {:?} was before {:?}", prev, this).into())
                    }
                }
            }
            prev = Some(this);

            // Check data invariants.
            if diff == 0 {
                return Err(format!("update with 0 diff: {:?}", PrettyRecord(update)).into());
            }
        }

        Ok(())
    }
}

impl BlobUnsealedBatch {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if the equal case (an empty desc) is
        // useful/harmful. Feel free to make this a less_than if empty descs end
        // up making sense.
        if self.desc.end <= self.desc.start {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }
        // TODO: It's unclear if this invariant is useful/harmful. Feel free to
        // remove it if it ends up not making sense.
        if self.updates.is_empty() {
            return Err("updates is empty".into());
        }

        Ok(())
    }
}

// BlobUnsealedBatch doesn't really need to implement Codec (it's never stored
// as a key or value in a persisted record) but it's nice to have a common
// interface for this.
impl Codec for BlobUnsealedBatch {
    fn codec_name() -> String {
        "parquet[UnsealedBatch]".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        encode_unsealed_parquet(&mut buf.writer(), &self).expect("writes to BufMut are infallible");
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        decode_unsealed_parquet(&mut Cursor::new(&buf)).map_err(|err| err.to_string())
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

        let mut prev: Option<(PrettyBytes<'_>, PrettyBytes<'_>, u64)> = None;
        for update in self.updates.iter().flat_map(|u| u.iter()) {
            let ((key, val), ts, diff) = update;
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

            // Check ordering.
            let this = (PrettyBytes(&key), PrettyBytes(&val), ts);
            if let Some(prev) = prev {
                match prev.cmp(&this) {
                    Ordering::Less => {} // Correct.
                    Ordering::Equal => return Err(format!("unconsolidated: {:?}", this).into()),
                    Ordering::Greater => {
                        return Err(format!("unsorted: {:?} was before {:?}", prev, this).into())
                    }
                }
            }
            prev = Some(this);

            // Check data invariants.
            if diff == 0 {
                return Err(format!("update with 0 diff: {:?}", PrettyRecord(update)).into());
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

struct PrettyRecord<'a>(((&'a [u8], &'a [u8]), u64, i64));

impl fmt::Debug for PrettyRecord<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ((k, v), ts, diff) = &self.0;
        fmt::Debug::fmt(&((PrettyBytes(&k), PrettyBytes(&v)), ts, diff), f)
    }
}

impl From<ProtoMeta> for BlobMeta {
    fn from(x: ProtoMeta) -> Self {
        let mut meta = BlobMeta {
            seqno: SeqNo(x.seqno),
            id_mapping: x.id_mapping.into_iter().map(|x| x.into()).collect(),
            graveyard: x.graveyard.into_iter().map(|x| x.into()).collect(),
            arrangements: x.arrangements.into_iter().map(|x| x.into()).collect(),
        };
        // TODO: Make the types on BlobMeta be HashMaps and remove this sort.
        meta.id_mapping.sort_by_key(|x| x.id);
        meta.graveyard.sort_by_key(|x| x.id);
        meta.arrangements.sort_by_key(|x| x.id);
        meta
    }
}

impl From<(u64, ProtoArrangement)> for ArrangementMeta {
    fn from(x: (u64, ProtoArrangement)) -> Self {
        let (id, x) = x;
        ArrangementMeta {
            id: Id(id),
            seal: x
                .seal
                .map_or_else(|| Antichain::from_elem(u64::minimum()), |x| x.into()),
            since: x
                .since
                .map_or_else(|| Antichain::from_elem(u64::minimum()), |x| x.into()),
            unsealed_batches: x.unsealed_batches.into_iter().map(|x| x.into()).collect(),
            trace_batches: x.trace_batches.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl From<(u64, ProtoStreamRegistration)> for StreamRegistration {
    fn from(x: (u64, ProtoStreamRegistration)) -> Self {
        let (id, x) = x;
        StreamRegistration {
            id: Id(id),
            name: x.name,
            key_codec_name: x.key_codec_name,
            val_codec_name: x.val_codec_name,
        }
    }
}

impl From<ProtoUnsealedBatchMeta> for UnsealedBatchMeta {
    fn from(x: ProtoUnsealedBatchMeta) -> Self {
        UnsealedBatchMeta {
            format: x.format(),
            key: x.key,
            desc: SeqNo(x.seqno_lower)..SeqNo(x.seqno_upper),
            ts_upper: x.ts_upper,
            ts_lower: x.ts_lower,
            size_bytes: x.size_bytes,
        }
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

impl From<(&BlobMeta, &Version)> for ProtoMeta {
    fn from(x: (&BlobMeta, &Version)) -> Self {
        let (x, b) = x;
        ProtoMeta {
            version: b.to_string(),
            seqno: x.seqno.0,
            id_mapping: x.id_mapping.iter().map(|x| (x.id.0, x.into())).collect(),
            graveyard: x.graveyard.iter().map(|x| (x.id.0, x.into())).collect(),
            arrangements: x.arrangements.iter().map(|x| (x.id.0, x.into())).collect(),
        }
    }
}

impl From<&ArrangementMeta> for ProtoArrangement {
    fn from(x: &ArrangementMeta) -> Self {
        ProtoArrangement {
            since: Some((&x.since).into()),
            seal: Some((&x.seal).into()),
            unsealed_batches: x.unsealed_batches.iter().map(|x| x.into()).collect(),
            trace_batches: x.trace_batches.iter().map(|x| x.into()).collect(),
        }
    }
}

impl From<&StreamRegistration> for ProtoStreamRegistration {
    fn from(x: &StreamRegistration) -> Self {
        ProtoStreamRegistration {
            name: x.name.clone(),
            key_codec_name: x.key_codec_name.clone(),
            val_codec_name: x.val_codec_name.clone(),
        }
    }
}

impl From<&UnsealedBatchMeta> for ProtoUnsealedBatchMeta {
    fn from(x: &UnsealedBatchMeta) -> Self {
        ProtoUnsealedBatchMeta {
            key: x.key.clone(),
            format: x.format.into(),
            seqno_upper: x.desc.end.0,
            seqno_lower: x.desc.start.0,
            ts_upper: x.ts_upper,
            ts_lower: x.ts_lower,
            size_bytes: x.size_bytes,
        }
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

/// Encodes the inline metadata for an unsealed batch into a base64 string.
pub fn encode_unsealed_inline_meta(batch: &BlobUnsealedBatch, format: ProtoBatchFormat) -> String {
    let inline = ProtoBatchInline {
        batch_type: Some(proto_batch_inline::BatchType::Unsealed(
            ProtoUnsealedBatchInline {
                format: format.into(),
                seqno_lower: batch.desc.start.0,
                seqno_upper: batch.desc.end.0,
            },
        )),
    };
    let inline_encoded = inline.encode_to_vec();
    base64::encode(inline_encoded)
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

/// Decodes the inline metadata for an unsealed batch from a base64 string.
pub fn decode_unsealed_inline_meta(
    inline_base64: Option<&String>,
) -> Result<(ProtoBatchFormat, ProtoUnsealedBatchInline), Error> {
    let inline_base64 = inline_base64.ok_or("missing batch metadata")?;
    let inline_encoded = base64::decode(&inline_base64).map_err(|err| err.to_string())?;
    let inline = ProtoBatchInline::decode(&*inline_encoded).map_err(|err| err.to_string())?;
    match inline.batch_type {
        Some(proto_batch_inline::BatchType::Unsealed(x)) => {
            let format = ProtoBatchFormat::from_i32(x.format)
                .ok_or_else(|| Error::from(format!("unknown format: {}", x.format)))?;
            Ok((format, x))
        }
        x => return Err(format!("incorrect batch type: {:?}", x).into()),
    }
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

    use tokio::runtime::Runtime as AsyncRuntime;

    use crate::error::Error;
    use crate::indexed::columnar::ColumnarRecordsVec;
    use crate::indexed::Metrics;
    use crate::mem::MemRegistry;
    use crate::workload::DataGenerator;

    use super::*;

    fn update_with_ts(ts: u64) -> ((Vec<u8>, Vec<u8>), u64, i64) {
        (("".into(), "".into()), ts, 1)
    }

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

    fn batch_meta_full(lower: u64, upper: u64, since: u64, level: u64) -> TraceBatchMeta {
        TraceBatchMeta {
            keys: vec![],
            format: ProtoBatchFormat::Unknown,
            desc: u64_desc_since(lower, upper, since),
            level,
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

    fn unsealed_batch_meta(lower: u64, upper: u64) -> UnsealedBatchMeta {
        UnsealedBatchMeta {
            key: "".to_string(),
            format: ProtoBatchFormat::Unknown,
            desc: SeqNo(lower)..SeqNo(upper),
            ts_upper: 0,
            ts_lower: 0,
            size_bytes: 0,
        }
    }

    fn columnar_records(updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> Vec<ColumnarRecords> {
        updates.iter().collect::<ColumnarRecordsVec>().into_inner()
    }

    impl From<(&'_ str, Id)> for StreamRegistration {
        fn from(x: (&'_ str, Id)) -> Self {
            let (name, id) = x;
            StreamRegistration {
                name: name.to_owned(),
                id,
                key_codec_name: "".into(),
                val_codec_name: "".into(),
            }
        }
    }

    #[test]
    fn log_entry_validate() {
        // Normal case
        let b = LogEntry {
            updates: vec![(Id(0), vec![update_with_key(0, "0")])],
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: LogEntry = LogEntry { updates: vec![] };
        assert_eq!(b.validate(), Err("updates is empty".into()));
    }

    #[test]
    fn unsealed_batch_validate() {
        // Normal case
        let b = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(2),
            updates: columnar_records(vec![update_with_ts(0), update_with_ts(1)]),
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: BlobUnsealedBatch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(2),
            updates: vec![],
        };
        assert_eq!(b.validate(), Err("updates is empty".into()));

        // Invalid desc
        let b: BlobUnsealedBatch = BlobUnsealedBatch {
            desc: SeqNo(2)..SeqNo(0),
            updates: vec![],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("invalid desc: SeqNo(2)..SeqNo(0)"))
        );

        // Empty desc
        let b: BlobUnsealedBatch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(0),
            updates: vec![],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("invalid desc: SeqNo(0)..SeqNo(0)"))
        );
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

        // Not sorted by key
        let b = BlobTraceBatchPart {
            desc: u64_desc(0, 2),
            index: 0,
            updates: columnar_records(vec![update_with_key(0, "1"), update_with_key(1, "0")]),
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "unsorted: (\"1\", \"\", 0) was before (\"0\", \"\", 1)"
            ))
        );

        // Not consolidated
        let b = BlobTraceBatchPart {
            desc: u64_desc(0, 2),
            index: 0,
            updates: columnar_records(vec![update_with_key(0, "0"), update_with_key(0, "0")]),
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("unconsolidated: (\"0\", \"\", 0)"))
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

    #[test]
    fn trace_batch_meta_validate_data() -> Result<(), Error> {
        let async_runtime = Arc::new(AsyncRuntime::new()?);
        let blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            async_runtime,
            MemRegistry::new().blob_no_reentrance()?,
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

        let batch0_size_bytes = blob.set_trace_batch("b0".into(), batch0, format)?;
        let batch1_size_bytes = blob.set_trace_batch("b1".into(), batch1, format)?;
        let size_bytes = batch0_size_bytes + batch1_size_bytes;
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1".into()],
            format,
            desc: batch_desc.clone(),
            level: 0,
            size_bytes,
        };

        // Normal case:
        assert_eq!(batch_meta.validate_data(&blob), Ok(()));

        // Incorrect desc
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1".into()],
            format,
            desc: u64_desc_since(1, 3, 0),
            level: 0,
            size_bytes,
        };
        assert_eq!(batch_meta.validate_data(&blob), Err(Error::from("invalid trace batch part desc expected Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } } got Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));
        // Key with no corresponding batch part
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1".into(), "b2".into()],
            format,
            desc: batch_desc.clone(),
            level: 0,
            size_bytes,
        };
        assert_eq!(
            batch_meta.validate_data(&blob),
            Err(Error::from("no blob for trace batch at key: b2"))
        );
        // Batch parts not in index order
        let batch_meta = TraceBatchMeta {
            keys: vec!["b1".into(), "b0".into()],
            format,
            desc: batch_desc.clone(),
            level: 0,
            size_bytes,
        };
        assert_eq!(
            batch_meta.validate_data(&blob),
            Err(Error::from(
                "invalid index for blob trace batch part at key b1 expected 0 got 1"
            ))
        );

        // Unsorted updates across trace batch parts.
        let batch1_unsorted = BlobTraceBatchPart {
            desc: batch_desc.clone(),
            index: 1,
            updates: vec![
                (("k2".as_bytes(), "v2".as_bytes()), 2, 1),
                (("k5".as_bytes(), "v5".as_bytes()), 2, 1),
            ]
            .iter()
            .collect::<ColumnarRecordsVec>()
            .into_inner(),
        };
        let batch1_unsorted_size_bytes =
            blob.set_trace_batch("b1_unsorted".into(), batch1_unsorted, format)?;
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1_unsorted".into()],
            format,
            desc: batch_desc.clone(),
            level: 0,
            size_bytes: batch0_size_bytes + batch1_unsorted_size_bytes,
        };
        assert_eq!(
            batch_meta.validate_data(&blob),
            Err(Error::from(
                "unsorted: (\"k3\", \"v3\", 2) was before (\"k2\", \"v2\", 2)"
            ))
        );
        // Unconsolidated updates across trace batch parts.
        let batch1_unconsolidated = BlobTraceBatchPart {
            desc: batch_desc.clone(),
            index: 1,
            updates: vec![
                (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                (("k5".as_bytes(), "v5".as_bytes()), 2, 1),
            ]
            .iter()
            .collect::<ColumnarRecordsVec>()
            .into_inner(),
        };
        let batch1_unconsolidated_size_bytes =
            blob.set_trace_batch("b1_unconsolidated".into(), batch1_unconsolidated, format)?;
        let batch_meta = TraceBatchMeta {
            keys: vec!["b0".into(), "b1_unconsolidated".into()],
            format,
            desc: batch_desc,
            level: 0,
            size_bytes: batch0_size_bytes + batch1_unconsolidated_size_bytes,
        };
        assert_eq!(
            batch_meta.validate_data(&blob),
            Err(Error::from("unconsolidated: (\"k3\", \"v3\", 2)"))
        );

        Ok(())
    }

    #[test]
    fn trace_meta_validate() {
        // Empty
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(0),
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![batch_meta(0, 1), batch_meta(1, 2)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(2),
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Gap
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![batch_meta(0, 1), batch_meta(2, 3)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(3),
            ..Default::default()
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch sequence: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [1] }, since: Antichain { elements: [0] } } followed by Description { lower: Antichain { elements: [2] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));

        // Overlapping
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![batch_meta(0, 2), batch_meta(1, 3)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(3),
            ..Default::default()
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch sequence: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } } followed by Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));

        // Normal case: trace since before nonzero trace upper
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![batch_meta(0, 1), batch_meta(1, 2)],
            since: Antichain::from_elem(1),
            seal: Antichain::from_elem(2),
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Trace since at nonzero trace seal
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![batch_meta(0, 2), batch_meta(2, 3)],
            since: Antichain::from_elem(3),
            seal: Antichain::from_elem(3),
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Trace since in advance of nonzero trace seal
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![batch_meta(0, 2), batch_meta(2, 3)],
            since: Antichain::from_elem(4),
            seal: Antichain::from_elem(3),
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case: batch since at or before trace since
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![batch_meta(0, 1), batch_meta_full(1, 2, 1, 1)],
            since: Antichain::from_elem(1),
            seal: Antichain::from_elem(2),
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Batch since in advance of trace since
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![batch_meta(0, 1), batch_meta_full(1, 2, 2, 1)],
            since: Antichain::from_elem(1),
            seal: Antichain::from_elem(2),
            ..Default::default()
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch since: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [2] } } in advance of trace since Antichain { elements: [1] }")));

        // Normal case: decreasing or constant compaction levels
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![
                batch_meta_full(0, 1, 0, 2),
                batch_meta_full(1, 2, 0, 2),
                batch_meta_full(2, 3, 0, 1),
            ],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(3),
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Increasing compaction level.
        let b = ArrangementMeta {
            id: Id(0),
            trace_batches: vec![batch_meta_full(0, 1, 0, 1), batch_meta_full(1, 2, 0, 2)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(2),
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid batch sequence: compaction level 1 followed by 2"
            ))
        );
    }

    #[test]
    fn unsealed_batch_meta_validate() {
        // Normal case
        let b = unsealed_batch_meta(0, 1);
        assert_eq!(b.validate(), Ok(()));

        // Empty interval
        let b = unsealed_batch_meta(0, 0);
        assert_eq!(
            b.validate(),
            Err(Error::from("invalid desc: SeqNo(0)..SeqNo(0)"))
        );

        // Invalid desc
        let b = unsealed_batch_meta(1, 0);
        assert_eq!(
            b.validate(),
            Err(Error::from("invalid desc: SeqNo(1)..SeqNo(0)"))
        );
    }

    #[test]
    fn unsealed_meta_validate() {
        // Empty
        let b = ArrangementMeta {
            id: Id(0),
            unsealed_batches: vec![],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case
        let b = ArrangementMeta {
            id: Id(0),
            unsealed_batches: vec![unsealed_batch_meta(0, 1), unsealed_batch_meta(1, 2)],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case: gap between sequence number ranges.
        let b = ArrangementMeta {
            id: Id(0),
            unsealed_batches: vec![unsealed_batch_meta(0, 1), unsealed_batch_meta(2, 3)],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()),);

        // Overlapping
        let b = ArrangementMeta {
            id: Id(0),
            unsealed_batches: vec![unsealed_batch_meta(0, 2), unsealed_batch_meta(1, 3)],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid batch sequence: SeqNo(0)..SeqNo(2) followed by SeqNo(1)..SeqNo(3)"
            ))
        );
    }

    #[test]
    fn blob_meta_validate() {
        // Empty
        let b = BlobMeta::default();
        assert_eq!(b.validate(), Ok(()));

        // Normal case
        let b = BlobMeta {
            id_mapping: vec![("0", Id(0)).into(), ("1", Id(1)).into()],
            arrangements: vec![ArrangementMeta::new(Id(0)), ArrangementMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Duplicate external stream id
        let b = BlobMeta {
            id_mapping: vec![("1", Id(0)).into(), ("1", Id(1)).into()],
            arrangements: vec![ArrangementMeta::new(Id(0)), ArrangementMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate external stream name: 1"))
        );

        // Duplicate internal stream id
        let b = BlobMeta {
            id_mapping: vec![("0", Id(1)).into(), ("1", Id(1)).into()],
            arrangements: vec![ArrangementMeta::new(Id(0)), ArrangementMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate internal stream id: Id(1)"))
        );

        // Missing arrangement
        let b = BlobMeta {
            id_mapping: vec![("0", Id(0)).into()],
            arrangements: vec![],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "id_mapping id Id(0) not present in arrangements"
            ))
        );

        // Extra arrangement
        let b = BlobMeta {
            id_mapping: vec![],
            arrangements: vec![ArrangementMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "arrangements id Id(0) not present in id_mapping"
            ))
        );

        // Duplicate in arrangements
        let b = BlobMeta {
            id_mapping: vec![("0", Id(0)).into()],
            arrangements: vec![ArrangementMeta::new(Id(0)), ArrangementMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate arrangement: Id(0)"))
        );

        // Normal case: unsealed ts_lower < ts_upper
        let b = BlobMeta {
            id_mapping: vec![("0", Id(0)).into()],
            arrangements: vec![ArrangementMeta {
                id: Id(0),
                unsealed_batches: vec![],
                trace_batches: vec![batch_meta(0, 1)],
                since: Antichain::from_elem(0),
                seal: Antichain::from_elem(1),
            }],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()),);

        // Normal case: unsealed ts_lower at ts_upper
        let b = BlobMeta {
            id_mapping: vec![("0", Id(0)).into()],
            arrangements: vec![ArrangementMeta {
                id: Id(0),
                unsealed_batches: vec![],
                trace_batches: vec![batch_meta(0, 1)],
                since: Antichain::from_elem(0),
                seal: Antichain::from_elem(1),
            }],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()),);

        // seqno less than one of the unsealed seqno uppers
        let b = BlobMeta {
            id_mapping: vec![("0", Id(0)).into()],
            seqno: SeqNo(2),
            arrangements: vec![ArrangementMeta {
                id: Id(0),
                unsealed_batches: vec![unsealed_batch_meta(0, 3)],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "id Id(0) unsealed seqno_upper SeqNo(3) is not less or equal to the blob's seqno SeqNo(2)"
            ))
        );

        // Duplicate id in graveyard.
        let b = BlobMeta {
            graveyard: vec![("deleted", Id(0)).into(), ("1", Id(0)).into()],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate deleted internal stream id: Id(0)"))
        );

        // Duplicate stream name in graveyard.
        let b = BlobMeta {
            graveyard: vec![("deleted", Id(0)).into(), ("deleted", Id(1)).into()],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from(
                "duplicate deleted external stream name: deleted"
            ))
        );

        // Duplicate id across graveyard and id_mapping.
        let b = BlobMeta {
            id_mapping: vec![("deleted", Id(0)).into()],
            graveyard: vec![("1", Id(0)).into()],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from(
                "duplicate internal stream id Id(0) across deleted and registered streams"
            ))
        );

        // Duplicate stream name across graveyard and id_mapping.
        let b = BlobMeta {
            id_mapping: vec![("name", Id(1)).into()],
            graveyard: vec![("name", Id(0)).into()],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from(
                "duplicate external stream name name across deleted and registered streams"
            ))
        );

        // Next stream id != id_mapping + deleted
        let b = BlobMeta {
            id_mapping: vec![("name", Id(1)).into()],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from(
                "next stream Id(2), but only registered 1 ids and deleted 0 ids"
            ))
        );

        let b = BlobMeta {
            id_mapping: vec![("0", Id(0)).into(), ("1", Id(1)).into()],
            seqno: SeqNo(2),
            arrangements: vec![
                ArrangementMeta {
                    id: Id(0),
                    unsealed_batches: vec![unsealed_batch_meta(0, 1)],
                    ..Default::default()
                },
                ArrangementMeta {
                    id: Id(1),
                    unsealed_batches: vec![unsealed_batch_meta(0, 1)],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate batch key found in unsealed: "))
        );

        let trace_meta = TraceBatchMeta {
            keys: vec!["duplicate".to_string()],
            format: ProtoBatchFormat::Unknown,
            desc: u64_desc(0, 1),
            level: 1,
            size_bytes: 0,
        };

        let b = BlobMeta {
            id_mapping: vec![("0", Id(0)).into(), ("1", Id(1)).into()],
            arrangements: vec![
                ArrangementMeta {
                    id: Id(0),
                    trace_batches: vec![trace_meta.clone()],
                    since: Antichain::from_elem(0),
                    seal: Antichain::from_elem(1),
                    ..Default::default()
                },
                ArrangementMeta {
                    id: Id(1),
                    trace_batches: vec![trace_meta],
                    since: Antichain::from_elem(0),
                    seal: Antichain::from_elem(1),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate batch key found in trace: duplicate"))
        );
    }

    #[test]
    fn encoded_batch_sizes() {
        fn sizes(data: DataGenerator) -> (usize, usize) {
            let unsealed = BlobUnsealedBatch {
                desc: SeqNo(0)..SeqNo(1),
                updates: data.batches().collect(),
            };
            let trace = BlobTraceBatchPart {
                desc: Description::new(
                    Antichain::from_elem(0),
                    Antichain::from_elem(1),
                    Antichain::from_elem(0),
                ),
                index: 0,
                updates: data.batches().collect(),
            };
            let (mut unsealed_buf, mut trace_buf) = (Vec::new(), Vec::new());
            unsealed.encode(&mut unsealed_buf);
            trace.encode(&mut trace_buf);
            (unsealed_buf.len(), trace_buf.len())
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
            "1/1=(481, 501) 25/1=(2229, 2249) 1000/1=(72468, 72488) 1000/100=(106557, 106577)"
        );
    }
}
