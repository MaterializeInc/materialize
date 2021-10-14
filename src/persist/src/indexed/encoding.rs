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
use std::{fmt, io};

use abomonation::abomonated::Abomonated;
use abomonation_derive::Abomonation;
use differential_dataflow::trace::Description;
use ore::cast::CastFrom;
use persist_types::Codec;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::Error;
use crate::storage::SeqNo;

/// An internally unique id for a persisted stream. External users identify
/// streams with a string, which is then mapped internally to this.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Abomonation)]
pub struct Id(pub u64);

/// The structure serialized and stored as an entry in a
/// [crate::storage::Log].
///
/// Invariants:
/// - The updates field is non-empty.
#[derive(Debug, Abomonation)]
pub struct LogEntry {
    /// Pairs of stream id and the updates themselves.
    //
    // We could require that each Id is included at most once, but at the
    // moment, there's no particular reason we'd need to.
    pub updates: Vec<(Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>)>,
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
#[derive(Clone, Debug, Eq, PartialEq, Abomonation)]
pub struct BlobMeta {
    /// The next internal stream id to assign.
    pub next_stream_id: Id,
    /// The position of log the last time data was step'd into unsealeds.
    ///
    /// Invariant: For each UnsealedMeta in `unsealeds`, this is >= the last
    /// batch's upper. If they are not equal, there is logically an empty batch
    /// between [last batch's upper, unsealeds_seqno_upper).
    pub unsealeds_seqno_upper: SeqNo,
    /// Internal stream id indexed by external stream name.
    ///
    /// Invariant: Each stream name and stream id are in here at most once.
    pub id_mapping: Vec<StreamRegistration>,
    /// Set of deleted streams, indexed by external stream name.
    pub graveyard: Vec<StreamRegistration>,
    /// Unsealeds indexed by stream id.
    ///
    /// Invariant: Each stream id is in here at most once. This would be more
    /// naturally be modeled as a map from Id to UnsealedMeta, but that
    /// doesn't work with Abomonation.
    pub unsealeds: Vec<UnsealedMeta>,
    /// Traces indexed by stream id.
    ///
    /// Invariant: Each stream id is in here at most once. This would be more
    /// naturally be modeled as a map from Id to TraceMeta, but that doesn't
    /// work with Abomonation.
    pub traces: Vec<TraceMeta>,
}

/// Registration information for a single stream.
#[derive(Clone, Debug, Eq, PartialEq, Abomonation)]
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

/// The metadata necessary to reconstruct an Unsealed.
///
/// Invariants:
/// - The batch SeqNo ranges are sorted and non-overlapping.
#[derive(Clone, Debug, Eq, PartialEq, Abomonation)]
pub struct UnsealedMeta {
    /// The stream this unsealed belongs to.
    pub id: Id,
    /// A lower bound of data contained by this Unsealed. Data before this may
    /// be present in the batches, but has been logically moved into the trace
    /// and should be ignored.
    pub ts_lower: Antichain<u64>,
    /// The batches that make up the Unsealed.
    pub batches: Vec<UnsealedBatchMeta>,

    /// TODO: next_blob_id is deprecated, remove this once we can safely bump
    /// BlobMeta::CURRENT_VERSION.
    pub next_blob_id: u64,
}

/// The metadata necessary to reconstruct a [BlobUnsealedBatch].
///
/// Invariants:
/// - The [lower, upper) interval of sequence numbers in desc is non-empty.
#[derive(Clone, Debug, Eq, PartialEq, Abomonation)]
pub struct UnsealedBatchMeta {
    /// The key to retrieve the [BlobUnsealedBatch] from blob storage.
    pub key: String,
    /// Half-open interval [lower, upper) of sequence numbers that this batch
    /// contains updates for.
    pub desc: Description<SeqNo>,
    /// The maximum timestamp of any update contained in this batch.
    pub ts_upper: u64,
    /// The minimum timestamp from any update contained in this batch.
    pub ts_lower: u64,
    /// Size of the encoded batch.
    pub size_bytes: u64,
}

/// The metadata necessary to reconstruct a Trace.
///
/// Invariants:
/// - The batch Descriptions are sorted, non-overlapping, and contiguous.
/// - The since frontier is either 0 or < the trace's sealed frontier.
/// - Every batch's since frontier is <= the overall trace's since frontier.
/// - The compaction level of batches is weakly decreasing when iterating from oldest
///   to most recent time intervals.
/// - Every batch's upper is <= the overall trace's seal frontier.
/// - TODO: key uniqueness invariants?
#[derive(Clone, Debug, Eq, PartialEq, Abomonation)]
pub struct TraceMeta {
    /// The stream this trace belongs to.
    pub id: Id,
    /// The batches that make up the Trace.
    pub batches: Vec<TraceBatchMeta>,
    /// Compaction frontier for the batches contained in this trace.
    /// There may still be batches containing updates at times < since, but the
    /// the trace only contains correct answers for times at or in advance of this
    /// of this frontier. Readers are expected to advance any updates < since to
    /// since.
    pub since: Antichain<u64>,
    /// Frontier this trace has been sealed up to.
    pub seal: Antichain<u64>,

    /// TODO: next_blob_id is deprecated, remove this once we can safely bump
    /// BlobMeta::CURRENT_VERSION.
    pub next_blob_id: u64,
}

/// The metadata necessary to reconstruct a [BlobTraceBatch].
///
/// Invariants:
/// - The Description's time interval is non-empty.
/// - TODO: key invariants?
#[derive(Clone, Debug, Eq, PartialEq, Abomonation)]
pub struct TraceBatchMeta {
    /// The key to retrieve the batch's data from the blob store.
    pub key: String,
    /// The half-open time interval `[lower, upper)` this batch contains data
    /// for.
    pub desc: Description<u64>,
    /// The compaction level of each batch.
    pub level: u64,
    /// Size of the encoded batch.
    pub size_bytes: u64,
}

/// The structure serialized and stored as a value in [crate::storage::Blob]
/// storage for data keys corresponding to unsealed data.
///
/// Invariants:
/// - The [lower, upper) interval of sequence numbers in desc is non-empty.
/// - The values in updates are sorted by (time, key, value).
/// - The values in updates are "consolidated", i.e. (time, key, value) is
///   unique.
/// - All entries have a non-zero diff.
/// - The updates field is non-empty.
#[derive(Clone, Debug, Eq, PartialEq, Abomonation)]
pub struct BlobUnsealedBatch {
    /// Which updates are included in this batch.
    pub desc: Description<SeqNo>,
    /// The updates themselves.
    pub updates: Vec<((Vec<u8>, Vec<u8>), u64, isize)>,
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
/// - (Intentionally no invariant around update non-emptiness because we might
///   need empty batches to make the timestamps line up.)
///
/// TODO: This probably wants to be a different level of abstraction, so we can
/// put multiple small batches in a single blob but also break a very large
/// batch over multiple blobs. We also may want to break the latter into chunks
/// for checksum and encryption?
#[derive(Clone, Debug, Eq, PartialEq, Abomonation)]
pub struct BlobTraceBatch {
    /// Which updates are included in this batch.
    pub desc: Description<u64>,
    /// The updates themselves.
    pub updates: Vec<((Vec<u8>, Vec<u8>), u64, isize)>,
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
            next_stream_id: Id(0),
            unsealeds_seqno_upper: SeqNo(0),
            id_mapping: Vec::new(),
            graveyard: Vec::new(),
            unsealeds: Vec::new(),
            traces: Vec::new(),
        }
    }
}

struct ExtendWriteAdapter<'e, E>(&'e mut E);

impl<'e, E: for<'a> Extend<&'a u8>> io::Write for ExtendWriteAdapter<'e, E> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.0.extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

// The encoding of BlobMeta is:
// ```
// buf[0]          = <BlobMeta::CURRENT_VERSION>
// buf[1..3]       = <BlobMeta::MAGIC>
// buf[3..3+N]     = <the N-byte Abomonation serialization of BlobMeta>
// buf[3+N..3+N+2] = <BlobMeta::MAGIC>
// ```
//
// This will allow us to gracefully detect changes at startup and react to them.
// MZ is not (yet) a system of record, so in the short-term (persisting
// mz_metrics) we are free to simply delete the data. In the medium-term (fast
// restarts for kafka sources), we'll make an effort to decode old data. In the
// long-term (1.0), we'll have backward-compatibility guarantees.
impl Codec for BlobMeta {
    fn codec_name() -> &'static str {
        "BlobMeta"
    }

    fn size_hint(&self) -> usize {
        0
    }

    fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
        buf.extend(&[BlobMeta::CURRENT_VERSION]);
        buf.extend(BlobMeta::MAGIC);
        unsafe { abomonation::encode(self, &mut ExtendWriteAdapter(buf)) }
            .expect("write to ExtendWriteAdapter is infallible");
        buf.extend(BlobMeta::MAGIC);
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let version_pos = 0..1;
        let begin_magic_pos = version_pos.end..version_pos.end + BlobMeta::MAGIC.len();
        // Avoid panicking when buf is short.
        let end_magic_start = if begin_magic_pos.end + BlobMeta::MAGIC.len() > buf.len() {
            begin_magic_pos.end
        } else {
            buf.len() - BlobMeta::MAGIC.len()
        };
        let end_magic_pos = end_magic_start..buf.len();
        match buf.get(version_pos.start) {
            Some(&BlobMeta::CURRENT_VERSION) => {}
            Some(x) => return Err(format!("unsupported version: {}", x)),
            None => return Err("unknown version".into()),
        }
        match buf.get(begin_magic_pos.clone()) {
            Some(BlobMeta::MAGIC) => {}
            _ => return Err("bad magic".into()),
        }
        match buf.get(end_magic_pos.clone()) {
            Some(BlobMeta::MAGIC) => {}
            _ => return Err("bad magic".into()),
        }
        let buf = &buf[begin_magic_pos.end..end_magic_pos.start];
        let meta: Abomonated<BlobMeta, Vec<u8>> =
            unsafe { Abomonated::new(buf.to_owned()) }.ok_or_else(|| "invalid meta")?;
        Ok((*meta).clone())
    }
}

impl BlobMeta {
    const MAGIC: &'static [u8] = b"mz";
    const CURRENT_VERSION: u8 = 0;

    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        for r in self.id_mapping.iter() {
            if r.id >= self.next_stream_id {
                return Err(format!(
                    "contained stream id {:?} >= next_stream_id: {:?}",
                    r.id, self.next_stream_id
                )
                .into());
            }
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
            if r.id >= self.next_stream_id {
                return Err(format!(
                    "graveyard contained stream id {:?} >= next_stream_id: {:?}",
                    r.id, self.next_stream_id
                )
                .into());
            }

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

        if u64::cast_from(deleted_ids.len() + ids.len()) != self.next_stream_id.0 {
            return Err(format!(
                "next stream {:?}, but only registered {} ids and deleted {} ids",
                self.next_stream_id,
                ids.len(),
                deleted_ids.len()
            )
            .into());
        }

        let mut unsealeds = HashMap::new();
        for f in self.unsealeds.iter() {
            if !ids.contains(&f.id) {
                return Err(format!("unsealeds id {:?} not present in id_mapping", f.id).into());
            }

            if unsealeds.contains_key(&f.id) {
                return Err(format!("duplicate unsealed: {:?}", f.id).into());
            }
            unsealeds.insert(f.id, f);

            f.validate()?;
        }

        let mut traces = HashMap::new();
        for t in self.traces.iter() {
            if !ids.contains(&t.id) {
                return Err(format!("traces id {:?} not present in id_mapping", t.id).into());
            }

            if traces.contains_key(&t.id) {
                return Err(format!("duplicate trace: {:?}", t.id).into());
            }
            traces.insert(t.id, t);

            t.validate()?;
        }

        for id in ids.iter() {
            let unsealed = unsealeds.get(id).ok_or_else(|| {
                Error::from(format!("id_mapping id {:?} not present in unsealeds", id))
            })?;
            let trace = traces.get(id).ok_or_else(|| {
                Error::from(format!("id_mapping id {:?} not present in traces", id))
            })?;
            let unsealed_seqno_upper = unsealed.seqno_upper();
            if !unsealed_seqno_upper.less_equal(&self.unsealeds_seqno_upper) {
                return Err(Error::from(format!(
                    "id {:?} unsealed seqno_upper {:?} is not less than the blob's unsealed_seqno_upper {:?}",
                    id, unsealed_seqno_upper, self.unsealeds_seqno_upper,
                )));
            }
            let trace_ts_upper = trace.ts_upper();
            if !PartialOrder::less_equal(&unsealed.ts_lower, &trace_ts_upper) {
                return Err(Error::from(format!(
                    "id {:?} trace ts_upper {:?} is not at or in advance of unsealed ts_lower {:?}",
                    id, trace_ts_upper, unsealed.ts_lower,
                )));
            }
        }
        Ok(())
    }
}

impl UnsealedMeta {
    /// Create a new [UnsealedMeta] belonging to `id`.
    pub fn new(id: Id) -> Self {
        UnsealedMeta {
            id,
            ts_lower: Antichain::from_elem(Timestamp::minimum()),
            batches: Vec::new(),
            next_blob_id: 0,
        }
    }
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let mut prev: Option<&UnsealedBatchMeta> = None;
        for meta in self.batches.iter() {
            meta.validate()?;
            if let Some(prev) = prev {
                if !PartialOrder::less_equal(prev.desc.upper(), meta.desc.lower()) {
                    return Err(format!(
                        "invalid batch sequence: {:?} followed by {:?}",
                        prev.desc, meta.desc
                    )
                    .into());
                }
            }
            prev = Some(&meta)
        }
        Ok(())
    }

    /// Returns an open upper bound on the seqnos contained in this unsealed.
    pub fn seqno_upper(&self) -> Antichain<SeqNo> {
        self.batches.last().map_or_else(
            || Antichain::from_elem(SeqNo(0)),
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
        if PartialOrder::less_equal(self.desc.upper(), &self.desc.lower()) {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }

        Ok(())
    }
}

impl TraceMeta {
    /// Create a new [TraceMeta] belonging to `id`.
    pub fn new(id: Id) -> Self {
        TraceMeta {
            id,
            batches: Vec::new(),
            since: Antichain::from_elem(Timestamp::minimum()),
            seal: Antichain::from_elem(Timestamp::minimum()),
            next_blob_id: 0,
        }
    }
    /// Returns an open upper bound on the timestamps of data contained in this
    /// trace.
    pub fn ts_upper(&self) -> Antichain<u64> {
        self.batches.last().map_or_else(
            || Antichain::from_elem(Timestamp::minimum()),
            |meta| meta.desc.upper().clone(),
        )
    }

    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let upper = self.ts_upper();
        let min = Antichain::from_elem(Timestamp::minimum());

        if self.since != min && !PartialOrder::less_than(&self.since, &self.seal) {
            return Err(format!(
                "invalid trace since {:?} at or in advance of trace seal {:?}",
                self.since, upper
            )
            .into());
        }

        let mut prev: Option<&TraceBatchMeta> = None;
        for meta in self.batches.iter() {
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

            if let Some(prev) = prev {
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
            prev = Some(&meta)
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
}

impl BlobUnsealedBatch {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if the equal case (an empty desc) is
        // useful/harmful. Feel free to make this a less_than if empty descs end
        // up making sense.
        if PartialOrder::less_equal(self.desc.upper(), &self.desc.lower()) {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }
        // TODO: It's unclear if this invariant is useful/harmful. Feel free to
        // remove it if it ends up not making sense.
        if self.updates.is_empty() {
            return Err("updates is empty".into());
        }

        let mut prev: Option<(&u64, PrettyBytes<'_>, PrettyBytes<'_>)> = None;
        for update in self.updates.iter() {
            let ((key, val), ts, diff) = update;
            // Check ordering.
            let this = (ts, PrettyBytes(key), PrettyBytes(val));
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
            if *diff == 0 {
                return Err(format!("update with 0 diff: {:?}", PrettyRecord(update)).into());
            }
        }

        Ok(())
    }
}

impl BlobTraceBatch {
    /// Asserts the documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if the equal case (an empty desc) is
        // useful/harmful. Feel free to make this a less_than if empty descs end
        // up making sense.
        if PartialOrder::less_equal(self.desc.upper(), &self.desc.lower()) {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }

        let mut prev: Option<(PrettyBytes<'_>, PrettyBytes<'_>, &u64)> = None;
        for update in self.updates.iter() {
            let ((key, val), ts, diff) = update;
            // Check ts against desc.
            if !self.desc.lower().less_equal(ts) {
                return Err(format!(
                    "timestamp {} is less than the batch lower: {:?}",
                    ts, self.desc
                )
                .into());
            }

            if PartialOrder::less_than(self.desc.since(), self.desc.upper()) {
                if self.desc.upper().less_equal(ts) {
                    return Err(format!(
                        "timestamp {} is greater than or equal to the batch upper: {:?}",
                        ts, self.desc
                    )
                    .into());
                }
            } else if self.desc.since().less_than(ts) {
                return Err(format!(
                    "timestamp {} is greater than the batch since: {:?}",
                    ts, self.desc,
                )
                .into());
            }

            // Check ordering.
            let this = (PrettyBytes(key), PrettyBytes(val), ts);
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
            if *diff == 0 {
                return Err(format!("update with 0 diff: {:?}", PrettyRecord(update)).into());
            }
        }
        Ok(())
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

struct PrettyRecord<'a>(&'a ((Vec<u8>, Vec<u8>), u64, isize));

impl fmt::Debug for PrettyRecord<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ((k, v), ts, diff) = &self.0;
        fmt::Debug::fmt(&((PrettyBytes(&k), PrettyBytes(&v)), ts, diff), f)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;

    use super::*;

    fn update_with_ts(ts: u64) -> ((Vec<u8>, Vec<u8>), u64, isize) {
        (("".into(), "".into()), ts, 1)
    }

    fn update_with_key(ts: u64, key: &'static str) -> ((Vec<u8>, Vec<u8>), u64, isize) {
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
            key: "".to_string(),
            desc: u64_desc(lower, upper),
            level: 1,
            size_bytes: 0,
        }
    }

    fn batch_meta_full(lower: u64, upper: u64, since: u64, level: u64) -> TraceBatchMeta {
        TraceBatchMeta {
            key: "".to_string(),
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

    fn seqno_desc(lower: u64, upper: u64) -> Description<SeqNo> {
        Description::new(
            Antichain::from_elem(SeqNo(lower)),
            Antichain::from_elem(SeqNo(upper)),
            Antichain::from_elem(SeqNo(0)),
        )
    }

    fn unsealed_batch_meta(lower: u64, upper: u64) -> UnsealedBatchMeta {
        UnsealedBatchMeta {
            key: "".to_string(),
            desc: seqno_desc(lower, upper),
            ts_upper: 0,
            ts_lower: 0,
            size_bytes: 0,
        }
    }

    impl From<(&'_ str, Id)> for StreamRegistration {
        fn from(x: (&'_ str, Id)) -> Self {
            let (name, id) = x;
            StreamRegistration {
                name: name.to_owned(),
                id: id,
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
            desc: seqno_desc(0, 2),
            updates: vec![update_with_ts(0), update_with_ts(1)],
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: BlobUnsealedBatch = BlobUnsealedBatch {
            desc: seqno_desc(0, 2),
            updates: vec![],
        };
        assert_eq!(b.validate(), Err("updates is empty".into()));

        // Invalid desc
        let b: BlobUnsealedBatch = BlobUnsealedBatch {
            desc: seqno_desc(2, 0),
            updates: vec![],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid desc: Description { lower: Antichain { elements: [SeqNo(2)] }, upper: Antichain { elements: [SeqNo(0)] }, since: Antichain { elements: [SeqNo(0)] } }"
            ))
        );

        // Empty desc
        let b: BlobUnsealedBatch = BlobUnsealedBatch {
            desc: seqno_desc(0, 0),
            updates: vec![],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid desc: Description { lower: Antichain { elements: [SeqNo(0)] }, upper: Antichain { elements: [SeqNo(0)] }, since: Antichain { elements: [SeqNo(0)] } }"
            ))
        );

        // Not sorted by time
        let b = BlobUnsealedBatch {
            desc: seqno_desc(0, 2),
            updates: vec![update_with_ts(1), update_with_ts(0)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "unsorted: (1, \"\", \"\") was before (0, \"\", \"\")"
            ))
        );

        // Not consolidated
        let b = BlobUnsealedBatch {
            desc: seqno_desc(0, 2),
            updates: vec![update_with_ts(0), update_with_ts(0)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("unconsolidated: (0, \"\", \"\")"))
        );

        // Invalid update
        let b: BlobUnsealedBatch = BlobUnsealedBatch {
            desc: seqno_desc(0, 1),
            updates: vec![(("0".into(), "0".into()), 0, 0)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("update with 0 diff: ((\"0\", \"0\"), 0, 0)"))
        );
    }

    #[test]
    fn trace_batch_validate() {
        // Normal case
        let b = BlobTraceBatch {
            desc: u64_desc(0, 2),
            updates: vec![update_with_key(0, "0"), update_with_key(1, "1")],
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: BlobTraceBatch = BlobTraceBatch {
            desc: u64_desc(0, 2),
            updates: vec![],
        };
        assert_eq!(b.validate(), Ok(()));

        // Invalid desc
        let b: BlobTraceBatch = BlobTraceBatch {
            desc: u64_desc(2, 0),
            updates: vec![],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid desc: Description { lower: Antichain { elements: [2] }, upper: Antichain { elements: [0] }, since: Antichain { elements: [0] } }"
            ))
        );

        // Empty desc
        let b: BlobTraceBatch = BlobTraceBatch {
            desc: u64_desc(0, 0),
            updates: vec![],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [0] }, since: Antichain { elements: [0] } }"
            ))
        );

        // Not sorted by key
        let b = BlobTraceBatch {
            desc: u64_desc(0, 2),
            updates: vec![update_with_key(0, "1"), update_with_key(1, "0")],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "unsorted: (\"1\", \"\", 0) was before (\"0\", \"\", 1)"
            ))
        );

        // Not consolidated
        let b = BlobTraceBatch {
            desc: u64_desc(0, 2),
            updates: vec![update_with_key(0, "0"), update_with_key(0, "0")],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("unconsolidated: (\"0\", \"\", 0)"))
        );

        // Update "before" desc
        let b = BlobTraceBatch {
            desc: u64_desc(1, 2),
            updates: vec![update_with_key(0, "0")],
        };
        assert_eq!(b.validate(), Err(Error::from("timestamp 0 is less than the batch lower: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }")));

        // Update "after" desc
        let b = BlobTraceBatch {
            desc: u64_desc(1, 2),
            updates: vec![update_with_key(2, "0")],
        };
        assert_eq!(b.validate(), Err(Error::from("timestamp 2 is greater than or equal to the batch upper: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }")));

        // Normal case: update "after" desc and within since
        let b = BlobTraceBatch {
            desc: u64_desc_since(1, 2, 4),
            updates: vec![update_with_key(2, "0")],
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case: update "after" desc and at since
        let b = BlobTraceBatch {
            desc: u64_desc_since(1, 2, 4),
            updates: vec![update_with_key(4, "0")],
        };
        assert_eq!(b.validate(), Ok(()));

        // Update "after" desc since
        let b = BlobTraceBatch {
            desc: u64_desc_since(1, 2, 4),
            updates: vec![update_with_key(5, "0")],
        };
        assert_eq!(b.validate(), Err(Error::from("timestamp 5 is greater than the batch since: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [4] } }")));

        // Invalid update
        let b: BlobTraceBatch = BlobTraceBatch {
            desc: u64_desc(0, 1),
            updates: vec![(("0".into(), "0".into()), 0, 0)],
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
    fn trace_meta_validate() {
        // Empty
        let b = TraceMeta {
            id: Id(0),
            batches: vec![],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(0),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case
        let b = TraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta(1, 2)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(2),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Gap
        let b = TraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta(2, 3)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(3),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch sequence: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [1] }, since: Antichain { elements: [0] } } followed by Description { lower: Antichain { elements: [2] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));

        // Overlapping
        let b = TraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 2), batch_meta(1, 3)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(3),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch sequence: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } } followed by Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));

        // Normal case: trace since before nonzero trace upper
        let b = TraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta(1, 2)],
            since: Antichain::from_elem(1),
            seal: Antichain::from_elem(2),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Trace since at nonzero trace seal
        let b = TraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 2), batch_meta(2, 3)],
            since: Antichain::from_elem(3),
            seal: Antichain::from_elem(3),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Err(Error::from("invalid trace since Antichain { elements: [3] } at or in advance of trace seal Antichain { elements: [3] }")));

        // Trace since in advance of nonzero trace seal
        let b = TraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 2), batch_meta(2, 3)],
            since: Antichain::from_elem(4),
            seal: Antichain::from_elem(3),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Err(Error::from("invalid trace since Antichain { elements: [4] } at or in advance of trace seal Antichain { elements: [3] }")));

        // Normal case: batch since at or before trace since
        let b = TraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta_full(1, 2, 1, 1)],
            since: Antichain::from_elem(1),
            seal: Antichain::from_elem(2),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Batch since in advance of trace since
        let b = TraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta_full(1, 2, 2, 1)],
            since: Antichain::from_elem(1),
            seal: Antichain::from_elem(2),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch since: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [2] } } in advance of trace since Antichain { elements: [1] }")));

        // Normal case: decreasing or constant compaction levels
        let b = TraceMeta {
            id: Id(0),
            batches: vec![
                batch_meta_full(0, 1, 0, 2),
                batch_meta_full(1, 2, 0, 2),
                batch_meta_full(2, 3, 0, 1),
            ],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(3),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Increasing compaction level.
        let b = TraceMeta {
            id: Id(0),
            batches: vec![batch_meta_full(0, 1, 0, 1), batch_meta_full(1, 2, 0, 2)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(2),
            next_blob_id: 0,
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
        assert_eq!(b.validate(), Err(Error::from("invalid desc: Description { lower: Antichain { elements: [SeqNo(0)] }, upper: Antichain { elements: [SeqNo(0)] }, since: Antichain { elements: [SeqNo(0)] } }")));

        // Invalid desc
        let b = unsealed_batch_meta(1, 0);
        assert_eq!(b.validate(), Err(Error::from("invalid desc: Description { lower: Antichain { elements: [SeqNo(1)] }, upper: Antichain { elements: [SeqNo(0)] }, since: Antichain { elements: [SeqNo(0)] } }")));
    }

    #[test]
    fn unsealed_meta_validate() {
        // Empty
        let b = UnsealedMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![],
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case
        let b = UnsealedMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![unsealed_batch_meta(0, 1), unsealed_batch_meta(1, 2)],
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case: gap between sequence number ranges.
        let b = UnsealedMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![unsealed_batch_meta(0, 1), unsealed_batch_meta(2, 3)],
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()),);

        // Overlapping
        let b = UnsealedMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![unsealed_batch_meta(0, 2), unsealed_batch_meta(1, 3)],
            next_blob_id: 0,
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid batch sequence: Description { lower: Antichain { elements: [SeqNo(0)] }, upper: Antichain { elements: [SeqNo(2)] }, since: Antichain { elements: [SeqNo(0)] } } followed by Description { lower: Antichain { elements: [SeqNo(1)] }, upper: Antichain { elements: [SeqNo(3)] }, since: Antichain { elements: [SeqNo(0)] } }"
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
            next_stream_id: Id(2),
            id_mapping: vec![("0", Id(0)).into(), ("1", Id(1)).into()],
            unsealeds: vec![UnsealedMeta::new(Id(0)), UnsealedMeta::new(Id(1))],
            traces: vec![TraceMeta::new(Id(0)), TraceMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Duplicate external stream id
        let b = BlobMeta {
            next_stream_id: Id(2),
            id_mapping: vec![("1", Id(0)).into(), ("1", Id(1)).into()],
            unsealeds: vec![UnsealedMeta::new(Id(0)), UnsealedMeta::new(Id(1))],
            traces: vec![TraceMeta::new(Id(0)), TraceMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate external stream name: 1"))
        );

        // Duplicate internal stream id
        let b = BlobMeta {
            next_stream_id: Id(2),
            id_mapping: vec![("0", Id(1)).into(), ("1", Id(1)).into()],
            unsealeds: vec![UnsealedMeta::new(Id(0)), UnsealedMeta::new(Id(1))],
            traces: vec![TraceMeta::new(Id(0)), TraceMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate internal stream id: Id(1)"))
        );

        // Invalid next_stream_id
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0", Id(0)).into(), ("1", Id(1)).into()],
            unsealeds: vec![UnsealedMeta::new(Id(0)), UnsealedMeta::new(Id(1))],
            traces: vec![TraceMeta::new(Id(0)), TraceMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "contained stream id Id(1) >= next_stream_id: Id(1)"
            ))
        );

        // Missing unsealed
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0", Id(0)).into()],
            unsealeds: vec![],
            traces: vec![TraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("id_mapping id Id(0) not present in unsealeds"))
        );

        // Missing trace
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0", Id(0)).into()],
            unsealeds: vec![UnsealedMeta::new(Id(0))],
            traces: vec![],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("id_mapping id Id(0) not present in traces"))
        );

        // Extra unsealed
        let b = BlobMeta {
            next_stream_id: Id(0),
            id_mapping: vec![],
            unsealeds: vec![UnsealedMeta::new(Id(0))],
            traces: vec![],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("unsealeds id Id(0) not present in id_mapping"))
        );

        // Extra trace
        let b = BlobMeta {
            next_stream_id: Id(0),
            id_mapping: vec![],
            unsealeds: vec![],
            traces: vec![TraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("traces id Id(0) not present in id_mapping"))
        );

        // Duplicate in unsealeds
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0", Id(0)).into()],
            unsealeds: vec![UnsealedMeta::new(Id(0)), UnsealedMeta::new(Id(0))],
            traces: vec![TraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(b.validate(), Err(Error::from("duplicate unsealed: Id(0)")));

        // Duplicate in traces
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0", Id(0)).into()],
            unsealeds: vec![UnsealedMeta::new(Id(0))],
            traces: vec![TraceMeta::new(Id(0)), TraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(b.validate(), Err(Error::from("duplicate trace: Id(0)")));

        // Normal case: unsealed ts_lower < ts_upper
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0", Id(0)).into()],
            unsealeds: vec![UnsealedMeta {
                id: Id(0),
                ts_lower: vec![0].into(),
                batches: vec![],
                next_blob_id: 0,
            }],
            traces: vec![TraceMeta {
                id: Id(0),
                batches: vec![batch_meta(0, 1)],
                since: Antichain::from_elem(0),
                seal: Antichain::from_elem(1),
                next_blob_id: 0,
            }],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()),);

        // Normal case: unsealed ts_lower at ts_upper
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0", Id(0)).into()],
            unsealeds: vec![UnsealedMeta {
                id: Id(0),
                ts_lower: vec![1].into(),
                batches: vec![],
                next_blob_id: 0,
            }],
            traces: vec![TraceMeta {
                id: Id(0),
                batches: vec![batch_meta(0, 1)],
                since: Antichain::from_elem(0),
                seal: Antichain::from_elem(1),
                next_blob_id: 0,
            }],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()),);

        // Unsealed ts_lower in advance of ts_upper
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0", Id(0)).into()],
            unsealeds: vec![UnsealedMeta {
                id: Id(0),
                ts_lower: vec![2].into(),
                batches: vec![],
                next_blob_id: 0,
            }],
            traces: vec![TraceMeta {
                id: Id(0),
                batches: vec![batch_meta(0, 1)],
                since: Antichain::from_elem(0),
                seal: Antichain::from_elem(1),
                next_blob_id: 0,
            }],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "id Id(0) trace ts_upper Antichain { elements: [1] } is not at or in advance of unsealed ts_lower Antichain { elements: [2] }"
            ))
        );

        // unsealed_seqno_upper less than one of the unsealed seqno uppers
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0", Id(0)).into()],
            unsealeds_seqno_upper: SeqNo(2),
            unsealeds: vec![UnsealedMeta {
                id: Id(0),
                batches: vec![unsealed_batch_meta(0, 3)],
                next_blob_id: 0,
                ts_lower: vec![0].into(),
            }],
            traces: vec![TraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "id Id(0) unsealed seqno_upper Antichain { elements: [SeqNo(3)] } is not less than the blob's unsealed_seqno_upper SeqNo(2)"
            ))
        );

        // Duplicate id in graveyard.
        let b = BlobMeta {
            next_stream_id: Id(1),
            graveyard: vec![("deleted", Id(0)).into(), ("1", Id(0)).into()],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate deleted internal stream id: Id(0)"))
        );

        // Duplicate stream name in graveyard.
        let b = BlobMeta {
            next_stream_id: Id(2),
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
            next_stream_id: Id(2),
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
            next_stream_id: Id(2),
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
            next_stream_id: Id(2),
            id_mapping: vec![("name", Id(0)).into()],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from(
                "next stream Id(2), but only registered 1 ids and deleted 0 ids"
            ))
        );
    }

    #[test]
    fn blob_meta_codec() {
        // Sanity check that encode/decode roundtrips and that we don't panic
        // (or erroneously succeed) on invalid data.
        let original = BlobMeta {
            next_stream_id: Id(1),
            unsealeds_seqno_upper: SeqNo(2),
            // This is not a test of abomonation's roundtrip-ability, so don't
            // bother too much with the test data.
            id_mapping: vec![],
            graveyard: vec![],
            unsealeds: vec![],
            traces: vec![],
        };
        let mut encoded = Vec::new();
        original.encode(&mut encoded);
        let decoded = BlobMeta::decode(&encoded);
        assert_eq!(decoded, Ok(original));
        for i in 0..encoded.len() {
            assert!(BlobMeta::decode(&encoded[0..i]).is_err());
        }
    }
}
