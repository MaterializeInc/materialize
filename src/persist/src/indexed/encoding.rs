// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data structures stored in Blobs and Buffers in serialized form.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;

use abomonation_derive::Abomonation;
use differential_dataflow::trace::Description;
use ore::cast::CastFrom;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::Error;
use crate::storage::SeqNo;

/// An internally unique id for a persisted stream. External users identify
/// streams with a string, which is then mapped internally to this.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Abomonation)]
pub struct Id(pub u64);

/// The structure serialized and stored as an entry in a
/// [crate::storage::Buffer].
///
/// Invariants:
/// - The updates field is non-empty.
#[derive(Clone, Debug, Abomonation)]
pub enum BufferEntry<K, V> {
    /// Pairs of stream id and the updates themselves.
    //
    // We could require that each Id is included at most once, but at the
    // moment, there's no particular reason we'd need to.
    Write(Vec<(Id, Vec<((K, V), u64, isize)>)>),
    /// A set of stream ids, and the timestamp those ids should be sealed up to.
    Seal(Vec<Id>, u64),
    /// Register a new stream.
    Register(Id, String),
    /// Destroy a stream.
    Destroy(Id, String),
    /// The timestamp this ID can be compacted up to.
    AllowCompaction(Id, u64),
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
/// - The same set of ids are present in id_mapping, futures, and traces.
/// - For each id, the ts_lower in the future is == the ts_upper in the
///   corresponding trace.
/// - id_mapping.len() + graveyard.len() is == next_stream_id.
#[derive(Clone, Debug, PartialEq, Abomonation)]
pub struct BlobMeta {
    /// The next internal stream id to assign.
    pub next_stream_id: Id,
    /// The position of buffer the last time data was step'd into futures.
    ///
    /// Invariant: For each BlobFutureMeta in `futures`, this is >= the last
    /// batch's upper. If they are not equal, there is logically an empty batch
    /// between [last batch's upper, futures_seqno_upper).
    pub futures_seqno_upper: SeqNo,
    /// Internal stream id indexed by external stream name.
    ///
    /// Invariant: Each stream name and stream id are in here at most once.
    pub id_mapping: Vec<(String, Id)>,
    /// Set of deleted streams, indexed by external stream name.
    pub graveyard: Vec<(String, Id)>,
    /// BlobFutures indexed by stream id.
    ///
    /// Invariant: Each stream id is in here at most once. This would be more
    /// naturally be modeled as a map from Id to BlobFutureMeta, but that
    /// doesn't work with Abomonation.
    pub futures: Vec<BlobFutureMeta>,
    /// BlobFutures indexed by stream id.
    ///
    /// Invariant: Each stream id is in here at most once. This would be more
    /// naturally be modeled as a map from Id to BlobTraceMeta, but that doesn't
    /// work with Abomonation.
    pub traces: Vec<BlobTraceMeta>,
}

/// The metadata necessary to reconstruct a BlobFuture.
///
/// Invariants:
/// - The batch SeqNo ranges are sorted and non-overlapping.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobFutureMeta {
    /// The stream this future belongs to.
    pub id: Id,
    /// A lower bound of data contained by this BlobFuture. Data before this may
    /// be present in the batches, but has been logically moved into the trace
    /// and should be ignored.
    pub ts_lower: Antichain<u64>,
    /// The batches that make up the BlobFuture.
    pub batches: Vec<BlobFutureBatchMeta>,
    /// The next id used to assign a Blob key for this future.
    pub next_blob_id: u64,
}

impl PartialEq for BlobFutureMeta {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
            && self.ts_lower[0].eq(&other.ts_lower[0])
            && self.batches.eq(&other.batches)
            && self.next_blob_id.eq(&other.next_blob_id)
    }
}

/// The metadata necessary to reconstruct a BlobFutureBatch.
///
/// Invariants:
/// - The [lower, upper) interval of sequence numbers in desc is non-empty.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobFutureBatchMeta {
    /// The key to retrieve the [BlobFutureBatch] from blob storage.
    pub key: String,
    /// Half-open interval [lower, upper) of sequence numbers that this batch
    /// contains updates for.
    pub desc: Description<SeqNo>,
    /// The maximum timestamp of any update contained in this batch.
    pub ts_upper: u64,
    /// The minimum timestamp from any update contained in this batch.
    pub ts_lower: u64,
}

impl PartialEq for BlobFutureBatchMeta {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
            && self.desc.upper()[0].eq(&other.desc.upper()[0])
            && self.desc.lower()[0].eq(&other.desc.lower()[0])
            && self.desc.since()[0].eq(&other.desc.since()[0])
            && self.ts_upper.eq(&other.ts_upper)
            && self.ts_lower.eq(&other.ts_lower)
    }
}

/// The metadata necessary to reconstruct a BlobTrace.
///
/// Invariants:
/// - The batch Descriptions are sorted, non-overlapping, and contiguous.
/// - Every batch's since frontier is <= the overall trace's since frontier.
/// - The compaction level of batches is weakly decreasing when iterating from oldest
///   to most recent time intervals.
/// - Every batch's upper is <= the overall trace's seal frontier.
/// - TODO: key uniqueness invariants?
#[derive(Clone, Debug, Abomonation)]
pub struct BlobTraceMeta {
    /// The stream this trace belongs to.
    pub id: Id,
    /// Metadata about he batches that make up the BlobTrace.
    pub batches: Vec<BlobTraceBatchMeta>,
    /// Compaction frontier for the batches contained in this trace.
    /// There may still be batches containing updates at times < since, but the
    /// the trace only contains correct answers for times at or in advance of this
    /// of this frontier. Readers are expected to advance any updates < since to
    /// since.
    pub since: Antichain<u64>,
    /// Frontier this trace has been sealed up to.
    pub seal: Antichain<u64>,
    /// The next id used to assign a Blob key for this trace.
    pub next_blob_id: u64,
}

impl PartialEq for BlobTraceMeta {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
            && self.since[0].eq(&other.since[0])
            && self.batches.eq(&other.batches)
            && self.next_blob_id.eq(&other.next_blob_id)
    }
}

/// The metadata necessary to reconstruct a [BlobTraceBatch].
///
/// Invariants:
/// - The Description's time interval is non-empty.
/// - TODO: key invariants?
#[derive(Clone, Debug, Abomonation)]
pub struct BlobTraceBatchMeta {
    /// The key to retrieve the batch's data from the blob store.
    pub key: String,
    /// The half-open time interval `[lower, upper)` this batch contains data
    /// for.
    pub desc: Description<u64>,
    /// The compaction level of each batch.
    pub level: u64,
}

impl PartialEq for BlobTraceBatchMeta {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
            && self.desc.upper()[0].eq(&other.desc.upper()[0])
            && self.desc.lower()[0].eq(&other.desc.lower()[0])
            && self.desc.since()[0].eq(&other.desc.since()[0])
            && self.level.eq(&other.level)
    }
}

/// The structure serialized and stored as a value in [crate::storage::Blob]
/// storage for data keys corresponding to future data.
///
/// Invariants:
/// - The [lower, upper) interval of sequence numbers in desc is non-empty.
/// - The values in updates are sorted by (time, key, value).
/// - The values in updates are "consolidated", i.e. (time, key, value) is
///   unique.
/// - All entries have a non-zero diff.
/// - The updates field is non-empty.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobFutureBatch<K, V> {
    /// Which updates are included in this batch.
    pub desc: Description<SeqNo>,
    /// The updates themselves.
    pub updates: Vec<((K, V), u64, isize)>,
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
#[derive(Clone, Debug, Abomonation)]
pub struct BlobTraceBatch<K, V> {
    /// Which updates are included in this batch.
    pub desc: Description<u64>,
    /// The updates themselves.
    pub updates: Vec<((K, V), u64, isize)>,
}

impl<K, V> BufferEntry<K, V> {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if this invariant is useful/harmful. Feel free to
        // remove it if it ends up not making sense.
        match self {
            BufferEntry::Write(updates) => {
                if updates.is_empty() {
                    return Err("updates is empty".into());
                }
            }
            // WIP: TODO: Invariants for the other commands
            _ => (),
        }
        Ok(())
    }
}

impl Default for BlobMeta {
    fn default() -> Self {
        BlobMeta {
            next_stream_id: Id(0),
            futures_seqno_upper: SeqNo(0),
            id_mapping: Vec::new(),
            graveyard: Vec::new(),
            futures: Vec::new(),
            traces: Vec::new(),
        }
    }
}

impl BlobMeta {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        for (name, id) in self.id_mapping.iter() {
            if id >= &self.next_stream_id {
                return Err(format!(
                    "contained stream id {:?} >= next_stream_id: {:?}",
                    id, self.next_stream_id
                )
                .into());
            }
            if names.contains(name) {
                return Err(format!("duplicate external stream name: {}", name).into());
            }
            names.insert(name.clone());
            if ids.contains(id) {
                return Err(format!("duplicate internal stream id: {:?}", id).into());
            }
            ids.insert(*id);
        }

        let mut deleted_ids = HashSet::new();
        let mut deleted_names = HashSet::new();

        for (name, id) in self.graveyard.iter() {
            if id >= &self.next_stream_id {
                return Err(format!(
                    "graveyard contained stream id {:?} >= next_stream_id: {:?}",
                    id, self.next_stream_id
                )
                .into());
            }

            if names.contains(name) {
                return Err(format!(
                    "duplicate external stream name {} across deleted and registered streams",
                    name
                )
                .into());
            }

            if ids.contains(id) {
                return Err(format!(
                    "duplicate internal stream id {:?} across deleted and registered streams",
                    id
                )
                .into());
            }

            if deleted_names.contains(name) {
                return Err(format!("duplicate deleted external stream name: {}", name).into());
            }
            deleted_names.insert(name.clone());

            if deleted_ids.contains(id) {
                return Err(format!("duplicate deleted internal stream id: {:?}", id).into());
            }
            deleted_ids.insert(*id);
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

        let mut futures = HashMap::new();
        for f in self.futures.iter() {
            if !ids.contains(&f.id) {
                return Err(format!("futures id {:?} not present in id_mapping", f.id).into());
            }

            if futures.contains_key(&f.id) {
                return Err(format!("duplicate future: {:?}", f.id).into());
            }
            futures.insert(f.id, f);

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
            let future = futures.get(id).ok_or_else(|| {
                Error::from(format!("id_mapping id {:?} not present in futures", id))
            })?;
            let trace = traces.get(id).ok_or_else(|| {
                Error::from(format!("id_mapping id {:?} not present in traces", id))
            })?;
            let future_seqno_upper = future.seqno_upper();
            if !future_seqno_upper.less_equal(&self.futures_seqno_upper) {
                return Err(Error::from(format!(
                    "id {:?} future seqno_upper {:?} is not less than the blob's future_seqno_upper {:?}",
                    id, future_seqno_upper, self.futures_seqno_upper,
                )));
            }
            let trace_ts_upper = trace.ts_upper();
            if trace_ts_upper != future.ts_lower {
                return Err(Error::from(format!(
                    "id {:?} trace ts_upper {:?} does not match future ts_lower {:?}",
                    id, trace_ts_upper, future.ts_lower,
                )));
            }
        }
        Ok(())
    }
}

impl BlobFutureMeta {
    /// Create a new [BlobFutureMeta] belonging to `id`.
    pub fn new(id: Id) -> Self {
        BlobFutureMeta {
            id,
            ts_lower: Antichain::from_elem(Timestamp::minimum()),
            batches: Vec::new(),
            next_blob_id: 0,
        }
    }
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let mut prev: Option<&BlobFutureBatchMeta> = None;
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

    /// Returns an open upper bound on the seqnos contained in this future.
    pub fn seqno_upper(&self) -> Antichain<SeqNo> {
        self.batches.last().map_or_else(
            || Antichain::from_elem(SeqNo(0)),
            |meta| meta.desc.upper().clone(),
        )
    }
}

impl BlobFutureBatchMeta {
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

impl BlobTraceMeta {
    /// Create a new [BlobTraceMeta] belonging to `id`.
    pub fn new(id: Id) -> Self {
        BlobTraceMeta {
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
        let mut prev: Option<&BlobTraceBatchMeta> = None;
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

impl BlobTraceBatchMeta {
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

impl<K, V> BlobFutureBatch<K, V>
where
    K: fmt::Debug + Ord,
    V: fmt::Debug + Ord,
{
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

        let mut prev: Option<(&u64, &K, &V)> = None;
        for update in self.updates.iter() {
            let ((key, val), ts, diff) = update;
            // Check ordering.
            let this = (ts, key, val);
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
                return Err(format!("update with 0 diff: {:?}", update).into());
            }
        }

        Ok(())
    }
}

impl<K, V> BlobTraceBatch<K, V>
where
    K: fmt::Debug + Ord,
    V: fmt::Debug + Ord,
{
    /// Asserts the documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        // TODO: It's unclear if the equal case (an empty desc) is
        // useful/harmful. Feel free to make this a less_than if empty descs end
        // up making sense.
        if PartialOrder::less_equal(self.desc.upper(), &self.desc.lower()) {
            return Err(format!("invalid desc: {:?}", &self.desc).into());
        }

        let mut prev: Option<(&K, &V, &u64)> = None;
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
            let this = (key, val, ts);
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
                return Err(format!("update with 0 diff: {:?}", update).into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;

    use super::*;

    fn update_with_ts(ts: u64) -> ((String, String), u64, isize) {
        (("".into(), "".into()), ts, 1)
    }

    fn update_with_key(ts: u64, key: &'static str) -> ((String, String), u64, isize) {
        ((key.into(), "".into()), ts, 1)
    }

    fn u64_desc(lower: u64, upper: u64) -> Description<u64> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(0),
        )
    }

    fn batch_meta(lower: u64, upper: u64) -> BlobTraceBatchMeta {
        BlobTraceBatchMeta {
            key: "".to_string(),
            desc: u64_desc(lower, upper),
            level: 1,
        }
    }

    fn batch_meta_full(lower: u64, upper: u64, since: u64, level: u64) -> BlobTraceBatchMeta {
        BlobTraceBatchMeta {
            key: "".to_string(),
            desc: u64_desc_since(lower, upper, since),
            level,
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

    fn future_batch_meta(lower: u64, upper: u64) -> BlobFutureBatchMeta {
        BlobFutureBatchMeta {
            key: "".to_string(),
            desc: seqno_desc(lower, upper),
            ts_upper: 0,
            ts_lower: 0,
        }
    }

    #[test]
    fn buffer_entry_validate() {
        // Normal case
        let b = BufferEntry::Write(vec![(Id(0), vec![update_with_key(0, "0")])]);
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: BufferEntry<String, String> = BufferEntry::Write(vec![]);
        assert_eq!(b.validate(), Err("updates is empty".into()));
    }

    #[test]
    fn future_batch_validate() {
        // Normal case
        let b = BlobFutureBatch {
            desc: seqno_desc(0, 2),
            updates: vec![update_with_ts(0), update_with_ts(1)],
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: BlobFutureBatch<String, String> = BlobFutureBatch {
            desc: seqno_desc(0, 2),
            updates: vec![],
        };
        assert_eq!(b.validate(), Err("updates is empty".into()));

        // Invalid desc
        let b: BlobFutureBatch<String, String> = BlobFutureBatch {
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
        let b: BlobFutureBatch<String, String> = BlobFutureBatch {
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
        let b = BlobFutureBatch {
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
        let b = BlobFutureBatch {
            desc: seqno_desc(0, 2),
            updates: vec![update_with_ts(0), update_with_ts(0)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("unconsolidated: (0, \"\", \"\")"))
        );

        // Invalid update
        let b: BlobFutureBatch<String, String> = BlobFutureBatch {
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
        let b: BlobTraceBatch<String, String> = BlobTraceBatch {
            desc: u64_desc(0, 2),
            updates: vec![],
        };
        assert_eq!(b.validate(), Ok(()));

        // Invalid desc
        let b: BlobTraceBatch<String, String> = BlobTraceBatch {
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
        let b: BlobTraceBatch<String, String> = BlobTraceBatch {
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
        let b: BlobTraceBatch<String, String> = BlobTraceBatch {
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
        let b = BlobTraceMeta {
            id: Id(0),
            batches: vec![],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(0),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case
        let b = BlobTraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta(1, 2)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(2),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Gap
        let b = BlobTraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta(2, 3)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(3),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch sequence: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [1] }, since: Antichain { elements: [0] } } followed by Description { lower: Antichain { elements: [2] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));

        // Overlapping
        let b = BlobTraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 2), batch_meta(1, 3)],
            since: Antichain::from_elem(0),
            seal: Antichain::from_elem(3),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch sequence: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } } followed by Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));

        // Normal case: trace since before nonzero trace upper
        let b = BlobTraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta(1, 2)],
            since: Antichain::from_elem(1),
            seal: Antichain::from_elem(2),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case: batch since at or before trace since
        let b = BlobTraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta_full(1, 2, 1, 1)],
            since: Antichain::from_elem(1),
            seal: Antichain::from_elem(2),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Batch since in advance of trace since
        let b = BlobTraceMeta {
            id: Id(0),
            batches: vec![batch_meta(0, 1), batch_meta_full(1, 2, 2, 1)],
            since: Antichain::from_elem(1),
            seal: Antichain::from_elem(2),
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch since: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [2] } } in advance of trace since Antichain { elements: [1] }")));

        // Normal case: decreasing or constant compaction levels
        let b = BlobTraceMeta {
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
        let b = BlobTraceMeta {
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
    fn future_batch_meta_validate() {
        // Normal case
        let b = future_batch_meta(0, 1);
        assert_eq!(b.validate(), Ok(()));

        // Empty interval
        let b = future_batch_meta(0, 0);
        assert_eq!(b.validate(), Err(Error::from("invalid desc: Description { lower: Antichain { elements: [SeqNo(0)] }, upper: Antichain { elements: [SeqNo(0)] }, since: Antichain { elements: [SeqNo(0)] } }")));

        // Invalid desc
        let b = future_batch_meta(1, 0);
        assert_eq!(b.validate(), Err(Error::from("invalid desc: Description { lower: Antichain { elements: [SeqNo(1)] }, upper: Antichain { elements: [SeqNo(0)] }, since: Antichain { elements: [SeqNo(0)] } }")));
    }

    #[test]
    fn future_meta_validate() {
        // Empty
        let b = BlobFutureMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![],
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case
        let b = BlobFutureMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![future_batch_meta(0, 1), future_batch_meta(1, 2)],
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case: gap between sequence number ranges.
        let b = BlobFutureMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![future_batch_meta(0, 1), future_batch_meta(2, 3)],
            next_blob_id: 0,
        };
        assert_eq!(b.validate(), Ok(()),);

        // Overlapping
        let b = BlobFutureMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![future_batch_meta(0, 2), future_batch_meta(1, 3)],
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
            id_mapping: vec![("0".into(), Id(0)), ("1".into(), Id(1))],
            futures: vec![BlobFutureMeta::new(Id(0)), BlobFutureMeta::new(Id(1))],
            traces: vec![BlobTraceMeta::new(Id(0)), BlobTraceMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Duplicate external stream id
        let b = BlobMeta {
            next_stream_id: Id(2),
            id_mapping: vec![("1".into(), Id(0)), ("1".into(), Id(1))],
            futures: vec![BlobFutureMeta::new(Id(0)), BlobFutureMeta::new(Id(1))],
            traces: vec![BlobTraceMeta::new(Id(0)), BlobTraceMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate external stream name: 1"))
        );

        // Duplicate internal stream id
        let b = BlobMeta {
            next_stream_id: Id(2),
            id_mapping: vec![("0".into(), Id(1)), ("1".into(), Id(1))],
            futures: vec![BlobFutureMeta::new(Id(0)), BlobFutureMeta::new(Id(1))],
            traces: vec![BlobTraceMeta::new(Id(0)), BlobTraceMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate internal stream id: Id(1)"))
        );

        // Invalid next_stream_id
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0)), ("1".into(), Id(1))],
            futures: vec![BlobFutureMeta::new(Id(0)), BlobFutureMeta::new(Id(1))],
            traces: vec![BlobTraceMeta::new(Id(0)), BlobTraceMeta::new(Id(1))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "contained stream id Id(1) >= next_stream_id: Id(1)"
            ))
        );

        // Missing future
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0))],
            futures: vec![],
            traces: vec![BlobTraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("id_mapping id Id(0) not present in futures"))
        );

        // Missing trace
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0))],
            futures: vec![BlobFutureMeta::new(Id(0))],
            traces: vec![],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("id_mapping id Id(0) not present in traces"))
        );

        // Extra future
        let b = BlobMeta {
            next_stream_id: Id(0),
            id_mapping: vec![],
            futures: vec![BlobFutureMeta::new(Id(0))],
            traces: vec![],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("futures id Id(0) not present in id_mapping"))
        );

        // Extra trace
        let b = BlobMeta {
            next_stream_id: Id(0),
            id_mapping: vec![],
            futures: vec![],
            traces: vec![BlobTraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("traces id Id(0) not present in id_mapping"))
        );

        // Duplicate in futures
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0))],
            futures: vec![BlobFutureMeta::new(Id(0)), BlobFutureMeta::new(Id(0))],
            traces: vec![BlobTraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(b.validate(), Err(Error::from("duplicate future: Id(0)")));

        // Duplicate in traces
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0))],
            futures: vec![BlobFutureMeta::new(Id(0))],
            traces: vec![BlobTraceMeta::new(Id(0)), BlobTraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(b.validate(), Err(Error::from("duplicate trace: Id(0)")));

        // Future ts_lower doesn't match trace ts_upper
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0))],
            futures: vec![BlobFutureMeta {
                id: Id(0),
                ts_lower: vec![2].into(),
                batches: vec![],
                next_blob_id: 0,
            }],
            traces: vec![BlobTraceMeta {
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
                "id Id(0) trace ts_upper Antichain { elements: [1] } does not match future ts_lower Antichain { elements: [2] }"
            ))
        );

        // future_seqno_upper less than one of the future seqno uppers
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0))],
            futures_seqno_upper: SeqNo(2),
            futures: vec![BlobFutureMeta {
                id: Id(0),
                batches: vec![future_batch_meta(0, 3)],
                next_blob_id: 0,
                ts_lower: vec![0].into(),
            }],
            traces: vec![BlobTraceMeta::new(Id(0))],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "id Id(0) future seqno_upper Antichain { elements: [SeqNo(3)] } is not less than the blob's future_seqno_upper SeqNo(2)"
            ))
        );

        // Duplicate id in graveyard.
        let b = BlobMeta {
            next_stream_id: Id(1),
            graveyard: vec![("deleted".into(), Id(0)), ("1".into(), Id(0))],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate deleted internal stream id: Id(0)"))
        );

        // Duplicate stream name in graveyard.
        let b = BlobMeta {
            next_stream_id: Id(2),
            graveyard: vec![("deleted".into(), Id(0)), ("deleted".into(), Id(1))],
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
            id_mapping: vec![("deleted".into(), Id(0))],
            graveyard: vec![("1".into(), Id(0))],
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
            id_mapping: vec![("name".into(), Id(1))],
            graveyard: vec![("name".into(), Id(0))],
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
            id_mapping: vec![("name".into(), Id(0))],
            ..Default::default()
        };

        assert_eq!(
            b.validate(),
            Err(Error::from(
                "next stream Id(2), but only registered 1 ids and deleted 0 ids"
            ))
        );
    }
}
