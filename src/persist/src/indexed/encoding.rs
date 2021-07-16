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
#[derive(Debug, Abomonation)]
pub struct BufferEntry<K, V> {
    /// Id of the stream this batch belongs to.
    pub id: Id,
    /// The updates themselves.
    pub updates: Vec<((K, V), u64, isize)>,
}

/// The structure serialized and stored as a value in [crate::storage::Blob]
/// storage for metadata keys.
///
/// Invariants:
/// - All strings in id_mapping are unique.
/// - All ids in id_mapping are unique.
/// - The same set of ids are present in id_mapping, futures, and traces.
/// - For each id, the ts_lower in the future is == the ts_upper in the
///   corresponding trace.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobMeta {
    /// The most recently assigned key name.
    pub last_file_id: u64,
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
    /// BlobFutures indexed by stream id.
    ///
    /// Invariant: Each stream id is in here at most once. This would be more
    /// naturally be modeled as a map from Id to BlobFutureMeta, but that
    /// doesn't work with Abomonation.
    pub futures: Vec<(Id, BlobFutureMeta)>,
    /// BlobFutures indexed by stream id.
    ///
    /// Invariant: Each stream id is in here at most once. This would be more
    /// naturally be modeled as a map from Id to BlobTraceMeta, but that doesn't
    /// work with Abomonation.
    pub traces: Vec<(Id, BlobTraceMeta)>,
}

/// The metadata necessary to reconstruct a BlobFuture.
///
/// Invariants:
/// - The batch SeqNo ranges are sorted, non-overlapping, and contiguous.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobFutureMeta {
    /// A lower bound of data contained by this BlobFuture. Data before this may
    /// be present in the batches, but has been logically moved into the trace
    /// and should be ignored.
    pub ts_lower: Antichain<u64>,
    /// The batches that make up the BlobFuture, represented by their
    /// description and the key to retrieve the batch's data from the blob
    /// store. Note that Descriptions are half-open intervals `[lower, upper)`.
    pub batches: Vec<(Description<SeqNo>, String)>,
}

/// The metadata necessary to reconstruct a BlobTrace.
///
/// Invariants:
/// - The batch Descriptions are sorted, non-overlapping, and contiguous.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobTraceMeta {
    /// The batches that make up the BlobTrace, represented by their description
    /// and the key to retrieve the batch's data from the blob store. Note that
    /// Descriptions are half-open intervals `[lower, upper)`.
    pub batches: Vec<(Description<u64>, String)>,
}

/// The structure serialized and stored as a value in [crate::storage::Blob]
/// storage for data keys corresponding to future data.
///
/// Invariants:
/// - The values in updates are sorted by (time, key, value).
/// - The values in updates are "consolidated", i.e. (time, key, value) is
///   unique.
/// - All entries have a non-zero diff.
/// - The updates field is non-empty.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobFutureBatch<K, V> {
    /// Id of the stream this batch belongs to.
    pub id: Id,
    /// Which updates are included in this batch.
    pub desc: Description<SeqNo>,
    /// The updates themselves.
    pub updates: Vec<((K, V), u64, isize)>,
}

/// The structure serialized and stored as a value in [crate::storage::Blob]
/// storage for data keys corresponding to trace data.
///
/// Invariants:
/// - The timestamp of each update is >= to desc.lower() and < desc.upper().
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
    /// Id of the trace this batch belongs to.
    pub id: Id,
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
        if self.updates.is_empty() {
            return Err("updates is empty".into());
        }
        Ok(())
    }
}

impl Default for BlobMeta {
    fn default() -> Self {
        BlobMeta {
            last_file_id: 0,
            next_stream_id: Id(0),
            futures_seqno_upper: SeqNo(0),
            id_mapping: Vec::new(),
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

        let mut futures = HashMap::new();
        for (id, f) in self.futures.iter() {
            if !ids.contains(id) {
                return Err(format!("futures id {:?} not present in id_mapping", id).into());
            }

            if futures.contains_key(id) {
                return Err(format!("duplicate future: {:?}", id).into());
            }
            futures.insert(*id, f);

            f.validate()?;
        }

        let mut traces = HashMap::new();
        for (id, t) in self.traces.iter() {
            if !ids.contains(id) {
                return Err(format!("traces id {:?} not present in id_mapping", id).into());
            }

            if traces.contains_key(id) {
                return Err(format!("duplicate trace: {:?}", id).into());
            }
            traces.insert(*id, t);

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

impl Default for BlobFutureMeta {
    fn default() -> Self {
        BlobFutureMeta {
            ts_lower: Antichain::from_elem(Timestamp::minimum()),
            batches: Vec::new(),
        }
    }
}

impl BlobFutureMeta {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let mut prev: Option<&Description<SeqNo>> = None;
        for (desc, _) in self.batches.iter() {
            if let Some(prev) = prev {
                // TODO: It's definitely useful in Trace for us to enforce that
                // these line up, but is it useful in Future? Maybe not. It's
                // also harder since SeqNos are multiplexed for all streams, but
                // traces are sealed per-stream.
                if prev.upper() != desc.lower() {
                    return Err(format!(
                        "invalid batch sequence: {:?} followed by {:?}",
                        prev, desc
                    )
                    .into());
                }
            }
            prev = Some(desc)
        }
        Ok(())
    }

    /// Returns an open upper bound on the seqnos contained in this future.
    pub fn seqno_upper(&self) -> Antichain<SeqNo> {
        self.batches.last().map_or_else(
            || Antichain::from_elem(SeqNo(0)),
            |(d, _)| d.upper().clone(),
        )
    }
}

impl Default for BlobTraceMeta {
    fn default() -> Self {
        BlobTraceMeta {
            batches: Vec::new(),
        }
    }
}

impl BlobTraceMeta {
    /// Returns an open upper bound on the timestamps of data contained in this
    /// trace.
    pub fn ts_upper(&self) -> Antichain<u64> {
        self.batches.last().map_or_else(
            || Antichain::from_elem(Timestamp::minimum()),
            |(d, _)| d.upper().clone(),
        )
    }

    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let mut prev: Option<&Description<u64>> = None;
        for (desc, _) in self.batches.iter() {
            if let Some(prev) = prev {
                if prev.upper() != desc.lower() {
                    return Err(format!(
                        "invalid batch sequence: {:?} followed by {:?}",
                        prev, desc,
                    )
                    .into());
                }
            }
            prev = Some(desc)
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
            if self.desc.upper().less_equal(ts) {
                return Err(format!(
                    "timestamp {} is greater than or equal to the batch upper: {:?}",
                    ts, self.desc
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

    fn seqno_desc(lower: u64, upper: u64) -> Description<SeqNo> {
        Description::new(
            Antichain::from_elem(SeqNo(lower)),
            Antichain::from_elem(SeqNo(upper)),
            Antichain::from_elem(SeqNo(0)),
        )
    }

    #[test]
    fn buffer_entry_validate() {
        // Normal case
        let b = BufferEntry {
            id: Id(0),
            updates: vec![update_with_key(0, "0")],
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: BufferEntry<String, String> = BufferEntry {
            id: Id(0),
            updates: vec![],
        };
        assert_eq!(b.validate(), Err("updates is empty".into()));
    }

    #[test]
    fn future_batch_validate() {
        // Normal case
        let b = BlobFutureBatch {
            id: Id(0),
            desc: seqno_desc(0, 2),
            updates: vec![update_with_ts(0), update_with_ts(1)],
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: BlobFutureBatch<String, String> = BlobFutureBatch {
            id: Id(0),
            desc: seqno_desc(0, 2),
            updates: vec![],
        };
        assert_eq!(b.validate(), Err("updates is empty".into()));

        // Invalid desc
        let b: BlobFutureBatch<String, String> = BlobFutureBatch {
            id: Id(0),
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
            id: Id(0),
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
            id: Id(0),
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
            id: Id(0),
            desc: seqno_desc(0, 2),
            updates: vec![update_with_ts(0), update_with_ts(0)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("unconsolidated: (0, \"\", \"\")"))
        );

        // Invalid update
        let b: BlobFutureBatch<String, String> = BlobFutureBatch {
            id: Id(0),
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
            id: Id(0),
            desc: u64_desc(0, 2),
            updates: vec![update_with_key(0, "0"), update_with_key(1, "1")],
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b: BlobTraceBatch<String, String> = BlobTraceBatch {
            id: Id(0),
            desc: u64_desc(0, 2),
            updates: vec![],
        };
        assert_eq!(b.validate(), Ok(()));

        // Invalid desc
        let b: BlobTraceBatch<String, String> = BlobTraceBatch {
            id: Id(0),
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
            id: Id(0),
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
            id: Id(0),
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
            id: Id(0),
            desc: u64_desc(0, 2),
            updates: vec![update_with_key(0, "0"), update_with_key(0, "0")],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("unconsolidated: (\"0\", \"\", 0)"))
        );

        // Update "before" desc
        let b = BlobTraceBatch {
            id: Id(0),
            desc: u64_desc(1, 2),
            updates: vec![update_with_key(0, "0")],
        };
        assert_eq!(b.validate(), Err(Error::from("timestamp 0 is less than the batch lower: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }")));

        // Update "after" desc
        let b = BlobTraceBatch {
            id: Id(0),
            desc: u64_desc(1, 2),
            updates: vec![update_with_key(2, "0")],
        };
        assert_eq!(b.validate(), Err(Error::from("timestamp 2 is greater than or equal to the batch upper: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }")));

        // Invalid update
        let b: BlobTraceBatch<String, String> = BlobTraceBatch {
            id: Id(0),
            desc: u64_desc(0, 1),
            updates: vec![(("0".into(), "0".into()), 0, 0)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("update with 0 diff: ((\"0\", \"0\"), 0, 0)"))
        );
    }

    #[test]
    fn trace_meta_validate() {
        // Empty
        let b = BlobTraceMeta { batches: vec![] };
        assert_eq!(b.validate(), Ok(()));

        // Normal case
        let b = BlobTraceMeta {
            batches: vec![(u64_desc(0, 1), "".into()), (u64_desc(1, 2), "".into())],
        };
        assert_eq!(b.validate(), Ok(()));

        // Gap
        let b = BlobTraceMeta {
            batches: vec![(u64_desc(0, 1), "".into()), (u64_desc(2, 3), "".into())],
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch sequence: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [1] }, since: Antichain { elements: [0] } } followed by Description { lower: Antichain { elements: [2] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));

        // Overlapping
        let b = BlobTraceMeta {
            batches: vec![(u64_desc(0, 2), "".into()), (u64_desc(1, 3), "".into())],
        };
        assert_eq!(b.validate(), Err(Error::from("invalid batch sequence: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } } followed by Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }")));
    }

    #[test]
    fn future_meta_validate() {
        // Empty
        let b = BlobFutureMeta {
            ts_lower: Antichain::from_elem(0),
            batches: vec![],
        };
        assert_eq!(b.validate(), Ok(()));

        // Normal case
        let b = BlobFutureMeta {
            ts_lower: Antichain::from_elem(0),
            batches: vec![(seqno_desc(0, 1), "".into()), (seqno_desc(1, 2), "".into())],
        };
        assert_eq!(b.validate(), Ok(()));

        // Gap
        let b = BlobFutureMeta {
            ts_lower: Antichain::from_elem(0),
            batches: vec![(seqno_desc(0, 1), "".into()), (seqno_desc(2, 3), "".into())],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "invalid batch sequence: Description { lower: Antichain { elements: [SeqNo(0)] }, upper: Antichain { elements: [SeqNo(1)] }, since: Antichain { elements: [SeqNo(0)] } } followed by Description { lower: Antichain { elements: [SeqNo(2)] }, upper: Antichain { elements: [SeqNo(3)] }, since: Antichain { elements: [SeqNo(0)] } }"
            ))
        );

        // Overlapping
        let b = BlobFutureMeta {
            ts_lower: Antichain::from_elem(0),
            batches: vec![(seqno_desc(0, 2), "".into()), (seqno_desc(1, 3), "".into())],
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
            futures: vec![(Id(0), Default::default()), (Id(1), Default::default())],
            traces: vec![(Id(0), Default::default()), (Id(1), Default::default())],
            ..Default::default()
        };
        assert_eq!(b.validate(), Ok(()));

        // Duplicate external stream id
        let b = BlobMeta {
            next_stream_id: Id(2),
            id_mapping: vec![("1".into(), Id(0)), ("1".into(), Id(1))],
            futures: vec![(Id(0), Default::default()), (Id(1), Default::default())],
            traces: vec![(Id(0), Default::default()), (Id(1), Default::default())],
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
            futures: vec![(Id(0), Default::default()), (Id(1), Default::default())],
            traces: vec![(Id(0), Default::default()), (Id(1), Default::default())],
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
            futures: vec![(Id(0), Default::default()), (Id(1), Default::default())],
            traces: vec![(Id(0), Default::default()), (Id(1), Default::default())],
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
            traces: vec![(Id(0), Default::default())],
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
            futures: vec![(Id(0), Default::default())],
            traces: vec![],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("id_mapping id Id(0) not present in traces"))
        );

        // Extra future
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![],
            futures: vec![(Id(0), Default::default())],
            traces: vec![],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("futures id Id(0) not present in id_mapping"))
        );

        // Extra trace
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![],
            futures: vec![],
            traces: vec![(Id(0), Default::default())],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("traces id Id(0) not present in id_mapping"))
        );

        // Duplicate in futures
        let b = BlobMeta {
            last_file_id: 1,
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0))],
            futures: vec![(Id(0), Default::default()), (Id(0), Default::default())],
            traces: vec![(Id(0), Default::default())],
            ..Default::default()
        };
        assert_eq!(b.validate(), Err(Error::from("duplicate future: Id(0)")));

        // Duplicate in traces
        let b = BlobMeta {
            last_file_id: 1,
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0))],
            futures: vec![(Id(0), Default::default())],
            traces: vec![(Id(0), Default::default()), (Id(0), Default::default())],
            ..Default::default()
        };
        assert_eq!(b.validate(), Err(Error::from("duplicate trace: Id(0)")));

        // Future ts_lower doesn't match trace ts_upper
        let b = BlobMeta {
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0))],
            futures: vec![(
                Id(0),
                BlobFutureMeta {
                    ts_lower: vec![2].into(),
                    batches: vec![],
                },
            )],
            traces: vec![(
                Id(0),
                BlobTraceMeta {
                    batches: vec![(u64_desc(0, 1), "".into())],
                },
            )],
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
            futures: vec![(
                Id(0),
                BlobFutureMeta {
                    batches: vec![(seqno_desc(0, 3), "".into())],
                    ..Default::default()
                },
            )],
            traces: vec![(Id(0), Default::default())],
            ..Default::default()
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "id Id(0) future seqno_upper Antichain { elements: [SeqNo(3)] } is not less than the blob's future_seqno_upper SeqNo(2)"
            ))
        );
    }
}
