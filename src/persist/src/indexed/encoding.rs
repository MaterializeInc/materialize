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
use std::collections::HashSet;
use std::time::SystemTime;

use abomonation_derive::Abomonation;
use differential_dataflow::trace::Description;
use timely::progress::Antichain;
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
pub struct BufferEntry {
    /// Id of the stream this batch belongs to.
    pub id: Id,
    /// The updates themselves.
    pub updates: Vec<((String, String), u64, isize)>,
}

/// The structure serialized and stored as a value in [crate::storage::Blob]
/// storage for metadata keys.
///
/// TODO: Invariants
#[derive(Clone, Debug, Abomonation)]
pub struct BlobMeta {
    /// The most recently assigned key name.
    pub last_file_id: u128,
    /// The next internal stream id to assign.
    pub next_stream_id: Id,
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
/// - The seqno of each update is >= to desc.lower() and < desc.upper().
/// - The values in updates are sorted by (time, key, value).
/// - The values in updates are "consolidated", i.e. (time, key, value) is
///   unique.
/// - All entries have a non-zero diff.
/// - The updates field is non-empty.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobFutureBatch {
    /// Id of the stream this batch belongs to.
    pub id: Id,
    /// Which updates are included in this batch.
    pub desc: Description<SeqNo>,
    /// The updates themselves.
    pub updates: Vec<(SeqNo, (String, String), u64, isize)>,
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
pub struct BlobTraceBatch {
    /// Id of the trace this batch belongs to.
    pub id: Id,
    /// Which updates are included in this batch.
    pub desc: Description<u64>,
    /// The updates themselves.
    pub updates: Vec<((String, String), u64, isize)>,
}

impl BufferEntry {
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
            last_file_id: Self::now_millis(),
            next_stream_id: Id(0),
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
        for (_, f) in self.futures.iter() {
            f.validate()?;
        }
        for (_, t) in self.traces.iter() {
            t.validate()?;
        }
        // TODO: Assert that each stream id in futures is also present in traces
        // and vice-versa. Also validate that that ts_lower of each future lines
        // up wth the ts_upper of the corresponding trace.
        Ok(())
    }

    /// Returns the current SystemTime in millis since epoch.
    pub fn now_millis() -> u128 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    }
}

impl BlobFutureMeta {
    /// Asserts Self's documented invariants, returning an error if any are
    /// violated.
    pub fn validate(&self) -> Result<(), Error> {
        let mut prev: Option<&Description<SeqNo>> = None;
        for (desc, _) in self.batches.iter() {
            if let Some(prev) = prev {
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
}

impl BlobTraceMeta {
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

impl BlobFutureBatch {
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

        let mut prev: Option<(&u64, &String, &String)> = None;
        for update in self.updates.iter() {
            let (seqno, (key, val), ts, diff) = update;
            // Check seqno against desc.
            if !self.desc.lower().less_equal(seqno) {
                return Err(format!(
                    "seqno {:?} is less than the batch lower: {:?}",
                    seqno, self.desc
                )
                .into());
            }
            if self.desc.upper().less_equal(seqno) {
                return Err(format!(
                    "seqno {:?} is greater than or equal to the batch upper: {:?}",
                    seqno, self.desc
                )
                .into());
            }

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

        let mut prev: Option<(&String, &String, &u64)> = None;
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

    fn update_with_ts(seqno: u64, ts: u64) -> (SeqNo, (String, String), u64, isize) {
        (SeqNo(seqno), ("".into(), "".into()), ts, 1)
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
        let b = BufferEntry {
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
            updates: vec![update_with_ts(0, 0), update_with_ts(1, 1)],
        };
        assert_eq!(b.validate(), Ok(()));

        // Empty
        let b = BlobFutureBatch {
            id: Id(0),
            desc: seqno_desc(0, 2),
            updates: vec![],
        };
        assert_eq!(b.validate(), Err("updates is empty".into()));

        // Invalid desc
        let b = BlobFutureBatch {
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
        let b = BlobFutureBatch {
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
            updates: vec![update_with_ts(0, 1), update_with_ts(1, 0)],
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
            updates: vec![update_with_ts(0, 0), update_with_ts(1, 0)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("unconsolidated: (0, \"\", \"\")"))
        );

        // Update "before" desc
        let b = BlobFutureBatch {
            id: Id(0),
            desc: seqno_desc(1, 2),
            updates: vec![update_with_ts(0, 0)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "seqno SeqNo(0) is less than the batch lower: Description { lower: Antichain { elements: [SeqNo(1)] }, upper: Antichain { elements: [SeqNo(2)] }, since: Antichain { elements: [SeqNo(0)] } }"
            ))
        );

        // Update "after" desc
        let b = BlobFutureBatch {
            id: Id(0),
            desc: seqno_desc(0, 2),
            updates: vec![update_with_ts(2, 2)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "seqno SeqNo(2) is greater than or equal to the batch upper: Description { lower: Antichain { elements: [SeqNo(0)] }, upper: Antichain { elements: [SeqNo(2)] }, since: Antichain { elements: [SeqNo(0)] } }"
            ))
        );

        // Invalid update
        let b = BlobFutureBatch {
            id: Id(0),
            desc: seqno_desc(0, 1),
            updates: vec![(SeqNo(0), ("0".into(), "0".into()), 0, 0)],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "update with 0 diff: (SeqNo(0), (\"0\", \"0\"), 0, 0)"
            ))
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
        let b = BlobTraceBatch {
            id: Id(0),
            desc: u64_desc(0, 2),
            updates: vec![],
        };
        assert_eq!(b.validate(), Ok(()));

        // Invalid desc
        let b = BlobTraceBatch {
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
        let b = BlobTraceBatch {
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
        let b = BlobTraceBatch {
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
            last_file_id: 1,
            next_stream_id: Id(2),
            id_mapping: vec![("0".into(), Id(0)), ("1".into(), Id(1))],
            futures: vec![],
            traces: vec![],
        };
        assert_eq!(b.validate(), Ok(()));

        // Duplicate external stream id
        let b = BlobMeta {
            last_file_id: 1,
            next_stream_id: Id(2),
            id_mapping: vec![("1".into(), Id(0)), ("1".into(), Id(1))],
            futures: vec![],
            traces: vec![],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate external stream name: 1"))
        );

        // Duplicate internal stream id
        let b = BlobMeta {
            last_file_id: 1,
            next_stream_id: Id(2),
            id_mapping: vec![("0".into(), Id(1)), ("1".into(), Id(1))],
            futures: vec![],
            traces: vec![],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from("duplicate internal stream id: Id(1)"))
        );

        // Invalid next_stream_id
        let b = BlobMeta {
            last_file_id: 1,
            next_stream_id: Id(1),
            id_mapping: vec![("0".into(), Id(0)), ("1".into(), Id(1))],
            futures: vec![],
            traces: vec![],
        };
        assert_eq!(
            b.validate(),
            Err(Error::from(
                "contained stream id Id(1) >= next_stream_id: Id(1)"
            ))
        );
    }
}
