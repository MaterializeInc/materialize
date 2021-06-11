// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent, compacting data structure of `(Key, Value, Time, Diff)`
//! updates, indexed by time.

use std::sync::Arc;

use differential_dataflow::trace::Description;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::BlobFutureMeta;
use crate::indexed::BlobFutureBatch;
use crate::persister::Snapshot;
use crate::storage::{Blob, SeqNo};

/// A persistent, compacting data structure containing `(Key, Value, Time,
/// Diff)` entries indexed by `(time, key, value)`.
///
/// Invariants:
/// - All entries are after some time frontier and less than some SeqNo.
///   Intuitively, this is the data that has been drained out of a
///   [crate::storage::Buffer] but not yet "seal"ed into a
///   [crate::indexed::trace::BlobTrace].
/// - TODO: Space usage.
pub struct BlobFuture {
    ts_lower: Antichain<u64>,
    // NB: The SeqNo ranges here are sorted and contiguous half-open intervals
    // `[lower, upper)`.
    batches: Vec<(Description<SeqNo>, String)>,
}

impl Default for BlobFuture {
    fn default() -> Self {
        BlobFuture::new(BlobFutureMeta {
            ts_lower: Antichain::from_elem(Timestamp::minimum()),
            batches: Vec::new(),
        })
    }
}

impl BlobFuture {
    /// Returns a BlobFuture re-instantiated with the previously serialized
    /// state.
    pub fn new(meta: BlobFutureMeta) -> Self {
        BlobFuture {
            ts_lower: meta.ts_lower,
            batches: meta.batches,
        }
    }

    /// Serializes the state of this BlobFuture for later re-instantiation.
    pub fn meta(&self) -> BlobFutureMeta {
        BlobFutureMeta {
            ts_lower: self.ts_lower.clone(),
            batches: self.batches.clone(),
        }
    }

    /// An open upper bound on the seqnos of contained updates.
    pub fn seqno_upper(&self) -> Antichain<SeqNo> {
        self.batches.last().map_or_else(
            || Antichain::from_elem(SeqNo(0)),
            |(desc, _)| desc.upper().clone(),
        )
    }

    /// Writes the given batch to [Blob] storage at the given key and logically
    /// adds the contained updates to this future.
    pub fn append<L: Blob>(
        &mut self,
        key: String,
        batch: BlobFutureBatch,
        blob: &mut BlobCache<L>,
    ) -> Result<(), Error> {
        if batch.desc.lower() != &self.seqno_upper() {
            return Err(Error::from(format!(
                "batch lower doesn't match seqno_upper {:?}: {:?}",
                self.seqno_upper(),
                batch.desc
            )));
        }
        // TODO: Assert that nothing in the batch is before self.ts_lower. That
        // would indicate a logic error by the user of this BlobFuture.

        // TODO: Sort the updates in the batch by `(time, key, value)` (or
        // ensure that they're sorted, if it turns out this work should have
        // happened somewhere else).
        let desc = batch.desc.clone();
        blob.set_future_batch(key.clone(), batch)?;
        self.batches.push((desc, key));
        Ok(())
    }

    /// Returns a consistent read of the updates contained in this future
    /// matching the given filters (in practice, everything not in Trace).
    pub fn snapshot<L: Blob>(
        &self,
        ts_lower: Antichain<u64>,
        ts_upper: Option<Antichain<u64>>,
        blob: &BlobCache<L>,
    ) -> Result<FutureSnapshot, Error> {
        let mut updates = Vec::with_capacity(self.batches.len());
        for (_, key) in self.batches.iter() {
            updates.push(blob.get_future_batch(key)?);
        }
        Ok(FutureSnapshot {
            seqno_upper: self.seqno_upper(),
            ts_lower,
            ts_upper,
            updates,
        })
    }

    /// Removes all updates contained in this future before the given bound.
    pub fn truncate(&mut self, new_ts_lower: Antichain<u64>) -> Result<(), Error> {
        // TODO: Validate that this doesn't regress ts_lower.
        self.ts_lower = new_ts_lower;
        // TODO: Actually delete the data.
        Ok(())
    }
}

/// A consistent snapshot of the data currently in a persistent [BlobFuture].
pub struct FutureSnapshot {
    /// An open upper bound on the seqnos of contained updates.
    pub seqno_upper: Antichain<SeqNo>,
    /// A closed lower bound on the times of contained updates.
    pub ts_lower: Antichain<u64>,
    /// An optional open upper bound on the times of the contained updates.
    pub ts_upper: Option<Antichain<u64>>,
    updates: Vec<Arc<BlobFutureBatch>>,
}

impl Snapshot for FutureSnapshot {
    fn read<E: Extend<((String, String), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        if let Some(batch) = self.updates.pop() {
            let updates = batch
                .updates
                .iter()
                .filter(|(seqno, _, ts, _)| {
                    !self.seqno_upper.less_equal(seqno)
                        && self.ts_lower.less_equal(ts)
                        && self.ts_upper.as_ref().map_or(true, |u| !u.less_equal(ts))
                })
                .map(|(_, (key, val), ts, diff)| ((key.clone(), val.clone()), *ts, *diff));
            buf.extend(updates);
            return true;
        }
        false
    }
}
