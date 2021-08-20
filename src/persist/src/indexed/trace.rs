// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent, compacting data structure of `(Key, Value, Time, Diff)`
//! updates, indexed by key.
//!
//! This is directly a persistent analog of [differential_dataflow::trace::Trace].

use std::sync::Arc;

use differential_dataflow::trace::Description;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::Error;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{TraceBatchMeta, TraceMeta};
use crate::indexed::{BlobTraceBatch, Id, Snapshot};
use crate::storage::Blob;

/// A persistent, compacting data structure containing `(Key, Value, Time,
/// Diff)` entries indexed by `(key, value, time)`.
///
/// We maintain an append-only list of immutable batches that describe updates
/// corresponding to sorted, contiguous, non-overlapping ranges of times. The
/// `since` frontier defines a time before which we can compact the history of
/// updates (and correspondingly no longer answer queries about).
///
/// We can compact the updates prior to the since frontier physically, by combining
/// batches representing consecutive intervals into one large batch representing the
/// union of those intervals, and logically, by forwarding updates at times before
/// the since frontier to the since frontier.
///
/// We also want to achieve a balance between the compactness of the representation
/// with the computational effort required to maintain the representation. Specifically,
/// if we have N batches of data already compacted, we don't want every additional
/// batch to perform O(N) work (i.e. merge with N batches worth of data) in order
/// to get compacted. Instead, we would like to keep a geometrically decreasing
/// (when viewed from oldest to most recent) sequence of batches and perform O(N)
/// work every N calls to append.
/// Thankfully, we can achieve all of this with a few simple rules:
///  - Batches are able to be compacted once the since frontier is in advance of all
///    of the data in the batch.
///  - All batches are assigned a nonzero compaction level. When a new batch is appended
///    to the trace it is assigned a compaction level of 0.
///  - We periodically merge consecutive batches at the same level L representing
///    time intervals [lo, mid) and [mid, hi) into a single batch representing
///    all of the updates in [lo, hi) with level L + 1. Once two batches are merged
///    they are removed from the trace and replaced with the merged batch.
///  - Perform merges for the oldest batches possible first.
///
/// NB: this approach assumes that all batches are roughly uniformly sized when they
/// are first appended.
///
/// Invariants:
/// - All entries are before some time frontier.
/// - Batches are sorted by time and represent a sorted, consecutive, non-overlapping
///   list of time intervals.
/// - Individual batches are immutable, and their set of updates, the time interval
///   they describe and their compaction level all remain constant as long as the batch
///   remains in the trace.
/// - The compaction levels across the list of batches in a trace are weakly decreasing
///   (non-increasing) when iterating from oldest to most recent time intervals.
/// - TODO: Space usage.
pub struct Trace {
    id: Id,
    /// The next ID used to assign a Blob key for this trace.
    pub next_blob_id: u64,
    // NB: We may at some point need to break this up into separate logical and
    // physical compaction frontiers.
    since: Antichain<u64>,
    // NB: The Descriptions here are sorted and contiguous half-open intervals
    // `[lower, upper)`.
    // The frontier the trace has been sealed up to.
    seal: Antichain<u64>,
    batches: Vec<TraceBatchMeta>,
}

impl Trace {
    /// Returns a Trace re-instantiated with the previously serialized
    /// state.
    pub fn new(meta: TraceMeta) -> Self {
        Trace {
            id: meta.id,
            next_blob_id: meta.next_blob_id,
            since: meta.since,
            seal: meta.seal,
            batches: meta.batches,
        }
    }

    fn new_blob_key(&mut self) -> String {
        let key = format!("{:?}-trace-{:?}", self.id, self.next_blob_id);
        self.next_blob_id += 1;

        key
    }

    /// Serializes the state of this Trace for later re-instantiation.
    pub fn meta(&self) -> TraceMeta {
        TraceMeta {
            id: self.id,
            since: self.since.clone(),
            seal: self.seal.clone(),
            batches: self.batches.clone(),
            next_blob_id: self.next_blob_id,
        }
    }

    /// An upper bound on the times of contained updates in the seal.
    ///
    /// While `self.seal` tracks the frontier of times that have been logically been
    /// closed and are eligible to be moved into the trace, `self.ts_upper()` tracks
    /// the frontier of times that have actually been physically moved into the trace.
    /// `self.seal()` is required to manage invariants between commands (e.g. a seal request
    /// has to be at a time in advance of prior seal requests) whereas `self.ts_upper()`
    /// is required to manage physical reads and writes to the trace (e.g. to determine
    /// which times may be added that are not already present.
    /// Invariant:
    /// - self.ts_upper() <= self.seal()
    pub fn ts_upper(&self) -> Antichain<u64> {
        match self.batches.last() {
            Some(meta) => meta.desc.upper().clone(),
            None => Antichain::from_elem(Timestamp::minimum()),
        }
    }

    /// A logical upper bound on the times which may currently be added to the
    /// trace.
    pub fn get_seal(&self) -> Antichain<u64> {
        self.seal.clone()
    }

    /// Update the seal frontier to `ts`.
    ///
    /// This function intentionally does not do any checking to see if ts is
    /// in advance of the current seal frontier, because we sometimes need to
    /// use this to revert a seal update in the event of a storage failure.
    pub fn update_seal(&mut self, ts: u64) {
        let seal = Antichain::from_elem(ts);
        self.seal = seal;
    }

    /// Checks whether the given seal would be valid to pass to
    /// [Trace::update_seal].
    pub fn validate_seal(&self, ts: u64) -> Result<(), Error> {
        let prev = self.get_seal();
        if !prev.less_than(&ts) {
            return Err(Error::from(format!(
                "invalid seal for {:?}: {:?} not in advance of current seal frontier {:?}",
                self.id, ts, prev
            )));
        }
        Ok(())
    }

    /// A lower bound on the time at which updates may have been logically
    /// compacted together.
    pub fn since(&self) -> Antichain<u64> {
        self.since.clone()
    }

    /// Checks whether the given since would be valid to pass to
    /// [Trace::allow_compaction].
    pub fn validate_allow_compaction(&self, since: &Antichain<u64>) -> Result<(), Error> {
        if PartialOrder::less_equal(&self.seal, since) {
            return Err(Error::from(format!(
                "invalid compaction at or in advance of trace seal {:?}: {:?}",
                self.seal, since,
            )));
        }

        if PartialOrder::less_equal(since, &self.since) {
            return Err(Error::from(format!(
                "invalid compaction less than or equal to trace since {:?}: {:?}",
                self.since, since
            )));
        }

        Ok(())
    }

    /// Update the compaction frontier to `since`.
    ///
    /// This function intentionally does not do any checking to see if ts is
    /// in advance of the current seal frontier, because we sometimes need to
    /// use this to revert a seal update in the event of a storage failure.
    pub fn allow_compaction(&mut self, since: Antichain<u64>) {
        self.since = since;
    }

    /// Writes the given batch to [Blob] storage and logically adds the contained
    /// updates to this trace.
    pub fn append<B: Blob>(
        &mut self,
        batch: BlobTraceBatch,
        blob: &mut BlobCache<B>,
    ) -> Result<(), Error> {
        if &self.ts_upper() != batch.desc.lower() {
            return Err(Error::from(format!(
                "batch lower doesn't match trace upper {:?}: {:?}",
                self.ts_upper(),
                batch.desc
            )));
        }
        let desc = batch.desc.clone();
        let key = self.new_blob_key();
        let size_bytes = blob.set_trace_batch(key.clone(), batch)?;
        // As mentioned above, batches are inserted into the trace with compaction
        // level set to 0.
        self.batches.push(TraceBatchMeta {
            key,
            desc,
            level: 0,
            size_bytes,
        });
        Ok(())
    }

    /// Returns a consistent read of all the updates contained in this trace.
    pub fn snapshot<B: Blob>(&self, blob: &BlobCache<B>) -> Result<TraceSnapshot, Error> {
        let ts_upper = self.ts_upper();
        let since = self.since();
        let mut updates = Vec::with_capacity(self.batches.len());
        for meta in self.batches.iter() {
            updates.push(blob.get_trace_batch(&meta.key)?);
        }
        Ok(TraceSnapshot {
            ts_upper,
            since,
            updates,
        })
    }

    /// Merge two batches into one, forwarding all updates not beyond the current
    /// `since` frontier to the `since` frontier.
    fn merge<B: Blob>(
        &mut self,
        first: &TraceBatchMeta,
        second: &TraceBatchMeta,
        blob: &mut BlobCache<B>,
    ) -> Result<TraceBatchMeta, Error> {
        if first.desc.upper() != second.desc.lower() {
            return Err(Error::from(format!(
                "invalid merge of non-consecutive batches {:?} and {:?}",
                first, second
            )));
        }

        // Sanity check that both batches being merged are at identical compaction
        // levels.
        debug_assert_eq!(first.level, second.level);
        let merged_level = first.level + 1;

        let desc = Description::new(
            first.desc.lower().clone(),
            second.desc.upper().clone(),
            self.since.clone(),
        );

        let mut updates = vec![];

        updates.extend(blob.get_trace_batch(&first.key)?.updates.iter().cloned());
        updates.extend(blob.get_trace_batch(&second.key)?.updates.iter().cloned());

        // TODO: use antichain more idiomatically.
        let since_time = self.since[0];
        for ((_, _), t, _) in updates.iter_mut() {
            if *t < since_time {
                *t = since_time;
            }
        }

        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        let new_batch = BlobTraceBatch {
            desc: desc.clone(),
            updates,
        };

        let key = self.new_blob_key();
        // TODO: actually clear the unwanted batches from the blob storage
        let size_bytes = blob.set_trace_batch(key.clone(), new_batch)?;

        Ok(TraceBatchMeta {
            key,
            desc,
            level: merged_level,
            size_bytes,
        })
    }

    /// Take one step towards compacting the trace.
    ///
    /// Returns true if the trace was modified, false otherwise.
    pub fn step<B: Blob>(&mut self, blob: &mut BlobCache<B>) -> Result<bool, Error> {
        // TODO: should we remember our position in this list?
        for i in 1..self.batches.len() {
            if (self.batches[i - 1].level == self.batches[i].level)
                && PartialOrder::less_equal(self.batches[i].desc.upper(), &self.since)
            {
                let first = self.batches[i - 1].clone();
                let second = self.batches[i].clone();

                let new_batch = self.merge(&first, &second, blob)?;

                // TODO: more performant way to do this?
                self.batches.remove(i);
                self.batches[i - 1] = new_batch;

                // Sanity check that the modified list of batches satisfies
                // all invariants.
                if cfg!(any(debug_assertions, test)) {
                    self.meta().validate()?;
                }

                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// A consistent snapshot of the data currently in a persistent [Trace].
#[derive(Debug)]
pub struct TraceSnapshot {
    /// An open upper bound on the times of contained updates.
    pub ts_upper: Antichain<u64>,
    /// Since frontier of the given updates.
    ///
    /// All updates not at times greater than this frontier must be advanced
    /// to a time that is equivalent to this frontier.
    pub since: Antichain<u64>,
    updates: Vec<Arc<BlobTraceBatch>>,
}

impl Snapshot<Vec<u8>, Vec<u8>> for TraceSnapshot {
    fn read<E: Extend<((Vec<u8>, Vec<u8>), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        if let Some(batch) = self.updates.pop() {
            buf.extend(batch.updates.iter().cloned());
        }
        !self.updates.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::indexed::encoding::Id;
    use crate::indexed::metrics::Metrics;
    use crate::indexed::SnapshotExt;
    use crate::mem::MemBlob;

    use super::*;

    #[test]
    fn test_allow_compaction() -> Result<(), Error> {
        let mut t: Trace = Trace::new(TraceMeta {
            id: Id(0),
            batches: vec![TraceBatchMeta {
                key: "key1".to_string(),
                desc: Description::new(
                    Antichain::from_elem(0),
                    Antichain::from_elem(10),
                    Antichain::from_elem(5),
                ),
                level: 1,
                size_bytes: 0,
            }],
            since: Antichain::from_elem(5),
            seal: Antichain::from_elem(10),
            next_blob_id: 0,
        });

        // Normal case: advance since frontier.
        t.validate_allow_compaction(&Antichain::from_elem(6))?;
        t.allow_compaction(Antichain::from_elem(6));

        // Repeat same since frontier.
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(6)),
            Err(Error::from("invalid compaction less than or equal to trace since Antichain { elements: [6] }: Antichain { elements: [6] }")));

        // Regress since frontier.
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(5)),
            Err(Error::from("invalid compaction less than or equal to trace since Antichain { elements: [6] }: Antichain { elements: [5] }")));

        // Advance since frontier to seal
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(10)),
            Err(Error::from("invalid compaction at or in advance of trace seal Antichain { elements: [10] }: Antichain { elements: [10] }")));

        // Advance since frontier beyond seal
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(11)),
            Err(Error::from("invalid compaction at or in advance of trace seal Antichain { elements: [10] }: Antichain { elements: [11] }")));

        Ok(())
    }

    #[test]
    fn trace_compact() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("trace_compact"),
        );
        let mut t = Trace::new(TraceMeta::new(Id(0)));
        t.update_seal(10);

        let batch = BlobTraceBatch {
            desc: Description::new(
                Antichain::from_elem(0),
                Antichain::from_elem(1),
                Antichain::from_elem(0),
            ),
            updates: vec![(("k".into(), "v".into()), 0, 1)],
        };

        assert_eq!(t.append(batch, &mut blob), Ok(()));
        let batch = BlobTraceBatch {
            desc: Description::new(
                Antichain::from_elem(1),
                Antichain::from_elem(3),
                Antichain::from_elem(0),
            ),
            updates: vec![(("k".into(), "v".into()), 2, 1)],
        };
        assert_eq!(t.append(batch, &mut blob), Ok(()));

        let batch = BlobTraceBatch {
            desc: Description::new(
                Antichain::from_elem(3),
                Antichain::from_elem(9),
                Antichain::from_elem(0),
            ),
            updates: vec![(("k".into(), "v".into()), 5, 1)],
        };
        assert_eq!(t.append(batch, &mut blob), Ok(()));

        t.validate_allow_compaction(&Antichain::from_elem(3))?;
        t.allow_compaction(Antichain::from_elem(3));
        t.step(&mut blob)?;
        let batch_meta: Vec<_> = t
            .batches
            .iter()
            .map(|meta| {
                (
                    meta.key.clone(),
                    (
                        meta.desc.lower()[0],
                        meta.desc.upper()[0],
                        meta.desc.since()[0],
                    ),
                    meta.level,
                )
            })
            .collect();

        assert_eq!(
            batch_meta,
            vec![
                ("Id(0)-trace-3".to_string(), (0, 3, 3), 1),
                ("Id(0)-trace-2".to_string(), (3, 9, 0), 0)
            ]
        );

        let snapshot = t.snapshot(&blob)?;
        assert_eq!(snapshot.since, Antichain::from_elem(3));
        assert_eq!(snapshot.ts_upper, Antichain::from_elem(9));

        let updates = snapshot.read_to_end();

        assert_eq!(
            updates,
            vec![
                (("k".into(), "v".into()), 3, 2),
                (("k".into(), "v".into()), 5, 1)
            ]
        );

        Ok(())
    }
}
