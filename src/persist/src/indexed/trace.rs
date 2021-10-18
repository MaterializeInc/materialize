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

use std::collections::VecDeque;
use std::mem;
use std::num::NonZeroUsize;
use std::sync::Arc;

use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use uuid::Uuid;

use crate::error::Error;
use crate::future::Future;
use crate::indexed::background::{CompactTraceReq, Maintainer};
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
///    all of the updates in [lo, hi) with level L + 1 iff the new batch contains
///    more data than both of its parents, and L otherwise.. Once two batches are
///    merged they are removed from the trace and replaced with the merged batch.
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
    // NB: We may at some point need to break this up into separate logical and
    // physical compaction frontiers.
    since: Antichain<u64>,
    // NB: The Descriptions here are sorted and contiguous half-open intervals
    // `[lower, upper)`.
    // The frontier the trace has been sealed up to.
    seal: Antichain<u64>,
    batches: Vec<TraceBatchMeta>,

    // TODO: next_blob_id is deprecated, remove this once we can safely bump
    // BlobMeta::CURRENT_VERSION.
    deprecated_next_blob_id: u64,
}

impl Trace {
    /// Returns a Trace re-instantiated with the previously serialized
    /// state.
    pub fn new(meta: TraceMeta) -> Self {
        Trace {
            id: meta.id,
            since: meta.since,
            seal: meta.seal,
            batches: meta.batches,
            deprecated_next_blob_id: meta.next_blob_id,
        }
    }

    /// Returns a new key to write to the Blob store for a trace.
    pub fn new_blob_key() -> String {
        Uuid::new_v4().to_string()
    }

    /// Serializes the state of this Trace for later re-instantiation.
    pub fn meta(&self) -> TraceMeta {
        TraceMeta {
            id: self.id,
            since: self.since.clone(),
            seal: self.seal.clone(),
            batches: self.batches.clone(),
            next_blob_id: self.deprecated_next_blob_id,
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
        if !prev.less_equal(&ts) {
            return Err(Error::from(format!(
                "invalid seal for {:?}: {:?} not at or in advance of current seal frontier {:?}",
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

        if PartialOrder::less_than(since, &self.since) {
            return Err(Error::from(format!(
                "invalid compaction less than trace since {:?}: {:?}",
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
        let key = Trace::new_blob_key();
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
    pub fn snapshot<B: Blob>(&self, blob: &BlobCache<B>) -> TraceSnapshot {
        let ts_upper = self.ts_upper();
        let since = self.since();
        let mut batches = Vec::with_capacity(self.batches.len());
        for meta in self.batches.iter() {
            batches.push(blob.get_trace_batch_async(&meta.key));
        }
        TraceSnapshot {
            ts_upper,
            since,
            batches,
        }
    }

    /// Take one step towards compacting the trace.
    ///
    /// Returns a list of trace batches that can now be physically deleted after
    /// the compaction step is committed to durable storage.
    pub fn step<B: Blob>(
        &mut self,
        maintainer: &Maintainer<B>,
    ) -> Result<(u64, Vec<TraceBatchMeta>), Error> {
        let mut written_bytes = 0;
        let mut deleted = vec![];
        // TODO: should we remember our position in this list?
        for i in 1..self.batches.len() {
            if (self.batches[i - 1].level == self.batches[i].level)
                && PartialOrder::less_equal(self.batches[i].desc.upper(), &self.since)
            {
                let b0 = self.batches[i - 1].clone();
                let b1 = self.batches[i].clone();

                let req = CompactTraceReq {
                    b0,
                    b1,
                    since: self.since.clone(),
                };
                let res = maintainer.compact_trace(req).recv()?;
                let mut new_batch = res.merged;
                written_bytes += new_batch.size_bytes;

                // TODO: more performant way to do this?
                deleted.push(self.batches.remove(i));
                mem::swap(&mut self.batches[i - 1], &mut new_batch);
                deleted.push(new_batch);

                // Sanity check that the modified list of batches satisfies
                // all invariants.
                if cfg!(any(debug_assertions, test)) {
                    self.meta().validate()?;
                }

                break;
            }
        }
        Ok((written_bytes, deleted))
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
    batches: Vec<Future<Arc<BlobTraceBatch>>>,
}

impl Snapshot<Vec<u8>, Vec<u8>> for TraceSnapshot {
    type Iter = TraceSnapshotIter;

    fn into_iters(self, num_iters: NonZeroUsize) -> Vec<Self::Iter> {
        let mut iters = Vec::with_capacity(num_iters.get());
        iters.resize_with(num_iters.get(), || TraceSnapshotIter::default());
        // TODO: This should probably distribute batches based on size, but for
        // now it's simpler to round-robin them.
        for (i, batch) in self.batches.into_iter().enumerate() {
            let iter_idx = i % num_iters;
            iters[iter_idx].batches.push_back(batch);
        }
        iters
    }
}

/// An [Iterator] representing one part of the data in a [TraceSnapshot].
//
// This intentionally stores the batches as a VecDeque so we can return the data
// in roughly increasing timestamp order, but it's unclear if this is in any way
// important.
pub struct TraceSnapshotIter {
    current_batch: Vec<((Vec<u8>, Vec<u8>), u64, isize)>,
    batches: VecDeque<Future<Arc<BlobTraceBatch>>>,
}

impl Default for TraceSnapshotIter {
    fn default() -> Self {
        TraceSnapshotIter {
            current_batch: Vec::new(),
            batches: VecDeque::new(),
        }
    }
}

impl Iterator for TraceSnapshotIter {
    type Item = Result<((Vec<u8>, Vec<u8>), u64, isize), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.current_batch.is_empty() {
                let update = self.current_batch.pop().unwrap();
                return Some(Ok(update));
            } else {
                // current_batch is empty, find a new one.
                let b = match self.batches.pop_front() {
                    None => return None,
                    Some(b) => b,
                };
                match b.recv() {
                    Ok(b) => {
                        // Reverse the updates so we can pop them off the back
                        // in roughly increasing time order.
                        self.current_batch.extend(b.updates.iter().rev().cloned());
                        continue;
                    }
                    Err(err) => return Some(Err(err)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::Description;
    use tokio::runtime::Runtime;

    use crate::indexed::encoding::Id;
    use crate::indexed::metrics::Metrics;
    use crate::indexed::SnapshotExt;
    use crate::mem::MemRegistry;

    use super::*;

    fn desc_from(lower: u64, upper: u64, since: u64) -> Description<u64> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(since),
        )
    }

    // Keys are randomly generated, so clear them before we do any comparisons.
    fn cleared_keys(batches: &[TraceBatchMeta]) -> Vec<TraceBatchMeta> {
        batches
            .iter()
            .cloned()
            .map(|mut b| {
                b.key = "KEY".to_string();
                b
            })
            .collect()
    }

    #[test]
    fn test_allow_compaction() -> Result<(), Error> {
        let mut t: Trace = Trace::new(TraceMeta {
            id: Id(0),
            batches: vec![TraceBatchMeta {
                key: "key1".to_string(),
                desc: desc_from(0, 10, 5),
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
        t.validate_allow_compaction(&Antichain::from_elem(6))?;
        t.allow_compaction(Antichain::from_elem(6));

        // Regress since frontier.
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(5)),
            Err(Error::from("invalid compaction less than trace since Antichain { elements: [6] }: Antichain { elements: [5] }")));

        // Advance since frontier to seal
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(10)),
            Err(Error::from("invalid compaction at or in advance of trace seal Antichain { elements: [10] }: Antichain { elements: [10] }")));

        // Advance since frontier beyond seal
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(11)),
            Err(Error::from("invalid compaction at or in advance of trace seal Antichain { elements: [10] }: Antichain { elements: [11] }")));

        Ok(())
    }

    #[test]
    fn trace_seal() -> Result<(), Error> {
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

        // Normal case: advance seal frontier.
        t.validate_seal(11)?;
        t.update_seal(11);

        // Repeat same seal frontier.
        t.validate_seal(11)?;
        t.update_seal(11);

        // Regress seal frontier.
        assert_eq!(t.validate_seal(10),
            Err(Error::from("invalid seal for Id(0): 10 not at or in advance of current seal frontier Antichain { elements: [11] }")));

        Ok(())
    }

    #[test]
    fn trace_compact() -> Result<(), Error> {
        let mut blob = BlobCache::new(Metrics::default(), MemRegistry::new().blob_no_reentrance()?);
        let maintainer = Maintainer::new(blob.clone(), Arc::new(Runtime::new()?));
        let mut t = Trace::new(TraceMeta::new(Id(0)));
        t.update_seal(10);

        let batch = BlobTraceBatch {
            desc: desc_from(0, 1, 0),
            updates: vec![
                (("k".into(), "v".into()), 0, 1),
                (("k2".into(), "v2".into()), 0, 1),
            ],
        };

        assert_eq!(t.append(batch, &mut blob), Ok(()));
        let batch = BlobTraceBatch {
            desc: desc_from(1, 3, 0),
            updates: vec![
                (("k".into(), "v".into()), 2, 1),
                (("k3".into(), "v3".into()), 2, 1),
            ],
        };
        assert_eq!(t.append(batch, &mut blob), Ok(()));

        let batch = BlobTraceBatch {
            desc: desc_from(3, 9, 0),
            updates: vec![(("k".into(), "v".into()), 5, 1)],
        };
        assert_eq!(t.append(batch, &mut blob), Ok(()));

        t.validate_allow_compaction(&Antichain::from_elem(3))?;
        t.allow_compaction(Antichain::from_elem(3));
        let (written_bytes, deleted_batches) = t.step(&maintainer)?;
        // Change this to a >0 check if it starts to be a maintenance burden.
        assert_eq!(written_bytes, 322);
        assert_eq!(
            deleted_batches
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![desc_from(1, 3, 0), desc_from(0, 1, 0)]
        );

        // Check that step doesn't do anything when there's nothing to compact.
        let (written_bytes, deleted_batches) = t.step(&maintainer)?;
        assert_eq!(written_bytes, 0);
        assert_eq!(deleted_batches, vec![]);

        assert_eq!(
            cleared_keys(&t.batches),
            vec![
                TraceBatchMeta {
                    key: "KEY".to_string(),
                    desc: desc_from(0, 3, 3),
                    level: 1,
                    size_bytes: 322,
                },
                TraceBatchMeta {
                    key: "KEY".to_string(),
                    desc: desc_from(3, 9, 0),
                    level: 0,
                    size_bytes: 186,
                },
            ]
        );

        let snapshot = t.snapshot(&blob);
        assert_eq!(snapshot.since, Antichain::from_elem(3));
        assert_eq!(snapshot.ts_upper, Antichain::from_elem(9));

        let updates = snapshot.read_to_end()?;

        assert_eq!(
            updates,
            vec![
                (("k".into(), "v".into()), 3, 2),
                (("k".into(), "v".into()), 5, 1),
                (("k2".into(), "v2".into()), 3, 1),
                (("k3".into(), "v3".into()), 3, 1),
            ]
        );

        t.update_seal(11);

        let batch = BlobTraceBatch {
            desc: desc_from(9, 10, 0),
            updates: vec![(("k".into(), "v".into()), 9, 1)],
        };
        assert_eq!(t.append(batch, &mut blob), Ok(()));
        t.validate_allow_compaction(&Antichain::from_elem(10))?;
        t.allow_compaction(Antichain::from_elem(10));
        let (written_bytes, deleted_batches) = t.step(&maintainer)?;
        assert_eq!(written_bytes, 186);
        assert_eq!(
            deleted_batches
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![desc_from(9, 10, 0), desc_from(3, 9, 0)]
        );

        // Check that compactions which do not result in a batch larger than both
        // parents do not increment the result batch's compaction level.
        assert_eq!(
            cleared_keys(&t.batches),
            vec![
                TraceBatchMeta {
                    key: "KEY".to_string(),
                    desc: desc_from(0, 3, 3),
                    level: 1,
                    size_bytes: 322,
                },
                TraceBatchMeta {
                    key: "KEY".to_string(),
                    desc: desc_from(3, 10, 10),
                    level: 0,
                    size_bytes: 186,
                },
            ]
        );

        let snapshot = t.snapshot(&blob);
        assert_eq!(snapshot.since, Antichain::from_elem(10));
        assert_eq!(snapshot.ts_upper, Antichain::from_elem(10));

        let updates = snapshot.read_to_end()?;

        assert_eq!(
            updates,
            vec![
                (("k".into(), "v".into()), 3, 2),
                (("k".into(), "v".into()), 10, 2),
                (("k2".into(), "v2".into()), 3, 1),
                (("k3".into(), "v3".into()), 3, 1),
            ]
        );

        Ok(())
    }
}
