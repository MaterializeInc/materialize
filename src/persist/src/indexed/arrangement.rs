// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent, compacting data structure containing indexed `(Key, Value,
//! Time, Diff)` entries.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::{fmt, mem};

use differential_dataflow::lattice::Lattice;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use uuid::Uuid;

use crate::error::Error;
use crate::indexed::background::{CompactTraceReq, Maintainer};
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{
    BlobTraceBatch, TraceBatchMeta, TraceMeta, UnsealedBatchMeta, UnsealedMeta,
};
use crate::indexed::{BlobUnsealedBatch, Id, Snapshot};
use crate::pfuture::PFuture;
use crate::storage::{Blob, SeqNo};

/// A persistent, compacting data structure containing indexed `(Key, Value,
/// Time, Diff)` entries.
///
///
/// The data is logically and physically separated into two "buckets":
/// _unsealed_ and _trace_. It first enters and is initially placed into
/// unsealed, which is a holding pen roughly corresponding to the in-memory
/// buffer of a differential dataflow arrangement operator. At some point, the
/// arranged collection is _sealed_, which advances the _upper_ timestamp of the
/// collection and _logically_ (but not physically) moves the data into trace.
/// The trace bucket indexes the data by `(key, value, time)`. At some later
/// point, `unsealed_step` is called, which _physically_ moves the data from
/// unsealed to trace.
///
/// There are two notable differences between a persisted arrangement and a
/// differential in-mem one (besides the obvious durability):
/// - Because in-mem operations are so much faster than ones on durable storage,
///   the act of advancing the frontier and moving data into trace, one step in
///   differential, is split into separate steps in persist.
/// - The differential arrangement keeps the data arranged for efficient indexed
///   access (hence the name). Persist also keeps the data arranged the same
///   way, but finishing up the plumbing for indexed access is still a TODO.
///
/// Further details below.
///
/// # Unsealed
///
/// Unsealed exists to hold data that has been added to the persistent
/// collection but not yet "seal"ed into a trace. We store incoming data as
/// immutable batches of updates, corresponding to non-empty, sorted intervals
/// of [crate::storage::SeqNo]s.
///
/// As times get sealed and the corresponding updates get moved into the trace,
/// Unsealed can remove those updates, and eventually, entire batches. The
/// approach to removing sealed data optimizes for the common case, for which we
/// assume that:
/// - data arrives roughly in order,
/// - unsealed batches contain data for a small range of distinct times. Every
///   unsealed batch tracks the minimum and maximum update timestamp contained
///   within its list of updates, and we eagerly drop batches that only contain
///   data prior to the sealed frontier whenever possible. In the expected case,
///   this should be sufficient to ensure that Unsealed maintains a bounded
///   storage footprint. If either of the two assumptions are violated, either
///   because updates arrive out of order, or batches contain data at many
///   distinct timestamps, we periodically try to remove the updates strictly
///   behind the current sealed frontier from a given unsealed batch and replace
///   it with a "trimmed" batch that uses less storage.
///
/// This approach intentionally does nothing to physically coalesce multiple
/// unsealed batches into a single unsealed batch. Doing so has many potential
/// downsides; for example physically merging a batch containing updates 5
/// seconds ahead of the current sealed frontier with another batch containing
/// updates 5 hours ahead of the current sealed frontier would only hurt 5
/// seconds later, when the previously unmerged batch would have been dropped.
/// Instead, the merged batch has to be trimmed, which requires an extra read
/// and write. If we end up having significant amounts of data far ahead of the
/// current sealed frontier we likely will need a different structure that can
/// hold batches of updates organized by overlapping ranges of times and
/// physically merge unsealed batches using an approach similar to trace
/// physical compaction.
///
/// Invariants:
/// - All entries are after or equal to some time frontier and less than some
///   SeqNo.
/// - TODO: Space usage.
///
/// # Trace
///
/// An append-only list of immutable batches that describe updates corresponding
/// to sorted, contiguous, non-overlapping ranges of times. The `since` frontier
/// defines a time before which we can compact the history of updates (and
/// correspondingly no longer answer queries about).
///
/// We can compact the updates prior to the since frontier physically, by
/// combining batches representing consecutive intervals into one large batch
/// representing the union of those intervals, and logically, by forwarding
/// updates at times before the since frontier to the since frontier.
///
/// We also want to achieve a balance between the compactness of the
/// representation with the computational effort required to maintain the
/// representation. Specifically, if we have N batches of data already
/// compacted, we don't want every additional batch to perform O(N) work (i.e.
/// merge with N batches worth of data) in order to get compacted. Instead, we
/// would like to keep a geometrically decreasing (when viewed from oldest to
/// most recent) sequence of batches and perform O(N) work every N calls to
/// append. Thankfully, we can achieve all of this with a few simple rules:
///  - Batches are able to be compacted once the since frontier is in advance of
///    all of the data in the batch.
///  - All batches are assigned a nonzero compaction level. When a new batch is
///    appended to the trace it is assigned a compaction level of 0.
///  - We periodically merge consecutive batches at the same level L
///    representing time intervals [lo, mid) and [mid, hi) into a single batch
///    representing all of the updates in [lo, hi) with level L + 1 iff the new
///    batch contains more data than both of its parents, and L otherwise.. Once
///    two batches are merged they are removed from the trace and replaced with
///    the merged batch.
///  - Perform merges for the oldest batches possible first.
///
/// NB: this approach assumes that all batches are roughly uniformly sized when
/// they are first appended.
///
/// Invariants:
/// - All entries are before some time frontier.
/// - Batches are sorted by time and represent a sorted, consecutive,
///   non-overlapping list of time intervals.
/// - Individual batches are immutable, and their set of updates, the time
///   interval they describe and their compaction level all remain constant as
///   long as the batch remains in the trace.
/// - The compaction levels across the list of batches in a trace are weakly
///   decreasing (non-increasing) when iterating from oldest to most recent time
///   intervals.
/// - TODO: Space usage.
#[derive(Debug)]
pub struct Arrangement {
    id: Id,

    // TODO: This is redundant with the highest ts in trace_batches, remove it.
    unsealed_ts_lower: Antichain<u64>,

    // TODO: Rename to `upper` once we get rid of unsealed_ts_lower.
    seal: Antichain<u64>,
    since: Antichain<u64>,

    unsealed_batches: Vec<UnsealedBatchMeta>,
    trace_batches: Vec<TraceBatchMeta>,

    // TODO: next_blob_id is deprecated, remove these once we can safely bump
    // BlobMeta::CURRENT_VERSION.
    deprecated_unsealed_next_blob_id: u64,
    deprecated_trace_next_blob_id: u64,
}

impl Arrangement {
    /// Returns an Arrangement re-instantiated with the previously serialized
    /// state.
    pub fn new(unsealed: UnsealedMeta, trace: TraceMeta) -> Self {
        let id = unsealed.id;
        // TODO: Take id as a parameter instead once we can change the
        // serialization format.
        assert_eq!(
            trace.id, id,
            "internal error: unsealed id {:?} does not match trace id {:?}",
            unsealed.id, trace.id
        );
        Arrangement {
            id,
            unsealed_ts_lower: unsealed.ts_lower,
            seal: trace.seal,
            since: trace.since,
            unsealed_batches: unsealed.batches,
            trace_batches: trace.batches,
            deprecated_unsealed_next_blob_id: unsealed.next_blob_id,
            deprecated_trace_next_blob_id: trace.next_blob_id,
        }
    }

    /// Get a new key to write to the Blob store for this arrangement.
    pub fn new_blob_key() -> String {
        Uuid::new_v4().to_string()
    }

    /// Serializes the state of this Arrangement for later re-instantiation.
    pub fn meta(&self) -> (UnsealedMeta, TraceMeta) {
        let unsealed = UnsealedMeta {
            id: self.id,
            ts_lower: self.unsealed_ts_lower.clone(),
            batches: self.unsealed_batches.clone(),
            next_blob_id: self.deprecated_unsealed_next_blob_id,
        };
        let trace = TraceMeta {
            id: self.id,
            batches: self.trace_batches.clone(),
            since: self.since.clone(),
            seal: self.seal.clone(),
            next_blob_id: self.deprecated_trace_next_blob_id,
        };
        (unsealed, trace)
    }

    /// An open upper bound on the seqnos of contained updates.
    pub fn unsealed_seqno_upper(&self) -> SeqNo {
        self.unsealed_batches
            .last()
            .map_or_else(|| SeqNo(0), |meta| meta.desc.end)
    }

    /// Write a [BlobUnsealedBatch] to [Blob] storage and return the corresponding
    /// [UnsealedBatchMeta].
    ///
    /// The input batch is expected to satisfy all [BlobUnsealedBatch] invariants.
    fn unsealed_write_batch<L: Blob>(
        &mut self,
        batch: BlobUnsealedBatch,
        blob: &mut BlobCache<L>,
    ) -> Result<UnsealedBatchMeta, Error> {
        let key = Self::new_blob_key();
        let desc = batch.desc.clone();

        let (ts_upper, ts_lower) = {
            let mut upper_lower = None;

            for (_, ts, _) in batch.updates.iter() {
                upper_lower = match upper_lower {
                    None => Some((*ts, *ts)),
                    Some((mut upper, mut lower)) => {
                        if *ts > upper {
                            upper = *ts;
                        }

                        if *ts < lower {
                            lower = *ts;
                        }

                        Some((upper, lower))
                    }
                };
            }

            match upper_lower {
                None => {
                    return Err(Error::from(
                        "invalid unsealed batch: trying to write empty batch",
                    ))
                }
                Some((upper, lower)) => (upper, lower),
            }
        };

        debug_assert!(ts_upper >= ts_lower);
        let size_bytes = blob.set_unsealed_batch(key.clone(), batch)?;
        Ok(UnsealedBatchMeta {
            key,
            desc,
            ts_upper,
            ts_lower,
            size_bytes,
        })
    }

    /// Writes the given batch to [Blob] storage and logically adds the contained
    /// updates to this unsealed.
    pub fn unsealed_append<L: Blob>(
        &mut self,
        batch: BlobUnsealedBatch,
        blob: &mut BlobCache<L>,
    ) -> Result<(), Error> {
        if batch.desc.start != self.unsealed_seqno_upper() {
            return Err(Error::from(format!(
                "batch lower doesn't match seqno_upper {:?}: {:?}",
                self.unsealed_seqno_upper(),
                batch.desc
            )));
        }
        if cfg!(any(debug_assertions, test)) {
            // Batches being appended to this unsealed come from data being
            // drained out of the log. Indexed should have prevented this
            // write to the log, so this should never happen. Hopefully any
            // regressions in maintaining this invariant will be caught by this
            // debug/test check.
            for (_, ts, _) in batch.updates.iter() {
                if !self.unsealed_ts_lower.less_equal(ts) {
                    return Err(Error::from(format!(
                        "batch contains timestamp {:?} before ts_lower: {:?}",
                        ts, self.unsealed_ts_lower
                    )));
                }
            }
        }

        let meta = self.unsealed_write_batch(batch, blob)?;
        self.unsealed_batches.push(meta);
        Ok(())
    }

    /// Returns a consistent read of the updates contained in this unsealed
    /// matching the given filters (in practice, everything not in Trace).
    pub fn unsealed_snapshot<L: Blob>(
        &self,
        ts_lower: Antichain<u64>,
        ts_upper: Antichain<u64>,
        blob: &BlobCache<L>,
    ) -> Result<UnsealedSnapshot, Error> {
        if PartialOrder::less_than(&ts_upper, &ts_lower) {
            return Err(Error::from(format!(
                "invalid snapshot request: ts_upper {:?} is less than ts_lower {:?}",
                ts_upper, ts_lower
            )));
        }

        let mut batches = Vec::with_capacity(self.unsealed_batches.len());
        for meta in self.unsealed_batches.iter() {
            // We want to read this batch as long as it contains times [lo, hi] s.t.
            // they overlap with the requested [ts_lower, ts_upper).
            // More specifically, we can want to read this batch as long as both:
            // - ts_lower <= hi
            // - ts_upper > lo
            if ts_lower.less_equal(&meta.ts_upper) && !ts_upper.less_equal(&meta.ts_lower) {
                batches.push(blob.get_unsealed_batch_async(&meta.key));
            }
        }

        Ok(UnsealedSnapshot {
            ts_lower,
            ts_upper,
            batches,
        })
    }

    /// Removes all updates contained in this unsealed before the given bound.
    ///
    /// Returns a list of batches that can safely be deleted after the eviction is
    /// committed to durable storage.
    pub fn unsealed_truncate(
        &mut self,
        new_ts_lower: Antichain<u64>,
    ) -> Result<Vec<UnsealedBatchMeta>, Error> {
        if PartialOrder::less_than(&new_ts_lower, &self.unsealed_ts_lower) {
            return Err(format!(
                "cannot regress ts_lower from {:?} to {:?}",
                self.unsealed_ts_lower, new_ts_lower
            )
            .into());
        }
        self.unsealed_ts_lower = new_ts_lower;
        Ok(self.unsealed_evict())
    }

    /// Remove all batches containing only data strictly before the Unsealed's time
    /// lower bound.
    ///
    /// Returns a list of batches that can safely be deleted after the eviction is
    /// committed to durable storage.
    fn unsealed_evict(&mut self) -> Vec<UnsealedBatchMeta> {
        // TODO: actually physically free the old batches.
        let ts_lower = self.unsealed_ts_lower.clone();
        let evicted = self
            .unsealed_batches
            .iter()
            .filter(|b| !ts_lower.less_equal(&b.ts_upper))
            .cloned()
            .collect();
        self.unsealed_batches
            .retain(|b| ts_lower.less_equal(&b.ts_upper));

        evicted
    }

    /// Create a new [BlobUnsealedBatch] from `batch` containing only the subset of
    /// updates at or in advance of the Unsealed's time lower bound.
    ///
    /// `batch` is assumed not be eligible for eviction at the time of this function
    /// call, and to satisy all [BlobUnsealedBatch] invariants.
    fn unsealed_trim<L: Blob>(
        &mut self,
        batch: UnsealedBatchMeta,
        blob: &mut BlobCache<L>,
    ) -> Result<UnsealedBatchMeta, Error> {
        // Sanity check that batch cannot be evicted
        debug_assert!(self.unsealed_ts_lower.less_equal(&batch.ts_upper));
        let mut updates = vec![];

        updates.extend(
            blob.get_unsealed_batch_async(&batch.key)
                .recv()?
                .updates
                .iter()
                .filter(|(_, ts, _)| self.unsealed_ts_lower.less_equal(ts))
                .cloned(),
        );
        debug_assert!(!updates.is_empty());
        let new_batch = BlobUnsealedBatch {
            desc: batch.desc,
            updates,
        };

        self.unsealed_write_batch(new_batch, blob)
    }

    /// Take one step towards shrinking the representation of this unsealed.
    ///
    /// Returns true if the trace was modified, false otherwise.
    pub fn unsealed_step<L: Blob>(&mut self, blob: &mut BlobCache<L>) -> Result<bool, Error> {
        self.unsealed_evict();

        for (idx, batch) in self.unsealed_batches.iter_mut().enumerate() {
            // We can trim data out of the batch if it contains data at times < ts_lower.
            if !self.unsealed_ts_lower.less_equal(&batch.ts_lower) {
                let batch = batch.clone();
                let new_batch = self.unsealed_trim(batch, blob)?;
                self.unsealed_batches[idx] = new_batch;
                return Ok(true);
            }
        }

        Ok(false)
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
    pub fn trace_ts_upper(&self) -> Antichain<u64> {
        match self.trace_batches.last() {
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
    /// [Self::update_seal].
    pub fn validate_seal(&self, ts: u64) -> Result<(), String> {
        let prev = self.get_seal();
        if !prev.less_equal(&ts) {
            return Err(format!(
                "invalid seal for {:?}: {:?} not at or in advance of current seal frontier {:?}",
                self.id, ts, prev
            ));
        }
        Ok(())
    }

    /// A lower bound on the time at which updates may have been logically
    /// compacted together.
    pub fn since(&self) -> Antichain<u64> {
        self.since.clone()
    }

    /// Checks whether the given since would be valid to pass to
    /// [Self::allow_compaction].
    pub fn validate_allow_compaction(&self, since: &Antichain<u64>) -> Result<(), String> {
        if PartialOrder::less_equal(&self.seal, since) {
            return Err(format!(
                "invalid compaction at or in advance of trace seal {:?}: {:?}",
                self.seal, since,
            ));
        }

        if PartialOrder::less_than(since, &self.since) {
            return Err(format!(
                "invalid compaction less than trace since {:?}: {:?}",
                self.since, since
            ));
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
    pub fn trace_append<B: Blob>(
        &mut self,
        batch: BlobTraceBatch,
        blob: &mut BlobCache<B>,
    ) -> Result<(), Error> {
        if &self.trace_ts_upper() != batch.desc.lower() {
            return Err(Error::from(format!(
                "batch lower doesn't match trace upper {:?}: {:?}",
                self.trace_ts_upper(),
                batch.desc
            )));
        }
        let desc = batch.desc.clone();
        let key = Self::new_blob_key();
        let size_bytes = blob.set_trace_batch(key.clone(), batch)?;
        // As mentioned above, batches are inserted into the trace with compaction
        // level set to 0.
        self.trace_batches.push(TraceBatchMeta {
            key,
            desc,
            level: 0,
            size_bytes,
        });
        Ok(())
    }

    /// Returns a consistent read of all the updates contained in this trace.
    pub fn trace_snapshot<B: Blob>(&self, blob: &BlobCache<B>) -> TraceSnapshot {
        let ts_upper = self.trace_ts_upper();
        let since = self.since();
        let mut batches = Vec::with_capacity(self.trace_batches.len());
        for meta in self.trace_batches.iter() {
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
    pub fn trace_step<B: Blob>(
        &mut self,
        maintainer: &Maintainer<B>,
    ) -> Result<(u64, Vec<TraceBatchMeta>), Error> {
        let mut written_bytes = 0;
        let mut deleted = vec![];
        // TODO: should we remember our position in this list?
        for i in 1..self.trace_batches.len() {
            if (self.trace_batches[i - 1].level == self.trace_batches[i].level)
                && PartialOrder::less_equal(self.trace_batches[i].desc.upper(), &self.since)
            {
                let b0 = self.trace_batches[i - 1].clone();
                let b1 = self.trace_batches[i].clone();

                let req = CompactTraceReq {
                    b0,
                    b1,
                    since: self.since.clone(),
                };
                let res = maintainer.compact_trace(req).recv()?;
                let mut new_batch = res.merged;
                written_bytes += new_batch.size_bytes;

                // TODO: more performant way to do this?
                deleted.push(self.trace_batches.remove(i));
                mem::swap(&mut self.trace_batches[i - 1], &mut new_batch);
                deleted.push(new_batch);

                // Sanity check that the modified list of batches satisfies
                // all invariants.
                if cfg!(any(debug_assertions, test)) {
                    let (_, trace_meta) = self.meta();
                    trace_meta.validate()?;
                }

                break;
            }
        }
        Ok((written_bytes, deleted))
    }
}

/// A consistent snapshot of the data that is currently _physically_ in the
/// unsealed bucket of a persistent [Arrangement].
#[derive(Debug)]
pub struct UnsealedSnapshot {
    /// A closed lower bound on the times of contained updates.
    pub ts_lower: Antichain<u64>,
    /// An open upper bound on the times of the contained updates.
    pub ts_upper: Antichain<u64>,
    batches: Vec<PFuture<Arc<BlobUnsealedBatch>>>,
}

impl Snapshot<Vec<u8>, Vec<u8>> for UnsealedSnapshot {
    type Iter = UnsealedSnapshotIter;

    fn into_iters(self, num_iters: NonZeroUsize) -> Vec<Self::Iter> {
        let mut iters = Vec::with_capacity(num_iters.get());
        iters.resize_with(num_iters.get(), || UnsealedSnapshotIter {
            ts_lower: self.ts_lower.clone(),
            ts_upper: self.ts_upper.clone(),
            current_batch: Vec::new(),
            batches: VecDeque::new(),
        });
        // TODO: This should probably distribute batches based on size, but for
        // now it's simpler to round-robin them.
        for (i, batch) in self.batches.into_iter().enumerate() {
            let iter_idx = i % num_iters;
            iters[iter_idx].batches.push_back(batch);
        }
        iters
    }
}

/// An [Iterator] representing one part of the data in a [UnsealedSnapshot].
//
// This intentionally stores the batches as a VecDeque so we can return the data
// in roughly increasing timestamp order, but it's unclear if this is in any way
// important.
pub struct UnsealedSnapshotIter {
    /// A closed lower bound on the times of contained updates.
    ts_lower: Antichain<u64>,
    /// An open upper bound on the times of the contained updates.
    ts_upper: Antichain<u64>,

    current_batch: Vec<((Vec<u8>, Vec<u8>), u64, isize)>,
    batches: VecDeque<PFuture<Arc<BlobUnsealedBatch>>>,
}

impl fmt::Debug for UnsealedSnapshotIter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnsealedSnapshotIter")
            .field("ts_lower", &self.ts_lower)
            .field("ts_upper", &self.ts_upper)
            .field("current_batch(len)", &self.current_batch.len())
            .field("batches", &self.batches)
            .finish()
    }
}

impl Iterator for UnsealedSnapshotIter {
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
                        // in roughly increasing time order. At the same time,
                        // enforce our filter before we clone them.
                        let ts_lower = self.ts_lower.borrow();
                        let ts_upper = self.ts_upper.borrow();
                        self.current_batch.extend(
                            b.updates
                                .iter()
                                .rev()
                                .filter(|(_, ts, _)| {
                                    ts_lower.less_equal(&ts) && !ts_upper.less_equal(&ts)
                                })
                                .cloned(),
                        );
                        continue;
                    }
                    Err(err) => return Some(Err(err)),
                }
            }
        }
    }
}

/// A consistent snapshot of the data that is currently _physically_ in the
/// trace bucket of a persistent [Arrangement].
#[derive(Debug)]
pub struct TraceSnapshot {
    /// An open upper bound on the times of contained updates.
    pub ts_upper: Antichain<u64>,
    /// Since frontier of the given updates.
    ///
    /// All updates not at times greater than this frontier must be advanced
    /// to a time that is equivalent to this frontier.
    pub since: Antichain<u64>,
    batches: Vec<PFuture<Arc<BlobTraceBatch>>>,
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
    batches: VecDeque<PFuture<Arc<BlobTraceBatch>>>,
}

impl Default for TraceSnapshotIter {
    fn default() -> Self {
        TraceSnapshotIter {
            current_batch: Vec::new(),
            batches: VecDeque::new(),
        }
    }
}

impl fmt::Debug for TraceSnapshotIter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TraceSnapshotIter")
            .field("current_batch(len)", &self.current_batch.len())
            .field("batches", &self.batches)
            .finish()
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

/// A consistent snapshot of all data currently stored for an id.
#[derive(Debug)]
pub struct ArrangementSnapshot(
    pub(crate) UnsealedSnapshot,
    pub(crate) TraceSnapshot,
    pub(crate) SeqNo,
    pub(crate) Antichain<u64>,
);

impl ArrangementSnapshot {
    /// Returns the SeqNo at which this snapshot was run.
    ///
    /// All writes assigned a seqno < this are included.
    pub fn seqno(&self) -> SeqNo {
        self.2
    }

    /// Returns the since frontier of this snapshot.
    ///
    /// All updates at times less than this frontier must be forwarded
    /// to some time in this frontier.
    pub fn since(&self) -> Antichain<u64> {
        self.1.since.clone()
    }

    /// A logical upper bound on the times that had been added to the collection
    /// when this snapshot was taken
    pub(crate) fn get_seal(&self) -> Antichain<u64> {
        self.3.clone()
    }
}

impl Snapshot<Vec<u8>, Vec<u8>> for ArrangementSnapshot {
    type Iter = ArrangementSnapshotIter;

    fn into_iters(self, num_iters: NonZeroUsize) -> Vec<ArrangementSnapshotIter> {
        let since = self.since();
        let ArrangementSnapshot(unsealed, trace, _, _) = self;
        let unsealed_iters = unsealed.into_iters(num_iters);
        let trace_iters = trace.into_iters(num_iters);
        // I don't love the non-debug asserts, but it doesn't seem worth it to
        // plumb an error around here.
        assert_eq!(unsealed_iters.len(), num_iters.get());
        assert_eq!(trace_iters.len(), num_iters.get());
        unsealed_iters
            .into_iter()
            .zip(trace_iters.into_iter())
            .map(|(unsealed_iter, trace_iter)| ArrangementSnapshotIter {
                since: since.clone(),
                iter: trace_iter.chain(unsealed_iter),
            })
            .collect()
    }
}

/// An [Iterator] representing one part of the data in an [ArrangementSnapshot].
//
// This intentionally chains trace before unsealed so we get the data in roughly
// increasing timestamp order, but it's unclear if this is in any way important.
#[derive(Debug)]
pub struct ArrangementSnapshotIter {
    since: Antichain<u64>,
    iter: std::iter::Chain<TraceSnapshotIter, UnsealedSnapshotIter>,
}

impl Iterator for ArrangementSnapshotIter {
    type Item = Result<((Vec<u8>, Vec<u8>), u64, isize), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            x.map(|(kv, mut ts, diff)| {
                // When reading a snapshot, the contract of since is that all
                // update timestamps will be advanced to it. We do this
                // physically during compaction, but don't have hard guarantees
                // about how long that takes, so we have to account for
                // un-advanced batches on reads.
                ts.advance_by(self.since.borrow());
                (kv, ts, diff)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::Description;
    use tokio::runtime::Runtime;

    use crate::indexed::encoding::Id;
    use crate::indexed::metrics::Metrics;
    use crate::indexed::SnapshotExt;
    use crate::mem::{MemBlob, MemRegistry};

    use super::*;

    fn desc_from(lower: u64, upper: u64, since: u64) -> Description<u64> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(since),
        )
    }

    // Generate a list of ((k, v), t, 1) updates at all of the specified times.
    fn unsealed_updates(update_times: Vec<u64>) -> Vec<((Vec<u8>, Vec<u8>), u64, isize)> {
        update_times
            .into_iter()
            .map(|t| (("k".into(), "v".into()), t, 1))
            .collect()
    }

    // Generate an unsealed batch spanning the specified sequence numbers with
    // updates at the specified times.
    fn unsealed_batch(lower: u64, upper: u64, update_times: Vec<u64>) -> BlobUnsealedBatch {
        BlobUnsealedBatch {
            desc: SeqNo(lower)..SeqNo(upper),
            updates: unsealed_updates(update_times),
        }
    }

    fn unsealed_batch_meta(
        key: &str,
        lower: u64,
        upper: u64,
        ts_lower: u64,
        ts_upper: u64,
        size_bytes: u64,
    ) -> UnsealedBatchMeta {
        UnsealedBatchMeta {
            key: key.to_string(),
            desc: SeqNo(lower)..SeqNo(upper),
            ts_upper,
            ts_lower,
            size_bytes,
        }
    }

    // Attempt to read every update in `unsealed` at times in [lo, hi)
    fn slurp_unsealed_from<L: Blob>(
        arrangement: &Arrangement,
        blob: &BlobCache<L>,
        lo: u64,
        hi: Option<u64>,
    ) -> Result<Vec<((Vec<u8>, Vec<u8>), u64, isize)>, Error> {
        let hi = hi.map_or_else(|| Antichain::new(), |e| Antichain::from_elem(e));
        let snapshot = arrangement.unsealed_snapshot(Antichain::from_elem(lo), hi, &blob)?;
        let updates = snapshot.read_to_end()?;
        Ok(updates)
    }

    // Keys are randomly generated, so clear them before we do any comparisons.
    fn cleared_unsealed_keys(batches: &[UnsealedBatchMeta]) -> Vec<UnsealedBatchMeta> {
        batches
            .iter()
            .cloned()
            .map(|mut b| {
                b.key = "KEY".to_string();
                b
            })
            .collect()
    }

    // Keys are randomly generated, so clear them before we do any comparisons.
    fn cleared_trace_keys(batches: &[TraceBatchMeta]) -> Vec<TraceBatchMeta> {
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
    fn append_ts_lower_invariant() {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("append_ts_lower_invariant"),
        );
        let mut f = Arrangement::new(
            UnsealedMeta {
                id: Id(0),
                ts_lower: Antichain::from_elem(2),
                batches: vec![],
                next_blob_id: 0,
            },
            TraceMeta::new(Id(0)),
        );

        // ts < ts_lower.data()[0] is disallowed
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(1),
            updates: vec![(("k".into(), "v".into()), 1, 1)],
        };
        assert_eq!(
            f.unsealed_append(batch, &mut blob),
            Err(Error::from(
                "batch contains timestamp 1 before ts_lower: Antichain { elements: [2] }"
            ))
        );

        // ts == ts_lower.data()[0] is allowed
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(1),
            updates: vec![(("k".into(), "v".into()), 2, 1)],
        };
        assert_eq!(f.unsealed_append(batch, &mut blob), Ok(()));
    }

    /// This test checks whether we correctly determine the min/max times stored
    /// in a unsealed batch consisting of unsorted updates.
    #[test]
    fn append_detect_min_max_times() {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("append_ts_lower_invariant"),
        );
        let mut f = Arrangement::new(
            UnsealedMeta {
                id: Id(0),
                ts_lower: Antichain::from_elem(0),
                batches: vec![],
                next_blob_id: 0,
            },
            TraceMeta::new(Id(0)),
        );

        // Construct a unsealed batch where the updates are not sorted by time.
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(1),
            updates: vec![
                (("k".into(), "v".into()), 3, 1),
                (("k".into(), "v".into()), 2, 1),
            ],
        };

        assert_eq!(f.unsealed_append(batch, &mut blob), Ok(()));

        // Check that the batch has the correct min/max time bounds.
        let batch = &f.unsealed_batches[0];
        assert_eq!(batch.ts_lower, 2);
        assert_eq!(batch.ts_upper, 3);
    }

    #[test]
    fn truncate_regress() {
        let mut f = Arrangement::new(
            UnsealedMeta {
                id: Id(0),
                ts_lower: Antichain::from_elem(2),
                batches: vec![],
                next_blob_id: 0,
            },
            TraceMeta::new(Id(0)),
        );
        assert_eq!(f.unsealed_truncate(Antichain::from_elem(2)), Ok(vec![]));
        assert_eq!(
            f.unsealed_truncate(Antichain::from_elem(1)),
            Err(Error::from(
                "cannot regress ts_lower from Antichain { elements: [2] } to Antichain { elements: [1] }"
            ))
        );
    }

    #[test]
    fn unsealed_evict() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("unsealed_evict"),
        );
        let mut f = Arrangement::new(
            UnsealedMeta {
                id: Id(0),
                ts_lower: Antichain::from_elem(0),
                batches: vec![],
                next_blob_id: 0,
            },
            TraceMeta::new(Id(0)),
        );

        f.unsealed_append(unsealed_batch(0, 1, vec![0]), &mut blob)?;
        f.unsealed_append(unsealed_batch(1, 2, vec![1]), &mut blob)?;
        f.unsealed_append(unsealed_batch(2, 3, vec![0, 1]), &mut blob)?;

        let snapshot_updates = slurp_unsealed_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, unsealed_updates(vec![0, 0, 1, 1]));
        assert_eq!(
            cleared_unsealed_keys(&f.unsealed_batches),
            vec![
                unsealed_batch_meta("KEY", 0, 1, 0, 0, 58),
                unsealed_batch_meta("KEY", 1, 2, 1, 1, 58),
                unsealed_batch_meta("KEY", 2, 3, 0, 1, 92),
            ],
        );

        // Check that truncate doesn't do anything when no batches can be removed.
        assert_eq!(f.unsealed_truncate(Antichain::from_elem(0)), Ok(vec![]));

        // Check that truncate correctly returns the list of batches that can be
        // physically deleted.
        assert_eq!(
            f.unsealed_truncate(Antichain::from_elem(1))?
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![SeqNo(0)..SeqNo(1)]
        );

        // Check that repeatedly truncating the same time bound does not modify the unsealed.
        assert_eq!(f.unsealed_truncate(Antichain::from_elem(1)), Ok(vec![]));

        let snapshot_updates = slurp_unsealed_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, unsealed_updates(vec![0, 1, 1]));
        assert_eq!(
            cleared_unsealed_keys(&f.unsealed_batches),
            vec![
                unsealed_batch_meta("KEY", 1, 2, 1, 1, 58),
                unsealed_batch_meta("KEY", 2, 3, 0, 1, 92),
            ],
        );

        // Check that truncate correctly handles removing all data in the unsealed.
        assert_eq!(
            f.unsealed_truncate(Antichain::from_elem(2))?
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![SeqNo(1)..SeqNo(2), SeqNo(2)..SeqNo(3)]
        );

        // Check that truncate correctly handles the case where there are no more batches.
        assert_eq!(f.unsealed_truncate(Antichain::from_elem(2)), Ok(vec![]));

        Ok(())
    }

    #[test]
    fn unsealed_snapshot() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("unsealed_snapshot"),
        );
        let mut f = Arrangement::new(
            UnsealedMeta {
                id: Id(0),
                ts_lower: Antichain::from_elem(0),
                batches: vec![],
                next_blob_id: 0,
            },
            TraceMeta::new(Id(0)),
        );

        // Construct a batch holding updates for times [3, 5].
        let updates = vec![
            (("k".into(), "v".into()), 3, 1),
            (("k".into(), "v".into()), 5, 1),
        ];
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(2),
            updates: updates.clone(),
        };

        f.unsealed_append(batch, &mut blob)?;

        assert_eq!(slurp_unsealed_from(&f, &blob, 0, None)?, updates);
        assert_eq!(slurp_unsealed_from(&f, &blob, 0, Some(6))?, updates);

        assert_eq!(slurp_unsealed_from(&f, &blob, 0, Some(2))?, vec![]);
        assert_eq!(slurp_unsealed_from(&f, &blob, 6, None)?, vec![]);
        assert_eq!(slurp_unsealed_from(&f, &blob, 6, Some(8))?, vec![]);

        // hi == lo
        assert_eq!(slurp_unsealed_from(&f, &blob, 3, Some(3))?, vec![]);

        // invalid args: hi < lo
        assert_eq!(
            slurp_unsealed_from(&f, &blob, 4, Some(3)),
            Err(Error::from(
                    "invalid snapshot request: ts_upper Antichain { elements: [3] } is less than ts_lower Antichain { elements: [4] }"
            ))
        );

        // lo == batch_min, hi == batch_max + 1
        assert_eq!(slurp_unsealed_from(&f, &blob, 3, Some(6))?, updates);

        assert_eq!(slurp_unsealed_from(&f, &blob, 3, Some(4))?, updates[..1]);
        assert_eq!(slurp_unsealed_from(&f, &blob, 4, Some(5))?, vec![]);
        assert_eq!(slurp_unsealed_from(&f, &blob, 5, Some(6))?, updates[1..]);

        Ok(())
    }

    #[test]
    fn unsealed_batch_trim() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("unsealed_batch_trim"),
        );
        let mut f = Arrangement::new(
            UnsealedMeta {
                id: Id(0),
                ts_lower: Antichain::from_elem(0),
                batches: vec![],
                next_blob_id: 0,
            },
            TraceMeta::new(Id(0)),
        );

        // Construct a batch holding updates for times [0, 2].
        let updates = vec![
            (("k".into(), "v".into()), 0, 1),
            (("k".into(), "v".into()), 1, 1),
            (("k".into(), "v".into()), 2, 1),
        ];
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(2),
            updates: updates.clone(),
        };

        f.unsealed_append(batch, &mut blob)?;

        f.unsealed_truncate(Antichain::from_elem(1))?;

        // Check that no data is evicted after the truncate.
        let snapshot_updates = slurp_unsealed_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, updates);

        // Take a step to trim the batch
        assert!(f.unsealed_step(&mut blob)?);

        let snapshot_updates = slurp_unsealed_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, updates[1..]);

        assert_eq!(
            cleared_unsealed_keys(&f.unsealed_batches),
            vec![unsealed_batch_meta("KEY", 0, 2, 1, 2, 92)],
        );

        Ok(())
    }

    #[test]
    fn test_allow_compaction() -> Result<(), Error> {
        let mut t = Arrangement::new(
            UnsealedMeta::new(Id(0)),
            TraceMeta {
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
            },
        );

        // Normal case: advance since frontier.
        t.validate_allow_compaction(&Antichain::from_elem(6))?;
        t.allow_compaction(Antichain::from_elem(6));

        // Repeat same since frontier.
        t.validate_allow_compaction(&Antichain::from_elem(6))?;
        t.allow_compaction(Antichain::from_elem(6));

        // Regress since frontier.
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(5)),
            Err("invalid compaction less than trace since Antichain { elements: [6] }: Antichain { elements: [5] }".into()));

        // Advance since frontier to seal
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(10)),
            Err("invalid compaction at or in advance of trace seal Antichain { elements: [10] }: Antichain { elements: [10] }".into()));

        // Advance since frontier beyond seal
        assert_eq!(t.validate_allow_compaction(&Antichain::from_elem(11)),
            Err("invalid compaction at or in advance of trace seal Antichain { elements: [10] }: Antichain { elements: [11] }".into()));

        Ok(())
    }

    #[test]
    fn trace_seal() -> Result<(), Error> {
        let mut t = Arrangement::new(
            UnsealedMeta::new(Id(0)),
            TraceMeta {
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
            },
        );

        // Normal case: advance seal frontier.
        t.validate_seal(11)?;
        t.update_seal(11);

        // Repeat same seal frontier.
        t.validate_seal(11)?;
        t.update_seal(11);

        // Regress seal frontier.
        assert_eq!(t.validate_seal(10),
            Err("invalid seal for Id(0): 10 not at or in advance of current seal frontier Antichain { elements: [11] }".into()));

        Ok(())
    }

    #[test]
    fn trace_compact() -> Result<(), Error> {
        let mut blob = BlobCache::new(Metrics::default(), MemRegistry::new().blob_no_reentrance()?);
        let maintainer = Maintainer::new(blob.clone(), Arc::new(Runtime::new()?));
        let mut t = Arrangement::new(UnsealedMeta::new(Id(0)), TraceMeta::new(Id(0)));
        t.update_seal(10);

        let batch = BlobTraceBatch {
            desc: desc_from(0, 1, 0),
            updates: vec![
                (("k".into(), "v".into()), 0, 1),
                (("k2".into(), "v2".into()), 0, 1),
            ],
        };

        assert_eq!(t.trace_append(batch, &mut blob), Ok(()));
        let batch = BlobTraceBatch {
            desc: desc_from(1, 3, 0),
            updates: vec![
                (("k".into(), "v".into()), 2, 1),
                (("k3".into(), "v3".into()), 2, 1),
            ],
        };
        assert_eq!(t.trace_append(batch, &mut blob), Ok(()));

        let batch = BlobTraceBatch {
            desc: desc_from(3, 9, 0),
            updates: vec![(("k".into(), "v".into()), 5, 1)],
        };
        assert_eq!(t.trace_append(batch, &mut blob), Ok(()));

        t.validate_allow_compaction(&Antichain::from_elem(3))?;
        t.allow_compaction(Antichain::from_elem(3));
        let (written_bytes, deleted_batches) = t.trace_step(&maintainer)?;
        // Change this to a >0 check if it starts to be a maintenance burden.
        assert_eq!(written_bytes, 162);
        assert_eq!(
            deleted_batches
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![desc_from(1, 3, 0), desc_from(0, 1, 0)]
        );

        // Check that step doesn't do anything when there's nothing to compact.
        let (written_bytes, deleted_batches) = t.trace_step(&maintainer)?;
        assert_eq!(written_bytes, 0);
        assert_eq!(deleted_batches, vec![]);

        assert_eq!(
            cleared_trace_keys(&t.trace_batches),
            vec![
                TraceBatchMeta {
                    key: "KEY".to_string(),
                    desc: desc_from(0, 3, 3),
                    level: 1,
                    size_bytes: 162,
                },
                TraceBatchMeta {
                    key: "KEY".to_string(),
                    desc: desc_from(3, 9, 0),
                    level: 0,
                    size_bytes: 90,
                },
            ]
        );

        let snapshot = t.trace_snapshot(&blob);
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
        assert_eq!(t.trace_append(batch, &mut blob), Ok(()));
        t.validate_allow_compaction(&Antichain::from_elem(10))?;
        t.allow_compaction(Antichain::from_elem(10));
        let (written_bytes, deleted_batches) = t.trace_step(&maintainer)?;
        assert_eq!(written_bytes, 90);
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
            cleared_trace_keys(&t.trace_batches),
            vec![
                TraceBatchMeta {
                    key: "KEY".to_string(),
                    desc: desc_from(0, 3, 3),
                    level: 1,
                    size_bytes: 162,
                },
                TraceBatchMeta {
                    key: "KEY".to_string(),
                    desc: desc_from(3, 10, 10),
                    level: 0,
                    size_bytes: 90,
                },
            ]
        );

        let snapshot = t.trace_snapshot(&blob);
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
