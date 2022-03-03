// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent, compacting data structure containing indexed `(Key, Value,
//! Time, i64)` entries.

use std::mem;

use differential_dataflow::trace::Description;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use uuid::Uuid;

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::background::{
    CompactTraceReq, CompactTraceRes, DrainUnsealedReq, DrainUnsealedRes,
};
use crate::indexed::cache::{BlobCache, CacheHint};
use crate::indexed::columnar::ColumnarRecordsVec;
use crate::indexed::encoding::{
    ArrangementMeta, BlobTraceBatchPart, TraceBatchMeta, UnsealedBatchMeta, UnsealedSnapshotMeta,
};
use crate::indexed::snapshot::{ArrangementSnapshot, Snapshot, TraceSnapshot};
use crate::indexed::{BlobUnsealedBatch, Id};
use crate::storage::{Blob, BlobRead, SeqNo};

/// A persistent, compacting data structure containing indexed `(Key, Value,
/// Time, i64)` entries.
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
/// # Invariants
/// - New updates less than the seal frontier are never added to unsealed.
/// - Unsealed batches have non-overlapping SeqNo ranges.
/// - All trace updates are less than the seal frontier.
/// - Trace batches are sorted by time and represent a sorted, consecutive,
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

    // TODO: Rename to `upper` once we get rid of unsealed_ts_lower.
    seal: Antichain<u64>,
    since: Antichain<u64>,

    unsealed_batches: Vec<UnsealedBatchMeta>,
    trace_batches: Vec<TraceBatchMeta>,
}

impl Arrangement {
    /// Returns an Arrangement re-instantiated with the previously serialized
    /// state.
    pub fn new(meta: ArrangementMeta) -> Self {
        Arrangement {
            id: meta.id,
            seal: meta.seal,
            since: meta.since,
            unsealed_batches: meta.unsealed_batches,
            trace_batches: meta.trace_batches,
        }
    }

    /// Get a new key to write to the Blob store for this arrangement.
    pub fn new_blob_key() -> String {
        Uuid::new_v4().to_string()
    }

    /// Serializes the state of this Arrangement for later re-instantiation.
    pub fn meta(&self) -> ArrangementMeta {
        ArrangementMeta {
            id: self.id,
            since: self.since.clone(),
            seal: self.seal.clone(),
            unsealed_batches: self.unsealed_batches.clone(),
            trace_batches: self.trace_batches.clone(),
        }
    }

    /// An open upper bound on the seqnos of contained updates.
    pub fn unsealed_seqno_upper(&self) -> SeqNo {
        self.unsealed_batches
            .last()
            .map_or_else(|| SeqNo(0), |meta| meta.desc.end)
    }

    /// Returns a consistent read of all the updates contained in this
    /// arrangement.
    pub fn snapshot<L: BlobRead>(
        &self,
        seqno: SeqNo,
        blob: &BlobCache<L>,
    ) -> Result<ArrangementSnapshot, Error> {
        let seal_frontier = self.get_seal();
        let trace = self.trace_snapshot(blob);
        let unsealed = self
            .unsealed_snapshot(trace.ts_upper.clone(), Antichain::new())?
            .fetch(blob);
        Ok(ArrangementSnapshot(unsealed, trace, seqno, seal_frontier))
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

            for updates in batch.updates.iter() {
                for (_, ts, _) in updates.iter() {
                    upper_lower = match upper_lower {
                        None => Some((ts, ts)),
                        Some((mut upper, mut lower)) => {
                            if ts > upper {
                                upper = ts;
                            }

                            if ts < lower {
                                lower = ts;
                            }

                            Some((upper, lower))
                        }
                    };
                }
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
        let format = ProtoBatchFormat::ParquetKvtd;
        let size_bytes = blob.set_unsealed_batch(key.clone(), batch, format)?;
        Ok(UnsealedBatchMeta {
            key,
            format,
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
            // drained out of the log. Indexed should have prevented this write
            // to the log, so this should never happen. Hopefully any
            // regressions in maintaining this invariant will be caught by this
            // debug/test check.
            //
            // Unclear if there's anything to do here, but do note the
            // following:
            // - We can't check against the seal frontier, because the seal
            //   frontier might have advanced after we acknowledged the write
            //   which we are now trying to append into unsealed.
            // - This check interacts weirdly with the optimization to not write
            //   out empty trace batches. Said another way, that optimization
            //   delays advancing the trace's ts_upper. This means we could have
            //   a sequence of writes and seals up to t2 but if the writes
            //   compact down to nothing (or if there are no writes), the
            //   trace's ts_upper will still be at 0 because we refused to write
            //   an empty batch. Then if we have a write to t1, that will pass
            //   this check even though the seal frontier was already t2 and the
            //   trace's ts upper really should have been t2 as well.
            let trace_ts_upper = self.trace_ts_upper();
            for updates in batch.updates.iter() {
                for (_, ts, _) in updates.iter() {
                    if !trace_ts_upper.less_equal(&ts) {
                        return Err(Error::from(format!(
                            "batch contains timestamp {:?} before trace ts_upper: {:?}",
                            ts, trace_ts_upper
                        )));
                    }
                }
            }
        }

        let meta = self.unsealed_write_batch(batch, blob)?;
        self.unsealed_batches.push(meta);
        Ok(())
    }

    /// Returns a consistent read of the updates contained in this unsealed
    /// matching the given filters (in practice, everything not in Trace).
    pub fn unsealed_snapshot(
        &self,
        ts_lower: Antichain<u64>,
        ts_upper: Antichain<u64>,
    ) -> Result<UnsealedSnapshotMeta, Error> {
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
                batches.push(meta.clone());
            }
        }

        Ok(UnsealedSnapshotMeta {
            ts_lower,
            ts_upper,
            batches,
        })
    }

    /// Atomically moves all writes in unsealed not in advance of the trace's
    /// seal frontier into the trace and does any necessary resulting eviction
    /// work to remove unnecessary batches.
    pub fn unsealed_drain<L: Blob>(&mut self, blob: &mut BlobCache<L>) -> Result<(), Error> {
        let req = match self.unsealed_next_drain_req()? {
            Some(req) => req,
            None => return Ok(()),
        };
        let res = Self::drain_unsealed_blocking(blob, req)?;
        self.unsealed_handle_drain_response(res);
        Ok(())
    }

    /// Get the next available drain work from the unsealed, if some exists.
    pub fn unsealed_next_drain_req(&self) -> Result<Option<DrainUnsealedReq>, Error> {
        // If the trace's physical frontier matches the arrangement's logical
        // seal frontier, then nothing to do.
        let seal = self.get_seal();
        let trace_upper = self.trace_ts_upper();
        if seal == trace_upper {
            return Ok(None);
        }

        let desc = Description::new(
            trace_upper,
            seal,
            Antichain::from_elem(Timestamp::minimum()),
        );
        if PartialOrder::less_equal(desc.upper(), desc.lower()) {
            return Err(format!("invalid batch bounds: {:?}", desc).into());
        }

        // Move a batch of data from unsealed into trace by reading a
        // snapshot from unsealed...
        let snap = self.unsealed_snapshot(desc.lower().clone(), desc.upper().clone())?;
        Ok(Some(DrainUnsealedReq { desc, snap }))
    }

    /// Copies unsealed data matching the specified description into a new trace
    /// batch.
    pub fn drain_unsealed_blocking<B: Blob>(
        blob: &BlobCache<B>,
        req: DrainUnsealedReq,
    ) -> Result<DrainUnsealedRes, Error> {
        let snap = req.snap.clone().fetch(&blob);
        let mut updates = Snapshot::<Vec<u8>, Vec<u8>>::into_iter(snap)
            .collect::<Result<Vec<_>, Error>>()
            .map_err(|err| format!("failed to fetch snapshot: {}", err))?;

        // Don't bother minting empty trace batches that we'll just have to
        // compact later, it's wasteful of precious storage bandwidth and
        // everything works perfectly well when the trace upper hasn't yet
        // caught up to sealed.
        if updates.is_empty() {
            return Ok(DrainUnsealedRes { req, drained: None });
        }

        // Trace batches are required to be sorted and consolidated by ((k, v), t)
        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        let updates = updates.iter().collect::<ColumnarRecordsVec>().into_inner();
        let batch = BlobTraceBatchPart {
            desc: req.desc.clone(),
            index: 0,
            updates,
        };

        let desc = batch.desc.clone();
        let key = Self::new_blob_key();
        let format = ProtoBatchFormat::ParquetKvtd;
        let size_bytes = blob.set_trace_batch(key.clone(), batch, format)?;
        // Batches are inserted into the trace with compaction level set to 0.
        let drained = TraceBatchMeta {
            keys: vec![key],
            format,
            desc,
            level: 0,
            size_bytes,
        };

        Ok(DrainUnsealedRes {
            req,
            drained: Some(drained),
        })
    }

    /// Handle an externally completed trace compaction request.
    ///
    /// TODO: Call unsealed_evict at the end of this and return a list of
    /// unsealed batches that can now be physically deleted after the drain step
    /// is committed to durable storage. This could save us a META write.
    pub fn unsealed_handle_drain_response(&mut self, res: DrainUnsealedRes) {
        let batch = match res.drained {
            Some(batch) => batch,
            None => return,
        };

        if &self.trace_ts_upper() != batch.desc.lower() {
            // This trace batch doesn't line up with the ones we have. Nothing
            // to do here.
            return;
        }

        self.trace_batches.push(batch);
    }

    /// Remove all batches containing only data strictly before the trace's
    /// physical ts frontier.
    ///
    /// Returns a list of batches that can safely be deleted after the eviction
    /// is committed to durable storage.
    pub fn unsealed_evict(&mut self) -> Vec<UnsealedBatchMeta> {
        let ts_lower = self.trace_ts_upper();
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
        debug_assert!(self.trace_ts_upper().less_equal(&batch.ts_upper));
        let updates = ColumnarRecordsVec::from_iter(
            blob.get_unsealed_batch_async(&batch.key, CacheHint::NeverAdd)
                .recv()?
                .updates
                .iter()
                .flat_map(|u| u.iter())
                .filter(|(_, ts, _)| self.trace_ts_upper().less_equal(ts)),
        )
        .into_inner();
        debug_assert!(updates.len() != 0);
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

        let trace_ts_upper = self.trace_ts_upper();
        for (idx, batch) in self.unsealed_batches.iter_mut().enumerate() {
            // We can trim data out of the batch if it contains data at times <
            // trace_ts_upper.
            if !trace_ts_upper.less_equal(&batch.ts_lower) {
                let batch = batch.clone();
                let new_batch = self.unsealed_trim(batch, blob)?;
                self.unsealed_batches[idx] = new_batch;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// The frontier of times that have been physically moved into trace.
    ///
    /// While `self.seal` tracks the frontier of times that have been logically
    /// been closed and are eligible to be moved into the trace,
    /// `self.trace_ts_upper()` tracks the frontier of times that have
    /// actually been physically moved into the trace. `self.seal()` is required
    /// to manage invariants between commands (e.g. a seal request has to be at
    /// a time in advance of prior seal requests) whereas
    /// `self.trace_ts_upper()` is required to manage physical reads
    /// and writes to the trace (e.g. to determine which times may be added that
    /// are not already present.
    ///
    /// Invariant:
    /// - self.trace_ts_upper() <= self.seal()
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

    /// Returns a consistent read of all the updates contained in this trace.
    pub fn trace_snapshot<B: BlobRead>(&self, blob: &BlobCache<B>) -> TraceSnapshot {
        let ts_upper = self.trace_ts_upper();
        let since = self.since();
        let mut batches = Vec::with_capacity(self.trace_batches.len());
        for meta in self.trace_batches.iter() {
            // We want to save these results to the cache (if possible) because
            // currently the cache is purely in-memory and thus will be empty on
            // restart. This policy is much more clearly worthwhile for unsealed
            // batches, as every unsealed batch will eventually get moved into
            // trace and therefore every unsealed batch we can populate in the
            // saves us an external read later.
            //
            // The rationale for trace is more unclear. If the size of all data
            // in trace ends up being smaller than the size of the cache then
            // clearly populating the cache is better. Otherwise, we could get
            // unlucky and fill the cache with one large trace batch that won't
            // get compacted for a very long time. We chose to allow trace snapshots
            // to populate the cache because the default cache size is small
            // enough that in real workloads large batches won't pollute the
            // cache.
            for key in meta.keys.iter() {
                batches.push(blob.get_trace_batch_async(key, CacheHint::MaybeAdd));
            }
        }
        TraceSnapshot {
            ts_upper,
            since,
            batches,
        }
    }

    /// Get the next available compaction work from the trace, if some exists.
    pub fn trace_next_compact_req(&self) -> Result<Option<CompactTraceReq>, Error> {
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

                return Ok(Some(req));
            }
        }
        Ok(None)
    }

    /// Handle an externally completed trace compaction request.
    ///
    /// Returns a list of trace batches that can now be physically deleted after
    /// the compaction step is committed to durable storage.
    pub fn trace_handle_compact_response(&mut self, res: CompactTraceRes) -> Vec<TraceBatchMeta> {
        let mut deleted = vec![];
        for i in 1..self.trace_batches.len() {
            let b0 = &self.trace_batches[i - 1];
            let b1 = &self.trace_batches[i];

            if &res.req.b0 == b0 && &res.req.b1 == b1 {
                let mut new_batch = res.merged;
                deleted.push(self.trace_batches.remove(i));
                mem::swap(&mut self.trace_batches[i - 1], &mut new_batch);
                deleted.push(new_batch);

                // Sanity check that the modified list of batches satisfies
                // all invariants.
                debug_assert_eq!(self.meta().validate(), Ok(()));

                break;
            }
        }

        return deleted;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use differential_dataflow::trace::Description;
    use tokio::runtime::Runtime as AsyncRuntime;

    use crate::gen::persist::ProtoBatchFormat;
    use crate::indexed::columnar::ColumnarRecords;
    use crate::indexed::encoding::Id;
    use crate::indexed::metrics::Metrics;
    use crate::indexed::snapshot::SnapshotExt;
    use crate::indexed::Maintainer;
    use crate::mem::{MemBlob, MemRegistry};

    use super::*;

    /// Take one step towards compacting the trace.
    ///
    /// Returns a list of trace batches that can now be physically deleted after
    /// the compaction step is committed to durable storage.
    fn trace_step<B: Blob>(
        arrangement: &mut Arrangement,
        maintainer: &Maintainer<B>,
    ) -> Result<(u64, Vec<TraceBatchMeta>), Error> {
        let req = arrangement.trace_next_compact_req()?;

        if let Some(req) = req {
            let fut = maintainer.compact_trace(req);
            let res = fut.recv()?;
            let written_bytes = res.merged.size_bytes;
            Ok((
                written_bytes,
                arrangement.trace_handle_compact_response(res),
            ))
        } else {
            Ok((0, vec![]))
        }
    }

    fn desc_from(lower: u64, upper: u64, since: u64) -> Description<u64> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(since),
        )
    }

    // Generate a list of ((k, v), t, 1) updates at all of the specified times.
    fn unsealed_updates(update_times: Vec<u64>) -> Vec<((Vec<u8>, Vec<u8>), u64, i64)> {
        update_times
            .into_iter()
            .map(|t| (("k".into(), "v".into()), t, 1))
            .collect()
    }

    // Generate a ColumnarRecords containing the provided updates
    fn columnar_records(updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> Vec<ColumnarRecords> {
        updates.iter().collect::<ColumnarRecordsVec>().into_inner()
    }

    // Generate an unsealed batch spanning the specified sequence numbers with
    // updates at the specified times.
    fn unsealed_batch(lower: u64, upper: u64, update_times: Vec<u64>) -> BlobUnsealedBatch {
        BlobUnsealedBatch {
            desc: SeqNo(lower)..SeqNo(upper),
            updates: columnar_records(unsealed_updates(update_times)),
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
            format: ProtoBatchFormat::ParquetKvtd,
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
    ) -> Result<Vec<((Vec<u8>, Vec<u8>), u64, i64)>, Error> {
        let hi = hi.map_or_else(Antichain::new, Antichain::from_elem);
        let snapshot = arrangement
            .unsealed_snapshot(Antichain::from_elem(lo), hi)?
            .fetch(&blob);
        let updates = snapshot.read_to_end()?;
        Ok(updates)
    }

    // Keys are randomly generated, so clear them before we do any comparisons.
    // Also clear size_bytes so the tests don't have to be updated every time
    // it changes.
    fn cleared_unsealed(batches: &[UnsealedBatchMeta]) -> Vec<UnsealedBatchMeta> {
        batches
            .iter()
            .cloned()
            .map(|mut b| {
                b.key = "KEY".to_string();
                b.size_bytes = 0;
                b
            })
            .collect()
    }

    // Keys are randomly generated, so clear them before we do any comparisons.
    // Also clear size_bytes so the tests don't have to be updated every time
    // it changes.
    fn cleared_trace(batches: &[TraceBatchMeta]) -> Vec<TraceBatchMeta> {
        batches
            .iter()
            .cloned()
            .map(|mut b| {
                b.keys = vec![];
                b.size_bytes = 0;
                b
            })
            .collect()
    }

    #[test]
    fn append_trace_ts_upper_invariant() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::new(AsyncRuntime::new()?),
            MemBlob::new_no_reentrance("append_trace_ts_upper_invariant"),
            None,
        );
        let mut f = Arrangement::new(ArrangementMeta {
            id: Id(0),
            trace_batches: vec![TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(0, 2, 0),
                level: 1,
                size_bytes: 0,
            }],
            ..Default::default()
        });

        // ts < trace_ts_upper is disallowed
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(1),
            updates: columnar_records(vec![(("k".into(), "v".into()), 1, 1)]),
        };
        assert_eq!(
            f.unsealed_append(batch, &mut blob),
            Err(Error::from(
                "batch contains timestamp 1 before trace ts_upper: Antichain { elements: [2] }"
            ))
        );

        // ts == trace_ts_upper is allowed
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(1),
            updates: columnar_records(vec![(("k".into(), "v".into()), 2, 1)]),
        };
        assert_eq!(f.unsealed_append(batch, &mut blob), Ok(()));

        Ok(())
    }

    /// This test checks whether we correctly determine the min/max times stored
    /// in a unsealed batch consisting of unsorted updates.
    #[test]
    fn append_detect_min_max_times() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::new(AsyncRuntime::new()?),
            MemBlob::new_no_reentrance("append_ts_lower_invariant"),
            None,
        );
        let mut f = Arrangement::new(ArrangementMeta {
            id: Id(0),
            unsealed_batches: vec![],
            ..Default::default()
        });

        // Construct a unsealed batch where the updates are not sorted by time.
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(1),
            updates: columnar_records(vec![
                (("k".into(), "v".into()), 3, 1),
                (("k".into(), "v".into()), 2, 1),
            ]),
        };

        assert_eq!(f.unsealed_append(batch, &mut blob), Ok(()));

        // Check that the batch has the correct min/max time bounds.
        let meta = &f.unsealed_batches[0];
        assert_eq!(meta.ts_lower, 2);
        assert_eq!(meta.ts_upper, 3);

        Ok(())
    }

    #[test]
    fn unsealed_evict() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::new(AsyncRuntime::new()?),
            MemBlob::new_no_reentrance("unsealed_evict"),
            None,
        );
        let mut f = Arrangement::new(ArrangementMeta {
            id: Id(0),
            unsealed_batches: vec![],
            ..Default::default()
        });

        f.unsealed_append(unsealed_batch(0, 1, vec![0]), &mut blob)?;
        f.unsealed_append(unsealed_batch(1, 2, vec![1]), &mut blob)?;
        f.unsealed_append(unsealed_batch(2, 3, vec![0, 1]), &mut blob)?;

        let snapshot_updates = slurp_unsealed_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, unsealed_updates(vec![0, 0, 1, 1]));
        assert_eq!(
            cleared_unsealed(&f.unsealed_batches),
            vec![
                unsealed_batch_meta("KEY", 0, 1, 0, 0, 0),
                unsealed_batch_meta("KEY", 1, 2, 1, 1, 0),
                unsealed_batch_meta("KEY", 2, 3, 0, 1, 0),
            ],
        );

        // Check that evict doesn't do anything when no batches can be removed.
        assert_eq!(f.unsealed_evict(), vec![]);

        // Check that evict correctly returns the list of batches that can be
        // physically deleted.
        f.update_seal(1);
        f.unsealed_drain(&mut blob)?;
        assert_eq!(
            f.unsealed_evict()
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![SeqNo(0)..SeqNo(1)]
        );

        // Check that repeatedly evicting the same time bound does not modify
        // the unsealed.
        assert_eq!(f.unsealed_evict(), vec![]);

        let snapshot_updates = slurp_unsealed_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, unsealed_updates(vec![0, 1, 1]));
        assert_eq!(
            cleared_unsealed(&f.unsealed_batches),
            vec![
                unsealed_batch_meta("KEY", 1, 2, 1, 1, 0),
                unsealed_batch_meta("KEY", 2, 3, 0, 1, 0),
            ],
        );

        // Check that evict correctly handles removing all data in the unsealed.
        f.update_seal(2);
        f.unsealed_drain(&mut blob)?;
        assert_eq!(
            f.unsealed_evict()
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![SeqNo(1)..SeqNo(2), SeqNo(2)..SeqNo(3)]
        );

        // Check that evict correctly handles the case where there are no more batches.
        assert_eq!(f.unsealed_evict(), vec![]);

        Ok(())
    }

    #[test]
    fn unsealed_snapshot() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::new(AsyncRuntime::new()?),
            MemBlob::new_no_reentrance("unsealed_snapshot"),
            None,
        );
        let mut f = Arrangement::new(ArrangementMeta {
            id: Id(0),
            unsealed_batches: vec![],
            ..Default::default()
        });

        // Construct a batch holding updates for times [3, 5].
        let updates = vec![
            (("k".into(), "v".into()), 3, 1),
            (("k".into(), "v".into()), 5, 1),
        ];
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(2),
            updates: columnar_records(updates.clone()),
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
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::new(AsyncRuntime::new()?),
            MemBlob::new_no_reentrance("unsealed_batch_trim"),
            None,
        );
        let mut f = Arrangement::new(ArrangementMeta {
            id: Id(0),
            unsealed_batches: vec![],
            ..Default::default()
        });

        // Construct a batch holding updates for times [0, 2].
        let updates = vec![
            (("k".into(), "v".into()), 0, 1),
            (("k".into(), "v".into()), 1, 1),
            (("k".into(), "v".into()), 2, 1),
        ];
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(2),
            updates: columnar_records(updates.clone()),
        };

        f.unsealed_append(batch, &mut blob)?;

        f.update_seal(1);

        // Check that no data has been moved after the update_seal.
        let snapshot_updates = slurp_unsealed_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, updates);

        // Physically drain the sealed data from unsealed into trace.
        f.unsealed_drain(&mut blob)?;
        // Take a step to trim the batch
        assert!(f.unsealed_step(&mut blob)?);

        let snapshot_updates = slurp_unsealed_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, updates[1..]);

        assert_eq!(
            cleared_unsealed(&f.unsealed_batches),
            vec![unsealed_batch_meta("KEY", 0, 2, 1, 2, 0)],
        );

        Ok(())
    }

    #[test]
    fn test_allow_compaction() -> Result<(), Error> {
        let mut t = Arrangement::new(ArrangementMeta {
            id: Id(0),
            trace_batches: vec![TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(0, 10, 5),
                level: 1,
                size_bytes: 0,
            }],
            since: Antichain::from_elem(5),
            seal: Antichain::from_elem(10),
            ..Default::default()
        });

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
        t.validate_allow_compaction(&Antichain::from_elem(10))?;

        // Advance since frontier beyond seal
        t.validate_allow_compaction(&Antichain::from_elem(11))?;

        Ok(())
    }

    #[test]
    fn trace_seal() -> Result<(), Error> {
        let mut t = Arrangement::new(ArrangementMeta {
            id: Id(0),
            trace_batches: vec![TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
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
            ..Default::default()
        });

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
        let async_runtime = Arc::new(AsyncRuntime::new()?);
        let metrics = Arc::new(Metrics::default());
        let mut blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::clone(&async_runtime),
            MemRegistry::new().blob_no_reentrance()?,
            None,
        );
        let maintainer = Maintainer::new(blob.clone(), async_runtime, metrics);
        let mut t = Arrangement::new(ArrangementMeta::new(Id(0)));
        t.update_seal(10);

        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(1),
            updates: columnar_records(vec![
                (("k".into(), "v".into()), 0, 1),
                (("k2".into(), "v2".into()), 0, 1),
            ]),
        };
        assert_eq!(t.unsealed_append(batch, &mut blob), Ok(()));
        t.update_seal(1);
        assert_eq!(t.unsealed_drain(&mut blob), Ok(()));
        assert_eq!(t.trace_batches.len(), 1);

        let batch = BlobUnsealedBatch {
            desc: SeqNo(1)..SeqNo(2),
            updates: columnar_records(vec![
                (("k".into(), "v".into()), 2, 1),
                (("k3".into(), "v3".into()), 2, 1),
            ]),
        };
        assert_eq!(t.unsealed_append(batch, &mut blob), Ok(()));
        t.update_seal(3);
        assert_eq!(t.unsealed_drain(&mut blob), Ok(()));
        assert_eq!(t.trace_batches.len(), 2);

        let batch = BlobUnsealedBatch {
            desc: SeqNo(2)..SeqNo(3),
            updates: columnar_records(vec![(("k".into(), "v".into()), 5, 1)]),
        };
        assert_eq!(t.unsealed_append(batch, &mut blob), Ok(()));
        t.update_seal(9);
        assert_eq!(t.unsealed_drain(&mut blob), Ok(()));
        assert_eq!(t.trace_batches.len(), 3);

        t.validate_allow_compaction(&Antichain::from_elem(3))?;
        t.allow_compaction(Antichain::from_elem(3));
        let (written_bytes, deleted_batches) = trace_step(&mut t, &maintainer)?;
        // NB: This intentionally doesn't assert any particular size so this
        // test doesn't need to be updated if encoded batch size changes.
        assert!(written_bytes > 0);
        assert_eq!(
            deleted_batches
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![desc_from(1, 3, 0), desc_from(0, 1, 0)]
        );

        // Check that step doesn't do anything when there's nothing to compact.
        let (written_bytes, deleted_batches) = trace_step(&mut t, &maintainer)?;
        assert_eq!(written_bytes, 0);
        assert_eq!(deleted_batches, vec![]);

        assert_eq!(
            cleared_trace(&t.trace_batches),
            vec![
                TraceBatchMeta {
                    keys: vec![],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 3),
                    level: 1,
                    size_bytes: 0,
                },
                TraceBatchMeta {
                    keys: vec![],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(3, 9, 0),
                    level: 0,
                    size_bytes: 0,
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

        let batch = BlobUnsealedBatch {
            desc: SeqNo(3)..SeqNo(4),
            updates: columnar_records(vec![(("k".into(), "v".into()), 9, 1)]),
        };
        assert_eq!(t.unsealed_append(batch, &mut blob), Ok(()));
        t.update_seal(10);
        assert_eq!(t.unsealed_drain(&mut blob), Ok(()));

        t.validate_allow_compaction(&Antichain::from_elem(10))?;
        t.allow_compaction(Antichain::from_elem(10));
        let (written_bytes, deleted_batches) = trace_step(&mut t, &maintainer)?;
        // NB: This intentionally doesn't assert any particular size so this
        // test doesn't need to be updated if encoded batch size changes.
        assert!(written_bytes > 0);
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
            cleared_trace(&t.trace_batches),
            vec![
                TraceBatchMeta {
                    keys: vec![],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 3),
                    level: 1,
                    size_bytes: 0,
                },
                TraceBatchMeta {
                    keys: vec![],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(3, 10, 10),
                    level: 0,
                    size_bytes: 0,
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

    #[test]
    fn compaction_beyond_upper() -> Result<(), Error> {
        let async_runtime = Arc::new(AsyncRuntime::new()?);
        let metrics = Arc::new(Metrics::default());
        let mut blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::clone(&async_runtime),
            MemRegistry::new().blob_no_reentrance()?,
            None,
        );
        let maintainer = Maintainer::new(blob.clone(), async_runtime, metrics);
        let mut t = Arrangement::new(ArrangementMeta::new(Id(0)));

        // Add updates to both the trace and the unsealeds.
        let batch = BlobUnsealedBatch {
            desc: SeqNo(0)..SeqNo(1),
            updates: columnar_records(vec![
                (("k1".into(), "v1".into()), 0, 1),
                (("k2".into(), "v2".into()), 1, 1),
            ]),
        };
        assert_eq!(t.unsealed_append(batch, &mut blob), Ok(()));
        t.update_seal(10);
        assert_eq!(t.unsealed_drain(&mut blob), Ok(()));

        let unsealed_updates = vec![
            (("k3".into(), "v3".into()), 10, 1),
            (("k4".into(), "v4".into()), 11, 1),
        ];
        let batch = BlobUnsealedBatch {
            desc: SeqNo(1)..SeqNo(2),
            updates: columnar_records(unsealed_updates),
        };
        t.unsealed_append(batch, &mut blob)?;

        // Allow compaction beyond the seal frontier of [10].
        t.validate_allow_compaction(&Antichain::from_elem(30))?;
        t.allow_compaction(Antichain::from_elem(30));

        // The updated compaction since and seal frontier should hold even if we did not yet step
        // for compaction.

        let expected = vec![
            (("k1".into(), "v1".into()), 30, 1),
            (("k2".into(), "v2".into()), 30, 1),
            (("k3".into(), "v3".into()), 30, 1),
            (("k4".into(), "v4".into()), 30, 1),
        ];

        let snapshot = t.snapshot(SeqNo(42) /* this is unused */, &blob)?;
        assert_eq!(snapshot.since(), Antichain::from_elem(30));
        assert_eq!(snapshot.get_seal(), Antichain::from_elem(10));

        let updates = snapshot.read_to_end()?;

        assert_eq!(updates, expected,);

        // The yielded updates must be the same after compaction.

        trace_step(&mut t, &maintainer)?;

        let snapshot = t.snapshot(SeqNo(42) /* this is unused */, &blob)?;
        assert_eq!(snapshot.since(), Antichain::from_elem(30));
        assert_eq!(snapshot.get_seal(), Antichain::from_elem(10));

        let updates = snapshot.read_to_end()?;

        assert_eq!(updates, expected,);

        Ok(())
    }
}
