// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent, compacting, indexed data structure of `(Key, Value, Time,
//! Diff)` updates.

// NB: These really don't need to be public, but the public doc lint is nice.
pub mod background;
pub mod cache;
pub mod encoding;
pub mod metrics;
pub mod runtime;
pub mod trace;
pub mod unsealed;

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::time::Instant;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use ore::cast::CastFrom;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::Error;
use crate::future::FutureHandle;
use crate::indexed::background::Maintainer;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{
    BlobMeta, BlobTraceBatch, BlobUnsealedBatch, Id, StreamRegistration, TraceMeta, UnsealedMeta,
};
use crate::indexed::metrics::{metric_duration_ms, Metrics};
use crate::indexed::trace::{Trace, TraceSnapshot, TraceSnapshotIter};
use crate::indexed::unsealed::{Unsealed, UnsealedSnapshot, UnsealedSnapshotIter};
use crate::storage::{Blob, Log, SeqNo};

enum PendingResponse {
    SeqNo(FutureHandle<SeqNo>, Result<SeqNo, Error>),
    Unit(FutureHandle<()>, Result<(), Error>),
}

impl PendingResponse {
    pub fn fill(self) {
        match self {
            PendingResponse::SeqNo(f, resp) => f.fill(resp),
            PendingResponse::Unit(f, resp) => f.fill(resp),
        }
    }

    pub fn fill_err(self, err: Error) {
        match self {
            PendingResponse::SeqNo(f, resp) => {
                if resp.is_err() {
                    f.fill(resp);
                } else {
                    f.fill(Err(err));
                }
            }
            PendingResponse::Unit(f, resp) => {
                if resp.is_err() {
                    f.fill(resp);
                } else {
                    f.fill(Err(err));
                }
            }
        }
    }
}

/// This struct holds changes to [Indexed] that have not been committed to
/// persistent storage or sent to downstream listeners.
struct Pending {
    writes: HashMap<Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>>,
    responses: Vec<PendingResponse>,
    seals: HashMap<Id, u64>,
}

impl Pending {
    fn new() -> Self {
        Self {
            writes: HashMap::new(),
            responses: Vec::new(),
            seals: HashMap::new(),
        }
    }

    // Validate that there are no pending writes, seals or responses.
    #[cfg(any(debug_assertions, test))]
    fn validate_empty(&self) -> Result<(), Error> {
        if !self.writes.is_empty() {
            return Err(Error::from(format!(
                "still have {} pending writes after draining pending writes, expected 0.",
                self.writes.len()
            )));
        }

        if !self.responses.is_empty() {
            return Err(Error::from(format!(
                "still have {} pending responses after draining pending responses, expected 0.",
                self.responses.len()
            )));
        }

        if !self.seals.is_empty() {
            return Err(Error::from(format!(
                "still have {} pending seals after draining pending seals, expected 0.",
                self.seals.len()
            )));
        }

        Ok(())
    }

    fn add_writes(&mut self, updates: Vec<(Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>)>) {
        for (id, updates) in updates {
            self.writes.entry(id).or_default().extend(updates);
        }
    }

    fn add_response(&mut self, resp: PendingResponse) {
        self.responses.push(resp);
    }

    fn add_seals(&mut self, ids: Vec<Id>, seal: u64) {
        for id in ids {
            self.seals.insert(id, seal);
        }
    }

    /// Take the set of pending writes out of [Pending], leaving an empty hashmap.
    fn take_writes(&mut self) -> HashMap<Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>> {
        std::mem::take(&mut self.writes)
    }

    /// Take the set of pending responses out of [Pending], leaving an empty vector.
    fn take_responses(&mut self) -> Vec<PendingResponse> {
        std::mem::take(&mut self.responses)
    }

    /// Take the set of pending seals out of [Pending], leaving an empty hashmap.
    fn take_seals(&mut self) -> HashMap<Id, u64> {
        std::mem::take(&mut self.seals)
    }

    /// Return true if [Pending] has at least one pending response.
    ///
    /// TODO: It's unclear whether this API is worth doing. We could alternatively
    /// just make writes and responses public and avoid having this function and
    /// the various getter functions.
    fn has_responses(&self) -> bool {
        !self.responses.is_empty()
    }
}

/// A persistent, compacting, indexed data structure of `(Key, Value, Time,
/// Diff)` updates.
///
/// The lifecycle of contained entries is as follows:
/// - Initially: inserted into an [Unsealed], which indexes them by
///   `(time, key, value)`.
/// - Once the update's time has been "seal"ed: transferred from the
///   [Unsealed] into a [Trace], which indexes them by `(key, value,
///   time)`.
///
/// Notes:
/// - An entry should only logically exist in one of these places at a time,
///   even though it may physically exist in more than one of them.
/// - Similarly, `frontier` represents the border between data in [Unsealed]
///   and [Trace]. Trace is logically append-only, so data is
///   transferred to it once all the data for some timestamp has arrived. On
///   read, [Indexed] uses this frontier to ignore any data in Unsealed that
///   exists in in Trace.
/// - Writes, seals, and allow_compaction requests are not committed to durable
///   storage immediately because we want to amortize the cost of writing to
///   durable storage across many of those requests. Instead, those requests are
///   applied to the in-memory Indexed object, and their responses are stored in
///   `pending_responses`. Later pending writes and serialized metadata are
///   committed to durable storage in `drain_pending` which flushes all pending
///   writes to [Unsealed], commits the current serialized metadata to durable
///   storage, and clears and responds to all pending responses.
/// - Pending writes, seals, and allow_compactions are drained before processing
///   any other type of request.
pub struct Indexed<L: Log, B: Blob> {
    next_stream_id: Id,
    unsealeds_seqno_upper: SeqNo,
    // This is conceptually a map from `String` -> `Id`, but lookups are rare
    // and this representation is optimized for the metadata serialization path,
    // which is less rare.
    id_mapping: Vec<StreamRegistration>,
    graveyard: Vec<StreamRegistration>,
    // NB: we are not using Log for anything at the moment and instead have
    // all writes going directly to trace. At some point we'll need to revisit
    // what we want to do with Log, and whether we want it to live inside of
    // Indexed or somewhere else.
    log: L,
    blob: BlobCache<B>,
    maintainer: Maintainer<B>,
    unsealeds: BTreeMap<Id, Unsealed>,
    traces: BTreeMap<Id, Trace>,
    listeners: HashMap<Id, Vec<ListenFn<Vec<u8>, Vec<u8>>>>,
    metrics: Metrics,
    pending: Pending,
    prev_meta: BlobMeta,
}

impl<L: Log, B: Blob> Indexed<L, B> {
    /// Returns a new Indexed, initializing each Unsealed and Trace with the
    /// existing data for them in the blob storage, if any.
    pub fn new(
        mut log: L,
        mut blob: BlobCache<B>,
        maintainer: Maintainer<B>,
        metrics: Metrics,
    ) -> Result<Self, Error> {
        let meta = blob
            .get_meta()
            .map_err(|err| {
                // Indexed is expected to close the log and blob it's handed.
                // Usually that happens when close is called on Indexed itself,
                // but if there's an error constructing it, we never get to that
                // point and have to clean up ourselves.
                //
                // TODO: Regression test for this.
                if let Err(err) = log.close() {
                    log::warn!("error closing log: {}", err);
                }
                if let Err(err) = blob.close() {
                    log::warn!("error closing blob: {}", err);
                }
                err
            })?
            .unwrap_or_default();
        let meta_copy = meta.clone();
        let unsealeds = meta
            .unsealeds
            .into_iter()
            .map(|meta| (meta.id, Unsealed::new(meta)))
            .collect();
        let traces = meta
            .traces
            .into_iter()
            .map(|meta| (meta.id, Trace::new(meta)))
            .collect();
        let indexed = Indexed {
            next_stream_id: meta.next_stream_id,
            unsealeds_seqno_upper: meta.unsealeds_seqno_upper,
            id_mapping: meta.id_mapping,
            graveyard: meta.graveyard,
            log,
            blob,
            maintainer,
            unsealeds,
            traces,
            listeners: HashMap::new(),
            metrics,
            pending: Pending::new(),
            prev_meta: meta_copy,
        };

        Ok(indexed)
    }

    /// Revert the in-memory state back to a previously serialized version.
    ///
    /// Used to keep the in-memory and durably stored data structures consistent
    /// in the presence of errors.
    ///
    /// TODO: can we simplify this logic and combine it with Indexed::new()? In
    /// principle both functions are doing very similar things to start up given
    /// a set of serialized metadata.
    fn restore(&mut self) {
        let meta = self.prev_meta.clone();

        self.next_stream_id = meta.next_stream_id;
        self.unsealeds_seqno_upper = meta.unsealeds_seqno_upper;
        self.id_mapping = meta.id_mapping;
        self.graveyard = meta.graveyard;

        let restored_unsealeds: BTreeMap<Id, Unsealed> = meta
            .unsealeds
            .into_iter()
            .map(|meta| (meta.id, Unsealed::new(meta)))
            .collect();

        self.unsealeds = restored_unsealeds;

        let restored_traces: BTreeMap<Id, Trace> = meta
            .traces
            .into_iter()
            .map(|meta| (meta.id, Trace::new(meta)))
            .collect();

        self.traces = restored_traces;
    }

    /// Attempt to commit the current in-memory metadata state to durable storage,
    /// and if not, revert back to a previous version.
    fn try_set_meta(&mut self) -> Result<(), Error> {
        let new_meta = self.serialize_meta();
        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        if let Err(e) = self.blob.set_meta(&new_meta) {
            // We were unable to durably commit the in-memory state. Revert back to the
            // previous version of meta.
            self.restore();
            return Err(e);
        } else {
            self.prev_meta = new_meta;
        }

        self.metrics
            .stream_count
            .set(u64::cast_from(self.prev_meta.id_mapping.len()));
        let unsealed_blob_count: usize = self
            .prev_meta
            .unsealeds
            .iter()
            .map(|x| x.batches.len())
            .sum();
        self.metrics
            .unsealed_blob_count
            .set(u64::cast_from(unsealed_blob_count));
        let unsealed_blob_bytes: u64 = self
            .prev_meta
            .unsealeds
            .iter()
            .flat_map(|x| x.batches.iter().map(|x| x.size_bytes))
            .sum();
        self.metrics.unsealed_blob_bytes.set(unsealed_blob_bytes);
        let trace_blob_count: usize = self.prev_meta.traces.iter().map(|x| x.batches.len()).sum();
        self.metrics
            .trace_blob_count
            .set(u64::cast_from(trace_blob_count));
        let trace_blob_bytes: u64 = self
            .prev_meta
            .traces
            .iter()
            .flat_map(|x| x.batches.iter().map(|x| x.size_bytes))
            .sum();
        self.metrics.trace_blob_bytes.set(trace_blob_bytes);

        Ok(())
    }

    /// Releases exclusive-writer locks and causes all future commands to error.
    ///
    /// This method is idempotent.
    pub fn close(&mut self) -> Result<(), Error> {
        // Make sure all the listener closures are dropped.
        self.listeners.clear();
        // Be careful to attempt to close both log and blob even if one of the
        // closes fails.
        let log_res = self.log.close();
        let blob_res = self.blob.close();
        log_res?;
        blob_res?;
        Ok(())
    }

    /// Creates, if necessary, a new unsealed and trace with the given external
    /// stream name, returning the corresponding internal stream id.
    ///
    /// This method is idempotent: ids may be registered multiple times.
    pub fn register(
        &mut self,
        id_str: &str,
        key_codec_name: &str,
        val_codec_name: &str,
        res: FutureHandle<Id>,
    ) {
        let resp = self.do_register(id_str, key_codec_name, val_codec_name);
        res.fill(resp);
    }

    fn do_register(
        &mut self,
        id_str: &str,
        key_codec_name: &str,
        val_codec_name: &str,
    ) -> Result<Id, Error> {
        self.drain_pending()?;
        if self.graveyard.iter().any(|r| r.name == id_str) {
            return Err(Error::from(format!(
                "invalid registration: stream {} already destroyed",
                id_str
            )));
        }

        let id = self.id_mapping.iter().find(|s| s.name == id_str);
        let id = match id {
            Some(s) => {
                if key_codec_name != s.key_codec_name {
                    return Err(Error::from(format!(
                        "invalid registration: key codec mismatch {} vs previous {}",
                        key_codec_name, s.key_codec_name
                    )));
                }
                if val_codec_name != s.val_codec_name {
                    return Err(Error::from(format!(
                        "invalid registration: val codec mismatch {} vs previous {}",
                        val_codec_name, s.val_codec_name
                    )));
                }
                s.id
            }
            None => {
                let id = self.next_stream_id;
                self.id_mapping.push(StreamRegistration {
                    name: id_str.to_owned(),
                    id,
                    key_codec_name: key_codec_name.to_owned(),
                    val_codec_name: val_codec_name.to_owned(),
                });
                self.next_stream_id = Id(id.0 + 1);
                id
            }
        };
        self.unsealeds
            .entry(id)
            .or_insert_with_key(|id| Unsealed::new(UnsealedMeta::new(*id)));
        self.traces
            .entry(id)
            .or_insert_with_key(|id| Trace::new(TraceMeta::new(*id)));
        self.try_set_meta()?;
        Ok(id)
    }

    /// Removes a stream from the index.
    ///
    /// This method is idempotent and may be called multiple times. It returns
    /// true if the stream was destroyed from this call, and false if it was
    /// already destroyed.
    pub fn destroy(&mut self, id_str: &str, res: FutureHandle<bool>) {
        let resp = self.do_destroy(id_str);
        res.fill(resp);
    }

    fn do_destroy(&mut self, id_str: &str) -> Result<bool, Error> {
        self.drain_pending()?;
        if self.graveyard.iter().any(|r| r.name == id_str) {
            return Ok(false);
        }

        let mapping = self.id_mapping.iter().find(|r| r.name == id_str);

        let mapping = match mapping {
            Some(mapping) => mapping.clone(),
            None => {
                return Err(Error::from(format!(
                    "invalid destroy of stream {} that was never registered or destroyed",
                    id_str
                )));
            }
        };

        self.id_mapping.retain(|r| r.name != id_str);

        // TODO: actually physically delete the unsealed and trace batches.
        let unsealed = self.unsealeds.remove(&mapping.id);
        let trace = self.traces.remove(&mapping.id);

        // Sanity check that we actually removed the unsealed and trace for this
        // stream.
        debug_assert!(unsealed.is_some());
        debug_assert!(trace.is_some());

        self.graveyard.push(mapping);

        self.try_set_meta()?;

        Ok(true)
    }

    /// Validates the following preconditions for draining pending requests:
    ///
    /// - The meta we might roll back to must be equal to the durably
    ///   persisted meta.
    #[cfg(any(debug_assertions, test))]
    fn validate_drain_pending_preconditions(&self) -> Result<(), Error> {
        // We can only check this invariant when blob is available, as otherwise
        // we fail to make progress on draining pending requests and writes during nemesis
        // tests.
        match self.blob.get_meta() {
            Ok(m) => {
                let persisted_meta = m.unwrap_or_default();
                if persisted_meta != self.prev_meta {
                    return Err(Error::from(format!(
                        "different prev {:?} and persisted metadata {:?}",
                        self.prev_meta, persisted_meta
                    )));
                }
            }
            Err(e) => {
                log::error!("unable to read back persisted metadata: {:?}", e);
            }
        }

        Ok(())
    }

    /// Validates the following postconditions for draining pending requests:
    ///
    /// - The meta we might roll back to must be equal to the durably
    ///   persisted meta.
    /// - There are no more pending responses or pending writes.
    #[cfg(any(debug_assertions, test))]
    fn validate_drain_pending_postconditions(&self) -> Result<(), Error> {
        // The postconditions are strictly more general than the preconditions so validate those as well.
        self.validate_drain_pending_preconditions()?;

        self.pending.validate_empty()?;
        Ok(())
    }

    /// Commit any pending in-memory changes to persistent storage, respond to clients
    /// and notify any listeners.
    fn drain_pending(&mut self) -> Result<(), Error> {
        #[cfg(any(debug_assertions, test))]
        {
            assert_eq!(self.validate_drain_pending_preconditions(), Ok(()));
        }
        let ret = match self.drain_pending_inner() {
            Ok(_) => {
                let mut responses = self.pending.take_responses();
                responses.drain(..).for_each(|r| r.fill());
                Ok(())
            }
            Err(e) => {
                let mut responses = self.pending.take_responses();
                self.metrics
                    .cmd_failed_count
                    .inc_by(u64::cast_from(responses.len()));
                responses.drain(..).for_each(|r| r.fill_err(e.clone()));
                Err(e)
            }
        };

        #[cfg(any(debug_assertions, test))]
        {
            assert_eq!(self.validate_drain_pending_postconditions(), Ok(()));
        }

        ret
    }

    fn compact_inner(&mut self) -> Result<(), Error> {
        let mut total_written_bytes = 0;
        let mut deleted_unsealed_batches = vec![];
        let mut deleted_trace_batches = vec![];
        for (id, trace) in self.traces.iter_mut() {
            let unsealed = self
                .unsealeds
                .get_mut(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
            deleted_unsealed_batches.extend(unsealed.truncate(trace.ts_upper())?);
            let (written_bytes, deleted_batches) = trace.step(&self.maintainer)?;
            total_written_bytes += written_bytes;
            deleted_trace_batches.extend(deleted_batches);
        }

        self.try_set_meta()?;

        if !deleted_unsealed_batches.is_empty() || !deleted_trace_batches.is_empty() {
            self.metrics.compaction_count.inc();
        }
        self.metrics
            .compaction_write_bytes
            .inc_by(total_written_bytes);

        // After we've committed our logical deletions to durable storage, we can
        // physically delete the data.
        //
        // TODO: if there's an error in the middle of the deletions then any
        // undeleted blobs will forever be orphaned. We could instead retain a
        // pending_deletes list but we would lose that across restarts unless we
        // wrote it to persistent storage. Alternatively, we should expose a list
        // method on blob and have a periodic cleanup task that attempts to find
        // and delete unused blobs. We could also use the list method to verify
        // that all referenced blobs exist.
        for batch in deleted_unsealed_batches {
            self.blob.delete_unsealed_batch(&batch)?;
        }

        for batch in deleted_trace_batches {
            self.blob.delete_trace_batch(&batch)?;
        }

        Ok(())
    }

    /// Compact all traces and truncate all unsealeds, if possible.
    ///
    /// TODO: currently we do not attempt to compact unsealed batches and instead
    /// logically delete them from unsealed after all updates contained within a
    /// given unsealed batch have been moved over to trace. This policy works fine
    /// assuming data mostly arrives in order, or not very far in advance of the
    /// currently sealed time. We will need to revisit the unsealed compaction if
    /// that assumption stops being true.
    fn compact(&mut self) -> Result<(), Error> {
        let compaction_start = Instant::now();

        let ret = match self.compact_inner() {
            Ok(_) => Ok(()),
            Err(e) => {
                self.restore();
                Err(e)
            }
        };

        self.metrics
            .compaction_ms
            .inc_by(metric_duration_ms(compaction_start.elapsed()));

        ret
    }

    /// Drains writes from the log into the unsealed and does any necessary
    /// resulting compaction work.
    ///
    /// In production, step should just be called in a loop (probably with some
    /// smarts about waiting to call it only after there have been some writes),
    /// but it's exposed this way so we can write deterministic tests.
    pub fn step(&mut self) -> Result<(), Error> {
        self.drain_pending()?;
        self.drain_unsealed()?;
        self.compact()?;
        Ok(())
    }

    fn validate_write(
        &mut self,
        updates: &[(Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>)],
    ) -> Result<SeqNo, Error> {
        for (id, updates) in updates.iter() {
            let sealed_frontier = self.sealed_frontier(*id)?;
            for update in updates.iter() {
                if !sealed_frontier.less_equal(&update.1) {
                    return Err(format!(
                        "update for {:?} with time {} before sealed frontier: {:?}",
                        id, update.1, sealed_frontier,
                    )
                    .into());
                }
            }
        }
        Ok(self.unsealeds_seqno_upper)
    }

    /// Asynchronously persists (Key, Value, Time, Diff) updates for the stream
    /// with the given id.
    pub fn write(
        &mut self,
        updates: Vec<(Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>)>,
        res: FutureHandle<SeqNo>,
    ) {
        let resp = self.validate_write(&updates);

        if resp.is_ok() {
            self.pending.add_writes(updates);
        }
        self.pending.add_response(PendingResponse::SeqNo(res, resp));
    }

    /// Drain pending writes to unsealed.
    ///
    /// The caller is responsible for commiting metadata after this succeeds, and
    /// restoring metadata if this fails.
    fn drain_pending_writes(
        &mut self,
        mut writes_by_id: HashMap<Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>>,
    ) -> Result<(), Error> {
        if writes_by_id.is_empty() {
            return Ok(());
        }
        // Give each write a unique, incrementing sequence number, and use
        // unsealeds_seqno_upper to track the sequence number of the next write.
        let write_seqno = self.unsealeds_seqno_upper;
        self.unsealeds_seqno_upper = SeqNo(write_seqno.0 + 1);

        // This range represents the [lower, upper) of sequence numbers assigned
        // to this write.
        //
        // TODO: do we still need sequence numbers? This will make more sense
        // when we send multiple writes to unsealed at once but I'm not sure if
        // we need the concept of sequence numbers when we're not reading from
        // a log. On the other hand, how would we distinguish unsealed batches
        // from each other?
        let desc = write_seqno..self.unsealeds_seqno_upper;
        for (id, writes) in writes_by_id.drain() {
            let unsealed = self
                .unsealeds
                .get_mut(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

            // We maintain the invariant that the sequence number chosen for the
            // write is >= every unsealed's seqno_upper and that there is nothing
            // for that unsealed in [unsealed.seqno_upper, write_seqno).
            let seqno_upper = unsealed.seqno_upper()[0];
            debug_assert!(seqno_upper <= write_seqno);

            // We can artificially start the Unsealed batch at the unsealed's current
            // seqno_upper to make the batches be contiguous in terms of sequence
            // numbers
            let mut desc = desc.clone();
            desc.start = seqno_upper;

            self.drain_pending_writes_inner(id, writes, &desc)?;
        }

        self.unsealeds_seqno_upper = desc.end;

        Ok(())
    }

    /// Drain pending writes to unsealed, commit in-memory state and notify any
    /// listeners.
    ///
    /// The caller is responsible for draining any pending responses after this.
    fn drain_pending_inner(&mut self) -> Result<(), Error> {
        let updates_by_id = self.pending.take_writes();
        let mut seals_by_id = self.pending.take_seals();

        let mut updates_for_listeners = updates_by_id.clone();
        if let Err(e) = self.drain_pending_writes(updates_by_id) {
            self.restore();
            return Err(format!("failed to append to unsealed: {}", e).into());
        }

        // TODO: only update meta if something has changed, instead of unconditionally.
        self.try_set_meta().map_err(|e| {
            format!(
                "failed to commit metadata after appending to unsealed: {}",
                e
            )
        })?;

        {
            let mut update_count = 0;
            let mut update_bytes = 0;
            for updates in updates_for_listeners.values() {
                update_count += updates.len();
                for ((k, v), _, _) in updates.iter() {
                    update_bytes += k.len() + v.len() + 8 + 8;
                }
            }
            self.metrics
                .cmd_write_record_count
                .inc_by(u64::cast_from(update_count));
            self.metrics
                .cmd_write_record_bytes
                .inc_by(u64::cast_from(update_bytes));
        }

        for (id, updates) in updates_for_listeners.drain() {
            if let Some(listen_fns) = self.listeners.get(&id) {
                if listen_fns.len() == 1 {
                    listen_fns[0](ListenEvent::Records(updates));
                } else {
                    for listen_fn in listen_fns.iter() {
                        listen_fn(ListenEvent::Records(updates.clone()));
                    }
                }
            }
        }

        for (id, seal) in seals_by_id.drain() {
            if let Some(listen_fns) = self.listeners.get(&id) {
                for listen_fn in listen_fns.iter() {
                    listen_fn(ListenEvent::Sealed(seal));
                }
            }
        }

        Ok(())
    }

    /// Construct a new [BlobUnsealedBatch] out of the provided `updates` and add
    /// it to the unsealed for `id`.
    ///
    /// The caller is responsible for updating META after they've finished
    /// updating unsealeds.
    fn drain_pending_writes_inner(
        &mut self,
        id: Id,
        mut updates: Vec<((Vec<u8>, Vec<u8>), u64, isize)>,
        desc: &Range<SeqNo>,
    ) -> Result<(), Error> {
        let mut updates: Vec<_> = updates
            .drain(..)
            .map(|((k, v), t, d)| (t, (k, v), d))
            .collect();
        // Unsealed batches are required to be sorted and consolidated by ((ts, (k, v)).
        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        if updates.is_empty() {
            return Ok(());
        }

        // Reshape updates back to the desired type.
        let updates: Vec<_> = updates
            .drain(..)
            .map(|(t, (k, v), d)| ((k, v), t, d))
            .collect();
        let batch = BlobUnsealedBatch {
            desc: Description::new(
                Antichain::from_elem(desc.start),
                Antichain::from_elem(desc.end),
                // We never compact Unsealed, so since is always the minimum.
                Antichain::from_elem(SeqNo(0)),
            ),
            updates,
        };
        self.append_unsealed(id, batch)?;

        Ok(())
    }

    /// Atomically moves all writes in unsealed not in advance of the trace's
    /// seal frontier into the trace and does any necessary resulting eviction
    /// work to remove uneccessary batches.
    fn drain_unsealed(&mut self) -> Result<(), Error> {
        let mut updates_by_id = vec![];
        for (id, trace) in self.traces.iter_mut() {
            // If this unsealed is already properly sealed then we don't need
            // to do anything.
            let seal = trace.get_seal();
            let trace_upper = trace.ts_upper();
            if seal == trace_upper {
                continue;
            }

            let unsealed = self
                .unsealeds
                .get_mut(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

            let desc = Description::new(
                trace_upper,
                seal.clone(),
                Antichain::from_elem(Timestamp::minimum()),
            );
            if PartialOrder::less_equal(desc.upper(), desc.lower()) {
                return Err(format!("invalid batch bounds: {:?}", desc).into());
            }

            // Move a batch of data from unsealed into trace by reading a
            // snapshot from unsealed...
            let snap = unsealed.snapshot(desc.lower().clone(), desc.upper().clone(), &self.blob)?;
            let mut updates = snap
                .into_iter()
                .collect::<Result<Vec<_>, Error>>()
                .map_err(|err| format!("failed to fetch snapshot: {}", err))?;

            // Don't bother minting empty trace batches that we'll just have to
            // compact later, it's wasteful of precious storage bandwidth and
            // everything works perfectly well when the trace upper hasn't yet
            // caught up to sealed.
            if updates.is_empty() {
                continue;
            }

            // Trace batches are required to be sorted and consolidated by ((k, v), t)
            differential_dataflow::consolidation::consolidate_updates(&mut updates);
            updates_by_id.push((*id, seal, updates.clone()));

            // ...and atomically swapping that snapshot's data into trace.
            let batch = BlobTraceBatch { desc, updates };
            if let Err(e) = trace.append(batch, &mut self.blob) {
                self.restore();
                return Err(format!("failed to append to trace: {}", e).into());
            }
        }

        // We need to update metadata before we do any notification or unsealed
        // truncation because that's the final step of ensuring that things
        // get appended to trace.
        self.try_set_meta()?;
        Ok(())
    }

    /// Returns the current "sealed" frontier for an id.
    ///
    /// This frontier represents a contract of time such that all updates with a
    /// time less than it have arrived. This frontier is advanced though the
    /// `seal` method. Once a time has been sealed for an id, it becomes an
    /// error to later seal it at an time less than the sealed frontier. It is
    /// also an error to write new data with a time less than the sealed frontier.
    fn sealed_frontier(&self, id: Id) -> Result<Antichain<u64>, Error> {
        let trace = self
            .traces
            .get(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        Ok(trace.get_seal())
    }

    /// Apply a seal command to in-memory state if it is valid.
    fn do_seal(&mut self, ids: &[Id], seal_ts: u64) -> Result<(), Error> {
        for id in ids.iter() {
            let trace = self
                .traces
                .get(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
            trace.validate_seal(seal_ts)?;
        }

        for id in ids.iter() {
            let trace = self.traces.get_mut(id).expect("trace known to exist");

            trace.update_seal(seal_ts);
        }
        Ok(())
    }

    /// Sealing a time advances the "sealed" frontier for an id, which restricts
    /// what times can later be sealed and written for that id. See
    /// `sealed_frontier` for details.
    pub fn seal(&mut self, ids: Vec<Id>, seal_ts: u64, res: FutureHandle<()>) {
        let resp = self.do_seal(&ids, seal_ts);
        if resp.is_ok() {
            self.pending.add_seals(ids, seal_ts);
        }
        self.pending.add_response(PendingResponse::Unit(res, resp));
    }

    fn do_allow_compaction(&mut self, id_sinces: Vec<(Id, Antichain<u64>)>) -> Result<(), Error> {
        for (id, since) in id_sinces.iter() {
            let trace = self
                .traces
                .get(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
            trace.validate_allow_compaction(since)?;
        }

        for (id, since) in id_sinces {
            let trace = self.traces.get_mut(&id).expect("trace known to exist");

            trace.allow_compaction(since);
        }
        Ok(())
    }

    /// Permit compaction of updates at times <= since to since.
    ///
    /// The compaction frontier can never decrease and it is an error to call
    /// this function with a since argument that is less than the current compaction
    /// frontier. It is also an error to advance the compaction frontier beyond the
    /// current sealed frontier.
    pub fn allow_compaction(
        &mut self,
        id_sinces: Vec<(Id, Antichain<u64>)>,
        res: FutureHandle<()>,
    ) {
        let response = self.do_allow_compaction(id_sinces);
        self.pending
            .add_response(PendingResponse::Unit(res, response));
    }

    /// Appends the given `batch` to the unsealed for `id`, writing the data into
    /// blob storage.
    ///
    /// The caller is responsible for updating META after they've finished
    /// updating unsealeds.
    fn append_unsealed(&mut self, id: Id, batch: BlobUnsealedBatch) -> Result<(), Error> {
        let unsealed = self
            .unsealeds
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        unsealed.append(batch, &mut self.blob)
    }

    fn serialize_meta(&self) -> BlobMeta {
        BlobMeta {
            next_stream_id: self.next_stream_id,
            unsealeds_seqno_upper: self.unsealeds_seqno_upper,
            id_mapping: self.id_mapping.clone(),
            graveyard: self.graveyard.clone(),
            unsealeds: self
                .unsealeds
                .iter()
                .map(|(_, unsealed)| unsealed.meta())
                .collect(),
            traces: self.traces.iter().map(|(_, trace)| trace.meta()).collect(),
        }
    }

    /// Returns a [Snapshot] for the given id.
    pub fn snapshot(&mut self, id: Id, res: FutureHandle<IndexedSnapshot>) {
        let resp = self.do_snapshot(id);
        res.fill(resp);
    }

    fn do_snapshot(&mut self, id: Id) -> Result<IndexedSnapshot, Error> {
        self.drain_pending()?;
        let unsealed = self
            .unsealeds
            .get(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        let trace = self
            .traces
            .get(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        let seal_frontier = trace.get_seal();
        let trace = trace.snapshot(&self.blob);
        let unsealed = unsealed.snapshot(trace.ts_upper.clone(), Antichain::new(), &self.blob)?;

        Ok(IndexedSnapshot(
            unsealed,
            trace,
            self.unsealeds_seqno_upper,
            seal_frontier,
        ))
    }

    /// Registers a callback to be invoked on successful writes and seals.
    //
    // Also returns a copy of the snapshot so that users can, if they want,
    // apply their logic to a consistent read of the entire stream.
    //
    // TODO: Finish the naming bikeshed for this. Other options so far include
    // tail, subscribe, tee, inspect, and capture.
    pub fn listen(
        &mut self,
        id: Id,
        listen_fn: ListenFn<Vec<u8>, Vec<u8>>,
        res: FutureHandle<IndexedSnapshot>,
    ) {
        let resp = self.do_listen(id, listen_fn);
        res.fill(resp);
    }

    fn do_listen(
        &mut self,
        id: Id,
        listen_fn: ListenFn<Vec<u8>, Vec<u8>>,
    ) -> Result<IndexedSnapshot, Error> {
        self.drain_pending()?;
        // Verify that id has been registered.
        let _ = self.sealed_frontier(id)?;
        self.listeners.entry(id).or_default().push(listen_fn);
        self.do_snapshot(id)
    }
}

/// An event in a persisted stream.
//
// TODO: This is similar to timely's capture Event but just different enough
// that I couldn't see how to use it directly. Revisit.
#[derive(Clone, Debug)]
pub enum ListenEvent<K, V> {
    /// Records in the data stream.
    Records(Vec<((K, V), u64, isize)>),
    /// Progress of the data stream.
    Sealed(u64),
}

/// The callback used by [Indexed::listen].
pub type ListenFn<K, V> = Box<dyn Fn(ListenEvent<K, V>) + Send>;

/// An isolated, consistent read of previously written (Key, Value, Time, Diff)
/// updates.
//
// TODO: This <K, V> allows Snapshot to be generic over both IndexedSnapshot
// (and friends) and DecodedSnapshot, but does that get us anything?
pub trait Snapshot<K, V>: Sized {
    /// The kind of iterator we are turning this into.
    type Iter: Iterator<Item = Result<((K, V), u64, isize), Error>>;

    /// Returns a set of `num_iters` [Iterator]s that each output roughly
    /// `1/num_iters` of the data represented by this snapshot.
    fn into_iters(self, num_iters: NonZeroUsize) -> Vec<Self::Iter>;

    /// Returns a single [Iterator] that outputs the data represented by this
    /// snapshot.
    fn into_iter(self) -> Self::Iter {
        let mut iters = self.into_iters(NonZeroUsize::new(1).unwrap());
        assert_eq!(iters.len(), 1);
        iters.remove(0)
    }
}

/// Extension methods on `Snapshot<K, V>` for use in tests.
#[cfg(test)]
pub trait SnapshotExt<K: Ord, V: Ord>: Snapshot<K, V> + Sized {
    /// A full read of the data in the snapshot.
    fn read_to_end(self) -> Result<Vec<((K, V), u64, isize)>, Error> {
        let iter = self.into_iter();
        let mut buf = iter.collect::<Result<Vec<_>, Error>>()?;
        buf.sort();
        Ok(buf)
    }
}

#[cfg(test)]
impl<K: Ord, V: Ord, S: Snapshot<K, V> + Sized> SnapshotExt<K, V> for S {}

/// A consistent snapshot of all data currently stored for an id.
#[derive(Debug)]
pub struct IndexedSnapshot(UnsealedSnapshot, TraceSnapshot, SeqNo, Antichain<u64>);

impl IndexedSnapshot {
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
    fn get_seal(&self) -> Antichain<u64> {
        self.3.clone()
    }
}

impl Snapshot<Vec<u8>, Vec<u8>> for IndexedSnapshot {
    type Iter = IndexedSnapshotIter;

    fn into_iters(self, num_iters: NonZeroUsize) -> Vec<IndexedSnapshotIter> {
        let since = self.since();
        let IndexedSnapshot(unsealed, trace, _, _) = self;
        let unsealed_iters = unsealed.into_iters(num_iters);
        let trace_iters = trace.into_iters(num_iters);
        // I don't love the non-debug asserts, but it doesn't seem worth it to
        // plumb an error around here.
        assert_eq!(unsealed_iters.len(), num_iters.get());
        assert_eq!(trace_iters.len(), num_iters.get());
        unsealed_iters
            .into_iter()
            .zip(trace_iters.into_iter())
            .map(|(unsealed_iter, trace_iter)| IndexedSnapshotIter {
                since: since.clone(),
                iter: trace_iter.chain(unsealed_iter),
            })
            .collect()
    }
}

/// An [Iterator] representing one part of the data in a [IndexedSnapshot].
//
// This intentionally chains trace before unsealed so we get the data in roughly
// increasing timestamp order, but it's unclear if this is in any way important.
pub struct IndexedSnapshotIter {
    since: Antichain<u64>,
    iter: std::iter::Chain<TraceSnapshotIter, UnsealedSnapshotIter>,
}

impl Iterator for IndexedSnapshotIter {
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
    use std::sync::mpsc;

    use crate::error::Error;
    use crate::future::Future;
    use crate::mem::MemRegistry;

    use super::*;

    fn block_on_drain<T, F: FnOnce(&mut Indexed<L, B>, FutureHandle<T>), L: Log, B: Blob>(
        index: &mut Indexed<L, B>,
        f: F,
    ) -> Result<T, Error> {
        let (tx, rx) = Future::new();
        f(index, tx.into());
        index.drain_pending()?;
        rx.recv()
    }

    fn block_on<T, F: FnOnce(FutureHandle<T>)>(f: F) -> Result<T, Error> {
        let (tx, rx) = Future::new();
        f(tx.into());
        rx.recv()
    }

    #[test]
    fn single_stream() -> Result<(), Error> {
        let updates = vec![
            (("1".into(), "".into()), 1, 1),
            (("2".into(), "".into()), 2, 1),
        ];

        let mut i = MemRegistry::new().indexed_no_reentrance()?;
        let id = block_on(|res| i.register("0", "()", "()", res))?;

        // Empty things are empty.
        let IndexedSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, vec![]);
        assert_eq!(seqno.0, 0);
        assert_eq!(seal_frontier.elements(), &[0]);

        // Register a listener for writes.
        let (listen_tx, listen_rx) = mpsc::channel();
        let listen_fn: ListenFn<Vec<u8>, Vec<u8>> = Box::new(move |e| match e {
            ListenEvent::Records(records) => {
                for ((k, v), ts, diff) in records.iter() {
                    listen_tx
                        .send(((k.clone(), v.clone()), *ts, *diff))
                        .expect("rx hasn't been dropped");
                }
            }
            ListenEvent::Sealed(_) => {}
        });
        block_on(|res| i.listen(id, listen_fn, res))?;

        // After a write, all data is in the unsealed.
        block_on_drain(&mut i, |i, handle| {
            i.write(vec![(id, updates.clone())], handle)
        })?;
        assert_eq!(block_on(|res| i.snapshot(id, res))?.read_to_end()?, updates);
        let IndexedSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, updates);
        assert_eq!(trace.read_to_end()?, vec![]);
        assert_eq!(seqno.0, 1);
        assert_eq!(seal_frontier.elements(), &[0]);

        // After a step, it's all still in the unsealed as nothing has been sealed
        // yet.
        i.step()?;
        assert_eq!(block_on(|res| i.snapshot(id, res))?.read_to_end()?, updates);
        let IndexedSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, updates);
        assert_eq!(trace.read_to_end()?, vec![]);
        assert_eq!(seqno.0, 1);
        assert_eq!(seal_frontier.elements(), &[0]);

        // After a seal and a step, the relevant data has moved into the trace
        // part of the index. Since we haven't sealed all the data, some of it
        // is still in the unsealed.
        block_on_drain(&mut i, |i, handle| i.seal(vec![id], 2, handle))?;
        i.step()?;
        assert_eq!(block_on(|res| i.snapshot(id, res))?.read_to_end()?, updates);
        let IndexedSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, updates[1..]);
        assert_eq!(trace.read_to_end()?, updates[..1]);
        assert_eq!(seqno.0, 1);
        assert_eq!(seal_frontier.elements(), &[2]);

        // All the data has been sealed, so it's now all in the trace.
        block_on_drain(&mut i, |i, handle| i.seal(vec![id], 3, handle))?;
        i.step()?;
        assert_eq!(block_on(|res| i.snapshot(id, res))?.read_to_end()?, updates);
        let IndexedSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, updates);
        assert_eq!(seqno.0, 1);
        assert_eq!(seal_frontier.elements(), &[3]);

        // Verify that the listener got a copy of the writes.
        let listen_received = {
            let mut buf = Vec::new();
            while let Ok(x) = listen_rx.try_recv() {
                buf.push(x);
            }
            buf
        };
        assert_eq!(listen_received, updates);

        // Can advance compaction frontier to a time that has already been sealed
        block_on_drain(&mut i, |i, handle| {
            i.allow_compaction(vec![(id, Antichain::from_elem(2))], handle)
        })?;

        Ok(())
    }

    #[test]
    fn batch_sorting() -> Result<(), Error> {
        let updates = vec![
            (("1".into(), "".into()), 2, 1),
            (("2".into(), "".into()), 1, 1),
        ];

        let mut i = MemRegistry::new().indexed_no_reentrance()?;
        let id = block_on(|res| i.register("0", "", "", res))?;

        // Write the data and move it into the unsealed part of the index, which
        // orders it within each batch by time. It's not, so this will fire a
        // validations error if the sort code doesn't work.
        block_on_drain(&mut i, |i, handle| {
            i.write(vec![(id, updates.clone())], handle)
        })?;

        // Now move it into the trace part of the index, which orders it within
        // each batch by key. It should currently be ordered by time, which
        // given the data is not ordered by key, so again this should fire a
        // validations error if the sort code doesn't work.
        block_on_drain(&mut i, |i, handle| i.seal(vec![id], 3, handle))?;
        i.step()?;

        // Sanity check that all the data made it into trace as expected.
        let IndexedSnapshot(unsealed, trace, _, _) = block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, updates);
        Ok(())
    }

    #[test]
    fn batch_consolidation() -> Result<(), Error> {
        let updates = vec![
            (("1".into(), "".into()), 1, 1),
            (("1".into(), "".into()), 1, 1),
        ];

        let mut i = MemRegistry::new().indexed_no_reentrance()?;
        let id = block_on(|res| i.register("0", "", "", res))?;

        // Write the data and move it into the unsealed part of the index, which
        // consolidates updates to identical ((k, v), t). Since the writes are
        // not already consolidated this test will fail if the consolidation
        // code does not work.
        block_on_drain(&mut i, |i, handle| {
            i.write(vec![(id, updates.clone())], handle)
        })?;

        // Add another set of identical updates and place into another unsealed
        // batch.
        block_on_drain(&mut i, |i, handle| {
            i.write(vec![(id, updates.clone())], handle)
        })?;

        // Now move the data to the trace part of the index, which consolidates
        // updates at identical ((k, v), t). Since the writes are only consolidated
        // within individual unsealed batches this test will fail if trace batch
        // consolidation does not work.
        block_on_drain(&mut i, |i, handle| i.seal(vec![id], 2, handle))?;
        i.step()?;

        // Sanity check that all the data made it into trace as expected.
        let IndexedSnapshot(unsealed, trace, _, _) = block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, vec![(("1".into(), "".into()), 1, 4)]);

        Ok(())
    }

    #[test]
    fn batch_unsealed_empty() -> Result<(), Error> {
        let mut i = MemRegistry::new().indexed_no_reentrance()?;
        let id = block_on(|res| i.register("0", "", "", res))?;

        // Write an empty set of updates and try to move it into the unsealed part
        // of the index.
        block_on_drain(&mut i, |i, handle| i.write(vec![(id, vec![])], handle))?;

        // Sending updates with dif = 0.
        let updates = vec![(("1".into(), "".into()), 1, 0)];
        block_on_drain(&mut i, |i, handle| i.write(vec![(id, updates)], handle))?;

        // Now try again with a set of updates that consolidates down to the empty
        // set.
        let updates = vec![
            (("1".into(), "".into()), 1, 2),
            (("1".into(), "".into()), 1, -2),
        ];

        block_on_drain(&mut i, |i, handle| i.write(vec![(id, updates)], handle))?;
        Ok(())
    }

    // Regression test for two similar bugs causing unsealed batches with
    // non-adjacent seqno boundaries (which violates our invariants).
    #[test]
    fn regression_non_sequential_unsealed_batches() -> Result<(), Error> {
        let mut i = MemRegistry::new().indexed_no_reentrance()?;

        // First is some stream is registered, written to, and step'd, moving
        // seqno 0..X into unsealed. Then a second stream is registered, written
        // to, and step'd. When it goes to move X..Y into the unsealed, the second
        // stream is missing a batch for 0..X. (Newly registered streams are
        // missing 0 to the seqno that log was at when they are registered.)
        //
        // This caused a violation of our invariants (which are checked in tests
        // and debug mode), so we just need the following to run without error
        // to verify the fix.
        let s1 = block_on(|res| i.register("s1", "", "", res))?;
        block_on_drain(&mut i, |i, handle| {
            i.write(vec![(s1, vec![(("".into(), "".into()), 0, 1)])], handle)
        })?;
        let s2 = block_on(|res| i.register("s2", "", "", res))?;
        block_on_drain(&mut i, |i, handle| {
            i.write(vec![(s2, vec![(("".into(), "".into()), 1, 1)])], handle)
        })?;

        // The second flavor is similar. If we then write to the first stream
        // again and step, it is then missing X..Y. (A stream not written to
        // between two step calls doesn't get a batch.)
        block_on_drain(&mut i, |i, handle| {
            i.write(vec![(s1, vec![(("".into(), "".into()), 2, 1)])], handle)
        })?;

        Ok(())
    }

    #[test]
    fn test_destroy() -> Result<(), Error> {
        let mut i = MemRegistry::new().indexed_no_reentrance()?;

        let _ = block_on(|res| i.register("stream", "", "", res))?;

        // Normal case: destroy registered stream.
        assert_eq!(block_on(|res| i.destroy("stream", res)), Ok(true));

        // Normal case: destroy already destroyed stream.
        assert_eq!(block_on(|res| i.destroy("stream", res)), Ok(false));

        // Destroy stream that was never created.
        assert_eq!(
            block_on(|res| i.destroy("stream2", res)),
            Err(Error::from(
                "invalid destroy of stream stream2 that was never registered or destroyed"
            ))
        );

        // Creating a previously destroyed stream.
        assert_eq!(
            block_on(|res| i.register("stream", "", "", res)),
            Err(Error::from(
                "invalid registration: stream stream already destroyed"
            ))
        );

        Ok(())
    }

    #[test]
    fn codec_mismatch() -> Result<(), Error> {
        let mut i = MemRegistry::new().indexed_no_reentrance()?;

        let _ = block_on(|res| i.register("stream", "key", "val", res))?;

        // Normal case: registration uses same key and value codec.
        let _ = block_on(|res| i.register("stream", "key", "val", res))?;

        // Different key codec
        assert_eq!(
            block_on(|res| i.register("stream", "nope", "val", res)),
            Err(Error::from(
                "invalid registration: key codec mismatch nope vs previous key"
            ))
        );

        // Different val codec
        assert_eq!(
            block_on(|res| i.register("stream", "key", "nope", res)),
            Err(Error::from(
                "invalid registration: val codec mismatch nope vs previous val"
            ))
        );

        Ok(())
    }

    #[test]
    fn regression_8303_snapshot_advance_since() -> Result<(), Error> {
        let mut i = MemRegistry::new().indexed_no_reentrance()?;
        let id = block_on(|res| i.register("0", "", "", res))?;

        // Introduce some data, seal it, and advance since. Intentionally don't
        // call step because might compact it and accidentally produce the right
        // answer (at the time of the bug, compaction did the right thing, which
        // is why we didn't catch it initially).
        let updates = vec![
            (("1".into(), "".into()), 1, 1),
            (("1".into(), "".into()), 10, -1),
            (("2".into(), "".into()), 2, 1),
        ];
        block_on_drain(&mut i, |i, res| i.write(vec![(id, updates)], res))?;
        block_on_drain(&mut i, |i, res| i.seal(vec![id], 4, res))?;
        block_on_drain(&mut i, |i, res| {
            i.allow_compaction(vec![(id, Antichain::from_elem(3))], res)
        })?;
        let snap = block_on(|res| i.snapshot(id, res))?;

        // Now verify that the snapshot has the right since and that the data in
        // it has been advanced as expected.
        assert_eq!(snap.since(), Antichain::from_elem(3));
        let actual = snap.read_to_end()?;
        let expected = vec![
            (("1".into(), "".into()), 3, 1),
            (("1".into(), "".into()), 10, -1),
            (("2".into(), "".into()), 3, 1),
        ];
        assert_eq!(actual, expected);

        Ok(())
    }
}
