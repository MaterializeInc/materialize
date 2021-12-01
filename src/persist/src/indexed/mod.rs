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
pub mod arrangement;
pub mod background;
pub mod cache;
pub mod columnar;
pub mod encoding;
pub mod metrics;
pub mod runtime;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::time::Instant;

use ore::cast::CastFrom;
use timely::progress::Antichain;

use crate::error::Error;
use crate::indexed::arrangement::{Arrangement, ArrangementSnapshot};
use crate::indexed::background::Maintainer;
use crate::indexed::cache::BlobCache;
use crate::indexed::columnar::ColumnarRecords;
use crate::indexed::encoding::{
    ArrangementMeta, BlobMeta, BlobUnsealedBatch, Id, StreamRegistration, TraceBatchMeta,
    UnsealedBatchMeta,
};
use crate::indexed::metrics::Metrics;
use crate::pfuture::PFutureHandle;
use crate::storage::{Blob, Log, SeqNo};

#[derive(Debug)]
enum PendingResponse {
    SeqNo(PFutureHandle<SeqNo>, Result<SeqNo, Error>),
}

impl PendingResponse {
    pub fn fill(self) {
        match self {
            PendingResponse::SeqNo(f, resp) => f.fill(resp),
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
        }
    }
}

/// This struct holds changes to [Indexed] that have not been committed to
/// persistent storage or sent to downstream listeners.
#[derive(Debug)]
struct Pending {
    writes: HashMap<Id, Vec<ColumnarRecords>>,
    responses: Vec<PendingResponse>,
    seals: HashMap<Id, u64>,
    durable_meta: BlobMeta,
}

impl Pending {
    fn new(durable_meta: BlobMeta) -> Self {
        Self {
            writes: HashMap::new(),
            responses: Vec::new(),
            seals: HashMap::new(),
            durable_meta,
        }
    }

    fn add_writes(&mut self, updates: Vec<(Id, ColumnarRecords)>) {
        for (id, updates) in updates {
            self.writes.entry(id).or_default().push(updates);
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
}

/// A persistent, compacting, indexed data structure of `(Key, Value, Time,
/// Diff)` updates.
///
/// Indexed contains a set of named persistent [Arrangement]s.
///
/// Notes:
/// - Requests are split into two types: _unbatched_ and _batched_. An unbatched
///   command is run entirely by itself (the applied state has just been written
///   to durable storage, then the command is run, then the resulting state is
///   immediately written to durable storage). A batched command is applied to
///   the machine state, but instead of immediately serializing the state to
///   storage, we buffer the command response in Pending. Any other batched
///   command can also be run and similarly buffered in Pending. Then, the next
///   time we get an unbatched command (or `step` is called), all pending
///   batched commands are made durable at once (and responses filled, listeners
///   updated, etc). This is a performance optimization to amortize the cost of
///   writing to durable storage across many of those requests. The most common
///   requests (write, seal, allow_compaction) are all _batched_ to exploit
///   this. All unbatched commands are expected to be relatively infrequent (to
///   avoid excessive barriers in our pipelining).
/// - When evaluating a request, the work of updating the state is given to
///   AppliedState (which has no knowledge of storage, etc). Then, if this was
///   successful, Indexed will serialize AppliedState and durably write it down.
#[derive(Debug)]
pub struct Indexed<L: Log, B: Blob> {
    // NB: we are not using Log for anything at the moment and instead have
    // all writes going directly to trace. At some point we'll need to revisit
    // what we want to do with Log, and whether we want it to live inside of
    // Indexed or somewhere else.
    log: L,
    blob: BlobCache<B>,
    maintainer: Maintainer<B>,
    listeners: HashMap<Id, Vec<ListenFn<Vec<u8>, Vec<u8>>>>,
    metrics: Metrics,
    state: AppliedState,
    pending: Option<Pending>,
}

/// The cumulative state that results from applying some prefix of the persist
/// state change log.
///
/// BlobMeta is the serialized version of exactly this state.
#[derive(Debug)]
struct AppliedState {
    saved_seqno: SeqNo,
    highest_assigned_seqno: SeqNo,
    // This is conceptually a map from `String` -> `Id`, but lookups are rare
    // and this representation is optimized for the metadata serialization path,
    // which is less rare.
    id_mapping: Vec<StreamRegistration>,
    graveyard: Vec<StreamRegistration>,
    arrangements: BTreeMap<Id, Arrangement>,
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
        let state = AppliedState::new(meta);
        let indexed = Indexed {
            log,
            blob,
            maintainer,
            listeners: HashMap::new(),
            metrics,
            state,
            pending: None,
        };

        Ok(indexed)
    }
}

impl AppliedState {
    fn new(meta: BlobMeta) -> Self {
        let arrangements = meta
            .arrangements
            .into_iter()
            .map(|x| (x.id, Arrangement::new(x)))
            .collect();
        AppliedState {
            saved_seqno: meta.seqno,
            highest_assigned_seqno: meta.seqno,
            id_mapping: meta.id_mapping,
            graveyard: meta.graveyard,
            arrangements,
        }
    }

    fn assign_seqno(&mut self) -> SeqNo {
        let seqno = self.highest_assigned_seqno + 1;
        self.highest_assigned_seqno = seqno;
        seqno
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    /// Serializes and attempt to commit the current in-memory AppliedState to
    /// durable storage, and if not, reverts back to the given previous version
    /// (which is expected to match what's in durable storage).
    ///
    /// Precondition: pending has been emptied
    fn try_set_meta(&mut self, prev_meta: BlobMeta) -> Result<(), Error> {
        // NB: This validate_pending_empty is intentionally a returned error
        // instead of an assert because it's a precondition (and so a violation
        // means a usage error by the caller of this).
        self.validate_pending_empty()?;
        debug_assert_eq!(
            Self::validate_matches_storage(&self.blob, &prev_meta),
            Ok(())
        );

        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        let new_meta = self.state.serialize_meta();
        if prev_meta == new_meta {
            // Since prev_meta is what's in storage, don't bother overwriting it
            // with exactly the same bytes. An alternative approach would be to
            // detect these cases earlier and avoid calling try_set_meta, but
            // those checks would be difficult to maintain (and bugs in them
            // would surface as either unnecessary storage usage or correctness
            // issues).
            return Ok(());
        }
        if let Err(e) = self.blob.set_meta(&new_meta) {
            // We were unable to durably commit the in-memory state. Revert back to the
            // previous version of meta.
            self.state = AppliedState::new(prev_meta);
            return Err(e);
        } else {
            self.state.saved_seqno = new_meta.seqno;
        }

        self.metrics
            .stream_count
            .set(u64::cast_from(new_meta.id_mapping.len()));
        let unsealed_blob_count: usize = new_meta
            .arrangements
            .iter()
            .map(|x| x.unsealed_batches.len())
            .sum();
        self.metrics
            .unsealed_blob_count
            .set(u64::cast_from(unsealed_blob_count));
        let unsealed_blob_bytes: u64 = new_meta
            .arrangements
            .iter()
            .flat_map(|x| x.unsealed_batches.iter().map(|x| x.size_bytes))
            .sum();
        self.metrics.unsealed_blob_bytes.set(unsealed_blob_bytes);
        let trace_blob_count: usize = new_meta
            .arrangements
            .iter()
            .map(|x| x.trace_batches.len())
            .sum();
        self.metrics
            .trace_blob_count
            .set(u64::cast_from(trace_blob_count));
        let trace_blob_bytes: u64 = new_meta
            .arrangements
            .iter()
            .flat_map(|x| x.trace_batches.iter().map(|x| x.size_bytes))
            .sum();
        self.metrics.trace_blob_bytes.set(trace_blob_bytes);

        Ok(())
    }

    /// Applies an unbatched cmd to the machine state and snapshots the result
    /// to durable storage.
    ///
    /// Precondition: pending has been emptied
    fn apply_unbatched_cmd<
        T,
        WorkFn: FnOnce(&mut AppliedState, &mut BlobCache<B>, &mut Maintainer<B>) -> Result<T, Error>,
    >(
        &mut self,
        work_fn: WorkFn,
    ) -> Result<T, Error> {
        debug_assert_eq!(self.validate(), Ok(()));
        // NB: This validate_pending_empty is intentionally a returned error
        // instead of an assert because it's a precondition (and so a violation
        // means a usage error by the caller of this).
        self.validate_pending_empty()?;

        let meta_before = self.state.serialize_meta();
        let work_ret = match work_fn(&mut self.state, &mut self.blob, &mut self.maintainer) {
            Ok(work_ret) => work_ret,
            Err(err) => {
                self.state = AppliedState::new(meta_before);
                return Err(err);
            }
        };
        self.try_set_meta(meta_before)?;

        debug_assert_eq!(self.validate_pending_empty(), Ok(()));
        debug_assert_eq!(self.validate(), Ok(()));
        Ok(work_ret)
    }

    fn apply_batched_cmd<WorkFn: FnOnce(&mut AppliedState, &mut Pending)>(
        &mut self,
        work_fn: WorkFn,
    ) {
        debug_assert_eq!(self.validate(), Ok(()));

        let pending = self.pending.get_or_insert_with(|| {
            let durable_meta = self.state.serialize_meta();
            debug_assert_eq!(
                Self::validate_matches_storage(&self.blob, &durable_meta),
                Ok(())
            );
            Pending::new(durable_meta)
        });
        work_fn(&mut self.state, pending);

        debug_assert_eq!(self.validate(), Ok(()));
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
        res: PFutureHandle<Id>,
    ) {
        res.fill((|| {
            self.drain_pending()?;
            self.apply_unbatched_cmd(|state, _, _| {
                state.do_register(id_str, key_codec_name, val_codec_name)
            })
        })());
    }
}

impl AppliedState {
    fn do_register(
        &mut self,
        id_str: &str,
        key_codec_name: &str,
        val_codec_name: &str,
    ) -> Result<Id, Error> {
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
                let id = self.serialize_meta().next_stream_id();
                self.id_mapping.push(StreamRegistration {
                    name: id_str.to_owned(),
                    id,
                    key_codec_name: key_codec_name.to_owned(),
                    val_codec_name: val_codec_name.to_owned(),
                });
                let arrangement = Arrangement::new(ArrangementMeta::new(id));
                if let Some(prev) = self.arrangements.insert(id, arrangement) {
                    return Err(format!(
                        "internal error: unexpected previous arrangement: {:?}",
                        prev
                    )
                    .into());
                }
                id
            }
        };
        Ok(id)
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    /// Removes a stream from the index.
    ///
    /// This method is idempotent and may be called multiple times. It returns
    /// true if the stream was destroyed from this call, and false if it was
    /// already destroyed.
    pub fn destroy(&mut self, id_str: &str, res: PFutureHandle<bool>) {
        res.fill((|| {
            self.drain_pending()?;
            self.apply_unbatched_cmd(|state, _, _| state.do_destroy(id_str))
        })());
    }
}

impl AppliedState {
    fn do_destroy(&mut self, id_str: &str) -> Result<bool, Error> {
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
        let arrangement = self.arrangements.remove(&mapping.id);

        // Sanity check that we actually removed the arrangement for this
        // stream.
        debug_assert!(arrangement.is_some());

        self.graveyard.push(mapping);

        Ok(true)
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    fn validate(&self) -> Result<(), Error> {
        if let Some(pending) = self.pending.as_ref() {
            Self::validate_matches_storage(&self.blob, &pending.durable_meta)?;
        }
        self.validate_referenced_keys_exist()?;
        Ok(())
    }

    /// Validates that the meta we might roll back to must be equal to the
    /// durably persisted meta.
    fn validate_matches_storage(blob: &BlobCache<B>, meta: &BlobMeta) -> Result<(), Error> {
        // We can only check this invariant when blob is available, as otherwise
        // we fail to make progress on draining pending requests and writes
        // during nemesis tests.
        match blob.get_meta() {
            Ok(m) => {
                let persisted_meta = m.unwrap_or_default();
                if &persisted_meta != meta {
                    return Err(Error::from(format!(
                        "meta {:?} did not match the one in storage {:?}",
                        meta, persisted_meta
                    )));
                }
            }
            Err(e) => {
                log::error!("unable to read back persisted metadata: {:?}", e);
            }
        }
        Ok(())
    }

    /// Validates that all of the referenced blob keys in all unsealeds and
    /// traces actually exist in blob's key-val map.
    fn validate_referenced_keys_exist(&self) -> Result<(), Error> {
        match self.blob.list_keys() {
            // Same as validate_matches_storage, we can only check this
            // invariant if blob is available.
            Ok(list) => {
                let mut keys = HashSet::new();
                keys.extend(list);
                let meta = self.state.serialize_meta();

                for arrangement in meta.arrangements.iter() {
                    for batch in arrangement.unsealed_batches.iter() {
                        if !keys.contains(&batch.key) {
                            return Err(Error::from("key missing in unsealed batch"));
                        }
                    }
                    for batch in arrangement.trace_batches.iter() {
                        if !keys.contains(&batch.key) {
                            return Err(Error::from("key missing in trace batch"));
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("unable to read back persisted metadata: {:?}", e);
            }
        }

        Ok(())
    }

    fn validate_pending_empty(&self) -> Result<(), Error> {
        if let Some(pending) = self.pending.as_ref() {
            return Err(Error::from(format!(
                "still have pending, expected None: {:?}",
                pending
            )));
        }
        Ok(())
    }

    /// Return true if Pending has at least one pending response.
    pub fn has_pending_responses(&self) -> bool {
        self.pending
            .as_ref()
            .map_or(false, |p| !p.responses.is_empty())
    }

    /// Commit any pending in-memory changes to persistent storage, respond to clients
    /// and notify any listeners.
    fn drain_pending(&mut self) -> Result<(), Error> {
        debug_assert_eq!(self.validate(), Ok(()));

        let pending = match self.pending.take() {
            Some(pending) => pending,
            None => return Ok(()),
        };

        let meta_before = pending.durable_meta;
        let updates_by_id = pending.writes;
        let seals_for_listeners = pending.seals;
        let updates_for_listeners = updates_by_id.clone();

        let ret = (|| {
            // TODO: The following error handling took a while to debug, see if
            // we can make this more obvious.
            if let Err(err) = self
                .state
                .drain_pending_writes(updates_by_id, &mut self.blob)
            {
                self.state = AppliedState::new(meta_before);
                Err(err)
            } else {
                self.try_set_meta(meta_before)
            }
        })();

        let ret = match ret {
            Ok(()) => {
                let mut responses = pending.responses;
                responses.drain(..).for_each(|r| r.fill());
                self.update_listeners(updates_for_listeners, seals_for_listeners);
                Ok(())
            }
            Err(e) => {
                let mut responses = pending.responses;
                self.metrics
                    .cmd_failed_count
                    .inc_by(u64::cast_from(responses.len()));
                responses.drain(..).for_each(|r| r.fill_err(e.clone()));
                Err(e)
            }
        };

        debug_assert_eq!(self.validate(), Ok(()));
        debug_assert_eq!(self.validate_pending_empty(), Ok(()));

        ret
    }
}

impl AppliedState {
    fn compact_inner<B: Blob>(
        &mut self,
        maintainer: &Maintainer<B>,
    ) -> Result<(u64, Vec<UnsealedBatchMeta>, Vec<TraceBatchMeta>), Error> {
        let mut total_written_bytes = 0;
        let mut deleted_unsealed_batches = vec![];
        let mut deleted_trace_batches = vec![];
        for arrangement in self.arrangements.values_mut() {
            deleted_unsealed_batches.extend(arrangement.unsealed_evict());
            let (written_bytes, deleted_batches) = arrangement.trace_step(maintainer)?;
            total_written_bytes += written_bytes;
            deleted_trace_batches.extend(deleted_batches);
        }
        Ok((
            total_written_bytes,
            deleted_unsealed_batches,
            deleted_trace_batches,
        ))
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    /// Compact all traces and truncate all unsealeds, if possible.
    ///
    /// Precondition: pending has been emptied
    ///
    /// TODO: currently we do not attempt to compact unsealed batches and instead
    /// logically delete them from unsealed after all updates contained within a
    /// given unsealed batch have been moved over to trace. This policy works fine
    /// assuming data mostly arrives in order, or not very far in advance of the
    /// currently sealed time. We will need to revisit the unsealed compaction if
    /// that assumption stops being true.
    fn compact(&mut self) -> Result<(), Error> {
        // NB: This validate_pending_empty is intentionally a returned error
        // instead of an assert because it's a precondition (and so a violation
        // means a usage error by the caller of this).
        self.validate_pending_empty()?;

        let compaction_start = Instant::now();
        let ret = self.apply_unbatched_cmd(|state, _, maintainer| state.compact_inner(maintainer));

        // Track compaction_seconds even if compaction failed.
        self.metrics
            .compaction_seconds
            .inc_by(compaction_start.elapsed().as_secs_f64());

        let (total_written_bytes, deleted_unsealed_batches, deleted_trace_batches) = ret?;
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

    /// Drains writes from the log into the unsealed and does any necessary
    /// resulting compaction work.
    ///
    /// In production, step should just be called in a loop (probably with some
    /// smarts about waiting to call it only after there have been some writes),
    /// but it's exposed this way so we can write deterministic tests.
    pub fn step(&mut self) -> Result<(), Error> {
        self.drain_pending()?;
        self.apply_unbatched_cmd(|state, blob, _| state.drain_unsealed(blob))?;
        self.compact()?;
        Ok(())
    }
}

impl AppliedState {
    fn validate_write(&mut self, updates: &[(Id, ColumnarRecords)]) -> Result<(), String> {
        for (id, updates) in updates.iter() {
            let sealed_frontier = self.sealed_frontier(*id)?;
            for update in updates.iter() {
                if !sealed_frontier.less_equal(&update.1) {
                    return Err(format!(
                        "update for {:?} with time {} before sealed frontier: {:?}",
                        id, update.1, sealed_frontier,
                    ));
                }
            }
        }
        Ok(())
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    /// Asynchronously persists (Key, Value, Time, Diff) updates for the stream
    /// with the given id.
    pub fn write(&mut self, updates: Vec<(Id, ColumnarRecords)>, res: PFutureHandle<SeqNo>) {
        self.apply_batched_cmd(|state, pending| {
            let seqno = state.assign_seqno();
            let resp = state
                .validate_write(&updates)
                .map(|_| seqno)
                .map_err(|err| Error::Noop(seqno, err));

            if resp.is_ok() {
                pending.add_writes(updates);
            }
            pending.add_response(PendingResponse::SeqNo(res, resp));
        })
    }
}

impl AppliedState {
    /// Drain pending writes to unsealed.
    ///
    /// The caller is responsible for commiting metadata after this succeeds, and
    /// restoring metadata if this fails.
    fn drain_pending_writes<B: Blob>(
        &mut self,
        mut writes_by_id: HashMap<Id, Vec<ColumnarRecords>>,
        blob: &mut BlobCache<B>,
    ) -> Result<(), Error> {
        if writes_by_id.is_empty() {
            return Ok(());
        }
        // This range represents the [lower, upper) of sequence numbers assigned
        // to this write.
        let desc = self.saved_seqno..self.highest_assigned_seqno;
        for (id, writes) in writes_by_id.drain() {
            let arrangement = self
                .arrangements
                .get_mut(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

            // We maintain the invariant that the sequence number chosen for the
            // write is >= every unsealed's seqno_upper and that there is nothing
            // for that unsealed in [unsealed.seqno_upper, write_seqno).
            let seqno_upper = arrangement.unsealed_seqno_upper();
            debug_assert!(seqno_upper <= desc.start);

            // We can artificially start the Unsealed batch at the unsealed's current
            // seqno_upper to make the batches be contiguous in terms of sequence
            // numbers
            let mut desc = desc.clone();
            desc.start = seqno_upper;

            self.drain_pending_writes_inner(id, writes, &desc, blob)?;
        }

        Ok(())
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    fn update_listeners(
        &self,
        updates: HashMap<Id, Vec<ColumnarRecords>>,
        seals: HashMap<Id, u64>,
    ) {
        {
            let mut update_count = 0;
            let mut update_bytes = 0;
            for updates_vec in updates.values() {
                for updates in updates_vec.iter() {
                    update_count += updates.len();
                    for ((k, v), _, _) in updates.iter() {
                        update_bytes += k.len() + v.len() + 8 + 8;
                    }
                }
            }
            self.metrics
                .cmd_write_record_count
                .inc_by(u64::cast_from(update_count));
            self.metrics
                .cmd_write_record_bytes
                .inc_by(u64::cast_from(update_bytes));
        }

        for (id, updates) in updates {
            if let Some(listen_fns) = self.listeners.get(&id) {
                if listen_fns.is_empty() {
                    continue;
                }

                let updates = updates
                    .iter()
                    .flat_map(|u| u.iter())
                    .map(|((k, v), ts, diff)| ((k.to_vec(), v.to_vec()), ts, diff))
                    .collect();

                if listen_fns.len() == 1 {
                    listen_fns[0].0(ListenEvent::Records(updates));
                } else {
                    for listen_fn in listen_fns.iter() {
                        listen_fn.0(ListenEvent::Records(updates.clone()));
                    }
                }
            }
        }

        for (id, seal) in seals {
            if let Some(listen_fns) = self.listeners.get(&id) {
                for listen_fn in listen_fns.iter() {
                    listen_fn.0(ListenEvent::Sealed(seal));
                }
            }
        }
    }
}

impl AppliedState {
    /// Construct a new [BlobUnsealedBatch] out of the provided `updates` and add
    /// it to the unsealed for `id`.
    ///
    /// The caller is responsible for updating META after they've finished
    /// updating unsealeds.
    fn drain_pending_writes_inner<B: Blob>(
        &mut self,
        id: Id,
        updates: Vec<ColumnarRecords>,
        desc: &Range<SeqNo>,
        blob: &mut BlobCache<B>,
    ) -> Result<(), Error> {
        if updates.is_empty() {
            return Ok(());
        }

        let batch = BlobUnsealedBatch {
            desc: desc.clone(),
            updates,
        };
        self.append_unsealed(id, batch, blob)?;

        Ok(())
    }

    /// Atomically moves all writes in unsealed not in advance of the trace's
    /// seal frontier into the trace and does any necessary resulting eviction
    /// work to remove unnecessary batches.
    fn drain_unsealed<B: Blob>(&mut self, blob: &mut BlobCache<B>) -> Result<(), Error> {
        for arrangement in self.arrangements.values_mut() {
            arrangement.unsealed_drain(blob)?;
        }
        Ok(())
    }

    /// Returns the current "sealed" frontier for an id.
    ///
    /// This frontier represents a contract of time such that all updates with a
    /// time less than it have arrived. This frontier is advanced though the
    /// `seal` method. Once a time has been sealed for an id, it becomes an
    /// error to later seal it at an time less than the sealed frontier. It is
    /// also an error to write new data with a time less than the sealed frontier.
    fn sealed_frontier(&self, id: Id) -> Result<Antichain<u64>, String> {
        let arrangement = self
            .arrangements
            .get(&id)
            .ok_or_else(|| format!("never registered: {:?}", id))?;
        Ok(arrangement.get_seal())
    }

    /// Apply a seal command to in-memory state if it is valid.
    fn do_seal(&mut self, ids: &[Id], seal_ts: u64) -> Result<(), String> {
        for id in ids.iter() {
            let arrangement = self
                .arrangements
                .get(&id)
                .ok_or_else(|| format!("never registered: {:?}", id))?;
            arrangement.validate_seal(seal_ts)?;
        }

        for id in ids.iter() {
            let arrangement = self.arrangements.get_mut(id).expect("trace known to exist");

            arrangement.update_seal(seal_ts);
        }
        Ok(())
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    /// Sealing a time advances the "sealed" frontier for an id, which restricts
    /// what times can later be sealed and written for that id. See
    /// `sealed_frontier` for details.
    pub fn seal(&mut self, ids: Vec<Id>, seal_ts: u64, res: PFutureHandle<SeqNo>) {
        self.apply_batched_cmd(|state, pending| {
            let seqno = state.assign_seqno();
            let resp = state
                .do_seal(&ids, seal_ts)
                .map(|_| seqno)
                .map_err(|err| Error::Noop(seqno, err));
            if resp.is_ok() {
                pending.add_seals(ids, seal_ts);
            }
            pending.add_response(PendingResponse::SeqNo(res, resp));
        })
    }
}

impl AppliedState {
    fn do_allow_compaction(&mut self, id_sinces: Vec<(Id, Antichain<u64>)>) -> Result<(), String> {
        for (id, since) in id_sinces.iter() {
            let arrangement = self
                .arrangements
                .get(&id)
                .ok_or_else(|| format!("never registered: {:?}", id))?;
            arrangement.validate_allow_compaction(since)?;
        }

        for (id, since) in id_sinces {
            let arrangement = self
                .arrangements
                .get_mut(&id)
                .expect("trace known to exist");

            arrangement.allow_compaction(since);
        }
        Ok(())
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    /// Permit compaction of updates at times <= since to since.
    ///
    /// The compaction frontier can never decrease and it is an error to call
    /// this function with a since argument that is less than the current compaction
    /// frontier. It is also an error to advance the compaction frontier beyond the
    /// current sealed frontier.
    pub fn allow_compaction(
        &mut self,
        id_sinces: Vec<(Id, Antichain<u64>)>,
        res: PFutureHandle<SeqNo>,
    ) {
        self.apply_batched_cmd(|state, pending| {
            let seqno = state.assign_seqno();
            let response = state
                .do_allow_compaction(id_sinces)
                .map(|_| seqno)
                .map_err(|err| Error::Noop(seqno, err));
            pending.add_response(PendingResponse::SeqNo(res, response));
        })
    }
}

impl AppliedState {
    /// Appends the given `batch` to the unsealed for `id`, writing the data into
    /// blob storage.
    ///
    /// The caller is responsible for updating META after they've finished
    /// updating unsealeds.
    fn append_unsealed<B: Blob>(
        &mut self,
        id: Id,
        batch: BlobUnsealedBatch,
        blob: &mut BlobCache<B>,
    ) -> Result<(), Error> {
        let arrangement = self
            .arrangements
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        arrangement.unsealed_append(batch, blob)
    }

    fn serialize_meta(&self) -> BlobMeta {
        BlobMeta {
            seqno: self.highest_assigned_seqno,
            id_mapping: self.id_mapping.clone(),
            graveyard: self.graveyard.clone(),
            arrangements: self.arrangements.values().map(|x| x.meta()).collect(),
        }
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    /// Returns a [Snapshot] for the given id.
    pub fn snapshot(&mut self, id: Id, res: PFutureHandle<ArrangementSnapshot>) {
        res.fill((|| {
            self.drain_pending()?;
            self.state.do_snapshot(id, &self.blob)
        })());
    }
}

impl AppliedState {
    fn do_snapshot<B: Blob>(
        &self,
        id: Id,
        blob: &BlobCache<B>,
    ) -> Result<ArrangementSnapshot, Error> {
        let arrangement = self
            .arrangements
            .get(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        let seqno = self.highest_assigned_seqno;
        arrangement.snapshot(seqno, blob)
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
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
        res: PFutureHandle<ArrangementSnapshot>,
    ) {
        res.fill((|| {
            self.drain_pending()?;
            self.do_listen(id, listen_fn)
        })());
    }

    fn do_listen(
        &mut self,
        id: Id,
        listen_fn: ListenFn<Vec<u8>, Vec<u8>>,
    ) -> Result<ArrangementSnapshot, Error> {
        // Verify that id has been registered.
        let _ = self.state.sealed_frontier(id)?;
        let snapshot = self.state.do_snapshot(id, &self.blob)?;
        // NB: Keep this line after anything with an early return (aka anything
        // fallible). Otherwise, we might register the listener internally, but
        // fail the request.
        self.listeners.entry(id).or_default().push(listen_fn);
        Ok(snapshot)
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
pub struct ListenFn<K, V>(pub Box<dyn Fn(ListenEvent<K, V>) + Send>);

impl<K, V> fmt::Debug for ListenFn<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListenFn").finish_non_exhaustive()
    }
}

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

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use crate::error::Error;
    use crate::indexed::SnapshotExt;
    use crate::mem::MemRegistry;
    use crate::pfuture::PFuture;
    use crate::unreliable::UnreliableHandle;

    use super::*;

    fn block_on_drain<T, F: FnOnce(&mut Indexed<L, B>, PFutureHandle<T>), L: Log, B: Blob>(
        index: &mut Indexed<L, B>,
        f: F,
    ) -> Result<T, Error> {
        let (tx, rx) = PFuture::new();
        f(index, tx.into());
        index.drain_pending()?;
        rx.recv()
    }

    fn block_on<T, F: FnOnce(PFutureHandle<T>)>(f: F) -> Result<T, Error> {
        let (tx, rx) = PFuture::new();
        f(tx.into());
        rx.recv()
    }

    #[test]
    fn single_stream() -> Result<(), Error> {
        let updates: Vec<((Vec<u8>, Vec<u8>), u64, isize)> = vec![
            (("1".into(), "".into()), 1, 1),
            (("2".into(), "".into()), 2, 1),
        ];

        let mut i = MemRegistry::new().indexed_no_reentrance()?;
        let id = block_on(|res| i.register("0", "()", "()", res))?;

        // Empty things are empty.
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, vec![]);
        assert_eq!(seqno.0, 0);
        assert_eq!(seal_frontier.elements(), &[0]);

        // Register a listener for writes.
        let (listen_tx, listen_rx) = mpsc::channel();
        let listen_fn: ListenFn<Vec<u8>, Vec<u8>> = ListenFn(Box::new(move |e| match e {
            ListenEvent::Records(records) => {
                for ((k, v), ts, diff) in records.iter() {
                    listen_tx
                        .send(((k.clone(), v.clone()), *ts, *diff))
                        .expect("rx hasn't been dropped");
                }
            }
            ListenEvent::Sealed(_) => {}
        }));
        block_on(|res| i.listen(id, listen_fn, res))?;

        // After a write, all data is in the unsealed.
        block_on_drain(&mut i, |i, handle| {
            i.write(
                vec![(id, updates.iter().collect::<ColumnarRecords>())],
                handle,
            )
        })?;
        assert_eq!(block_on(|res| i.snapshot(id, res))?.read_to_end()?, updates);
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, updates);
        assert_eq!(trace.read_to_end()?, vec![]);
        assert_eq!(seqno.0, 1);
        assert_eq!(seal_frontier.elements(), &[0]);

        // After a step, it's all still in the unsealed as nothing has been sealed
        // yet.
        i.step()?;
        assert_eq!(block_on(|res| i.snapshot(id, res))?.read_to_end()?, updates);
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
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
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, updates[1..]);
        assert_eq!(trace.read_to_end()?, updates[..1]);
        assert_eq!(seqno.0, 2);
        assert_eq!(seal_frontier.elements(), &[2]);

        // All the data has been sealed, so it's now all in the trace.
        block_on_drain(&mut i, |i, handle| i.seal(vec![id], 3, handle))?;
        i.step()?;
        assert_eq!(block_on(|res| i.snapshot(id, res))?.read_to_end()?, updates);
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, updates);
        assert_eq!(seqno.0, 3);
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
            i.write(
                vec![(id, updates.iter().collect::<ColumnarRecords>())],
                handle,
            )
        })?;

        // Now move it into the trace part of the index, which orders it within
        // each batch by key. It should currently be ordered by time, which
        // given the data is not ordered by key, so again this should fire a
        // validations error if the sort code doesn't work.
        block_on_drain(&mut i, |i, handle| i.seal(vec![id], 3, handle))?;
        i.step()?;

        // Sanity check that all the data made it into trace as expected.
        let ArrangementSnapshot(unsealed, trace, _, _) = block_on(|res| i.snapshot(id, res))?;
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

        // Write the data and move it into the unsealed part of the index.
        block_on_drain(&mut i, |i, handle| {
            i.write(
                vec![(id, updates.iter().collect::<ColumnarRecords>())],
                handle,
            )
        })?;

        // Add another set of identical updates and place into another unsealed
        // batch.
        block_on_drain(&mut i, |i, handle| {
            i.write(
                vec![(id, updates.iter().collect::<ColumnarRecords>())],
                handle,
            )
        })?;

        // Sanity check that the data is all in unsealed and none of it is in trace.
        let ArrangementSnapshot(unsealed, trace, _, _) = block_on(|res| i.snapshot(id, res))?;
        assert_eq!(
            unsealed.read_to_end()?,
            vec![
                (("1".into(), "".into()), 1, 1),
                (("1".into(), "".into()), 1, 1),
                (("1".into(), "".into()), 1, 1),
                (("1".into(), "".into()), 1, 1)
            ]
        );
        assert_eq!(trace.read_to_end()?, vec![]);

        // Now move the data to the trace part of the index, which consolidates
        // updates at identical ((k, v), t). Since the writes are unconsolidated
        // this test will fail if trace batch consolidation does not work.
        block_on_drain(&mut i, |i, handle| i.seal(vec![id], 2, handle))?;
        i.step()?;

        // Sanity check that all the data made it into trace as expected.
        let ArrangementSnapshot(unsealed, trace, _, _) = block_on(|res| i.snapshot(id, res))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, vec![(("1".into(), "".into()), 1, 4)]);

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
            i.write(
                vec![(
                    s1,
                    vec![(("".into(), "".into()), 0, 1)]
                        .iter()
                        .collect::<ColumnarRecords>(),
                )],
                handle,
            )
        })?;
        let s2 = block_on(|res| i.register("s2", "", "", res))?;
        block_on_drain(&mut i, |i, handle| {
            i.write(
                vec![(
                    s2,
                    vec![(("".into(), "".into()), 1, 1)]
                        .iter()
                        .collect::<ColumnarRecords>(),
                )],
                handle,
            )
        })?;

        // The second flavor is similar. If we then write to the first stream
        // again and step, it is then missing X..Y. (A stream not written to
        // between two step calls doesn't get a batch.)
        block_on_drain(&mut i, |i, handle| {
            i.write(
                vec![(
                    s1,
                    vec![(("".into(), "".into()), 2, 1)]
                        .iter()
                        .collect::<ColumnarRecords>(),
                )],
                handle,
            )
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

    /// Test that verifies a performance and write amplification optimization
    /// that avoids writing out to META if what we're writing matches what's
    /// already in storage.
    #[test]
    fn try_set_meta_matches_storage() -> Result<(), Error> {
        let updates = vec![
            (("1".into(), "".into()), 2, 1),
            (("2".into(), "".into()), 1, 1),
        ];

        let mut unreliable = UnreliableHandle::default();
        let mut i = MemRegistry::new().indexed_unreliable(unreliable.clone())?;
        let id = block_on(|res| i.register("0", "", "", res))?;

        // Write the data out but don't close it.
        block_on_drain(&mut i, |i, handle| {
            i.write(
                vec![(id, updates.iter().collect::<ColumnarRecords>())],
                handle,
            )
        })?;

        // We haven't closed the data, so nothing for step to do. If the
        // optimization works, this doesn't need storage.
        unreliable.make_unavailable();
        i.step()?;
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
        block_on_drain(&mut i, |i, res| {
            i.write(vec![(id, updates.iter().collect::<ColumnarRecords>())], res)
        })?;
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
