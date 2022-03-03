// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent, compacting, indexed data structure of `(Key, Value, Time,
//! i64)` updates.

// NB: These really don't need to be public, but the public doc lint is nice.
pub mod arrangement;
pub mod background;
pub mod cache;
pub mod columnar;
pub mod encoding;
pub mod metrics;
pub mod snapshot;

use std::any::TypeId;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use mz_ore::cast::CastFrom;
use timely::progress::Antichain;
use tracing::{error, trace, warn};

use crate::error::Error;
use crate::indexed::arrangement::Arrangement;
use crate::indexed::background::{CompactTraceReq, CompactTraceRes, Maintainer};
use crate::indexed::cache::BlobCache;
use crate::indexed::columnar::ColumnarRecords;
use crate::indexed::encoding::{
    ArrangementMeta, BlobMeta, BlobUnsealedBatch, Id, StreamRegistration, TraceBatchMeta,
    UnsealedBatchMeta,
};
use crate::indexed::metrics::Metrics;
use crate::indexed::snapshot::ArrangementSnapshot;
use crate::mem::MemBlob;
use crate::pfuture::{PFuture, PFutureHandle};
use crate::storage::{Blob, BlobRead, Log, SeqNo};
use crate::unreliable::UnreliableBlob;

/// The description of a persisted stream.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamDesc {
    /// The external name of this stream.
    pub name: String,
    /// The upper of this stream.
    pub upper: Antichain<u64>,
    /// The since of this stream.
    pub since: Antichain<u64>,
}

/// A read-only input to the persist state machine.
#[derive(Debug)]
pub enum CmdRead {
    /// Loads the arrangement with the given external stream name, returning the
    /// corresponding internal stream id.
    Load(String, (String, String), PFutureHandle<Option<Id>>),
    /// Returns the [StreamDesc] of every active (non-deleted) stream.
    GetDescriptions(PFutureHandle<HashMap<Id, StreamDesc>>),
    /// Returns a [crate::indexed::snapshot::Snapshot] for the given id.
    Snapshot(Id, PFutureHandle<ArrangementSnapshot>),
    /// Registers a callback to be invoked on successful writes and seals.
    ///
    /// Also returns a copy of the snapshot so that users can, if they want,
    /// apply their logic to a consistent read of the entire stream.
    Listen(
        Id,
        crossbeam_channel::Sender<ListenEvent>,
        PFutureHandle<ArrangementSnapshot>,
    ),
    /// Flush out any pending work and close the underlying storage, causing
    /// all future commands to error.
    ///
    /// If applicable, releases exclusive-writer locks.
    Stop(PFutureHandle<()>),
}

impl CmdRead {
    /// Fills self's response, if applicable, with Error::RuntimeShutdown.
    pub fn fill_err_runtime_shutdown(self) {
        match self {
            CmdRead::Load(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
            CmdRead::GetDescriptions(res) => res.fill(Err(Error::RuntimeShutdown)),
            CmdRead::Snapshot(_, res) => res.fill(Err(Error::RuntimeShutdown)),
            CmdRead::Listen(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
            CmdRead::Stop(res) => {
                // Already stopped: no-op.
                res.fill(Ok(()))
            }
        }
    }
}

/// An input to the persist state machine.
//
// NB: The set of these that aren't in the CmdRead variant is exactly the set of
// things that we'll serialize to Log when we add that back in.
#[derive(Debug)]
pub enum Cmd {
    /// Creates, if necessary, a new unsealed and trace with the given external
    /// stream name, returning the corresponding internal stream id.
    ///
    /// This is idempotent: names may be registered multiple times.
    Register(String, (String, String), PFutureHandle<Id>),
    /// Removes a stream from the index.
    ///
    /// This is idempotent and may be called multiple times. It returns true if
    /// the stream was destroyed from this call, and false if it was already
    /// destroyed.
    Destroy(String, PFutureHandle<bool>),
    /// Asynchronously persists (Key, Value, Time, i64) updates for the stream
    /// with the given id.
    Write(Vec<(Id, ColumnarRecords)>, PFutureHandle<SeqNo>),
    /// Sealing a time advances the "sealed" frontier for an id, which restricts
    /// what times can later be sealed and written for that id. See
    /// `sealed_frontier` for details.
    Seal(Vec<Id>, u64, PFutureHandle<SeqNo>),
    /// Permit compaction of updates at times <= since to since.
    ///
    /// The compaction frontier can never decrease and it is an error to call
    /// this function with a since argument that is less than the current
    /// compaction frontier.
    ///
    /// While it may seem counter-intuitive to advance the compaction frontier
    /// past the seal frontier, this is perfectly valid. It can happen when
    /// joining updates from one stream to updates from another stream, and we
    /// already know that the other stream is compacted further along. Allowing
    /// compaction on this, the first stream, then is saying that we are fine
    /// with losing historical detail, and that we already allow compaction of
    /// updates that are yet to come because we don't need them at their full
    /// resolution. A similar case is when we know that any outstanding queries
    /// have an `as_of` that is in the future of the seal: we can also
    /// pro-actively allow compaction of updates that did not yet arrive.
    AllowCompaction(Vec<(Id, Antichain<u64>)>, PFutureHandle<SeqNo>),
    /// A response from a previously sent maintenance request.
    Maintenance(MaintenanceRes),
    /// A read-only input to the persist state machine.
    Read(CmdRead),
}

impl Cmd {
    /// Fills self's response, if applicable, with Error::RuntimeShutdown.
    pub fn fill_err_runtime_shutdown(self) {
        match self {
            Cmd::Register(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
            Cmd::Destroy(_, res) => res.fill(Err(Error::RuntimeShutdown)),
            Cmd::Write(_, res) => res.fill(Err(Error::RuntimeShutdown)),
            Cmd::Seal(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
            Cmd::AllowCompaction(_, res) => res.fill(Err(Error::RuntimeShutdown)),
            Cmd::Maintenance(_) => {}
            Cmd::Read(cmd_read) => cmd_read.fill_err_runtime_shutdown(),
        }
    }
}

/// A request for some work e.g. trace compaction, that can be performed outside
/// of the main [Indexed] loop.
#[derive(Clone, Debug, PartialEq)]
pub enum MaintenanceReq {
    /// A request to compact a trace by merging together some immutable batches.
    CompactTrace((Id, CompactTraceReq)),
}

/// A future for some work that can be completed outside of the main [Indexed]
/// loop.
#[derive(Debug)]
pub enum MaintenanceFuture {
    /// A future to perform some trace compaction.
    CompactTrace((Id, PFuture<CompactTraceRes>)),
}

/// A response for some work that was completed outside of the main [Indexed] loop.
#[derive(Clone, Debug, PartialEq)]
pub enum MaintenanceRes {
    /// The results of performing some trace compaction by merging together some
    /// immutable trace batches.
    CompactTrace((Id, Result<CompactTraceRes, Error>)),
}

impl MaintenanceReq {
    /// Asynchronously runs the requested maintenance work on the work pool
    /// provided at construction time.
    ///
    /// Returns a [MaintenanceFuture] that can be used to get the results of the
    /// requested work.
    pub fn run_async<B: Blob>(self, maintainer: &Maintainer<B>) -> MaintenanceFuture {
        match self {
            MaintenanceReq::CompactTrace((id, req)) => {
                let fut = maintainer.compact_trace(req);
                MaintenanceFuture::CompactTrace((id, fut))
            }
        }
    }
}

impl MaintenanceFuture {
    /// Blocks and asynchronously receives the result for some previously requested
    /// maintenance work
    pub async fn recv(self) -> MaintenanceRes {
        match self {
            MaintenanceFuture::CompactTrace((id, fut)) => {
                MaintenanceRes::CompactTrace((id, fut.await))
            }
        }
    }
}

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
    deleted_trace_batches: Vec<Vec<TraceBatchMeta>>,
    durable_meta: BlobMeta,
}

impl Pending {
    fn new(durable_meta: BlobMeta) -> Self {
        Self {
            writes: HashMap::new(),
            responses: Vec::new(),
            seals: HashMap::new(),
            deleted_trace_batches: Vec::new(),
            durable_meta,
        }
    }

    // Add all non-empty writes to be persisted into an arrangement in the future.
    fn add_writes(&mut self, updates: Vec<(Id, ColumnarRecords)>) {
        for (id, updates) in updates {
            if updates.len() != 0 {
                self.writes.entry(id).or_default().push(updates);
            }
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

    fn add_deleted_trace_batches(&mut self, batches: Vec<TraceBatchMeta>) {
        self.deleted_trace_batches.push(batches);
    }
}

/// A persistent, compacting, indexed data structure of `(Key, Value, Time,
/// i64)` updates.
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
pub struct Indexed<L: Log, B: BlobRead> {
    // NB: we are not using Log for anything at the moment and instead have
    // all writes going directly to trace. At some point we'll need to revisit
    // what we want to do with Log, and whether we want it to live inside of
    // Indexed or somewhere else.
    log: L,
    blob: BlobCache<B>,
    listeners: HashMap<Id, Vec<crossbeam_channel::Sender<ListenEvent>>>,
    metrics: Arc<Metrics>,
    state: AppliedState,
    in_flight_trace_compactions: HashMap<Id, CompactTraceReq>,
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

impl<L: Log, B: BlobRead> Indexed<L, B> {
    /// Returns a new Indexed, initializing each Unsealed and Trace with the
    /// existing data for them in the blob storage, if any.
    pub fn new(mut log: L, mut blob: BlobCache<B>, metrics: Arc<Metrics>) -> Result<Self, Error> {
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
                    warn!("error closing log: {}", err);
                }
                if let Err(err) = blob.close() {
                    warn!("error closing blob: {}", err);
                }
                err
            })?
            .unwrap_or_default();
        let state = AppliedState::new(meta);
        let indexed = Indexed {
            log,
            blob,
            listeners: HashMap::new(),
            metrics,
            state,
            in_flight_trace_compactions: HashMap::new(),
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
        WorkFn: FnOnce(&mut AppliedState, &mut BlobCache<B>) -> Result<T, Error>,
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
        let work_ret = match work_fn(&mut self.state, &mut self.blob) {
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

    // A test helper for Cmd::Register.
    #[cfg(test)]
    fn register(
        &mut self,
        name: &str,
        key_codec_name: &str,
        val_codec_name: &str,
        res: PFutureHandle<Id>,
    ) -> bool {
        self.apply(Cmd::Register(
            name.into(),
            (key_codec_name.to_owned(), val_codec_name.to_owned()),
            res,
        ))
    }

    /// Applies the given input to the state machine.
    ///
    /// Returns false to indicate a graceful shutdown, true otherwise.
    pub fn apply(&mut self, cmd: Cmd) -> bool {
        // TODO: Should this metric be made more general so we count every
        // command type?
        if let Cmd::Write(..) = &cmd {
            self.metrics.cmd_write_count.inc();
        }
        match cmd {
            // Commands that can be batched.
            Cmd::Write(updates, res) => self.apply_batched_cmd(|state, pending| {
                let seqno = state.assign_seqno();
                let resp = state
                    .validate_write(&updates)
                    .map(|_| seqno)
                    .map_err(|err| Error::Noop(seqno, err));

                if resp.is_ok() {
                    pending.add_writes(updates);
                }
                pending.add_response(PendingResponse::SeqNo(res, resp));
            }),
            Cmd::Seal(ids, seal_ts, res) => self.apply_batched_cmd(|state, pending| {
                let seqno = state.assign_seqno();
                let resp = state
                    .do_seal(&ids, seal_ts)
                    .map(|_| seqno)
                    .map_err(|err| Error::Noop(seqno, err));
                if resp.is_ok() {
                    pending.add_seals(ids, seal_ts);
                }
                pending.add_response(PendingResponse::SeqNo(res, resp));
            }),
            Cmd::AllowCompaction(id_sinces, res) => self.apply_batched_cmd(|state, pending| {
                let seqno = state.assign_seqno();
                let resp = state
                    .do_allow_compaction(id_sinces)
                    .map(|_| seqno)
                    .map_err(|err| Error::Noop(seqno, err));
                pending.add_response(PendingResponse::SeqNo(res, resp));
            }),
            Cmd::Maintenance(res) => self.handle_maintenance_res(res),

            // Commands that cannot be batched.
            Cmd::Register(id, (key_codec_name, val_codec_name), res) => res.fill((|| {
                self.drain_pending()?;
                self.apply_unbatched_cmd(|state, _| {
                    state.do_register(&id, &key_codec_name, &val_codec_name)
                })
            })()),
            Cmd::Destroy(name, res) => res.fill((|| {
                self.drain_pending()?;
                self.apply_unbatched_cmd(|state, _| state.do_destroy(&name))
            })()),

            // Commands that are read-only.
            Cmd::Read(CmdRead::Load(name, (key_codec_name, val_codec_name), res)) => {
                res.fill((|| {
                    self.drain_pending()?;
                    self.state.do_load(&name, &key_codec_name, &val_codec_name)
                })())
            }
            Cmd::Read(CmdRead::GetDescriptions(res)) => res.fill((|| {
                self.drain_pending()?;
                self.state.do_get_descriptions()
            })()),
            Cmd::Read(CmdRead::Snapshot(id, res)) => res.fill((|| {
                self.drain_pending()?;
                self.state.do_snapshot(id, &self.blob)
            })()),
            Cmd::Read(CmdRead::Listen(id, sender, res)) => res.fill((|| {
                self.drain_pending()?;
                self.do_listen(id, sender)
            })()),

            // Stop is special, it's the only one that indicates future work is
            // impossible (return false).
            Cmd::Read(CmdRead::Stop(res)) => {
                // Finish up any pending work that we can before closing, but
                // don't bother kicking off any maintenance requests.
                self.step_or_log();
                res.fill(self.close());
                return false;
            }
        }
        true
    }
}

impl<L: Log, B: BlobRead> Indexed<L, B> {
    /// Applies the given read-only input to the state machine.
    ///
    /// Returns false to indicate a graceful shutdown, true otherwise.
    pub fn apply_read(&mut self, cmd: CmdRead) -> bool {
        // TODO: It'd be nice to share code between this and apply, but it's not
        // yet clear how to do that in a reasonable way.
        debug_assert_eq!(self.validate_pending_empty(), Ok(()));
        match cmd {
            CmdRead::Load(name, (key_codec_name, val_codec_name), res) => res.fill({
                // We validated that pending was empty above.
                self.state.do_load(&name, &key_codec_name, &val_codec_name)
            }),
            CmdRead::GetDescriptions(res) => res.fill({
                // We validated that pending was empty above.
                self.state.do_get_descriptions()
            }),
            CmdRead::Snapshot(id, res) => res.fill({
                // We validated that pending was empty above.
                self.state.do_snapshot(id, &self.blob)
            }),
            CmdRead::Listen(id, listen_fn, res) => res.fill({
                // We validated that pending was empty above.
                self.do_listen(id, listen_fn)
            }),

            // Stop is special, it's the only one that indicates future work is
            // impossible (return false).
            CmdRead::Stop(res) => {
                res.fill(Ok(()));
                return false;
            }
        }
        true
    }
}

impl AppliedState {
    fn do_load(
        &mut self,
        id_str: &str,
        key_codec_name: &str,
        val_codec_name: &str,
    ) -> Result<Option<Id>, Error> {
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
                Some(s.id)
            }
            None => None,
        };
        Ok(id)
    }

    fn do_register(
        &mut self,
        id_str: &str,
        key_codec_name: &str,
        val_codec_name: &str,
    ) -> Result<Id, Error> {
        let id = match self.do_load(id_str, key_codec_name, val_codec_name)? {
            Some(id) => id,
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

impl<L: Log, B: BlobRead> Indexed<L, B> {
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
                if TypeId::of::<B>() == TypeId::of::<UnreliableBlob<MemBlob>>() {
                    // This is a test and we've almost certainly used
                    // UnreliableBlob to make storage unavailable, log it at a
                    // lower level to keep test output a little less spammy.
                    trace!("unable to read back persisted metadata: {:?}", e);
                } else {
                    error!("unable to read back persisted metadata: {:?}", e);
                }
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
                        for key in batch.keys.iter() {
                            if !keys.contains(key) {
                                return Err(Error::from("key missing in trace batch"));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                if TypeId::of::<B>() == TypeId::of::<UnreliableBlob<MemBlob>>() {
                    // This is a test and we've almost certainly used
                    // UnreliableBlob to make storage unavailable, log it at a
                    // lower level to keep test output a little less spammy.
                    trace!("unable to read back persisted metadata: {:?}", e);
                } else {
                    error!("unable to read back persisted metadata: {:?}", e);
                }
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
}

impl<L: Log, B: Blob> Indexed<L, B> {
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

        let ret = {
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
        };

        let ret = match ret {
            Ok(()) => {
                let mut responses = pending.responses;
                responses.drain(..).for_each(|r| r.fill());
                self.update_listeners(updates_for_listeners, seals_for_listeners);

                // We can physically delete these trace batches because we successfully
                // committed the logical deletes to meta.
                let deleted_trace_batches = pending.deleted_trace_batches;
                for batches in deleted_trace_batches {
                    self.metrics.compaction_count.inc();
                    for batch in batches {
                        self.blob.delete_trace_batch(&batch)?;
                    }
                }

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
    fn compact_unsealed(&mut self) -> Result<Vec<UnsealedBatchMeta>, Error> {
        let mut deleted_unsealed_batches = vec![];
        for arrangement in self.arrangements.values_mut() {
            deleted_unsealed_batches.extend(arrangement.unsealed_evict());
        }
        Ok(deleted_unsealed_batches)
    }
}

impl<L: Log, B: Blob> Indexed<L, B> {
    /// Truncate all unsealeds, as much as possible.
    ///
    /// Precondition: pending has been emptied
    ///
    /// TODO: currently we do not attempt to compact unsealed batches and instead
    /// logically delete them from unsealed after all updates contained within a
    /// given unsealed batch have been moved over to trace. This policy works fine
    /// assuming data mostly arrives in order, or not very far in advance of the
    /// currently sealed time. We will need to revisit the unsealed compaction if
    /// that assumption stops being true.
    fn compact_unsealed(&mut self) -> Result<(), Error> {
        // NB: This validate_pending_empty is intentionally a returned error
        // instead of an assert because it's a precondition (and so a violation
        // means a usage error by the caller of this).
        self.validate_pending_empty()?;

        let compaction_start = Instant::now();
        let ret = self.apply_unbatched_cmd(|state, _| state.compact_unsealed());

        // Track compaction_seconds even if compaction failed.
        self.metrics
            .compaction_seconds
            .inc_by(compaction_start.elapsed().as_secs_f64());

        let deleted_unsealed_batches = ret?;
        if !deleted_unsealed_batches.is_empty() {
            self.metrics.compaction_count.inc();
        }

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

        Ok(())
    }

    fn compact_trace_maintenance_reqs(&mut self) -> Result<Vec<MaintenanceReq>, Error> {
        let mut reqs = vec![];
        for (id, arrangement) in self.state.arrangements.iter() {
            // Skip over the arrangement if it already has a compaction request in flight.
            //
            // TODO: note that this works because right now the connection between Indexed
            // and Maintainer is infallible, and the two cannot fail independently of each
            // other. If that changes, we will have to revisit the policy around waiting
            // arbitrarily long for maintenance responses and most likely institute
            // a timeout.
            if self.in_flight_trace_compactions.contains_key(&id) {
                self.metrics.trace_compaction_skipped_count.inc();
                continue;
            }
            if let Some(req) = arrangement.trace_next_compact_req()? {
                reqs.push(MaintenanceReq::CompactTrace((*id, req.clone())));
                self.in_flight_trace_compactions.insert(*id, req);
            }
        }
        Ok(reqs)
    }

    /// Handle the results of a prior request to compact a trace.
    fn handle_compact_trace_res(&mut self, id: Id, res: Result<CompactTraceRes, Error>) {
        // Check that the request belongs to a stream with currently in-flight
        // compaction requests, and matches the request currently in flight for
        // that stream. We expect this condition to always be true while the connection
        // between Indexed and Maintainer is infallible.
        //
        // TODO: can we get rid of this check and simply attempt to handle the response
        // we've received?
        let response_expected = {
            match (self.in_flight_trace_compactions.get(&id), &res) {
                (Some(req), Ok(res)) => req == &res.req,
                (Some(_), Err(_)) => {
                    // TODO: if the error response also contained the original
                    // request, we could make sure to only handle error responses
                    // that match the currently in-flight request.
                    true
                }
                (None, _) => false,
            }
        };

        debug_assert!(
            response_expected,
            "unexpected compaction response for stream: {:?}",
            id
        );
        if !response_expected {
            return;
        }

        self.in_flight_trace_compactions.remove(&id);

        // Ignore the response if it corresponds to an error encountered while
        // trying to compact the stream. The next call to compact_trace_maintenance_reqs
        // will retry the compaction request.
        let response = match res {
            Ok(res) => res,
            Err(_) => {
                self.metrics.trace_compaction_error_response_count.inc();
                return;
            }
        };

        let written_bytes = response.merged.size_bytes;
        self.metrics.compaction_write_bytes.inc_by(written_bytes);

        self.apply_batched_cmd(|state, pending| {
            // Ignore the response if it belongs to a stream that has since been
            // deleted.
            let arrangement = match state.arrangements.get_mut(&id) {
                Some(arrangement) => arrangement,
                None => return,
            };
            let deleted_trace_batches = arrangement.trace_handle_compact_response(response);
            if !deleted_trace_batches.is_empty() {
                pending.add_deleted_trace_batches(deleted_trace_batches);
            }
        });
    }

    /// Handle the results of a previously sent maintenance request.
    fn handle_maintenance_res(&mut self, res: MaintenanceRes) {
        match res {
            MaintenanceRes::CompactTrace((id, res)) => self.handle_compact_trace_res(id, res),
        };
    }

    fn step(&mut self) -> Result<Vec<MaintenanceReq>, Error> {
        self.drain_pending()?;
        self.apply_unbatched_cmd(|state, blob| state.drain_unsealed(blob))?;
        self.compact_unsealed()?;
        self.compact_trace_maintenance_reqs()
    }

    /// Drains writes from the log into the unsealed and does any necessary
    /// resulting compaction work.
    ///
    /// In production, step_or_log should just be called in a loop (probably
    /// with some smarts about waiting to call it only after there have been
    /// some writes), but it's exposed this way so we can write deterministic
    /// tests.
    pub fn step_or_log(&mut self) -> Vec<MaintenanceReq> {
        match self.step() {
            Err(e) => {
                self.metrics.cmd_step_error_count.inc();
                // TODO: revisit whether we need to move this to a different log level
                // depending on how spammy it ends up being. Alternatively, we
                // may want to rate-limit our logging here.
                warn!("error running step: {:?}", e);
                vec![]
            }
            Ok(reqs) => reqs,
        }
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

    /// Drain pending writes to unsealed.
    ///
    /// The caller is responsible for committing metadata after this succeeds,
    /// and restoring metadata if this fails.
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
            if let Some(listeners) = self.listeners.get(&id) {
                if listeners.is_empty() {
                    continue;
                }

                let updates = updates
                    .iter()
                    .flat_map(|u| u.iter())
                    .map(|((k, v), ts, diff)| ((k.to_vec(), v.to_vec()), ts, diff))
                    .collect();

                // TODO: remove the listener if it hangs up.
                if listeners.len() == 1 {
                    let events = ListenEvent::Records(updates);
                    if let Err(crossbeam_channel::SendError(_)) = listeners[0].send(events) {}
                } else {
                    for listener in listeners.iter() {
                        let events = ListenEvent::Records(updates.clone());
                        if let Err(crossbeam_channel::SendError(_)) = listener.send(events) {}
                    }
                }
            }
        }

        for (id, seal) in seals {
            if let Some(listeners) = self.listeners.get(&id) {
                for listener in listeners.iter() {
                    let seal_event = ListenEvent::Sealed(seal);
                    if let Err(crossbeam_channel::SendError(_)) = listener.send(seal_event) {}
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

        // Sanity check the invariant that only non-empty writes get appended to
        // unsealed.
        if cfg!(debug_assertions) {
            for update in updates.iter() {
                assert!(update.len() > 0);
            }
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

    fn do_get_descriptions(&self) -> Result<HashMap<Id, StreamDesc>, Error> {
        let mut descs = HashMap::with_capacity(self.id_mapping.len());
        for x in self.id_mapping.iter() {
            let arrangement = self.arrangements.get(&x.id).expect("missing arrangement");
            let desc = StreamDesc {
                name: x.name.clone(),
                upper: arrangement.get_seal(),
                since: arrangement.since(),
            };
            descs.insert(x.id, desc);
        }
        Ok(descs)
    }

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

    fn do_snapshot<B: BlobRead>(
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

impl<L: Log, B: BlobRead> Indexed<L, B> {
    fn do_listen(
        &mut self,
        id: Id,
        sender: crossbeam_channel::Sender<ListenEvent>,
    ) -> Result<ArrangementSnapshot, Error> {
        // Verify that id has been registered.
        let _ = self.state.sealed_frontier(id)?;
        let snapshot = self.state.do_snapshot(id, &self.blob)?;
        // NB: Keep this line after anything with an early return (aka anything
        // fallible). Otherwise, we might register the listener internally, but
        // fail the request.
        self.listeners.entry(id).or_default().push(sender);
        Ok(snapshot)
    }
}

/// An event in a persisted stream.
//
// TODO: This is similar to timely's capture Event but just different enough
// that I couldn't see how to use it directly. Revisit.
#[derive(Clone, Debug, PartialEq)]
pub enum ListenEvent {
    /// Records in the data stream.
    Records(Vec<((Vec<u8>, Vec<u8>), u64, i64)>),
    /// Progress of the data stream.
    Sealed(u64),
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use tokio::runtime::Runtime as AsyncRuntime;

    use crate::error::Error;
    use crate::indexed::columnar::ColumnarRecordsVec;
    use crate::indexed::snapshot::SnapshotExt;
    use crate::mem::{MemBlob, MemLog, MemRegistry};
    use crate::pfuture::PFuture;
    use crate::unreliable::{UnreliableBlob, UnreliableHandle, UnreliableLog};

    use super::*;

    fn block_on_drain<
        T,
        F: FnOnce(&mut Indexed<L, B>, PFutureHandle<T>) -> bool,
        L: Log,
        B: Blob,
    >(
        index: &mut Indexed<L, B>,
        f: F,
    ) -> Result<T, Error> {
        let (tx, rx) = PFuture::new();
        debug_assert!(f(index, tx));
        index.drain_pending()?;
        rx.recv()
    }

    fn block_on<T, F: FnOnce(PFutureHandle<T>) -> bool>(f: F) -> Result<T, Error> {
        let (tx, rx) = PFuture::new();
        debug_assert!(f(tx));
        rx.recv()
    }

    fn write_req_payload(
        id: Id,
        updates: &[((Vec<u8>, Vec<u8>), u64, i64)],
    ) -> Vec<(Id, ColumnarRecords)> {
        updates
            .iter()
            .collect::<ColumnarRecordsVec>()
            .into_inner()
            .into_iter()
            .map(|x| (id, x))
            .collect()
    }

    fn indexed_and_maintainer(
        unreliable: UnreliableHandle,
    ) -> Result<
        (
            Indexed<UnreliableLog<MemLog>, UnreliableBlob<MemBlob>>,
            Maintainer<UnreliableBlob<MemBlob>>,
        ),
        Error,
    > {
        let registry = MemRegistry::new();
        let log = registry.log_no_reentrance()?;
        let log = UnreliableLog::from_handle(log, unreliable.clone());
        let metrics = Arc::new(Metrics::default());
        let async_runtime = Arc::new(AsyncRuntime::new()?);
        let blob = registry.blob_no_reentrance()?;
        let blob = UnreliableBlob::from_handle(blob, unreliable);
        let blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::clone(&metrics),
            Arc::clone(&async_runtime),
            blob,
            None,
        );
        let maintainer = Maintainer::new(blob.clone(), async_runtime, Arc::clone(&metrics));
        let i = Indexed::new(log, blob, metrics)?;

        Ok((i, maintainer))
    }

    #[test]
    fn single_stream() -> Result<(), Error> {
        let updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)> = vec![
            (("1".into(), "".into()), 1, 1),
            (("2".into(), "".into()), 2, 1),
        ];

        let mut i = MemRegistry::new().indexed_no_reentrance()?;
        let id = block_on(|res| i.register("0", "()", "()", res))?;

        // Empty things are empty.
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, vec![]);
        assert_eq!(seqno.0, 0);
        assert_eq!(seal_frontier.elements(), &[0]);

        // Register a listener for writes.
        let (listen_tx, listen_rx) = crossbeam_channel::unbounded();
        block_on(|res| i.apply(Cmd::Read(CmdRead::Listen(id, listen_tx, res))))?;

        // After a write, all data is in the unsealed.
        block_on_drain(&mut i, |i, handle| {
            i.apply(Cmd::Write(write_req_payload(id, &updates), handle))
        })?;
        assert_eq!(
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?.read_to_end()?,
            updates
        );
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?;
        assert_eq!(unsealed.read_to_end()?, updates);
        assert_eq!(trace.read_to_end()?, vec![]);
        assert_eq!(seqno.0, 1);
        assert_eq!(seal_frontier.elements(), &[0]);

        // After a step, it's all still in the unsealed as nothing has been sealed
        // yet.
        i.step()?;
        assert_eq!(
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?.read_to_end()?,
            updates
        );
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?;
        assert_eq!(unsealed.read_to_end()?, updates);
        assert_eq!(trace.read_to_end()?, vec![]);
        assert_eq!(seqno.0, 1);
        assert_eq!(seal_frontier.elements(), &[0]);

        // After a seal and a step, the relevant data has moved into the trace
        // part of the index. Since we haven't sealed all the data, some of it
        // is still in the unsealed.
        block_on_drain(&mut i, |i, handle| i.apply(Cmd::Seal(vec![id], 2, handle)))?;
        i.step()?;
        assert_eq!(
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?.read_to_end()?,
            updates
        );
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?;
        assert_eq!(unsealed.read_to_end()?, updates[1..]);
        assert_eq!(trace.read_to_end()?, updates[..1]);
        assert_eq!(seqno.0, 2);
        assert_eq!(seal_frontier.elements(), &[2]);

        // All the data has been sealed, so it's now all in the trace.
        block_on_drain(&mut i, |i, handle| i.apply(Cmd::Seal(vec![id], 3, handle)))?;
        i.step()?;
        assert_eq!(
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?.read_to_end()?,
            updates
        );
        let ArrangementSnapshot(unsealed, trace, seqno, seal_frontier) =
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, updates);
        assert_eq!(seqno.0, 3);
        assert_eq!(seal_frontier.elements(), &[3]);

        // Verify that the listener got a copy of the writes.
        let listen_received = {
            let mut buf = Vec::new();
            while let Ok(x) = listen_rx.try_recv() {
                match x {
                    ListenEvent::Records(mut records) => {
                        buf.append(&mut records);
                    }
                    _ => (),
                }
            }
            buf
        };
        assert_eq!(listen_received, updates);

        // Can advance compaction frontier to a time that has already been sealed
        block_on_drain(&mut i, |i, handle| {
            i.apply(Cmd::AllowCompaction(
                vec![(id, Antichain::from_elem(2))],
                handle,
            ))
        })?;

        Ok(())
    }

    fn trace_compaction_setup(
        unreliable: UnreliableHandle,
    ) -> Result<
        (
            Indexed<UnreliableLog<MemLog>, UnreliableBlob<MemBlob>>,
            Maintainer<UnreliableBlob<MemBlob>>,
            Id,
        ),
        Error,
    > {
        let (mut i, maintainer) = indexed_and_maintainer(unreliable)?;

        let updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)> = vec![
            (("1".into(), "".into()), 1, 1),
            (("2".into(), "".into()), 2, 1),
            (("3".into(), "".into()), 3, 1),
            (("4".into(), "".into()), 4, 1),
            (("5".into(), "".into()), 5, 1),
        ];

        let reqs = i.step()?;
        // No trace compaction requests when no streams are registered.
        assert_eq!(reqs, vec![]);

        let id = block_on(|res| i.register("0", "()", "()", res))?;

        let reqs = i.step()?;
        // No maintenance requests when the registered stream has no data
        // in the trace.
        assert_eq!(reqs, vec![]);

        // Move data into unsealed.
        block_on_drain(&mut i, |i, handle| {
            i.apply(Cmd::Write(write_req_payload(id, &updates), handle))
        })?;

        // Move data into trace, one timestamp at a time.
        for t in 2..7 {
            block_on_drain(&mut i, |i, handle| i.apply(Cmd::Seal(vec![id], t, handle)))?;
            let reqs = i.step()?;
            // There will not be any compaction requests because the compaction
            // frontier has not advanced.
            assert_eq!(reqs, vec![]);
        }

        // Advance the compaction frontier to a time that has been sealed.
        block_on_drain(&mut i, |i, handle| {
            i.apply(Cmd::AllowCompaction(
                vec![(id, Antichain::from_elem(5))],
                handle,
            ))
        })?;

        Ok((i, maintainer, id))
    }

    /// Test the communication protocol between [Indexed] and [Maintainer]
    /// specifically for trace compaction requests.
    #[test]
    fn trace_compaction() -> Result<(), Error> {
        let mut unreliable = UnreliableHandle::default();
        let (mut i, maintainer, id) = trace_compaction_setup(unreliable.clone())?;

        let reqs = i.step()?;
        assert_eq!(reqs.len(), 1);
        // Check that Indexed also knows there is a compaction request in flight.
        assert_eq!(i.in_flight_trace_compactions.len(), 1);
        let request = reqs[0].clone();

        // Once a request is in flight, don't send requests for the same stream.
        let reqs = i.step()?;
        assert_eq!(i.in_flight_trace_compactions.len(), 1);
        assert_eq!(reqs, vec![]);

        // Handle receiving an error MaintenanceRes.
        let error_response =
            MaintenanceRes::CompactTrace((id, Err(Error::from("test compaction error"))));
        i.handle_maintenance_res(error_response.clone());
        assert_eq!(i.drain_pending(), Ok(()));
        // Check that Indexed resets its state to reflect that no compactions in flight.
        assert_eq!(i.in_flight_trace_compactions.len(), 0);

        // After receiving an error, retry the original request.
        let reqs = i.step()?;
        assert_eq!(reqs.len(), 1);
        assert_eq!(i.in_flight_trace_compactions.len(), 1);
        assert_eq!(reqs[0], request);

        // Handle receiving a valid MaintenanceRes.
        let response = futures_executor::block_on(request.clone().run_async(&maintainer).recv());

        i.handle_maintenance_res(response);
        assert_eq!(i.drain_pending(), Ok(()));
        // Check that Indexed resets its state to reflect that no compactions in flight.
        assert_eq!(i.in_flight_trace_compactions.len(), 0);

        // After receiving a valid response, issue a new compaction request.
        let reqs = i.step()?;
        assert_eq!(reqs.len(), 1);
        assert_eq!(i.in_flight_trace_compactions.len(), 1);
        let request2 = reqs[0].clone();
        assert!(request2 != request);

        // Construct a new valid, response.
        let response2 = futures_executor::block_on(request2.clone().run_async(&maintainer).recv());

        // Send back an error response. After this there are no requests in flight.
        i.handle_maintenance_res(error_response);
        assert_eq!(i.drain_pending(), Ok(()));
        assert_eq!(i.in_flight_trace_compactions.len(), 0);

        // Retry request after error response.
        let reqs = i.step()?;
        assert_eq!(reqs.len(), 1);
        assert_eq!(i.in_flight_trace_compactions.len(), 1);
        assert_eq!(reqs[0], request2);

        // Handle failed write to metadata after receiving a response.
        i.handle_maintenance_res(response2.clone());
        unreliable.make_unavailable();
        assert_eq!(i.drain_pending(), Err(Error::from("unavailable: blob set")));
        unreliable.make_available();
        assert_eq!(i.in_flight_trace_compactions.len(), 0);

        // Retry request after failed meta write.
        let reqs = i.step()?;
        assert_eq!(i.in_flight_trace_compactions.len(), 1);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], request2);

        // Handle receiving a response after the trace is deleted.
        block_on(|res| i.apply(Cmd::Destroy("0".into(), res)))?;

        // Handle receiving an valid MaintenanceRes for a stream that has been deleted.
        i.handle_maintenance_res(response2);
        assert_eq!(i.drain_pending(), Ok(()));
        assert_eq!(i.in_flight_trace_compactions.len(), 0);

        // Don't produce any more requests because the stream is deleted.
        let reqs = i.step()?;
        assert_eq!(i.in_flight_trace_compactions.len(), 0);
        assert_eq!(reqs, vec![]);

        Ok(())
    }

    /// This test checks that Indexed correctly handles receiving an unexpected
    /// MainenanceRes (one whose request does not match the currently in-flight
    /// request). This test is expected to panic when run with
    /// cfg!(debug_assertions).
    fn trace_compaction_unexpected_nonmatching_response() -> Result<(), Error> {
        let unreliable = UnreliableHandle::default();
        let (mut i, maintainer, _) = trace_compaction_setup(unreliable)?;

        let reqs = i.step()?;
        assert_eq!(reqs.len(), 1);
        let request = reqs[0].clone();

        // Handle receiving an unexpected MaintenanceRes (one whoese request does
        // not match the one currently in flight)
        let mut response = futures_executor::block_on(request.run_async(&maintainer).recv());
        if let MaintenanceRes::CompactTrace((_, Ok(response))) = &mut response {
            response.req.b0.keys = vec!["unexpected".into()];
        }
        i.handle_maintenance_res(response);
        assert_eq!(i.drain_pending(), Ok(()));

        Ok(())
    }

    /// This test checks that Indexed correctly handles receiving a MaintenanceRes when
    /// no request is in flight for the given stream. This test is expected to panic when
    /// run with cfg!(debug_assertions).
    fn trace_compaction_unexpected_response() -> Result<(), Error> {
        let unreliable = UnreliableHandle::default();
        let (mut i, maintainer, id) = trace_compaction_setup(unreliable)?;

        let reqs = i.step()?;
        assert_eq!(reqs.len(), 1);
        let request = reqs[0].clone();

        // Send an error response. Now there are no more requests in flight.
        let error_response =
            MaintenanceRes::CompactTrace((id, Err(Error::from("test compaction error"))));
        i.handle_maintenance_res(error_response);
        assert_eq!(i.drain_pending(), Ok(()));

        // Handle receiving an unexpected (valid) MaintenanceRes when none is in flight.
        let response = futures_executor::block_on(request.run_async(&maintainer).recv());
        i.handle_maintenance_res(response);
        assert_eq!(i.drain_pending(), Ok(()));

        Ok(())
    }

    /// This test checks that Indexed correctly handles receiving an error MaintenanceRes
    /// even when no requests are in flight for that stream. This test is expected to
    /// panic when run with cfg!(debug_assertions).
    fn trace_compaction_unexpected_error_response() -> Result<(), Error> {
        let unreliable = UnreliableHandle::default();
        let (mut i, _, id) = trace_compaction_setup(unreliable)?;

        let reqs = i.step()?;
        assert_eq!(reqs.len(), 1);

        // Send an error response. Now there are no more requests in flight.
        let error_response =
            MaintenanceRes::CompactTrace((id, Err(Error::from("test compaction error"))));
        i.handle_maintenance_res(error_response.clone());
        assert_eq!(i.drain_pending(), Ok(()));

        // Handle receiving an unexpected error MaintenanceRes when no request is in flight.
        i.handle_maintenance_res(error_response);
        assert_eq!(i.drain_pending(), Ok(()));

        Ok(())
    }

    #[test]
    fn trace_compaction_expected_panics() -> Result<(), Error> {
        fn check_error(
            e: Option<Box<dyn Any + Send + 'static>>,
            expected: &str,
        ) -> Result<(), Error> {
            let e = match e {
                Some(e) => e,
                None => return Err(Error::from("unexpected success while checking error")),
            };

            let error = match e.downcast_ref::<String>() {
                Some(error) => error.clone(),
                None => "unable to check error".to_string(),
            };

            assert!(
                error.contains(expected),
                "{} not found in {}",
                expected,
                error
            );
            Ok(())
        }
        let result = std::panic::catch_unwind(|| {
            let _ = trace_compaction_unexpected_response();
        });
        check_error(
            result.err(),
            "unexpected compaction response for stream: Id(0)",
        )?;

        let result = std::panic::catch_unwind(|| {
            let _ = trace_compaction_unexpected_error_response();
        });
        check_error(
            result.err(),
            "unexpected compaction response for stream: Id(0)",
        )?;

        let result = std::panic::catch_unwind(|| {
            let _ = trace_compaction_unexpected_nonmatching_response();
        });
        check_error(
            result.err(),
            "unexpected compaction response for stream: Id(0)",
        )?;

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
            i.apply(Cmd::Write(write_req_payload(id, &updates), handle))
        })?;

        // Now move it into the trace part of the index, which orders it within
        // each batch by key. It should currently be ordered by time, which
        // given the data is not ordered by key, so again this should fire a
        // validations error if the sort code doesn't work.
        block_on_drain(&mut i, |i, handle| i.apply(Cmd::Seal(vec![id], 3, handle)))?;
        i.step()?;

        // Sanity check that all the data made it into trace as expected.
        let ArrangementSnapshot(unsealed, trace, _, _) =
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?;
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
            i.apply(Cmd::Write(write_req_payload(id, &updates), handle))
        })?;

        // Add another set of identical updates and place into another unsealed
        // batch.
        block_on_drain(&mut i, |i, handle| {
            i.apply(Cmd::Write(write_req_payload(id, &updates), handle))
        })?;

        // Sanity check that the data is all in unsealed and none of it is in trace.
        let ArrangementSnapshot(unsealed, trace, _, _) =
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?;
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
        block_on_drain(&mut i, |i, handle| i.apply(Cmd::Seal(vec![id], 2, handle)))?;
        i.step()?;

        // Sanity check that all the data made it into trace as expected.
        let ArrangementSnapshot(unsealed, trace, _, _) =
            block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?;
        assert_eq!(unsealed.read_to_end()?, vec![]);
        assert_eq!(trace.read_to_end()?, vec![(("1".into(), "".into()), 1, 4)]);

        Ok(())
    }

    #[test]
    fn regression_empty_unsealed_batch() -> Result<(), Error> {
        let updates: Vec<((Vec<u8>, Vec<u8>), _, _)> = vec![];

        let mut i = MemRegistry::new().indexed_no_reentrance()?;
        let id = block_on(|res| i.register("0", "", "", res))?;

        // Write the data and move it into the unsealed part of the index.
        assert_eq!(
            block_on_drain(&mut i, |i, handle| {
                i.apply(Cmd::Write(write_req_payload(id, &updates), handle))
            }),
            Ok(SeqNo(1))
        );

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
            i.apply(Cmd::Write(
                write_req_payload(s1, &[(("".into(), "".into()), 0, 1)]),
                handle,
            ))
        })?;
        let s2 = block_on(|res| i.register("s2", "", "", res))?;
        block_on_drain(&mut i, |i, handle| {
            i.apply(Cmd::Write(
                write_req_payload(s2, &[(("".into(), "".into()), 1, 1)]),
                handle,
            ))
        })?;

        // The second flavor is similar. If we then write to the first stream
        // again and step, it is then missing X..Y. (A stream not written to
        // between two step calls doesn't get a batch.)
        block_on_drain(&mut i, |i, handle| {
            i.apply(Cmd::Write(
                write_req_payload(s1, &[(("".into(), "".into()), 2, 1)]),
                handle,
            ))
        })?;

        Ok(())
    }

    #[test]
    fn test_destroy() -> Result<(), Error> {
        let mut i = MemRegistry::new().indexed_no_reentrance()?;

        let _ = block_on(|res| i.register("stream", "", "", res))?;

        // Normal case: destroy registered stream.
        assert_eq!(
            block_on(|res| i.apply(Cmd::Destroy("stream".into(), res))),
            Ok(true)
        );

        // Normal case: destroy already destroyed stream.
        assert_eq!(
            block_on(|res| i.apply(Cmd::Destroy("stream".into(), res))),
            Ok(false)
        );

        // Destroy stream that was never created.
        assert_eq!(
            block_on(|res| i.apply(Cmd::Destroy("stream2".into(), res))),
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

        // i64erent key codec
        assert_eq!(
            block_on(|res| i.register("stream", "nope", "val", res)),
            Err(Error::from(
                "invalid registration: key codec mismatch nope vs previous key"
            ))
        );

        // i64erent val codec
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
            i.apply(Cmd::Write(write_req_payload(id, &updates), handle))
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
            i.apply(Cmd::Write(write_req_payload(id, &updates), res))
        })?;
        block_on_drain(&mut i, |i, res| i.apply(Cmd::Seal(vec![id], 4, res)))?;
        block_on_drain(&mut i, |i, res| {
            i.apply(Cmd::AllowCompaction(
                vec![(id, Antichain::from_elem(3))],
                res,
            ))
        })?;
        let snap = block_on(|res| i.apply(Cmd::Read(CmdRead::Snapshot(id, res))))?;

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
