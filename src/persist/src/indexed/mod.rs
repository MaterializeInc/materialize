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
pub mod cache;
pub mod encoding;
pub mod future;
pub mod runtime;
pub mod trace;

use std::collections::HashMap;
use std::ops::Range;

use abomonation::abomonated::Abomonated;
use differential_dataflow::trace::Description;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::Error;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{
    BlobFutureBatch, BlobFutureMeta, BlobMeta, BlobTraceBatch, BlobTraceMeta, BufferEntry, Id,
};
use crate::indexed::future::{BlobFuture, FutureSnapshot};
use crate::indexed::runtime::Cmd;
use crate::indexed::trace::{BlobTrace, TraceSnapshot};
use crate::storage::{Blob, Buffer, SeqNo};
use crate::Data;

/// A persistent, compacting, indexed data structure of `(Key, Value, Time,
/// Diff)` updates.
///
/// The lifecycle of contained entries is as follows:
/// - Initially: inserted into a durable, un-indexed [Buffer].
/// - ASAP: atomically transferred from the buffer into a [BlobFuture], which
///   indexes them by `(time, key, value)`.
/// - Once the update's time has been "seal"ed: transferred from the
///   [BlobFuture] into a [BlobTrace], which indexes them by `(key, value,
///   time)`.
///
/// Notes:
/// - An entry should only logically exist in one of these places at a time,
///   even though it may physically exist in more than one of them. Atomicity
///   between Buffer and BlobFuture is accomplished by assigning an incrementing
///   SeqNo to each entry as it's persisted by the buffer. Then, entries are
///   transferred to the BlobFuture in batch, noting the SeqNos. On read,
///   [Indexed] then uses these SeqNos to ignore any data in Buffer that exists
///   in BlobFuture.
/// - Similarly, `frontier` represents the border between data in [BlobFuture]
///   and [BlobTrace]. BlobTrace is logically append-only, so data is
///   transferred to it once all the data for some timestamp has arrived. On
///   read, [Indexed] uses this frontier to ignore any data in BlobFuture that
///   exists in in BlobTrace.
/// - Note that data is transferred from Buffer to BlobFuture in the order it
///   was written, but transferred from BlobFuture to Trace in order of its
///   *timestamp*.
///
/// TODO: We should probably pull the Buffer to be external to this. Then, when
/// entries are written to the Buffer, also tee them here with the resulting
/// SeqNo attached. Once the BlobFuture has persisted them, inform the Buffer
/// that they are no longer needed. This would make them immediately available
/// for indexed use, instead of the current situation, which is more complicated
/// to reason about.
pub struct Indexed<K, V, U: Buffer, L: Blob> {
    next_stream_id: Id,
    futures_seqno_upper: SeqNo,
    // Temporary variable used to simulate a sequence number
    // now that we don't have a buffer.
    // TODO: do we really need SeqNo anymore?
    seqno: SeqNo,
    // This is conceptually a map from `String` -> `Id`, but lookups are rare
    // and this representation is optimized for the metadata serialization path,
    // which is less rare.
    id_mapping: Vec<(String, Id)>,
    graveyard: Vec<(String, Id)>,
    buf: U,
    blob: BlobCache<K, V, L>,
    futures: HashMap<Id, BlobFuture<K, V>>,
    traces: HashMap<Id, BlobTrace<K, V>>,
    listeners: HashMap<Id, Vec<ListenFn<K, V>>>,
}

/// A response to a command from the command queue.
enum Response<K, V> {
    Register(Id),
    Destroy(bool),
    Write(SeqNo),
    Seal,
    AllowCompaction,
    // TODO change.
    Snapshot(Option<IndexedSnapshot<K, V>>),
    Listen,
    Stop,
}

impl<K: Data, V: Data, U: Buffer, L: Blob> Indexed<K, V, U, L> {
    /// Returns a new Indexed, initializing each Future and Trace with the
    /// existing data for them in the blob storage, if any.
    pub fn new(mut buf: U, blob: L) -> Result<Self, Error> {
        let mut blob = BlobCache::new(blob);
        let meta = blob
            .get_meta()
            .map_err(|err| {
                // Indexed is expected to close the buffer and blob it's handed.
                // Usually that happens when close is called on Indexed itself,
                // but if there's an error constructing it, we never get to that
                // point and have to clean up ourselves.
                //
                // TODO: Regression test for this.
                if let Err(err) = buf.close() {
                    log::warn!("error closing buffer: {}", err);
                }
                if let Err(err) = blob.close() {
                    log::warn!("error closing blob: {}", err);
                }
                err
            })?
            .unwrap_or_default();
        let futures = meta
            .futures
            .into_iter()
            .map(|meta| (meta.id, BlobFuture::new(meta)))
            .collect();
        let traces = meta
            .traces
            .into_iter()
            .map(|meta| (meta.id, BlobTrace::new(meta)))
            .collect();
        let indexed = Indexed {
            next_stream_id: meta.next_stream_id,
            futures_seqno_upper: meta.futures_seqno_upper,
            seqno: meta.futures_seqno_upper,
            id_mapping: meta.id_mapping,
            graveyard: meta.graveyard,
            buf,
            blob,
            futures,
            traces,
            listeners: HashMap::new(),
        };
        Ok(indexed)
    }

    /// Releases exclusive-writer locks and causes all future commands to error.
    ///
    /// This method is idempotent.
    pub fn close(&mut self) -> Result<(), Error> {
        // Make sure all the listener closures are dropped.
        self.listeners.clear();
        // Be careful to attempt to close both buf and blob even if one of the
        // closes fails.
        let buf_res = self.buf.close();
        let blob_res = self.blob.close();
        buf_res?;
        blob_res?;
        Ok(())
    }

    /// Check if a registration command is logically valid.
    fn validate_register(&self, name: &str) -> Result<(), Error> {
        if self.graveyard.iter().any(|(s, _)| s == &name) {
            return Err(Error::from(format!(
                "invalid registration: stream {} already destroyed",
                name
            )));
        }

        Ok(())
    }

    /// Apply a pre-validated register command to the in-memory state.
    ///
    /// This has to be infallible.
    fn do_register_mem(&mut self, name: &str) -> Id {
        let id = self.id_mapping.iter().find(|(s, _)| s == &name);
        let id = match id {
            Some((_, id)) => *id,
            None => {
                let id = self.next_stream_id;
                self.id_mapping.push((name.to_owned(), id));
                self.next_stream_id = Id(id.0 + 1);
                id
            }
        };
        self.futures
            .entry(id)
            .or_insert_with_key(|id| BlobFuture::new(BlobFutureMeta::new(*id)));
        self.traces
            .entry(id)
            .or_insert_with_key(|id| BlobTrace::new(BlobTraceMeta::new(*id)));

        id
    }

    /// Creates, if necessary, a new future and trace with the given external
    /// stream name, returning the corresponding internal stream id.
    ///
    /// This method is idempotent: ids may be registered multiple times.
    pub fn register(&mut self, id_str: &str) -> Result<Id, Error> {
        self.validate_register(id_str)?;

        // TODO: this function needs to commit the registration to memory
        // and be able to revert the in-memory modifications if it was unable
        // to commit.
        Ok(self.do_register_mem(id_str))
    }

    fn validate_destroy(&self, name: &str) -> Result<(), Error> {
        if self
            .graveyard
            .iter()
            .any(|(destroyed_name, _)| destroyed_name == &name)
            || self
                .id_mapping
                .iter()
                .any(|(stream_name, _)| stream_name == &name)
        {
            Ok(())
        } else {
            Err(Error::from(format!(
                "invalid destroy of stream {} that was never registered or destroyed",
                name
            )))
        }
    }

    fn do_destroy_mem(&mut self, id_str: &str) -> bool {
        if self
            .graveyard
            .iter()
            .any(|(destroyed_name, _)| destroyed_name == &id_str)
        {
            return false;
        }

        let mapping = self
            .id_mapping
            .iter()
            .find(|(name, _)| name == &id_str)
            .expect("name known to exist")
            .clone();

        self.id_mapping.retain(|(name, _)| name != &id_str);

        // TODO: actually physically delete the future and trace batches.
        let future = self.futures.remove(&mapping.1);
        let trace = self.traces.remove(&mapping.1);

        // Sanity check that we actually removed the future and trace for this
        // stream.
        debug_assert!(future.is_some());
        debug_assert!(trace.is_some());

        self.graveyard.push(mapping);

        true
    }

    /// Removes a stream from the index.
    ///
    /// This method is idempotent and may be called multiple times. It returns
    /// true if the stream was destroyed from this call, and false if it was
    /// already destroyed.
    pub fn destroy(&mut self, id_str: &str) -> Result<bool, Error> {
        self.validate_destroy(id_str)?;
        return Ok(self.do_destroy_mem(id_str));

        // TODO this function still needs to commit the in-memory updates
        // to durable storage and be able to undo them if that durable
        // commit fails.

        // TODO: we still need to physically delete any batches associated with
        // this stream.
    }

    /// Drains writes from the buffer into the future and does any necessary
    /// resulting compaction work.
    ///
    /// In production, step should just be called in a loop (probably with some
    /// smarts about waiting to call it only after there have been some writes),
    /// but it's exposed this way so we can write deterministic tests.
    pub fn step(&mut self) -> Result<(), Error> {
        self.drain_buf()?;
        self.drain_future();

        Ok(())
        // TODO: Incrementally compact future.
    }

    fn validate_write_sync(
        &self,
        updates: &[(Id, Vec<((K, V), u64, isize)>)],
    ) -> Result<(), Error> {
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

        Ok(())
    }

    /// Synchronously persists (Key, Value, Time, Diff) updates for the stream
    /// with the given id.
    pub fn write_sync(
        &mut self,
        updates: Vec<(Id, Vec<((K, V), u64, isize)>)>,
    ) -> Result<SeqNo, Error> {
        self.validate_write_sync(&updates)?;

        let entry = BufferEntry { updates };
        let mut entry_bytes = Vec::new();
        unsafe { abomonation::encode(&entry, &mut entry_bytes) }
            .expect("write to Vec is infallible");
        let seqno = self.buf.write_sync(entry_bytes)?;
        Ok(seqno)
    }

    /// Handle all the commands in `cmdq` and either:
    /// - execute and commit all of them
    /// - execute and commit none of them.
    ///
    /// This doesn't mean they all have to be valid, just that
    /// all of the ones that needed to get written down do get written down
    /// TODO make infallible.
    fn handle_commands(&mut self, cmdq: Vec<Cmd<K, V>>) {
        let mut writes_by_id = HashMap::new();
        let mut pending_responses: Vec<Result<Response<K, V>, Error>> = vec![];
        for cmd in cmdq.iter() {
            match self.validate_command(cmd) {
                Err(e) => pending_responses.push(Err(e)),
                Ok(_) => pending_responses.push(Ok(self.do_command_mem(cmd, &mut writes_by_id))),
            }
        }

        let desc = self.futures_seqno_upper..self.seqno;
        self.futures_seqno_upper = desc.end;

        // Persist writes and metadata changes to durable storage. Any failure here
        // needs to be handled by sending an error response for all commands and
        // reverting the in-memory state back to what it was prior to processing all
        // commands.

        // First, persist the writes.
        for (id, updates) in writes_by_id {
            if !updates.is_empty() {
                debug_assert!(desc.end > desc.start);
                // TODO: correctly handle error here.
                self.drain_buf_inner(id, updates, &desc).expect("WIP");
            }
        }

        // TODO: correctly handle error here.
        self.blob.set_meta(self.serialize_meta()).expect("WIP");

        // We're basically home free now. Go ahead and perform any snapshots and
        // listen functions as needed.
        for (cmd, rsp) in cmdq.into_iter().zip(pending_responses.into_iter()) {
            match (cmd, rsp) {
                (Cmd::Register(_, res), Err(e)) => res.fill(Err(e)),
                (Cmd::Destroy(_, res), Err(e)) => res.fill(Err(e)),
                (Cmd::Write(_, res), Err(e)) => res.fill(Err(e)),
                (Cmd::Seal(_, _, res), Err(e)) => res.fill(Err(e)),
                (Cmd::AllowCompaction(_, _, res), Err(e)) => res.fill(Err(e)),
                (Cmd::Snapshot(_, res), Err(e)) => res.fill(Err(e)),
                (Cmd::Listen(_, _, res), Err(e)) => res.fill(Err(e)),
                (Cmd::Stop(res), Err(e)) => res.fill(Err(e)),
                (Cmd::Register(_, res), Ok(Response::Register(id))) => res.fill(Ok(id)),
                (Cmd::Destroy(_, res), Ok(Response::Destroy(b))) => res.fill(Ok(b)),
                (Cmd::Write(_, res), Ok(Response::Write(seqno))) => res.fill(Ok(seqno)),
                (Cmd::Seal(_, _, res), Ok(Response::Seal)) => res.fill(Ok(())),
                (Cmd::AllowCompaction(_, _, res), Ok(Response::AllowCompaction)) => {
                    res.fill(Ok(()))
                }
                (Cmd::Snapshot(id, res), Ok(Response::Snapshot(_))) => {
                    // TODO: this is only valid if you assume snapshot's
                    // happen without any following commands.
                    res.fill(self.snapshot(id))
                }
                (Cmd::Listen(id, listen_fn, res), Ok(Response::Listen)) => {
                    // TODO this is only valid if you assume listen's
                    // happen without any following commands.
                    self.do_listen_mem(id, listen_fn);
                    res.fill(Ok(()))
                }
                (Cmd::Stop(res), Ok(Response::Stop)) => {
                    // TODO: this is only valid if you assume close's
                    // happen without any following commands.
                    res.fill(self.close())
                }
                _ => panic!("invalid cmd / rsp combination"),
            }
        }
    }

    fn validate_command(&self, cmd: &Cmd<K, V>) -> Result<(), Error> {
        match cmd {
            Cmd::Register(name, _) => self.validate_register(name),
            Cmd::Destroy(name, _) => self.validate_destroy(name),
            Cmd::Write(writes, _) => self.validate_write_sync(writes),
            Cmd::Seal(ids, ts_upper, _) => self.validate_seal(ids, *ts_upper).map(|_| ()),
            Cmd::AllowCompaction(id, ts, _) => self.validate_allow_compaction(*id, *ts),
            Cmd::Snapshot(id, _) => self.validate_registered_id(*id),
            Cmd::Listen(id, _, _) => self.validate_registered_id(*id),
            Cmd::Stop(_) => Ok(()),
        }
    }

    /// Update in-memory state for commands that are valid.
    ///
    /// Intentionally not fallible.
    /// TODO: better name?
    fn do_command_mem(
        &mut self,
        cmd: &Cmd<K, V>,
        writes_by_id: &mut HashMap<Id, Vec<(SeqNo, (K, V), u64, isize)>>,
    ) -> Response<K, V> {
        match cmd {
            Cmd::Register(name, _) => Response::Register(self.do_register_mem(name)),
            Cmd::Destroy(name, _) => {
                // TODO: WIP: normally you would need to clear out the pending
                // writes at `name`. For now let's assume that destroy triggers
                // a pipeline flush.
                //
                // Edit: I no longer believe that is true, and instead think
                // it is just a performance optimization.
                Response::Destroy(self.do_destroy_mem(name))
            }
            Cmd::Write(writes, _) => {
                let seqno = self.seqno;
                for (id, updates) in writes.iter() {
                    writes_by_id
                        .entry(*id)
                        .or_default()
                        .extend(updates.iter().map(|((key, val), ts, diff)| {
                            (seqno, (key.clone(), val.clone()), *ts, *diff)
                        }));
                }

                self.seqno = SeqNo(seqno.0 + 1);
                Response::Write(seqno)
            }
            Cmd::Seal(ids, ts_upper, _) => {
                self.do_seal_mem(ids, *ts_upper);
                Response::Seal
            }
            Cmd::AllowCompaction(id, since, _) => {
                self.do_allow_compaction_mem(*id, *since);
                Response::AllowCompaction
            }
            Cmd::Snapshot(..) => {
                // We need to wait until everything is flushed out to
                // durable storage before taking the snapshot.
                Response::Snapshot(None)
            }
            Cmd::Listen(..) => {
                // TODO: WIP: It seems like cloning trait objects is
                // nontrivial. Going to do the simplest thing I can think
                // of _right now_ and do nothing here.
                //self.do_listen_mem(*id, listen_fn);
                Response::Listen
            }

            Cmd::Stop(_) => Response::Stop,
        }
    }

    /// Atomically moves all writes currently in the buffer into the future.
    fn drain_buf(&mut self) -> Result<(), Error> {
        let mut updates_by_id: HashMap<Id, Vec<(SeqNo, (K, V), u64, isize)>> = HashMap::new();
        let desc = self.buf.snapshot(|seqno, buf| {
            let mut buf = buf.to_vec();
            let (entry, remaining) = unsafe { abomonation::decode::<BufferEntry<K, V>>(&mut buf) }
                .ok_or_else(|| Error::from(format!("invalid buffer entry")))?;
            if !remaining.is_empty() {
                return Err(format!("invalid buffer entry").into());
            }
            for (id, updates) in entry.updates.iter() {
                // iter and cloned instead of append because I don't have a mental
                // model of what's safe with abomonation.
                updates_by_id.entry(*id).or_default().extend(
                    updates.iter().map(|((key, val), ts, diff)| {
                        (seqno, (key.clone(), val.clone()), *ts, *diff)
                    }),
                );
            }

            Ok(())
        })?;

        // If there's nothing in the buffer we can exit early because there's
        // nothing left to do.
        if desc.start == desc.end {
            debug_assert!(updates_by_id.is_empty());
            debug_assert_eq!(self.futures_seqno_upper, desc.end);
            return Ok(());
        }

        for (id, updates) in updates_by_id.drain() {
            let future = self
                .futures
                .get_mut(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

            // We maintain the invariant that futures_seqno_upper is >= every
            // future's seqno_upper and that there is nothing for that future in
            // [future.seqno_upper, self.futures_seqno_upper). Use this to make the
            // seqnos of all the future batches line up.
            debug_assert_eq!(desc.start, self.futures_seqno_upper);
            let new_start = future.seqno_upper()[0];
            debug_assert!(new_start <= desc.start);
            let mut desc = desc.clone();
            desc.start = new_start;

            self.drain_buf_inner(id, updates, &desc)?;
        }

        self.futures_seqno_upper = desc.end;
        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())?;

        self.buf.truncate(desc.end)
    }

    /// Construct a new [BlobFutureBatch] out of the provided `updates` and add
    /// it to the future for `id`.
    ///
    /// The caller is responsible for updating META after they've finished
    /// updating futures.
    fn drain_buf_inner(
        &mut self,
        id: Id,
        mut updates: Vec<(SeqNo, (K, V), u64, isize)>,
        desc: &Range<SeqNo>,
    ) -> Result<(), Error> {
        if cfg!(any(debug_assertions, test)) {
            // Sanity check that all received sequence numbers fall within the stated
            // [lower, upper) range
            for (seqno, _, _, _) in &updates {
                if seqno < &desc.start || seqno >= &desc.end {
                    return Err(Error::from(format!(
                            "invalid sequence number in snapshot {:?}, expected value greater than or equal to {:?} and less than {:?}",
                                                   seqno, desc.start, desc.end)));
                }
            }
        }

        let mut updates: Vec<_> = updates
            .drain(..)
            .map(|(_, (k, v), t, d)| (t, (k, v), d))
            .collect();
        // Future batches are required to be sorted and consolidated by ((ts, (k, v)).
        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        if updates.is_empty() {
            return Ok(());
        }

        // Reshape updates back to the desired type.
        let updates: Vec<_> = updates
            .drain(..)
            .map(|(t, (k, v), d)| ((k, v), t, d))
            .collect();
        let batch = BlobFutureBatch {
            desc: Description::new(
                Antichain::from_elem(desc.start),
                Antichain::from_elem(desc.end),
                // We never compact BlobFuture, so since is always the minimum.
                Antichain::from_elem(SeqNo(0)),
            ),
            updates,
        };
        self.append_future(id, batch)?;

        Ok(())
    }

    /// Atomically moves all writes in future not in advance of the trace's
    /// seal frontier into the trace and does any necessary resulting eviction
    /// work to remove uneccessary batches.
    fn drain_future(&mut self) {
        let mut updates_by_id = vec![];
        for (id, trace) in self.traces.iter_mut() {
            // If this future is already properly sealed then we don't need
            // to do anything.
            let seal = trace.get_seal();
            let trace_upper = trace.ts_upper();
            if seal == trace_upper {
                continue;
            } else {
                debug_assert!(PartialOrder::less_equal(&trace_upper, &seal));
            }

            let future = self.futures.get_mut(&id).expect("future known to exist");

            let desc = Description::new(
                trace_upper,
                seal.clone(),
                Antichain::from_elem(Timestamp::minimum()),
            );

            // Move a batch of data from future into trace by reading a
            // snapshot from future...
            let mut updates = Vec::new();
            {
                let mut snap = future
                    .snapshot(desc.lower().clone(), desc.upper().clone(), &self.blob)
                    .expect("wip");
                while snap.read(&mut updates) {}
            }

            // Trace batches are required to be sorted and consolidated by ((k, v), t)
            differential_dataflow::consolidation::consolidate_updates(&mut updates);
            updates_by_id.push((*id, seal, updates.clone()));

            // ...and atomically swapping that snapshot's data into trace.
            let batch = BlobTraceBatch { desc, updates };
            let new_future_ts_lower = batch.desc.upper().clone();

            // TODO handle these errors correctly.
            trace.append(batch, &mut self.blob).expect("wip");
            future.truncate(new_future_ts_lower).expect("wip");
        }

        // TODO: This is a good point to compact future. The data that's been
        // moved is still there but now irrelevant. It may also be a good time
        // to compact trace.

        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        // TODO handle this error correctly.
        self.blob
            .set_meta(self.serialize_meta())
            .expect("wip need to rollback state if we fail to write meta");

        for (id, seal, updates) in updates_by_id {
            if let Some(listen_fns) = self.listeners.get(&id) {
                for listen_fn in listen_fns.iter() {
                    listen_fn(ListenEvent::Records(updates.clone()));
                    listen_fn(ListenEvent::Sealed(seal[0]));
                }
            }
        }
    }

    /// Returns the current "sealed" frontier for an id.
    ///
    /// This frontier represents a contract of time such that all updates with a
    /// time less than it have arrived. This frontier is advanced though the
    /// `seal` method. Once a time has been sealed for an id, it becomes an
    /// error to later seal it at an time less than or equal to the sealed
    /// frontier. It is also an error to write new data with a time less than
    /// the sealed frontier.
    fn sealed_frontier(&self, id: Id) -> Result<Antichain<u64>, Error> {
        let trace = self
            .traces
            .get(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        Ok(trace.get_seal())
    }

    /// Check if a given seal command is valid and if so, return the set of ids and
    /// their previous seal frontiers.
    fn validate_seal(&self, ids: &[Id], seal_ts: u64) -> Result<Vec<(Id, u64)>, Error> {
        ids.iter()
            .map(|id| {
                let prev = self.sealed_frontier(*id)?;

                if !prev.less_than(&seal_ts) {
                    Err(Error::from(format!(
                        "invalid seal for {:?}: {:?} not in advance of current seal frontier {:?}",
                        id, seal_ts, prev
                    )))
                } else {
                    Ok((*id, prev[0]))
                }
            })
            .collect()
    }

    /// Apply the pre-validated seal command to the in-memory state.
    ///
    /// Intentionally not fallible.
    fn do_seal_mem(&mut self, ids: &[Id], seal_ts: u64) {
        // TODO: we should keep the in-memory state immutable until after the
        // results have been safely committed to durable storage. For now this
        // protocol is simple enough to reason about.
        for id in ids.iter() {
            let trace = self.traces.get_mut(id).expect("trace known to exist");

            trace.update_seal(seal_ts);
        }
    }

    /// Sealing a time advances the "sealed" frontier for an id, which restricts
    /// what times can later be sealed and written for that id. See
    /// `sealed_frontier` for details.
    pub fn seal(&mut self, ids: Vec<Id>, ts_upper: u64) -> Result<(), Error> {
        let prev = self.validate_seal(&ids, ts_upper)?;

        self.do_seal_mem(&ids, ts_upper);
        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        if let Err(e) = self.blob.set_meta(self.serialize_meta()) {
            // Revert in-memory state back to its previous version so that
            // things are consistent between the durably persisted version
            // and the in-memory version.
            for (id, seal) in prev {
                let trace = self.traces.get_mut(&id).expect("trace known to exist");

                trace.update_seal(seal);
            }

            Err(e)
        } else {
            Ok(())
        }
    }

    fn validate_allow_compaction(&self, id: Id, since: u64) -> Result<(), Error> {
        let trace = self
            .traces
            .get(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        let prev = trace.since();
        if !prev.less_than(&since) {
            Err(Error::from(format!(
                "invalid since for {:?}: {:?} not in advance of current since frontier {:?}",
                id, since, prev
            )))
        } else {
            Ok(())
        }
    }

    /// Update the since frontier for a trace in memory.
    ///
    /// This function assumes the proposed frontier was pre-validated and
    /// is intentionally infallible.
    fn do_allow_compaction_mem(&mut self, id: Id, since: u64) {
        let trace = self.traces.get_mut(&id).expect("trace known to exist");
        trace.update_allow_compaction(since);
    }

    /// Permit compaction of updates at times < since to since.
    ///
    /// The compaction frontier can only monotonically increase and it is an error
    /// to call this function with a since argument that is less than or equal to
    /// the current compaction frontier. It is also an error to advance the
    /// compaction frontier beyond the current sealed frontier.
    ///
    /// TODO: it's unclear whether this function needs to be so restrictive about
    /// calls with a frontier <= current_compaction_frontier. We chose to mirror
    /// the `seal` API here but if that doesn't make sense, remove the restrictions.
    pub fn allow_compaction(&mut self, id: Id, since: u64) -> Result<(), Error> {
        self.validate_allow_compaction(id, since)?;
        self.do_allow_compaction_mem(id, since);
        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())
        // TODO: this function needs to revert the in-memory change if the
        // commit to physical storage fails.
    }

    /// Appends the given `batch` to the future for `id`, writing the data into
    /// blob storage.
    ///
    /// The caller is responsible for updating META after they've finished
    /// updating futures.
    fn append_future(&mut self, id: Id, batch: BlobFutureBatch<K, V>) -> Result<(), Error> {
        let future = self
            .futures
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        future.append(batch, &mut self.blob)
    }

    fn serialize_meta(&self) -> BlobMeta {
        BlobMeta {
            next_stream_id: self.next_stream_id,
            futures_seqno_upper: self.futures_seqno_upper,
            id_mapping: self.id_mapping.clone(),
            graveyard: self.graveyard.clone(),
            futures: self
                .futures
                .iter()
                .map(|(_, future)| future.meta())
                .collect(),
            traces: self.traces.iter().map(|(_, trace)| trace.meta()).collect(),
        }
    }

    fn validate_registered_id(&self, id: Id) -> Result<(), Error> {
        if !self.futures.contains_key(&id) {
            Err(Error::from(format!("never registered: {:?}", id)))
        } else {
            Ok(())
        }
    }

    /// Returns a [Snapshot] for the given id.
    pub fn snapshot(&self, id: Id) -> Result<IndexedSnapshot<K, V>, Error> {
        self.validate_registered_id(id)?;
        let future = self.futures.get(&id).expect("future known to exist");
        let trace = self.traces.get(&id).expect("trace known to exist");
        let trace = trace.snapshot(&self.blob)?;
        let future = future.snapshot(trace.ts_upper.clone(), Antichain::new(), &self.blob)?;

        // A closed lower bound on the updates we should include in `buffer` (i.e.
        // the ones not included in `future`).
        let buf_lower = &future.seqno_upper;
        let buffer = {
            let mut data = Vec::new();
            let seqno = self
                .buf
                .snapshot(|seqno, buf| {
                    let entry: Abomonated<BufferEntry<K, V>, Vec<u8>> =
                        unsafe { Abomonated::new(buf.to_owned()) }
                            .ok_or_else(|| Error::from(format!("invalid buffer entry")))?;
                    for (entry_id, updates) in entry.updates.iter() {
                        if *entry_id != id || !buf_lower.less_equal(&seqno) {
                            continue;
                        }
                        data.extend(updates.iter().cloned());
                    }
                    Ok(())
                })?
                .end;
            BufferSnapshot(seqno, data)
        };

        Ok(IndexedSnapshot(buffer, future, trace))
    }

    /// Registers a callback to a pre-validated stream id.
    ///
    /// Intentionally not fallible.
    pub fn do_listen_mem(&mut self, id: Id, listen_fn: ListenFn<K, V>) {
        self.listeners.entry(id).or_default().push(listen_fn);
    }

    /// Registers a callback to be invoked on successful writes and seals.
    //
    // TODO: Finish the naming bikeshed for this. Other options so far include
    // tail, subscribe, tee, inspect, and capture.
    pub fn listen(&mut self, id: Id, listen_fn: ListenFn<K, V>) -> Result<(), Error> {
        // Verify that id has been registered.
        self.validate_registered_id(id)?;
        self.do_listen_mem(id, listen_fn);
        Ok(())
    }
}

/// An event in a persisted stream.
//
// TODO: This is similar to timely's capture Event but just different enough
// that I couldn't see how to use it directly. Revisit.
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
pub trait Snapshot<K, V> {
    /// A partial read of the data in the snapshot.
    ///
    /// Returns true if read needs to be called again for more data.
    fn read<E: Extend<((K, V), u64, isize)>>(&mut self, buf: &mut E) -> bool;
}

/// Extension methods on `Snapshot<K, V>` for use in tests.
#[cfg(test)]
pub trait SnapshotExt<K: Ord, V: Ord>: Snapshot<K, V> + Sized {
    /// A full read of the data in the snapshot.
    fn read_to_end(mut self) -> Vec<((K, V), u64, isize)> {
        let mut buf = Vec::new();
        while self.read(&mut buf) {}
        buf.sort();
        buf
    }
}

#[cfg(test)]
impl<K: Ord, V: Ord, S: Snapshot<K, V> + Sized> SnapshotExt<K, V> for S {}

/// A consistent snapshot of the data currently in a [Buffer].
#[derive(Debug)]
struct BufferSnapshot<K, V>(SeqNo, Vec<((K, V), u64, isize)>);

impl<K, V> Snapshot<K, V> for BufferSnapshot<K, V> {
    fn read<E: Extend<((K, V), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        buf.extend(self.1.drain(..));
        false
    }
}

/// A consistent snapshot of all data currently stored for an id.
#[derive(Debug)]
pub struct IndexedSnapshot<K, V>(
    BufferSnapshot<K, V>,
    FutureSnapshot<K, V>,
    TraceSnapshot<K, V>,
);

impl<K, V> IndexedSnapshot<K, V> {
    /// Returns the SeqNo at which this snapshot was run.
    ///
    /// All writes assigned a seqno < this are included.
    pub fn seqno(&self) -> SeqNo {
        self.0 .0
    }
}

impl<K: Clone, V: Clone> Snapshot<K, V> for IndexedSnapshot<K, V> {
    fn read<E: Extend<((K, V), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        self.0.read(buf) || self.1.read(buf) || self.2.read(buf)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::mpsc;

    use crate::error::Error as IndexedError;
    use crate::mem::{MemBlob, MemBuffer};

    use super::*;

    fn record_with_seqno(seqno: u64) -> (SeqNo, (String, String), u64, isize) {
        (SeqNo(seqno), ("".to_string(), "".to_string()), 1, 1)
    }

    #[test]
    fn single_stream() -> Result<(), Box<dyn Error>> {
        let updates = vec![
            (("1".to_string(), "".to_string()), 1, 1),
            (("2".to_string(), "".to_string()), 2, 1),
        ];

        let mut i = Indexed::new(
            MemBuffer::new("single_stream"),
            MemBlob::new("single_stream"),
        )?;
        let id = i.register("0")?;

        // Empty things are empty.
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(buf.read_to_end(), vec![]);
        assert_eq!(future.read_to_end(), vec![]);
        assert_eq!(trace.read_to_end(), vec![]);

        // Register a listener for writes.
        let (listen_tx, listen_rx) = mpsc::channel();
        let listen_fn: ListenFn<String, String> = Box::new(move |e| match e {
            ListenEvent::Records(records) => {
                for ((k, v), ts, diff) in records.iter() {
                    listen_tx
                        .send(((k.clone(), v.clone()), *ts, *diff))
                        .expect("rx hasn't been dropped");
                }
            }
            ListenEvent::Sealed(_) => {}
        });
        i.listen(id, listen_fn)?;

        // After a write, all data is in the buffer.
        i.write_sync(vec![(id, updates.clone())])?;
        assert_eq!(i.snapshot(id)?.read_to_end(), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(buf.read_to_end(), updates);
        assert_eq!(future.read_to_end(), vec![]);
        assert_eq!(trace.read_to_end(), vec![]);

        // After a step, it's all moved into the future part of the index.
        i.step()?;
        assert_eq!(i.snapshot(id)?.read_to_end(), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(buf.read_to_end(), vec![]);
        assert_eq!(future.read_to_end(), updates);
        assert_eq!(trace.read_to_end(), vec![]);

        // After a seal and a step, the relevant data has moved into the trace
        // part of the index. Since we haven't sealed all the data, some of it
        // is still in the future.
        i.seal(vec![id], 2)?;
        i.step()?;
        assert_eq!(i.snapshot(id)?.read_to_end(), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(buf.read_to_end(), vec![]);
        assert_eq!(future.read_to_end(), updates[1..]);
        assert_eq!(trace.read_to_end(), updates[..1]);

        // All the data has been sealed, so it's now all in the trace.
        i.seal(vec![id], 3)?;
        i.step()?;
        assert_eq!(i.snapshot(id)?.read_to_end(), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(buf.read_to_end(), vec![]);
        assert_eq!(future.read_to_end(), vec![]);
        assert_eq!(trace.read_to_end(), updates);

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
        i.allow_compaction(id, 2)?;

        Ok(())
    }

    #[test]
    fn batch_sorting() -> Result<(), Box<dyn Error>> {
        let updates = vec![
            (("1".to_string(), "".to_string()), 2, 1),
            (("2".to_string(), "".to_string()), 1, 1),
        ];

        let mut i = Indexed::new(
            MemBuffer::new("batch_sorting"),
            MemBlob::new("batch_sorting"),
        )?;
        let id = i.register("0")?;

        // Write the data and move it into the future part of the index, which
        // orders it within each batch by time. It's not, so this will fire a
        // validations error if the sort code doesn't work.
        i.write_sync(vec![(id, updates)])?;
        i.step()?;

        // Now move it into the trace part of the index, which orders it within
        // each batch by key. It should currently be ordered by time, which
        // given the data is not ordered by key, so again this should fire a
        // validations error if the sort code doesn't work.
        i.seal(vec![id], 3)?;
        i.step()?;
        Ok(())
    }

    #[test]
    fn batch_consolidation() -> Result<(), Box<dyn Error>> {
        let updates = vec![
            (("1".to_string(), "".to_string()), 1, 1),
            (("1".to_string(), "".to_string()), 1, 1),
        ];

        let mut i = Indexed::new(
            MemBuffer::new("batch_consolidation"),
            MemBlob::new("batch_consolidation"),
        )?;
        let id = i.register("0")?;

        // Write the data and move it into the future part of the index, which
        // consolidates updates to identical ((k, v), t). Since the writes are
        // not already consolidated this test will fail if the consolidation
        // code does not work.
        i.write_sync(vec![(id, updates.clone())])?;
        i.step()?;

        // Add another set of identical updates and place into another future
        // batch.
        i.write_sync(vec![(id, updates)])?;
        i.step()?;

        // Now move the data to the trace part of the index, which consolidates
        // updates at identical ((k, v), t). Since the writes are only consolidated
        // within individual future batches this test will fail if trace batch
        // consolidation does not work.
        i.seal(vec![id], 2)?;
        i.step()?;

        Ok(())
    }

    #[test]
    fn batch_future_empty() -> Result<(), Box<dyn Error>> {
        let mut i = Indexed::new(
            MemBuffer::new("batch_future_empty"),
            MemBlob::new("batch_future_empty"),
        )?;
        let id = i.register("0")?;

        // Write an empty set of updates and try to move it into the future part
        // of the index.
        i.write_sync(vec![(id, vec![])])?;
        i.step()?;

        // Sending updates with dif = 0.
        let updates = vec![(("1".to_string(), "".to_string()), 1, 0)];
        i.write_sync(vec![(id, updates)])?;
        i.step()?;

        // Now try again with a set of updates that consolidates down to the empty
        // set.
        let updates = vec![
            (("1".to_string(), "".to_string()), 1, 2),
            (("1".to_string(), "".to_string()), 1, -2),
        ];

        i.write_sync(vec![(id, updates)])?;
        i.step()?;
        Ok(())
    }

    #[test]
    fn drain_buf_validate() -> Result<(), IndexedError> {
        let mut i = Indexed::new(
            MemBuffer::new("drain_buf_validate"),
            MemBlob::new("drain_buf_validate"),
        )?;
        let id = i.register("0")?;

        // Normal case (equals lower)
        assert_eq!(
            i.drain_buf_inner(id, vec![record_with_seqno(0)], &(SeqNo(0)..SeqNo(2))),
            Ok(())
        );

        // Normal case (between (lower, upper))
        assert_eq!(
            i.drain_buf_inner(id, vec![record_with_seqno(3)], &(SeqNo(2)..SeqNo(4))),
            Ok(())
        );

        // Less than lower
        assert_eq!(
            i.drain_buf_inner(id, vec![record_with_seqno(3)], &(SeqNo(4)..SeqNo(6))),
            Err(IndexedError::from(
                "invalid sequence number in snapshot SeqNo(3), expected value greater than or equal to SeqNo(4) and less than SeqNo(6)"
            ))
        );

        // Equal to upper
        assert_eq!(
            i.drain_buf_inner(id, vec![record_with_seqno(6)], &(SeqNo(4)..SeqNo(6))),
            Err(IndexedError::from(
                "invalid sequence number in snapshot SeqNo(6), expected value greater than or equal to SeqNo(4) and less than SeqNo(6)"
            ))
        );

        // Greater than upper
        assert_eq!(
            i.drain_buf_inner(id, vec![record_with_seqno(7)], &(SeqNo(4)..SeqNo(6))),
            Err(IndexedError::from(
                "invalid sequence number in snapshot SeqNo(7), expected value greater than or equal to SeqNo(4) and less than SeqNo(6)"
            ))
        );

        Ok(())
    }

    // Regression test for two similar bugs causing future batches with
    // non-adjacent seqno boundaries (which violates our invariants).
    #[test]
    fn regression_non_sequential_future_batches() -> Result<(), IndexedError> {
        let mut i = Indexed::new(MemBuffer::new("lock"), MemBlob::new("lock"))?;

        // First is some stream is registered, written to, and step'd, moving
        // seqno 0..X into future. Then a second stream is registered, written
        // to, and step'd. When it goes to move X..Y into the future, the second
        // stream is missing a batch for 0..X. (Newly registered streams are
        // missing 0 to the seqno that buffer was at when they are registered.)
        //
        // This caused a violation of our invariants (which are checked in tests
        // and debug mode), so we just need the following to run without error
        // to verify the fix.
        let s1 = i.register("s1")?;
        i.write_sync(vec![(s1, vec![(((), ()), 0, 1)])])?;
        i.step()?;
        let s2 = i.register("s2")?;
        i.write_sync(vec![(s2, vec![(((), ()), 1, 1)])])?;
        i.step()?;

        // The second flavor is similar. If we then write to the first stream
        // again and step, it is then missing X..Y. (A stream not written to
        // between two step calls doesn't get a batch.)
        i.write_sync(vec![(s1, vec![(((), ()), 2, 1)])])?;
        i.step()?;

        Ok(())
    }

    #[test]
    fn test_destroy() -> Result<(), IndexedError> {
        let mut i: Indexed<String, String, _, _> =
            Indexed::new(MemBuffer::new("destroy"), MemBlob::new("destroy"))?;

        let _ = i.register("stream")?;

        // Normal case: destroy registered stream.
        assert_eq!(i.destroy("stream"), Ok(true));

        // Normal case: destroy already destroyed stream.
        assert_eq!(i.destroy("stream"), Ok(false));

        // Destroy stream that was never created.
        assert_eq!(
            i.destroy("stream2"),
            Err(IndexedError::from(
                "invalid destroy of stream stream2 that was never registered or destroyed"
            ))
        );

        // Creating a previously destroyed stream.
        assert_eq!(
            i.register("stream"),
            Err(IndexedError::from(
                "invalid registration: stream stream already destroyed"
            ))
        );

        Ok(())
    }
}
