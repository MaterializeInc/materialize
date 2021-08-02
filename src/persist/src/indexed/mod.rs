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

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::thread::{self, JoinHandle};

use abomonation::abomonated::Abomonated;
use differential_dataflow::trace::Description;
use timely::progress::Antichain;
use timely::PartialOrder;

use crate::error::Error;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{
    BlobFutureBatch, BlobFutureMeta, BlobMeta, BlobTraceBatch, BlobTraceMeta, BufferEntry, Id,
};
use crate::indexed::future::{BlobFuture, FutureSnapshot};
use crate::indexed::trace::{BlobTrace, TraceSnapshot};
use crate::storage::{Blob, Buffer, BufferHandle, BufferRead, BufferShared, BufferWrite, SeqNo};
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
pub struct Indexed<K, V, U: Buffer, L: Blob> {
    next_stream_id: Id,
    futures_seqno_upper: SeqNo,
    // This is conceptually a map from `String` -> `Id`, but lookups are rare
    // and this representation is optimized for the metadata serialization path,
    // which is less rare.
    id_mapping: Vec<(String, Id)>,
    graveyard: Vec<(String, Id)>,
    buf: U,
    blob: BlobCache<K, V, L>,
    futures: BTreeMap<Id, BlobFuture<K, V>>,
    traces: BTreeMap<Id, BlobTrace<K, V>>,
    listeners: BTreeMap<Id, Vec<ListenFn<K, V>>>,
}

/// TODO documentation
/// TODO: I absolutely do not understand why I can't express this in terms
/// of BufferWrite.
pub struct IndexedClient<K, V, U: Buffer, L: Blob> {
    ids: BTreeMap<String, Id>,
    graveyard: BTreeMap<String, Id>,
    seal_frontiers: BTreeMap<Id, u64>,
    since_frontiers: BTreeMap<Id, u64>,
    listeners: BTreeMap<Id, Vec<ListenFn<K, V>>>,
    next_stream_id: Id,
    buf_writer: BufferHandle<U>,
    blob: BlobCache<K, V, L>,
    meta_rx: crossbeam_channel::Receiver<BlobMeta>,
    meta: BlobMeta,
    handle: Option<JoinHandle<()>>,
}

struct IndexedCore<K, V, R: BufferRead, L: Blob> {
    next_stream_id: Id,
    futures_seqno_upper: SeqNo,
    // This is conceptually a map from `String` -> `Id`, but lookups are rare
    // and this representation is optimized for the metadata serialization path,
    // which is less rare.
    id_mapping: Vec<(String, Id)>,
    graveyard: Vec<(String, Id)>,
    buf_reader: R,
    blob: BlobCache<K, V, L>,
    futures: BTreeMap<Id, BlobFuture<K, V>>,
    traces: BTreeMap<Id, BlobTrace<K, V>>,
    meta_tx: crossbeam_channel::Sender<BlobMeta>,
}

impl<K, V, U, L> IndexedClient<K, V, U, L>
where
    K: Data + Send + Sync + 'static,
    V: Data + Send + Sync + 'static,
    U: Buffer + Send + 'static,
    L: Blob + Send + 'static,
{
    /// Returns a new IndexedClient, initializing IndexedCore with the appropriate data from
    /// the blob store, and reading out the rest of the metadata from the buffer.
    /// Spawns IndexedCore to run in a background thread.
    pub fn new(mut buf: U, blob: L) -> Result<Self, Error> {
        let (buf_reader, buf_writer) = buf.read_write()?;
        let blob = BlobCache::new(blob);

        let (meta_tx, meta_rx) = crossbeam_channel::bounded(1_000);

        let mut core = IndexedCore::new(buf_reader, blob.clone(), meta_tx)?;
        // Hydrate the core with any remaining data from buffer left
        // behind from last startup.
        core.drain_buf()?;

        let meta = core.serialize_meta();
        let core_f = move || {
            while core.work() {}
        };

        let handle = thread::Builder::new()
            .name("persist-indexed-core".to_string())
            .spawn(core_f)?;

        Ok(IndexedClient {
            ids: meta.id_mapping.iter().cloned().collect(),
            graveyard: meta.graveyard.iter().cloned().collect(),
            seal_frontiers: meta
                .traces
                .iter()
                .map(|meta| (meta.id, meta.seal[0]))
                .collect(),
            since_frontiers: meta
                .traces
                .iter()
                .map(|meta| (meta.id, meta.since[0]))
                .collect(),
            listeners: BTreeMap::new(),
            next_stream_id: meta.next_stream_id,
            buf_writer,
            blob,
            meta_rx,
            meta,
            handle: Some(handle),
        })
    }

    /// Releases exclusive-writer locks and causes all future commands to error.
    ///
    /// Needs to block and wait for IndexedCore to return as well.
    /// This method is idempotent.
    pub fn close(&mut self) -> Result<(), Error> {
        unimplemented!()
    }

    // Perform truncation and update the meta received from IndexedCore
    //
    // TODO: I really want to do this on a timer instead of on every call.
    fn maintenence(&mut self) -> Result<(), Error> {
        for meta in self.meta_rx.try_iter() {
            self.meta = meta;
        }

        self.buf_writer.truncate(self.meta.futures_seqno_upper)
    }

    /// Logically creates, if necessary, a new future and trace with the given external
    /// stream name, returning the corresponding internal stream id.
    ///
    /// This method is idempotent: ids may be registered multiple times.
    pub fn register(&mut self, id_str: &str) -> Result<Id, Error> {
        self.maintenence()?;
        if self.graveyard.contains_key(id_str) {
            return Err(Error::from(format!(
                "invalid registration: stream {} already destroyed",
                id_str
            )));
        }

        let id = match self.ids.get(id_str) {
            Some(id) => *id,
            None => {
                let id = self.next_stream_id;
                self.ids.insert(id_str.to_string(), id);
                self.since_frontiers.insert(id, 0);
                self.seal_frontiers.insert(id, 0);
                self.next_stream_id = Id(id.0 + 1);
                let entry: BufferEntry<K, V> = BufferEntry::Register(id, id_str.to_string());
                let mut entry_bytes = Vec::new();
                unsafe { abomonation::encode(&entry, &mut entry_bytes) }
                    .expect("write to Vec is infallible");
                self.buf_writer.write_sync(entry_bytes)?;
                id
            }
        };

        Ok(id)
    }

    /// Logically removes a stream from the index.
    ///
    /// This method is idempotent and may be called multiple times. It returns
    /// true if the stream was destroyed from this call, and false if it was
    /// already destroyed.
    pub fn destroy(&mut self, id_str: &str) -> Result<bool, Error> {
        unimplemented!()
    }

    /// Synchronously persists (Key, Value, Time, Diff) updates for the stream
    /// with the given id.
    pub fn write_sync(
        &mut self,
        updates: Vec<(Id, Vec<((K, V), u64, isize)>)>,
    ) -> Result<SeqNo, Error> {
        for (id, updates) in updates.iter() {
            match self.seal_frontiers.get(id) {
                Some(seal_frontier) => {
                    for update in updates.iter() {
                        if update.1 <= *seal_frontier {
                            return Err(format!(
                                "update for {:?} with time {} before sealed frontier: {:?}",
                                id, update.1, seal_frontier,
                            )
                            .into());
                        }
                    }
                }
                None => {
                    return Err(format!("invalid write to id: {:?} that doesn't exist.", id).into())
                }
            }
        }

        let entry = BufferEntry::Write(updates);
        let mut entry_bytes = Vec::new();
        unsafe { abomonation::encode(&entry, &mut entry_bytes) }
            .expect("write to Vec is infallible");
        let seqno = self.buf_writer.write_sync(entry_bytes)?;

        // WIP: TODO: this is very silly is there a better way to pull the list
        // of updates out of the entry?
        if let BufferEntry::Write(updates) = entry {
            for (id, updates) in updates.iter() {
                if let Some(listen_fns) = self.listeners.get(id) {
                    for listen_fn in listen_fns.iter() {
                        listen_fn(ListenEvent::Records(updates.clone()));
                    }
                }
            }
        }
        Ok(seqno)
    }

    /// Sealing a time advances the "sealed" frontier for an id, which restricts
    /// what times can later be sealed and written for that id. See
    /// `sealed_frontier` for details.
    pub fn seal(&mut self, ids: Vec<Id>, ts_upper: u64) -> Result<(), Error> {
        self.maintenence()?;
        for id in ids.iter() {
            match self.seal_frontiers.get(id) {
                Some(frontier) => {
                    if ts_upper <= *frontier {
                        return Err(format!("invalid seal for id {:?} not in advance of existing seal frontier {:?}: {:?}",
                                           id, frontier, ts_upper).into());
                    }
                }
                None => return Err(format!("invalid seal: id {:?} does not exist", id).into()),
            }
        }

        // Now that we've established all of the seal's are valid go ahead and write them.
        for id in ids.iter() {
            let ret = self.seal_frontiers.insert(*id, ts_upper);
            debug_assert!(ret.is_some());

            // TODO: does this belong here?
            if let Some(listen_fns) = self.listeners.get(&id) {
                for listen_fn in listen_fns.iter() {
                    listen_fn(ListenEvent::Sealed(ts_upper));
                }
            }
        }

        let entry: BufferEntry<K, V> = BufferEntry::Seal(ids, ts_upper);
        let mut entry_bytes = Vec::new();
        unsafe { abomonation::encode(&entry, &mut entry_bytes) }
            .expect("write to Vec is infallible");
        self.buf_writer.write_sync(entry_bytes)?;

        Ok(())
    }

    /// Returns a [Snapshot] for the given id.
    pub fn snapshot(&self, id: Id) -> Result<IndexedSnapshot<K, V>, Error> {
        // TODO this is kind of awkward maybe we should have forward and reverse
        // indexes for this.
        if !self.seal_frontiers.contains_key(&id) {
            return Err(format!("invalid snapshot: id {:?} not found", id).into());
        }
        let trace_meta = self.meta.traces.iter().find(|meta| meta.id == id);

        // TODO: we want to make these functions over the encoded representations
        // so that they can be shared between the trace and indexedclient impls.
        let trace = match trace_meta {
            None => TraceSnapshot {
                ts_upper: Antichain::from_elem(0),
                updates: vec![],
            },
            Some(trace_meta) => {
                let ts_upper = trace_meta.ts_upper();
                let mut updates = Vec::with_capacity(trace_meta.batches.len());
                for batch in trace_meta.batches.iter() {
                    updates.push(self.blob.get_trace_batch(&batch.key)?);
                }

                TraceSnapshot { ts_upper, updates }
            }
        };

        let future_meta = self.meta.futures.iter().find(|meta| meta.id == id);
        let future = match future_meta {
            None => FutureSnapshot {
                seqno_upper: Antichain::from_elem(SeqNo(0)),
                ts_lower: trace.ts_upper.clone(),
                ts_upper: Antichain::new(),
                updates: vec![],
            },
            Some(future_meta) => {
                let mut updates = Vec::with_capacity(future_meta.batches.len());
                for batch in future_meta.batches.iter() {
                    // TODO: filter out excess
                    updates.push(self.blob.get_future_batch(&batch.key)?);
                }

                FutureSnapshot {
                    seqno_upper: future_meta.seqno_upper(),
                    ts_lower: trace.ts_upper.clone(),
                    ts_upper: Antichain::new(),
                    updates,
                }
            }
        };

        // TODO: should this be meta.future_seqno_upper instead?
        let buf_lower = &future.seqno_upper;
        let buffer = {
            let mut data = Vec::new();
            let seqno = self
                .buf_writer
                .snapshot(|seqno, buf| {
                    let entry: Abomonated<BufferEntry<K, V>, Vec<u8>> =
                        unsafe { Abomonated::new(buf.to_owned()) }
                            .ok_or_else(|| Error::from(format!("invalid buffer entry")))?;
                    // WIP: TODO: is this the right way to work with Abomonated?
                    let entry: BufferEntry<K, V> = (*entry).clone();
                    match entry {
                        BufferEntry::Write(updates) => {
                            for (entry_id, updates) in updates.into_iter() {
                                if entry_id != id || !buf_lower.less_equal(&seqno) {
                                    continue;
                                }
                                data.extend(updates.into_iter());
                            }
                        }
                        _ => (),
                    }
                    Ok(())
                })?
                .end;
            BufferSnapshot(seqno, data)
        };

        Ok(IndexedSnapshot(buffer, future, trace))
    }
}

impl<K, V, R, L> IndexedCore<K, V, R, L>
where
    K: Data + Send + Sync + 'static,
    V: Data + Send + Sync + 'static,
    R: BufferRead + Send + 'static,
    L: Blob + Send + 'static,
{
    fn new(
        mut buf_reader: R,
        mut blob: BlobCache<K, V, L>,
        meta_tx: crossbeam_channel::Sender<BlobMeta>,
    ) -> Result<Self, Error> {
        let meta = blob
            .get_meta()
            .map_err(|err| {
                // Indexed is expected to close the buffer and blob it's handed.
                // Usually that happens when close is called on Indexed itself,
                // but if there's an error constructing it, we never get to that
                // point and have to clean up ourselves.
                //
                // TODO: Regression test for this.
                if let Err(err) = buf_reader.close() {
                    log::warn!("error closing buffer reader: {}", err);
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

        debug_assert!(meta_tx.capacity().unwrap_or(0) > 0);
        let indexed = Self {
            next_stream_id: meta.next_stream_id,
            futures_seqno_upper: meta.futures_seqno_upper,
            id_mapping: meta.id_mapping,
            graveyard: meta.graveyard,
            buf_reader,
            blob,
            futures,
            traces,
            meta_tx,
        };

        indexed.check_invariants()?;
        Ok(indexed)
    }

    /// Sanity check invariants at runtime.
    fn check_invariants(&self) -> Result<(), Error> {
        if cfg!(any(test, debug)) {
            let stored_meta = self.blob.get_meta()?.unwrap_or_default();
            let local_meta = self.serialize_meta();

            assert_eq!(stored_meta, local_meta);
            local_meta.validate()?;
        }

        Ok(())
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

    /// Drains writes from the buffer into the future and does any necessary
    /// resulting compaction work.
    ///
    /// In production, step should just be called in a loop (probably with some
    /// smarts about waiting to call it only after there have been some writes),
    /// but it's exposed this way so we can write deterministic tests.
    pub fn work(&mut self) -> bool {
        match self.step() {
            Ok(ret) => ret,
            Err(_) => false,
        }
    }

    fn step(&mut self) -> Result<bool, Error> {
        self.check_invariants()?;
        self.drain_buf()?;
        self.drain_future()?;
        // TODO: Incrementally compact future and trace.
        let meta = self.serialize_meta();
        self.blob.set_meta(meta.clone())?;
        // WIP: TODO: handle error better
        // WIP: TODO: need to have a provision for overflow
        if !self.meta_tx.is_full() {
            self.meta_tx
                .send(meta)
                .expect("wip cannot fail unless receiver dropped");
        }
        Ok(true)
    }

    /// Atomically moves all writes currently in the buffer into the future.
    fn drain_buf(&mut self) -> Result<(), Error> {
        let mut updates_by_id: BTreeMap<Id, Vec<(SeqNo, (K, V), u64, isize)>> = BTreeMap::new();
        let mut creates_deletes_by_id: BTreeMap<Id, (String, bool)> = BTreeMap::new();
        let mut seals_by_id: BTreeMap<Id, u64> = BTreeMap::new();
        let mut sinces_by_id: BTreeMap<Id, u64> = BTreeMap::new();

        let desc = self.buf_reader.snapshot(|seqno, buf| {
            if seqno < self.futures_seqno_upper {
                return Ok(());
            }
            let mut buf = buf.to_vec();
            let (entry, remaining) = unsafe { abomonation::decode::<BufferEntry<K, V>>(&mut buf) }
                .ok_or_else(|| Error::from(format!("invalid buffer entry")))?;
            if !remaining.is_empty() {
                return Err(format!("invalid buffer entry").into());
            }

            match entry {
                BufferEntry::Write(updates) => {
                    for (id, updates) in updates.iter() {
                        // iter and cloned instead of append because I don't have a mental
                        // model of what's safe with abomonation.
                        updates_by_id
                            .entry(*id)
                            .or_default()
                            .extend(updates.iter().map(|((key, val), ts, diff)| {
                                (seqno, (key.clone(), val.clone()), *ts, *diff)
                            }));
                    }
                }
                BufferEntry::Register(id, name) => {
                    creates_deletes_by_id.insert(*id, (name.clone(), true));
                }
                BufferEntry::Seal(ids, time) => {
                    for id in ids.iter() {
                        seals_by_id.insert(*id, *time);
                    }
                }
            }

            Ok(())
        })?;

        if desc.start == desc.end {
            // No updates, can exit early.
            // WIP: TODO: check some invariants
            return Ok(());
        }

        for (id, (name, is_create)) in creates_deletes_by_id.into_iter() {
            if is_create {
                debug_assert_eq!(id, self.next_stream_id);
                self.id_mapping.push((name.to_owned(), id));
                self.next_stream_id = Id(id.0 + 1);
                self.futures
                    .insert(id, BlobFuture::new(BlobFutureMeta::new(id)));
                self.traces
                    .insert(id, BlobTrace::new(BlobTraceMeta::new(id)));
            } else {
                debug_assert!(id <= self.next_stream_id);
                if id == self.next_stream_id {
                    // This stream was created and destroyed
                    continue;
                }
                // TODO Handle deletes here
            }
        }

        for (id, ts_upper) in seals_by_id.into_iter() {
            let trace = self
                .traces
                .get_mut(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

            trace.update_seal(ts_upper)?;
        }

        for (id, updates) in updates_by_id.into_iter() {
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
        // We need to fully persist our state here in case we end up needing
        // to rehydrate from Buffer after restart.
        self.blob.set_meta(self.serialize_meta())?;

        self.buf_reader.allow_truncation(desc.end)
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
        if cfg!(any(debug, test)) {
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

    /// Move data at times that have been sealed from future to
    /// trace.
    /// The caller is responsible for updating META after they've finished
    /// updating the traces.
    fn drain_future(&mut self) -> Result<(), Error> {
        for (id, trace) in self.traces.iter_mut() {
            // If this future is already properly sealed then we don't need
            // to do anything.
            let seal = trace.seal();
            let trace_upper = trace.ts_upper();
            if seal == trace_upper {
                continue;
            }

            let future = self
                .futures
                .get_mut(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

            let desc = Description::new(trace_upper, seal, trace.since());
            if PartialOrder::less_equal(desc.upper(), desc.lower()) {
                return Err(format!("invalid batch bounds: {:?}", desc).into());
            }

            // Move a batch of data from future into trace by reading a
            // snapshot from future...
            let mut updates = Vec::new();
            {
                let mut snap =
                    future.snapshot(desc.lower().clone(), desc.upper().clone(), &self.blob)?;
                while snap.read(&mut updates) {}
            }

            // Trace batches are required to be sorted and consolidated by ((k, v), t)
            differential_dataflow::consolidation::consolidate_updates(&mut updates);

            // ...and atomically swapping that snapshot's data into trace.
            let batch = BlobTraceBatch { desc, updates };
            // TODO: lifetime issues
            // I think this is all worth moving into drain_future_inner
            //self.append_trace(*id, batch)?;
            let new_future_ts_lower = batch.desc.upper().clone();
            trace.append(batch, &mut self.blob)?;
            future.truncate(new_future_ts_lower)?;
        }

        Ok(())
    }
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
            id_mapping: meta.id_mapping,
            graveyard: meta.graveyard,
            buf,
            blob,
            futures,
            traces,
            listeners: BTreeMap::new(),
        };

        indexed.check_invariants()?;
        Ok(indexed)
    }

    /// Sanity check invariants at runtime.
    fn check_invariants(&self) -> Result<(), Error> {
        if cfg!(any(test, debug)) {
            let stored_meta = self.blob.get_meta()?.unwrap_or_default();
            let local_meta = self.serialize_meta();

            assert_eq!(stored_meta, local_meta);
            local_meta.validate()?;
        }

        Ok(())
    }

    /// Releases exclusive-writer locks and causes all future commands to error.
    ///
    /// This method is idempotent.
    pub fn close(&mut self) -> Result<(), Error> {
        self.check_invariants()?;
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

    /// Creates, if necessary, a new future and trace with the given external
    /// stream name, returning the corresponding internal stream id.
    ///
    /// This method is idempotent: ids may be registered multiple times.
    pub fn register(&mut self, id_str: &str) -> Result<Id, Error> {
        self.check_invariants()?;
        if self.graveyard.iter().any(|(s, _)| s == &id_str) {
            return Err(Error::from(format!(
                "invalid registration: stream {} already destroyed",
                id_str
            )));
        }
        let id = self.id_mapping.iter().find(|(s, _)| s == &id_str);
        let id = match id {
            Some((_, id)) => *id,
            None => {
                let id = self.next_stream_id;
                self.id_mapping.push((id_str.to_owned(), id));
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

        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())?;
        Ok(id)
    }

    /// Removes a stream from the index.
    ///
    /// This method is idempotent and may be called multiple times. It returns
    /// true if the stream was destroyed from this call, and false if it was
    /// already destroyed.
    pub fn destroy(&mut self, id_str: &str) -> Result<bool, Error> {
        self.check_invariants()?;
        if self
            .graveyard
            .iter()
            .any(|(destroyed_name, _)| destroyed_name == &id_str)
        {
            return Ok(false);
        }

        let mapping = self.id_mapping.iter().find(|(name, _)| name == &id_str);

        let mapping = match mapping {
            Some(mapping) => mapping.clone(),
            None => {
                return Err(Error::from(format!(
                    "invalid destroy of stream {} that was never registered or destroyed",
                    id_str
                )));
            }
        };

        // TODO: actually physically delete the future and trace batches.
        let future = self.futures.remove(&mapping.1);
        let trace = self.traces.remove(&mapping.1);

        // Sanity check that we actually removed the future and trace for this
        // stream.
        debug_assert!(future.is_some());
        debug_assert!(trace.is_some());

        self.id_mapping.retain(|id_mapping| id_mapping != &mapping);
        self.graveyard.push(mapping);
        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())?;

        return Ok(true);
    }

    /// Drains writes from the buffer into the future and does any necessary
    /// resulting compaction work.
    ///
    /// In production, step should just be called in a loop (probably with some
    /// smarts about waiting to call it only after there have been some writes),
    /// but it's exposed this way so we can write deterministic tests.
    pub fn step(&mut self) -> Result<(), Error> {
        self.check_invariants()?;
        self.drain_buf()?;
        self.drain_future()
        // TODO: Incrementally compact future.
    }

    /// Synchronously persists (Key, Value, Time, Diff) updates for the stream
    /// with the given id.
    pub fn write_sync(
        &mut self,
        updates: Vec<(Id, Vec<((K, V), u64, isize)>)>,
    ) -> Result<SeqNo, Error> {
        self.check_invariants()?;
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

        let entry = BufferEntry::Write(updates);
        let mut entry_bytes = Vec::new();
        unsafe { abomonation::encode(&entry, &mut entry_bytes) }
            .expect("write to Vec is infallible");
        let seqno = self.buf.write_sync(entry_bytes)?;

        // WIP: TODO: this is very silly is there a better way to pull the list
        // of updates out of the entry?
        if let BufferEntry::Write(updates) = entry {
            for (id, updates) in updates.iter() {
                if let Some(listen_fns) = self.listeners.get(id) {
                    for listen_fn in listen_fns.iter() {
                        listen_fn(ListenEvent::Records(updates.clone()));
                    }
                }
            }
        }
        Ok(seqno)
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

            match entry {
                BufferEntry::Write(updates) => {
                    for (id, updates) in updates.iter() {
                        // iter and cloned instead of append because I don't have a mental
                        // model of what's safe with abomonation.
                        updates_by_id
                            .entry(*id)
                            .or_default()
                            .extend(updates.iter().map(|((key, val), ts, diff)| {
                                (seqno, (key.clone(), val.clone()), *ts, *diff)
                            }));
                    }
                }
                BufferEntry::Seal(_, _) | BufferEntry::Register(_, _) => unimplemented!(),
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
        if cfg!(any(debug, test)) {
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

    /// Move data at times that have been sealed from future to
    /// trace.
    fn drain_future(&mut self) -> Result<(), Error> {
        for (id, trace) in self.traces.iter_mut() {
            // If this future is already properly sealed then we don't need
            // to do anything.
            let seal = trace.seal();
            let trace_upper = trace.ts_upper();
            if seal == trace_upper {
                continue;
            }

            let future = self
                .futures
                .get_mut(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

            let desc = Description::new(trace_upper, seal, trace.since());
            if PartialOrder::less_equal(desc.upper(), desc.lower()) {
                return Err(format!("invalid batch bounds: {:?}", desc).into());
            }

            // Move a batch of data from future into trace by reading a
            // snapshot from future...
            let mut updates = Vec::new();
            {
                let mut snap =
                    future.snapshot(desc.lower().clone(), desc.upper().clone(), &self.blob)?;
                while snap.read(&mut updates) {}
            }

            // Trace batches are required to be sorted and consolidated by ((k, v), t)
            differential_dataflow::consolidation::consolidate_updates(&mut updates);

            // ...and atomically swapping that snapshot's data into trace.
            let batch = BlobTraceBatch { desc, updates };
            // TODO: lifetime issues
            // I think this is all worth moving into drain_future_inner
            //self.append_trace(*id, batch)?;
            let new_future_ts_lower = batch.desc.upper().clone();
            trace.append(batch, &mut self.blob)?;
            future.truncate(new_future_ts_lower)?;
        }

        // TODO: This is a good point to compact future. The data that's been
        // moved is still there but now irrelevant. It may also be a good time
        // to compact trace.

        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())?;
        Ok(())
    }

    /// Returns the current "sealed" frontier for an id.
    ///
    /// This frontier represents a contract of time such that all updates with a
    /// time less than it have arrived. This frontier is advanced though the
    /// `seal` method. Once a time has been sealed for an id, it becomes an
    /// error to later seal it at an time less than or equal to the sealed
    /// frontier. It is also an error to write new data with a time less than
    /// the sealed frontier.
    fn sealed_frontier(&mut self, id: Id) -> Result<Antichain<u64>, Error> {
        self.check_invariants()?;
        let trace = self
            .traces
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        Ok(trace.seal())
    }

    /// Atomically moves all writes in future not in advance of the given
    /// timestamp into the trace and does any necessary resulting compaction
    /// work.
    ///
    /// Sealing a time advances the "sealed" frontier for an id, which restricts
    /// what times can later be sealed and written for that id. See
    /// `sealed_frontier` for details.
    pub fn seal(&mut self, ids: Vec<Id>, ts_upper: u64) -> Result<(), Error> {
        self.check_invariants()?;
        // TODO: Separate the logical work of seal which just disallows future
        // updates and seals at times <= ts_upper from the physical work of
        // moving things from the future to the trace. This could let us
        // amortize the work of doing so across frequent seal calls? All the
        // physical movement could live in `step`.

        for id in ids.into_iter() {
            let trace = self
                .traces
                .get_mut(&id)
                .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

            trace.update_seal(ts_upper)?;

            // TODO: does this belong here?
            if let Some(listen_fns) = self.listeners.get(&id) {
                for listen_fn in listen_fns.iter() {
                    listen_fn(ListenEvent::Sealed(ts_upper));
                }
            }
        }

        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())
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
        self.check_invariants()?;
        let trace = self
            .traces
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        let since = Antichain::from_elem(since);

        trace.allow_compaction(since)?;
        // Atomically update the meta with both the trace and future changes.
        //
        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())
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

    /// Returns a [Snapshot] for the given id.
    pub fn snapshot(&self, id: Id) -> Result<IndexedSnapshot<K, V>, Error> {
        self.check_invariants()?;
        let future = self
            .futures
            .get(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        let trace = self
            .traces
            .get(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
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
                    // WIP: TODO: is this the right way to work with Abomonated?
                    let entry: BufferEntry<K, V> = (*entry).clone();
                    match entry {
                        BufferEntry::Write(updates) => {
                            for (entry_id, updates) in updates.into_iter() {
                                if entry_id != id || !buf_lower.less_equal(&seqno) {
                                    continue;
                                }
                                data.extend(updates.into_iter());
                            }
                        }
                        BufferEntry::Seal(_, _) | BufferEntry::Register(_, _) => (),
                    }
                    Ok(())
                })?
                .end;
            BufferSnapshot(seqno, data)
        };

        Ok(IndexedSnapshot(buffer, future, trace))
    }

    /// Registers a callback to be invoked on successful writes and seals.
    //
    // TODO: Finish the naming bikeshed for this. Other options so far include
    // tail, subscribe, tee, inspect, and capture.
    pub fn listen(&mut self, id: Id, listen_fn: ListenFn<K, V>) -> Result<(), Error> {
        self.check_invariants()?;
        // Verify that id has been registered.
        let _ = self.sealed_frontier(id)?;
        self.listeners.entry(id).or_default().push(listen_fn);
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

        // Verify that the listener got a copy of the writes.
        let listen_received = {
            let mut buf = Vec::new();
            while let Ok(x) = listen_rx.try_recv() {
                buf.push(x);
            }
            buf
        };
        assert_eq!(listen_received, updates);

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

        // Can advance compaction frontier to a time that has already been sealed
        i.allow_compaction(id, 2)?;

        Ok(())
    }

    #[test]
    fn test_indexed_client() -> Result<(), Box<dyn Error>> {
        let updates = vec![
            (("1".to_string(), "".to_string()), 1, 1),
            (("2".to_string(), "".to_string()), 2, 1),
        ];

        let mut i = IndexedClient::new(
            MemBuffer::new("indexed_client"),
            MemBlob::new("indexed_client"),
        )?;
        let id = i.register("0")?;
        // After a write, all data is in the buffer.
        i.write_sync(vec![(id, updates.clone())])?;
        assert_eq!(i.snapshot(id)?.read_to_end(), updates);
        i.seal(vec![id], 2)?;
        assert_eq!(i.snapshot(id)?.read_to_end(), updates);
        assert_eq!(i.snapshot(id)?.read_to_end(), updates);

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
