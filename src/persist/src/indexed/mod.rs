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
    StreamRegistration,
};
use crate::indexed::future::{BlobFuture, FutureSnapshot};
use crate::indexed::trace::{BlobTrace, TraceSnapshot};
use crate::storage::{Blob, Buffer, SeqNo};

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
pub struct Indexed<U: Buffer, L: Blob> {
    next_stream_id: Id,
    futures_seqno_upper: SeqNo,
    // This is conceptually a map from `String` -> `Id`, but lookups are rare
    // and this representation is optimized for the metadata serialization path,
    // which is less rare.
    id_mapping: Vec<StreamRegistration>,
    graveyard: Vec<StreamRegistration>,
    buf: U,
    blob: BlobCache<L>,
    futures: HashMap<Id, BlobFuture>,
    traces: HashMap<Id, BlobTrace>,
    listeners: HashMap<Id, Vec<ListenFn<Vec<u8>, Vec<u8>>>>,
}

impl<U: Buffer, L: Blob> Indexed<U, L> {
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

    /// Creates, if necessary, a new future and trace with the given external
    /// stream name, returning the corresponding internal stream id.
    ///
    /// This method is idempotent: ids may be registered multiple times.
    pub fn register(
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
        self.futures
            .entry(id)
            .or_insert_with_key(|id| BlobFuture::new(BlobFutureMeta::new(*id)));
        self.traces
            .entry(id)
            .or_insert_with_key(|id| BlobTrace::new(BlobTraceMeta::new(*id)));
        Ok(id)
    }

    /// Removes a stream from the index.
    ///
    /// This method is idempotent and may be called multiple times. It returns
    /// true if the stream was destroyed from this call, and false if it was
    /// already destroyed.
    pub fn destroy(&mut self, id_str: &str) -> Result<bool, Error> {
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

        // TODO: actually physically delete the future and trace batches.
        let future = self.futures.remove(&mapping.id);
        let trace = self.traces.remove(&mapping.id);

        // Sanity check that we actually removed the future and trace for this
        // stream.
        debug_assert!(future.is_some());
        debug_assert!(trace.is_some());

        self.graveyard.push(mapping);

        return Ok(true);
    }

    /// Drains writes from the buffer into the future and does any necessary
    /// resulting compaction work.
    ///
    /// In production, step should just be called in a loop (probably with some
    /// smarts about waiting to call it only after there have been some writes),
    /// but it's exposed this way so we can write deterministic tests.
    pub fn step(&mut self) -> Result<(), Error> {
        self.drain_buf()?;
        self.drain_future()
        // TODO: Incrementally compact future.
    }

    /// Synchronously persists (Key, Value, Time, Diff) updates for the stream
    /// with the given id.
    pub fn write_sync(
        &mut self,
        updates: Vec<(Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>)>,
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

        let entry = BufferEntry { updates };
        let mut entry_bytes = Vec::new();
        unsafe { abomonation::encode(&entry, &mut entry_bytes) }
            .expect("write to Vec is infallible");
        let seqno = self.buf.write_sync(entry_bytes)?;
        Ok(seqno)
    }

    /// Atomically moves all writes currently in the buffer into the future.
    fn drain_buf(&mut self) -> Result<(), Error> {
        let mut updates_by_id: HashMap<Id, Vec<(SeqNo, (Vec<u8>, Vec<u8>), u64, isize)>> =
            HashMap::new();
        let desc = self.buf.snapshot(|seqno, buf| {
            let mut buf = buf.to_vec();
            let (entry, remaining) = unsafe { abomonation::decode::<BufferEntry>(&mut buf) }
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
        mut updates: Vec<(SeqNo, (Vec<u8>, Vec<u8>), u64, isize)>,
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
    fn drain_future(&mut self) -> Result<(), Error> {
        let mut updates_by_id = vec![];
        for (id, trace) in self.traces.iter_mut() {
            // If this future is already properly sealed then we don't need
            // to do anything.
            let seal = trace.get_seal();
            let trace_upper = trace.ts_upper();
            if seal == trace_upper {
                continue;
            }

            let future = self
                .futures
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
            updates_by_id.push((*id, seal, updates.clone()));

            // ...and atomically swapping that snapshot's data into trace.
            let batch = BlobTraceBatch { desc, updates };
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

        for (id, seal, updates) in updates_by_id {
            if let Some(listen_fns) = self.listeners.get(&id) {
                for listen_fn in listen_fns.iter() {
                    listen_fn(ListenEvent::Records(updates.clone()));
                    listen_fn(ListenEvent::Sealed(seal[0]));
                }
            }
        }

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

    /// Sealing a time advances the "sealed" frontier for an id, which restricts
    /// what times can later be sealed and written for that id. See
    /// `sealed_frontier` for details.
    pub fn seal(&mut self, ids: Vec<Id>, seal_ts: u64) -> Result<(), Error> {
        let prev = self.validate_seal(&ids, seal_ts)?;

        // Update in-memory state to reflect the new seal.
        // TODO: we should keep the in-memory state immutable until after the
        // results have been safely committed to durable storage. For now this
        // protocol is simple enough to reason about.
        for id in ids.iter() {
            let trace = self.traces.get_mut(id).expect("trace known to exist");

            trace.update_seal(seal_ts);
        }

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
    fn append_future(&mut self, id: Id, batch: BlobFutureBatch) -> Result<(), Error> {
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
    pub fn snapshot(&self, id: Id) -> Result<IndexedSnapshot, Error> {
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
                    let entry: Abomonated<BufferEntry, Vec<u8>> =
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

    /// Registers a callback to be invoked on successful writes and seals.
    //
    // TODO: Finish the naming bikeshed for this. Other options so far include
    // tail, subscribe, tee, inspect, and capture.
    pub fn listen(&mut self, id: Id, listen_fn: ListenFn<Vec<u8>, Vec<u8>>) -> Result<(), Error> {
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
//
// TODO: This <K, V> allows Snapshot to be generic over both IndexedSnapshot
// (and friends) and DecodedSnapshot, but does that get us anything? Does
// Snapshot even get us anything over regular Iterator?
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
struct BufferSnapshot(SeqNo, Vec<((Vec<u8>, Vec<u8>), u64, isize)>);

impl Snapshot<Vec<u8>, Vec<u8>> for BufferSnapshot {
    fn read<E: Extend<((Vec<u8>, Vec<u8>), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        buf.extend(self.1.drain(..));
        false
    }
}

/// A consistent snapshot of all data currently stored for an id.
#[derive(Debug)]
pub struct IndexedSnapshot(BufferSnapshot, FutureSnapshot, TraceSnapshot);

impl IndexedSnapshot {
    /// Returns the SeqNo at which this snapshot was run.
    ///
    /// All writes assigned a seqno < this are included.
    pub fn seqno(&self) -> SeqNo {
        self.0 .0
    }
}

impl Snapshot<Vec<u8>, Vec<u8>> for IndexedSnapshot {
    fn read<E: Extend<((Vec<u8>, Vec<u8>), u64, isize)>>(&mut self, buf: &mut E) -> bool {
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

    fn record_with_seqno(seqno: u64) -> (SeqNo, (Vec<u8>, Vec<u8>), u64, isize) {
        (SeqNo(seqno), ("".into(), "".into()), 1, 1)
    }

    #[test]
    fn single_stream() -> Result<(), Box<dyn Error>> {
        let updates = vec![
            (("1".into(), "".into()), 1, 1),
            (("2".into(), "".into()), 2, 1),
        ];

        let mut i = Indexed::new(
            MemBuffer::new("single_stream"),
            MemBlob::new("single_stream"),
        )?;
        let id = i.register("0", "()", "()")?;

        // Empty things are empty.
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(buf.read_to_end(), vec![]);
        assert_eq!(future.read_to_end(), vec![]);
        assert_eq!(trace.read_to_end(), vec![]);

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
            (("1".into(), "".into()), 2, 1),
            (("2".into(), "".into()), 1, 1),
        ];

        let mut i = Indexed::new(
            MemBuffer::new("batch_sorting"),
            MemBlob::new("batch_sorting"),
        )?;
        let id = i.register("0", "", "")?;

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
            (("1".into(), "".into()), 1, 1),
            (("1".into(), "".into()), 1, 1),
        ];

        let mut i = Indexed::new(
            MemBuffer::new("batch_consolidation"),
            MemBlob::new("batch_consolidation"),
        )?;
        let id = i.register("0", "", "")?;

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
        let id = i.register("0", "", "")?;

        // Write an empty set of updates and try to move it into the future part
        // of the index.
        i.write_sync(vec![(id, vec![])])?;
        i.step()?;

        // Sending updates with dif = 0.
        let updates = vec![(("1".into(), "".into()), 1, 0)];
        i.write_sync(vec![(id, updates)])?;
        i.step()?;

        // Now try again with a set of updates that consolidates down to the empty
        // set.
        let updates = vec![
            (("1".into(), "".into()), 1, 2),
            (("1".into(), "".into()), 1, -2),
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
        let id = i.register("0", "", "")?;

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
        let s1 = i.register("s1", "", "")?;
        i.write_sync(vec![(s1, vec![(("".into(), "".into()), 0, 1)])])?;
        i.step()?;
        let s2 = i.register("s2", "", "")?;
        i.write_sync(vec![(s2, vec![(("".into(), "".into()), 1, 1)])])?;
        i.step()?;

        // The second flavor is similar. If we then write to the first stream
        // again and step, it is then missing X..Y. (A stream not written to
        // between two step calls doesn't get a batch.)
        i.write_sync(vec![(s1, vec![(("".into(), "".into()), 2, 1)])])?;
        i.step()?;

        Ok(())
    }

    #[test]
    fn test_destroy() -> Result<(), IndexedError> {
        let mut i = Indexed::new(MemBuffer::new("destroy"), MemBlob::new("destroy"))?;

        let _ = i.register("stream", "", "")?;

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
            i.register("stream", "", ""),
            Err(IndexedError::from(
                "invalid registration: stream stream already destroyed"
            ))
        );

        Ok(())
    }

    #[test]
    fn codec_mismatch() -> Result<(), IndexedError> {
        let mut i = Indexed::new(
            MemBuffer::new("codec_mismatch"),
            MemBlob::new("codec_mismatch"),
        )?;

        let _ = i.register("stream", "key", "val")?;

        // Normal case: registration uses same key and value codec.
        let _ = i.register("stream", "key", "val")?;

        // Different key codec
        assert_eq!(
            i.register("stream", "nope", "val"),
            Err(IndexedError::from(
                "invalid registration: key codec mismatch nope vs previous key"
            ))
        );

        // Different val codec
        assert_eq!(
            i.register("stream", "key", "nope"),
            Err(IndexedError::from(
                "invalid registration: val codec mismatch nope vs previous val"
            ))
        );

        Ok(())
    }
}
