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
pub mod future;
pub mod handle;
pub mod trace;

use std::collections::HashMap;
use std::time::SystemTime;

use abomonation::abomonated::Abomonated;
use abomonation_derive::Abomonation;
use differential_dataflow::trace::Description;
use timely::progress::Antichain;
use timely::PartialOrder;

use crate::error::Error;
use crate::indexed::cache::BlobCache;
use crate::indexed::future::{BlobFuture, FutureSnapshot};
use crate::indexed::trace::{BlobTrace, TraceSnapshot};
use crate::persister::Snapshot;
use crate::storage::{Blob, Buffer, SeqNo};
use crate::Id;

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
    blob: BlobCache<L>,
    last_file_id: u128,
    buf: U,
    futures: HashMap<Id, BlobFuture>,
    traces: HashMap<Id, BlobTrace>,
}

impl<U: Buffer, L: Blob> Indexed<U, L> {
    /// Returns a new Indexed, initializing each Future and Trace with the
    /// existing data for them in the blob storage, if any.
    pub fn new(buf: U, blob: L) -> Result<Self, Error> {
        let blob = BlobCache::new(blob);
        let meta = blob.get_meta()?.unwrap_or_else(|| BlobMeta {
            last_file_id: Self::now_millis(),
            futures: Vec::new(),
            traces: Vec::new(),
        });
        let futures = meta
            .futures
            .into_iter()
            .map(|(id, meta)| (Id(id), BlobFuture::new(meta)))
            .collect();
        let traces = meta
            .traces
            .into_iter()
            .map(|(id, meta)| (Id(id), BlobTrace::new(meta)))
            .collect();
        let indexed = Indexed {
            blob,
            last_file_id: meta.last_file_id,
            buf,
            futures,
            traces,
        };
        Ok(indexed)
    }

    fn new_blob_key(&mut self) -> String {
        // TODO: Use meaningful file names? Something like id+desc might be
        // useful when debugging.
        let mut file_id = Self::now_millis();
        if file_id <= self.last_file_id {
            file_id = self.last_file_id + 1;
        }
        self.last_file_id = file_id;
        file_id.to_string()
    }

    fn now_millis() -> u128 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    }

    /// Creates, if necessary, a new future and trace with the given id.
    ///
    /// This method is idempotent: ids may be registered multiple times. TODO:
    /// Is this idempotence necessary/useful?
    pub fn register(&mut self, id: Id) {
        self.futures.entry(id).or_default();
        self.traces.entry(id).or_default();
    }

    /// Drains writes from the buffer into the future and does any necessary
    /// resulting compaction work.
    ///
    /// In production, step should just be called in a loop (probably with some
    /// smarts about waiting to call it only after there have been some writes),
    /// but it's exposed this way so we can write deterministic tests.
    pub fn step(&mut self) -> Result<(), Error> {
        self.drain_buf()
        // TODO: Incrementally compact future.
    }

    /// Synchronously persists (Key, Value, Time, Diff) updates for the stream
    /// with the given id.
    pub fn write_sync(
        &mut self,
        id: Id,
        updates: &[((String, String), u64, isize)],
    ) -> Result<(), Error> {
        let sealed_frontier = self.sealed_frontier(id)?;
        for update in updates.iter() {
            if !sealed_frontier.less_equal(&update.1) {
                return Err(format!(
                    "update for {:?} with time before sealed frontier {:?}: {:?}",
                    id, sealed_frontier, update
                )
                .into());
            }
        }

        let entry = BufferEntry {
            id: id.0,
            updates: updates.to_vec(),
        };
        let mut entry_bytes = Vec::new();
        unsafe { abomonation::encode(&entry, &mut entry_bytes) }
            .expect("write to Vec is infallible");
        self.buf.write_sync(entry_bytes)?;
        Ok(())
    }

    /// Atomically moves all writes currently in the buffer into the future.
    fn drain_buf(&mut self) -> Result<(), Error> {
        let mut updates_by_id: HashMap<u64, Vec<(SeqNo, (String, String), u64, isize)>> =
            HashMap::new();
        let desc = self.buf.snapshot(|seqno, buf| {
            let mut buf = buf.to_vec();
            let (entry, remaining) = unsafe { abomonation::decode::<BufferEntry>(&mut buf) }
                .ok_or_else(|| Error::from(format!("invalid buffer entry")))?;
            if !remaining.is_empty() {
                return Err(format!("invalid buffer entry").into());
            }
            // iter and cloned instead of append because I don't have a mental
            // model of what's safe with abomonation.
            updates_by_id.entry(entry.id).or_default().extend(
                entry
                    .updates
                    .iter()
                    .map(|((key, val), ts, diff)| (seqno, (key.clone(), val.clone()), *ts, *diff)),
            );
            Ok(())
        })?;
        for (id, updates) in updates_by_id.drain() {
            let batch = BlobFutureBatch {
                id,
                desc: Description::new(
                    Antichain::from_elem(desc.start),
                    Antichain::from_elem(desc.end),
                    // We never compact BlobFuture, so since is always the minimum.
                    Antichain::from_elem(SeqNo(0)),
                ),
                updates: updates,
            };
            let key = self.new_blob_key();
            self.append_future(key, batch)?;
        }
        self.buf.truncate(desc.end)
    }

    /// Returns the current "sealed" frontier for an id.
    ///
    /// This frontier represents a contract of time such that all updates with a
    /// time less than it have arrived. This frontier is advanced though the
    /// `seal` method. Once a time has been sealed for an id, it becomes an
    /// error to later seal it at an time less than or equal to the sealed
    /// frontier. It is also an error to write new data with a time less than
    /// the sealed frontier.
    pub fn sealed_frontier(&mut self, id: Id) -> Result<Antichain<u64>, Error> {
        let trace = self
            .traces
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        Ok(trace.ts_upper())
    }

    /// Atomically moves all writes in future not in advance of the given
    /// timestamp into the trace and does any necessary resulting compaction
    /// work.
    ///
    /// Sealing a time advances the "sealed" frontier for an id, which restricts
    /// what times can later be sealed and written for that id. See
    /// `sealed_frontier` for details.
    pub fn seal(&mut self, id: Id, ts_upper: u64) -> Result<(), Error> {
        // TODO: Separate the logical work of seal which just disallows future
        // updates and seals at times <= ts_upper from the physical work of
        // moving things from the future to the trace. This could let us
        // amortize the work of doing so across frequent seal calls? All the
        // physical movement could live in `step`.

        let key = self.new_blob_key();
        let future = self
            .futures
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        let trace = self
            .traces
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

        let batch_upper = Antichain::from_elem(ts_upper);
        let desc = Description::new(trace.ts_upper(), batch_upper, trace.since());
        if PartialOrder::less_equal(desc.upper(), desc.lower()) {
            return Err(format!("invalid batch bounds: {:?}", desc).into());
        }

        // Atomically move a batch of data from future into trace by reading a
        // snapshot from future...
        let mut updates = Vec::new();
        {
            let mut snap =
                future.snapshot(desc.lower().clone(), Some(desc.upper().clone()), &self.blob)?;
            while snap.read(&mut updates) {}
        }

        // ...writing that snapshot's data into trace...
        let batch = BlobTraceBatch {
            id: id.0,
            desc: desc.clone(),
            updates,
        };
        self.append_trace(key, batch)?;
        let future = self
            .futures
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;

        // .. and removing the data from future once that's successful.
        future.truncate(desc.upper().clone())

        // TODO: Incrementally compact trace.
    }

    /// Appends the given `batch` to the trace for `id`, writing the data at
    /// `key` in the blob storage.
    fn append_future(&mut self, key: String, batch: BlobFutureBatch) -> Result<(), Error> {
        let future = self
            .futures
            .get_mut(&Id(batch.id))
            .ok_or_else(|| Error::from(format!("never registered: {:?}", batch.id)))?;
        future.append(key, batch, &mut self.blob)?;
        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())
    }

    /// Appends the given `batch` to the trace for `id`, writing the data at
    /// `key` in the blob storage.
    fn append_trace(&mut self, key: String, batch: BlobTraceBatch) -> Result<(), Error> {
        let trace = self
            .traces
            .get_mut(&Id(batch.id))
            .ok_or_else(|| Error::from(format!("never registered: {:?}", batch.id)))?;
        trace.append(key, batch, &mut self.blob)?;
        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())
    }

    fn serialize_meta(&self) -> BlobMeta {
        BlobMeta {
            last_file_id: self.last_file_id,
            futures: self
                .futures
                .iter()
                .map(|(id, future)| (id.0, future.meta()))
                .collect(),
            traces: self
                .traces
                .iter()
                .map(|(id, trace)| (id.0, trace.meta()))
                .collect(),
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
        let future = future.snapshot(trace.ts_upper.clone(), None, &self.blob)?;

        // A closed lower bound on the updates we should include in `buffer` (i.e.
        // the ones not included in `future`).
        let buf_lower = &future.seqno_upper;
        let mut buffer = BufferSnapshot(Vec::new());
        {
            self.buf.snapshot(|seqno, buf| {
                let entry: Abomonated<BufferEntry, Vec<u8>> =
                    unsafe { Abomonated::new(buf.to_owned()) }
                        .ok_or_else(|| Error::from(format!("invalid buffer entry")))?;
                if entry.id != id.0 || !buf_lower.less_equal(&seqno) {
                    return Ok(());
                }
                buffer.0.extend(entry.updates.iter().cloned());
                Ok(())
            })?;
        }

        Ok(IndexedSnapshot(buffer, future, trace))
    }
}

/// A consistent snapshot of the data currently in a [Buffer].
struct BufferSnapshot(Vec<((String, String), u64, isize)>);

impl Snapshot for BufferSnapshot {
    fn read<E: Extend<((String, String), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        buf.extend(self.0.drain(..));
        false
    }
}

/// A consistent snapshot of all data currently stored for an id.
pub struct IndexedSnapshot(BufferSnapshot, FutureSnapshot, TraceSnapshot);

impl Snapshot for IndexedSnapshot {
    fn read<E: Extend<((String, String), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        self.0.read(buf) || self.1.read(buf) || self.2.read(buf)
    }
}

/// The structure serialized and stored as an entry in a Buffer.
#[derive(Debug, Abomonation)]
pub struct BufferEntry {
    /// Id of the stream this batch belongs to.
    id: u64,
    /// The updates themselves.
    updates: Vec<((String, String), u64, isize)>,
}

/// The structure serialized and stored as a value in [Blob] storage for
/// metadata keys.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobMeta {
    /// The most recently assigned key name.
    last_file_id: u128,
    /// BlobFutures indexed by stream id.
    futures: Vec<(u64, BlobFutureMeta)>,
    /// BlobTraces indexed by stream id.
    traces: Vec<(u64, BlobTraceMeta)>,
}

/// The metadata necessary to reconstruct a BlobFuture.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobFutureMeta {
    /// A lower bound of data contained by this BlobFuture. Data before this may
    /// be present in the batches, but has been logically moved into the trace
    /// and should be ignored.
    ts_lower: Antichain<u64>,
    /// The batches that make up the BlobFuture, represented by their
    /// description and the key to retrieve the batch's data from the blob
    /// store. Note that Descriptions are half-open intervals `[lower, upper)`.
    batches: Vec<(Description<SeqNo>, String)>,
}

/// The metadata necessary to reconstruct a BlobTrace.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobTraceMeta {
    /// The batches that make up the BlobTrace, represented by their description
    /// and the key to retrieve the batch's data from the blob store. Note that
    /// Descriptions are half-open intervals `[lower, upper)`.
    batches: Vec<(Description<u64>, String)>,
}

/// The structure serialized and stored as a value in [Blob] storage for
/// data keys corresponding to future data.
#[derive(Clone, Debug, Abomonation)]
pub struct BlobFutureBatch {
    /// Id of the stream this batch belongs to.
    id: u64,
    /// Which updates are included in this batch.
    desc: Description<SeqNo>,
    /// The updates themselves.
    updates: Vec<(SeqNo, (String, String), u64, isize)>,
}

/// The structure serialized and stored as a value in [Blob] storage for data
/// keys corresponding to trace data.
///
/// TODO: This probably wants to be a different level of abstraction, so we can
/// put multiple small batches in a single blob but also break a very large
/// batch over multiple blobs. We also may want to break the latter into chunks
/// for checksum and encryption?
#[derive(Clone, Debug, Abomonation)]
pub struct BlobTraceBatch {
    /// Id of the trace this batch belongs to.
    id: u64,
    /// Which updates are included in this batch.
    desc: Description<u64>,
    /// The updates themselves.
    updates: Vec<((String, String), u64, isize)>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::mem::MemBlob;
    use crate::mem::MemBuffer;
    use crate::persister::Snapshot;

    use super::*;

    fn extract<S: Snapshot>(mut s: S) -> Vec<((String, String), u64, isize)> {
        let mut buf = Vec::new();
        while s.read(&mut buf) {}
        buf.sort();
        buf
    }

    #[test]
    fn single_stream() -> Result<(), Box<dyn Error>> {
        let updates = vec![
            (("1".into(), "".into()), 1, 1),
            (("2".into(), "".into()), 2, 1),
        ];

        let mut i = Indexed::new(MemBuffer::new(), MemBlob::new())?;
        let id = Id(0);
        i.register(id);

        // Empty things are empty.
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(extract(buf), vec![]);
        assert_eq!(extract(future), vec![]);
        assert_eq!(extract(trace), vec![]);

        // After a write, all data is in the buffer.
        i.write_sync(id, &updates)?;
        assert_eq!(extract(i.snapshot(id)?), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(extract(buf), updates);
        assert_eq!(extract(future), vec![]);
        assert_eq!(extract(trace), vec![]);

        // After a step, it's all moved into the future part of the index.
        i.step()?;
        assert_eq!(extract(i.snapshot(id)?), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(extract(buf), vec![]);
        assert_eq!(extract(future), updates);
        assert_eq!(extract(trace), vec![]);

        // After a seal, the relevant data has moved into the trace part of the
        // index. Since we haven't sealed all the data, some of it is still in
        // the future.
        i.seal(id, 2)?;
        assert_eq!(extract(i.snapshot(id)?), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(extract(buf), vec![]);
        assert_eq!(extract(future), updates[1..]);
        assert_eq!(extract(trace), updates[..1]);

        // All the data has been sealed, so it's now all in the trace.
        i.seal(id, 3)?;
        assert_eq!(extract(i.snapshot(id)?), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(extract(buf), vec![]);
        assert_eq!(extract(future), vec![]);
        assert_eq!(extract(trace), updates);

        Ok(())
    }
}
