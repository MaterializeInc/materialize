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
use timely::progress::Antichain;
use timely::PartialOrder;

use crate::error::Error;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{
    BlobFutureBatch, BlobFutureMeta, BlobMeta, BlobTraceBatch, BlobTraceMeta, BufferEntry, Id,
};
use crate::indexed::future::{BlobFuture, FutureSnapshot};
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
    // This is conceptually a map from `String` -> `Id`, but lookups are rare
    // and this representation is optimized for the metadata serialization path,
    // which is less rare.
    id_mapping: Vec<(String, Id)>,
    buf: U,
    blob: BlobCache<K, V, L>,
    futures: HashMap<Id, BlobFuture<K, V>>,
    traces: HashMap<Id, BlobTrace<K, V>>,
}

impl<K: Data, V: Data, U: Buffer, L: Blob> Indexed<K, V, U, L> {
    /// Returns a new Indexed, initializing each Future and Trace with the
    /// existing data for them in the blob storage, if any.
    pub fn new(buf: U, blob: L) -> Result<Self, Error> {
        let blob = BlobCache::new(blob);
        let meta = blob.get_meta()?.unwrap_or_default();
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
            buf,
            blob,
            futures,
            traces,
        };
        Ok(indexed)
    }

    /// Releases exclusive-writer locks and causes all future commands to error.
    ///
    /// This method is idempotent.
    pub fn close(&mut self) -> Result<(), Error> {
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
    pub fn register(&mut self, id_str: &str) -> Id {
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
        id
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
    pub fn write_sync(&mut self, id: Id, updates: &[((K, V), u64, isize)]) -> Result<SeqNo, Error> {
        let sealed_frontier = self.sealed_frontier(id)?;
        for update in updates.iter() {
            if !sealed_frontier.less_equal(&update.1) {
                return Err(format!(
                    "update for {:?} with time {} before sealed frontier: {:?}",
                    id, update.1, sealed_frontier,
                )
                .into());
            }
        }

        let entry = BufferEntry {
            id: id,
            updates: updates.to_vec(),
        };
        let mut entry_bytes = Vec::new();
        unsafe { abomonation::encode(&entry, &mut entry_bytes) }
            .expect("write to Vec is infallible");
        self.buf.write_sync(entry_bytes)
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

        // Move a batch of data from future into trace by reading a
        // snapshot from future...
        let mut updates = Vec::new();
        {
            let mut snap =
                future.snapshot(desc.lower().clone(), Some(desc.upper().clone()), &self.blob)?;
            while snap.read(&mut updates) {}
        }

        // Trace batches are required to be sorted by (k, v, ts).
        updates.sort_unstable_by(|((k1, v1), t1, _), ((k2, v2), t2, _)| {
            (k1, v1, t1).cmp(&(k2, v2, t2))
        });

        // Trace batches are required to be sorted and consolidated by ((k, v), t)
        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        // ...and atomically swapping that snapshot's data into trace.
        let batch = BlobTraceBatch { desc, updates };
        self.append_trace(id, batch)

        // TODO: This is a good point to compact future. The data that's been
        // moved is still there but now irrelevant. It may also be a good time
        // to compact trace.
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

    /// Appends the given `batch` to the trace for `id`, writing the data into
    /// blob storage.
    fn append_trace(&mut self, id: Id, batch: BlobTraceBatch<K, V>) -> Result<(), Error> {
        let trace = self
            .traces
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        let future = self
            .futures
            .get_mut(&id)
            .ok_or_else(|| Error::from(format!("never registered: {:?}", id)))?;
        let new_future_ts_lower = batch.desc.upper().clone();
        trace.append(batch, &mut self.blob)?;
        future.truncate(new_future_ts_lower)?;

        // Atomically update the meta with both the trace and future changes.
        //
        // TODO: Instead of fully overwriting META each time, this should be
        // more like a compactable log.
        self.blob.set_meta(self.serialize_meta())
    }

    fn serialize_meta(&self) -> BlobMeta {
        BlobMeta {
            next_stream_id: self.next_stream_id,
            futures_seqno_upper: self.futures_seqno_upper,
            id_mapping: self.id_mapping.clone(),
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
        let buffer = {
            let mut data = Vec::new();
            let seqno = self
                .buf
                .snapshot(|seqno, buf| {
                    let entry: Abomonated<BufferEntry<K, V>, Vec<u8>> =
                        unsafe { Abomonated::new(buf.to_owned()) }
                            .ok_or_else(|| Error::from(format!("invalid buffer entry")))?;
                    if entry.id != id || !buf_lower.less_equal(&seqno) {
                        return Ok(());
                    }
                    data.extend(entry.updates.iter().cloned());
                    Ok(())
                })?
                .end;
            BufferSnapshot(seqno, data)
        };

        Ok(IndexedSnapshot(buffer, future, trace))
    }
}

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

    use crate::error::Error as IndexedError;
    use crate::mem::MemBlob;
    use crate::mem::MemBuffer;

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
            MemBuffer::new("single_stream")?,
            MemBlob::new("single_stream")?,
        )?;
        let id = i.register("0");

        // Empty things are empty.
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(buf.read_to_end(), vec![]);
        assert_eq!(future.read_to_end(), vec![]);
        assert_eq!(trace.read_to_end(), vec![]);

        // After a write, all data is in the buffer.
        i.write_sync(id, &updates)?;
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

        // After a seal, the relevant data has moved into the trace part of the
        // index. Since we haven't sealed all the data, some of it is still in
        // the future.
        i.seal(id, 2)?;
        assert_eq!(i.snapshot(id)?.read_to_end(), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(buf.read_to_end(), vec![]);
        assert_eq!(future.read_to_end(), updates[1..]);
        assert_eq!(trace.read_to_end(), updates[..1]);

        // All the data has been sealed, so it's now all in the trace.
        i.seal(id, 3)?;
        assert_eq!(i.snapshot(id)?.read_to_end(), updates);
        let IndexedSnapshot(buf, future, trace) = i.snapshot(id)?;
        assert_eq!(buf.read_to_end(), vec![]);
        assert_eq!(future.read_to_end(), vec![]);
        assert_eq!(trace.read_to_end(), updates);

        Ok(())
    }

    #[test]
    fn batch_sorting() -> Result<(), Box<dyn Error>> {
        let updates = vec![
            (("1".to_string(), "".to_string()), 2, 1),
            (("2".to_string(), "".to_string()), 1, 1),
        ];

        let mut i = Indexed::new(
            MemBuffer::new("batch_sorting")?,
            MemBlob::new("batch_sorting")?,
        )?;
        let id = i.register("0");

        // Write the data and move it into the future part of the index, which
        // orders it within each batch by time. It's not, so this will fire a
        // validations error if the sort code doesn't work.
        i.write_sync(id, &updates)?;
        i.step()?;

        // Now move it into the trace part of the index, which orders it within
        // each batch by key. It should currently be ordered by time, which
        // given the data is not ordered by key, so again this should fire a
        // validations error if the sort code doesn't work.
        i.seal(id, 3)?;
        Ok(())
    }

    #[test]
    fn batch_consolidation() -> Result<(), Box<dyn Error>> {
        let updates = vec![
            (("1".to_string(), "".to_string()), 1, 1),
            (("1".to_string(), "".to_string()), 1, 1),
        ];

        let mut i = Indexed::new(
            MemBuffer::new("batch_consolidation")?,
            MemBlob::new("batch_consolidation")?,
        )?;
        let id = i.register("0");

        // Write the data and move it into the future part of the index, which
        // consolidates updates to identical ((k, v), t). Since the writes are
        // not already consolidated this test will fail if the consolidation
        // code does not work.
        i.write_sync(id, &updates)?;
        i.step()?;

        // Add another set of identical updates and place into another future
        // batch.
        i.write_sync(id, &updates)?;
        i.step()?;

        // Now move the data to the trace part of the index, which consolidates
        // updates at identical ((k, v), t). Since the writes are only consolidated
        // within individual future batches this test will fail if trace batch
        // consolidation does not work.
        i.seal(id, 2)?;

        Ok(())
    }

    #[test]
    fn batch_future_empty() -> Result<(), Box<dyn Error>> {
        let mut i = Indexed::new(
            MemBuffer::new("batch_future_empty")?,
            MemBlob::new("batch_future_empty")?,
        )?;
        let id = i.register("0");

        // Write an empty set of updates and try to move it into the future part
        // of the index.
        i.write_sync(id, &[])?;
        i.step()?;

        // Sending updates with dif = 0.
        let updates = vec![(("1".to_string(), "".to_string()), 1, 0)];
        i.write_sync(id, &updates)?;
        i.step()?;

        // Now try again with a set of updates that consolidates down to the empty
        // set.
        let updates = vec![
            (("1".to_string(), "".to_string()), 1, 2),
            (("1".to_string(), "".to_string()), 1, -2),
        ];

        i.write_sync(id, &updates)?;
        i.step()?;
        Ok(())
    }

    #[test]
    fn drain_buf_validate() -> Result<(), IndexedError> {
        let mut i = Indexed::new(
            MemBuffer::new("drain_buf_validate")?,
            MemBlob::new("drain_buf_validate")?,
        )?;
        let id = i.register("0");

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
}
