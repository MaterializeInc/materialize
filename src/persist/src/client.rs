// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The public "async API" for persist.

use std::collections::HashSet;
use std::fmt;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;

use differential_dataflow::trace::Description;
use persist_types::Codec;
use timely::progress::Antichain;

use crate::error::Error;
use crate::indexed::arrangement::{ArrangementSnapshot, ArrangementSnapshotIter};
use crate::indexed::columnar::{ColumnarRecords, ColumnarRecordsBuilder};
use crate::indexed::encoding::Id;
use crate::indexed::{ListenEvent, Snapshot};
use crate::pfuture::{PFuture, PFutureHandle};
use crate::storage::SeqNo;

/// A clone-able handle to the persistence runtime.
///
/// The runtime is stopped when any client calls [RuntimeClient::stop] or when
/// all clients have been dropped, whichever comes first.
///
/// NB: The methods below are executed concurrently. For a some call X to be
/// guaranteed as "previous" to another call Y, X's response must have been
/// received before Y's call started.
#[derive(Clone)]
pub struct RuntimeClient {
    id: RuntimeId,
    core: Arc<RuntimeCore>,
}

impl fmt::Debug for RuntimeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeClient")
            .field(&"id", &self.id)
            .field(&"core", &"..")
            .finish()
    }
}

impl PartialEq for RuntimeClient {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for RuntimeClient {}

impl RuntimeClient {
    /// Synchronously registers a new stream for writes and reads.
    ///
    /// This method is idempotent. Returns read and write handles used to
    /// construct a persisted stream operator.
    ///
    /// TODO: This description seems outdated? Or at least only half true.
    /// If data was written by a previous [RuntimeClient] for this id, it's
    /// loaded and replayed into the stream once constructed.
    pub fn create_or_load<K: Codec, V: Codec>(
        &self,
        name: &str,
    ) -> (StreamWriteHandle<K, V>, StreamReadHandle<K, V>) {
        let (tx, rx) = PFuture::new();
        self.core.send(Cmd::Register(
            name.to_owned(),
            (K::codec_name(), V::codec_name()),
            tx,
        ));
        let id = rx.recv();
        let write = StreamWriteHandle::new(name.to_owned(), id.clone(), self.clone());
        let meta = StreamReadHandle::new(name.to_owned(), id, self.clone());
        (write, meta)
    }

    /// Returns a [Description] of the stream identified by `id_str`.
    // TODO: We might want to think about returning only the compaction frontier (since) and seal
    // timestamp (upper) here. Description seems more oriented towards describing batches, and in
    // our case the lower is always `Antichain::from_elem(Timestamp::minimum())`. We could return a
    // tuple or create our own Description-like return type for this.
    pub fn get_description(&self, id_str: &str) -> Result<Description<u64>, Error> {
        let (tx, rx) = PFuture::new();
        self.core.send(Cmd::GetDescription(id_str.to_owned(), tx));
        let seal_frontier = rx.recv()?;
        Ok(seal_frontier)
    }

    /// Asynchronously persists `(Key, Value, Time, Diff)` updates for the
    /// streams with the given ids.
    ///
    /// The ids must have previously been registered.
    pub(crate) fn write(&self, updates: Vec<(Id, ColumnarRecords)>, res: PFutureHandle<SeqNo>) {
        self.core.send(Cmd::Write(updates, res))
    }

    /// Asynchronously advances the "sealed" frontier for the streams with the
    /// given ids, which restricts what times can later be sealed and/or written
    /// for those ids.
    ///
    /// The ids must have previously been registered.
    fn seal(&self, ids: &[Id], ts: u64, res: PFutureHandle<SeqNo>) {
        self.core.send(Cmd::Seal(ids.to_vec(), ts, res))
    }

    /// Asynchronously advances the compaction frontier for the streams with the
    /// given ids, which lets the stream discard historical detail for times not
    /// beyond the compaction frontier. This also restricts what times the
    /// compaction frontier can later be advanced to for these ids.
    ///
    /// The ids must have previously been registered.
    fn allow_compaction(&self, id_sinces: &[(Id, Antichain<u64>)], res: PFutureHandle<SeqNo>) {
        self.core
            .send(Cmd::AllowCompaction(id_sinces.to_vec(), res))
    }

    /// Asynchronously returns a [crate::indexed::Snapshot] for the stream
    /// with the given id.
    ///
    /// This snapshot is guaranteed to include any previous writes.
    ///
    /// The id must have previously been registered.
    fn snapshot(&self, id: Id, res: PFutureHandle<ArrangementSnapshot>) {
        self.core.send(Cmd::Snapshot(id, res))
    }

    /// Asynchronously registers a callback to be invoked on successful writes
    /// and seals.
    fn listen(
        &self,
        id: Id,
        sender: crossbeam_channel::Sender<ListenEvent>,
        res: PFutureHandle<ArrangementSnapshot>,
    ) {
        self.core.send(Cmd::Listen(id, sender, res))
    }

    /// Synchronously closes the runtime, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// This method is idempotent.
    pub fn stop(&mut self) -> Result<(), Error> {
        self.core.stop()
    }

    /// Synchronously remove the persisted stream.
    ///
    /// This method is idempotent and returns true if the stream was actually
    /// destroyed, false if the stream had already been destroyed previously.
    pub fn destroy(&mut self, id: &str) -> Result<bool, Error> {
        let (tx, rx) = PFuture::new();
        self.core.send(Cmd::Destroy(id.to_owned(), tx));
        rx.recv()
    }
}

/// A handle that allows writes of ((Key, Value), Time, Diff) updates into an
/// [crate::indexed::Indexed] via a [RuntimeClient].
#[derive(PartialEq)]
pub struct StreamWriteHandle<K, V> {
    name: String,
    id: Result<Id, Error>,
    runtime: RuntimeClient,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for StreamWriteHandle<K, V> {
    fn clone(&self) -> Self {
        StreamWriteHandle {
            name: self.name.clone(),
            id: self.id.clone(),
            runtime: self.runtime.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<K, V> fmt::Debug for StreamWriteHandle<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamWriteHandle")
            .field("name", &self.name)
            .field("id", &self.id)
            .field("runtime", &self.runtime)
            .finish()
    }
}

impl<K: Codec, V: Codec> StreamWriteHandle<K, V> {
    /// Returns a new [StreamWriteHandle] for the given stream.
    pub fn new(name: String, id: Result<Id, Error>, runtime: RuntimeClient) -> Self {
        StreamWriteHandle {
            name,
            id,
            runtime,
            _phantom: PhantomData,
        }
    }

    /// Returns the external stream name for this handle.
    pub fn stream_name(&self) -> &str {
        &self.name
    }

    /// Returns the internal stream [Id] for this handle.
    pub fn stream_id(&self) -> Result<Id, Error> {
        self.id.clone()
    }

    /// Synchronously writes (Key, Value, Time, Diff) updates.
    pub fn write<'a, I>(&self, updates: I) -> PFuture<SeqNo>
    where
        I: IntoIterator<Item = &'a ((K, V), u64, isize)>,
    {
        let (tx, rx) = PFuture::new();

        match self.id {
            Ok(id) => {
                let mut updates = WriteReqBuilder::<K, V>::from_iter(updates);
                self.runtime.write(vec![(id, updates.finish())], tx);
            }
            Err(ref e) => {
                tx.fill(Err(e.clone()));
            }
        }

        rx
    }

    /// Closes the stream at the given timestamp, migrating data strictly less
    /// than it into the trace.
    pub fn seal(&self, upper: u64) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();

        match self.id {
            Ok(id) => {
                self.runtime.seal(&[id], upper, tx);
            }
            Err(ref e) => {
                tx.fill(Err(e.clone()));
            }
        }

        rx
    }

    /// Unblocks compaction for updates at or before `since`.
    ///
    /// The compaction frontier can never decrease and it is an error to call this function with a
    /// since argument that is less than the current compaction frontier.
    ///
    /// While it may seem counter-intuitive to advance the compaction frontier past the seal
    /// frontier, this is perfectly valid. It can happen when joining updates from one stream to
    /// updates from another stream, and we already know that the other stream is compacted further
    /// along. Allowing compaction on this, the first stream, then is saying that we are fine with
    /// losing historical detail, and that we already allow compaction of updates that are yet to
    /// come because we don't need them at their full resolution. A similar case is when we know
    /// that any outstanding queries have an `as_of` that is in the future of the seal: we can also
    /// pro-actively allow compaction of updates that did not yet arrive.
    ///
    pub fn allow_compaction(&self, since: Antichain<u64>) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();

        match self.id {
            Ok(id) => {
                self.runtime.allow_compaction(&[(id, since)], tx);
            }
            Err(ref e) => {
                tx.fill(Err(e.clone()));
            }
        }

        rx
    }
}

/// A handle to construct a [ColumnarRecords] from a vector of records for writes.
#[derive(Debug)]
pub struct WriteReqBuilder<K: Codec, V: Codec> {
    records: ColumnarRecordsBuilder,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    _phantom: PhantomData<(K, V)>,
}

impl<K: Codec, V: Codec> WriteReqBuilder<K, V> {
    /// Finalize a write request into [ColumnarRecords].
    pub fn finish(&mut self) -> ColumnarRecords {
        std::mem::take(&mut self.records).finish()
    }
}

impl<'a, K: Codec, V: Codec> FromIterator<&'a ((K, V), u64, isize)> for WriteReqBuilder<K, V> {
    fn from_iter<T: IntoIterator<Item = &'a ((K, V), u64, isize)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint();

        let mut builder = WriteReqBuilder {
            records: ColumnarRecordsBuilder::default(),
            key_buf: Vec::new(),
            val_buf: Vec::new(),
            _phantom: PhantomData,
        };
        for record in iter {
            let ((key, val), ts, diff) = record;
            builder.key_buf.clear();
            key.encode(&mut builder.key_buf);
            builder.val_buf.clear();
            val.encode(&mut builder.val_buf);

            if builder.records.len() == 0 {
                // Use the first record to attempt to pre-size the builder
                // allocations. This uses the iter's size_hint's lower+1 to
                // match the logic in Vec.
                let (lower, _) = size_hint;
                let additional = usize::saturating_add(lower, 1);
                builder
                    .records
                    .reserve(additional, builder.key_buf.len(), builder.val_buf.len());
            }
            builder
                .records
                .push(((&builder.key_buf, &builder.val_buf), *ts, *diff));
        }
        builder
    }
}

/// A handle for writing to multiple streams.
#[derive(Debug, PartialEq, Eq)]
pub struct MultiWriteHandle<K, V> {
    ids: HashSet<Id>,
    runtime: RuntimeClient,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for MultiWriteHandle<K, V> {
    fn clone(&self) -> Self {
        MultiWriteHandle {
            ids: self.ids.clone(),
            runtime: self.runtime.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<K: Codec, V: Codec> MultiWriteHandle<K, V> {
    /// Returns a new [MultiWriteHandle] for the given streams.
    pub fn new(handles: &[&StreamWriteHandle<K, V>]) -> Result<Self, Error> {
        let mut stream_ids = HashSet::new();
        let runtime = if let Some(handle) = handles.first() {
            handle.runtime.clone()
        } else {
            return Err(Error::from("MultiWriteHandle received no streams"));
        };
        for handle in handles.iter() {
            if handle.runtime.id != runtime.id {
                return Err(Error::from(format!(
                    "MultiWriteHandle got handles from two runtimes: {:?} and {:?}",
                    runtime.id, handle.runtime.id
                )));
            }
            // It's odd if there are duplicates but the semantics of what that
            // means are straightforward, so for now we support it.
            stream_ids.insert(handle.stream_id()?);
        }
        Ok(MultiWriteHandle {
            ids: stream_ids,
            runtime,
            _phantom: PhantomData,
        })
    }

    /// Atomically writes the given updates to the paired streams.
    ///
    /// Either all of the writes will be made durable for replay or none of them
    /// will.
    ///
    /// Ids may be duplicated. However, the updates are passed down to storage
    /// unchanged, so users should coalesce them when that's not otherwise
    /// slower.
    //
    // TODO: This could take &StreamWriteHandle instead of Id to avoid surfacing
    // Id to users, but that would require an extra Vec. Revisit.
    //
    // TODO: Make this take a two-layer IntoIterator to mirror how
    // StreamWriteHandle::write works.
    pub fn write_atomic(&self, updates: Vec<(Id, Vec<((K, V), u64, isize)>)>) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();
        for (id, _) in updates.iter() {
            if !self.ids.contains(id) {
                tx.fill(Err(Error::from(format!(
                    "MultiWriteHandle cannot write to stream: {:?}",
                    id
                ))));
                return rx;
            }
        }
        let updates = updates
            .iter()
            .map(|(id, updates)| {
                let mut updates = WriteReqBuilder::<K, V>::from_iter(updates);
                (*id, updates.finish())
            })
            .collect();
        self.runtime.write(updates, tx);
        rx
    }

    /// Closes the streams at the given timestamp, migrating data strictly less
    /// than it into the trace.
    ///
    /// Ids may not be duplicated (this is equivalent to sealing the stream
    /// twice at the same timestamp, which we currently disallow).
    pub fn seal(&self, ids: &[Id], upper: u64) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();
        for id in ids {
            if !self.ids.contains(id) {
                tx.fill(Err(Error::from(format!(
                    "MultiWriteHandle cannot seal stream: {:?}",
                    id
                ))));
                return rx;
            }
        }
        self.runtime.seal(ids, upper, tx);
        rx
    }

    /// Unblocks compaction for updates for the given streams at the paired
    /// `since` timestamp.
    ///
    /// Ids may not be duplicated (this is equivalent to allowing compaction on
    /// the stream twice at the same timestamp, which we currently disallow).
    pub fn allow_compaction(&self, id_sinces: &[(Id, Antichain<u64>)]) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();
        for (id, _) in id_sinces {
            if !self.ids.contains(id) {
                tx.fill(Err(Error::from(format!(
                    "MultiWriteHandle cannot allow_compaction stream: {:?}",
                    id
                ))));
                return rx;
            }
        }
        self.runtime.allow_compaction(id_sinces, tx);
        rx
    }
}

/// A consistent snapshot of all data currently stored for an id, with keys and
/// vals decoded.
#[derive(Debug)]
pub struct DecodedSnapshot<K, V> {
    snap: ArrangementSnapshot,
    _phantom: PhantomData<(K, V)>,
}

impl<K: Codec, V: Codec> DecodedSnapshot<K, V> {
    pub(crate) fn new(snap: ArrangementSnapshot) -> Self {
        DecodedSnapshot {
            snap,
            _phantom: PhantomData,
        }
    }

    /// Returns the SeqNo at which this snapshot was run.
    ///
    /// All writes assigned a seqno < this are included.
    pub fn seqno(&self) -> SeqNo {
        self.snap.seqno()
    }

    /// Returns the since frontier of this snapshot.
    ///
    /// All updates at times less than this frontier must be forwarded
    /// to some time in this frontier.
    pub fn since(&self) -> Antichain<u64> {
        self.snap.since()
    }

    /// A logical upper bound on the times that had been added to the collection
    /// when this snapshot was taken
    pub fn get_seal(&self) -> Antichain<u64> {
        self.snap.get_seal()
    }
}

impl<K: Codec, V: Codec> Snapshot<K, V> for DecodedSnapshot<K, V> {
    type Iter = DecodedSnapshotIter<K, V>;

    fn into_iters(self, num_iters: NonZeroUsize) -> Vec<Self::Iter> {
        self.snap
            .into_iters(num_iters)
            .into_iter()
            .map(|iter| DecodedSnapshotIter {
                iter,
                _phantom: PhantomData,
            })
            .collect()
    }
}

/// An [Iterator] representing one part of the data in a [DecodedSnapshot].
#[derive(Debug)]
pub struct DecodedSnapshotIter<K, V> {
    iter: ArrangementSnapshotIter,
    _phantom: PhantomData<(K, V)>,
}

impl<K: Codec, V: Codec> Iterator for DecodedSnapshotIter<K, V> {
    type Item = Result<((K, V), u64, isize), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            let ((k, v), ts, diff) = x?;
            let k = K::decode(&k)?;
            let v = V::decode(&v)?;
            Ok(((k, v), ts, diff))
        })
    }
}

/// A handle for a persisted stream of ((Key, Value), Time, Diff) updates backed
/// by an [crate::indexed::Indexed] via a [RuntimeClient].
#[derive(Debug, PartialEq)]
pub struct StreamReadHandle<K, V> {
    name: String,
    id: Result<Id, Error>,
    runtime: RuntimeClient,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for StreamReadHandle<K, V> {
    fn clone(&self) -> Self {
        StreamReadHandle {
            name: self.name.clone(),
            id: self.id.clone(),
            runtime: self.runtime.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<K: Codec, V: Codec> StreamReadHandle<K, V> {
    /// Returns a new [StreamReadHandle] for the given stream.
    pub fn new(name: String, id: Result<Id, Error>, runtime: RuntimeClient) -> Self {
        StreamReadHandle {
            name,
            id,
            runtime,
            _phantom: PhantomData,
        }
    }

    /// Returns the external stream name for this handle.
    pub fn stream_name(&self) -> &str {
        &self.name
    }

    /// Returns a consistent snapshot of all previously persisted stream data.
    pub fn snapshot(&self) -> Result<DecodedSnapshot<K, V>, Error> {
        let id = match self.id {
            Ok(id) => id,
            Err(ref e) => return Err(e.clone()),
        };

        // TODO: Make snapshot signature non-blocking.
        let (tx, rx) = PFuture::new();
        self.runtime.snapshot(id, tx);
        let snap = rx.recv()?;
        Ok(DecodedSnapshot::new(snap))
    }

    /// Registers a callback to be invoked on successful writes and seals.
    ///
    /// Also returns a snapshot so that users can, if they choose, perform their
    /// logic on everything that was previously persisted before registering the
    /// listener, and all writes and seals that happen after registration without
    /// duplicating or dropping data.
    pub fn listen(
        &self,
        sender: crossbeam_channel::Sender<ListenEvent>,
    ) -> Result<DecodedSnapshot<K, V>, Error> {
        let id = match self.id {
            Ok(id) => id,
            Err(ref e) => return Err(e.clone()),
        };

        let (tx, rx) = PFuture::new();
        self.runtime.listen(id, sender, tx);
        let snap = rx.recv()?;
        Ok(DecodedSnapshot::new(snap))
    }
}
