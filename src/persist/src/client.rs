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
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use differential_dataflow::trace::Description;
use persist_types::Codec;
use timely::progress::Antichain;

use crate::error::Error;
use crate::indexed::arrangement::{ArrangementSnapshot, ArrangementSnapshotIter};
use crate::indexed::columnar::{ColumnarRecords, ColumnarRecordsVecBuilder};
use crate::indexed::encoding::Id;
use crate::indexed::metrics::Metrics;
use crate::indexed::{Cmd, CmdRead, ListenEvent, Snapshot};
use crate::pfuture::PFuture;
use crate::runtime::{RuntimeCmd, RuntimeHandle, RuntimeId};
use crate::storage::SeqNo;

/// A clone-able handle to the persistence runtime.
///
/// The runtime is stopped when any client calls [RuntimeClient::stop] or when
/// all clients have been dropped, whichever comes first.
///
/// NB: The methods below are executed concurrently. For a some call X to be
/// guaranteed as "previous" to another call Y, X's future must have been
/// received before Y's call started (though X's future doesn't need to have
/// resolved).
#[derive(Clone, Debug)]
pub struct RuntimeClient {
    sender: RuntimeCmdSender,
    stopper: Arc<StopRuntimeOnDrop>,
}

impl PartialEq for RuntimeClient {
    fn eq(&self, other: &Self) -> bool {
        self.stopper.runtime_id.eq(&other.stopper.runtime_id)
    }
}

impl Eq for RuntimeClient {}

impl RuntimeClient {
    pub(crate) fn new(
        handle: RuntimeHandle,
        tx: crossbeam_channel::Sender<RuntimeCmd>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let runtime_id = handle.id();
        let sender = RuntimeCmdSender { metrics, tx };
        let stopper = Arc::new(StopRuntimeOnDrop {
            runtime_id,
            sender: CmdReadSender::Full(sender.clone()),
            handle: Mutex::new(Some(handle)),
        });
        RuntimeClient { sender, stopper }
    }

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
        self.sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Register(
                name.to_owned(),
                (K::codec_name(), V::codec_name()),
                tx,
            )));
        let stream_id = rx.recv();

        let write = StreamWriteHandle {
            name: name.to_owned(),
            stream_id: stream_id.clone(),
            client: self.clone(),
            _phantom: PhantomData,
        };
        let read = StreamReadHandle {
            name: name.to_owned(),
            stream_id,
            client: RuntimeReadClient {
                sender: CmdReadSender::Full(self.sender.clone()),
                stopper: self.stopper.clone(),
            },
            _phantom: PhantomData,
        };
        (write, read)
    }

    /// Returns a [Description] of the stream identified by `id_str`.
    //
    // TODO: We might want to think about returning only the compaction frontier
    // (since) and seal timestamp (upper) here. Description seems more oriented
    // towards describing batches, and in our case the lower is always
    // `Antichain::from_elem(Timestamp::minimum())`. We could return a tuple or
    // create our own Description-like return type for this.
    pub fn get_description(&self, id_str: &str) -> Result<Description<u64>, Error> {
        let (tx, rx) = PFuture::new();
        self.sender
            .send_cmd_read(CmdRead::GetDescription(id_str.to_owned(), tx));
        rx.recv()
    }

    /// Synchronously closes the runtime, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// This method is idempotent.
    pub fn stop(&mut self) -> Result<(), Error> {
        self.stopper.stop()
    }

    /// Synchronously remove the persisted stream.
    ///
    /// This method is idempotent and returns true if the stream was actually
    /// destroyed, false if the stream had already been destroyed previously.
    pub fn destroy(&mut self, id: &str) -> Result<bool, Error> {
        let (tx, rx) = PFuture::new();
        self.sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Destroy(id.to_owned(), tx)));
        rx.recv()
    }
}

/// A clone-able handle to the read-only persistence runtime.
///
/// The runtime is stopped when any client calls [RuntimeReadClient::stop] or
/// when all clients have been dropped, whichever comes first.
///
/// NB: The methods below are executed concurrently. For a some call X to be
/// guaranteed as "previous" to another call Y, X's future must have been
/// received before Y's call started (though X's future doesn't need to have
/// resolved).
#[derive(Clone, Debug)]
pub struct RuntimeReadClient {
    sender: CmdReadSender,
    stopper: Arc<StopRuntimeOnDrop>,
}

impl PartialEq for RuntimeReadClient {
    fn eq(&self, other: &Self) -> bool {
        self.stopper.runtime_id.eq(&other.stopper.runtime_id)
    }
}

impl Eq for RuntimeReadClient {}

impl RuntimeReadClient {
    pub(crate) fn new(
        handle: RuntimeHandle,
        tx: crossbeam_channel::Sender<CmdRead>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let runtime_id = handle.id();
        let sender = CmdReadSender::Read { metrics, tx };
        let stopper = Arc::new(StopRuntimeOnDrop {
            runtime_id,
            sender: sender.clone(),
            handle: Mutex::new(Some(handle)),
        });
        RuntimeReadClient { stopper, sender }
    }

    /// Synchronously loads a stream for reads.
    pub fn load<K: Codec, V: Codec>(&self, name: &str) -> StreamReadHandle<K, V> {
        let (tx, rx) = PFuture::new();
        self.sender.send_cmd_read(CmdRead::Load(
            name.to_owned(),
            (K::codec_name(), V::codec_name()),
            tx,
        ));
        let stream_id = match rx.recv() {
            Ok(Some(stream_id)) => Ok(stream_id),
            Ok(None) => Err(format!("unregistered collection: {}", name).into()),
            Err(err) => Err(err),
        };
        StreamReadHandle {
            name: name.to_owned(),
            stream_id,
            client: self.clone(),
            _phantom: PhantomData,
        }
    }

    /// Returns a [Description] of the stream identified by `id_str`.
    //
    // TODO: We might want to think about returning only the compaction frontier
    // (since) and seal timestamp (upper) here. Description seems more oriented
    // towards describing batches, and in our case the lower is always
    // `Antichain::from_elem(Timestamp::minimum())`. We could return a tuple or
    // create our own Description-like return type for this.
    pub fn get_description(&self, id_str: &str) -> Result<Description<u64>, Error> {
        let (tx, rx) = PFuture::new();
        self.sender
            .send_cmd_read(CmdRead::GetDescription(id_str.to_owned(), tx));
        let seal_frontier = rx.recv()?;
        Ok(seal_frontier)
    }

    /// Synchronously closes the runtime, causing all future commands to error.
    ///
    /// This method is idempotent.
    pub fn stop(&mut self) -> Result<(), Error> {
        self.stopper.stop()
    }
}

/// A handle that allows writes of ((Key, Value), Time, Diff) updates into an
/// [crate::indexed::Indexed] via a [RuntimeClient].
#[derive(Debug)]
pub struct StreamWriteHandle<K, V> {
    name: String,
    stream_id: Result<Id, Error>,
    client: RuntimeClient,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for StreamWriteHandle<K, V> {
    fn clone(&self) -> Self {
        StreamWriteHandle {
            name: self.name.clone(),
            stream_id: self.stream_id.clone(),
            client: self.client.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<K: Codec, V: Codec> StreamWriteHandle<K, V> {
    /// Returns the external stream name for this handle.
    pub fn stream_name(&self) -> &str {
        &self.name
    }

    /// Returns the internal stream [Id] for this handle.
    pub fn stream_id(&self) -> Result<Id, Error> {
        self.stream_id.clone()
    }

    /// Asynchronously persists `(Key, Value, Time, Diff)` updates.
    pub fn write<'a, I>(&self, updates: I) -> PFuture<SeqNo>
    where
        I: IntoIterator<Item = &'a ((K, V), u64, isize)>,
    {
        let (tx, rx) = PFuture::new();

        match self.stream_id {
            Ok(stream_id) => {
                let mut updates = WriteReqBuilder::<K, V>::from_iter(updates);
                let updates = updates
                    .finish()
                    .into_iter()
                    .map(|x| (stream_id, x))
                    .collect();
                self.client
                    .sender
                    .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Write(updates, tx)));
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

        match self.stream_id {
            Ok(stream_id) => {
                self.client
                    .sender
                    .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Seal(
                        vec![stream_id],
                        upper,
                        tx,
                    )));
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
    pub fn allow_compaction(&self, since: Antichain<u64>) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();

        match self.stream_id {
            Ok(stream_id) => {
                self.client
                    .sender
                    .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::AllowCompaction(
                        vec![(stream_id, since)],
                        tx,
                    )));
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
    records: ColumnarRecordsVecBuilder,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    _phantom: PhantomData<(K, V)>,
}

impl<K: Codec, V: Codec> WriteReqBuilder<K, V> {
    /// Finalize a write request into [Vec<ColumnarRecords>].
    pub fn finish(&mut self) -> Vec<ColumnarRecords> {
        std::mem::take(&mut self.records).finish()
    }
}

impl<'a, K: Codec, V: Codec> FromIterator<&'a ((K, V), u64, isize)> for WriteReqBuilder<K, V> {
    fn from_iter<T: IntoIterator<Item = &'a ((K, V), u64, isize)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint();

        let mut builder = WriteReqBuilder {
            records: ColumnarRecordsVecBuilder::default(),
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
    stream_ids: HashSet<Id>,
    client: RuntimeClient,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for MultiWriteHandle<K, V> {
    fn clone(&self) -> Self {
        MultiWriteHandle {
            stream_ids: self.stream_ids.clone(),
            client: self.client.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<K: Codec, V: Codec> MultiWriteHandle<K, V> {
    /// Returns a new [MultiWriteHandle] for the given streams.
    pub fn new(handles: &[&StreamWriteHandle<K, V>]) -> Result<Self, Error> {
        let mut stream_ids = HashSet::new();
        let client = if let Some(handle) = handles.first() {
            handle.client.clone()
        } else {
            return Err(Error::from("MultiWriteHandle received no streams"));
        };
        for handle in handles.iter() {
            if handle.client != client {
                return Err(Error::from(format!(
                    "MultiWriteHandle got handles from two runtimes: {:?} and {:?}",
                    client, handle.client
                )));
            }
            // It's odd if there are duplicates but the semantics of what that
            // means are straightforward, so for now we support it.
            stream_ids.insert(handle.stream_id()?);
        }
        Ok(MultiWriteHandle {
            stream_ids,
            client,
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
        for (stream_id, _) in updates.iter() {
            if !self.stream_ids.contains(stream_id) {
                tx.fill(Err(Error::from(format!(
                    "MultiWriteHandle cannot write to stream: {:?}",
                    stream_id
                ))));
                return rx;
            }
        }
        let updates = updates
            .iter()
            .flat_map(|(id, updates)| {
                let mut updates = WriteReqBuilder::<K, V>::from_iter(updates);
                updates.finish().into_iter().map(|x| (*id, x))
            })
            .collect();
        self.client
            .sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Write(updates, tx)));
        rx
    }

    /// Closes the streams at the given timestamp, migrating data strictly less
    /// than it into the trace.
    ///
    /// Ids may not be duplicated (this is equivalent to sealing the stream
    /// twice at the same timestamp, which we currently disallow).
    pub fn seal(&self, ids: &[Id], upper: u64) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();
        for stream_id in ids {
            if !self.stream_ids.contains(stream_id) {
                tx.fill(Err(Error::from(format!(
                    "MultiWriteHandle cannot seal stream: {:?}",
                    stream_id
                ))));
                return rx;
            }
        }
        self.client
            .sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Seal(ids.to_vec(), upper, tx)));
        rx
    }

    /// Unblocks compaction for updates for the given streams at the paired
    /// `since` timestamp.
    ///
    /// Ids may not be duplicated (this is equivalent to allowing compaction on
    /// the stream twice at the same timestamp, which we currently disallow).
    pub fn allow_compaction(&self, id_sinces: &[(Id, Antichain<u64>)]) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();
        for (stream_id, _) in id_sinces {
            if !self.stream_ids.contains(stream_id) {
                tx.fill(Err(Error::from(format!(
                    "MultiWriteHandle cannot allow_compaction stream: {:?}",
                    stream_id
                ))));
                return rx;
            }
        }
        self.client
            .sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::AllowCompaction(
                id_sinces.to_vec(),
                tx,
            )));
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

#[derive(Debug)]
struct StopRuntimeOnDrop {
    runtime_id: RuntimeId,
    sender: CmdReadSender,
    handle: Mutex<Option<RuntimeHandle>>,
}

impl Drop for StopRuntimeOnDrop {
    fn drop(&mut self) {
        if let Err(err) = self.stop() {
            tracing::error!("error while stopping dropped persist runtime: {}", err);
        }
    }
}

impl StopRuntimeOnDrop {
    fn stop(&self) -> Result<(), Error> {
        if let Some(handle) = self.handle.lock()?.take() {
            let (tx, rx) = PFuture::new();
            self.sender.send_cmd_read(CmdRead::Stop(tx));
            // NB: Make sure there are no early returns before this `join`,
            // otherwise the runtime thread might still be cleaning up when this
            // returns (flushing out final writes, cleaning up LOCK files, etc).
            //
            // TODO: Regression test for this.
            handle.join();
            rx.recv()
        } else {
            Ok(())
        }
    }
}

/// A handle for a persisted stream of ((Key, Value), Time, Diff) updates backed
/// by an [crate::indexed::Indexed] via a [RuntimeClient].
#[derive(Debug)]
pub struct StreamReadHandle<K, V> {
    name: String,
    stream_id: Result<Id, Error>,
    client: RuntimeReadClient,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for StreamReadHandle<K, V> {
    fn clone(&self) -> Self {
        StreamReadHandle {
            name: self.name.clone(),
            stream_id: self.stream_id.clone(),
            client: self.client.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<K: Codec, V: Codec> StreamReadHandle<K, V> {
    /// Returns the external stream name for this handle.
    pub fn stream_name(&self) -> &str {
        &self.name
    }

    /// Returns a consistent snapshot of all previously persisted stream data.
    pub fn snapshot(&self) -> Result<DecodedSnapshot<K, V>, Error> {
        let stream_id = match self.stream_id {
            Ok(stream_id) => stream_id,
            Err(ref e) => return Err(e.clone()),
        };

        // TODO: Make snapshot signature non-blocking.
        let (tx, rx) = PFuture::new();
        self.client
            .sender
            .send_cmd_read(CmdRead::Snapshot(stream_id, tx));
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
        let stream_id = match self.stream_id {
            Ok(stream_id) => stream_id,
            Err(ref e) => return Err(e.clone()),
        };

        let (tx, rx) = PFuture::new();
        self.client
            .sender
            .send_cmd_read(CmdRead::Listen(stream_id, sender, tx));
        let snap = rx.recv()?;
        Ok(DecodedSnapshot::new(snap))
    }
}

#[derive(Clone, Debug)]
struct RuntimeCmdSender {
    metrics: Arc<Metrics>,
    tx: crossbeam_channel::Sender<RuntimeCmd>,
}

impl RuntimeCmdSender {
    pub fn send_cmd_read(&self, cmd: CmdRead) {
        self.send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Read(cmd)));
    }

    pub fn send_runtime_cmd(&self, cmd: RuntimeCmd) {
        self.metrics.cmd_queue_in.inc();
        if let Err(crossbeam_channel::SendError(cmd)) = self.tx.send(cmd) {
            // According to the docs, a SendError can only happen if the
            // receiver has hung up, which in this case only happens if the
            // thread has exited. The thread only exits if we send it a
            // Cmd::Stop (or it panics).
            cmd.fill_err_runtime_shutdown();
        }
    }
}

#[derive(Clone, Debug)]
enum CmdReadSender {
    Full(RuntimeCmdSender),
    Read {
        metrics: Arc<Metrics>,
        tx: crossbeam_channel::Sender<CmdRead>,
    },
}

impl CmdReadSender {
    pub fn send_cmd_read(&self, cmd: CmdRead) {
        match self {
            CmdReadSender::Full(x) => x.send_cmd_read(cmd),
            CmdReadSender::Read { metrics, tx } => {
                metrics.cmd_queue_in.inc();
                if let Err(crossbeam_channel::SendError(cmd)) = tx.send(cmd) {
                    // According to the docs, a SendError can only happen if the
                    // receiver has hung up, which in this case only happens if the
                    // thread has exited. The thread only exits if we send it a
                    // Cmd::Stop (or it panics).
                    cmd.fill_err_runtime_shutdown();
                }
            }
        }
    }
}

// NB: All the tests for this are in crate::runtime.
