// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The public "async API" for persist.

use std::borrow::Borrow;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use mz_persist_types::Codec;
use timely::progress::Antichain;
use tracing::error;

use crate::error::Error;
use crate::indexed::columnar::{ColumnarRecords, ColumnarRecordsVecBuilder};
use crate::indexed::encoding::Id;
use crate::indexed::metrics::Metrics;
use crate::indexed::snapshot::{ArrangementSnapshot, ArrangementSnapshotIter, Snapshot};
use crate::indexed::{Cmd, CmdRead, ListenEvent, StreamDesc};
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
                stopper: Arc::clone(&self.stopper),
            },
            _phantom: PhantomData,
        };
        (write, read)
    }

    /// Returns a [StreamDesc] of the stream identified by `name`.
    pub fn get_description(&self, name: &str) -> Result<StreamDesc, Error> {
        let (tx, rx) = PFuture::new();
        self.sender.send_cmd_read(CmdRead::GetDescriptions(tx));
        let descs = rx.recv()?;
        let desc = descs
            .into_values()
            .find(|x| x.name == name)
            .ok_or_else(|| Error::UnknownRegistration(name.to_owned()))?;
        Ok(desc)
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

    /// Returns a [StreamDesc] of the stream identified by `name`.
    pub fn get_description(&self, name: &str) -> Result<StreamDesc, Error> {
        let (tx, rx) = PFuture::new();
        self.sender.send_cmd_read(CmdRead::GetDescriptions(tx));
        let descs = rx.recv()?;
        let desc = descs
            .into_values()
            .find(|x| x.name == name)
            .ok_or_else(|| Error::UnknownRegistration(name.to_owned()))?;
        Ok(desc)
    }

    /// Synchronously closes the runtime, causing all future commands to error.
    ///
    /// This method is idempotent.
    pub fn stop(&mut self) -> Result<(), Error> {
        self.stopper.stop()
    }
}

/// A handle that allows writes of ((Key, Value), Time, i64) updates into an
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

    /// Asynchronously persists `(Key, Value, Time, i64)` updates.
    pub fn write<'a, I>(&self, updates: I) -> PFuture<SeqNo>
    where
        I: IntoIterator<Item = &'a ((K, V), u64, i64)>,
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

impl<'a, K: Codec, V: Codec, T: Borrow<((K, V), u64, i64)>> FromIterator<T>
    for WriteReqBuilder<K, V>
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint();

        let mut builder = WriteReqBuilder {
            records: ColumnarRecordsVecBuilder::default(),
            key_buf: Vec::new(),
            val_buf: Vec::new(),
            _phantom: PhantomData,
        };
        for record in iter {
            let ((key, val), ts, diff) = record.borrow();
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
#[derive(Debug, Clone)]
pub struct MultiWriteHandle {
    stream_ids: HashSet<Id>,
    // This is an `Err` in case any (potentially recoverable) setup operation failed. We don't fail
    // on those setup calls because we cannot fail when setting up dataflows.
    //
    // We do return an `Err` from initialization code on usage errors or unrecoverable errors, such
    // as using two different runtime clients with the same `MultiWriteHandle`.
    client: Result<RuntimeClient, Error>,
}

// Errored handles are never equal to another handle. If there are no errors, defer to
// `RuntimeClient` and compare contained stream IDs.
impl PartialEq for MultiWriteHandle {
    fn eq(&self, other: &Self) -> bool {
        if self.client.is_err() || other.client.is_err() {
            return false;
        }

        self.stream_ids.eq(&other.stream_ids) && self.client.eq(&other.client)
    }
}

impl MultiWriteHandle {
    /// Returns `Some(Error)` in case any initialization operation on this [`MultiWriteHandle`]
    /// resulted in an `Error`.
    pub fn err(&self) -> Option<Error> {
        self.client.as_ref().err().cloned()
    }

    /// Returns a new [MultiWriteHandle] for the given stream.
    pub fn new<K: Codec, V: Codec>(handle: &StreamWriteHandle<K, V>) -> Self {
        let mut stream_ids = HashSet::new();

        let client = match handle.stream_id() {
            Ok(id) => {
                stream_ids.insert(id);
                Ok(handle.client.clone())
            }
            Err(e) => Err(e),
        };

        MultiWriteHandle { stream_ids, client }
    }

    /// Returns a new [MultiWriteHandle] for the given streams.
    ///
    /// Convenience function for when all the streams have matching K and V.
    ///
    /// NOTE: This only returns an `Error` in case the passed iterator contains no handles or when
    /// passing in handles from different persist runtimes. All other errors are handled
    /// internally.
    pub fn new_from_streams<K, V, H, I>(mut handles: I) -> Result<Self, Error>
    where
        K: Codec,
        V: Codec,
        H: Borrow<StreamWriteHandle<K, V>>,
        I: Iterator<Item = H>,
    {
        // Manually pop the first handle to construct the MultiWriteHandle, then
        // continue with the same iter to add the rest.
        let mut ret = match handles.next() {
            Some(handle) => Self::new(handle.borrow()),
            None => return Err(Error::from("MultiWriteHandle requires at least one stream")),
        };

        for handle in handles {
            ret.add_stream(handle.borrow())?;
        }

        Ok(ret)
    }

    /// Adds the given stream to the set
    ///
    /// NOTE: This only returns an `Error` when passing in handles from different persist runtimes.
    /// All other errors are handled internally.
    pub fn add_stream<K: Codec, V: Codec>(
        &mut self,
        handle: &StreamWriteHandle<K, V>,
    ) -> Result<(), Error> {
        let client = match self.client.as_ref() {
            Ok(client) => client,
            Err(_) => {
                // Return, because we already are in an errored state.
                // NOTE: The errors we keep in `client` are potentially recoverable/are not usage
                // errors, so we only surface them when calling the methods that use the handle.
                return Ok(());
            }
        };

        if handle.client != *client {
            return Err(Error::from(format!(
                "MultiWriteHandle got handles from two runtimes: {:?} and {:?}",
                self.client, handle.client
            )));
        }

        match handle.stream_id() {
            Ok(id) => {
                // It's odd if there are duplicates but the semantics of what that
                // means are straightforward, so for now we support it.
                self.stream_ids.insert(id);
            }
            Err(e) => {
                // Remember the error for future calls.
                self.client = Err(e);
            }
        }

        Ok(())
    }

    /// Atomically writes the given updates to the paired streams.
    ///
    /// Either all of the writes will be made durable for replay or none of them
    /// will.
    pub fn write_atomic<F: FnOnce(&mut AtomicWriteBuilder<'_>) -> Result<(), Error>>(
        &self,
        f: F,
    ) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();

        let client = match self.client.as_ref() {
            Ok(client) => client,
            Err(e) => {
                tx.fill(Err(e.clone()));
                return rx;
            }
        };

        let mut builder = AtomicWriteBuilder {
            stream_ids: &self.stream_ids,
            records: Vec::new(),
        };

        if let Err(err) = f(&mut builder) {
            tx.fill(Err(err));
            return rx;
        }
        client
            .sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Write(builder.records, tx)));
        rx
    }

    /// Closes the streams at the given timestamp, migrating data strictly less
    /// than it into the trace.
    ///
    /// Ids may not be duplicated (this is equivalent to sealing the stream
    /// twice at the same timestamp, which we currently disallow).
    pub fn seal(&self, ids: &[Id], upper: u64) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();

        let client = match self.client.as_ref() {
            Ok(client) => client,
            Err(e) => {
                tx.fill(Err(e.clone()));
                return rx;
            }
        };

        for stream_id in ids {
            if !self.stream_ids.contains(stream_id) {
                tx.fill(Err(Error::from(format!(
                    "MultiWriteHandle cannot seal stream: {:?}",
                    stream_id
                ))));
                return rx;
            }
        }
        client
            .sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Seal(ids.to_vec(), upper, tx)));
        rx
    }

    /// Closes all contained streams at the given timestamp, migrating data strictly less than it
    /// into the trace.
    pub fn seal_all(&self, upper: u64) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();

        let client = match self.client.as_ref() {
            Ok(client) => client,
            Err(e) => {
                tx.fill(Err(e.clone()));
                return rx;
            }
        };

        let ids = self.stream_ids.iter().map(|id| *id).collect::<Vec<_>>();

        client
            .sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::Seal(ids, upper, tx)));
        rx
    }

    /// Unblocks compaction for updates for the given streams at the paired
    /// `since` timestamp.
    ///
    /// Ids may not be duplicated (this is equivalent to allowing compaction on
    /// the stream twice at the same timestamp, which we currently disallow).
    pub fn allow_compaction(&self, id_sinces: &[(Id, Antichain<u64>)]) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();

        let client = match self.client.as_ref() {
            Ok(client) => client,
            Err(e) => {
                tx.fill(Err(e.clone()));
                return rx;
            }
        };

        for (stream_id, _) in id_sinces {
            if !self.stream_ids.contains(stream_id) {
                tx.fill(Err(Error::from(format!(
                    "MultiWriteHandle cannot allow_compaction stream: {:?}",
                    stream_id
                ))));
                return rx;
            }
        }
        client
            .sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::AllowCompaction(
                id_sinces.to_vec(),
                tx,
            )));
        rx
    }

    /// Atomically unblocks compaction for all contained streams up to the given `since` frontier.
    pub fn allow_compaction_all(&self, since: Antichain<u64>) -> PFuture<SeqNo> {
        let (tx, rx) = PFuture::new();

        let client = match self.client.as_ref() {
            Ok(client) => client,
            Err(e) => {
                tx.fill(Err(e.clone()));
                return rx;
            }
        };

        let id_sinces = self
            .stream_ids
            .iter()
            .map(|id| (*id, since.clone()))
            .collect::<Vec<_>>();

        client
            .sender
            .send_runtime_cmd(RuntimeCmd::IndexedCmd(Cmd::AllowCompaction(id_sinces, tx)));
        rx
    }
}

/// A buffer for staging a set of records to write atomically.
///
/// Feel free to think of this as a write-only transaction.
#[derive(Debug)]
pub struct AtomicWriteBuilder<'a> {
    stream_ids: &'a HashSet<Id>,
    records: Vec<(Id, ColumnarRecords)>,
}

impl AtomicWriteBuilder<'_> {
    /// Adds the given updates to the set that will be written atomically.
    ///
    /// Streams may be duplicated. However, the updates are passed down to
    /// storage unchanged, so users should coalesce them when that's not
    /// otherwise slower.
    pub fn add_write<K, V, T, I>(
        &mut self,
        handle: &StreamWriteHandle<K, V>,
        updates: I,
    ) -> Result<(), Error>
    where
        K: Codec,
        V: Codec,
        T: Borrow<((K, V), u64, i64)>,
        I: IntoIterator<Item = T>,
    {
        let stream_id = handle.stream_id()?;
        if !self.stream_ids.contains(&stream_id) {
            return Err(Error::from(format!(
                "MultiWriteHandle cannot write to stream: {:?}",
                stream_id
            )));
        }
        let records = WriteReqBuilder::<K, V>::from_iter(updates).finish();
        self.records
            .extend(records.into_iter().map(|x| (stream_id, x)));
        Ok(())
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

    /// Returns a set of `num_iters` [Iterator]s that each output roughly
    /// `1/num_iters` of the data represented by this snapshot.
    pub fn into_iters(self, num_iters: NonZeroUsize) -> Vec<DecodedSnapshotIter<K, V>> {
        self.snap
            .into_iters(num_iters)
            .into_iter()
            .map(|iter| DecodedSnapshotIter { iter })
            .collect()
    }

    /// Returns a single [Iterator] that outputs the data represented by this
    /// snapshot.
    pub fn into_iter(self) -> DecodedSnapshotIter<K, V> {
        let mut iters = self.into_iters(NonZeroUsize::new(1).unwrap());
        assert_eq!(iters.len(), 1);
        iters.remove(0)
    }
}

#[cfg(test)]
impl<K: Codec + Ord, V: Codec + Ord> DecodedSnapshot<K, V> {
    /// A full read of the data in the snapshot.
    pub fn read_to_end(self) -> Result<Vec<((K, V), u64, i64)>, Error> {
        let iter = self.into_iter();
        let mut buf = iter.collect::<Result<Vec<_>, Error>>()?;
        buf.sort();
        Ok(buf)
    }
}

/// An [Iterator] representing one part of the data in a [DecodedSnapshot].
#[derive(Debug)]
pub struct DecodedSnapshotIter<K, V> {
    iter: ArrangementSnapshotIter<Result<K, String>, Result<V, String>>,
}

impl<K: Codec, V: Codec> Iterator for DecodedSnapshotIter<K, V> {
    type Item = Result<((K, V), u64, i64), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            let ((k, v), ts, diff) = x?;
            let k = k?;
            let v = v?;
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
            error!("error while stopping dropped persist runtime: {}", err);
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

/// A handle for a persisted stream of ((Key, Value), Time, i64) updates backed
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
