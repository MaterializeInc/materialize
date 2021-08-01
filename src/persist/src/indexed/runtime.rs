// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runtime for concurrent, asynchronous use of [Indexed], and the public API
//! used by the rest of the crate to connect to it.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use log;

use crate::error::Error;
use crate::indexed::encoding::Id;
use crate::indexed::{Indexed, IndexedSnapshot, ListenFn};
use crate::storage::{Blob, Buffer, SeqNo};
use crate::Data;

enum Cmd<K, V> {
    Register(String, CmdResponse<Id>),
    Destroy(String, CmdResponse<bool>),
    Write(Vec<(Id, Vec<((K, V), u64, isize)>)>, CmdResponse<SeqNo>),
    Seal(Vec<Id>, u64, CmdResponse<()>),
    AllowCompaction(Id, u64, CmdResponse<()>),
    Snapshot(Id, CmdResponse<IndexedSnapshot<K, V>>),
    Listen(Id, ListenFn<K, V>, CmdResponse<()>),
    Stop(CmdResponse<()>),
}

/// Starts the runtime in a [std::thread].
///
/// This returns a clone-able client handle. The runtime is stopped when any
/// client calls [RuntimeClient::stop] or when all clients have been dropped.
///
// TODO: At the moment, this runs IO and heavy cpu work in a single thread.
// Move this work out into whatever async runtime the user likes, via something
// like https://docs.rs/rdkafka/0.26.0/rdkafka/util/trait.AsyncRuntime.html
pub fn start<K, V, U, L>(buf: U, blob: L) -> Result<RuntimeClient<K, V>, Error>
where
    K: Data + Send + Sync + 'static,
    V: Data + Send + Sync + 'static,
    U: Buffer + Send + 'static,
    L: Blob + Send + 'static,
{
    let indexed = Indexed::new(buf, blob)?;
    // TODO: Is an unbounded channel the right thing to do here?
    let (tx, rx) = crossbeam_channel::unbounded();
    let runtime_f = move || {
        // TODO: Set up the tokio or other async runtime context here.
        let mut l = RuntimeImpl { indexed, rx };
        while l.work() {}
    };
    let id = RuntimeId::new();
    let handle = thread::Builder::new()
        .name(format!("persist-runtime-{}", id.0))
        .spawn(runtime_f)?;
    let handle = Mutex::new(Some(handle));
    let core = RuntimeCore { handle, tx };
    Ok(RuntimeClient {
        id,
        core: Arc::new(core),
    })
}

/// A receiver for the `Result<T, Error>` response of an asynchronous command.
pub enum CmdResponse<T> {
    /// No response is desired.
    Ignore,
    /// A single channel send per command call.
    ///
    /// It is supported for multiple command results to be sent to the same
    /// channel by cloning the sender. This even works if they are different
    /// types of command, but the expected usage is to issue N writes and then
    /// wait on them all by receiving N times.
    ///
    /// NB: The given Sender may be dropped without having received a message if
    /// the runtime is shut down.
    StdChannel(mpsc::Sender<Result<T, Error>>),
    /// A callback executed once.
    ///
    /// NB: This isn't guaranteed to get called because of a race with runtime
    /// shutdown.
    Callback(Box<dyn FnOnce(Result<T, Error>) + Send + 'static>),
    // TODO: Add a std::future::Future variant to better play with async? I
    // think the Future impl would be pretty straightforward.
}

impl<T> From<mpsc::Sender<Result<T, Error>>> for CmdResponse<T> {
    fn from(c: mpsc::Sender<Result<T, Error>>) -> Self {
        CmdResponse::StdChannel(c)
    }
}

impl<T> From<Box<dyn FnOnce(Result<T, Error>) + Send + 'static>> for CmdResponse<T> {
    fn from(c: Box<dyn FnOnce(Result<T, Error>) + Send + 'static>) -> Self {
        CmdResponse::Callback(c)
    }
}

impl<T> CmdResponse<T> {
    pub(crate) fn send(self, t: Result<T, Error>) {
        match self {
            CmdResponse::Ignore => {}
            CmdResponse::Callback(c) => c(t),
            CmdResponse::StdChannel(c) => {
                if let Err(mpsc::SendError(_)) = c.send(t) {
                    // The SendError docs indicate that this only happens if the
                    // receiver was dropped. If the client that sent this cmd
                    // has hung up (imagine an INSERT spawned a Cmd::Write, but
                    // the pgwire connection has been severed while it synced to
                    // disk), then no one is listening and that's okay.
                }
                // Defensively drop c, just to make it obvious.
                drop(c)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RuntimeId(u64);

impl RuntimeId {
    pub fn new() -> Self {
        let mut h = DefaultHasher::new();
        Instant::now().hash(&mut h);
        RuntimeId(h.finish())
    }
}

#[derive(Debug)]
struct RuntimeCore<K, V> {
    handle: Mutex<Option<JoinHandle<()>>>,
    tx: crossbeam_channel::Sender<Cmd<K, V>>,
}

impl<K, V> RuntimeCore<K, V> {
    fn send(&self, cmd: Cmd<K, V>) {
        if let Err(crossbeam_channel::SendError(cmd)) = self.tx.send(cmd) {
            // According to the docs, a SendError can only happen if the
            // receiver has hung up, which in this case only happens if the
            // thread has exited. The thread only exits if we send it a
            // Cmd::Stop (or it panics).
            match cmd {
                Cmd::Stop(res) => {
                    // Already stopped: no-op.
                    res.send(Ok(()))
                }
                Cmd::Register(_, res) => {
                    res.send(Err(Error::from("register cmd sent to stopped runtime")))
                }
                Cmd::Destroy(_, res) => {
                    res.send(Err(Error::from("destroy cmd sent to stopped runtime")))
                }
                Cmd::Write(_, res) => {
                    res.send(Err(Error::from("write cmd sent to stopped runtime")))
                }
                Cmd::Seal(_, _, res) => {
                    res.send(Err(Error::from("seal cmd sent to stopped runtime")))
                }
                Cmd::AllowCompaction(_, _, res) => res.send(Err(Error::from(
                    "allow_compaction cmd sent to stopped runtime",
                ))),
                Cmd::Snapshot(_, res) => {
                    res.send(Err(Error::from("snapshot cmd sent to stopped runtime")))
                }
                Cmd::Listen(_, _, res) => {
                    res.send(Err(Error::from("listen cmd sent to stopped runtime")))
                }
            }
        }
    }

    fn stop(&self) -> Result<(), Error> {
        if let Some(handle) = self.handle.lock()?.take() {
            let (tx, rx) = mpsc::channel();
            self.send(Cmd::Stop(tx.into()));
            // NB: Make sure there are no early returns before this `join`,
            // otherwise the runtime thread might still be cleaning up when this
            // returns (flushing out final writes, cleaning up LOCK files, etc).
            //
            // TODO: Regression test for this.
            if let Err(_) = handle.join() {
                // If the thread panic'd, then by definition it has been
                // stopped, so we can return an Ok. This is surprising, though,
                // so log a message. Unfortunately, there isn't really a way to
                // put the panic message in this log.
                log::error!("persist runtime thread panic'd");
            }
            rx.recv().map_err(|_| Error::RuntimeShutdown)?
        } else {
            Ok(())
        }
    }
}

impl<K, V> Drop for RuntimeCore<K, V> {
    fn drop(&mut self) {
        if let Err(err) = self.stop() {
            log::error!("error while stopping dropped persist runtime: {}", err);
        }
    }
}

/// A clone-able handle to the persistence runtime.
///
/// The runtime is stopped when any client calls [RuntimeClient::stop] or when
/// all clients have been dropped, whichever comes first.
///
/// NB: The methods below are executed concurrently. For a some call X to be
/// guaranteed as "previous" to another call Y, X's response must have been
/// received before Y's call started.
pub struct RuntimeClient<K, V> {
    id: RuntimeId,
    core: Arc<RuntimeCore<K, V>>,
}

impl<K, V> Clone for RuntimeClient<K, V> {
    fn clone(&self) -> Self {
        RuntimeClient {
            id: self.id.clone(),
            core: self.core.clone(),
        }
    }
}

impl<K, V> fmt::Debug for RuntimeClient<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeClient")
            .field(&"id", &self.id)
            .field(&"core", &"..")
            .finish()
    }
}

impl<K, V> PartialEq for RuntimeClient<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<K, V> Eq for RuntimeClient<K, V> {}

impl<K: Clone, V: Clone> RuntimeClient<K, V> {
    /// Synchronously registers a new stream for writes and reads.
    ///
    /// This method is idempotent. Returns read and write handles used to
    /// construct a persisted stream operator.
    ///
    /// If data was written by a previous [RuntimeClient] for this id, it's
    /// loaded and replayed into the stream once constructed.
    pub fn create_or_load(
        &self,
        id: &str,
    ) -> Result<(StreamWriteHandle<K, V>, StreamReadHandle<K, V>), Error> {
        let (tx, rx) = mpsc::channel();
        self.core.send(Cmd::Register(id.to_owned(), tx.into()));
        let id = rx.recv().map_err(|_| Error::RuntimeShutdown)??;
        let write = StreamWriteHandle::new(id, self.clone());
        let meta = StreamReadHandle::new(id, self.clone());
        Ok((write, meta))
    }

    /// Asynchronously persists `(Key, Value, Time, Diff)` updates for the
    /// streams with the given ids.
    ///
    /// The ids must have previously been registered.
    fn write(&self, updates: Vec<(Id, Vec<((K, V), u64, isize)>)>, res: CmdResponse<SeqNo>) {
        self.core.send(Cmd::Write(updates, res))
    }

    /// Asynchronously advances the "sealed" frontier for the streams with the
    /// given ids, which restricts what times can later be sealed and/or written
    /// for those ids.
    ///
    /// The ids must have previously been registered.
    fn seal(&self, ids: &[Id], ts: u64, res: CmdResponse<()>) {
        self.core.send(Cmd::Seal(ids.to_vec(), ts, res))
    }

    /// Asynchronously advances the compaction frontier for the stream with the given id,
    /// which lets the stream discard historical detail for times not beyond the
    /// compaction frontier. This also restricts what times the compaction frontier
    /// can later be advanced to for this id.
    ///
    /// The id must have previously been registered.
    fn allow_compaction(&self, id: Id, ts: u64, res: CmdResponse<()>) {
        self.core.send(Cmd::AllowCompaction(id, ts, res))
    }

    /// Asynchronously returns a [crate::indexed::Snapshot] for the stream
    /// with the given id.
    ///
    /// This snapshot is guaranteed to include any previous writes.
    ///
    /// The id must have previously been registered.
    fn snapshot(&self, id: Id, res: CmdResponse<IndexedSnapshot<K, V>>) {
        self.core.send(Cmd::Snapshot(id, res))
    }

    /// Asynchronously registers a callback to be invoked on successful writes
    /// and seals.
    fn listen(&self, id: Id, listen_fn: ListenFn<K, V>, res: CmdResponse<()>) {
        self.core.send(Cmd::Listen(id, listen_fn, res))
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
        let (tx, rx) = mpsc::channel();
        self.core.send(Cmd::Destroy(id.to_owned(), tx.into()));
        rx.recv().map_err(|_| Error::RuntimeShutdown)?
    }
}

/// A handle that allows writes of ((Key, Value), Time, Diff) updates into an
/// [crate::indexed::Indexed] via a [RuntimeClient].
#[derive(Debug)]
pub struct StreamWriteHandle<K, V> {
    id: Id,
    runtime: RuntimeClient<K, V>,
}

impl<K, V> Clone for StreamWriteHandle<K, V> {
    fn clone(&self) -> Self {
        StreamWriteHandle {
            id: self.id,
            runtime: self.runtime.clone(),
        }
    }
}

impl<K: Clone, V: Clone> StreamWriteHandle<K, V> {
    /// Returns a new [StreamWriteHandle] for the given stream.
    pub fn new(id: Id, runtime: RuntimeClient<K, V>) -> Self {
        StreamWriteHandle { id, runtime }
    }

    /// Returns the stream [Id] for this handle.
    pub fn stream_id(&self) -> Id {
        self.id
    }

    /// Synchronously writes (Key, Value, Time, Diff) updates.
    pub fn write(&self, updates: &[((K, V), u64, isize)], res: CmdResponse<SeqNo>) {
        self.runtime.write(vec![(self.id, updates.to_vec())], res);
    }

    /// Closes the stream at the given timestamp, migrating data strictly less
    /// than it into the trace.
    pub fn seal(&self, upper: u64, res: CmdResponse<()>) {
        self.runtime.seal(&[self.id], upper, res);
    }
}

/// A handle for writing to multiple streams.
#[derive(Debug, PartialEq, Eq)]
pub struct MultiWriteHandle<K, V> {
    ids: HashSet<Id>,
    runtime: RuntimeClient<K, V>,
}

impl<K, V> Clone for MultiWriteHandle<K, V> {
    fn clone(&self) -> Self {
        MultiWriteHandle {
            ids: self.ids.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl<K: Clone, V: Clone> MultiWriteHandle<K, V> {
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
            stream_ids.insert(handle.id);
        }
        Ok(MultiWriteHandle {
            ids: stream_ids,
            runtime,
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
    pub fn write_atomic(
        &self,
        updates: Vec<(Id, Vec<((K, V), u64, isize)>)>,
        res: CmdResponse<SeqNo>,
    ) {
        for (id, _) in updates.iter() {
            if !self.ids.contains(id) {
                res.send(Err(Error::from(format!(
                    "MultiWriteHandle cannot write to stream: {:?}",
                    id
                ))));
                return;
            }
        }
        self.runtime.write(updates, res)
    }

    /// Closes the streams at the given timestamp, migrating data strictly less
    /// than it into the trace.
    ///
    /// Ids may not be duplicated (this is equivalent to sealing the stream
    /// twice at the same timestamp, which we currently disallow).
    pub fn seal(&self, ids: &[Id], upper: u64, res: CmdResponse<()>) {
        for id in ids {
            if !self.ids.contains(id) {
                res.send(Err(Error::from(format!(
                    "MultiWriteHandle cannot seal stream: {:?}",
                    id
                ))));
                return;
            }
        }
        self.runtime.seal(ids, upper, res)
    }
}

/// A handle for a persisted stream of ((Key, Value), Time, Diff) updates backed
/// by an [crate::indexed::Indexed] via a [RuntimeClient].
#[derive(Debug)]
pub struct StreamReadHandle<K, V> {
    id: Id,
    runtime: RuntimeClient<K, V>,
}

impl<K, V> Clone for StreamReadHandle<K, V> {
    fn clone(&self) -> Self {
        StreamReadHandle {
            id: self.id,
            runtime: self.runtime.clone(),
        }
    }
}

impl<K: Clone, V: Clone> StreamReadHandle<K, V> {
    /// Returns a new [StreamReadHandle] for the given stream.
    pub fn new(id: Id, runtime: RuntimeClient<K, V>) -> Self {
        StreamReadHandle { id, runtime }
    }

    /// Returns a consistent snapshot of all previously persisted stream data.
    pub fn snapshot(&self) -> Result<IndexedSnapshot<K, V>, Error> {
        // TODO: Make snapshot signature non-blocking.
        let (rx, tx) = mpsc::channel();
        self.runtime.snapshot(self.id, rx.into());
        tx.recv()
            .map_err(|_| Error::RuntimeShutdown)
            .and_then(std::convert::identity)
    }

    /// Registers a callback to be invoked on successful writes and seals.
    pub fn listen(&self, listen_fn: ListenFn<K, V>) -> Result<(), Error> {
        let (rx, tx) = mpsc::channel();
        self.runtime.listen(self.id, listen_fn, rx.into());
        tx.recv().map_err(|_| Error::RuntimeShutdown)?
    }

    /// Unblocks compaction for updates at or before `since`.
    pub fn allow_compaction(&mut self, since: u64, res: CmdResponse<()>) {
        self.runtime.allow_compaction(self.id, since, res);
    }
}

struct RuntimeImpl<K, V, U: Buffer, L: Blob> {
    indexed: Indexed<K, V, U, L>,
    rx: crossbeam_channel::Receiver<Cmd<K, V>>,
}

impl<K: Data, V: Data, U: Buffer, L: Blob> RuntimeImpl<K, V, U, L> {
    /// Synchronously waits for the next command, executes it, and responds.
    ///
    /// Returns false to indicate a graceful shutdown, true otherwise.
    fn work(&mut self) -> bool {
        let cmd = match self.rx.recv() {
            Ok(cmd) => cmd,
            Err(crossbeam_channel::RecvError) => {
                // All Runtime handles hung up. Drop should have shut things down
                // nicely, so this is unexpected.
                return false;
            }
        };
        match cmd {
            Cmd::Stop(res) => {
                res.send(self.indexed.close());
                return false;
            }
            Cmd::Register(id, res) => {
                let r = self.indexed.register(&id);
                res.send(r);
            }
            Cmd::Destroy(id, res) => {
                let r = self.indexed.destroy(&id);
                res.send(r);
            }
            Cmd::Write(updates, res) => {
                let write_res = self.indexed.write_sync(updates);
                // TODO: Move this to a Cmd::Tick or something.
                let step_res = self.indexed.step();
                res.send(step_res.and_then(|_| write_res));
            }
            Cmd::Seal(ids, ts, res) => {
                let r = self.indexed.seal(ids, ts);
                res.send(r);
            }
            Cmd::AllowCompaction(id, ts, res) => {
                let r = self.indexed.allow_compaction(id, ts);
                res.send(r);
            }
            Cmd::Snapshot(id, res) => {
                let r = self.indexed.snapshot(id);
                res.send(r);
            }
            Cmd::Listen(id, listen_fn, res) => {
                let r = self.indexed.listen(id, listen_fn);
                res.send(r);
            }
        }
        return true;
    }
}

#[cfg(test)]
mod tests {
    use crate::indexed::SnapshotExt;
    use crate::mem::{MemBlob, MemBuffer, MemRegistry};

    use super::*;

    fn block_on<T, F: FnOnce(CmdResponse<T>)>(f: F) -> Result<T, Error> {
        let (tx, rx) = mpsc::channel();
        f(tx.into());
        rx.recv().map_err(|_| Error::RuntimeShutdown)?
    }

    #[test]
    fn runtime() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let buffer = MemBuffer::new("runtime");
        let blob = MemBlob::new("runtime");
        let mut runtime = start(buffer, blob)?;

        let (write, meta) = runtime.create_or_load("0")?;
        block_on(|res| write.write(&data, res))?;
        let snap = meta.snapshot()?;
        assert_eq!(snap.read_to_end(), data);

        // Commands sent after stop return an error, but calling stop again is
        // fine.
        runtime.stop()?;
        assert!(runtime.create_or_load("0").is_err());
        runtime.stop()?;

        Ok(())
    }

    #[test]
    fn concurrent() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let buffer = MemBuffer::new("concurrent");
        let blob = MemBlob::new("concurrent");
        let client1 = start(buffer, blob)?;
        let _ = client1.create_or_load("0")?;

        // Everything is still running after client1 is dropped.
        let mut client2 = client1.clone();
        drop(client1);
        let (write, meta) = client2.create_or_load("0")?;
        block_on(|res| write.write(&data, res))?;
        let snap = meta.snapshot()?;
        assert_eq!(snap.read_to_end(), data);
        client2.stop()?;

        Ok(())
    }

    #[test]
    fn restart() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let mut registry = MemRegistry::new();

        // Shutdown happens if we explicitly call stop, unlocking the buffer and
        // blob and allowing them to be reused in the next Indexed.
        let mut persister = registry.open("path", "restart-1")?;
        let (write, _) = persister.create_or_load("0")?;
        block_on(|res| write.write(&data[0..1], res))?;
        assert_eq!(persister.stop(), Ok(()));

        // Shutdown happens if all handles are dropped, even if we don't call
        // stop.
        let persister = registry.open("path", "restart-2")?;
        let (write, _) = persister.create_or_load("0")?;
        block_on(|res| write.write(&data[1..2], res))?;
        drop(write);
        drop(persister);

        // We can read back what we previously wrote.
        {
            let persister = registry.open("path", "restart-1")?;
            let (_, meta) = persister.create_or_load("0")?;
            let snap = meta.snapshot()?;
            assert_eq!(snap.read_to_end(), data);
        }

        Ok(())
    }

    #[test]
    fn multi_write_handle() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), ()), 1, 1),
            (("key2".to_string(), ()), 1, 1),
        ];

        let mut registry = MemRegistry::new();
        let client1 = registry.open::<String, ()>("1", "multi")?;
        let client2 = registry.open::<String, ()>("2", "multi")?;

        let (c1s1, c1s1_read) = client1.create_or_load("1")?;
        let (c1s2, c1s2_read) = client1.create_or_load("2")?;
        let (c2s1, _) = client2.create_or_load("1")?;

        // Cannot construct with no streams.
        assert!(MultiWriteHandle::<(), ()>::new(&[]).is_err());

        // Cannot construct with streams from different runtimes.
        assert!(MultiWriteHandle::new(&[&c1s2, &c2s1]).is_err());

        // Normal write
        let multi = MultiWriteHandle::new(&[&c1s1, &c1s2])?;
        let updates = vec![
            (c1s1.stream_id(), data[..1].to_vec()),
            (c1s2.stream_id(), data[1..].to_vec()),
        ];
        block_on(|res| multi.write_atomic(updates, res))?;
        assert_eq!(c1s1_read.snapshot()?.read_to_end(), data[..1].to_vec());
        assert_eq!(c1s2_read.snapshot()?.read_to_end(), data[1..].to_vec());

        // Normal seal
        let ids = &[c1s1.stream_id(), c1s2.stream_id()];
        block_on(|res| multi.seal(ids, 2, res))?;
        // We don't expose reading the seal directly, so hack it a bit here by
        // verifying that we can't re-seal at the same timestamp (which is
        // disallowed).
        let expected_seal_err = Err(Error::from("invalid seal: Antichain { elements: [2] } not in advance of current seal frontier Antichain { elements: [2] }"));
        assert_eq!(block_on(|res| c1s1.seal(2, res)), expected_seal_err);
        assert_eq!(block_on(|res| c1s2.seal(2, res)), expected_seal_err);

        // Cannot write to streams not specified during construction.
        let (c1s3, _) = client1.create_or_load("3")?;
        assert!(
            block_on(|res| multi.write_atomic(vec![(c1s3.stream_id(), data.clone())], res))
                .is_err()
        );

        // Cannot seal streams not specified during construction.
        assert!(block_on(|res| multi.seal(&[c1s3.stream_id()], 3, res)).is_err());

        Ok(())
    }
}
