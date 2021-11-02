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
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use log;
use ore::metrics::MetricsRegistry;
use persist_types::Codec;
use timely::progress::Antichain;
use tokio::runtime::Runtime;

use crate::error::Error;
use crate::indexed::background::Maintainer;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::Id;
use crate::indexed::metrics::{metric_duration_ms, Metrics};
use crate::indexed::{Indexed, IndexedSnapshot, IndexedSnapshotIter, ListenFn, Snapshot};
use crate::pfuture::{PFuture, PFutureHandle};
use crate::storage::{Blob, Log, SeqNo};

enum Cmd {
    Register(String, (&'static str, &'static str), PFutureHandle<Id>),
    Destroy(String, PFutureHandle<bool>),
    Write(
        Vec<(Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>)>,
        PFutureHandle<SeqNo>,
    ),
    Seal(Vec<Id>, u64, PFutureHandle<()>),
    AllowCompaction(Vec<(Id, Antichain<u64>)>, PFutureHandle<()>),
    Snapshot(Id, PFutureHandle<IndexedSnapshot>),
    Listen(
        Id,
        ListenFn<Vec<u8>, Vec<u8>>,
        PFutureHandle<IndexedSnapshot>,
    ),
    Stop(PFutureHandle<()>),
    /// A no-op command sent on a regular interval so the runtime has an
    /// opportunity to do periodic maintenance work.
    Tick,
}

/// Starts the runtime in a [std::thread].
///
/// This returns a clone-able client handle. The runtime is stopped when any
/// client calls [RuntimeClient::stop] or when all clients have been dropped.
///
/// If Some, the given [tokio::runtime::Runtime] is used for IO and cpu heavy
/// operations. If None, a new Runtime is constructed for this. The latter
/// requires that we are not in the context of an existing Runtime, so if this
/// is the case, the caller must use the Some form.
//
// TODO: The rust doc above is still a bit of a lie. Actually use this runtime
// for all IO and cpu heavy operations.
//
// TODO: The whole story around Runtime usage in persist is pretty awkward and
// still pretty unprincipled. I think when we do the TODO to make the Log and
// Blob storage traits async, this will clear up a bit.
pub fn start<L, B>(
    config: RuntimeConfig,
    log: L,
    blob: B,
    reg: &MetricsRegistry,
    pool: Option<Arc<Runtime>>,
) -> Result<RuntimeClient, Error>
where
    L: Log + Send + 'static,
    B: Blob + Send + 'static,
{
    // TODO: Is an unbounded channel the right thing to do here?
    let (tx, rx) = crossbeam_channel::unbounded();
    let metrics = Metrics::register_with(reg);

    // Any usage of S3Blob requires a runtime context to be set. `Indexed::new`
    // use the blob impl to start the recovery process, so make sure this stays
    // early.
    let pool = match pool {
        Some(pool) => pool,
        None => Arc::new(Runtime::new()?),
    };
    let pool_guard = pool.enter();

    // Start up the runtime.
    let blob = BlobCache::new(metrics.clone(), blob);
    let maintainer = Maintainer::new(blob.clone(), pool.clone());
    let indexed = Indexed::new(log, blob, maintainer, metrics.clone())?;
    let mut runtime = RuntimeImpl::new(config.clone(), indexed, rx, metrics.clone());
    let id = RuntimeId::new();
    let runtime_pool = pool.clone();
    let impl_handle = thread::Builder::new()
        .name(format!("persist-runtime-{}", id.0))
        .spawn(move || {
            let pool_guard = runtime_pool.enter();
            while runtime.work() {}
            // Explictly drop the pool guard so the lifetime is obvious.
            drop(pool_guard);
        })?;

    // Start up the ticker thread.
    //
    // TODO: Now that we have a runtime threaded here, we could use async stuff
    // to do this.
    let ticker_tx = tx.clone();
    let ticker_handle = thread::Builder::new()
        .name(format!("persist-ticker"))
        .spawn(move || {
            // Try to keep worst case command response times to roughly `110% of
            // min_step_interval` by ensuring there's a tick relatively shortly
            // after a step becomes eligible. We could just as easily make this
            // 2 if we decide 150% is okay.
            let tick_interval = config.min_step_interval / 10;
            loop {
                thread::sleep(tick_interval);
                match ticker_tx.send(Cmd::Tick) {
                    Ok(_) => {}
                    Err(_) => {
                        // Runtime has shut down, we can stop ticking.
                        return;
                    }
                }
            }
        })?;

    // Construct the client.
    let handles = Mutex::new(Some(RuntimeHandles {
        impl_handle,
        ticker_handle,
    }));
    let core = RuntimeCore {
        handles,
        tx,
        metrics,
    };
    let client = RuntimeClient {
        id,
        core: Arc::new(core),
    };

    // Explictly drop the pool guard so the lifetime is obvious.
    drop(pool_guard);

    Ok(client)
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
struct RuntimeHandles {
    impl_handle: JoinHandle<()>,
    ticker_handle: JoinHandle<()>,
}

#[derive(Debug)]
struct RuntimeCore {
    handles: Mutex<Option<RuntimeHandles>>,
    tx: crossbeam_channel::Sender<Cmd>,
    metrics: Metrics,
}

impl RuntimeCore {
    fn send(&self, cmd: Cmd) {
        self.metrics.cmd_queue_in.inc();
        if let Err(crossbeam_channel::SendError(cmd)) = self.tx.send(cmd) {
            // According to the docs, a SendError can only happen if the
            // receiver has hung up, which in this case only happens if the
            // thread has exited. The thread only exits if we send it a
            // Cmd::Stop (or it panics).
            match cmd {
                Cmd::Stop(res) => {
                    // Already stopped: no-op.
                    res.fill(Ok(()))
                }
                Cmd::Register(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Destroy(_, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Write(_, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Seal(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::AllowCompaction(_, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Snapshot(_, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Listen(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Tick => {}
            }
        }
    }

    fn stop(&self) -> Result<(), Error> {
        if let Some(handles) = self.handles.lock()?.take() {
            let (tx, rx) = PFuture::new();
            self.send(Cmd::Stop(tx));
            // NB: Make sure there are no early returns before this `join`,
            // otherwise the runtime thread might still be cleaning up when this
            // returns (flushing out final writes, cleaning up LOCK files, etc).
            //
            // TODO: Regression test for this.
            if let Err(_) = handles.impl_handle.join() {
                // If the thread panic'd, then by definition it has been
                // stopped, so we can return an Ok. This is surprising, though,
                // so log a message. Unfortunately, there isn't really a way to
                // put the panic message in this log.
                log::error!("persist runtime thread panic'd");
            }
            if let Err(_) = handles.ticker_handle.join() {
                // If the thread panic'd, then by definition it has been
                // stopped, so we can return an Ok. This is surprising, though,
                // so log a message. Unfortunately, there isn't really a way to
                // put the panic message in this log.
                log::error!("persist ticker thread panic'd");
            }
            rx.recv()
        } else {
            Ok(())
        }
    }
}

impl Drop for RuntimeCore {
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
    /// If data was written by a previous [RuntimeClient] for this id, it's
    /// loaded and replayed into the stream once constructed.
    pub fn create_or_load<K: Codec, V: Codec>(
        &self,
        id: &str,
    ) -> Result<(StreamWriteHandle<K, V>, StreamReadHandle<K, V>), Error> {
        let (tx, rx) = PFuture::new();
        self.core.send(Cmd::Register(
            id.to_owned(),
            (K::codec_name(), V::codec_name()),
            tx,
        ));
        let id = rx.recv()?;
        let write = StreamWriteHandle::new(id, self.clone());
        let meta = StreamReadHandle::new(id, self.clone());
        Ok((write, meta))
    }

    /// Asynchronously persists `(Key, Value, Time, Diff)` updates for the
    /// streams with the given ids.
    ///
    /// The ids must have previously been registered.
    fn write(
        &self,
        updates: Vec<(Id, Vec<((Vec<u8>, Vec<u8>), u64, isize)>)>,
        res: PFutureHandle<SeqNo>,
    ) {
        self.core.send(Cmd::Write(updates, res))
    }

    /// Asynchronously advances the "sealed" frontier for the streams with the
    /// given ids, which restricts what times can later be sealed and/or written
    /// for those ids.
    ///
    /// The ids must have previously been registered.
    fn seal(&self, ids: &[Id], ts: u64, res: PFutureHandle<()>) {
        self.core.send(Cmd::Seal(ids.to_vec(), ts, res))
    }

    /// Asynchronously advances the compaction frontier for the streams with the
    /// given ids, which lets the stream discard historical detail for times not
    /// beyond the compaction frontier. This also restricts what times the
    /// compaction frontier can later be advanced to for these ids.
    ///
    /// The ids must have previously been registered.
    fn allow_compaction(&self, id_sinces: &[(Id, Antichain<u64>)], res: PFutureHandle<()>) {
        self.core
            .send(Cmd::AllowCompaction(id_sinces.to_vec(), res))
    }

    /// Asynchronously returns a [crate::indexed::Snapshot] for the stream
    /// with the given id.
    ///
    /// This snapshot is guaranteed to include any previous writes.
    ///
    /// The id must have previously been registered.
    fn snapshot(&self, id: Id, res: PFutureHandle<IndexedSnapshot>) {
        self.core.send(Cmd::Snapshot(id, res))
    }

    /// Asynchronously registers a callback to be invoked on successful writes
    /// and seals.
    fn listen(
        &self,
        id: Id,
        listen_fn: ListenFn<Vec<u8>, Vec<u8>>,
        res: PFutureHandle<IndexedSnapshot>,
    ) {
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
        let (tx, rx) = PFuture::new();
        self.core.send(Cmd::Destroy(id.to_owned(), tx));
        rx.recv()
    }
}

fn encode_updates<'a, K, V, I>(updates: I) -> Vec<((Vec<u8>, Vec<u8>), u64, isize)>
where
    K: Codec,
    V: Codec,
    I: IntoIterator<Item = &'a ((K, V), u64, isize)>,
{
    updates
        .into_iter()
        .map(|((k, v), ts, diff)| {
            let mut key_encoded = Vec::with_capacity(k.size_hint());
            let mut val_encoded = Vec::with_capacity(v.size_hint());
            k.encode(&mut key_encoded);
            v.encode(&mut val_encoded);
            ((key_encoded, val_encoded), *ts, *diff)
        })
        .collect()
}

/// A handle that allows writes of ((Key, Value), Time, Diff) updates into an
/// [crate::indexed::Indexed] via a [RuntimeClient].
#[derive(PartialEq, Eq)]
pub struct StreamWriteHandle<K, V> {
    id: Id,
    runtime: RuntimeClient,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for StreamWriteHandle<K, V> {
    fn clone(&self) -> Self {
        StreamWriteHandle {
            id: self.id,
            runtime: self.runtime.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<K, V> fmt::Debug for StreamWriteHandle<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamWriteHandle")
            .field("id", &self.id)
            .field("runtime", &self.runtime)
            .finish()
    }
}

impl<K: Codec, V: Codec> StreamWriteHandle<K, V> {
    /// Returns a new [StreamWriteHandle] for the given stream.
    pub fn new(id: Id, runtime: RuntimeClient) -> Self {
        StreamWriteHandle {
            id,
            runtime,
            _phantom: PhantomData,
        }
    }

    /// Returns the stream [Id] for this handle.
    pub fn stream_id(&self) -> Id {
        self.id
    }

    /// Synchronously writes (Key, Value, Time, Diff) updates.
    pub fn write<'a, I>(&self, updates: I) -> PFuture<SeqNo>
    where
        I: IntoIterator<Item = &'a ((K, V), u64, isize)>,
    {
        let updates = encode_updates(updates);
        let (tx, rx) = PFuture::new();
        self.runtime.write(vec![(self.id, updates)], tx);
        rx
    }

    /// Closes the stream at the given timestamp, migrating data strictly less
    /// than it into the trace.
    pub fn seal(&self, upper: u64) -> PFuture<()> {
        let (tx, rx) = PFuture::new();
        self.runtime.seal(&[self.id], upper, tx);
        rx
    }

    /// Unblocks compaction for updates at or before `since`.
    pub fn allow_compaction(&self, since: Antichain<u64>) -> PFuture<()> {
        let (tx, rx) = PFuture::new();
        self.runtime.allow_compaction(&[(self.id, since)], tx);
        rx
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
            stream_ids.insert(handle.id);
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
                let updates = encode_updates(updates);
                (*id, updates)
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
    pub fn seal(&self, ids: &[Id], upper: u64) -> PFuture<()> {
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
    pub fn allow_compaction(&self, id_sinces: &[(Id, Antichain<u64>)]) -> PFuture<()> {
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
    snap: IndexedSnapshot,
    _phantom: PhantomData<(K, V)>,
}

impl<K: Codec, V: Codec> DecodedSnapshot<K, V> {
    pub(crate) fn new(snap: IndexedSnapshot) -> Self {
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
    iter: IndexedSnapshotIter,
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
#[derive(Debug, PartialEq, Eq)]
pub struct StreamReadHandle<K, V> {
    id: Id,
    runtime: RuntimeClient,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for StreamReadHandle<K, V> {
    fn clone(&self) -> Self {
        StreamReadHandle {
            id: self.id,
            runtime: self.runtime.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<K: Codec, V: Codec> StreamReadHandle<K, V> {
    /// Returns a new [StreamReadHandle] for the given stream.
    pub fn new(id: Id, runtime: RuntimeClient) -> Self {
        StreamReadHandle {
            id,
            runtime,
            _phantom: PhantomData,
        }
    }

    /// Returns a consistent snapshot of all previously persisted stream data.
    pub fn snapshot(&self) -> Result<DecodedSnapshot<K, V>, Error> {
        // TODO: Make snapshot signature non-blocking.
        let (tx, rx) = PFuture::new();
        self.runtime.snapshot(self.id, tx);
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
        listen_fn: ListenFn<Vec<u8>, Vec<u8>>,
    ) -> Result<DecodedSnapshot<K, V>, Error> {
        let (tx, rx) = PFuture::new();
        self.runtime.listen(self.id, listen_fn, tx);
        let snap = rx.recv()?;
        Ok(DecodedSnapshot::new(snap))
    }
}

struct RuntimeImpl<L: Log, B: Blob> {
    indexed: Indexed<L, B>,
    rx: crossbeam_channel::Receiver<Cmd>,
    metrics: Metrics,
    prev_step: Instant,
    min_step_interval: Duration,
}

/// Configuration for [start]ing a [RuntimeClient].
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Minimum step interval to use
    min_step_interval: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            min_step_interval: Self::DEFAULT_MIN_STEP_INTERVAL,
        }
    }
}

impl RuntimeConfig {
    const DEFAULT_MIN_STEP_INTERVAL: Duration = Duration::from_millis(1000);

    /// An alternate configuration that minimizes latency at the cost of
    /// increased storage traffic.
    pub(crate) fn for_tests() -> Self {
        RuntimeConfig {
            min_step_interval: Duration::from_millis(1),
        }
    }

    /// A configuration with a configurable min_step_interval
    pub fn with_min_step_interval(min_step_interval: Duration) -> Self {
        RuntimeConfig { min_step_interval }
    }
}

impl<L: Log, B: Blob> RuntimeImpl<L, B> {
    fn new(
        config: RuntimeConfig,
        indexed: Indexed<L, B>,
        rx: crossbeam_channel::Receiver<Cmd>,
        metrics: Metrics,
    ) -> Self {
        RuntimeImpl {
            indexed,
            rx,
            metrics,
            // Initialize this so it's ready to trigger immediately.
            prev_step: Instant::now() - config.min_step_interval,
            min_step_interval: config.min_step_interval,
        }
    }

    /// Synchronously waits for the next command, executes it, and responds.
    ///
    /// Returns false to indicate a graceful shutdown, true otherwise.
    fn work(&mut self) -> bool {
        let mut cmds = vec![];
        match self.rx.recv() {
            Ok(cmd) => cmds.push(cmd),
            Err(crossbeam_channel::RecvError) => {
                // All Runtime handles hung up. Drop should have shut things down
                // nicely, so this is unexpected.
                return false;
            }
        };

        let mut more_work = true;

        // Grab as many commands as we can out of the channel and execute them
        // all before calling `step` again to amortise the cost of trace
        // maintenance between them.
        loop {
            match self.rx.try_recv() {
                Ok(cmd) => cmds.push(cmd),
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    break;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    // All Runtime handles hung up. Drop should have shut things down
                    // nicely, so this is unexpected.
                    more_work = false;
                }
            }
        }

        let run_start = Instant::now();
        for cmd in cmds {
            match cmd {
                Cmd::Stop(res) => {
                    // Finish up any pending work that we can before closing.
                    if let Err(e) = self.indexed.step() {
                        self.metrics.cmd_step_error_count.inc();
                        log::warn!("error running step: {:?}", e);
                    }
                    res.fill(self.indexed.close());
                    return false;
                }
                Cmd::Register(id, (key_codec_name, val_codec_name), res) => {
                    self.indexed
                        .register(&id, key_codec_name, val_codec_name, res);
                }
                Cmd::Destroy(id, res) => {
                    self.indexed.destroy(&id, res);
                }
                Cmd::Write(updates, res) => {
                    self.metrics.cmd_write_count.inc();
                    self.indexed.write(updates, res);
                }
                Cmd::Seal(ids, ts, res) => {
                    self.indexed.seal(ids, ts, res);
                }
                Cmd::AllowCompaction(id_sinces, res) => {
                    self.indexed.allow_compaction(id_sinces, res);
                }
                Cmd::Snapshot(id, res) => {
                    self.indexed.snapshot(id, res);
                }
                Cmd::Listen(id, listen_fn, res) => {
                    self.indexed.listen(id, listen_fn, res);
                }
                Cmd::Tick => {
                    // This is a no-op. It's only here to give us the
                    // opportunity to hit the step logic below on some minimum
                    // interval, even when no other commands are coming in.
                }
            }
            self.metrics.cmd_run_count.inc()
        }
        let step_start = Instant::now();
        self.metrics
            .cmd_run_ms
            .inc_by(metric_duration_ms(step_start.duration_since(run_start)));

        // HACK: This rate limits how much we call step in response to workloads
        // consisting entirely of write, seal, and allow_compaction (which is
        // exactly what we expect in the common/steady state). At the moment,
        // step is too aggressive about compaction if called in a tight loop,
        // causing excess disk usage. This is exactly what happens if coord is
        // getting steady read traffic over indexes derived directly or
        // indirectly from tables (it continually seals).
        //
        // The downside to this rate limiting is that it introduces artificial
        // latency into filling the response futures we returned to the client
        // as well as to updating listeners. The former blocks literally no
        // critical paths in the initial persisted system tables test, which is
        // considered an acceptable tradeoff for the short-term. In the
        // long-term, we'll do something better (probably by using Buffer).
        //
        // For context, in the persistent system tables test, we get writes
        // every ~30s (how often we write to mz_metrics and
        // mz_metric_histograms). In a non-loaded system as well as anything
        // that's only selecting from views purely derived from sources, we also
        // get seals every ~30s. In a system that's selecting from mz_metrics in
        // a loop, the selects all take ~1s and we get seals every ~1s. In a
        // system that's selecting from a user table in a tight loop, the
        // selects take ~10ms (the same amount of time they would with
        // persistence disabled) and we get seals every ~10ms.
        //
        // TODO: It would almost certainly be better to separate the rate limit
        // of drain_pending vs the rest of step. The other parts (drain_unsealed
        // and compact) can and should be called even less frequently than this.
        // However, this change is going into a release at the last minute and
        // I'm less confidant about unknown ramifications of a two interval
        // strategy.
        let need_step = step_start.duration_since(self.prev_step) > self.min_step_interval;

        // BONUS HACK: If pending_responses is empty, then step would be a no-op
        // from a user's perspective. Unfortunately, step would still write out
        // meta (three times, in fact). So, we manually skip over it here as an
        // optimization. In a system that's not selecting from tables, this
        // reduces the frequency of step calls from every ~1s
        // (DEFAULT_MIN_STEP_INTERVAL) to ~30s (the frequency of writing to
        // mz_metrics). Otherwise, it has no effect.
        //
        // TODO: Make step smarter and remove this hack.
        let need_step = need_step && self.indexed.pending.has_responses();

        if need_step {
            self.prev_step = step_start;
            if let Err(e) = self.indexed.step() {
                self.metrics.cmd_step_error_count.inc();
                // TODO: revisit whether we need to move this to a different log level
                // depending on how spammy it ends up being. Alternatively, we
                // may want to rate-limit our logging here.
                log::warn!("error running step: {:?}", e);
            }

            self.metrics
                .cmd_step_ms
                .inc_by(metric_duration_ms(step_start.elapsed()));
        }

        return more_work;
    }
}

/// Returns the seal timestamp of the given collection.
// TODO: There are some things we should change about this:
//
// 1. Getting the seal timestamp shouldn't require knowing the types of the collection. This can be
//    achieved by adding a `get_seal(id)` method on `RuntimeClient`. Even better, the method should
//    probably be `get_description()` and return a complete differential `Description` of the
//    contained updates.
//
// 2. We need to figure out what our nomenclature should be around seal vs. upper vs. "a whole
//    Description".
//
pub fn sealed_ts<K: persist_types::Codec, V: persist_types::Codec>(
    read: &StreamReadHandle<K, V>,
) -> Result<u64, Error> {
    let seal_ts = read.snapshot()?.get_seal();

    if let Some(sealed) = seal_ts.first() {
        Ok(*sealed)
    } else {
        use timely::progress::Timestamp;
        Ok(Timestamp::minimum())
    }
}

#[cfg(test)]
mod tests {
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, OkErr, Probe};
    use timely::dataflow::ProbeHandle;

    use crate::indexed::SnapshotExt;
    use crate::mem::{MemMultiRegistry, MemRegistry};
    use crate::operators::source::PersistedSource;
    use crate::operators::split_ok_err;

    use super::*;

    #[test]
    fn runtime() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let mut runtime = MemRegistry::new().runtime_no_reentrance()?;

        let (write, meta) = runtime.create_or_load("0")?;
        write.write(&data).recv()?;
        assert_eq!(meta.snapshot()?.read_to_end()?, data);

        // Commands sent after stop return an error, but calling stop again is
        // fine.
        runtime.stop()?;
        assert!(runtime.create_or_load::<(), ()>("0").is_err());
        runtime.stop()?;

        Ok(())
    }

    #[test]
    fn concurrent() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let client1 = MemRegistry::new().runtime_no_reentrance()?;
        let _ = client1.create_or_load::<String, String>("0")?;

        // Everything is still running after client1 is dropped.
        let mut client2 = client1.clone();
        drop(client1);
        let (write, meta) = client2.create_or_load("0")?;
        write.write(&data).recv()?;
        assert_eq!(meta.snapshot()?.read_to_end()?, data);
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

        // Shutdown happens if we explicitly call stop, unlocking the log and
        // blob and allowing them to be reused in the next Indexed.
        let mut persister = registry.runtime_no_reentrance()?;
        let (write, _) = persister.create_or_load("0")?;
        write.write(&data[0..1]).recv()?;
        assert_eq!(persister.stop(), Ok(()));

        // Shutdown happens if all handles are dropped, even if we don't call
        // stop.
        let persister = registry.runtime_no_reentrance()?;
        let (write, _) = persister.create_or_load("0")?;
        write.write(&data[1..2]).recv()?;
        drop(write);
        drop(persister);

        // We can read back what we previously wrote.
        {
            let persister = registry.runtime_no_reentrance()?;
            let (_, meta) = persister.create_or_load("0")?;
            assert_eq!(meta.snapshot()?.read_to_end()?, data);
        }

        Ok(())
    }

    #[test]
    fn multi_write_handle() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), ()), 1, 1),
            (("key2".to_string(), ()), 1, 1),
        ];

        let mut registry = MemMultiRegistry::new();
        let client1 = registry.open("1", "multi")?;
        let client2 = registry.open("2", "multi")?;

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
        multi.write_atomic(updates).recv()?;
        assert_eq!(c1s1_read.snapshot()?.read_to_end()?, data[..1].to_vec());
        assert_eq!(c1s2_read.snapshot()?.read_to_end()?, data[1..].to_vec());

        // Normal seal
        let ids = &[c1s1.stream_id(), c1s2.stream_id()];
        multi.seal(ids, 2).recv()?;
        // We don't expose reading the seal directly, so hack it a bit here by
        // verifying that we can't re-seal at a prior timestamp (which is
        // disallowed).
        assert_eq!(c1s1.seal(1).recv(), Err(Error::from("invalid seal for Id(0): 1 not at or in advance of current seal frontier Antichain { elements: [2] }")));
        assert_eq!(c1s2.seal(1).recv(), Err(Error::from("invalid seal for Id(1): 1 not at or in advance of current seal frontier Antichain { elements: [2] }")));

        // Cannot write to streams not specified during construction.
        let (c1s3, _) = client1.create_or_load::<(), ()>("3")?;
        assert!(multi
            .write_atomic(vec![(c1s3.stream_id(), data)])
            .recv()
            .is_err());

        // Cannot seal streams not specified during construction.
        assert!(multi.seal(&[c1s3.stream_id()], 3).recv().is_err());

        Ok(())
    }

    #[test]
    fn codec_mismatch() -> Result<(), Error> {
        let client = MemRegistry::new().runtime_no_reentrance()?;

        let _ = client.create_or_load::<(), String>("stream")?;

        // Normal case: registration uses same key and value codec.
        let _ = client.create_or_load::<(), String>("stream")?;

        // Different key codec
        assert_eq!(
            client.create_or_load::<Vec<u8>, String>("stream"),
            Err(Error::from(
                "invalid registration: key codec mismatch Vec<u8> vs previous ()"
            ))
        );

        // Different val codec
        assert_eq!(
            client.create_or_load::<(), Vec<u8>>("stream"),
            Err(Error::from(
                "invalid registration: val codec mismatch Vec<u8> vs previous String"
            ))
        );

        Ok(())
    }

    /// Previously, the persisted source would first register a listener, and
    /// then read a snapshot in two separate commands. This approach had the
    /// problem that there could be some data duplication between the snapshot
    /// and the listener. We attempted to solve that problem by filtering out all
    /// records <= the snapshot's sealed frontier from the listener, and allowed the
    /// listener to send us records > the snapshot's sealed frontier.
    ///
    /// Unfortunately, we also allowed the snapshot to send us records > the
    /// snapshot's sealed frontier so this didn't fix the data duplication issue.
    /// #8606 manifested as an issue with the persistent system tables test where
    /// some records had negative multiplicity. Basically, something like the
    /// following sequence of events happened:
    ///
    /// 1. Register a listener
    /// 2. Insert (foo, t1, +1)
    /// 3. Insert (foo, t100, -1)
    /// 4. Seal t2
    /// 5. Take a snapshot - which has sealed frontier t2
    /// 6. Start reading from the listener and snapshot.
    ///
    /// Now when we did step 6 - we received from the snapshot:
    ///
    /// (foo, t1, +1)
    /// (foo, t100, -1)
    ///
    /// because we didn't filter anything from the snapshot.
    ///
    /// From the listener, we received:
    ///
    /// (foo, t100, -1) because we filtered out all records at times < t2
    ///
    /// at t100, we now have a negative multiplicity.
    ///
    /// This test attempts to replicate that scenario by interleaving writes
    /// and seals with persisted source creation to catch any regressions where
    /// taking a snapshot and registering a listener are not properly atomic.
    #[test]
    fn regression_8606_snapshot_listener_atomicity() -> Result<(), Error> {
        let data = vec![(("foo".into(), ()), 1, 1), (("foo".into(), ()), 1000, -1)];

        let mut p = MemRegistry::new().runtime_no_reentrance()?;
        let handles = p.create_or_load::<String, ()>("1");
        let read = handles
            .as_ref()
            .map(|(_write, read)| read.clone())
            .map_err(|e| e.clone());
        let write = handles
            .as_ref()
            .map(|(write, _read)| write.clone())
            .unwrap();

        let ok = timely::execute_directly(move |worker| {
            let writes = std::thread::spawn(move || {
                write.write(&data).recv().expect("write was successful");
                write.seal(2).recv().expect("seal was successful");
            });

            let mut probe = ProbeHandle::new();
            let ok_stream = worker.dataflow(|scope| {
                let (ok_stream, _err_stream) =
                    scope.persisted_source(read).ok_err(|x| split_ok_err(x));
                ok_stream.probe_with(&mut probe).capture()
            });

            writes.join().expect("write thread succeeds");

            while probe.less_than(&2) {
                worker.step();
            }
            p.stop().expect("stop was successful");

            ok_stream
        });

        let diff_sum: isize = ok
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter().map(|(_, _, diff)| diff))
            .sum();
        assert_eq!(diff_sum, 0);

        Ok(())
    }
}
