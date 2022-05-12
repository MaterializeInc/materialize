// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The primary implementation of [Runtime].
//!
//! As mentioned elsewhere, nemesis's goal is to exercise persist's public API,
//! record the responses/externally observable behaviors, and post-hoc verify
//! that none of persist's external invariants were violated. [Generator] and an
//! implementation of [Runtime] (such as [Direct] here) are together responsible
//! for exercising the public API in "interesting" ways and [Validator] is
//! responsible for validating the history.
//!
//! For nemesis's purposes, "interesting" traffic against the persist API means
//! that it is possible (better yet, likely) to reproduce any persist bug that
//! we might hit in production. To explain our attempt at doing this, we first
//! need a bit of background.
//!
//! Persist's place in a platform world is something like the following:
//!
//!     ingestd -> persist -> dataflowd
//!
//! Where "ingestd" represents a process that roughly corresponds to the sources
//! part of what is currently src/dataflow (tables are similar enough that we
//! ignore them here) and "dataflowd" represents a set of HA processes that
//! incrementally compute things from those sources of data. Persist's main
//! responsibilities are to make source data (1) durable and (2) [definite] as
//! well as (3) providing a separation between compute and storage such that
//! they can scale independently. These names are what we're using at the time
//! this was written, but it's the early days platform, so this all could very
//! well rot.
//!
//! [definite]: https://github.com/MaterializeInc/materialize/blob/v0.9.11/doc/developer/design/20210831_correctness.md
//!
//! The nemesis testing then internally structures its API usage in the same
//! way. A set of persisted [collection]s are written to from an ingestd process
//! and read from one (or many) dataflowd processes. The only communication
//! between these two is via persist.
//!
//! [collection]: https://docs.rs/differential-dataflow/0.12.0/differential_dataflow/collection/struct.Collection.html
//!
//! The external API guarantees of persist are in terms of its
//! [StreamReadHandle]s and [StreamWriteHandle]s. Also included is the
//! [PersistedSource] operator which wraps a StreamReadHandle to give native
//! differential dataflow semantics.
//!
//! The ingestd side uses the read and write handles (technically it currently
//! uses some extra dataflow operators that wrap them, but ingestd will
//! eventually get off dataflow so these are going away at some point). We model
//! this as a set of [RuntimeWorker]s (here [DataflowWorker] is the impl of
//! that), each concurrently issuing calls to read and write handles and
//! recording the responses.
//!
//! The dataflowd side uses the PersistedSource operator. We model this with a
//! multi-[timely::worker::Worker] dataflow per persisted collection which
//! simply captures the output for nemesis validation. These workers are driven
//! using exactly the [timely::execute] runtime used in production. Additionally
//! a small bit of [DataflowProgress] tracking is added, which is used by the
//! ingestd-side to keep traffic more "interesting" by situationally waiting
//! until the dataflow-side has caught up.
//!
//! Not unexpectedly, concurrency was a source of many of persist's initial
//! bugs, so we make a particular effort to ensure that the production
//! concurrency is modeled. One tricky thing is that the real ingestd and
//! dataflowd tie the lifetime of the persist runtime to the lifetime of the
//! process. However, we want nemesis to model restarts (another likely source
//! of bugs), which is in tension with maximal concurrency (because persist
//! requires an exclusive-writer lock, which serializes runtime start/stop
//! operations). As a compromise, we model the source of truth in [DirectCore]
//! behind a Mutex, and then each ingestd-side [DataflowWorker] caches the read
//! and write handles for each collection the first time they are used after a
//! persist runtime restart. These caches are invalidated using lock-free
//! atomics to avoid introducing a barrier between the ingestd workers.
//! [DirectShared] sits in between these two layers and is mostly a helper to
//! deal with cache invalidation.
//!
//! [DirectShared] is also a convenient place to house nemesis's odd split of
//! getting a snapshot and reading it. We do this split to model a very large
//! snapshot that takes a long time to consume, so other reads and writes
//! continue on concurrently in the background.
//!
//! A missing piece in all this is that the ingestd and dataflowd processes (and
//! thus their respective persist runtimes) can be restarted independently.
//! Sadly, support for this is coming to persist, but it's not here quite yet.
//! In the interim, nemesis has a single persist runtime shared between them.
//! Once this is built, we'll split [DirectCore] into one for ingestd and one
//! for dataflowd and allow them to be restarted independently.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;
use std::{fmt, thread};

use timely::communication::WorkerGuards;
use timely::dataflow::operators::capture::{Capture, Event as TimelyCaptureEvent};
use timely::dataflow::operators::Probe;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;

use crate::client::{
    DecodedSnapshot, MultiWriteHandle, RuntimeClient, StreamReadHandle, StreamWriteHandle,
};
use crate::error::Error;
use crate::location::SeqNo;
use crate::nemesis::progress::{DataflowProgress, DataflowProgressHandle};
use crate::nemesis::{
    AllowCompactionReq, FutureRes, FutureStep, Input, ReadOutputEvent, ReadOutputReq,
    ReadOutputRes, ReadSnapshotReq, ReadSnapshotRes, Req, Res, Runtime, RuntimeWorker, SealReq,
    SnapshotId, TakeSnapshotReq, WriteReq, WriteReqMulti, WriteReqSingle,
    NUM_DATAFLOW_WORKER_THREADS,
};
use crate::operators::source::PersistedSource;
use crate::pfuture::PFuture;
use crate::unreliable::UnreliableHandle;

/// A handle to the "ingestd" (input) side of a persisted collection.
#[derive(Debug, Clone)]
struct Ingest {
    write: StreamWriteHandle<String, ()>,
    read: StreamReadHandle<String, ()>,
    progress_rx: DataflowProgress,
}

/// A handle to the "dataflowd" (output) side of a persisted collection.
#[derive(Debug)]
struct Dataflow {
    output: Receiver<TimelyCaptureEvent<u64, (Result<(String, ()), String>, u64, i64)>>,
    workers: TimelyWorkers,
    progress_tx: DataflowProgressHandle,
}

#[derive(Debug)]
struct DirectCore {
    start_fn: Box<dyn StartRuntime>,
    unreliable: UnreliableHandle,
    runtime: RuntimeClient,
    streams: HashMap<String, (Ingest, Dataflow)>,
}

impl DirectCore {
    // Selected to be a small prime number.
    const OUTPUTS_PER_YIELD: usize = 3;

    fn stream(&mut self, name: &str) -> Result<Ingest, Error> {
        match self.streams.entry(name.to_owned()) {
            Entry::Occupied(x) => {
                let (ingest, _) = x.get();
                Ok(ingest.clone())
            }
            Entry::Vacant(x) => {
                let (write, read) = self.runtime.create_or_load(name);
                // Error if the create_or_load wasn't successful.
                let _ = write.stream_id()?;
                let dataflow_read = read.clone();
                let (output_tx, output_rx) = mpsc::channel();
                let output_tx = Arc::new(Mutex::new(output_tx));

                let (progress_tx, progress_rx) = DataflowProgress::new();
                let progress_handle = progress_tx.clone();
                let workers = timely::execute(
                    timely::Config::process(NUM_DATAFLOW_WORKER_THREADS),
                    move |worker| {
                        let dataflow_read = dataflow_read.clone();
                        let mut probe = ProbeHandle::new();
                        worker.dataflow(|scope| {
                            let output_tx = output_tx
                                .lock()
                                .expect("clone doesn't panic and poison lock")
                                .clone();
                            let data = scope.persisted_source_yield(
                                dataflow_read,
                                &Antichain::from_elem(0),
                                Self::OUTPUTS_PER_YIELD,
                            );
                            data.probe_with(&mut probe).capture_into(output_tx);
                        });
                        while worker.step_or_park(None) {
                            probe.with_frontier(|frontier| {
                                progress_tx.maybe_progress(frontier);
                            })
                        }
                        progress_tx.close();
                    },
                )?;
                let input = Ingest {
                    write,
                    read,
                    progress_rx,
                };
                let output = Dataflow {
                    workers: TimelyWorkers(workers),
                    output: output_rx,
                    progress_tx: progress_handle,
                };
                x.insert((input.clone(), output));
                Ok(input)
            }
        }
    }

    fn start(&mut self) -> Result<(), Error> {
        let _ = self.stop()?;
        // The self.stop call clears dataflows but do it again defensively.
        self.streams.clear();

        let runtime = self.start_fn.start_runtime(self.unreliable.clone())?;
        self.runtime = runtime;

        Ok(())
    }

    fn stop(&mut self) -> Result<(), Error> {
        let res = self.runtime.stop();

        // Stopping the persister allows the compute dataflows to finish.
        for (_, (_, dataflow)) in self.streams.drain() {
            dataflow.progress_tx.close();
            dataflow.workers.join();
        }
        res
    }
}

#[derive(Debug)]
struct DirectShared {
    unreliable: UnreliableHandle,
    snapshots: Arc<Mutex<HashMap<SnapshotId, DecodedSnapshot<String, ()>>>>,

    core: Mutex<DirectCore>,
    /// A count incremented each time persist has been restarted.
    ///
    /// This is used for lock-free invalidation of the cached copy of the
    /// persist handles that DirectWorker keeps.
    //
    // TODO: Once we split out a read-only version of persist, we can restart
    // the ingestd/dataflowd halves independently.
    generation: AtomicUsize,
}

impl DirectShared {
    fn new(mut start_fn: Box<dyn StartRuntime>) -> Result<Self, Error> {
        let unreliable = UnreliableHandle::default();
        let runtime = start_fn.start_runtime(unreliable.clone())?;
        let core = DirectCore {
            start_fn,
            unreliable: unreliable.clone(),
            runtime,
            streams: HashMap::new(),
        };
        let initial_generation = 0;
        let shared = DirectShared {
            core: Mutex::new(core),
            generation: AtomicUsize::new(initial_generation),
            unreliable,
            snapshots: Arc::new(Mutex::new(HashMap::new())),
        };
        Ok(shared)
    }

    fn generation(&self) -> usize {
        self.generation.load(Ordering::SeqCst)
    }

    fn read_output(&self, req: ReadOutputReq) -> Result<ReadOutputRes, Error> {
        // TODO: This should probably be run async in a thread.
        let mut contents = Vec::new();
        if let Some((_, dataflow)) = self.core.lock()?.streams.get_mut(&req.stream) {
            for e in dataflow.output.try_iter() {
                match e {
                    TimelyCaptureEvent::Progress(x) => {
                        // TODO: This isn't even a little bit right, but it
                        // happens to work.
                        for (ts, ts_diff) in x {
                            if ts_diff > 0 {
                                contents.push(ReadOutputEvent::Sealed(ts));
                            }
                        }
                    }
                    TimelyCaptureEvent::Messages(_, x) => {
                        contents.push(ReadOutputEvent::Records(x));
                    }
                }
            }
        };
        Ok(ReadOutputRes { contents })
    }

    fn save_snapshot(
        &self,
        snap_id: SnapshotId,
        snap: DecodedSnapshot<String, ()>,
    ) -> Result<SeqNo, Error> {
        let seqno = snap.seqno();
        match self.snapshots.lock()?.entry(snap_id) {
            Entry::Occupied(x) => {
                return Err(format!(
                    "internal nemesis error: duplicate snapshot id {:?}",
                    x.key()
                )
                .into())
            }
            Entry::Vacant(x) => {
                x.insert(snap);
            }
        }
        Ok(seqno)
    }

    fn read_snapshot(&self, req: ReadSnapshotReq) -> Result<ReadSnapshotRes, Error> {
        let snap = match self.snapshots.lock()?.remove(&req.snap) {
            Some(snap) => snap,
            None => return Err(format!("unknown snap: {:?}", req.snap).into()),
        };
        let (seqno, since) = (snap.seqno().0, snap.since());
        let contents = snap.read_to_end()?;
        Ok(ReadSnapshotRes {
            seqno,
            since,
            contents,
        })
    }

    pub fn start(&self) -> Result<(), Error> {
        let res = self.core.lock()?.start();
        let _ = self.generation.fetch_add(1, Ordering::SeqCst);
        res
    }

    pub fn stop(&self) -> Result<(), Error> {
        self.core.lock()?.stop()
    }
}

// TODO: With the recent addition of dataflows, this is much less "direct" than
// it used to be. We should probably rename this to something like `Threads` (to
// leave room for a future one that runs timely with processes and can stop them
// without graceful shutdown) and reimplement Direct using Indexed.
#[derive(Debug)]
pub struct Direct {
    workers: usize,
    shared: Arc<DirectShared>,
}

impl Runtime for Direct {
    type Worker = DirectWorker;

    fn add_worker(&mut self) -> DirectWorker {
        self.workers += 1;
        DirectWorker::new(Arc::clone(&self.shared))
    }

    fn finish(self) {
        let _ = self.shared.stop();
    }
}

pub trait StartRuntime: fmt::Debug + Send + 'static {
    fn start_runtime(&mut self, unreliable: UnreliableHandle) -> Result<RuntimeClient, Error>;
}

impl Direct {
    pub fn new<F: StartRuntime>(start_fn: F) -> Result<Self, Error> {
        let shared = DirectShared::new(Box::new(start_fn))?;
        Ok(Direct {
            workers: 0,
            shared: Arc::new(shared),
        })
    }

    pub fn runtime(&self) -> Result<RuntimeClient, Error> {
        Ok(self.shared.core.lock()?.runtime.clone())
    }
}

#[derive(Debug)]
pub struct DirectWorker {
    shared: Arc<DirectShared>,
    unreliable: UnreliableHandle,

    // The generation of the persist runtime these cached handles are from. Used
    // for lock-free invalidation of the cached copy of the persist handles that
    // DirectWorker keeps. When this doesn't match `shared.generation()`, we're
    // guaranteed that the contents of `self.handles` are from a persist runtime
    // that is shut down, so to keep things interesting we need to get new ones.
    // This protocol is race-y with the start of a new runtime (it's possible to
    // keep using old handles for a bit after the new runtime starts), but this
    // is best-effort and so that's fine.
    //
    // NB: The motivation for avoiding the DirectCore Mutex in DirectShared is
    // not performance, but rather avoiding the barrier of the lock in pursuit
    // of maximal concurrency in nemesis testing.
    handles_generation: usize,
    handles: HashMap<String, Ingest>,
}

impl RuntimeWorker for DirectWorker {
    fn run(&mut self, i: Input) -> FutureStep {
        let before = Instant::now();
        let res = match i.req {
            Req::Write(WriteReq::Single(req)) => {
                FutureRes::Write(WriteReq::Single(req.clone()), self.write_single(req))
            }
            Req::Write(WriteReq::Multi(req)) => {
                FutureRes::Write(WriteReq::Multi(req.clone()), self.write_multi(req))
            }
            Req::ReadOutput(req) => {
                let res = Res::ReadOutput(req.clone(), self.shared.read_output(req));
                FutureRes::Ready(res)
            }
            Req::Seal(req) => FutureRes::Seal(req.clone(), self.seal(req)),
            Req::AllowCompaction(req) => {
                FutureRes::AllowCompaction(req.clone(), self.allow_compaction(req))
            }
            Req::TakeSnapshot(req) => {
                let res = Res::TakeSnapshot(req.clone(), self.take_snapshot(req));
                FutureRes::Ready(res)
            }
            Req::ReadSnapshot(req) => {
                let res = Res::ReadSnapshot(req.clone(), self.shared.read_snapshot(req));
                FutureRes::Ready(res)
            }
            Req::Start => {
                let res = Res::Start(self.shared.start());
                FutureRes::Ready(res)
            }
            Req::Stop => {
                let res = Res::Stop(self.shared.stop());
                FutureRes::Ready(res)
            }
            Req::StorageUnavailable => {
                self.unreliable.make_unavailable();
                FutureRes::Ready(Res::StorageUnavailable)
            }
            Req::StorageAvailable => {
                self.unreliable.make_available();
                FutureRes::Ready(Res::StorageAvailable)
            }
        };

        FutureStep {
            req_id: i.req_id,
            before,
            res,
        }
    }
}

impl DirectWorker {
    fn new(shared: Arc<DirectShared>) -> Self {
        let unreliable = shared.unreliable.clone();
        DirectWorker {
            shared,
            unreliable,
            handles_generation: 0,
            handles: HashMap::new(),
        }
    }

    fn stream(&mut self, name: &str) -> Result<Ingest, Error> {
        let shared_generation = self.shared.generation();
        if self.handles_generation != shared_generation {
            self.handles.clear();
            self.handles_generation = shared_generation;
        }
        if let Some(d) = self.handles.get(name) {
            return Ok(d.clone());
        }
        let handle = self.shared.core.lock()?.stream(name)?;
        self.handles.insert(name.to_owned(), handle.clone());
        Ok(handle)
    }

    fn write_single(&mut self, req: WriteReqSingle) -> Result<PFuture<SeqNo>, Error> {
        let stream = self.stream(&req.stream)?;
        Ok(stream.write.write(&[req.update]))
    }

    fn write_multi(&mut self, req: WriteReqMulti) -> Result<PFuture<SeqNo>, Error> {
        let mut updates = Vec::new();
        for req in req.writes {
            let stream = self.stream(&req.stream)?;
            updates.push((stream.write.clone(), vec![req.update]));
        }
        let multi = MultiWriteHandle::new_from_streams(updates.iter().map(|(x, _)| x))?;

        Ok(multi.write_atomic(|builder| {
            for (handle, updates) in updates {
                builder.add_write(&handle, updates.iter())?;
            }
            Ok(())
        }))
    }

    fn seal(&mut self, req: SealReq) -> Result<PFuture<SeqNo>, Error> {
        let stream = self.stream(&req.stream)?;
        let seal_ts = req.ts;
        let seal_res = stream.write.seal(seal_ts);
        let progress = stream.progress_rx;
        let (tx, rx) = PFuture::new();
        let _ = thread::Builder::new()
            .name("nemesis:seal".into())
            .spawn(move || {
                tx.fill(|| -> Result<SeqNo, Error> {
                    // Wait for the seal to succeed or fail. Then, only if it
                    // succeeded, also block until the output corresponding to this
                    // stream catches up to the sealed timestamp. Otherwise, we
                    // might end up testing the uninteresting but correct case of no
                    // output.
                    let seqno = seal_res.recv()?;
                    let output_progress = progress.wait(seal_ts);
                    // NB: super subtle, return the original Ok() even if the
                    // dataflow shuts down before we see it in the output.
                    let _ = output_progress.recv();
                    Ok(seqno)
                }())
            })
            .expect("thread name is valid");
        Ok(rx)
    }

    fn allow_compaction(&mut self, req: AllowCompactionReq) -> Result<PFuture<SeqNo>, Error> {
        let stream = self.stream(&req.stream)?;
        Ok(stream.write.allow_compaction(Antichain::from_elem(req.ts)))
    }

    fn take_snapshot(&mut self, req: TakeSnapshotReq) -> Result<SeqNo, Error> {
        let stream = self.stream(&req.stream)?;
        let snap = stream.read.snapshot()?;
        // TODO: This should probably be run async in a thread.
        self.shared.save_snapshot(req.snap, snap)
    }
}

struct StartFn(Box<dyn FnMut(UnreliableHandle) -> Result<RuntimeClient, Error>>);

impl fmt::Debug for StartFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StartFn").finish_non_exhaustive()
    }
}

struct TimelyWorkers(WorkerGuards<()>);

impl fmt::Debug for TimelyWorkers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimelyWorkers").finish_non_exhaustive()
    }
}

impl TimelyWorkers {
    fn join(self) -> Vec<Result<(), String>> {
        self.0.join()
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::metrics::MetricsRegistry;
    use tempfile::TempDir;
    use tokio::runtime::Runtime as AsyncRuntime;

    use crate::error::ErrorLog;
    use crate::file::FileBlob;
    use crate::location::Blob;
    use crate::mem::MemRegistry;
    use crate::nemesis::generator::GeneratorConfig;
    use crate::runtime::RuntimeConfig;
    use crate::s3::{S3Blob, S3BlobConfig};
    use crate::unreliable::{UnreliableBlob, UnreliableLog};
    use crate::{nemesis, runtime};

    use super::*;

    impl StartRuntime for MemRegistry {
        fn start_runtime(&mut self, unreliable: UnreliableHandle) -> Result<RuntimeClient, Error> {
            self.runtime_unreliable(unreliable)
        }
    }

    #[test]
    fn direct_mem() {
        let registry = MemRegistry::new();
        let direct = Direct::new(registry).expect("initial start failed");
        nemesis::run(100, GeneratorConfig::default(), direct)
    }

    impl StartRuntime for TempDir {
        fn start_runtime(&mut self, unreliable: UnreliableHandle) -> Result<RuntimeClient, Error> {
            let blob_dir = self.path().join("blob");
            let log = ErrorLog;
            let log = UnreliableLog::from_handle(log, unreliable.clone());
            let blob =
                FileBlob::open_exclusive(blob_dir.into(), ("reentrance0", "direct_file").into())?;
            let blob = UnreliableBlob::from_handle(blob, unreliable);
            runtime::start(
                RuntimeConfig::for_tests(),
                log,
                blob,
                mz_build_info::DUMMY_BUILD_INFO,
                &MetricsRegistry::new(),
                None,
            )
        }
    }

    #[test]
    fn direct_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir creation failed");
        let direct = Direct::new(temp_dir).expect("initial start failed");
        // TODO: At the moment, running this for 100 steps takes a bit over a
        // second, so run this one for fewer steps than the other tests. Revisit
        // once we pipeline write calls in Log.
        nemesis::run(10, GeneratorConfig::default(), direct);
    }

    impl StartRuntime for (Arc<AsyncRuntime>, S3BlobConfig) {
        fn start_runtime(&mut self, unreliable: UnreliableHandle) -> Result<RuntimeClient, Error> {
            let (runtime, config) = self;
            let (runtime, config) = (Arc::clone(runtime), config.clone());
            let guard = runtime.enter();
            let blob = S3Blob::open_exclusive(config, ("reentrance0", "direct_s3").into())?;
            drop(guard);
            let blob = UnreliableBlob::from_handle(blob, unreliable);
            runtime::start(
                RuntimeConfig::for_tests(),
                ErrorLog,
                blob,
                mz_build_info::DUMMY_BUILD_INFO,
                &MetricsRegistry::new(),
                Some(runtime),
            )
        }
    }

    #[test]
    fn direct_s3() {
        let runtime = Arc::new(AsyncRuntime::new().expect("failed to create async runtime"));
        let config = runtime
            .block_on(S3BlobConfig::new_for_test())
            .expect("loading s3 config failed");
        if let Some(config) = config {
            let direct = Direct::new((runtime, config)).expect("initial start failed");
            nemesis::run(10, GeneratorConfig::default(), direct);
        }
    }

    // A variant with a traffic pattern vaguely like production usage of
    // Materialize.
    #[test]
    fn direct_mzlike() {
        let config = GeneratorConfig {
            // Writes are likely to outnumber other operations.
            write_unsealed_weight: 10,
            write_multi_weight: 10,
            // Writes to sealed timestamps are errors that we don't expect in
            // production usage.
            write_sealed_weight: 0,
            ..Default::default()
        };
        let registry = MemRegistry::new();
        let direct = Direct::new(registry).expect("initial start failed");
        nemesis::run(100, config, direct)
    }
}
