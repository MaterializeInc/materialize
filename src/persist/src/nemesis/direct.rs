// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;
use std::{fmt, thread};

use timely::communication::WorkerGuards;
use timely::dataflow::operators::capture::{Capture, Event as TimelyCaptureEvent};
use timely::dataflow::operators::Probe;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;

use crate::error::Error;
use crate::indexed::runtime::{
    self, DecodedSnapshot, MultiWriteHandle, RuntimeClient, StreamReadHandle, StreamWriteHandle,
};
use crate::indexed::SnapshotExt;
use crate::nemesis::progress::{DataflowProgress, DataflowProgressHandle};
use crate::nemesis::{
    AllowCompactionReq, FutureRes, FutureStep, Input, ReadOutputEvent, ReadOutputReq,
    ReadOutputRes, ReadSnapshotReq, ReadSnapshotRes, Req, Res, Runtime, SealReq, SnapshotId,
    TakeSnapshotReq, WriteReq, WriteReqMulti, WriteReqSingle,
};
use crate::operators::source::PersistedSource;
use crate::pfuture::PFuture;
use crate::storage::SeqNo;
use crate::unreliable::UnreliableHandle;

#[derive(Debug)]
struct Dataflow {
    write: StreamWriteHandle<String, ()>,
    read: StreamReadHandle<String, ()>,
    output: Receiver<TimelyCaptureEvent<u64, (Result<(String, ()), String>, u64, isize)>>,
    workers: TimelyWorkers,
    progress: DataflowProgress,
    progress_handle: DataflowProgressHandle,
}

// TODO: With the recent addition of dataflows, this is much less "direct" than
// it used to be. We should probably rename this to something like `Threads` (to
// leave room for a future one that runs timely with processes and can stop them
// without graceful shutdown) and reimplement Direct using Indexed.
#[derive(Debug)]
pub struct Direct {
    start_fn: StartFn,
    pub persister: RuntimeClient,
    unreliable: UnreliableHandle,
    dataflows: HashMap<String, Dataflow>,
    snapshots: HashMap<SnapshotId, DecodedSnapshot<String, ()>>,
}

impl Runtime for Direct {
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
                let res = Res::ReadOutput(req.clone(), self.read_output(req));
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
                let res = Res::ReadSnapshot(req.clone(), self.read_snapshot(req));
                FutureRes::Ready(res)
            }
            Req::Start => {
                let res = Res::Start(self.start());
                FutureRes::Ready(res)
            }
            Req::Stop => {
                let res = Res::Stop(self.stop());
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

    fn finish(mut self) {
        let _ = self.stop();
    }
}

impl Direct {
    const DATAFLOW_WORKERS: usize = 2;

    pub fn new<F: FnMut(UnreliableHandle) -> Result<RuntimeClient, Error> + 'static>(
        mut start_fn: F,
    ) -> Result<Self, Error> {
        let unreliable = UnreliableHandle::default();
        let persister = start_fn(unreliable.clone())?;
        Ok(Direct {
            start_fn: StartFn(Box::new(start_fn)),
            persister,
            unreliable,
            dataflows: HashMap::new(),
            snapshots: HashMap::new(),
        })
    }

    fn stream(&mut self, name: &str) -> Result<&mut Dataflow, Error> {
        match self.dataflows.entry(name.to_owned()) {
            Entry::Occupied(x) => Ok(x.into_mut()),
            Entry::Vacant(x) => {
                let (write, read) = self.persister.create_or_load(name)?;
                let dataflow_read = read.clone();
                let (output_tx, output_rx) = mpsc::channel();
                let output_tx = Arc::new(Mutex::new(output_tx));

                let (progress_tx, progress_rx) = DataflowProgress::new();
                let progress_handle = progress_tx.clone();
                let workers = timely::execute(
                    timely::Config::process(Self::DATAFLOW_WORKERS),
                    move |worker| {
                        let dataflow_read = dataflow_read.clone();
                        let mut probe = ProbeHandle::new();
                        worker.dataflow(|scope| {
                            let output_tx = output_tx
                                .lock()
                                .expect("clone doesn't panic and poison lock")
                                .clone();
                            let data = scope.persisted_source(Ok(dataflow_read));
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
                let dataflow = Dataflow {
                    write: write,
                    read: read,
                    output: output_rx,
                    progress: progress_rx,
                    progress_handle,
                    workers: TimelyWorkers(workers),
                };
                Ok(x.insert(dataflow))
            }
        }
    }

    fn write_single(&mut self, req: WriteReqSingle) -> Result<PFuture<SeqNo>, Error> {
        let stream = self.stream(&req.stream)?;
        Ok(stream.write.write(&[req.update]))
    }

    fn write_multi(&mut self, req: WriteReqMulti) -> Result<PFuture<SeqNo>, Error> {
        let mut write_handles = Vec::new();
        let mut updates = Vec::new();
        for req in req.writes {
            let stream = self.stream(&req.stream)?;
            updates.push((stream.write.stream_id(), vec![req.update]));
            write_handles.push(stream.write.clone());
        }
        let write_handles = write_handles.iter().collect::<Vec<_>>();
        let multi = MultiWriteHandle::new(&write_handles)?;

        Ok(multi.write_atomic(updates))
    }

    fn read_output(&mut self, req: ReadOutputReq) -> Result<ReadOutputRes, Error> {
        // TODO: This should probably be run async in a thread.
        let mut contents = Vec::new();
        if let Some(dataflow) = self.dataflows.get_mut(&req.stream) {
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

    fn seal(&mut self, req: SealReq) -> Result<PFuture<()>, Error> {
        let stream = self.stream(&req.stream)?;
        let seal_ts = req.ts;
        let seal_res = stream.write.seal(seal_ts);
        let progress = stream.progress.clone();
        let (tx, rx) = PFuture::new();
        let _ = thread::Builder::new()
            .name("nemesis-seal".into())
            .spawn(move || {
                tx.fill(|| -> Result<(), Error> {
                    // Wait for the seal to succeed or fail. Then, only if it
                    // succeeded, also block until the output corresponding to this
                    // stream catches up to the sealed timestamp. Otherwise, we
                    // might end up testing the uninteresting but correct case of no
                    // output.
                    let _ = seal_res.recv()?;
                    let output_progress = progress.wait(seal_ts);
                    // NB: super subtle, return the original Ok() even if the
                    // dataflow shuts down before we see it in the output.
                    let _ = output_progress.recv();
                    Ok(())
                }())
            })
            .expect("thread name is valid");
        Ok(rx)
    }

    fn allow_compaction(&mut self, req: AllowCompactionReq) -> Result<PFuture<()>, Error> {
        let stream = self.stream(&req.stream)?;
        Ok(stream.write.allow_compaction(Antichain::from_elem(req.ts)))
    }

    fn take_snapshot(&mut self, req: TakeSnapshotReq) -> Result<(), Error> {
        let stream = self.stream(&req.stream)?;
        let snap = stream.read.snapshot()?;
        match self.snapshots.entry(req.snap) {
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
        Ok(())
    }

    fn read_snapshot(&mut self, req: ReadSnapshotReq) -> Result<ReadSnapshotRes, Error> {
        let snap = match self.snapshots.remove(&req.snap) {
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

    fn start(&mut self) -> Result<(), Error> {
        let _ = self.stop()?;
        // The self.stop call clears dataflows but do it again defensively.
        self.dataflows.clear();

        let persister = (self.start_fn.0)(self.unreliable.clone())?;
        self.persister = persister;

        Ok(())
    }

    fn stop(&mut self) -> Result<(), Error> {
        let res = self.persister.stop();

        // Stopping the persister allows the dataflows to finish.
        for (_, dataflow) in self.dataflows.drain() {
            dataflow.progress_handle.close();
            dataflow.workers.join();
        }
        res
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
    use ore::metrics::MetricsRegistry;

    use crate::file::{FileBlob, FileLog};
    use crate::indexed::runtime::RuntimeConfig;
    use crate::mem::MemRegistry;
    use crate::nemesis;
    use crate::nemesis::generator::GeneratorConfig;
    use crate::unreliable::{UnreliableBlob, UnreliableLog};

    use super::*;

    #[test]
    fn direct_mem() {
        let mut registry = MemRegistry::new();
        let direct = Direct::new(move |unreliable| registry.runtime_unreliable(unreliable))
            .expect("initial start failed");
        nemesis::run(100, GeneratorConfig::default(), direct)
    }

    #[test]
    fn direct_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir creation failed");
        let direct = Direct::new(move |unreliable| {
            let (log_dir, blob_dir) = (temp_dir.path().join("log"), temp_dir.path().join("blob"));
            let log = FileLog::new(log_dir, ("reentrance0", "direct_file").into())?;
            let log = UnreliableLog::from_handle(log, unreliable.clone());
            let blob = FileBlob::new(blob_dir, ("reentrance0", "direct_file").into())?;
            let blob = UnreliableBlob::from_handle(blob, unreliable);
            runtime::start(
                RuntimeConfig::for_tests(),
                log,
                blob,
                &MetricsRegistry::new(),
                None,
            )
        })
        .expect("initial start failed");
        // TODO: At the moment, running this for 100 steps takes a bit over a
        // second, so run this one for fewer steps than the other tests. Revisit
        // once we pipeline write calls in Log.
        nemesis::run(10, GeneratorConfig::default(), direct);
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
        let mut registry = MemRegistry::new();
        let direct = Direct::new(move |unreliable| registry.runtime_unreliable(unreliable))
            .expect("initial start failed");
        nemesis::run(100, config, direct)
    }
}
