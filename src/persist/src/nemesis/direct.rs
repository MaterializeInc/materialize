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
use std::sync::mpsc;
use std::sync::mpsc::Receiver;

use timely::communication::allocator::Thread;
use timely::dataflow::operators::capture::{Capture, Event as TimelyCaptureEvent};
use timely::dataflow::operators::probe::{Handle as TimelyProbe, Probe};
use timely::progress::Antichain;
use timely::worker::Worker;
use timely::WorkerConfig;

use crate::error::Error;
use crate::indexed::runtime::{
    self, DecodedSnapshot, MultiWriteHandle, RuntimeClient, StreamReadHandle, StreamWriteHandle,
};
use crate::indexed::{ListenEvent, SnapshotExt};
use crate::nemesis::{
    AllowCompactionReq, Input, ReadOutputReq, ReadOutputRes, ReadSnapshotReq, ReadSnapshotRes, Req,
    Res, Runtime, SealReq, SnapshotId, Step, TakeSnapshotReq, WriteReq, WriteReqMulti,
    WriteReqSingle, WriteRes,
};
use crate::operators::source::PersistedSource;
use crate::unreliable::UnreliableHandle;

// TODO: With the recent addition of dataflows, this is much less "direct" than
// it used to be. We should probably rename this to something like `Threads` (to
// leave room for a future one that runs timely with processes and can stop them
// without graceful shutdown) and reimplement Direct using Indexed.
pub struct Direct {
    start_fn: Box<dyn FnMut(UnreliableHandle) -> Result<RuntimeClient, Error>>,
    pub persister: RuntimeClient,
    worker: Worker<Thread>,
    unreliable: UnreliableHandle,
    streams: HashMap<
        String,
        (
            StreamWriteHandle<String, ()>,
            StreamReadHandle<String, ()>,
            TimelyProbe<u64>,
        ),
    >,
    output_by_stream_name:
        HashMap<String, Receiver<TimelyCaptureEvent<u64, ((String, ()), u64, isize)>>>,
    snapshots: HashMap<SnapshotId, DecodedSnapshot<String, ()>>,
}

impl Runtime for Direct {
    fn run(&mut self, i: Input) -> Step {
        let res = match i.req {
            Req::Write(WriteReq::Single(req)) => {
                Res::Write(WriteReq::Single(req.clone()), self.write_single(req))
            }
            Req::Write(WriteReq::Multi(req)) => {
                Res::Write(WriteReq::Multi(req.clone()), self.write_multi(req))
            }
            Req::ReadOutput(req) => Res::ReadOutput(req.clone(), self.read_output(req)),
            Req::Seal(req) => Res::Seal(req.clone(), self.seal(req)),
            Req::AllowCompaction(req) => {
                Res::AllowCompaction(req.clone(), self.allow_compaction(req))
            }
            Req::TakeSnapshot(req) => Res::TakeSnapshot(req.clone(), self.take_snapshot(req)),
            Req::ReadSnapshot(req) => Res::ReadSnapshot(req.clone(), self.read_snapshot(req)),
            Req::Start => Res::Start(self.start()),
            Req::Stop => Res::Stop(self.stop()),
            Req::StorageUnavailable => {
                self.unreliable.make_unavailable();
                Res::StorageUnavailable
            }
            Req::StorageAvailable => {
                self.unreliable.make_available();
                Res::StorageAvailable
            }
        };

        // Poke the dataflows a bit. We really only need the one in seal (and
        // stop) but it can't hurt and maybe we'll uncover something.
        self.worker.step();

        Step {
            req_id: i.req_id,
            res,
        }
    }

    fn finish(self) {}
}

impl Direct {
    pub fn new<F: FnMut(UnreliableHandle) -> Result<RuntimeClient, Error> + 'static>(
        mut start_fn: F,
    ) -> Result<Self, Error> {
        let unreliable = UnreliableHandle::default();
        let persister = start_fn(unreliable.clone())?;
        let worker = Worker::new(WorkerConfig::default(), Thread::new());
        Ok(Direct {
            start_fn: Box::new(start_fn),
            persister,
            worker,
            unreliable,
            streams: HashMap::new(),
            output_by_stream_name: HashMap::new(),
            snapshots: HashMap::new(),
        })
    }

    fn stream(
        &mut self,
        name: &str,
    ) -> Result<
        &mut (
            StreamWriteHandle<String, ()>,
            StreamReadHandle<String, ()>,
            TimelyProbe<u64>,
        ),
        Error,
    > {
        let (streams, persister, worker) =
            (&mut self.streams, &mut self.persister, &mut self.worker);
        match streams.entry(name.to_string()) {
            Entry::Occupied(x) => Ok(x.into_mut()),
            Entry::Vacant(x) => {
                let (write, read) = persister.create_or_load::<String, ()>(name)?;

                let (output_tx, output_rx) = mpsc::channel();
                let previous_output = self
                    .output_by_stream_name
                    .insert(name.to_string(), output_rx);
                // This is expected to have been cleared by start.
                debug_assert!(previous_output.is_none());

                let probe = worker.dataflow(|scope| {
                    let mut probe = TimelyProbe::new();
                    let (ok_stream, _err_stream) = scope.persisted_source(&read);
                    // TODO: Do something with err_stream.
                    ok_stream.probe_with(&mut probe).capture_into(output_tx);
                    probe
                });

                Ok(x.insert((write, read, probe)))
            }
        }
    }

    fn write_single(&mut self, req: WriteReqSingle) -> Result<WriteRes, Error> {
        let (write, _, _) = self.stream(&req.stream)?;
        let seqno = write.write(&[req.update]).recv()?.0;
        Ok(WriteRes { seqno })
    }

    fn write_multi(&mut self, req: WriteReqMulti) -> Result<WriteRes, Error> {
        let mut write_handles = Vec::new();
        let mut updates = Vec::new();
        for req in req.writes {
            let (write, _, _) = self.stream(&req.stream)?;
            updates.push((write.stream_id(), vec![req.update]));
            write_handles.push(write.clone());
        }
        let write_handles = write_handles.iter().collect::<Vec<_>>();
        let multi = MultiWriteHandle::new(&write_handles)?;

        let seqno = multi.write_atomic(updates).recv()?.0;
        Ok(WriteRes { seqno })
    }

    fn read_output(&mut self, req: ReadOutputReq) -> Result<ReadOutputRes, Error> {
        let mut contents = Vec::new();
        if let Some(output) = self.output_by_stream_name.get_mut(&req.stream) {
            for e in output.try_iter() {
                match e {
                    TimelyCaptureEvent::Progress(x) => {
                        // TODO: This isn't even a little bit right, but it
                        // happens to work.
                        for (ts, ts_diff) in x {
                            if ts_diff > 0 {
                                contents.push(ListenEvent::Sealed(ts));
                            }
                        }
                    }
                    TimelyCaptureEvent::Messages(_, x) => {
                        contents.push(ListenEvent::Records(x));
                    }
                }
            }
        };
        Ok(ReadOutputRes { contents })
    }

    fn seal(&mut self, req: SealReq) -> Result<(), Error> {
        let (write, _, probe) = self.stream(&req.stream)?;
        write.seal(req.ts).recv()?;

        // Force the dataflows to make progress, so we don't end up validating
        // the very uninteresting case of no output.
        let probe = probe.clone();
        self.worker.step_while(|| probe.less_than(&req.ts));

        Ok(())
    }

    fn allow_compaction(&mut self, req: AllowCompactionReq) -> Result<(), Error> {
        let (write, _, _) = self.stream(&req.stream)?;
        write.allow_compaction(Antichain::from_elem(req.ts)).recv()
    }

    fn take_snapshot(&mut self, req: TakeSnapshotReq) -> Result<(), Error> {
        let (_, read, _) = self.stream(&req.stream)?;
        let snap = read.snapshot()?;
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
        // The handles from the previous persister cannot be used after stop.
        self.streams.clear();

        // New dataflow means new output.
        self.output_by_stream_name.clear();
        self.worker = Worker::new(WorkerConfig::default(), Thread::new());

        let persister = (self.start_fn)(self.unreliable.clone())?;
        self.persister = persister;

        Ok(())
    }

    fn stop(&mut self) -> Result<(), Error> {
        let res = self.persister.stop();

        // Stopping the persister should allow the dataflows to finish.
        while self.worker.step() {}

        res
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
