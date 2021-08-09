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

use crate::error::Error;
use crate::indexed::runtime::{
    self, DecodedSnapshot, RuntimeClient, StreamReadHandle, StreamWriteHandle,
};
use crate::indexed::IndexedConfig;
use crate::nemesis::{
    AllowCompactionReq, Input, ReadSnapshotReq, ReadSnapshotRes, Req, Res, Runtime, SealReq,
    SnapshotId, Step, TakeSnapshotReq, WriteReq, WriteRes,
};
use crate::unreliable::UnreliableHandle;

pub struct Direct {
    start_fn: Box<dyn FnMut(UnreliableHandle) -> Result<RuntimeClient, Error>>,
    persister: RuntimeClient,
    unreliable: UnreliableHandle,
    streams: HashMap<String, (StreamWriteHandle<String, ()>, StreamReadHandle<String, ()>)>,
    snapshots: HashMap<SnapshotId, DecodedSnapshot<String, ()>>,
}

impl Runtime for Direct {
    fn run(&mut self, i: Input) -> Step {
        let res = match i.req {
            Req::Write(req) => Res::Write(req.clone(), self.write(req)),
            Req::Seal(req) => Res::Seal(req.clone(), self.seal(req)),
            Req::AllowCompaction(req) => {
                Res::AllowCompaction(req.clone(), self.allow_compaction(req))
            }
            Req::TakeSnapshot(req) => Res::TakeSnapshot(req.clone(), self.take_snapshot(req)),
            Req::ReadSnapshot(req) => Res::ReadSnapshot(req.clone(), self.read_snapshot(req)),
            Req::Restart => Res::Restart(self.restart()),
            Req::StorageUnavailable => {
                self.unreliable.make_unavailable();
                Res::StorageUnavailable
            }
            Req::StorageAvailable => {
                self.unreliable.make_available();
                Res::StorageAvailable
            }
        };
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
        Ok(Direct {
            start_fn: Box::new(start_fn),
            persister,
            unreliable,
            streams: HashMap::new(),
            snapshots: HashMap::new(),
        })
    }

    fn stream(
        &mut self,
        name: &str,
    ) -> Result<&mut (StreamWriteHandle<String, ()>, StreamReadHandle<String, ()>), Error> {
        let (streams, persister) = (&mut self.streams, &mut self.persister);
        match streams.entry(name.to_string()) {
            Entry::Occupied(x) => Ok(x.into_mut()),
            Entry::Vacant(x) => persister.create_or_load(name).map(|token| x.insert(token)),
        }
    }

    fn write(&mut self, req: WriteReq) -> Result<WriteRes, Error> {
        let (write, _) = self.stream(&req.stream)?;
        let seqno = write.write(&[req.update]).recv()?.0;
        Ok(WriteRes { seqno })
    }

    fn seal(&mut self, req: SealReq) -> Result<(), Error> {
        let (write, _) = self.stream(&req.stream)?;
        write.seal(req.ts).recv()
    }

    fn allow_compaction(&mut self, req: AllowCompactionReq) -> Result<(), Error> {
        let (_, meta) = self.stream(&req.stream)?;
        meta.allow_compaction(req.ts).recv()
    }

    fn take_snapshot(&mut self, req: TakeSnapshotReq) -> Result<(), Error> {
        let (_, meta) = self.stream(&req.stream)?;
        let snap = meta.snapshot()?;
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
        let mut snap = match self.snapshots.remove(&req.snap) {
            Some(snap) => snap,
            None => return Err(format!("unknown snap: {:?}", req.snap).into()),
        };
        let contents = snap.read_to_end_flattened()?;
        Ok(ReadSnapshotRes {
            seqno: snap.seqno().0,
            contents,
        })
    }

    fn restart(&mut self) -> Result<(), Error> {
        let stop_res = self.persister.stop();
        // The handles from the previous persister cannot be used after stop.
        self.streams.clear();
        let persister = (self.start_fn)(self.unreliable.clone())?;
        self.persister = persister;
        stop_res
    }
}

#[cfg(test)]
mod tests {
    use crate::file::{FileBlob, FileBuffer};
    use crate::mem::MemRegistry;
    use crate::nemesis;
    use crate::nemesis::generator::GeneratorConfig;
    use crate::unreliable::{UnreliableBlob, UnreliableBuffer};

    use super::*;

    #[test]
    fn direct_mem() {
        let mut registry = MemRegistry::new();
        let direct = Direct::new(move |unreliable| {
            registry.open_unreliable("direct_mem", "direct_mem", unreliable)
        })
        .expect("initial start failed");
        nemesis::run(100, GeneratorConfig::default(), direct)
    }

    #[test]
    fn direct_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir creation failed");
        let direct = Direct::new(move |unreliable| {
            let (buf_dir, blob_dir) = (temp_dir.path().join("buf"), temp_dir.path().join("blob"));
            let buf = FileBuffer::new(buf_dir, ("reentrance0", "direct_file").into())?;
            let buf = UnreliableBuffer::from_handle(buf, unreliable.clone());
            let blob = FileBlob::new(blob_dir, ("reentrance0", "direct_file").into())?;
            let blob = UnreliableBlob::from_handle(blob, unreliable);
            // TODO: use a more realistic configuration that allows for some delay
            // between when writes are submitted and moved to blob storage.
            runtime::start(buf, blob, IndexedConfig::default())
        })
        .expect("initial start failed");
        // TODO: At the moment, running this for 100 steps takes a bit over a
        // second, so run this one for fewer steps than the other tests. Revisit
        // once we pipeline write calls in Buffer.
        nemesis::run(10, GeneratorConfig::default(), direct);
    }

    // A variant with a traffic pattern vaguely like production usage of
    // Materialize.
    #[test]
    fn direct_mzlike() {
        let config = GeneratorConfig {
            // Writes are likely to outnumber other operations.
            write_unsealed_weight: 20,
            // Writes to sealed timestamps are errors that we don't expect in
            // production usage.
            write_sealed_weight: 0,
            ..Default::default()
        };
        let mut registry = MemRegistry::new();
        let direct = Direct::new(move |unreliable| {
            registry.open_unreliable("direct_mzlike", "direct_mzlike", unreliable)
        })
        .expect("initial start failed");
        nemesis::run(100, config, direct)
    }
}
