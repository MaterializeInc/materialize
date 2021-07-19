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

use crate::error::Error;
use crate::indexed::runtime::{self, RuntimeClient, StreamReadHandle, StreamWriteHandle};
use crate::indexed::{IndexedSnapshot, Snapshot};
use crate::nemesis::{
    Input, ReadSnapshotReq, ReadSnapshotRes, Req, Res, Runtime, SealReq, SnapshotId, Step,
    TakeSnapshotReq, WriteReq, WriteRes,
};

pub struct Direct {
    persister: RuntimeClient<String, ()>,
    streams: HashMap<String, (StreamWriteHandle<String, ()>, StreamReadHandle<String, ()>)>,
    snapshots: HashMap<SnapshotId, IndexedSnapshot<String, ()>>,
}

impl Runtime for Direct {
    fn run(&mut self, i: Input) -> Step {
        let res = match i.req {
            Req::Write(req) => Res::Write(req.clone(), self.write(req)),
            Req::Seal(req) => Res::Seal(req.clone(), self.seal(req)),
            Req::TakeSnapshot(req) => Res::TakeSnapshot(req.clone(), self.take_snapshot(req)),
            Req::ReadSnapshot(req) => Res::ReadSnapshot(req.clone(), self.read_snapshot(req)),
        };
        Step {
            req_id: i.req_id,
            res,
        }
    }

    fn finish(self) {}
}

impl Direct {
    pub fn new<F: FnMut() -> Result<RuntimeClient<String, ()>, Error>>(
        mut start_fn: F,
    ) -> Result<Self, Error> {
        let persister = start_fn()?;
        Ok(Direct {
            persister,
            streams: HashMap::new(),
            snapshots: HashMap::new(),
        })
    }

    fn stream(
        &mut self,
        name: &str,
    ) -> &mut (StreamWriteHandle<String, ()>, StreamReadHandle<String, ()>) {
        let (streams, persister) = (&mut self.streams, &mut self.persister);
        streams.entry(name.to_string()).or_insert_with(|| {
            persister
                .create_or_load(name)
                .expect("we only ever register once")
                .into_inner()
        })
    }

    fn write(&mut self, req: WriteReq) -> Result<WriteRes, Error> {
        let (write, _) = self.stream(&req.stream);
        let (tx, rx) = mpsc::channel();
        write.write(&[req.update], tx.into());
        let seqno = rx.recv().map_err(|_| Error::RuntimeShutdown)??.0;
        Ok(WriteRes { seqno })
    }

    fn seal(&mut self, req: SealReq) -> Result<(), Error> {
        let (write, _) = self.stream(&req.stream);
        let (tx, rx) = mpsc::channel();
        write.seal(req.ts, tx.into());
        rx.recv().map_err(|_| Error::RuntimeShutdown)?
    }

    fn take_snapshot(&mut self, req: TakeSnapshotReq) -> Result<(), Error> {
        let (_, meta) = self.stream(&req.stream);
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
        let mut contents = Vec::new();
        while snap.read(&mut contents) {}
        Ok(ReadSnapshotRes {
            seqno: snap.seqno().0,
            contents,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::file::{FileBlob, FileBuffer};
    use crate::mem::MemRegistry;
    use crate::nemesis;
    use crate::nemesis::generator::GeneratorConfig;

    use super::*;

    #[test]
    fn direct_mem() {
        let mut registry = MemRegistry::new();
        let direct = Direct::new(|| registry.open("direct_mem", "direct_mem"))
            .expect("initial start failed");
        nemesis::run(100, GeneratorConfig::default(), direct)
    }

    #[test]
    fn direct_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir creation failed");
        let direct = Direct::new(|| {
            let (buffer_dir, blob_dir) =
                (temp_dir.path().join("buffer"), temp_dir.path().join("blob"));
            let buffer = FileBuffer::new(buffer_dir, "direct_file")?;
            let blob = FileBlob::new(blob_dir, "direct_file")?;
            runtime::start(buffer, blob)
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
        let direct = Direct::new(|| registry.open("direct_mzlike", "direct_mzlike"))
            .expect("initial start failed");
        nemesis::run(100, config, direct)
    }
}
