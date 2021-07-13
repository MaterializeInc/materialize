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
use crate::mem::{MemBlob, MemBuffer};
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
    pub fn new(lock_info: &str) -> Result<Self, Error> {
        let buffer = MemBuffer::new(lock_info)?;
        let blob = MemBlob::new(lock_info)?;
        let persister = runtime::start(buffer, blob)?;
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
    use crate::nemesis;

    use super::*;

    // TODO: Un-ignore this once the bugs it's catching are fixed.
    #[test]
    #[ignore]
    fn direct_mem() {
        nemesis::run(100, || {
            Direct::new("direct_mem").expect("new empty persist runtime is infallible")
        })
    }
}
