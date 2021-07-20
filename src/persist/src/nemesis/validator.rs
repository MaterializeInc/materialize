// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use crate::error::Error;
use crate::nemesis::{
    ReadSnapshotReq, ReadSnapshotRes, ReqId, Res, SealReq, SnapshotId, Step, TakeSnapshotReq,
    WriteReq, WriteRes,
};
use crate::storage::SeqNo;

pub struct Validator {
    sealed: HashMap<String, u64>,
    writes_by_seqno: BTreeMap<(String, SeqNo), ((String, ()), u64, isize)>,
    available_snapshots: HashMap<SnapshotId, String>,
    errors: Vec<String>,
}

// TODO: Unit tests for Validator. This is already fairly complicatd and it's
// only going to get more complicated as we add more checks for API invariants.
impl Validator {
    pub fn validate<I: IntoIterator<Item = Step>>(history: I) -> Result<(), Vec<String>> {
        let mut v = Validator::new();
        // TODO: We'll need a sort here once we start validating concurrent
        // timelines. Also likely a before and after time for each
        // request/response.
        for step in history.into_iter() {
            v.step(step);
        }
        v.finish()
    }

    fn new() -> Self {
        Validator {
            sealed: HashMap::new(),
            writes_by_seqno: BTreeMap::new(),
            available_snapshots: HashMap::new(),
            errors: Vec::new(),
        }
    }

    fn finish(self) -> Result<(), Vec<String>> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }

    fn step(&mut self, s: Step) {
        // TODO: Figure out how to get the log crate working.
        eprintln!("step: {:?}", &s);
        match s.res {
            Res::Write(req, res) => self.step_write(s.req_id, req, res),
            Res::Seal(req, res) => self.step_seal(s.req_id, req, res),
            Res::TakeSnapshot(req, res) => self.step_take_snapshot(s.req_id, req, res),
            Res::ReadSnapshot(req, res) => self.step_read_snapshot(s.req_id, req, res),
        }
    }

    fn check_success<T: fmt::Debug>(
        &mut self,
        req_id: ReqId,
        res: &Result<T, Error>,
        should_succeed: bool,
    ) {
        match (should_succeed, res) {
            (true, Ok(_)) | (false, Err(_)) => {}
            (true, Err(err)) => self.errors.push(format!(
                "expected success but got error {:?}: {}",
                req_id, err
            )),
            (false, Ok(res)) => self.errors.push(format!(
                "expected error but got success {:?}: {:?}",
                req_id, res
            )),
        }
    }

    fn step_write(&mut self, req_id: ReqId, req: WriteReq, res: Result<WriteRes, Error>) {
        let should_succeed =
            req.update.1 >= self.sealed.get(&req.stream).copied().unwrap_or_default();
        self.check_success(req_id, &res, should_succeed);
        if let Ok(res) = res {
            self.writes_by_seqno
                .insert((req.stream, SeqNo(res.seqno)), req.update);
        }
    }

    fn step_seal(&mut self, req_id: ReqId, req: SealReq, res: Result<(), Error>) {
        let should_succeed = req.ts > self.sealed.get(&req.stream).copied().unwrap_or_default();
        self.check_success(req_id, &res, should_succeed);
        if let Ok(_) = res {
            self.sealed.insert(req.stream, req.ts);
        }
    }

    fn step_take_snapshot(&mut self, req_id: ReqId, req: TakeSnapshotReq, res: Result<(), Error>) {
        // At the moment, there are no nemeses that would prevent a snapshot
        // from working, so we shouldn't see any errors.
        let should_succeed = true;
        self.check_success(req_id, &res, should_succeed);
        if let Ok(_) = res {
            self.available_snapshots.insert(req.snap, req.stream);
        }
    }

    fn step_read_snapshot(
        &mut self,
        req_id: ReqId,
        req: ReadSnapshotReq,
        res: Result<ReadSnapshotRes, Error>,
    ) {
        match self.available_snapshots.remove(&req.snap) {
            None => {
                self.check_success(req_id, &res, false);
            }
            Some(stream) => {
                self.check_success(req_id, &res, true);
                if let Ok(res) = res {
                    let mut actual = res.contents;
                    let mut expected: Vec<((String, ()), u64, isize)> = self
                        .writes_by_seqno
                        .range((stream.clone(), SeqNo(0))..(stream, SeqNo(res.seqno)))
                        .map(|(_, v)| v)
                        .cloned()
                        .collect();
                    // TODO: This is also used by the implementation. Write a
                    // slower but more obvious impl of consolidation here and
                    // use it for validation.
                    differential_dataflow::consolidation::consolidate_updates(&mut actual);
                    differential_dataflow::consolidation::consolidate_updates(&mut expected);
                    if actual != expected {
                        self.errors.push(format!(
                            "incorrect snapshot {:?} expected {:?} got: {:?}",
                            req_id, expected, actual
                        ));
                    }
                }
            }
        }
    }
}
