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
    AllowCompactionReq, ReadSnapshotReq, ReadSnapshotRes, ReqId, Res, SealReq, SnapshotId, Step,
    TakeSnapshotReq, WriteReq, WriteReqMulti, WriteReqSingle, WriteRes,
};
use crate::storage::SeqNo;

pub struct Validator {
    seal_frontier: HashMap<String, u64>,
    since_frontier: HashMap<String, u64>,
    writes_by_seqno: BTreeMap<(String, SeqNo), Vec<((String, ()), u64, isize)>>,
    available_snapshots: HashMap<SnapshotId, String>,
    errors: Vec<String>,
    storage_available: bool,
    runtime_available: bool,
}

// TODO: Unit tests for Validator. This is already fairly complicated and it's
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
            seal_frontier: HashMap::new(),
            since_frontier: HashMap::new(),
            writes_by_seqno: BTreeMap::new(),
            available_snapshots: HashMap::new(),
            errors: Vec::new(),
            storage_available: true,
            runtime_available: true,
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
            Res::Write(WriteReq::Single(req), res) => self.step_write_single(s.req_id, req, res),
            Res::Write(WriteReq::Multi(req), res) => self.step_write_multi(s.req_id, req, res),
            Res::Seal(req, res) => self.step_seal(s.req_id, req, res),
            Res::AllowCompaction(req, res) => self.step_allow_compaction(s.req_id, req, res),
            Res::TakeSnapshot(req, res) => self.step_take_snapshot(s.req_id, req, res),
            Res::ReadSnapshot(req, res) => self.step_read_snapshot(s.req_id, req, res),
            Res::Start(res) => self.step_start(s.req_id, res),
            Res::Stop(res) => self.step_stop(s.req_id, res),
            Res::StorageUnavailable => {
                self.storage_available = false;
            }
            Res::StorageAvailable => {
                self.storage_available = true;
            }
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

    fn step_write_single(
        &mut self,
        req_id: ReqId,
        req: WriteReqSingle,
        res: Result<WriteRes, Error>,
    ) {
        let should_succeed = self.runtime_available
            && self.storage_available
            && req.update.1
                >= self
                    .seal_frontier
                    .get(&req.stream)
                    .copied()
                    .unwrap_or_default();
        self.check_success(req_id, &res, should_succeed);
        if let Ok(res) = res {
            self.writes_by_seqno
                .entry((req.stream, SeqNo(res.seqno)))
                .or_default()
                .push(req.update);
        }
    }

    fn step_write_multi(
        &mut self,
        req_id: ReqId,
        req: WriteReqMulti,
        res: Result<WriteRes, Error>,
    ) {
        let should_succeed = self.runtime_available
            && self.storage_available
            && req.writes.len() > 0
            && req.writes.iter().all(|req| {
                req.update.1
                    >= self
                        .seal_frontier
                        .get(&req.stream)
                        .copied()
                        .unwrap_or_default()
            });
        self.check_success(req_id, &res, should_succeed);
        if let Ok(res) = res {
            for req in req.writes {
                self.writes_by_seqno
                    .entry((req.stream, SeqNo(res.seqno)))
                    .or_default()
                    .push(req.update);
            }
        }
    }

    fn step_seal(&mut self, req_id: ReqId, req: SealReq, res: Result<(), Error>) {
        let should_succeed = self.runtime_available
            && self.storage_available
            && req.ts
                > self
                    .seal_frontier
                    .get(&req.stream)
                    .copied()
                    .unwrap_or_default();
        self.check_success(req_id, &res, should_succeed);
        if let Ok(_) = res {
            self.seal_frontier.insert(req.stream, req.ts);
        }
    }

    fn step_allow_compaction(
        &mut self,
        req_id: ReqId,
        req: AllowCompactionReq,
        res: Result<(), Error>,
    ) {
        let should_succeed = self.runtime_available
            && self.storage_available
            && req.ts
                > self
                    .since_frontier
                    .get(&req.stream)
                    .copied()
                    .unwrap_or_default()
            && req.ts
                < self
                    .seal_frontier
                    .get(&req.stream)
                    .copied()
                    .unwrap_or_default();
        self.check_success(req_id, &res, should_succeed);
        if let Ok(_) = res {
            self.since_frontier.insert(req.stream, req.ts);
        }
    }

    fn step_take_snapshot(&mut self, req_id: ReqId, req: TakeSnapshotReq, res: Result<(), Error>) {
        let should_succeed = self.runtime_available && self.storage_available;
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
                        .flat_map(|(_, v)| v)
                        .cloned()
                        .collect();
                    // TODO: This is also used by the implementation. Write a
                    // slower but more obvious impl of consolidation here and
                    // use it for validation.
                    // TODO: The actual snapshot will eventually be compacted up
                    // to some since frontier and the expected snapshot will need
                    // to account for that when checking equality.
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

    fn step_start(&mut self, req_id: ReqId, res: Result<(), Error>) {
        // The semantics of Req::Start are pretty blunt. It unconditionally
        // attempts to start a new persister. If the storage is down, the new
        // one won't be able to read metadata and will fail to start. This will
        // cause all operations on the persister to fail until it gets another
        // Req::Start with storage available. This ends up being a pretty
        // uninteresting state to test, so we filter out everything but start
        // and storage_available when the runtime is not available.
        let should_succeed = self.storage_available;
        self.check_success(req_id, &res, should_succeed);
        self.runtime_available = res.is_ok();
    }

    fn step_stop(&mut self, req_id: ReqId, res: Result<(), Error>) {
        // Stop is a no-op if the runtime is already down (and so will succeed).
        // Otherwise, it will succeed if it can cleanly release locks, which
        // requires the storage to be available.
        let should_succeed = !self.runtime_available || self.storage_available;
        self.check_success(req_id, &res, should_succeed);
        self.runtime_available = false;
    }
}
