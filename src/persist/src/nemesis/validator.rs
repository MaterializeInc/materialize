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

use differential_dataflow::lattice::Lattice;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::nemesis::{
    AllowCompactionReq, ReadOutputEvent, ReadOutputReq, ReadOutputRes, ReadSnapshotReq,
    ReadSnapshotRes, ReqId, Res, SealReq, SnapshotId, Step, TakeSnapshotReq, WriteReq,
    WriteReqMulti, WriteReqSingle, WriteRes,
};
use crate::storage::SeqNo;

#[derive(Debug)]
pub struct Validator {
    seal_frontier: HashMap<String, u64>,
    since_frontier: HashMap<String, u64>,
    writes_by_seqno: BTreeMap<(String, SeqNo), Vec<((String, ()), u64, isize)>>,
    output_by_stream:
        HashMap<String, Vec<ReadOutputEvent<(Result<(String, ()), String>, u64, isize)>>>,
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
            output_by_stream: HashMap::new(),
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
        log::debug!("step: {:?}", &s);
        match s.res {
            Res::Write(WriteReq::Single(req), res) => self.step_write_single(s.req_id, req, res),
            Res::Write(WriteReq::Multi(req), res) => self.step_write_multi(s.req_id, req, res),
            Res::ReadOutput(req, res) => self.step_read_output(s.req_id, req, res),
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

    fn step_read_output(
        &mut self,
        req_id: ReqId,
        req: ReadOutputReq,
        res: Result<ReadOutputRes, Error>,
    ) {
        let should_succeed = true;
        self.check_success(req_id, &res, should_succeed);

        if let Ok(res) = res {
            let all_stream_output = self.output_by_stream.entry(req.stream.clone()).or_default();
            all_stream_output.extend(res.contents);

            // Seal acts as a barrier, so we're guaranteed to receive any writes
            // that were sent for times before the seal. However, we're not
            // guaranteed to see every seal we sent, especially across restarts.
            // Start by finding the latest seal.
            let mut latest_seal = Timestamp::minimum();
            let mut all_received_writes = Vec::new();
            let mut all_received_errors = Vec::new();
            for e in all_stream_output.iter() {
                match e {
                    ReadOutputEvent::Sealed(ts) => {
                        if *ts > latest_seal {
                            latest_seal = *ts;
                        }
                    }
                    ReadOutputEvent::Records(records) => {
                        for r in records.iter() {
                            match r {
                                (Ok((k, v)), ts, diff) => {
                                    all_received_writes.push(((k.clone(), *v), *ts, *diff))
                                }
                                (Err(err), _, _) => all_received_errors.push(err.clone()),
                            }
                        }
                    }
                }
            }

            // If we've gotten any errors out of the dataflow, all bets are off.
            if !all_received_errors.is_empty() {
                return;
            }

            // The latest seal shouldn't be past anything we sent.
            let latest_seal_sent = self
                .seal_frontier
                .get(&req.stream)
                .copied()
                .unwrap_or_default();
            if latest_seal > latest_seal_sent {
                self.errors.push(format!(
                    "received seal {} greater than the latest one we sent {}",
                    latest_seal, latest_seal_sent
                ));
            }

            // To compare two sets of records, we first need to ensure they have
            // the same since.
            //
            // In the writes_by_seqno map that Validator keeps internally, the
            // original full-fidelity records are kept. This corresponds to a
            // since of 0 (more specifically, the empty antichain).
            //
            // Because the records output by the PersistedSource operator
            // includes replaying a snapshot, it may have a since >0. Once we
            // fix #8608, we'll know exactly what it is (because #8608 is all
            // about specifying the since/as_of at construction time) but until
            // then we don't really know what it is.
            //
            // So what we do for now is forward both sets of records to a since
            // that's guaranteed to be at in advance of both of them:
            // specifically the the largest thing we've allowed_compaction to.
            let as_of = Antichain::from_elem(
                self.since_frontier
                    .get(&req.stream)
                    .copied()
                    .unwrap_or_default(),
            );

            // Verify that the output contains all sent writes less than the
            // latest seal it contains.
            //
            // TODO: Figure out what our contract is for writes we've received
            // in advance of this latest seal. There should be something we can
            // do here.
            let mut actual = all_received_writes
                .into_iter()
                .filter(|(_, ts, _)| *ts < latest_seal)
                .map(|(kv, mut ts, diff)| {
                    // TODO: For the same reason we only advance the "expected"
                    // side of updates_eq (the Validator's writes_by_seqno),
                    // once we've fixed #8608, we should only advance the
                    // `expected` side of updates_eq to whatever we set as the
                    // as_of when constructing the PersistedSource operator.
                    // Correct adherence to the since/as_of is part of the
                    // interface of Snapshot and #8608 is all about making it
                    // part of the interface of PersistedSource as well.
                    ts.advance_by(as_of.borrow());
                    (kv, ts, diff)
                })
                .collect();
            let mut expected: Vec<((String, ()), u64, isize)> = self
                .writes_by_seqno
                .range((req.stream.clone(), SeqNo(0))..(req.stream, SeqNo(u64::MAX)))
                .flat_map(|(_, v)| v)
                .filter(|(_, ts, _)| *ts < latest_seal)
                .cloned()
                .collect();
            if !updates_eq(&mut actual, &mut expected, as_of) {
                self.errors.push(format!(
                    "incorrect output {:?} up to {}, expected {:?} got: {:?}",
                    req_id, latest_seal, expected, actual
                ));
            }
        }
    }

    fn step_seal(&mut self, req_id: ReqId, req: SealReq, res: Result<(), Error>) {
        let should_succeed = self.runtime_available
            && self.storage_available
            && req.ts
                >= self
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
                >= self
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
                    if !updates_eq(&mut actual, &mut expected, res.since) {
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
        self.output_by_stream.clear();
    }
}

fn updates_eq(
    actual: &mut Vec<((String, ()), u64, isize)>,
    expected: &mut Vec<((String, ()), u64, isize)>,
    since: Antichain<u64>,
) -> bool {
    // TODO: This is also used by the implementation. Write a slower but more
    // obvious impl of consolidation here and use it for validation.

    // The snapshot has been logically compacted to since, so update our
    // expected to match.
    for (_, t, _) in expected.iter_mut() {
        t.advance_by(since.borrow());
    }
    differential_dataflow::consolidation::consolidate_updates(actual);
    differential_dataflow::consolidation::consolidate_updates(expected);
    actual == expected
}
