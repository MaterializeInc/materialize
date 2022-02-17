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
use std::time::Instant;
use tracing::info;

use differential_dataflow::lattice::Lattice;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::nemesis::validator::uptime::Uptime;
use crate::nemesis::{
    AllowCompactionReq, ReadOutputEvent, ReadOutputReq, ReadOutputRes, ReadSnapshotReq,
    ReadSnapshotRes, Res, SealReq, SnapshotId, Step, StepMeta, TakeSnapshotReq, WriteReq,
    WriteReqMulti, WriteReqSingle,
};
use crate::storage::SeqNo;

#[derive(Debug)]
pub struct Validator {
    seal_frontier: HashMap<String, u64>,
    since_frontier: HashMap<String, u64>,
    writes_by_seqno: BTreeMap<(String, SeqNo), Vec<((String, ()), u64, isize)>>,
    output_by_stream:
        HashMap<String, Vec<ReadOutputEvent<(Result<(String, ()), String>, u64, isize)>>>,
    available_snapshots: HashMap<SnapshotId, (String, Instant)>,
    errors: Vec<String>,
    uptime: Uptime,
}

// TODO: Unit tests for Validator. This is already fairly complicated and it's
// only going to get more complicated as we add more checks for API invariants.
impl Validator {
    pub fn validate(mut history: Vec<Step>) -> Result<(), Vec<String>> {
        // Uptime requires that its input be sorted by before.
        history.sort_by_key(|s| s.meta.before);
        let uptime = Uptime::new(&history);
        let mut v = Validator::new(uptime);
        for step in history.into_iter() {
            v.step(step);
        }
        v.finish()
    }

    fn new(uptime: Uptime) -> Self {
        Validator {
            seal_frontier: HashMap::new(),
            since_frontier: HashMap::new(),
            writes_by_seqno: BTreeMap::new(),
            output_by_stream: HashMap::new(),
            available_snapshots: HashMap::new(),
            errors: Vec::new(),
            uptime,
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
        info!("step: {:?}", &s);
        match s.res {
            Res::Write(WriteReq::Single(req), res) => self.step_write_single(&s.meta, req, res),
            Res::Write(WriteReq::Multi(req), res) => self.step_write_multi(&s.meta, req, res),
            Res::ReadOutput(req, res) => self.step_read_output(&s.meta, req, res),
            Res::Seal(req, res) => self.step_seal(&s.meta, req, res),
            Res::AllowCompaction(req, res) => self.step_allow_compaction(&s.meta, req, res),
            Res::TakeSnapshot(req, res) => self.step_take_snapshot(&s.meta, req, res),
            Res::ReadSnapshot(req, res) => self.step_read_snapshot(&s.meta, req, res),
            Res::Start(res) => self.step_start(&s.meta, res),
            Res::Stop(res) => self.step_stop(&s.meta, res),
            Res::StorageUnavailable | Res::StorageAvailable => {}
        }
    }

    /// Enqueues an error if the given result was an error but we know
    /// externally that it should have succeeded.
    ///
    /// This used to also require failure when it was known externally that it
    /// shouldn't have been able to work (e.g. a write when storage is
    /// unavailable). That worked when nemesis was totally deterministic, but
    /// was already becoming a maintenance burden. As we move toward pipelining
    /// requests, running them concurrently in threads, and adding periods of
    /// non-determinism to the unreliable storage, it will require trinary logic
    /// (yes/no/maybe), which gets surprisingly complicated.
    ///
    /// Stepping back, nemesis's purpose is to verify that we adhere to our
    /// external API guarantees. This means assertions on histories of
    /// request/response pairs and outputs of dataflow operators, not anything
    /// around what to expect given detailed history of the availability of
    /// external systems.
    ///
    /// However, we do keep this one "liveness" check around so we don't
    /// accidentally pass validation by simply erroring every request (which is
    /// a valid, but not useful history).
    fn check_success<T: fmt::Debug>(
        &mut self,
        meta: &StepMeta,
        res: &Result<T, Error>,
        require_succeed: bool,
    ) {
        match (require_succeed, res) {
            (true, Err(err)) => self.errors.push(format!(
                "expected success but got error {:?}: {}",
                meta.req_id, err
            )),
            _ => {}
        }
    }

    fn check_failure<T: fmt::Debug>(
        &mut self,
        meta: &StepMeta,
        res: &Result<T, Error>,
        require_error: bool,
    ) {
        match (require_error, res) {
            (true, Ok(res)) => self.errors.push(format!(
                "expected error but got success {:?}: {:?}",
                meta.req_id, res
            )),
            _ => {}
        }
    }

    fn step_write_single(
        &mut self,
        meta: &StepMeta,
        req: WriteReqSingle,
        res: Result<SeqNo, Error>,
    ) {
        let req_ok = req.update.1
            >= self
                .seal_frontier
                .get(&req.stream)
                .copied()
                .unwrap_or_default();
        let require_succeed = self.uptime.storage_available(meta.before, meta.after)
            && self.uptime.runtime_available(meta.before, meta.after)
            && req_ok;
        self.check_success(meta, &res, require_succeed);
        self.check_failure(meta, &res, !req_ok);
        if let Ok(res) = res {
            self.writes_by_seqno
                .entry((req.stream, res))
                .or_default()
                .push(req.update);
        }
    }

    fn step_write_multi(&mut self, meta: &StepMeta, req: WriteReqMulti, res: Result<SeqNo, Error>) {
        let req_ok = req.writes.len() > 0
            && req.writes.iter().all(|req| {
                req.update.1
                    >= self
                        .seal_frontier
                        .get(&req.stream)
                        .copied()
                        .unwrap_or_default()
            });
        let require_succeed = self.uptime.storage_available(meta.before, meta.after)
            && self.uptime.runtime_available(meta.before, meta.after)
            && req_ok;
        self.check_success(meta, &res, require_succeed);
        self.check_failure(meta, &res, !req_ok);
        if let Ok(res) = res {
            for req in req.writes {
                self.writes_by_seqno
                    .entry((req.stream, res))
                    .or_default()
                    .push(req.update);
            }
        }
    }

    fn step_read_output(
        &mut self,
        meta: &StepMeta,
        req: ReadOutputReq,
        res: Result<ReadOutputRes, Error>,
    ) {
        let require_succeed = true;
        self.check_success(meta, &res, require_succeed);

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

            if !as_of.less_than(&latest_seal) {
                // TODO: We cannot currently verify cases where the compaction frontier is beyond
                // the since, because we cannot determine anymore (based on the latest seal
                // timestamp) which records (both from the expected writes and the writes we get
                // from timely) are eligible for verification.
                return;
            }

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
                    meta.req_id, latest_seal, expected, actual
                ));
            }
        }
    }

    fn step_seal(&mut self, meta: &StepMeta, req: SealReq, res: Result<SeqNo, Error>) {
        let req_ok = req.ts
            >= self
                .seal_frontier
                .get(&req.stream)
                .copied()
                .unwrap_or_default();
        let require_succeed = self.uptime.storage_available(meta.before, meta.after)
            && self.uptime.runtime_available(meta.before, meta.after)
            && req_ok;
        self.check_success(meta, &res, require_succeed);
        self.check_failure(meta, &res, !req_ok);
        if let Ok(_) = res {
            self.seal_frontier.insert(req.stream, req.ts);
        }
    }

    fn step_allow_compaction(
        &mut self,
        meta: &StepMeta,
        req: AllowCompactionReq,
        res: Result<SeqNo, Error>,
    ) {
        let req_ok = req.ts
            >= self
                .since_frontier
                .get(&req.stream)
                .copied()
                .unwrap_or_default();
        let require_succeed = self.uptime.storage_available(meta.before, meta.after)
            && self.uptime.runtime_available(meta.before, meta.after)
            && req_ok;
        self.check_success(meta, &res, require_succeed);
        self.check_failure(meta, &res, !req_ok);
        if let Ok(_) = res {
            self.since_frontier.insert(req.stream, req.ts);
        }
    }

    fn step_take_snapshot(
        &mut self,
        meta: &StepMeta,
        req: TakeSnapshotReq,
        res: Result<SeqNo, Error>,
    ) {
        let require_succeed = self.uptime.storage_available(meta.before, meta.after)
            && self.uptime.runtime_available(meta.before, meta.after);
        self.check_success(meta, &res, require_succeed);
        if let Ok(_) = res {
            self.available_snapshots
                .insert(req.snap, (req.stream, meta.before));
        }
    }

    fn step_read_snapshot(
        &mut self,
        meta: &StepMeta,
        req: ReadSnapshotReq,
        res: Result<ReadSnapshotRes, Error>,
    ) {
        match self.available_snapshots.remove(&req.snap) {
            None => {
                self.check_success(meta, &res, false);
            }
            Some((stream, before_snap_start)) => {
                let require_succeed = self.uptime.storage_available(before_snap_start, meta.after)
                    && self.uptime.runtime_available(before_snap_start, meta.after);
                self.check_success(meta, &res, require_succeed);
                if let Ok(res) = res {
                    let mut actual = res.contents;
                    let mut expected: Vec<((String, ()), u64, isize)> = self
                        .writes_by_seqno
                        .range((stream.clone(), SeqNo(0))..=(stream, SeqNo(res.seqno)))
                        .flat_map(|(_, v)| v)
                        .cloned()
                        .collect();
                    if !updates_eq(&mut actual, &mut expected, res.since) {
                        self.errors.push(format!(
                            "incorrect snapshot {:?} expected {:?} got: {:?}",
                            meta.req_id, expected, actual
                        ));
                    }
                }
            }
        }
    }

    fn step_start(&mut self, meta: &StepMeta, res: Result<(), Error>) {
        // The semantics of Req::Start are pretty blunt. It unconditionally
        // attempts to start a new persister. If the storage is down, the new
        // one won't be able to read metadata and will fail to start. This will
        // cause all operations on the persister to fail until it gets another
        // Req::Start with storage available. This ends up being a pretty
        // uninteresting state to test, so we filter out everything but start
        // and storage_available when the runtime is not available.
        let require_succeed = self.uptime.storage_available(meta.before, meta.after);
        self.check_success(meta, &res, require_succeed);
    }

    fn step_stop(&mut self, meta: &StepMeta, res: Result<(), Error>) {
        // Stop will succeed if it can cleanly release locks, which
        // requires the storage to be available.
        let require_succeed = self.uptime.storage_available(meta.before, meta.after);
        self.check_success(meta, &res, require_succeed);
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

mod uptime {
    use std::cmp;
    use std::collections::BTreeMap;
    use std::time::Instant;

    use crate::nemesis::{Res, Step};

    /// A helper for Validate that tracks which times storage/runtime were
    /// unambiguously up.
    #[derive(Debug)]
    pub struct Uptime {
        storage_downtime: IntervalTree<Instant>,
        runtime_downtime: IntervalTree<Instant>,
    }

    impl Uptime {
        /// Returns a new [Uptime] from the given steps.
        ///
        /// Steps must be sorted by before.
        pub fn new(steps: &[Step]) -> Self {
            let (storage_downtime, runtime_downtime) =
                match steps.iter().max_by_key(|s| s.meta.after) {
                    Some(max_after) => (
                        Uptime::storage_downtime(steps, max_after.meta.after),
                        Uptime::runtime_downtime(steps, max_after.meta.after),
                    ),
                    None => (IntervalTree::default(), IntervalTree::default()),
                };
            Uptime {
                storage_downtime,
                runtime_downtime,
            }
        }

        /// Returns true if storage was unambiguously available for the entire given
        /// range.
        ///
        /// Before and after are both inclusive.
        pub fn storage_available(&self, before: Instant, after: Instant) -> bool {
            !self.storage_downtime.overlaps(before, after)
        }

        /// Returns true if the runtime was unambiguously available for the entire
        /// given range.
        ///
        /// Before and after are both inclusive.
        pub fn runtime_available(&self, before: Instant, after: Instant) -> bool {
            !self.runtime_downtime.overlaps(before, after)
        }

        // NB: Steps must be sorted by before.
        fn storage_downtime(steps: &[Step], after_all_steps: Instant) -> IntervalTree<Instant> {
            let mut downtime_before = None;
            let mut storage_downtime = IntervalTree::default();
            for step in steps {
                match step.res {
                    Res::StorageAvailable => match downtime_before.take() {
                        Some(downtime_before) => {
                            storage_downtime.push(downtime_before, step.meta.after)
                        }
                        None => {}
                    },
                    Res::StorageUnavailable => {
                        if downtime_before.is_none() {
                            downtime_before = Some(step.meta.before)
                        }
                    }
                    _ => {}
                }
            }
            // If downtime_before is still a Some, that means the test ended in
            // downtime.
            if let Some(downtime_before) = downtime_before.take() {
                storage_downtime.push(downtime_before, after_all_steps);
            }
            storage_downtime
        }

        // NB: Steps must be sorted by before.
        fn runtime_downtime(steps: &[Step], after_all_steps: Instant) -> IntervalTree<Instant> {
            let mut downtime_before = None;
            let mut runtime_downtime = IntervalTree::default();
            for step in steps {
                match step.res {
                    // We tried to start the runtime and it succeeded.
                    Res::Start(Ok(_)) => match downtime_before.take() {
                        Some(downtime_before) => {
                            runtime_downtime.push(downtime_before, step.meta.after)
                        }
                        None => {}
                    },
                    // We either tried to start the runtime and it failed (most
                    // likely the storage was down) or we stopped the runtime.
                    Res::Start(Err(_)) | Res::Stop(_) => {
                        if downtime_before.is_none() {
                            downtime_before = Some(step.meta.before)
                        }
                    }
                    _ => {}
                }
            }
            // If downtime_before is still a Some, that means the test ended in
            // downtime.
            if let Some(downtime_before) = downtime_before.take() {
                runtime_downtime.push(downtime_before, after_all_steps);
            }
            runtime_downtime
        }
    }

    /// Just enough of an interval tree implementation for Validator's needs.
    ///
    /// If this gets any more complicated, we should probably switch to an interval
    /// tree library. Simplifying requirements of this impl:
    /// - Construction happens sorted by start.
    /// - All added intervals are merged.
    #[derive(Debug)]
    pub struct IntervalTree<T> {
        intervals: BTreeMap<T, T>,
    }

    impl<T: Ord> Default for IntervalTree<T> {
        fn default() -> Self {
            Self {
                intervals: BTreeMap::new(),
            }
        }
    }

    impl<T: Ord + Copy> IntervalTree<T> {
        /// Add a new interval to this tree.
        ///
        /// Start must be >= the start of any previous interval added. Both start
        /// and end are inclusive.
        pub fn push(&mut self, start: T, end: T) {
            assert!(end >= start);
            assert!(self
                .intervals
                .iter()
                .last()
                .map_or(true, |(s, _)| *s <= start));
            let overlapping = self.intervals.iter().last().and_then(|(s, e)| {
                if *e >= start {
                    Some((*s, *e))
                } else {
                    None
                }
            });
            match overlapping {
                Some((overlapping_s, overlapping_e)) => self
                    .intervals
                    .insert(overlapping_s, cmp::max(overlapping_e, end)),
                None => self.intervals.insert(start, end),
            };
        }

        /// Returns whether the given interval overlaps any previously added
        /// interval.
        ///
        /// Both start and end are inclusive.
        pub fn overlaps(&self, begin: T, end: T) -> bool {
            let overlaps_lt_begin = self
                .intervals
                .range(..begin)
                .last()
                .map_or(false, |(_, e)| *e >= begin);
            let overlaps_ge_begin = self.intervals.range(begin..=end).next().is_some();
            overlaps_lt_begin || overlaps_ge_begin
        }
    }

    #[cfg(test)]
    mod tests {
        use std::time::{Duration, Instant};

        use crate::error::Error;
        use crate::nemesis::{ReqId, StepMeta};

        use super::*;

        #[test]
        fn uptime() {
            let beginning_of_time = Instant::now();
            let ts = |offset_micros| beginning_of_time + Duration::from_micros(offset_micros);
            let storage_up = |before, after| Step {
                meta: StepMeta {
                    req_id: ReqId(0),
                    before: ts(before),
                    after: ts(after),
                },
                res: Res::StorageAvailable,
            };
            let storage_down = |before, after| Step {
                meta: StepMeta {
                    req_id: ReqId(0),
                    before: ts(before),
                    after: ts(after),
                },
                res: Res::StorageUnavailable,
            };
            let runtime_up = |before, after, success| Step {
                meta: StepMeta {
                    req_id: ReqId(0),
                    before: ts(before),
                    after: ts(after),
                },
                res: Res::Start(if success {
                    Ok(())
                } else {
                    Err(Error::from("failed"))
                }),
            };
            let runtime_down = |before, after| Step {
                meta: StepMeta {
                    req_id: ReqId(0),
                    before: ts(before),
                    after: ts(after),
                },
                res: Res::Stop(Ok(())),
            };

            // Empty. Both storage and runtime start as available.
            let u = Uptime::new(&[]);
            assert_eq!(u.storage_available(ts(0), ts(1)), true);
            assert_eq!(u.runtime_available(ts(0), ts(1)), true);

            // Check storage.
            let u = Uptime::new(&[
                // Storage starts up, so this is a no-op.
                storage_up(1, 2),
                // The [4,5] interval here are a bounds on some instant when it
                // became unavailable. It stays down until the next up.
                storage_down(4, 5),
                // Storage became re-available somewhere in this interval, but
                // we don't know for sure that it's back until _after_ this
                // entire interval. Since the end is closed, that meant ts(9) is
                // the first time it's guaranteed back up.l
                storage_up(7, 8),
                // End with storage down to catch an edge case.
                storage_down(11, 12),
            ]);
            // Storage starts up so the [1, 2] storage_up is a no-op.
            assert_eq!(u.storage_available(ts(0), ts(3)), true);
            // All intervals are inclusive on both ends, so anything touching
            // [4,8] is down.
            assert_eq!(u.storage_available(ts(2), ts(3)), true);
            assert_eq!(u.storage_available(ts(3), ts(3)), true);
            assert_eq!(u.storage_available(ts(3), ts(4)), false);
            assert_eq!(u.storage_available(ts(4), ts(4)), false);
            assert_eq!(u.storage_available(ts(4), ts(8)), false);
            assert_eq!(u.storage_available(ts(5), ts(7)), false);
            assert_eq!(u.storage_available(ts(8), ts(8)), false);
            assert_eq!(u.storage_available(ts(9), ts(9)), true);
            assert_eq!(u.storage_available(ts(9), ts(10)), true);

            // Runtime has its own wrinkles (unlike making storage available,
            // starting the runtime can fail), so exercise those. We could also
            // duplicate the storage tests above for runtime too, but the code
            // pathways are the same so don't bother.
            let u = Uptime::new(&[
                runtime_down(1, 2),
                // Runtime fails to start, so is still down
                runtime_up(4, 5, false),
                // Runtime actually starts.
                runtime_up(7, 8, true),
            ]);
            assert_eq!(u.runtime_available(ts(0), ts(0)), true);
            assert_eq!(u.runtime_available(ts(1), ts(4)), false);
            assert_eq!(u.runtime_available(ts(6), ts(6)), false);
            assert_eq!(u.runtime_available(ts(9), ts(9)), true);

            // Check that runtime and storage are independent. (They're not in
            // reality, runtime can't come up with storage down, but we let the
            // step results tell us that instead of inferring it.)
            let u = Uptime::new(&[
                runtime_down(1, 2),
                runtime_up(4, 5, true),
                storage_down(7, 8),
                storage_up(10, 11),
            ]);
            assert_eq!(u.storage_available(ts(1), ts(5)), true);
            assert_eq!(u.runtime_available(ts(1), ts(5)), false);
            assert_eq!(u.storage_available(ts(7), ts(11)), false);
            assert_eq!(u.runtime_available(ts(7), ts(11)), true);
        }

        #[test]
        fn interval_tree() {
            let mut i = IntervalTree::<usize>::default();

            // Empty tree.
            assert_eq!(i.overlaps(0, 1), false);

            // Add some data and check overlaps.
            i.push(2, 3);
            i.push(10, 13);
            assert_eq!(i.overlaps(0, 1), false);
            assert_eq!(i.overlaps(0, 2), true);
            assert_eq!(i.overlaps(1, 2), true);
            assert_eq!(i.overlaps(1, 3), true);
            assert_eq!(i.overlaps(1, 4), true);
            assert_eq!(i.overlaps(2, 2), true);
            assert_eq!(i.overlaps(2, 3), true);
            assert_eq!(i.overlaps(3, 3), true);
            assert_eq!(i.overlaps(3, 4), true);
            assert_eq!(i.overlaps(4, 5), false);
            assert_eq!(i.overlaps(11, 12), true);

            // Regression test for a bug in the initial impl where the `end` of
            // the interval being pushed was always kept, even if one in the
            // tree already had a later end.
            assert_eq!(i.overlaps(12, 13), true);
            assert_eq!(i.overlaps(13, 14), true);
            i.push(11, 12);
            assert_eq!(i.overlaps(13, 14), true);
        }
    }
}
