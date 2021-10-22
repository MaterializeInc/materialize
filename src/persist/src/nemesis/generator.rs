// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use rand::distributions::WeightedIndex;
use rand::prelude::{Distribution, IteratorRandom, SliceRandom, SmallRng};
use rand::{Rng, SeedableRng};
use serde::Serialize;

use crate::nemesis::{
    AllowCompactionReq, Input, ReadOutputReq, ReadSnapshotReq, Req, ReqId, SealReq, SnapshotId,
    TakeSnapshotReq, WriteReq, WriteReqMulti, WriteReqSingle,
};

/// Configuration of the relative probabilities of producing various
// operations.
#[derive(Clone, Debug, Serialize)]
pub struct GeneratorConfig {
    pub write_unsealed_weight: u32,
    pub write_sealed_weight: u32,
    pub write_multi_weight: u32,
    pub read_output_weight: u32,
    pub seal_weight: u32,
    pub allow_compaction_weight: u32,
    pub take_snapshot_weight: u32,
    pub read_snapshot_weight: u32,
    pub start_weight: u32,
    pub stop_weight: u32,
    pub storage_unavailable: u32,
    pub storage_available: u32,
}

impl GeneratorConfig {
    fn all_operations() -> GeneratorConfig {
        GeneratorConfig {
            write_unsealed_weight: 1,
            write_sealed_weight: 1,
            write_multi_weight: 1,
            read_output_weight: 1,
            seal_weight: 1,
            allow_compaction_weight: 1,
            take_snapshot_weight: 1,
            read_snapshot_weight: 1,
            start_weight: 1,
            stop_weight: 1,
            storage_unavailable: 1,
            storage_available: 1,
        }
    }
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        #[allow(unused_mut)]
        let mut ops = Self::all_operations();
        // NB: If we need to temporarily disable an operation in all the nemesis
        // tests, set it to 0 here. (As opposed to clearing it in the impl of
        // `all_operations`, which will break the Generator tests.)
        ops
    }
}

#[derive(Debug)]
struct GeneratorState {
    running: bool,
    seal_frontier: HashMap<String, u64>,
    since_frontier: HashMap<String, u64>,
    snap_id: SnapshotId,
    outstanding_snaps: HashMap<SnapshotId, String>,
    storage_available: bool,
    streams: Vec<String>,
    stream_weights: WeightedIndex<u32>,
    keys: Vec<String>,
    key_weights: WeightedIndex<u32>,
}

impl Default for GeneratorState {
    fn default() -> Self {
        GeneratorState {
            running: true,
            seal_frontier: HashMap::new(),
            since_frontier: HashMap::new(),
            snap_id: SnapshotId(0),
            outstanding_snaps: HashMap::new(),
            storage_available: true,
            // TODO: Allow for a dynamic number of streams and keys
            streams: ('a'..='e').map(|x| x.to_string()).collect(),
            stream_weights: WeightedIndex::new(&[9, 5, 3, 1, 1]).expect("weights are valid"),
            keys: ('a'..='e').map(|x| x.to_string()).collect(),
            key_weights: WeightedIndex::new(&[9, 5, 3, 1, 1]).expect("weights are valid"),
        }
    }
}

impl GeneratorState {
    fn rng_stream(&self, rng: &mut SmallRng) -> String {
        self.streams[self.stream_weights.sample(rng)].clone()
    }

    fn rng_key(&self, rng: &mut SmallRng) -> String {
        self.keys[self.key_weights.sample(rng)].clone()
    }
}

enum ReqGenerator {
    WriteUnsealed,
    WriteSealed,
    WriteMulti,
    ReadOutput,
    Seal,
    AllowCompaction,
    TakeSnapshot,
    ReadSnapshot,
    Stop,
    Start,
    StorageUnavailable,
    StorageAvailable,
}

impl ReqGenerator {
    fn write_unsealed(rng: &mut SmallRng, state: &mut GeneratorState) -> WriteReqSingle {
        let stream = state.rng_stream(rng);
        let key = state.rng_key(rng);
        let stream_sealed_ts = state
            .seal_frontier
            .get(&stream)
            .copied()
            .unwrap_or_default();
        let ts = stream_sealed_ts + rng.gen_range(0..5);
        // TODO: Tables and sources generally will be using -1 and
        // 1. 0, -2, and 2 might find some more interesting edge
        // cases. Is it worth expanding this to something with a
        // wider range (and possibly a non-uniform distribution)
        // once we start thinking of operator persistence? Perhaps
        // overflows, what else might be relevant here then?
        let diff = rng.gen_range(-2..=2);
        WriteReqSingle {
            stream,
            update: ((key, ()), ts, diff),
        }
    }

    fn write_sealed(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let key = state.rng_key(rng);
        let (stream, ts) = state
            .seal_frontier
            .iter()
            .filter(|(_, ts)| **ts > 0)
            .choose(rng)
            .map(|(stream, ts)| (stream.clone(), rng.gen_range(0..*ts)))
            .expect("internal nemesis error: no streams has a sealed ts > 0");
        // This is expected to error so it doesn't matter what diff is.
        let diff = 1;
        Req::Write(WriteReq::Single(WriteReqSingle {
            stream: stream,
            update: ((key, ()), ts, diff),
        }))
    }

    // NB: Unlike WriteReqSingle, this is always generated so that the timestamp
    // will not be sealed. We already cover that error case with write_sealed
    // and, given that a single write ends up being mapped to the same code path
    // by the time it gets to the Runtime barrier, it's not interesting to also
    // test it here.
    fn write_multi(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        // MultiWriteHandle and atomic_write both gracefully handle duplication,
        // so we can simply generate some number of normal writes to perform
        // atomically.
        //
        // NB: We generate up to state.streams.len() writes to apply atomically,
        // but, again, this doesn't mean they are distinct streams. It's just a
        // convenient way to make this scale up proportionally to the number of
        // streams (which may stop being hardcoded in the future).
        let writes = (0..state.streams.len())
            .map(|_| ReqGenerator::write_unsealed(rng, state))
            .collect();
        Req::Write(WriteReq::Multi(WriteReqMulti { writes }))
    }

    fn read_output(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let stream = state.rng_stream(rng);
        Req::ReadOutput(ReadOutputReq { stream })
    }

    fn seal(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let stream = state.rng_stream(rng);
        let stream_sealed_ts = state
            .seal_frontier
            .get(&stream)
            .copied()
            .unwrap_or_default();
        let ts = stream_sealed_ts + rng.gen_range(0..5);
        // This seal request might end up failing if (e.g. storage is down),
        // but, unlike in Validator, optimistically updating the seal frontier
        // here does no harm, it just bumps up the time at which we'll issue
        // future seal requests.
        state.seal_frontier.insert(stream.clone(), ts);
        Req::Seal(SealReq { stream, ts })
    }

    fn allow_compaction(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let stream = state.rng_stream(rng);
        let bonus = rng.gen_range(0..5);
        let ts = if rng.gen_bool(0.5) {
            let base = state
                .since_frontier
                .get(&stream)
                .copied()
                .unwrap_or_default();
            let ts = base + bonus;
            // This allow_compaction request might end up failing if (e.g.
            // storage is down), but, unlike in Validator, optimistically
            // updating the since frontier here does no harm, it just bumps up
            // the time at which we'll issue future allow_compaction requests.
            state.since_frontier.insert(stream.to_string(), ts);
            ts
        } else {
            let base = state
                .seal_frontier
                .get(&stream)
                .copied()
                .unwrap_or_default();
            let ts = base + bonus;
            // This allow_compaction request is expected to fail, so don't
            // update the since frontier.
            ts
        };
        Req::AllowCompaction(AllowCompactionReq { stream, ts })
    }

    fn take_snapshot(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let stream = state.rng_stream(rng);
        let snap = SnapshotId(state.snap_id.0);
        state.snap_id = SnapshotId(snap.0 + 1);
        state.outstanding_snaps.insert(snap, stream.clone());
        Req::TakeSnapshot(TakeSnapshotReq { stream, snap })
    }

    fn read_snapshot(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let snap = state
            .outstanding_snaps
            .keys()
            .choose(rng)
            .copied()
            .expect("internal nemesis error: no outstanding snapshots to read");
        state.outstanding_snaps.remove(&snap);
        Req::ReadSnapshot(ReadSnapshotReq { snap })
    }

    fn gen(&self, rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        match self {
            ReqGenerator::WriteUnsealed => {
                Req::Write(WriteReq::Single(ReqGenerator::write_unsealed(rng, state)))
            }
            ReqGenerator::WriteSealed => ReqGenerator::write_sealed(rng, state),
            ReqGenerator::WriteMulti => ReqGenerator::write_multi(rng, state),
            ReqGenerator::ReadOutput => ReqGenerator::read_output(rng, state),
            ReqGenerator::Seal => ReqGenerator::seal(rng, state),
            ReqGenerator::AllowCompaction => ReqGenerator::allow_compaction(rng, state),
            ReqGenerator::TakeSnapshot => ReqGenerator::take_snapshot(rng, state),
            ReqGenerator::ReadSnapshot => ReqGenerator::read_snapshot(rng, state),
            ReqGenerator::Start => {
                state.running = state.storage_available;
                Req::Start
            }
            ReqGenerator::Stop => {
                state.running = false;
                Req::Stop
            }
            ReqGenerator::StorageUnavailable => {
                state.storage_available = false;
                Req::StorageUnavailable
            }
            ReqGenerator::StorageAvailable => {
                state.storage_available = true;
                Req::StorageAvailable
            }
        }
    }
}

#[derive(Debug)]
pub struct Generator {
    seed: u64,
    config: GeneratorConfig,
    req_id: u64,
    state: GeneratorState,
}

impl Generator {
    pub fn new(seed: u64, config: GeneratorConfig) -> Self {
        Generator {
            seed,
            config,
            req_id: 0,
            state: GeneratorState::default(),
        }
    }

    // fill_relevant appends a `(weight, ReqGenerator)` pair for each relevant
    // request type.
    //
    // Examples of when requests are only sometimes relevant:
    // - WriteSealed can only apply if at least one stream has been sealed to a
    //   timestamp > 0.
    // - ReadSnapshot can only apply if we have previously taken a snapshot but
    //   not consumed (read) it yet.
    fn fill_relevant(&mut self, relevant: &mut Vec<(u32, ReqGenerator)>) {
        let only_when_running = vec![
            Some((
                self.config.write_unsealed_weight,
                ReqGenerator::WriteUnsealed,
            )),
            Some((self.config.write_sealed_weight, ReqGenerator::WriteSealed))
                .filter(|_| self.state.seal_frontier.iter().any(|(_, ts)| *ts > 0)),
            Some((self.config.write_multi_weight, ReqGenerator::WriteMulti)),
            Some((self.config.read_output_weight, ReqGenerator::ReadOutput)),
            Some((self.config.seal_weight, ReqGenerator::Seal)),
            Some((
                self.config.allow_compaction_weight,
                ReqGenerator::AllowCompaction,
            )),
            Some((self.config.take_snapshot_weight, ReqGenerator::TakeSnapshot)),
            Some((self.config.read_snapshot_weight, ReqGenerator::ReadSnapshot))
                .filter(|_| !self.state.outstanding_snaps.is_empty()),
            Some((self.config.stop_weight, ReqGenerator::Stop)).filter(|_| self.state.running),
            Some((
                self.config.storage_unavailable,
                ReqGenerator::StorageUnavailable,
            ))
            .filter(|_| self.state.storage_available),
        ];
        let also_when_not_running = vec![
            Some((
                self.config.storage_available,
                ReqGenerator::StorageAvailable,
            ))
            .filter(|_| self.state.storage_available == false),
            Some((self.config.start_weight, ReqGenerator::Start))
                .filter(|_| self.state.running == false),
        ];

        // Most operations just error if the runtime is down and this is fairly
        // uninteresting to test. Maximize our time spent by focusing on
        // operations that help get the runtime started (Start,
        // StorageAvailable) if it's not.
        if self.state.running {
            relevant.extend(only_when_running.into_iter().flatten());
        }
        relevant.extend(also_when_not_running.into_iter().flatten());
    }

    fn gen(&mut self) -> Option<Input> {
        let req_id = ReqId(self.req_id);
        self.req_id += 1;
        let mut rng = SmallRng::seed_from_u64(self.seed + req_id.0);

        let mut relevant = Vec::new();
        self.fill_relevant(&mut relevant);
        if let Ok((_, req)) = relevant.choose_weighted(&mut rng, |(w, _)| *w) {
            let req = req.gen(&mut rng, &mut self.state);
            Some(Input { req_id, req })
        } else {
            None
        }
    }
}

impl Iterator for Generator {
    type Item = Input;

    fn next(&mut self) -> Option<Self::Item> {
        self.gen()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::{self, Display};
    use std::{cmp, error};

    use serde::ser::{
        self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    };
    use serde::Serializer;

    use super::*;

    // Sanity check that we don't generate uninteresting traffic (like writes)
    // while the runtime isn't up. The only thing we should generate when the
    // runtime is down is making storage available and starting.
    #[test]
    fn operations_while_runtime_down() {
        let (seed, config) = (0, GeneratorConfig::all_operations());
        let mut g = Generator::new(seed, config);
        for _ in 0..1000 {
            g.state.running = false;
            let input = g.gen().expect("some input should always be available");
            match input.req {
                Req::Start | Req::StorageAvailable => {}
                r => panic!("unexpected req type: {:?}", r),
            }
        }
    }

    // Regression test for a bug where sealed writes would be generated even
    // when disabled.
    #[test]
    fn seal_write_disabled() {
        let (seed, mut config) = (0, GeneratorConfig::all_operations());
        config.write_sealed_weight = 0;
        let mut g = Generator::new(seed, config);
        let mut counter = ReqCounter::default();
        for _ in 0..1000 {
            let input = g.gen().expect("some input should always be available");
            counter.count(&input.req);
        }
        assert_eq!(counter.counts().write_sealed_weight, 0);
    }

    // Generate random inputs until we've seen each type at least N times. This
    // both verifies that the config returned by `all_operations()` in fact
    // contains all operations as well as verifies that Generator actually
    // generates all of these operation types.
    #[test]
    fn all_operations() {
        const MIN_EACH_TYPE: u32 = 5;
        let (seed, config) = (0, GeneratorConfig::all_operations());
        let mut g = Generator::new(seed, config);
        let mut counter = ReqCounter::default();
        while !for_all_u32_fields(counter.counts(), |x| x >= MIN_EACH_TYPE) {
            let input = g.gen().expect("some input should always be available");
            counter.count(&input.req)
        }
    }

    struct ReqCounter {
        counts: GeneratorConfig,
        closed_by_stream: HashMap<String, u64>,
    }

    impl Default for ReqCounter {
        fn default() -> Self {
            let counts = GeneratorConfig {
                write_unsealed_weight: 0,
                write_sealed_weight: 0,
                write_multi_weight: 0,
                read_output_weight: 0,
                seal_weight: 0,
                allow_compaction_weight: 0,
                take_snapshot_weight: 0,
                read_snapshot_weight: 0,
                start_weight: 0,
                stop_weight: 0,
                storage_unavailable: 0,
                storage_available: 0,
            };
            ReqCounter {
                counts,
                closed_by_stream: HashMap::new(),
            }
        }
    }

    impl ReqCounter {
        fn counts(&self) -> &GeneratorConfig {
            &self.counts
        }

        fn count(&mut self, req: &Req) {
            match req {
                Req::Write(WriteReq::Single(r)) => {
                    if r.update.1
                        >= self
                            .closed_by_stream
                            .get(&r.stream)
                            .copied()
                            .unwrap_or_default()
                    {
                        self.counts.write_unsealed_weight += 1;
                    } else {
                        self.counts.write_sealed_weight += 1;
                    }
                }
                Req::Write(WriteReq::Multi(_)) => {
                    self.counts.write_multi_weight += 1;
                }
                Req::ReadOutput(_) => {
                    self.counts.read_output_weight += 1;
                }
                Req::Seal(r) => {
                    let ts = cmp::max(
                        r.ts,
                        self.closed_by_stream
                            .get(&r.stream)
                            .copied()
                            .unwrap_or_default(),
                    );
                    self.closed_by_stream.insert(r.stream.clone(), ts);
                    self.counts.seal_weight += 1;
                }
                Req::AllowCompaction(_) => {
                    self.counts.allow_compaction_weight += 1;
                }
                Req::TakeSnapshot(_) => {
                    self.counts.take_snapshot_weight += 1;
                }
                Req::ReadSnapshot(_) => {
                    self.counts.read_snapshot_weight += 1;
                }
                Req::Start => {
                    self.counts.start_weight += 1;
                }
                Req::Stop => {
                    self.counts.stop_weight += 1;
                }
                Req::StorageUnavailable => {
                    self.counts.storage_unavailable += 1;
                }
                Req::StorageAvailable => {
                    self.counts.storage_available += 1;
                }
            }
        }
    }

    // Returns true if the given closure returns true for *all* u32 fields.
    //
    // Recurses into struct fields, verifying the closure on all u32 fields on
    // them as well. Does not recurse into seqs, maps, tuples, newtypes, or
    // options.
    //
    // Implemented internally with a custom serde::Serializer. This feels a bit
    // like overkill, but the equivalent of this `all_operations` test has saved
    // all kinds of headaches in a previous nemesis test.
    fn for_all_u32_fields<S: Serialize, F: Fn(u32) -> bool>(s: S, f: F) -> bool {
        let mut x = ForAllU32Fields(true, f);
        s.serialize(&mut x).expect("ForAllU32Fields never errors")
    }

    #[test]
    fn for_all_u32_fields_test() {
        let s = ForAllU32FieldsTest::default();
        assert_eq!(for_all_u32_fields(&s, |x| x == 0), true);

        let s = ForAllU32FieldsTest {
            f1: 1,
            ..Default::default()
        };
        assert_eq!(for_all_u32_fields(&s, |x| x > 0), false);
        let s = ForAllU32FieldsTest {
            f2: ForAllU32FieldsInner { i: 1 },
            ..Default::default()
        };
        assert_eq!(for_all_u32_fields(&s, |x| x > 0), false);
        let s = ForAllU32FieldsTest {
            f3: 1,
            ..Default::default()
        };
        assert_eq!(for_all_u32_fields(&s, |x| x > 0), false);
        let s = ForAllU32FieldsTest {
            f4: ForAllU32FieldsInner { i: 1 },
            ..Default::default()
        };
        assert_eq!(for_all_u32_fields(&s, |x| x > 0), false);
        let s = ForAllU32FieldsTest {
            f5: 1,
            ..Default::default()
        };
        assert_eq!(for_all_u32_fields(&s, |x| x > 0), false);
    }

    #[derive(Debug, Default, Serialize)]
    struct ForAllU32FieldsTest {
        f1: u32,
        f2: ForAllU32FieldsInner,
        f3: u32,
        f4: ForAllU32FieldsInner,
        f5: u32,
    }

    #[derive(Debug, Default, Serialize)]
    struct ForAllU32FieldsInner {
        i: u32,
    }

    struct ForAllU32Fields<F: Fn(u32) -> bool>(bool, F);

    #[derive(Debug)]
    struct ForAllU32FieldsError;

    impl ser::Error for ForAllU32FieldsError {
        fn custom<T>(_: T) -> Self
        where
            T: Display,
        {
            ForAllU32FieldsError
        }
    }

    impl error::Error for ForAllU32FieldsError {}

    impl fmt::Display for ForAllU32FieldsError {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            Ok(())
        }
    }

    impl<F: Fn(u32) -> bool> Serializer for &mut ForAllU32Fields<F> {
        type Ok = bool;

        type Error = ForAllU32FieldsError;

        type SerializeSeq = Self;

        type SerializeTuple = Self;

        type SerializeTupleStruct = Self;

        type SerializeTupleVariant = Self;

        type SerializeMap = Self;

        type SerializeStruct = Self;

        type SerializeStructVariant = Self;

        fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
            Ok(self.0 && (self.1)(v))
        }

        fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
        where
            T: Serialize,
        {
            Ok(self.0)
        }

        fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }

        fn serialize_newtype_struct<T: ?Sized>(
            self,
            _name: &'static str,
            _value: &T,
        ) -> Result<Self::Ok, Self::Error>
        where
            T: Serialize,
        {
            Ok(self.0)
        }

        fn serialize_newtype_variant<T: ?Sized>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _value: &T,
        ) -> Result<Self::Ok, Self::Error>
        where
            T: Serialize,
        {
            Ok(self.0)
        }

        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
            Ok(self)
        }

        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
            Ok(self)
        }

        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct, Self::Error> {
            Ok(self)
        }

        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant, Self::Error> {
            Ok(self)
        }

        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
            Ok(self)
        }

        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct, Self::Error> {
            Ok(self)
        }

        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant, Self::Error> {
            Ok(self)
        }
    }

    impl<F: Fn(u32) -> bool> SerializeSeq for &mut ForAllU32Fields<F> {
        type Ok = bool;

        type Error = ForAllU32FieldsError;

        fn serialize_element<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
        where
            T: Serialize,
        {
            Ok(())
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }
    }

    impl<F: Fn(u32) -> bool> SerializeTuple for &mut ForAllU32Fields<F> {
        type Ok = bool;

        type Error = ForAllU32FieldsError;

        fn serialize_element<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
        where
            T: Serialize,
        {
            Ok(())
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }
    }

    impl<F: Fn(u32) -> bool> SerializeTupleStruct for &mut ForAllU32Fields<F> {
        type Ok = bool;

        type Error = ForAllU32FieldsError;

        fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
        where
            T: Serialize,
        {
            Ok(())
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }
    }

    impl<F: Fn(u32) -> bool> SerializeTupleVariant for &mut ForAllU32Fields<F> {
        type Ok = bool;

        type Error = ForAllU32FieldsError;

        fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
        where
            T: Serialize,
        {
            Ok(())
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }
    }

    impl<F: Fn(u32) -> bool> SerializeMap for &mut ForAllU32Fields<F> {
        type Ok = bool;

        type Error = ForAllU32FieldsError;

        fn serialize_key<T: ?Sized>(&mut self, _key: &T) -> Result<(), Self::Error>
        where
            T: Serialize,
        {
            Ok(())
        }

        fn serialize_value<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
        where
            T: Serialize,
        {
            Ok(())
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }
    }

    impl<F: Fn(u32) -> bool> SerializeStruct for &mut ForAllU32Fields<F> {
        type Ok = bool;

        type Error = ForAllU32FieldsError;

        fn serialize_field<T: ?Sized>(
            &mut self,
            _key: &'static str,
            value: &T,
        ) -> Result<(), Self::Error>
        where
            T: Serialize,
        {
            self.0 = self.0 && value.serialize(&mut **self)?;
            Ok(())
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }
    }

    impl<F: Fn(u32) -> bool> SerializeStructVariant for &mut ForAllU32Fields<F> {
        type Ok = bool;

        type Error = ForAllU32FieldsError;

        fn serialize_field<T: ?Sized>(
            &mut self,
            _key: &'static str,
            _value: &T,
        ) -> Result<(), Self::Error>
        where
            T: Serialize,
        {
            Ok(())
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(self.0)
        }
    }
}
