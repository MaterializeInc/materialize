// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use rand::prelude::{IteratorRandom, SliceRandom, SmallRng};
use rand::{Rng, SeedableRng};
use serde::Serialize;

use crate::nemesis::{
    AllowCompactionReq, Input, ReadSnapshotReq, Req, ReqId, SealReq, SnapshotId, TakeSnapshotReq,
    WriteReq,
};

/// Configuration of the relative probabilities of producing various
// operations.
#[derive(Clone, Debug, Serialize)]
pub struct GeneratorConfig {
    pub write_unsealed_weight: u32,
    pub write_sealed_weight: u32,
    pub seal_weight: u32,
    pub allow_compaction_weight: u32,
    pub take_snapshot_weight: u32,
    pub read_snapshot_weight: u32,
    pub restart_weight: u32,
    pub storage_unavailable: u32,
    pub storage_available: u32,
}

impl GeneratorConfig {
    fn all_operations() -> GeneratorConfig {
        GeneratorConfig {
            write_unsealed_weight: 1,
            write_sealed_weight: 1,
            seal_weight: 1,
            allow_compaction_weight: 1,
            take_snapshot_weight: 1,
            read_snapshot_weight: 1,
            restart_weight: 1,
            // WIP: TODO: logical seal doesn't interact well with storage unavailability.
            // Disable for now.
            storage_unavailable: 0,
            storage_available: 0,
        }
    }
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self::all_operations()
    }
}

struct GeneratorState {
    seal_frontier: HashMap<String, u64>,
    since_frontier: HashMap<String, u64>,
    snap_id: SnapshotId,
    outstanding_snaps: HashMap<SnapshotId, String>,
    storage_available: bool,
}

impl Default for GeneratorState {
    fn default() -> Self {
        GeneratorState {
            seal_frontier: HashMap::new(),
            since_frontier: HashMap::new(),
            snap_id: SnapshotId(0),
            outstanding_snaps: HashMap::new(),
            storage_available: true,
        }
    }
}

enum ReqGenerator {
    WriteUnsealed,
    WriteSealed,
    Seal,
    AllowCompaction,
    TakeSnapshot,
    ReadSnapshot,
    Restart,
    StorageUnavailable,
    StorageAvailable,
}

fn rng_stream(rng: &mut SmallRng) -> String {
    // TODO: Compute this from some WeightedIndex with configurable weights
    // defaulting to zipfian.
    rng.gen_range('a'..'e').to_string()
}

fn rng_key(rng: &mut SmallRng) -> String {
    // TODO: Compute this from some WeightedIndex with configurable weights
    // defaulting to zipfian.
    rng.gen_range('a'..'e').to_string()
}

impl ReqGenerator {
    fn write_unsealed(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let stream = rng_stream(rng);
        let key = rng_key(rng);
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
        Req::Write(WriteReq {
            stream,
            update: ((key, ()), ts, diff),
        })
    }

    fn write_sealed(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let key = rng_key(rng);
        let (stream, ts) = state
            .seal_frontier
            .iter()
            .filter(|(_, ts)| **ts > 0)
            .choose(rng)
            .map(|(stream, ts)| (stream.clone(), rng.gen_range(0..*ts)))
            .expect("internal nemesis error: no streams has a sealed ts > 0");
        // This is expected to error so it doesn't matter what diff is.
        let diff = 1;
        Req::Write(WriteReq {
            stream: stream,
            update: ((key, ()), ts, diff),
        })
    }

    fn seal(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let stream = rng_stream(rng);
        let stream_sealed_ts = state
            .seal_frontier
            .get(&stream)
            .copied()
            .unwrap_or_default();
        let ts = stream_sealed_ts + rng.gen_range(0..5);
        Req::Seal(SealReq { stream, ts })
    }

    fn allow_compaction(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let stream = rng_stream(rng);
        let ts = {
            let base = if rng.gen_bool(0.5) {
                state
                    .since_frontier
                    .get(&stream)
                    .copied()
                    .unwrap_or_default()
            } else {
                state
                    .seal_frontier
                    .get(&stream)
                    .copied()
                    .unwrap_or_default()
            };

            base + rng.gen_range(0..5)
        };
        Req::AllowCompaction(AllowCompactionReq { stream, ts })
    }

    fn take_snapshot(rng: &mut SmallRng, state: &mut GeneratorState) -> Req {
        let stream = rng_stream(rng);
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
            ReqGenerator::WriteUnsealed => ReqGenerator::write_unsealed(rng, state),
            ReqGenerator::WriteSealed => ReqGenerator::write_sealed(rng, state),
            ReqGenerator::Seal => ReqGenerator::seal(rng, state),
            ReqGenerator::AllowCompaction => ReqGenerator::allow_compaction(rng, state),
            ReqGenerator::TakeSnapshot => ReqGenerator::take_snapshot(rng, state),
            ReqGenerator::ReadSnapshot => ReqGenerator::read_snapshot(rng, state),
            ReqGenerator::Restart => Req::Restart,
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
        let req_weights = [
            (
                self.config.write_unsealed_weight,
                Some(ReqGenerator::WriteUnsealed),
            ),
            (
                self.config.write_sealed_weight,
                Some(ReqGenerator::WriteSealed)
                    .filter(|_| self.state.seal_frontier.iter().any(|(_, ts)| *ts > 0)),
            ),
            (self.config.seal_weight, Some(ReqGenerator::Seal)),
            (
                self.config.allow_compaction_weight,
                Some(ReqGenerator::AllowCompaction),
            ),
            (
                self.config.take_snapshot_weight,
                Some(ReqGenerator::TakeSnapshot),
            ),
            (
                self.config.read_snapshot_weight,
                Some(ReqGenerator::ReadSnapshot)
                    .filter(|_| !self.state.outstanding_snaps.is_empty()),
            ),
            (self.config.restart_weight, Some(ReqGenerator::Restart)),
            (
                self.config.storage_unavailable,
                Some(ReqGenerator::StorageUnavailable).filter(|_| self.state.storage_available),
            ),
            (
                self.config.storage_available,
                Some(ReqGenerator::StorageAvailable)
                    .filter(|_| self.state.storage_available == false),
            ),
        ];
        for (weight, req) in req_weights {
            if let Some(req) = req {
                relevant.push((weight, req));
            }
        }
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

    // Generate random inputs until we've seen each type at least N times. This
    // both verifies that the config returned by `all_operations()` in fact
    // contains all operations as well as verifies that Generator actually
    // generates all of these operation types.
    #[test]
    fn all_operations() {
        let mut closed_by_stream = HashMap::new();
        let mut count_input = move |input: Input, counts: &mut GeneratorConfig| match input.req {
            Req::Write(r) => {
                if r.update.1 >= closed_by_stream.get(&r.stream).copied().unwrap_or_default() {
                    counts.write_unsealed_weight += 1;
                } else {
                    counts.write_sealed_weight += 1;
                }
            }
            Req::Seal(r) => {
                let ts = cmp::max(
                    r.ts,
                    closed_by_stream.get(&r.stream).copied().unwrap_or_default(),
                );
                closed_by_stream.insert(r.stream, ts);
                counts.seal_weight += 1;
            }
            Req::AllowCompaction(_) => {
                counts.allow_compaction_weight += 1;
            }
            Req::TakeSnapshot(_) => {
                counts.take_snapshot_weight += 1;
            }
            Req::ReadSnapshot(_) => {
                counts.read_snapshot_weight += 1;
            }
            Req::Restart => {
                counts.restart_weight += 1;
            }
            Req::StorageUnavailable => {
                counts.storage_unavailable += 1;
            }
            Req::StorageAvailable => {
                counts.storage_available += 1;
            }
        };

        let mut counts = GeneratorConfig {
            write_unsealed_weight: 0,
            write_sealed_weight: 0,
            seal_weight: 0,
            allow_compaction_weight: 0,
            take_snapshot_weight: 0,
            read_snapshot_weight: 0,
            restart_weight: 0,
            // WIP: TODO: re-enable.
            storage_unavailable: 5,
            storage_available: 5,
        };

        const MIN_EACH_TYPE: u32 = 5;
        let (seed, config) = (0, GeneratorConfig::all_operations());
        let mut g = Generator::new(seed, config);
        while !for_all_u32_fields(&counts, |x| x >= MIN_EACH_TYPE) {
            let input = g.gen().expect("some input should always be available");
            count_input(input, &mut counts);
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
