// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `Interval` proto round-trips losslessly.
//!
//! Two arms (the first byte selects):
//!  - Arbitrary arm: drive `Interval`'s proptest `Arbitrary` strategy from the
//!    fuzzer bytes to build a *valid* interval (months/days/micros across the
//!    full range) and assert `from_proto(into_proto(v)) == v`.
//!  - Raw-bytes arm: decode arbitrary bytes as `ProtoInterval`, into Rust, and
//!    re-encode, keeping coverage of the bare wire decoder against hostile
//!    input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, RustType};
use mz_repr::adt::interval::{Interval, ProtoInterval};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use prost::Message;

fn arbitrary_arm(seed: &[u8]) {
    let mut buf = [0u8; 32];
    for (dst, src) in buf.iter_mut().zip(seed.iter()) {
        *dst = *src;
    }
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &buf);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    let value =
        match <Interval as proptest::arbitrary::Arbitrary>::arbitrary().new_tree(&mut runner) {
            Ok(tree) => tree.current(),
            Err(_) => return,
        };

    let proto = value.into_proto();
    let back = Interval::from_proto(proto).expect("valid Interval must round-trip");
    assert_eq!(value, back, "Interval changed across proto roundtrip");
}

fn raw_arm(data: &[u8]) {
    let Ok(proto) = ProtoInterval::decode(data) else {
        return;
    };
    let orig: Interval = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };

    let proto2 = <ProtoInterval as ProtoType<Interval>>::from_rust(&orig);
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoInterval::decode(bytes2.as_slice())
        .expect("re-encode of valid Interval must decode");
    let round: Interval = proto3
        .into_rust()
        .expect("re-encoded Interval must convert back to Rust");

    assert_eq!(orig, round, "Interval changed across proto roundtrip");
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    if mode & 1 == 0 {
        arbitrary_arm(rest);
    } else {
        raw_arm(rest);
    }
});
