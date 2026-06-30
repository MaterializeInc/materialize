// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `SqlScalarType` proto round-trips losslessly.
//! `SqlScalarType` describes the type of every column in every relation, so
//! any decoder bug here propagates through the type system.
//!
//! Two arms (the first byte selects):
//!  - Arbitrary arm: drive `SqlScalarType`'s proptest `Arbitrary` strategy from
//!    the fuzzer bytes to build a *valid, deeply-nested* type (the recursive
//!    `List`/`Map`/`Array`/`Record`/`Range` variants, boundary `max_scale` and
//!    char/varchar `length`, custom OIDs, etc.) and assert
//!    `from_proto(into_proto(v)) == v`. Random proto bytes almost never reach
//!    these variants, so this arm is what actually exercises the encoder.
//!  - Raw-bytes arm: decode arbitrary bytes as `ProtoScalarType`, into Rust, and
//!    re-encode, keeping coverage of the bare wire decoder against hostile
//!    input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, RustType};
use mz_repr::{ProtoScalarType, SqlScalarType};
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
    let value = match <SqlScalarType as proptest::arbitrary::Arbitrary>::arbitrary()
        .new_tree(&mut runner)
    {
        Ok(tree) => tree.current(),
        Err(_) => return,
    };

    let proto = value.into_proto();
    let back = SqlScalarType::from_proto(proto).expect("valid SqlScalarType must round-trip");
    assert_eq!(value, back, "SqlScalarType changed across proto roundtrip");
}

fn raw_arm(data: &[u8]) {
    let Ok(proto) = ProtoScalarType::decode(data) else {
        return;
    };
    let orig: SqlScalarType = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };

    let proto2 = <ProtoScalarType as ProtoType<SqlScalarType>>::from_rust(&orig);
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoScalarType::decode(bytes2.as_slice())
        .expect("re-encode of valid SqlScalarType must decode");
    let round: SqlScalarType = proto3
        .into_rust()
        .expect("re-encoded SqlScalarType must convert back to Rust");

    assert_eq!(orig, round, "SqlScalarType changed across proto roundtrip");
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
