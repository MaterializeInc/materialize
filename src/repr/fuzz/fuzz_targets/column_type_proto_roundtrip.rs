// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `SqlColumnType` proto round-trips losslessly.
//!
//! Two arms (the first byte selects):
//!  - Arbitrary arm: drive `SqlColumnType`'s proptest `Arbitrary` strategy from
//!    the fuzzer bytes to build a *valid* column type pairing a deeply-nested
//!    `SqlScalarType` with a nullable flag, and assert
//!    `from_proto(into_proto(v)) == v`. Random proto bytes leave the inner
//!    scalar type near-empty; this arm reaches the recursive scalar variants.
//!  - Raw-bytes arm: decode arbitrary bytes as `ProtoColumnType`, into Rust, and
//!    re-encode; keeps coverage of the bare wire decoder against hostile input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, RustType};
use mz_repr::{ProtoColumnType, SqlColumnType};
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
    let value = match <SqlColumnType as proptest::arbitrary::Arbitrary>::arbitrary()
        .new_tree(&mut runner)
    {
        Ok(tree) => tree.current(),
        Err(_) => return,
    };

    let proto = value.into_proto();
    let back = SqlColumnType::from_proto(proto).expect("valid SqlColumnType must round-trip");
    assert_eq!(value, back, "SqlColumnType changed across proto roundtrip");
}

fn raw_arm(data: &[u8]) {
    let Ok(proto) = ProtoColumnType::decode(data) else {
        return;
    };
    let orig: SqlColumnType = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };

    let proto2 = <ProtoColumnType as ProtoType<SqlColumnType>>::from_rust(&orig);
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoColumnType::decode(bytes2.as_slice())
        .expect("re-encode of valid SqlColumnType must decode");
    let round: SqlColumnType = proto3
        .into_rust()
        .expect("re-encoded SqlColumnType must convert back to Rust");

    assert_eq!(orig, round, "SqlColumnType changed across proto roundtrip");
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
