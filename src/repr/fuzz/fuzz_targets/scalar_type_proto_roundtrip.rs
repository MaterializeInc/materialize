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
//!    the fuzzer bytes to build a *valid* type (boundary `max_scale` and
//!    char/varchar `length`, custom OIDs, the recursive `List`/`Map`/`Record`
//!    variants up to the strategy's depth cap of 2) and assert it survives an
//!    encode/decode through the wire codec. Valid values are where a proto3
//!    presence bug shows up: a default-valued field that silently stops being
//!    written to the wire still passes an in-memory `RustType` round-trip.
//!  - Raw-bytes arm: decode arbitrary bytes as `ProtoScalarType`, into Rust, and
//!    re-encode, keeping coverage of the bare wire decoder against hostile
//!    input. `prost` is built with `no-recursion-limit`, so this is also the arm
//!    that reaches deep nesting, far past what the strategy above produces.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, protobuf_roundtrip};
use mz_repr::{ProtoScalarType, SqlScalarType};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use prost::Message;

fn arbitrary_arm(seed: &[u8]) {
    let mut buf = [0u8; 32];
    for (dst, src) in buf.iter_mut().zip(seed.iter()) {
        *dst = *src;
    }
    // NOTE: hashing the fuzzer bytes into a ChaCha seed costs libFuzzer its
    // mutation gradient, one flipped bit re-rolls the whole value. The obvious
    // fix, `RngAlgorithm::PassThrough`, hangs: it feeds the fuzzer bytes in as
    // the random stream and then yields zeros forever once they run out, and
    // `rand`'s Lemire sampler loops until a draw clears `thresh`, which a zero
    // never does for a range that is not a power of two. Every strategy here
    // outdraws a 4096-byte input.
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &buf);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    let value = match <SqlScalarType as proptest::arbitrary::Arbitrary>::arbitrary()
        .new_tree(&mut runner)
    {
        Ok(tree) => tree.current(),
        Err(_) => return,
    };

    let back = protobuf_roundtrip::<_, ProtoScalarType>(&value)
        .expect("valid SqlScalarType must round-trip");
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
