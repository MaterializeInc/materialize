// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `RelationDesc` proto round-trips losslessly.
//! `RelationDesc` is the schema of every persisted collection, so a decoder
//! bug here corrupts catalog/persist state.
//!
//! Two arms (the first byte selects):
//!  - Arbitrary arm: drive `RelationDesc`'s proptest `Arbitrary` strategy from
//!    the fuzzer bytes to build a *valid* desc — column names paired with
//!    deeply-nested `SqlColumnType`s, where the names<->metadata length
//!    invariant and the per-version migration metadata are well-formed. Assert
//!    `from_proto(into_proto(v)) == v`. Random proto bytes never satisfy the
//!    length invariant, so the encoder's rollup/migration-default paths are
//!    only reached here.
//!  - Raw-bytes arm: decode arbitrary bytes as `ProtoRelationDesc`, into Rust,
//!    and re-encode; keeps coverage of the bare wire decoder against hostile
//!    input (including descs that violate the length invariant on decode).

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, RustType};
use mz_repr::{ProtoRelationDesc, RelationDesc};
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
    let value = match <RelationDesc as proptest::arbitrary::Arbitrary>::arbitrary()
        .new_tree(&mut runner)
    {
        Ok(tree) => tree.current(),
        Err(_) => return,
    };

    let proto = value.into_proto();
    let back = RelationDesc::from_proto(proto).expect("valid RelationDesc must round-trip");
    assert_eq!(value, back, "RelationDesc changed across proto roundtrip");
}

fn raw_arm(data: &[u8]) {
    let Ok(proto) = ProtoRelationDesc::decode(data) else {
        return;
    };
    let orig: RelationDesc = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };

    let proto2 = <ProtoRelationDesc as ProtoType<RelationDesc>>::from_rust(&orig);
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoRelationDesc::decode(bytes2.as_slice())
        .expect("re-encode of valid RelationDesc must decode");
    let round: RelationDesc = proto3
        .into_rust()
        .expect("re-encoded RelationDesc must convert back to Rust");

    assert_eq!(orig, round, "RelationDesc changed across proto roundtrip");
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
