// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `AclItem` proto round-trips losslessly.
//! Distinct from `MzAclItem`: `AclItem` is the PostgreSQL-style ACL entry.
//!
//! Two arms (the first byte selects):
//!  - Arbitrary arm: drive `AclItem`'s proptest `Arbitrary` strategy from the
//!    fuzzer bytes to build a *valid* entry (grantee/grantor Oids + an arbitrary
//!    `AclMode` bit flag) and assert `from_proto(into_proto(v)) == v`.
//!  - Raw-bytes arm: decode arbitrary bytes as `ProtoAclItem`, into Rust, and
//!    re-encode, keeping coverage of the bare wire decoder against hostile
//!    input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, RustType};
use mz_repr::adt::mz_acl_item::{AclItem, ProtoAclItem};
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
        match <AclItem as proptest::arbitrary::Arbitrary>::arbitrary().new_tree(&mut runner) {
            Ok(tree) => tree.current(),
            Err(_) => return,
        };

    let proto = value.into_proto();
    let back = AclItem::from_proto(proto).expect("valid AclItem must round-trip");
    assert_eq!(value, back, "AclItem changed across proto roundtrip");
}

fn raw_arm(data: &[u8]) {
    let Ok(proto) = ProtoAclItem::decode(data) else {
        return;
    };
    let orig: AclItem = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };

    let proto2 = <ProtoAclItem as ProtoType<AclItem>>::from_rust(&orig);
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoAclItem::decode(bytes2.as_slice())
        .expect("re-encode of valid AclItem must decode");
    let round: AclItem = proto3
        .into_rust()
        .expect("re-encoded AclItem must convert back to Rust");

    assert_eq!(orig, round, "AclItem changed across proto roundtrip");
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
