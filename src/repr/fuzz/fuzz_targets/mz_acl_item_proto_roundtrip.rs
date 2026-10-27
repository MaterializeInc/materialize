// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `MzAclItem` proto round-trips losslessly.
//! ACL items are access-control data. A decoder bug here is a security
//! concern (a malformed proto could decode into a value the rest of the
//! catalog doesn't expect).
//!
//! Two arms (the first byte selects):
//!  - Arbitrary arm: drive `MzAclItem`'s proptest `Arbitrary` strategy from the
//!    fuzzer bytes to build a *valid* entry (grantee/grantor `RoleId`s across
//!    all variants + an arbitrary `AclMode` bit flag) and assert
//!    `from_proto(into_proto(v)) == v`.
//!  - Raw-bytes arm: decode arbitrary bytes as `ProtoMzAclItem`, into Rust, and
//!    re-encode, keeping coverage of the bare wire decoder against hostile
//!    input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, RustType};
use mz_repr::adt::mz_acl_item::{MzAclItem, ProtoMzAclItem};
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
        match <MzAclItem as proptest::arbitrary::Arbitrary>::arbitrary().new_tree(&mut runner) {
            Ok(tree) => tree.current(),
            Err(_) => return,
        };

    let proto = value.into_proto();
    let back = MzAclItem::from_proto(proto).expect("valid MzAclItem must round-trip");
    assert_eq!(value, back, "MzAclItem changed across proto roundtrip");
}

fn raw_arm(data: &[u8]) {
    let Ok(proto) = ProtoMzAclItem::decode(data) else {
        return;
    };
    let orig: MzAclItem = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };

    let proto2 = <ProtoMzAclItem as ProtoType<MzAclItem>>::from_rust(&orig);
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoMzAclItem::decode(bytes2.as_slice())
        .expect("re-encode of valid MzAclItem must decode");
    let round: MzAclItem = proto3
        .into_rust()
        .expect("re-encoded MzAclItem must convert back to Rust");

    assert_eq!(orig, round, "MzAclItem changed across proto roundtrip");
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
