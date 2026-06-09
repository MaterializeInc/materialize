// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: every byte sequence that decodes as `ProtoRow` and then into
//! a Rust `Row` must survive a proto re-encode + re-decode with the same
//! value. `Row` is the core value representation in Materialize and the
//! proto wire format is part of the persist on-disk format, so any decoder
//! laxity that doesn't round-trip is a real correctness risk.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::ProtoType;
use mz_repr::{ProtoRow, Row};
use prost::Message;

fuzz_target!(|data: &[u8]| {
    let Ok(proto) = ProtoRow::decode(data) else {
        return;
    };
    let orig: Row = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };

    let proto2 = <ProtoRow as ProtoType<Row>>::from_rust(&orig);
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoRow::decode(bytes2.as_slice())
        .expect("re-encode of valid Row must decode");
    let round: Row = proto3
        .into_rust()
        .expect("re-encoded Row must convert back to Rust");

    assert_eq!(orig, round, "Row changed across proto roundtrip");
});
