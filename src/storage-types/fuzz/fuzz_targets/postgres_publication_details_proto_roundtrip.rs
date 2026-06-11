// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: a `PostgresSourcePublicationDetails` must survive a proto
//! encode + decode round trip losslessly. These descriptors travel between the
//! storage controller and clusterd and persist replication state, so a decoder
//! bug here is reachable via the storage RPC and on-disk format.
//!
//! The Rust type is near-trivial (two `String`s plus an `Option<u64>`), so
//! rather than wiring up proptest we synthesize a valid value directly from the
//! input bytes. Two complementary input arms (the first byte picks):
//!
//!  * **Structured arm** — splits the remaining bytes into the `slot` and
//!    `database` strings (lossy UTF-8 so arbitrary bytes always yield a valid
//!    `String`) and derives `timeline_id` from a length byte, then asserts the
//!    full `Rust -> Proto -> Rust` round trip is the identity. This guarantees
//!    non-empty fields and a populated `Option`, which random proto bytes rarely
//!    produce.
//!  * **Raw-bytes arm** — keeps the original behavior of decoding arbitrary
//!    bytes straight into the proto, exercising the decoder against
//!    malformed/adversarial wire input, then re-encoding the recovered value.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::ProtoType;
use mz_storage_types::sources::postgres::{
    PostgresSourcePublicationDetails, ProtoPostgresSourcePublicationDetails,
};
use prost::Message;

/// `Rust -> Proto -> Rust` must be the identity for any valid value.
fn assert_roundtrip(orig: PostgresSourcePublicationDetails) {
    let proto = <ProtoPostgresSourcePublicationDetails as ProtoType<
        PostgresSourcePublicationDetails,
    >>::from_rust(&orig);
    let bytes = proto.encode_to_vec();
    let proto2 = ProtoPostgresSourcePublicationDetails::decode(bytes.as_slice())
        .expect("re-encode of valid PostgresSourcePublicationDetails must decode");
    let round: PostgresSourcePublicationDetails = proto2
        .into_rust()
        .expect("re-encoded PostgresSourcePublicationDetails must convert back to Rust");
    assert_eq!(
        orig, round,
        "PostgresSourcePublicationDetails changed across proto roundtrip"
    );
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };

    if mode & 1 == 0 {
        // Structured arm: build a valid value directly from the bytes.
        let mid = rest.len() / 2;
        let (slot_bytes, db_bytes) = rest.split_at(mid);
        let timeline_id = if mode & 2 == 0 {
            None
        } else {
            // Pack up to 8 bytes of the input into the u64 timeline id.
            let mut buf = [0u8; 8];
            let n = rest.len().min(8);
            buf[..n].copy_from_slice(&rest[..n]);
            Some(u64::from_le_bytes(buf))
        };
        assert_roundtrip(PostgresSourcePublicationDetails {
            slot: String::from_utf8_lossy(slot_bytes).into_owned(),
            timeline_id,
            database: String::from_utf8_lossy(db_bytes).into_owned(),
        });
    } else {
        // Raw-bytes arm: decode adversarial wire bytes, then round-trip.
        let Ok(proto) = ProtoPostgresSourcePublicationDetails::decode(rest) else {
            return;
        };
        let orig: PostgresSourcePublicationDetails = match proto.into_rust() {
            Ok(v) => v,
            Err(_) => return,
        };
        assert_roundtrip(orig);
    }
});
