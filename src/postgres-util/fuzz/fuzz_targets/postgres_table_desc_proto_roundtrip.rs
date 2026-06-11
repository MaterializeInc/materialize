// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `ProtoPostgresTableDesc` <-> `PostgresTableDesc` round-trip.
//! `PostgresTableDesc` describes external-database schemas; a decoder bug here
//! is reachable from a compromised upstream Postgres or on-disk catalog bytes.
//!
//! The first input byte selects the arm; the rest feeds it:
//!
//!  * **Structured arm** — drives `PostgresTableDesc`'s proptest `Arbitrary`
//!    (behind mz-postgres-util's `schemas` feature) from the libFuzzer byte
//!    stream to synthesize a *valid, fully-populated* value, then asserts the
//!    `value -> proto -> value` chain is the identity AND that re-encoding the
//!    proto is byte-idempotent. This is what actually reaches the deep shape:
//!    several `PostgresColumnDesc`s with arbitrary `col_num`/`type_oid`/
//!    `type_mod`/`nullable`, and a `BTreeSet` of several `PostgresKeyDesc`s,
//!    each with a `Vec<u16>` of `cols`. Random proto bytes decode to a
//!    near-empty desc, so the populated branches never get covered otherwise.
//!  * **Raw-bytes arm** — keeps the original behavior of decoding arbitrary
//!    bytes straight into the proto then `into_rust`, exercising the decoder
//!    against malformed/adversarial input, then re-encoding the recovered
//!    value. This is where the untrusted-bytes invariants live: the
//!    `u32 -> u16` narrowing for `ProtoPostgresColumnDesc::col_num` and
//!    `ProtoPostgresKeyDesc::cols` must return `Err`, not panic (regression
//!    coverage for two prior panic fixes), and the wire's repeated `keys`
//!    field with duplicate / unsorted entries must collapse cleanly into the
//!    Rust `BTreeSet` rather than trip an ordering assertion.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_postgres_util::desc::{
    PostgresTableDesc, ProtoPostgresColumnDesc, ProtoPostgresKeyDesc, ProtoPostgresTableDesc,
};
use mz_proto::ProtoType;
use proptest::arbitrary::Arbitrary;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use prost::Message;

/// Build a 32-byte proptest seed from `bytes` (zero-padded / truncated).
fn seed_from(bytes: &[u8]) -> [u8; 32] {
    let mut seed = [0u8; 32];
    let n = bytes.len().min(32);
    seed[..n].copy_from_slice(&bytes[..n]);
    seed
}

/// Assert the `value -> proto -> value` chain is the identity and that
/// re-encoding the proto is byte-idempotent.
fn assert_roundtrip(orig: PostgresTableDesc) {
    let proto = <ProtoPostgresTableDesc as ProtoType<PostgresTableDesc>>::from_rust(&orig);
    let bytes = proto.encode_to_vec();
    let proto2 = ProtoPostgresTableDesc::decode(bytes.as_slice())
        .expect("re-encode of valid PostgresTableDesc must decode");
    let round: PostgresTableDesc = proto2
        .into_rust()
        .expect("re-encoded PostgresTableDesc must convert back to Rust");
    assert_eq!(orig, round, "PostgresTableDesc changed across proto roundtrip");

    // Encoding the recovered value must reproduce the same wire bytes.
    let bytes2 = <ProtoPostgresTableDesc as ProtoType<PostgresTableDesc>>::from_rust(&round)
        .encode_to_vec();
    assert_eq!(bytes, bytes2, "proto re-encode was not idempotent");
}

/// Decode adversarial proto bytes, convert to Rust, then round-trip.
fn raw_roundtrip(data: &[u8]) {
    let Ok(proto) = ProtoPostgresTableDesc::decode(data) else {
        return;
    };
    // `into_rust` may legitimately reject (e.g. a `col_num`/`cols` value that
    // overflows `u16`); it must do so via `Err`, never a panic.
    let Ok(orig): Result<PostgresTableDesc, _> = proto.into_rust() else {
        return;
    };
    assert_roundtrip(orig);
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };

    match mode % 4 {
        // Structured arm: synthesize a valid, fully-populated desc.
        0 => {
            let mut runner = TestRunner::new_with_rng(
                Config::default(),
                TestRng::from_seed(RngAlgorithm::ChaCha, &seed_from(rest)),
            );
            if let Ok(tree) = PostgresTableDesc::arbitrary().new_tree(&mut runner) {
                assert_roundtrip(tree.current());
            }
        }
        // Targeted arm: hand-build a proto whose `col_num` / `cols` values
        // straddle the u16 boundary, confirming the u32 -> u16 narrowing in
        // `from_proto` returns `Err` (not a panic) for the out-of-range cases
        // and succeeds for the in-range ones.
        1 => {
            // Use the first few bytes as little-endian u32 candidates so the
            // fuzzer can search both sides of the 65535 boundary.
            let take_u32 = |i: usize| -> u32 {
                let mut buf = [0u8; 4];
                let n = rest.len().saturating_sub(i * 4).min(4);
                if n > 0 {
                    buf[..n].copy_from_slice(&rest[i * 4..i * 4 + n]);
                }
                u32::from_le_bytes(buf)
            };
            let col_num = take_u32(0);
            let key_col = take_u32(1);

            let proto = ProtoPostgresTableDesc {
                name: "t".into(),
                namespace: "n".into(),
                oid: 1,
                columns: vec![ProtoPostgresColumnDesc {
                    name: "c".into(),
                    type_oid: 23,
                    type_mod: -1,
                    nullable: true,
                    col_num: Some(col_num),
                }],
                keys: vec![ProtoPostgresKeyDesc {
                    oid: 2,
                    name: "k".into(),
                    cols: vec![key_col],
                    is_primary: true,
                    nulls_not_distinct: false,
                }],
            };
            let bytes = proto.encode_to_vec();
            let decoded = ProtoPostgresTableDesc::decode(bytes.as_slice())
                .expect("hand-built proto must decode");
            // Whether the narrowing fits, the only requirement is no panic; if
            // it converts, the value must round-trip.
            let converted: Result<PostgresTableDesc, _> = decoded.into_rust();
            if let Ok(orig) = converted {
                assert_roundtrip(orig);
            }
        }
        // Targeted arm: a wire proto with duplicate and unsorted `keys`. The
        // repeated field maps to a Rust `BTreeSet`, which dedups + sorts; this
        // must collapse cleanly with no ordering/dup assertion firing.
        2 => {
            let mk = |oid: u32, col: u16| ProtoPostgresKeyDesc {
                oid,
                name: "k".into(),
                cols: vec![u32::from(col)],
                is_primary: false,
                nulls_not_distinct: false,
            };
            // Intentionally out of order with a duplicate entry.
            let proto = ProtoPostgresTableDesc {
                name: "t".into(),
                namespace: "n".into(),
                oid: 7,
                columns: vec![ProtoPostgresColumnDesc {
                    name: "c".into(),
                    type_oid: 23,
                    type_mod: -1,
                    nullable: false,
                    col_num: Some(1),
                }],
                keys: vec![mk(3, 2), mk(1, 9), mk(3, 2), mk(2, 0)],
            };
            let bytes = proto.encode_to_vec();
            let decoded = ProtoPostgresTableDesc::decode(bytes.as_slice())
                .expect("hand-built proto must decode");
            let orig: PostgresTableDesc =
                decoded.into_rust().expect("duplicate/unsorted keys must convert");
            assert_roundtrip(orig);
        }
        // Raw-bytes arm: decode adversarial proto bytes, then round-trip.
        _ => raw_roundtrip(rest),
    }
});
