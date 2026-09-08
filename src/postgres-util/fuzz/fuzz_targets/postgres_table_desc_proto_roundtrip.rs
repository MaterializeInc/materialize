// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `ProtoPostgresTableDesc` <-> `PostgresTableDesc` round-trip.
//! `PostgresTableDesc` describes external-database schemas, so a decoder bug
//! here is reachable from a compromised upstream Postgres or on-disk catalog
//! bytes.
//!
//! The first input byte selects one of four arms. The rest feeds it:
//!
//!  * **Structured arm.** Drives `PostgresTableDesc`'s proptest `Arbitrary`
//!    (behind mz-postgres-util's `schemas` feature) from the libFuzzer byte
//!    stream to synthesize a *valid, fully-populated* value, then asserts the
//!    `value -> proto -> value` chain is the identity. This is what actually
//!    reaches the deep shape: several `PostgresColumnDesc`s with arbitrary
//!    `col_num`/`type_oid`/`type_mod`/`nullable`, and a `BTreeSet` of several
//!    `PostgresKeyDesc`s, each with a `Vec<u16>` of `cols`. Random proto bytes
//!    decode to a near-empty desc, so the populated branches never get covered
//!    otherwise.
//!  * **Narrowing arm.** Hand-builds a proto whose `ProtoPostgresColumnDesc::
//!    col_num` and `ProtoPostgresKeyDesc::cols` sit in a dense neighborhood of
//!    the `u16::MAX` boundary, and asserts the `u32 -> u16` narrowing in
//!    `from_proto` rejects *exactly* the out-of-range values via `Err` (never a
//!    panic, never a silent truncation) and preserves the in-range ones bit for
//!    bit. `col_num` is a column's positional identity, matched by equality when
//!    purification resolves key columns and when a source checks an upstream
//!    schema for compatibility, so a truncating narrowing would silently remap
//!    or drop key columns instead of rejecting the input.
//!  * **Duplicate-keys arm.** Builds a proto whose repeated `keys` field carries
//!    duplicate and unsorted entries, which must collapse into the Rust
//!    `BTreeSet` without tripping an ordering assertion and without letting the
//!    wire order leak into the decoded value. The key bodies come from the input
//!    so the mutator can vary `cols` (including empty) and the flags while the
//!    duplicate/unsorted shape stays fixed.
//!  * **Raw-bytes arm.** Decodes arbitrary bytes straight into the proto then
//!    `into_rust`, exercising the decoder against malformed/adversarial input,
//!    then re-encodes the recovered value. Random bytes essentially never reach
//!    the boundary or duplicate-key shapes, which is why those get their own
//!    arms.

#![no_main]

use std::collections::BTreeSet;

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

/// Read `bytes[i]`, treating a short input as zero-padded.
fn byte(bytes: &[u8], i: usize) -> u8 {
    bytes.get(i).copied().unwrap_or(0)
}

/// Assert the `value -> proto -> value` chain is the identity.
fn assert_roundtrip(orig: PostgresTableDesc) {
    let proto = <ProtoPostgresTableDesc as ProtoType<PostgresTableDesc>>::from_rust(&orig);
    let bytes = proto.encode_to_vec();
    let proto2 = ProtoPostgresTableDesc::decode(bytes.as_slice())
        .expect("re-encode of valid PostgresTableDesc must decode");
    let round: PostgresTableDesc = proto2
        .into_rust()
        .expect("re-encoded PostgresTableDesc must convert back to Rust");
    assert_eq!(
        orig, round,
        "PostgresTableDesc changed across proto roundtrip"
    );

    // Implied by the equality above for as long as every field `into_proto`
    // reads takes part in `PartialEq`. Kept as a guard for the day one doesn't,
    // not as coverage of a separate invariant.
    let bytes2 =
        <ProtoPostgresTableDesc as ProtoType<PostgresTableDesc>>::from_rust(&round).encode_to_vec();
    assert_eq!(bytes, bytes2, "proto re-encode was not idempotent");
}

/// Decode adversarial proto bytes, convert to Rust, then round-trip.
fn raw_roundtrip(data: &[u8]) {
    let Ok(proto) = ProtoPostgresTableDesc::decode(data) else {
        return;
    };
    // `into_rust` may legitimately reject (e.g. a `col_num`/`cols` value that
    // overflows `u16`). It must do so via `Err`, never a panic.
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
        // `from_proto` returns `Err` (not a panic, not a truncated value) for
        // the out-of-range cases and preserves the in-range ones.
        1 => {
            // Draw each candidate from a dense neighborhood of the boundary. A
            // plain little-endian u32 over 4 input bytes would put the in-range
            // side at 2^-16 of executions, and the fuzzer gets no coverage
            // gradient toward it, because the success path is already covered by
            // the other arms.
            let candidate = |lo: usize, sel: usize| -> u32 {
                let base = u32::from(u16::from_le_bytes([byte(rest, lo), byte(rest, lo + 1)]));
                match byte(rest, sel) % 4 {
                    0 => base,
                    1 => u32::from(u16::MAX),
                    2 => u32::from(u16::MAX) + 1 + base,
                    _ => u32::MAX - base,
                }
            };
            let col_num = candidate(0, 2);
            let key_col = candidate(3, 5);

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
            let converted: Result<PostgresTableDesc, _> = decoded.into_rust();
            let fits = col_num <= u32::from(u16::MAX) && key_col <= u32::from(u16::MAX);
            assert_eq!(
                converted.is_ok(),
                fits,
                "u32 -> u16 narrowing must reject exactly the out-of-range values \
                 (col_num={col_num}, key_col={key_col})"
            );
            let Ok(orig) = converted else {
                return;
            };
            // In-range values must survive intact, neither truncated nor clamped.
            // The arm builds exactly one column and one key, so this indexing
            // holds.
            assert_eq!(u32::from(orig.columns[0].col_num), col_num);
            assert_eq!(
                orig.keys.iter().next().expect("one key").cols,
                vec![u16::try_from(key_col).expect("in range")],
            );
            assert_roundtrip(orig);
        }
        // Targeted arm: a wire proto with duplicate and unsorted `keys`. The
        // repeated field maps to a Rust `BTreeSet`, which dedups + sorts. This
        // must collapse cleanly with no ordering/dup assertion firing.
        2 => {
            // Seed the key bodies from the input so the mutator can vary `cols`
            // (length 0 to 3, so the empty-key case is reachable), the columns
            // they reference, and the flags, while the duplicate/unsorted shape
            // stays fixed.
            let cols_at = |base: usize| -> Vec<u32> {
                (0..usize::from(byte(rest, base) % 4))
                    .map(|i| u32::from(byte(rest, base + 1 + i)))
                    .collect()
            };
            let mk = |oid: u32, cols: Vec<u32>| ProtoPostgresKeyDesc {
                oid,
                name: format!("k{oid}"),
                cols,
                is_primary: oid & 1 == 0,
                nulls_not_distinct: oid & 2 == 0,
            };
            let key_a = mk(u32::from(byte(rest, 0)), cols_at(1));
            let key_b = mk(u32::from(byte(rest, 5)), cols_at(6));

            // Give every column a key references a matching `col_num`. Nothing in
            // this arm's purpose needs the desc to be internally inconsistent,
            // and the `.expect` below would become a false crash the day
            // `from_proto` starts validating what purification already checks
            // downstream, that a key's columns exist.
            let col_nums: BTreeSet<u32> = key_a.cols.iter().chain(&key_b.cols).copied().collect();
            let columns: Vec<_> = col_nums
                .iter()
                .map(|&col_num| ProtoPostgresColumnDesc {
                    name: format!("c{col_num}"),
                    type_oid: 23,
                    type_mod: -1,
                    nullable: false,
                    col_num: Some(col_num),
                })
                .collect();

            // Intentionally out of order with duplicate entries.
            let proto = ProtoPostgresTableDesc {
                name: "t".into(),
                namespace: "n".into(),
                oid: 7,
                columns,
                keys: vec![key_b.clone(), key_a.clone(), key_a, key_b],
            };
            let bytes = proto.encode_to_vec();
            let decoded = ProtoPostgresTableDesc::decode(bytes.as_slice())
                .expect("hand-built proto must decode");
            let orig: PostgresTableDesc = decoded
                .clone()
                .into_rust()
                .expect("duplicate/unsorted keys must convert");

            // Reversing the wire order must not change what we decode.
            let mut reversed = decoded;
            reversed.keys.reverse();
            let flipped: PostgresTableDesc = reversed
                .into_rust()
                .expect("duplicate/unsorted keys must convert");
            assert_eq!(orig, flipped, "decoded desc depends on wire key order");

            assert_roundtrip(orig);
        }
        // Raw-bytes arm: decode adversarial proto bytes, then round-trip.
        _ => raw_roundtrip(rest),
    }
});
