// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `ProtoMySqlTableDesc` <-> `MySqlTableDesc` round-trip.
//! Describes external-database schemas, so a decoder bug here is reachable
//! from a compromised upstream MySQL or on-disk catalog bytes.
//!
//! Input generation is split across three arms keyed off the first input
//! byte so a single byte stream exercises all of them over time:
//!
//! 1. **Valid-value arm.** A 32-byte seed (zero-padded from the input) drives
//!    proptest's `Arbitrary for MySqlTableDesc` to build a *structurally
//!    valid, deeply-populated* descriptor. Non-empty columns with real
//!    `SqlColumnType`s, every `MySqlColumnMeta` variant, and a populated
//!    `BTreeSet<MySqlKeyDesc>`. It asserts the canonical
//!    `from_proto(into_proto(v)) == v` Rust round-trip, which a
//!    random-bytes-only target almost never reaches (random protobuf
//!    decodes to near-empty messages).
//!
//! 2. **Duplicate/unsorted-keys arm.** Crafts a `ProtoMySqlTableDesc`
//!    whose `keys` field (a repeated proto `Vec`) contains duplicates in
//!    non-sorted order, then asserts the two properties the
//!    `Vec -> BTreeSet` decode owes its callers: the decoded descriptor does
//!    not depend on the wire order, and the duplicates collapse. The
//!    conversion itself must succeed, because the crafted proto is valid by
//!    construction.
//!
//! 3. **Raw-bytes arm.** Decode arbitrary bytes and, if they happen to form a
//!    valid descriptor, check the proto round-trip is stable. This guards
//!    robustness against the real wire/catalog format.

#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use mz_mysql_util::{MySqlTableDesc, ProtoMySqlKeyDesc, ProtoMySqlTableDesc};
use mz_proto::{ProtoType, RustType};
use proptest::strategy::{BoxedStrategy, Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use prost::Message;

// `Arbitrary::arbitrary()` rebuilds the entire boxed strategy graph on every
// call: `SqlScalarType`'s ~31-variant `Union` plus a second copy of it for
// `Array`, the `prop_recursive` wrapper, and a `.*` regex compile per
// `any::<String>()` leaf. `Config::default()` re-reads the process environment.
// Both are per-process constants, so pay for them once instead of once per
// execution. libFuzzer runs a single execution at a time per process, so a
// `thread_local` suffices for the non-`Sync` strategy.
thread_local! {
    static DESC_STRATEGY: BoxedStrategy<MySqlTableDesc> =
        <MySqlTableDesc as proptest::arbitrary::Arbitrary>::arbitrary().boxed();
}

fn config() -> Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(Config::default).clone()
}

/// Assert that a `MySqlTableDesc` survives a full Rust round-trip through
/// its proto representation unchanged, including a re-encode/decode of the
/// wire bytes.
fn assert_rust_roundtrip(orig: &MySqlTableDesc) {
    let proto = orig.into_proto();
    let bytes = proto.encode_to_vec();
    let proto2 = ProtoMySqlTableDesc::decode(bytes.as_slice())
        .expect("re-encode of valid MySqlTableDesc must decode");
    let round: MySqlTableDesc = proto2
        .into_rust()
        .expect("re-encoded MySqlTableDesc must convert back to Rust");
    assert_eq!(
        orig, &round,
        "MySqlTableDesc changed across proto roundtrip"
    );
}

/// Decode `bytes` as a proto, and if it is a valid descriptor, assert the
/// proto round-trip is stable. Used by the raw-bytes arm, where a decode or
/// conversion failure is a legitimate outcome, and where the *first* decode may
/// normalize a `Vec`-shaped field into a `BTreeSet`, so we only require
/// idempotence from that normalized value on.
fn check_decoded(bytes: &[u8]) {
    let Ok(proto) = ProtoMySqlTableDesc::decode(bytes) else {
        return;
    };
    let orig: MySqlTableDesc = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };
    assert_rust_roundtrip(&orig);
}

/// Build a proto whose `keys` Vec deliberately violates the `BTreeSet`
/// invariants (duplicates and reverse order), seeded from `data` so the
/// fuzzer can vary the contents while keeping the shape pathological.
fn craft_unsorted_dup_keys(data: &[u8]) -> ProtoMySqlTableDesc {
    // Derive a couple of distinct key bodies from the seed bytes.
    let pick = |i: usize| -> String {
        let b = data.get(i).copied().unwrap_or(i as u8);
        format!("k{}", b % 5)
    };
    let key_a = ProtoMySqlKeyDesc {
        name: pick(0),
        is_primary: data.first().copied().unwrap_or(0) & 1 == 0,
        columns: vec![pick(1), pick(2)],
    };
    let key_b = ProtoMySqlKeyDesc {
        name: pick(3),
        is_primary: data.get(1).copied().unwrap_or(0) & 1 == 0,
        columns: vec![pick(4)],
    };
    // Emit duplicates and in deliberately non-sorted order. The decoder
    // collapses these into a BTreeSet, which is the round-trip trap.
    ProtoMySqlTableDesc {
        name: "fuzz".to_string(),
        schema_name: "fuzz".to_string(),
        columns: vec![],
        keys: vec![key_b.clone(), key_a.clone(), key_a.clone(), key_b],
    }
}

/// Encode a hand-built proto, decode the wire bytes, and convert to Rust. Every
/// step must succeed: the input is structurally valid by construction, so a
/// failure is the bug the crafted arm exists to catch, for example a
/// `from_proto` hardened to reject duplicate or unsorted wire keys.
fn decode_crafted(proto: &ProtoMySqlTableDesc) -> MySqlTableDesc {
    ProtoMySqlTableDesc::decode(proto.encode_to_vec().as_slice())
        .expect("hand-built proto must decode")
        .into_rust()
        .expect("duplicate/unsorted keys must convert")
}

fuzz_target!(|data: &[u8]| {
    // The first byte selects the arm, everything after it is that arm's input.
    // The arms overlap in the byte stream on purpose: only one of them runs per
    // execution, so reading them all from byte 1 keeps every mutation live for
    // whichever arm the mode byte picks. The proptest seed is zero-padded rather
    // than all-or-nothing, otherwise every input shorter than 33 bytes would
    // reuse the all-zero seed and libFuzzer grows inputs up from empty.
    let mode = data.first().copied().unwrap_or(0);
    let tail = data.get(1..).unwrap_or(&[]);
    let mut seed = [0u8; 32];
    let n = tail.len().min(32);
    seed[..n].copy_from_slice(&tail[..n]);

    match mode % 3 {
        0 => {
            // Valid-value arm: drive proptest's Arbitrary from the seed.
            let mut runner =
                TestRunner::new_with_rng(config(), TestRng::from_seed(RngAlgorithm::ChaCha, &seed));
            let value = match DESC_STRATEGY.with(|s| s.new_tree(&mut runner)) {
                Ok(tree) => tree.current(),
                Err(_) => return,
            };
            assert_rust_roundtrip(&value);
        }
        1 => {
            // Duplicate/unsorted-keys arm.
            let proto = craft_unsorted_dup_keys(tail);
            let decoded = decode_crafted(&proto);
            // The wire field is a repeated Vec and the Rust field a BTreeSet, so
            // decoding must be insensitive to the wire order and must collapse
            // the duplicates rather than keep them or reject the message.
            //
            // NOTE: rotate, don't reverse. `craft_unsorted_dup_keys` lays the
            // keys out as a palindrome, so reversing them would re-encode the
            // very same wire bytes and assert nothing.
            let mut permuted = proto;
            permuted.keys.rotate_left(1);
            assert_eq!(
                decoded,
                decode_crafted(&permuted),
                "wire key order changed the decoded descriptor"
            );
            // `craft_unsorted_dup_keys` builds exactly two distinct keys (their
            // `columns` differ in length, so they can never compare equal),
            // each repeated twice.
            assert_eq!(
                decoded.keys.len(),
                2,
                "duplicate wire keys did not collapse"
            );
            assert_rust_roundtrip(&decoded);
        }
        _ => {
            // Raw-bytes arm: decode arbitrary bytes directly.
            check_decoded(tail);
        }
    }
});
