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
//! 1. **Valid-value arm.** A 32-byte seed (drawn from the input) drives
//!    proptest's `Arbitrary for MySqlTableDesc` to build a *structurally
//!    valid, deeply-populated* descriptor. Non-empty columns with real
//!    `SqlColumnType`s, every `MySqlColumnMeta` variant, and a populated
//!    `BTreeSet<MySqlKeyDesc>`. It asserts the canonical
//!    `from_proto(into_proto(v)) == v` Rust round-trip, which a
//!    random-bytes-only target almost never reaches (random protobuf
//!    decodes to near-empty messages).
//!
//! 2. **Duplicate/unsorted-keys arm.** Crafts a `ProtoMySqlTableDesc`
//!    whose `keys` field (a repeated proto `Vec`) contains duplicates and
//!    out-of-order entries to probe the classic `Vec -> BTreeSet -> Vec`
//!    round-trip trap: the *first* decode normalizes (dedups + sorts), so
//!    we assert the conversion is a *fixed point* afterwards rather than
//!    byte-identical to the crafted input.
//!
//! 3. **Raw-bytes arm.** Decode arbitrary bytes and, if they happen to form a
//!    valid descriptor, check the proto round-trip is stable. This guards
//!    robustness against the real wire/catalog format.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_mysql_util::{MySqlKeyDesc, MySqlTableDesc, ProtoMySqlKeyDesc, ProtoMySqlTableDesc};
use mz_proto::{ProtoType, RustType};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use prost::Message;

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
    assert_eq!(orig, &round, "MySqlTableDesc changed across proto roundtrip");
}

/// Decode `bytes` as a proto, and if it is a valid descriptor, assert the
/// proto round-trip is stable. Used by both the crafted and raw-bytes arms,
/// where the *first* decode may normalize a `Vec`-shaped field into a
/// `BTreeSet`, so we only require idempotence from that normalized value on.
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

fuzz_target!(|data: &[u8]| {
    // Reserve the first byte as a mode selector and the next 32 bytes as the
    // proptest seed. Everything after that feeds the raw-bytes / crafting
    // logic so a single input can drive any arm.
    let mode = data.first().copied().unwrap_or(0);
    let mut seed = [0u8; 32];
    let seed_src = data.get(1..33).unwrap_or(&[]);
    seed[..seed_src.len()].copy_from_slice(seed_src);
    let rest = data.get(33..).unwrap_or(&[]);

    match mode % 3 {
        0 => {
            // Valid-value arm: drive proptest's Arbitrary from the seed.
            let mut runner = TestRunner::new_with_rng(
                Config::default(),
                TestRng::from_seed(RngAlgorithm::ChaCha, &seed),
            );
            let value = match <MySqlTableDesc as proptest::arbitrary::Arbitrary>::arbitrary()
                .new_tree(&mut runner)
            {
                Ok(tree) => tree.current(),
                Err(_) => return,
            };
            assert_rust_roundtrip(&value);

            // The proptest-built value is already normalized (its keys come
            // from a BTreeSet). Sanity-check that the BTreeSet semantics hold
            // after a wire decode by encoding and re-decoding, then confirm
            // re-collecting the keys into a fresh set is idempotent.
            let decoded: MySqlTableDesc = value
                .into_proto()
                .encode_to_vec()
                .as_slice()
                .pipe(ProtoMySqlTableDesc::decode)
                .expect("decode")
                .into_rust()
                .expect("into_rust");
            let recollected: std::collections::BTreeSet<MySqlKeyDesc> =
                decoded.keys.iter().cloned().collect();
            assert_eq!(decoded.keys, recollected, "key set not idempotent");
        }
        1 => {
            // Duplicate/unsorted-keys arm.
            let proto = craft_unsorted_dup_keys(rest);
            check_decoded(proto.encode_to_vec().as_slice());
        }
        _ => {
            // Raw-bytes arm: decode arbitrary bytes directly.
            check_decoded(rest);
        }
    }
});

/// Tiny extension trait so the valid-value arm can read top-to-bottom.
trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R {
        f(self)
    }
}
impl<T> Pipe for T {}
