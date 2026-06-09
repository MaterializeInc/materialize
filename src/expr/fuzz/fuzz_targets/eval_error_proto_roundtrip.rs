// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `EvalError` must survive a proto re-encode + re-decode with the
//! same value, exercising lossy proto conversions where the wire form decodes
//! into a value that doesn't round-trip back to itself.
//!
//! Two input modes share the byte stream (the first byte selects the mode):
//!
//!   * Mode A (Arbitrary): drive proptest's `Arbitrary` impl for `EvalError`
//!     from the libFuzzer byte stream to synthesize a *valid, deeply nested*
//!     `EvalError` (one of 80+ variants, several carrying `char`, `usize`,
//!     nested `NumericMaxScale`/`InvalidArrayError`/`DomainLimit` invariants).
//!     We then assert `from_proto(into_proto(v)) == v`. Random bytes decoded as
//!     a proto almost always yield near-empty/default messages, so this arm is
//!     what actually reaches the interesting variants and their narrowing /
//!     validation logic (`char::from_proto`, `usize`/`u32` casts, etc.).
//!
//!   * Mode B (raw bytes): decode the remaining bytes directly as
//!     `ProtoEvalError` and, if it converts to Rust, assert it round-trips.
//!     Kept for robustness against hand-crafted / malformed wire forms that the
//!     structured generator would never produce.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_expr::{EvalError, ProtoEvalError};
use mz_proto::ProtoType;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use prost::Message;

/// Assert that a `EvalError` survives encode -> decode -> into_rust unchanged.
fn assert_roundtrip(orig: &EvalError) {
    let proto = <ProtoEvalError as ProtoType<EvalError>>::from_rust(orig);
    let bytes = proto.encode_to_vec();
    let decoded = ProtoEvalError::decode(bytes.as_slice())
        .expect("re-encode of valid EvalError must decode");
    let round: EvalError = decoded
        .into_rust()
        .expect("re-encoded EvalError must convert back to Rust");
    assert_eq!(orig, &round, "EvalError changed across proto roundtrip");
}

/// Decode `data` directly as a `ProtoEvalError`; if it converts to a Rust
/// `EvalError`, assert that value round-trips.
fn raw_bytes_arm(data: &[u8]) {
    let Ok(proto) = ProtoEvalError::decode(data) else {
        return;
    };
    let orig: EvalError = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };
    assert_roundtrip(&orig);
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };

    if mode & 1 == 0 {
        // Mode A: structured Arbitrary generation. Derive a 32-byte seed from
        // the remaining bytes (cycled / zero-padded) so even short inputs are
        // usable, then drive proptest's Arbitrary impl from it.
        let mut seed = [0u8; 32];
        if !rest.is_empty() {
            for (i, slot) in seed.iter_mut().enumerate() {
                *slot = rest[i % rest.len()];
            }
        }
        let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
        let mut runner = TestRunner::new_with_rng(Config::default(), rng);
        let value = <EvalError as proptest::arbitrary::Arbitrary>::arbitrary()
            .new_tree(&mut runner)
            .expect("valuetree")
            .current();
        assert_roundtrip(&value);
    } else {
        // Mode B: raw wire bytes.
        raw_bytes_arm(rest);
    }
});
