// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `DataflowError`s must survive a proto encode + decode round trip
//! losslessly. `DataflowError`s travel between the storage controller and
//! clusterd, so a decoder bug here is a crash/poisoning risk for the storage
//! path.
//!
//! Two complementary input arms (the first byte picks):
//!
//!  * **Structured arm.** Drives `DataflowError`'s proptest `Arbitrary` from the
//!    libFuzzer byte stream to synthesize a *valid, deeply-populated* value
//!    (the 4-arm oneof, including the nested `EvalError` / `SourceError` /
//!    `EnvelopeError` / `DecodeError` trees). Random proto bytes almost never
//!    reach these inner variants, so this is where the interesting branches of
//!    `into_proto`/`from_proto` actually get covered. We assert the full
//!    `Rust -> Proto -> Rust` round trip is the identity.
//!  * **Raw-bytes arm.** Decodes arbitrary bytes straight into
//!    `ProtoDataflowError`, exercising the decoder against malformed/adversarial
//!    wire input, then re-encodes the recovered value.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::ProtoType;
use mz_storage_types::errors::{DataflowError, ProtoDataflowError};
use prost::Message;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

/// Build a 32-byte proptest seed from `bytes` (zero-padded / truncated).
fn seed_from(bytes: &[u8]) -> [u8; 32] {
    let mut seed = [0u8; 32];
    let n = bytes.len().min(32);
    seed[..n].copy_from_slice(&bytes[..n]);
    seed
}

/// `Rust -> Proto -> Rust` must be the identity for any valid `DataflowError`.
fn assert_roundtrip(orig: DataflowError) {
    let proto = <ProtoDataflowError as ProtoType<DataflowError>>::from_rust(&orig);
    let bytes = proto.encode_to_vec();
    let proto2 = ProtoDataflowError::decode(bytes.as_slice())
        .expect("re-encode of valid DataflowError must decode");
    let round: DataflowError = proto2
        .into_rust()
        .expect("re-encoded DataflowError must convert back to Rust");
    assert_eq!(orig, round, "DataflowError changed across proto roundtrip");
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };

    if mode & 1 == 0 {
        // Structured arm: synthesize a valid, deep value via proptest Arbitrary.
        let seed = seed_from(rest);
        let mut runner = TestRunner::new_with_rng(
            Config::default(),
            TestRng::from_seed(RngAlgorithm::ChaCha, &seed),
        );
        let Ok(tree) = <DataflowError as proptest::arbitrary::Arbitrary>::arbitrary()
            .new_tree(&mut runner)
        else {
            return;
        };
        assert_roundtrip(tree.current());
    } else {
        // Raw-bytes arm: decode adversarial wire bytes, then round-trip.
        let Ok(proto) = ProtoDataflowError::decode(rest) else {
            return;
        };
        let orig: DataflowError = match proto.into_rust() {
            Ok(v) => v,
            Err(_) => return,
        };
        assert_roundtrip(orig);
    }
});
