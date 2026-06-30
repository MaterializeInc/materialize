// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: a `SourceData` must survive a proto encode + decode round trip
//! losslessly. `SourceData` is the persisted/wire payload of every storage
//! collection, so a decoder bug here corrupts source output.
//!
//! `SourceData` wraps a `Result<Row, DataflowError>`. Two complementary input
//! arms (the first byte picks):
//!
//!  * **Structured arm.** Drives proptest `Arbitrary` from the libFuzzer byte
//!    stream to synthesize a *valid, populated* value. The `Ok` branch packs a
//!    full random `Row` (every datum kind, including the tricky numeric / date /
//!    array / map encodings). The `Err` branch synthesizes a deep
//!    `DataflowError` (the 4-arm oneof with nested EvalError/Source/Envelope
//!    trees). Random proto bytes essentially never reach a non-empty Row or a
//!    deep error, so this is where `Row::into_proto`/`from_proto` and the error
//!    conversions actually get exercised. We assert the full
//!    `Rust -> Proto -> Rust` round trip is the identity.
//!  * **Raw-bytes arm.** Decodes arbitrary bytes straight into
//!    `ProtoSourceData`, exercising the decoder against malformed/adversarial
//!    wire input, then re-encodes the recovered value.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::ProtoType;
use mz_repr::Row;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::{ProtoSourceData, SourceData};
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

/// `Rust -> Proto -> Rust` must be the identity for any valid `SourceData`.
fn assert_roundtrip(orig: SourceData) {
    let proto = <ProtoSourceData as ProtoType<SourceData>>::from_rust(&orig);
    let bytes = proto.encode_to_vec();
    let proto2 = ProtoSourceData::decode(bytes.as_slice())
        .expect("re-encode of valid SourceData must decode");
    let round: SourceData = proto2
        .into_rust()
        .expect("re-encoded SourceData must convert back to Rust");
    assert_eq!(orig, round, "SourceData changed across proto roundtrip");
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };

    if mode & 1 == 0 {
        // Structured arm: synthesize a valid value via proptest Arbitrary. The
        // low bit of `mode` selected this arm, the next bit picks Ok vs Err.
        let seed = seed_from(rest);
        let mut runner = TestRunner::new_with_rng(
            Config::default(),
            TestRng::from_seed(RngAlgorithm::ChaCha, &seed),
        );
        let value = if mode & 2 == 0 {
            let Ok(tree) = <Row as proptest::arbitrary::Arbitrary>::arbitrary().new_tree(&mut runner)
            else {
                return;
            };
            SourceData(Ok(tree.current()))
        } else {
            let Ok(tree) =
                <DataflowError as proptest::arbitrary::Arbitrary>::arbitrary().new_tree(&mut runner)
            else {
                return;
            };
            SourceData(Err(tree.current()))
        };
        assert_roundtrip(value);
    } else {
        // Raw-bytes arm: decode adversarial wire bytes, then round-trip.
        let Ok(proto) = ProtoSourceData::decode(rest) else {
            return;
        };
        let orig: SourceData = match proto.into_rust() {
            Ok(v) => v,
            Err(_) => return,
        };
        assert_roundtrip(orig);
    }
});
