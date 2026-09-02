// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `SqlColumnType` proto round-trips losslessly.
//!
//! Two arms (the first byte selects):
//!  - Arbitrary arm: drive `SqlColumnType`'s proptest `Arbitrary` strategy from
//!    the fuzzer bytes to build a *valid* column type pairing a nested
//!    `SqlScalarType` with a nullable flag, and assert it survives an
//!    encode/decode through the wire codec. Valid values are where a proto3
//!    presence bug shows up: a default-valued field that silently stops being
//!    written to the wire still passes an in-memory `RustType` round-trip.
//!  - Raw-bytes arm: decode arbitrary bytes as `ProtoColumnType`, into Rust, and
//!    re-encode, keeping coverage of the bare wire decoder against hostile
//!    input. `prost` is built with `no-recursion-limit`, so this is also the arm
//!    that reaches deep nesting in the inner scalar type.
//!
//! Both arms also run a domain oracle over the decoded type parameters. The
//! round-trip oracle on its own cannot see decoder laxity here: `into_proto`
//! writes every type parameter back verbatim, so byte preservation holds for
//! out-of-domain values too and `assert_eq!(orig, round)` degenerates into "did
//! not panic".

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, protobuf_roundtrip};
use mz_repr::adt::char::CharLength;
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{ProtoColumnType, SqlColumnType, SqlScalarType};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use prost::Message;

/// Asserts every type parameter reachable from `t` is one the planner could have
/// produced.
///
/// Each newtype enforces its domain in `TryFrom<i64>`, and consumers assume that
/// invariant holds however the value was built: a `Timestamp` precision above
/// `MAX_PRECISION` panics `CheckedTimestamp::round_to_precision`, which the SQL
/// Server source decoder reaches with the precision carried by a decoded
/// `SqlColumnType`.
fn assert_type_params_in_domain(t: &SqlScalarType) {
    match t {
        SqlScalarType::Timestamp { precision } | SqlScalarType::TimestampTz { precision } => {
            if let Some(p) = precision {
                assert!(
                    TimestampPrecision::try_from(i64::from(p.into_u8())).is_ok(),
                    "out-of-domain timestamp precision {}",
                    p.into_u8()
                );
            }
        }
        SqlScalarType::Numeric { max_scale } => {
            if let Some(s) = max_scale {
                assert!(
                    NumericMaxScale::try_from(i64::from(s.into_u8())).is_ok(),
                    "out-of-domain numeric max scale {}",
                    s.into_u8()
                );
            }
        }
        SqlScalarType::Char { length } => {
            if let Some(l) = length {
                assert!(
                    CharLength::try_from(i64::from(l.into_u32())).is_ok(),
                    "out-of-domain char length {}",
                    l.into_u32()
                );
            }
        }
        SqlScalarType::VarChar { max_length } => {
            if let Some(l) = max_length {
                assert!(
                    VarCharMaxLength::try_from(i64::from(l.into_u32())).is_ok(),
                    "out-of-domain varchar max length {}",
                    l.into_u32()
                );
            }
        }
        SqlScalarType::Array(inner)
        | SqlScalarType::Range {
            element_type: inner,
        } => assert_type_params_in_domain(inner),
        SqlScalarType::List {
            element_type: inner,
            ..
        }
        | SqlScalarType::Map {
            value_type: inner, ..
        } => assert_type_params_in_domain(inner),
        SqlScalarType::Record { fields, .. } => {
            for (_, ct) in fields.iter() {
                assert_type_params_in_domain(&ct.scalar_type);
            }
        }
        _ => {}
    }
}

fn arbitrary_arm(seed: &[u8]) {
    let mut buf = [0u8; 32];
    for (dst, src) in buf.iter_mut().zip(seed.iter()) {
        *dst = *src;
    }
    // NOTE: hashing the fuzzer bytes into a ChaCha seed costs libFuzzer its
    // mutation gradient, one flipped bit re-rolls the whole value. The obvious
    // fix, `RngAlgorithm::PassThrough`, hangs: it feeds the fuzzer bytes in as
    // the random stream and then yields zeros forever once they run out, and
    // `rand`'s Lemire sampler loops until a draw clears `thresh`, which a zero
    // never does for a range that is not a power of two. Every strategy here
    // outdraws a 4096-byte input.
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &buf);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    let value = match <SqlColumnType as proptest::arbitrary::Arbitrary>::arbitrary()
        .new_tree(&mut runner)
    {
        Ok(tree) => tree.current(),
        Err(_) => return,
    };

    // The generator is meant to model what the planner can produce, so a failure
    // here is a generator defect, not a decoder defect.
    assert_type_params_in_domain(&value.scalar_type);

    let back = protobuf_roundtrip::<_, ProtoColumnType>(&value)
        .expect("valid SqlColumnType must round-trip");
    assert_eq!(value, back, "SqlColumnType changed across proto roundtrip");
}

fn raw_arm(data: &[u8]) {
    let Ok(proto) = ProtoColumnType::decode(data) else {
        return;
    };
    let orig: SqlColumnType = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };
    assert_type_params_in_domain(&orig.scalar_type);

    let proto2 = <ProtoColumnType as ProtoType<SqlColumnType>>::from_rust(&orig);
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoColumnType::decode(bytes2.as_slice())
        .expect("re-encode of valid SqlColumnType must decode");
    let round: SqlColumnType = proto3
        .into_rust()
        .expect("re-encoded SqlColumnType must convert back to Rust");

    assert_eq!(orig, round, "SqlColumnType changed across proto roundtrip");
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
