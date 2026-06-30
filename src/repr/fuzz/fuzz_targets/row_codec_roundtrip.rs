// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `Row`'s `Codec` impl (the persistence wrapper around
//! `ProtoRow`) round-trips losslessly. This goes through `Codec::decode +
//! Codec::encode`, which is the path persist actually uses on read/write. It is
//! distinct from the bare `ProtoRow` path in `row_proto_roundtrip` because it
//! threads the `RelationDesc` schema through.
//!
//! The schema is the whole point of this target, so we decode against a
//! NON-empty, randomized `RelationDesc` rather than `RelationDesc::empty()`.
//! With the empty schema the schema-directed decode loop
//! (`Row::decode_from_proto`, which iterates `desc.iter_all()` and pulls each
//! column's proto datum, padding missing columns with `Datum::Null`) never
//! runs. It produces an empty `Row` for any input, so the round trip is
//! trivially satisfied and the schema threading this target tests goes
//! unexercised.
//!
//! Generation: the first few input bytes pick a column count and a column type
//! per column (the type menu mirrors the scalar shapes persist stores). The
//! remaining bytes are the encoded `ProtoRow` body. Note `decode_from_proto`
//! does not type-check the proto datums against the column types, so the schema
//! controls the column *count* and the per-column iteration / null-padding,
//! which a non-empty schema exercises and the empty schema does not. The
//! invariant under test is that decode(encode(decode(bytes))) == decode(bytes)
//! for a fixed schema.

#![no_main]

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_persist_types::Codec;
use mz_repr::{RelationDesc, Row, SqlColumnType, SqlScalarType};

/// Build a small, randomized non-empty `RelationDesc` from a prefix of the
/// input. The column types only affect the schema-directed decode via the
/// column *count* and iteration order (decode does not validate proto datums
/// against the types), but we still vary the types so the schema is realistic
/// and any type-sensitive code reachable through the desc gets exercised.
fn arb_relation_desc(u: &mut Unstructured) -> RelationDesc {
    // 1..=8 columns. Fall back to a single column if the buffer is exhausted so
    // we always test a genuinely non-empty schema.
    let ncols = u.int_in_range(1usize..=8).unwrap_or(1);
    let mut builder = RelationDesc::builder();
    for i in 0..ncols {
        let scalar_type = arb_scalar_type(u);
        let nullable = bool::arbitrary(u).unwrap_or(true);
        builder = builder.with_column(format!("c{i}"), SqlColumnType { scalar_type, nullable });
    }
    builder.finish()
}

/// Pick a scalar type for a schema column. Defaults to `String` when the buffer
/// is exhausted.
fn arb_scalar_type(u: &mut Unstructured) -> SqlScalarType {
    match u.int_in_range(0u8..=14).unwrap_or(0) {
        0 => SqlScalarType::Bool,
        1 => SqlScalarType::Int16,
        2 => SqlScalarType::Int32,
        3 => SqlScalarType::Int64,
        4 => SqlScalarType::UInt32,
        5 => SqlScalarType::UInt64,
        6 => SqlScalarType::Float32,
        7 => SqlScalarType::Float64,
        8 => SqlScalarType::String,
        9 => SqlScalarType::Bytes,
        10 => SqlScalarType::Date,
        11 => SqlScalarType::Time,
        12 => SqlScalarType::Timestamp { precision: None },
        13 => SqlScalarType::Numeric { max_scale: None },
        _ => SqlScalarType::Jsonb,
    }
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    // Derive the schema from the front of the buffer, then decode the rest as a
    // `ProtoRow` against that non-empty schema.
    let schema = arb_relation_desc(&mut u);
    let rest = u.take_rest();

    let Ok(orig) = Row::decode(rest, &schema) else {
        return;
    };
    let mut buf = Vec::new();
    orig.encode(&mut buf);
    let round = Row::decode(&buf, &schema).expect("re-encode of a valid Row must decode");
    assert_eq!(orig, round, "Row changed across Codec roundtrip (schema = {schema:?})");
});
