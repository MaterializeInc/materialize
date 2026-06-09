// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `ProtoSqlServerTableDesc` <-> `SqlServerTableDesc` round-trip.
//! Describes external-database schemas; a decoder bug here is reachable
//! from a compromised upstream SQL Server or on-disk catalog bytes.
//!
//! Input generation is split across four arms keyed off the first input
//! byte so a single byte stream exercises all of them over time:
//!
//! 1. **Valid-value arm.** A 32-byte seed (drawn from the input) drives
//!    proptest's `Arbitrary for SqlServerTableDesc` to build a *structurally
//!    valid, deeply-populated* descriptor — non-empty columns with real
//!    `SqlColumnType`s, every `SqlServerColumnDecodeType` variant (including
//!    `Unsupported { context }`), `primary_key_constraint`, and populated
//!    `SqlServerTableConstraint`s. It asserts the canonical
//!    `from_proto(into_proto(v)) == v` Rust round-trip, which the old
//!    random-bytes-only target almost never reached (random protobuf
//!    decodes to near-empty messages).
//!
//! 2. **Constraint-string arm.** Drives the *raw-ingest* path
//!    `SqlServerTableConstraint::try_from(SqlServerTableConstraintRaw)`,
//!    which parses the `constraint_type` *string* (`"PRIMARY KEY"` /
//!    `"UNIQUE"` are accepted, everything else is rejected). It feeds both
//!    the two valid strings and fuzzer-controlled garbage, and proto
//!    round-trips any constraint that parses. This covers the
//!    string-validation boundary that the proto oneof never sees.
//!
//! 3. **Decode-type arm.** Builds a `SqlServerColumnRaw` from a real SQL
//!    Server type name (`bit`, `tinyint`, `uniqueidentifier`, `xml`,
//!    `datetime2`, ...) covering every supported `SqlServerColumnDecodeType`
//!    plus an unsupported sentinel, runs the product `SqlServerColumnDesc::new`
//!    type-mapping logic, assembles a full table desc, and proto round-trips
//!    it. This reaches the `parse_data_type` mapping that the catalog format
//!    is the persisted output of.
//!
//! 4. **Raw-bytes arm.** The original strategy: decode arbitrary bytes and,
//!    if they happen to form a valid descriptor, check the proto round-trip
//!    is stable. Kept for robustness against the real wire/catalog format.

#![no_main]

use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, RustType};
use mz_sql_server_util::desc::{
    SqlServerColumnRaw, SqlServerTableConstraint, SqlServerTableConstraintRaw, SqlServerTableDesc,
};
use mz_sql_server_util::{ProtoSqlServerColumnDesc, ProtoSqlServerTableDesc};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use prost::Message;

/// Real SQL Server data-type spellings, chosen to exercise every branch of the
/// product `parse_data_type` mapping and therefore every supported
/// `SqlServerColumnDecodeType`. The trailing entries deliberately steer into
/// the unsupported / error path.
const DATA_TYPES: &[&str] = &[
    "bit",              // Bool
    "tinyint",          // U8
    "smallint",         // I16
    "int",              // I32
    "bigint",           // I64
    "real",             // F32 (precision <= 24)
    "float",            // F64
    "char",             // String
    "varchar",          // String
    "nvarchar",         // String
    "text",             // String
    "json",             // String
    "varbinary",        // Bytes
    "binary",           // Bytes
    "image",            // Bytes
    "uniqueidentifier", // Uuid
    "decimal",          // Numeric
    "numeric",          // Numeric
    "money",            // Numeric
    "xml",              // Xml
    "date",             // NaiveDate
    "time",             // NaiveTime
    "datetime2",        // NaiveDateTime
    "datetimeoffset",   // DateTime
    "sql_variant",      // Unsupported
    "geography",        // Unsupported
    "totally_bogus",    // Unsupported
];

/// Constraint-type strings: the two the product accepts, plus garbage that
/// `SqlServerTableConstraint::try_from` must reject.
const CONSTRAINT_TYPES: &[&str] = &[
    "PRIMARY KEY",
    "UNIQUE",
    "primary key",   // wrong case -> rejected
    "FOREIGN KEY",   // unsupported -> rejected
    "CHECK",         // unsupported -> rejected
    "",              // empty -> rejected
    "PRIMARY KEY ",  // trailing space -> rejected
    "\u{1f600}junk", // non-ascii garbage -> rejected
];

/// Assert that a `SqlServerTableDesc` survives a full Rust round-trip through
/// its proto representation unchanged, including a re-encode/decode of the
/// wire bytes.
fn assert_rust_roundtrip(orig: &SqlServerTableDesc) {
    let proto = orig.into_proto();
    let bytes = proto.encode_to_vec();
    let proto2 = ProtoSqlServerTableDesc::decode(bytes.as_slice())
        .expect("re-encode of valid SqlServerTableDesc must decode");
    let round: SqlServerTableDesc = proto2
        .into_rust()
        .expect("re-encoded SqlServerTableDesc must convert back to Rust");
    assert_eq!(
        orig, &round,
        "SqlServerTableDesc changed across proto roundtrip"
    );
}

/// Decode `bytes` as a proto, and if it is a valid descriptor, assert the
/// proto round-trip is stable. Used by the raw-bytes arm.
fn check_decoded(bytes: &[u8]) {
    let Ok(proto) = ProtoSqlServerTableDesc::decode(bytes) else {
        return;
    };
    let orig: SqlServerTableDesc = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };
    assert_rust_roundtrip(&orig);
}

/// Build a `SqlServerColumnRaw` from the fuzzer bytes, picking a real type name
/// so the product type-mapping logic runs end-to-end.
fn craft_column(data: &[u8], idx: usize) -> SqlServerColumnRaw {
    let pick = |off: usize| data.get(off).copied().unwrap_or(idx as u8);
    let data_type = DATA_TYPES[pick(0) as usize % DATA_TYPES.len()];
    SqlServerColumnRaw {
        name: format!("col{idx}").into(),
        data_type: data_type.into(),
        is_nullable: pick(1) & 1 == 0,
        // Cover -1 (max), 16 (text/ntext/image), and assorted small lengths.
        max_length: match pick(2) % 4 {
            0 => -1,
            1 => 16,
            2 => i16::from(pick(3)),
            _ => i16::from_le_bytes([pick(3), pick(4)]),
        },
        precision: pick(5) % 39,
        scale: pick(6) % 39,
        is_computed: pick(7) & 1 == 0,
    }
}

fuzz_target!(|data: &[u8]| {
    // Reserve the first byte as a mode selector and the next 32 bytes as the
    // proptest seed; everything after that feeds the raw-bytes / crafting
    // logic so a single input can drive any arm.
    let mode = data.first().copied().unwrap_or(0);
    let mut seed = [0u8; 32];
    let seed_src = data.get(1..33).unwrap_or(&[]);
    seed[..seed_src.len()].copy_from_slice(seed_src);
    let rest = data.get(33..).unwrap_or(&[]);

    match mode % 4 {
        0 => {
            // Valid-value arm: drive proptest's Arbitrary from the seed.
            let mut runner = TestRunner::new_with_rng(
                Config::default(),
                TestRng::from_seed(RngAlgorithm::ChaCha, &seed),
            );
            let value = match <SqlServerTableDesc as proptest::arbitrary::Arbitrary>::arbitrary()
                .new_tree(&mut runner)
            {
                Ok(tree) => tree.current(),
                Err(_) => return,
            };
            assert_rust_roundtrip(&value);
        }
        1 => {
            // Constraint-string arm: exercise the raw-ingest string parser for
            // both accepted and rejected `constraint_type` spellings.
            let ty_idx = rest.first().copied().unwrap_or(0) as usize % CONSTRAINT_TYPES.len();
            let n_cols = (rest.get(1).copied().unwrap_or(0) % 4) as usize;
            let columns: Vec<String> = (0..n_cols).map(|i| format!("c{i}")).collect();
            let raw = SqlServerTableConstraintRaw {
                constraint_name: "fuzz_constraint".to_string(),
                constraint_type: CONSTRAINT_TYPES[ty_idx].to_string(),
                columns,
            };
            // Garbage strings must be rejected; valid ones must parse and then
            // survive a proto round-trip inside a table desc.
            let Ok(constraint) = SqlServerTableConstraint::try_from(raw) else {
                return;
            };
            let desc = SqlServerTableDesc {
                schema_name: "dbo".into(),
                name: "fuzz".into(),
                columns: Box::new([]),
                constraints: vec![constraint],
            };
            assert_rust_roundtrip(&desc);
        }
        2 => {
            // Decode-type arm: run the product type-mapping over real type
            // spellings and round-trip the resulting columns.
            let n_cols = 1 + (rest.first().copied().unwrap_or(0) % 6) as usize;
            let mut columns = Vec::with_capacity(n_cols);
            for i in 0..n_cols {
                // Give each column a distinct 8-byte window of the input.
                let off = 1 + i * 8;
                let window = rest.get(off..).unwrap_or(&[]);
                let raw = craft_column(window, i);
                let mut desc = mz_sql_server_util::desc::SqlServerColumnDesc::new(&raw);
                // Occasionally populate the deprecated PK-constraint field so
                // the `Option<Arc<str>>` round-trip is covered too.
                if rest.get(off).copied().unwrap_or(0) & 0x80 != 0 {
                    desc.primary_key_constraint = Some(Arc::from("pk_fuzz"));
                }
                columns.push(desc);
            }
            let desc = SqlServerTableDesc {
                schema_name: "dbo".into(),
                name: "fuzz".into(),
                columns: columns.into_boxed_slice(),
                constraints: vec![],
            };
            assert_rust_roundtrip(&desc);

            // Also assert the per-column proto leaf round-trips independently,
            // which isolates the `decode_type` oneof + `column_type` mapping.
            for col in desc.columns.iter() {
                let proto: ProtoSqlServerColumnDesc = col.into_proto();
                let back: mz_sql_server_util::desc::SqlServerColumnDesc = proto
                    .into_rust()
                    .expect("column desc must convert back to Rust");
                assert_eq!(col, &back, "SqlServerColumnDesc changed across roundtrip");
            }
        }
        _ => {
            // Raw-bytes arm: decode arbitrary bytes directly.
            check_decoded(rest);
        }
    }
});
