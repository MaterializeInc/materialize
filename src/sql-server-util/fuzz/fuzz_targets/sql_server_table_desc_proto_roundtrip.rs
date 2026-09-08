// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `ProtoSqlServerTableDesc` <-> `SqlServerTableDesc` round-trip.
//! Describes external-database schemas, so a decoder bug here is reachable
//! from a compromised upstream SQL Server or on-disk catalog bytes.
//!
//! Input generation is split across four arms keyed off the first input
//! byte so a single byte stream exercises all of them over time:
//!
//! 1. **Valid-value arm.** A 32-byte seed (drawn from the input) drives
//!    proptest's `Arbitrary for SqlServerTableDesc` to build a *structurally
//!    valid, deeply-populated* descriptor. Non-empty columns with real
//!    `SqlColumnType`s, every `SqlServerColumnDecodeType` variant (including
//!    `Unsupported { context }`), `primary_key_constraint`, and populated
//!    `SqlServerTableConstraint`s. It asserts the canonical
//!    `from_proto(into_proto(v)) == v` Rust round-trip, which a
//!    random-bytes-only target almost never reaches (random protobuf
//!    decodes to near-empty messages).
//!
//! 2. **Constraint-string arm.** Drives the *raw-ingest* path
//!    `SqlServerTableConstraint::try_from(SqlServerTableConstraintRaw)`,
//!    which parses the `constraint_type` *string*. Only the exact spellings
//!    `"PRIMARY KEY"` and `"UNIQUE"` are accepted. The arm asserts both
//!    directions of that boundary and the variant each accepted spelling maps
//!    to, then proto round-trips the constraint inside a table desc.
//!
//!    The assertions are the point of the arm, not decoration. Its input space
//!    is only `CONSTRAINT_TYPES.len() * 4` fixed cases, so a round-trip-only
//!    oracle would contribute nothing beyond "does not panic", and it would be
//!    blind to all three ways this parser can regress: accepting a normalized
//!    or unknown spelling, rejecting a valid one (which makes the whole arm
//!    inert and indistinguishable from a passing arm), and swapping the two
//!    variants (which a round trip preserves). The parsed variant drives
//!    `is_primary` on the `TableConstraint::Unique` that purification writes
//!    into the generated subsource DDL, so misparsing a non-unique upstream
//!    constraint as `PrimaryKey` declares a key that does not hold.
//!
//! 3. **Decode-type arm.** Builds a `SqlServerColumnRaw` from a real SQL
//!    Server type name (`bit`, `tinyint`, `uniqueidentifier`, `xml`,
//!    `datetime2`, ...) covering every supported `SqlServerColumnDecodeType`
//!    plus an unsupported sentinel, runs the product `SqlServerColumnDesc::new`
//!    type-mapping logic, assembles a full table desc, and proto round-trips
//!    it. This reaches the `parse_data_type` mapping that the catalog format
//!    is the persisted output of.
//!
//! 4. **Raw-bytes arm.** Decode arbitrary bytes and, if they happen to form a
//!    valid descriptor, check the proto round-trip is stable. This guards
//!    robustness against the real wire/catalog format.

#![no_main]

use std::sync::{Arc, OnceLock};

use libfuzzer_sys::fuzz_target;
use mz_proto::{ProtoType, RustType};
use mz_sql_server_util::ProtoSqlServerTableDesc;
use mz_sql_server_util::desc::{
    SqlServerColumnDesc, SqlServerColumnRaw, SqlServerTableConstraint, SqlServerTableConstraintRaw,
    SqlServerTableConstraintType, SqlServerTableDesc,
};
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
    static DESC_STRATEGY: BoxedStrategy<SqlServerTableDesc> =
        <SqlServerTableDesc as proptest::arbitrary::Arbitrary>::arbitrary().boxed();
}

fn config() -> Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(Config::default).clone()
}

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
    "real",             // F32, selected by max_length == 4
    "float",            // F64, selected by max_length == 8
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
/// `SqlServerTableConstraint::try_from` must reject. Parsing is an exact match,
/// so the near-misses (case, whitespace) belong in the rejected set.
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
///
/// This covers each column and constraint too: `into_proto`/`from_proto`
/// delegate per element, and equality is structural, so a leaf that failed to
/// round-trip would show up here.
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
    // For the LOB types `text`/`ntext`/`image`, SQL Server's `sys.columns`
    // invariably reports `max_length = 16` (the size of the in-row root
    // pointer, not the data length), and `SqlServerColumnDesc::new` soft-asserts
    // exactly that. That assertion is a correct developer tripwire for a real
    // upstream invariant, so we must feed these types the length a live SQL
    // Server would actually report rather than synthesizing one it never
    // could, otherwise we trip the assertion on structurally-impossible input.
    // Every other type legitimately carries a range of lengths, so keep
    // fuzzing those across -1 (max), 16, and assorted small/arbitrary values.
    //
    // 4 and 8 are pinned because they are the *only* lengths that reach the
    // `F32` and `F64` decode types: `real`/`float`/`double precision` choose by
    // byte width rather than by name or precision. Leaving them to the two
    // random branches made two documented decode types a coincidence.
    let max_length = if matches!(data_type, "text" | "ntext" | "image") {
        16
    } else {
        match pick(2) % 6 {
            0 => -1,
            1 => 16,
            2 => 4,
            3 => 8,
            4 => i16::from(pick(3)),
            _ => i16::from_le_bytes([pick(3), pick(4)]),
        }
    };
    // Modulo 45, not 39: `parse_data_type` rejects a `precision` above 39, and
    // rejects a `scale` that `NumericMaxScale` cannot hold. Capping both at 38
    // made those two branches unreachable by construction rather than merely
    // rare. `SqlServerColumnDesc::new` turns either rejection into an
    // `Unsupported` decode type, which still round-trips.
    SqlServerColumnRaw {
        name: format!("col{idx}").into(),
        data_type: data_type.into(),
        is_nullable: pick(1) & 1 == 0,
        max_length,
        precision: pick(5) % 45,
        scale: pick(6) % 45,
        is_computed: pick(7) & 1 == 0,
    }
}

fuzz_target!(|data: &[u8]| {
    // The first byte selects the arm. Every arm then reads from byte 1 on:
    // only the proptest arm consumes a seed, and it is the one that consumes it,
    // so there is no shared reservation to skip past. Carving out a fixed
    // 32-byte seed window for all four arms would starve the other three, since
    // libFuzzer grows inputs up from empty and their selector bytes would sit
    // past the window at a length it takes a long time to reach. It would also
    // decapitate any genuine encoded `ProtoSqlServerTableDesc` dropped into the
    // corpus (which `--corpus-sync` accumulates) before the raw-bytes arm saw it.
    let mode = data.first().copied().unwrap_or(0);
    let tail = data.get(1..).unwrap_or(&[]);

    match mode % 4 {
        0 => {
            // Valid-value arm: drive proptest's Arbitrary from the seed. Padded
            // with zeros rather than requiring all 32 bytes, so short inputs
            // still steer generation instead of all reusing the zero seed.
            let mut seed = [0u8; 32];
            let n = tail.len().min(32);
            seed[..n].copy_from_slice(&tail[..n]);
            let mut runner =
                TestRunner::new_with_rng(config(), TestRng::from_seed(RngAlgorithm::ChaCha, &seed));
            let value = match DESC_STRATEGY.with(|s| s.new_tree(&mut runner)) {
                Ok(tree) => tree.current(),
                Err(_) => return,
            };
            assert_rust_roundtrip(&value);
        }
        1 => {
            // Constraint-string arm: exercise the raw-ingest string parser for
            // both accepted and rejected `constraint_type` spellings.
            let ty_idx = tail.first().copied().unwrap_or(0) as usize % CONSTRAINT_TYPES.len();
            let ty = CONSTRAINT_TYPES[ty_idx];
            let n_cols = (tail.get(1).copied().unwrap_or(0) % 4) as usize;
            let column_names: Vec<String> = (0..n_cols).map(|i| format!("c{i}")).collect();
            let raw = SqlServerTableConstraintRaw {
                constraint_name: "fuzz_constraint".to_string(),
                constraint_type: ty.to_string(),
                columns: column_names.clone(),
            };
            let parsed = SqlServerTableConstraint::try_from(raw);

            let expected = match ty {
                "PRIMARY KEY" => Some(SqlServerTableConstraintType::PrimaryKey),
                "UNIQUE" => Some(SqlServerTableConstraintType::Unique),
                _ => None,
            };
            let Some(expected) = expected else {
                assert!(
                    parsed.is_err(),
                    "constraint_type {ty:?} must be rejected, parsed as {parsed:?}"
                );
                return;
            };
            let constraint = parsed.expect("an accepted constraint_type must parse");
            assert_eq!(
                constraint.constraint_type, expected,
                "constraint_type {ty:?} mapped to the wrong variant"
            );

            // Give the desc the columns its constraint names, so the descriptor
            // is structurally consistent. A future consistency check in
            // `from_proto` would otherwise report this arm's own inputs.
            let columns: Box<[SqlServerColumnDesc]> = column_names
                .iter()
                .map(|name| {
                    SqlServerColumnDesc::new(&SqlServerColumnRaw {
                        name: name.as_str().into(),
                        data_type: "int".into(),
                        is_nullable: false,
                        max_length: 4,
                        precision: 0,
                        scale: 0,
                        is_computed: false,
                    })
                })
                .collect();
            let desc = SqlServerTableDesc {
                schema_name: "dbo".into(),
                name: "fuzz".into(),
                columns,
                constraints: vec![constraint],
            };
            assert_rust_roundtrip(&desc);
        }
        2 => {
            // Decode-type arm: run the product type-mapping over real type
            // spellings and round-trip the resulting columns.
            let n_cols = 1 + (tail.first().copied().unwrap_or(0) % 6) as usize;
            let mut columns = Vec::with_capacity(n_cols);
            for i in 0..n_cols {
                // Give each column a distinct 8-byte window of the input.
                let off = 1 + i * 8;
                let window = tail.get(off..).unwrap_or(&[]);
                let raw = craft_column(window, i);
                let mut desc = SqlServerColumnDesc::new(&raw);
                // Occasionally populate the deprecated PK-constraint field so
                // the `Option<Arc<str>>` round-trip is covered too.
                if tail.get(off).copied().unwrap_or(0) & 0x80 != 0 {
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
        }
        _ => {
            // Raw-bytes arm: decode arbitrary bytes directly.
            check_decoded(tail);
        }
    }
});
