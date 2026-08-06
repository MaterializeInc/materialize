// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coverage-guided check of the persist filter pushdown soundness property:
//! a part whose rows produce output under an MFP must never be reported as
//! irrelevant by [`RelationPartStats::may_match_mfp`], the pushdown entry
//! point of the peek path.
//!
//! The whole pipeline runs on real production code: rows are packed into a
//! persist part, the part's column statistics are computed by the write
//! path, and the interpreter consumes them exactly as pushdown does. The
//! fuzzer's value over the proptest harnesses in `mz_storage_operators`'s
//! `filter_pushdown_audit` module is raw bit-pattern access: float columns
//! take arbitrary `u64` bit patterns (every NaN payload and sign, subnormals)
//! and strings take arbitrary bytes, while coverage feedback steers the
//! search toward rarely-taken stats and interpreter branches.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use mz_expr::func::variadic::{And, Or};
use mz_expr::func::{
    AddFloat64, Eq, Gt, Gte, IsNull, JsonbGetString, JsonbGetStringStringify, Lt, Lte, MulFloat64,
    Not,
};
use mz_expr::{BinaryFunc, MapFilterProject, MirScalarExpr, ResultSpec, UnaryFunc};
use mz_ore::metrics::MetricsRegistry;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::part::PartBuilder;
use mz_persist_types::stats::{PartStats, PartStatsMetrics};
use mz_repr::adt::numeric::Numeric;
use mz_repr::{
    Datum, Diff, RelationDesc, ReprScalarType, Row, RowArena, SqlScalarType, Timestamp,
};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_storage_types::stats::RelationPartStats;

const NUM: usize = 0;
const F64: usize = 1;
const STR: usize = 2;
const JSON: usize = 3;
const BOOL: usize = 4;
const ARITY: usize = 5;

#[derive(Arbitrary, Debug)]
struct FuzzRow {
    num: FuzzDatum,
    f64_bits: FuzzDatum,
    string: FuzzDatum,
    json: FuzzDatum,
    bool_null: FuzzDatum,
}

/// A datum source: raw bits for coverage-guided exploration, or an index into
/// the column type's `interesting_datums` pool for the known poison values.
#[derive(Arbitrary, Debug)]
enum FuzzDatum {
    Null,
    Bits(u64),
    Bytes([u8; 8]),
    Pool(u8),
}

#[derive(Arbitrary, Debug)]
enum Cmp {
    Lt,
    Lte,
    Gt,
    Gte,
    Eq,
}

#[derive(Arbitrary, Debug)]
enum Pred {
    /// `col <cmp> lit`, the literal drawn like a row value.
    CmpColLit { col: u8, cmp: Cmp, lit: FuzzDatum },
    IsNull { col: u8, negate: bool },
    /// `(json ->(>) 'key') IS NULL` over the Nested specs from real map stats.
    JsonbKey { key: u8, stringify: bool },
    /// `(c_f64 + a) * b <cmp> c`, aimed at the infinity guard and NaN
    /// arithmetic, with fuzzer-chosen bit patterns.
    FloatArith { cmp: Cmp, a: u64, b: u64, c: u64 },
    And(Box<Pred>, Box<Pred>),
    Or(Box<Pred>, Box<Pred>),
    Not(Box<Pred>),
}

#[derive(Arbitrary, Debug)]
struct Input {
    rows: Vec<FuzzRow>,
    /// Bitmask of error rows appended to the part.
    err_rows: u8,
    pred: Pred,
}

fn schema() -> RelationDesc {
    RelationDesc::builder()
        .with_column("c_num", SqlScalarType::Numeric { max_scale: None }.nullable(true))
        .with_column("c_f64", SqlScalarType::Float64.nullable(true))
        .with_column("c_str", SqlScalarType::String.nullable(true))
        .with_column("c_json", SqlScalarType::Jsonb.nullable(true))
        .with_column("c_bool", SqlScalarType::Bool.nullable(true))
        .finish()
}

fn col_type(col: usize) -> SqlScalarType {
    match col {
        NUM => SqlScalarType::Numeric { max_scale: None },
        F64 => SqlScalarType::Float64,
        STR => SqlScalarType::String,
        JSON => SqlScalarType::Jsonb,
        BOOL => SqlScalarType::Bool,
        _ => unreachable!(),
    }
}

fn repr_type(col: usize) -> ReprScalarType {
    match col {
        NUM => ReprScalarType::Numeric,
        F64 => ReprScalarType::Float64,
        STR => ReprScalarType::String,
        JSON => ReprScalarType::Jsonb,
        BOOL => ReprScalarType::Bool,
        _ => unreachable!(),
    }
}

/// Materialize a datum for `col` into the packer.
fn push_datum(packer: &mut mz_repr::RowPacker, col: usize, d: &FuzzDatum) {
    match d {
        FuzzDatum::Null => packer.push(Datum::Null),
        FuzzDatum::Bits(bits) => match col {
            NUM => packer.push(Datum::from(Numeric::from(f64::from_bits(*bits)))),
            F64 => packer.push(Datum::from(f64::from_bits(*bits))),
            STR | JSON => packer.push(Datum::String(if bits % 2 == 0 { "a" } else { "b" })),
            BOOL => packer.push(Datum::from(*bits % 2 == 0)),
            _ => unreachable!(),
        },
        FuzzDatum::Bytes(bytes) => match col {
            STR => packer.push(Datum::String(
                std::str::from_utf8(bytes).unwrap_or("\u{fffd}"),
            )),
            _ => push_pool(packer, col, bytes[0]),
        },
        FuzzDatum::Pool(idx) => push_pool(packer, col, *idx),
    }
}

fn push_pool(packer: &mut mz_repr::RowPacker, col: usize, idx: u8) {
    let pool: Vec<Datum<'static>> = col_type(col).interesting_datums().collect();
    if pool.is_empty() {
        packer.push(Datum::Null);
    } else {
        packer.push(pool[idx as usize % pool.len()]);
    }
}

fn lit(col: usize, d: &FuzzDatum) -> MirScalarExpr {
    let mut row = Row::default();
    push_datum(&mut row.packer(), col, d);
    let datum = row.iter().next().unwrap();
    if datum.is_null() {
        MirScalarExpr::literal_null(repr_type(col))
    } else {
        MirScalarExpr::literal_ok(datum, repr_type(col))
    }
}

fn cmp_func(cmp: &Cmp) -> BinaryFunc {
    match cmp {
        Cmp::Lt => BinaryFunc::Lt(Lt),
        Cmp::Lte => BinaryFunc::Lte(Lte),
        Cmp::Gt => BinaryFunc::Gt(Gt),
        Cmp::Gte => BinaryFunc::Gte(Gte),
        Cmp::Eq => BinaryFunc::Eq(Eq),
    }
}

fn binary(func: BinaryFunc, a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
    MirScalarExpr::CallBinary {
        func,
        expr1: Box::new(a),
        expr2: Box::new(b),
    }
}

fn f64_lit(bits: u64) -> MirScalarExpr {
    MirScalarExpr::literal_ok(Datum::from(f64::from_bits(bits)), ReprScalarType::Float64)
}

fn build_pred(pred: &Pred) -> MirScalarExpr {
    match pred {
        Pred::CmpColLit { col, cmp, lit: l } => {
            let col = *col as usize % ARITY;
            binary(cmp_func(cmp), MirScalarExpr::column(col), lit(col, l))
        }
        Pred::IsNull { col, negate } => {
            let expr = MirScalarExpr::CallUnary {
                func: UnaryFunc::IsNull(IsNull),
                expr: Box::new(MirScalarExpr::column(*col as usize % ARITY)),
            };
            if *negate {
                MirScalarExpr::CallUnary {
                    func: UnaryFunc::Not(Not),
                    expr: Box::new(expr),
                }
            } else {
                expr
            }
        }
        Pred::JsonbKey { key, stringify } => {
            let keys = ["x", "y", "nested", "absent"];
            let func = if *stringify {
                BinaryFunc::JsonbGetStringStringify(JsonbGetStringStringify)
            } else {
                BinaryFunc::JsonbGetString(JsonbGetString)
            };
            let get = binary(
                func,
                MirScalarExpr::column(JSON),
                MirScalarExpr::literal_ok(
                    Datum::String(keys[*key as usize % keys.len()]),
                    ReprScalarType::String,
                ),
            );
            MirScalarExpr::CallUnary {
                func: UnaryFunc::IsNull(IsNull),
                expr: Box::new(get),
            }
        }
        Pred::FloatArith { cmp, a, b, c } => {
            let add = binary(
                BinaryFunc::AddFloat64(AddFloat64),
                MirScalarExpr::column(F64),
                f64_lit(*a),
            );
            let mul = binary(BinaryFunc::MulFloat64(MulFloat64), add, f64_lit(*b));
            binary(cmp_func(cmp), mul, f64_lit(*c))
        }
        Pred::And(a, b) => MirScalarExpr::CallVariadic {
            func: And.into(),
            exprs: vec![build_pred(a), build_pred(b)],
        },
        Pred::Or(a, b) => MirScalarExpr::CallVariadic {
            func: Or.into(),
            exprs: vec![build_pred(a), build_pred(b)],
        },
        Pred::Not(a) => MirScalarExpr::CallUnary {
            func: UnaryFunc::Not(Not),
            expr: Box::new(build_pred(a)),
        },
    }
}

fn check(input: Input) {
    let desc = schema();

    let mut rows = Vec::new();
    for fuzz_row in input.rows.iter().take(8) {
        let mut row = Row::default();
        let mut packer = row.packer();
        for (col, datum) in [
            &fuzz_row.num,
            &fuzz_row.f64_bits,
            &fuzz_row.string,
            &fuzz_row.json,
            &fuzz_row.bool_null,
        ]
        .into_iter()
        .enumerate()
        {
            push_datum(&mut packer, col, datum);
        }
        drop(packer);
        rows.push(SourceData(Ok(row)));
    }
    for _ in 0..input.err_rows.count_ones().min(2) {
        rows.push(SourceData(Err(DataflowError::from(
            mz_expr::EvalError::DivisionByZero,
        ))));
    }
    if rows.is_empty() {
        return;
    }

    let mfp = MapFilterProject::new(ARITY).filter(std::iter::once(build_pred(&input.pred)));
    let Ok(plan) = mfp.clone().into_plan() else {
        return;
    };

    let mut builder = PartBuilder::new(&desc, &UnitSchema);
    for row in &rows {
        builder.push(row, &(), 1u64, 1i64);
    }
    let part = builder.finish();
    let part_stats = PartStats::new::<SourceData, RelationDesc>(&part, &desc).expect("stats");
    let metrics = PartStatsMetrics::new(&MetricsRegistry::new());
    let stats = RelationPartStats::new("fuzz", &metrics, &desc, &part_stats);

    // Ground truth: does any row produce output? Error rows always surface.
    let arena = RowArena::new();
    let mut row_builder = Row::default();
    let yields_output = rows.iter().any(|source_data| match &source_data.0 {
        Err(_) => true,
        Ok(row) => {
            let mut datums: Vec<Datum> = row.iter().collect();
            plan.evaluate::<DataflowError, _>(
                &mut datums,
                &arena,
                Timestamp::MIN,
                Diff::from(1),
                |_| true,
                &mut row_builder,
            )
            .next()
            .is_some()
        }
    });

    if yields_output {
        assert!(
            stats.may_match_mfp(ResultSpec::anything(), &mfp),
            "pushdown claims no row can match, but the MFP yields output on a real row \
             (wrongly-skipped part)\nrows={rows:?}\nmfp={mfp:?}",
        );
    }
}

fuzz_target!(|input: Input| check(input));
