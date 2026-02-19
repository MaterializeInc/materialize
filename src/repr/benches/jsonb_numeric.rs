// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for JSONB numeric serialization.
//!
//! Measures the cost of serializing Numeric values within JSONB.
//! The optimization uses NumericStackStr (zero heap allocations) instead of
//! to_standard_notation_string() (2 heap allocations per value).

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dec::Context;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::{Numeric, NumericStackStr};
use mz_repr::{Datum, Row};

fn parse_numeric(s: &str) -> Numeric {
    let mut cx = Context::<Numeric>::default();
    cx.parse(s).unwrap()
}

fn make_test_values() -> Vec<(&'static str, Numeric)> {
    vec![
        ("zero", parse_numeric("0")),
        ("one", parse_numeric("1")),
        ("small_int", parse_numeric("42")),
        ("decimal", parse_numeric("123.456789")),
        ("negative", parse_numeric("-3.14159265358979323846264338327950288")),
        ("tiny", parse_numeric("0.000001")),
        ("large_int", parse_numeric("999999999999999999999999999999999999999")),
        ("small_exp", parse_numeric("1E+10")),
        ("neg_exp", parse_numeric("1E-10")),
        ("pi", parse_numeric("3.14159265358979323846264338327950288")),
    ]
}

/// Build a JSONB row containing a single numeric value: {"value": <n>}.
fn make_jsonb_row(n: &Numeric) -> Row {
    let mut row = Row::default();
    row.packer().push_dict_with(|packer| {
        packer.push(Datum::String("value"));
        packer.push(Datum::from(n.clone()));
    });
    row
}

/// Isolated formatting comparison: old to_standard_notation_string() vs new NumericStackStr.
fn bench_format_comparison(c: &mut Criterion) {
    let values = make_test_values();

    let mut group = c.benchmark_group("jsonb_numeric_format");
    for (name, val) in &values {
        group.bench_with_input(BenchmarkId::new("old_to_string", name), val, |b, v| {
            b.iter(|| {
                let s = black_box(v).to_standard_notation_string();
                black_box(s);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_stack_str", name), val, |b, v| {
            b.iter(|| {
                let buf = NumericStackStr::new(black_box(v));
                black_box(buf.as_str());
            });
        });
    }
    group.finish();
}

/// Serialization comparison: old approach (serialize heap String) vs new (serialize stack &str).
/// This simulates the exact serde_json number serialization path used in JSONB.
fn bench_serde_comparison(c: &mut Criterion) {
    let token = "$serde_json::private::Number";
    let values = make_test_values();

    let mut group = c.benchmark_group("jsonb_numeric_serde");
    for (name, val) in &values {
        group.bench_with_input(BenchmarkId::new("old_heap", name), val, |b, v| {
            b.iter(|| {
                let s = black_box(v).to_standard_notation_string();
                let json = serde_json::to_string(&{
                    // Simulate the magic struct serialization
                    let mut map = serde_json::Map::new();
                    map.insert(
                        token.to_string(),
                        serde_json::Value::String(s),
                    );
                    map
                })
                .unwrap();
                black_box(json);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_stack", name), val, |b, v| {
            b.iter(|| {
                let buf = NumericStackStr::new(black_box(v));
                let json = serde_json::to_string(&{
                    let mut map = serde_json::Map::new();
                    map.insert(
                        token.to_string(),
                        serde_json::Value::String(buf.as_str().to_owned()),
                    );
                    map
                })
                .unwrap();
                black_box(json);
            });
        });
    }
    group.finish();
}

/// End-to-end JSONB serialization: to_serde_json and to_string on JSONB rows.
fn bench_jsonb_serialize(c: &mut Criterion) {
    let values = make_test_values();

    let mut group = c.benchmark_group("jsonb_numeric_e2e");
    for (name, val) in &values {
        let row = make_jsonb_row(val);
        let datum = row.unpack_first();
        group.bench_with_input(
            BenchmarkId::new("to_serde_json", name),
            &datum,
            |b, d| {
                b.iter(|| {
                    let jsonb_ref = JsonbRef::from_datum(black_box(*d));
                    let json_value = jsonb_ref.to_serde_json();
                    black_box(json_value);
                });
            },
        );
        group.bench_with_input(BenchmarkId::new("to_string", name), &datum, |b, d| {
            b.iter(|| {
                let jsonb_ref = JsonbRef::from_datum(black_box(*d));
                let s = jsonb_ref.to_string();
                black_box(s);
            });
        });
    }
    group.finish();
}

/// Batch benchmark: serialize 10k JSONB numeric values.
fn bench_jsonb_batch(c: &mut Criterion) {
    let all_values = make_test_values();
    let rows: Vec<Row> = (0..10_000)
        .map(|i| make_jsonb_row(&all_values[i % all_values.len()].1))
        .collect();

    let mut group = c.benchmark_group("jsonb_numeric_batch");

    group.bench_function("to_serde_json_10k", |b| {
        b.iter(|| {
            for row in &rows {
                let datum = row.unpack_first();
                let jsonb_ref = JsonbRef::from_datum(black_box(datum));
                let json_value = jsonb_ref.to_serde_json();
                black_box(json_value);
            }
        });
    });

    group.bench_function("to_string_10k", |b| {
        b.iter(|| {
            let mut buf = String::with_capacity(64);
            for row in &rows {
                buf.clear();
                let datum = row.unpack_first();
                let jsonb_ref = JsonbRef::from_datum(black_box(datum));
                std::fmt::Write::write_fmt(&mut buf, format_args!("{}", jsonb_ref)).unwrap();
                black_box(&buf);
            }
        });
    });

    // Batch comparison: old formatting vs new
    let numerics: Vec<Numeric> = (0..10_000)
        .map(|i| all_values[i % all_values.len()].1.clone())
        .collect();

    group.bench_function("format_old_10k", |b| {
        b.iter(|| {
            for v in &numerics {
                let s = black_box(v).to_standard_notation_string();
                black_box(s);
            }
        });
    });

    group.bench_function("format_new_10k", |b| {
        b.iter(|| {
            for v in &numerics {
                let buf = NumericStackStr::new(black_box(v));
                black_box(buf.as_str());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_format_comparison,
    bench_serde_comparison,
    bench_jsonb_serialize,
    bench_jsonb_batch,
);
criterion_main!(benches);
