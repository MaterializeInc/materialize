// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for JSONB formatting (format_jsonb).
//!
//! Compares two approaches:
//! - Old: format_jsonb → write!(buf, "{}", jsonb) → serde_json::to_writer
//!   (creates Serializer state machine, WriterFormatter adapter, fmt::Formatter)
//! - New: format_jsonb → direct match on Datum type, write directly to buffer
//!   (bypasses serde_json entirely for simple scalar types)

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dec::Context;
use mz_ore::fmt::FormatBuffer;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::Numeric;
use mz_repr::strconv;
use mz_repr::{Datum, Row};

fn parse_numeric(s: &str) -> Numeric {
    let mut cx = Context::<Numeric>::default();
    cx.parse(s).unwrap()
}

/// Build test JSONB datums as rows (since Datum borrows from Row).
fn make_test_rows() -> Vec<(&'static str, Row)> {
    let mut result = Vec::new();

    // Null
    result.push(("null", Row::pack_slice(&[Datum::JsonNull])));

    // Booleans
    result.push(("true", Row::pack_slice(&[Datum::True])));
    result.push(("false", Row::pack_slice(&[Datum::False])));

    // Numeric values
    let numerics = [
        ("num_zero", "0"),
        ("num_one", "1"),
        ("num_42", "42"),
        ("num_pi", "3.14159265358979323846264338327950288"),
        ("num_negative", "-99.99"),
        ("num_tiny", "0.000001"),
        ("num_large", "999999999999999999999999999999999999999"),
    ];
    for (name, s) in &numerics {
        let n = parse_numeric(s);
        result.push((name, Row::pack_slice(&[Datum::from(n)])));
    }

    // String values - no escaping
    let clean_strings = [
        ("str_empty", ""),
        ("str_hello", "hello"),
        ("str_medium", "the quick brown fox jumps over the lazy dog"),
        ("str_long", &"abcdefghij".repeat(20)),
    ];
    for (name, s) in &clean_strings {
        result.push((name, Row::pack_slice(&[Datum::String(s)])));
    }

    // String values - with escaping
    result.push(("str_quote", Row::pack_slice(&[Datum::String("hello\"world")])));
    result.push((
        "str_backslash",
        Row::pack_slice(&[Datum::String("path\\to\\file")]),
    ));
    result.push((
        "str_newline",
        Row::pack_slice(&[Datum::String("line1\nline2\nline3")]),
    ));
    result.push((
        "str_mixed_esc",
        Row::pack_slice(&[Datum::String("tab\there\nnewline\"quote")]),
    ));

    result
}

/// Old approach: serde_json via Display.
fn format_old(jsonb: JsonbRef, buf: &mut String) {
    std::fmt::Write::write_fmt(buf, format_args!("{}", jsonb)).unwrap();
}

/// New approach: format_jsonb (which now has the direct fast path).
fn format_new<F: FormatBuffer>(jsonb: JsonbRef, buf: &mut F) {
    strconv::format_jsonb(buf, jsonb);
}

/// Per-value benchmarks comparing old (serde_json) vs new (direct).
fn bench_per_value(c: &mut Criterion) {
    let rows = make_test_rows();

    let mut group = c.benchmark_group("jsonb_format");
    for (name, row) in &rows {
        let datum = row.unpack_first();

        group.bench_with_input(BenchmarkId::new("old_serde", name), &datum, |b, d| {
            let mut buf = String::with_capacity(256);
            b.iter(|| {
                buf.clear();
                format_old(JsonbRef::from_datum(black_box(*d)), &mut buf);
                black_box(&buf);
            });
        });

        group.bench_with_input(BenchmarkId::new("new_direct", name), &datum, |b, d| {
            let mut buf = String::with_capacity(256);
            b.iter(|| {
                buf.clear();
                format_new(JsonbRef::from_datum(black_box(*d)), &mut buf);
                black_box(&buf);
            });
        });
    }
    group.finish();
}

/// Batch benchmark: 10k mixed JSONB values.
fn bench_batch(c: &mut Criterion) {
    let rows = make_test_rows();
    let batch_rows: Vec<Row> = (0..10_000)
        .map(|i| rows[i % rows.len()].1.clone())
        .collect();

    let mut group = c.benchmark_group("jsonb_format_batch");

    group.bench_function("old_serde_10k", |b| {
        let mut buf = String::with_capacity(256);
        b.iter(|| {
            for row in &batch_rows {
                buf.clear();
                let datum = row.unpack_first();
                format_old(JsonbRef::from_datum(black_box(datum)), &mut buf);
                black_box(&buf);
            }
        });
    });

    group.bench_function("new_direct_10k", |b| {
        let mut buf = String::with_capacity(256);
        b.iter(|| {
            for row in &batch_rows {
                buf.clear();
                let datum = row.unpack_first();
                format_new(JsonbRef::from_datum(black_box(datum)), &mut buf);
                black_box(&buf);
            }
        });
    });

    group.finish();
}

/// Batch with only simple scalar values (null, bool, number, string).
fn bench_batch_scalars(c: &mut Criterion) {
    // Create a batch of only scalar values (the fast-path targets)
    let scalar_rows: Vec<Row> = vec![
        Row::pack_slice(&[Datum::JsonNull]),
        Row::pack_slice(&[Datum::True]),
        Row::pack_slice(&[Datum::False]),
        Row::pack_slice(&[Datum::from(parse_numeric("42"))]),
        Row::pack_slice(&[Datum::from(parse_numeric("3.14"))]),
        Row::pack_slice(&[Datum::from(parse_numeric("0"))]),
        Row::pack_slice(&[Datum::String("hello")]),
        Row::pack_slice(&[Datum::String("world")]),
        Row::pack_slice(&[Datum::String("")]),
        Row::pack_slice(&[Datum::from(parse_numeric("-99.99"))]),
    ];
    let batch: Vec<Row> = (0..10_000)
        .map(|i| scalar_rows[i % scalar_rows.len()].clone())
        .collect();

    let mut group = c.benchmark_group("jsonb_format_scalars");

    group.bench_function("old_serde_10k", |b| {
        let mut buf = String::with_capacity(256);
        b.iter(|| {
            for row in &batch {
                buf.clear();
                let datum = row.unpack_first();
                format_old(JsonbRef::from_datum(black_box(datum)), &mut buf);
                black_box(&buf);
            }
        });
    });

    group.bench_function("new_direct_10k", |b| {
        let mut buf = String::with_capacity(256);
        b.iter(|| {
            for row in &batch {
                buf.clear();
                let datum = row.unpack_first();
                format_new(JsonbRef::from_datum(black_box(datum)), &mut buf);
                black_box(&buf);
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_per_value, bench_batch, bench_batch_scalars);
criterion_main!(benches);
