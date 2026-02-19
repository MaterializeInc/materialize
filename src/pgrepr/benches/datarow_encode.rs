// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks comparing DataRow encoding approaches:
//!
//! - Old: `values_from_row()` creates `Vec<Option<Value>>`, then each Value is
//!   encoded to BytesMut via `Value::encode_text()`. This allocates the Vec per
//!   row and clones all String/bytes data into owned Value variants.
//!
//! - New: `encode_data_row_direct()` encodes directly from Datum to BytesMut,
//!   skipping the intermediate Value representation. No Vec allocation, no
//!   String/bytes cloning.

use std::hint::black_box;

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_pgrepr::{Type, encode_data_row_direct, values_from_row};
use mz_pgwire_common::Format;
use mz_repr::{Datum, Row, SqlColumnType, SqlRelationType, SqlScalarType};

/// Build a row with the specified datums.
fn build_row(datums: &[Datum<'_>]) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    for d in datums {
        packer.push(*d);
    }
    row
}

/// Build SqlRelationType from scalar types.
fn build_rel_type(types: &[SqlScalarType]) -> SqlRelationType {
    SqlRelationType {
        column_types: types
            .iter()
            .map(|t| SqlColumnType {
                scalar_type: t.clone(),
                nullable: true,
            })
            .collect(),
        keys: vec![],
    }
}

/// Build encode_state for all-text format.
fn build_encode_state(types: &[SqlScalarType]) -> Vec<(Type, Format)> {
    types
        .iter()
        .map(|t| (Type::from(t), Format::Text))
        .collect()
}

/// Old path: values_from_row + Value::encode_text for each value.
fn encode_old(row: &mz_repr::RowRef, typ: &SqlRelationType, dst: &mut BytesMut) {
    let values = values_from_row(row, typ);
    // Simulate DataRow encoding: field count + per-field encoding.
    dst.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for v in &values {
        match v {
            Some(v) => {
                let base = dst.len();
                dst.extend_from_slice(&0i32.to_be_bytes());
                v.encode_text(dst);
                let len = (dst.len() - base - 4) as i32;
                dst[base..base + 4].copy_from_slice(&len.to_be_bytes());
            }
            None => {
                dst.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }
    }
}

fn bench_datarow_encode(c: &mut Criterion) {
    // Scenario 1: Integers only (3 i32 + 2 i64)
    let int_datums: Vec<Datum<'_>> = vec![
        Datum::Int32(42),
        Datum::Int32(1_000_000),
        Datum::Int32(-99),
        Datum::Int64(9_876_543_210),
        Datum::Int64(-42),
    ];
    let int_types = vec![
        SqlScalarType::Int32,
        SqlScalarType::Int32,
        SqlScalarType::Int32,
        SqlScalarType::Int64,
        SqlScalarType::Int64,
    ];
    let int_row = build_row(&int_datums);
    let int_rel = build_rel_type(&int_types);
    let int_enc = build_encode_state(&int_types);

    // Scenario 2: Mixed types with strings (typical OLTP row)
    let mixed_datums: Vec<Datum<'_>> = vec![
        Datum::Int32(1),
        Datum::String("hello world this is a test string"),
        Datum::Int64(9_876_543_210),
        Datum::String("another column with text data here"),
        Datum::True,
    ];
    let mixed_types = vec![
        SqlScalarType::Int32,
        SqlScalarType::String,
        SqlScalarType::Int64,
        SqlScalarType::String,
        SqlScalarType::Bool,
    ];
    let mixed_row = build_row(&mixed_datums);
    let mixed_rel = build_rel_type(&mixed_types);
    let mixed_enc = build_encode_state(&mixed_types);

    // Scenario 3: String-heavy (5 text columns)
    let str_datums: Vec<Datum<'_>> = vec![
        Datum::String("The quick brown fox jumps over the lazy dog"),
        Datum::String("Lorem ipsum dolor sit amet consectetur"),
        Datum::String("abcdefghijklmnopqrstuvwxyz0123456789"),
        Datum::String("short"),
        Datum::String("a somewhat longer string that has more characters in it"),
    ];
    let str_types = vec![SqlScalarType::String; 5];
    let str_row = build_row(&str_datums);
    let str_rel = build_rel_type(&str_types);
    let str_enc = build_encode_state(&str_types);

    // Scenario 4: Wide row (15 columns mixed)
    let wide_datums: Vec<Datum<'_>> = vec![
        Datum::Int32(1),
        Datum::Int32(2),
        Datum::Int32(3),
        Datum::Int64(100),
        Datum::Int64(200),
        Datum::String("col_text_1"),
        Datum::String("col_text_2_longer_value"),
        Datum::String("col_text_3_even_longer_text_value_here"),
        Datum::True,
        Datum::False,
        Datum::Float64(3.14159.into()),
        Datum::Float64(2.71828.into()),
        Datum::Int32(42),
        Datum::String("final_text"),
        Datum::Int64(999_999),
    ];
    let wide_types = vec![
        SqlScalarType::Int32,
        SqlScalarType::Int32,
        SqlScalarType::Int32,
        SqlScalarType::Int64,
        SqlScalarType::Int64,
        SqlScalarType::String,
        SqlScalarType::String,
        SqlScalarType::String,
        SqlScalarType::Bool,
        SqlScalarType::Bool,
        SqlScalarType::Float64,
        SqlScalarType::Float64,
        SqlScalarType::Int32,
        SqlScalarType::String,
        SqlScalarType::Int64,
    ];
    let wide_row = build_row(&wide_datums);
    let wide_rel = build_rel_type(&wide_types);
    let wide_enc = build_encode_state(&wide_types);

    // Per-row benchmarks
    let mut group = c.benchmark_group("datarow_per_row");
    let scenarios: Vec<(&str, &Row, &SqlRelationType, &[(Type, Format)])> = vec![
        ("integers_5col", &int_row, &int_rel, &int_enc),
        ("mixed_5col", &mixed_row, &mixed_rel, &mixed_enc),
        ("strings_5col", &str_row, &str_rel, &str_enc),
        ("wide_15col", &wide_row, &wide_rel, &wide_enc),
    ];

    for (name, row, rel, enc) in &scenarios {
        group.bench_with_input(BenchmarkId::new("old_values_from_row", name), row, |b, row| {
            let mut dst = BytesMut::with_capacity(512);
            b.iter(|| {
                dst.clear();
                encode_old(black_box(row.as_ref()), rel, &mut dst);
                black_box(&dst);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_direct_encode", name), row, |b, row| {
            let mut dst = BytesMut::with_capacity(512);
            b.iter(|| {
                dst.clear();
                encode_data_row_direct(black_box(row.as_ref()), &rel.column_types, enc, &mut dst)
                    .unwrap();
                black_box(&dst);
            });
        });
    }
    group.finish();

    // Batch benchmarks (10k rows)
    let mut group = c.benchmark_group("datarow_batch_10k");

    // Build 10k rows for batch test (mixed scenario)
    let batch_rows: Vec<Row> = (0..10_000)
        .map(|i| {
            let mut row = Row::default();
            let mut packer = row.packer();
            packer.push(Datum::Int32(i as i32));
            packer.push(Datum::String("hello world this is row data"));
            packer.push(Datum::Int64(i as i64 * 1000));
            packer.push(Datum::String("another text column value"));
            packer.push(Datum::True);
            row
        })
        .collect();

    group.bench_function("old_values_from_row", |b| {
        let mut dst = BytesMut::with_capacity(1024 * 1024);
        b.iter(|| {
            dst.clear();
            for row in &batch_rows {
                encode_old(black_box(row.as_ref()), &mixed_rel, &mut dst);
            }
            black_box(&dst);
        });
    });

    group.bench_function("new_direct_encode", |b| {
        let mut dst = BytesMut::with_capacity(1024 * 1024);
        b.iter(|| {
            dst.clear();
            for row in &batch_rows {
                encode_data_row_direct(
                    black_box(row.as_ref()),
                    &mixed_rel.column_types,
                    &mixed_enc,
                    &mut dst,
                )
                .unwrap();
            }
            black_box(&dst);
        });
    });
    group.finish();

    // Batch benchmarks: string-heavy (10k rows, 5 text columns)
    let mut group = c.benchmark_group("datarow_batch_10k_strings");
    let str_batch_rows: Vec<Row> = (0..10_000)
        .map(|_| {
            let mut row = Row::default();
            let mut packer = row.packer();
            packer.push(Datum::String("The quick brown fox jumps over the lazy dog"));
            packer.push(Datum::String("Lorem ipsum dolor sit amet consectetur"));
            packer.push(Datum::String("abcdefghijklmnopqrstuvwxyz0123456789"));
            packer.push(Datum::String("short"));
            packer.push(Datum::String("a somewhat longer string that has more characters in it"));
            row
        })
        .collect();

    group.bench_function("old_values_from_row", |b| {
        let mut dst = BytesMut::with_capacity(2 * 1024 * 1024);
        b.iter(|| {
            dst.clear();
            for row in &str_batch_rows {
                encode_old(black_box(row.as_ref()), &str_rel, &mut dst);
            }
            black_box(&dst);
        });
    });

    group.bench_function("new_direct_encode", |b| {
        let mut dst = BytesMut::with_capacity(2 * 1024 * 1024);
        b.iter(|| {
            dst.clear();
            for row in &str_batch_rows {
                encode_data_row_direct(
                    black_box(row.as_ref()),
                    &str_rel.column_types,
                    &str_enc,
                    &mut dst,
                )
                .unwrap();
            }
            black_box(&dst);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_datarow_encode);
criterion_main!(benches);
