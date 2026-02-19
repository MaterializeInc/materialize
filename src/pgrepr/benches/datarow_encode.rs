// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for direct DataRow encoding vs values_from_row path.

use bytes::BytesMut;
use criterion::{Criterion, criterion_group, criterion_main};
use mz_pgrepr::{Type, Value, encode_data_row_direct, values_from_row};
use mz_pgwire_common::Format;
use mz_repr::{Datum, Row, SqlColumnType, SqlRelationType, SqlScalarType};
use std::hint::black_box;

fn make_type(scalar: SqlScalarType) -> SqlColumnType {
    SqlColumnType {
        nullable: false,
        scalar_type: scalar,
    }
}

fn make_encode_state(col_types: &[SqlColumnType]) -> Vec<(Type, Format)> {
    col_types
        .iter()
        .map(|ct| (Type::from(&ct.scalar_type), Format::Text))
        .collect()
}

fn bench_integers_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("datarow_int5");
    let row = Row::pack_slice(&[
        Datum::Int32(42),
        Datum::Int64(123456789),
        Datum::Int32(-100),
        Datum::Int64(0),
        Datum::Int32(999999),
    ]);
    let col_types = vec![
        make_type(SqlScalarType::Int32),
        make_type(SqlScalarType::Int64),
        make_type(SqlScalarType::Int32),
        make_type(SqlScalarType::Int64),
        make_type(SqlScalarType::Int32),
    ];
    let rel_type = SqlRelationType {
        column_types: col_types.clone(),
        keys: vec![],
    };
    let encode_state = make_encode_state(&col_types);
    let mut buf = BytesMut::with_capacity(256);

    group.bench_function("direct", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_direct(black_box(&row), &col_types, &encode_state, &mut buf).unwrap();
            black_box(&buf);
        })
    });
    group.bench_function("values_from_row", |b| {
        b.iter(|| {
            let values = values_from_row(black_box(&row), &rel_type);
            black_box(values);
        })
    });
    group.finish();
}

fn bench_strings_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("datarow_str5");
    let row = Row::pack_slice(&[
        Datum::String("hello world"),
        Datum::String("this is a test string"),
        Datum::String("short"),
        Datum::String("another longer string value here"),
        Datum::String("x"),
    ]);
    let col_types = vec![
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::String),
    ];
    let rel_type = SqlRelationType {
        column_types: col_types.clone(),
        keys: vec![],
    };
    let encode_state = make_encode_state(&col_types);
    let mut buf = BytesMut::with_capacity(256);

    group.bench_function("direct", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_direct(black_box(&row), &col_types, &encode_state, &mut buf).unwrap();
            black_box(&buf);
        })
    });
    group.bench_function("values_from_row", |b| {
        b.iter(|| {
            let values = values_from_row(black_box(&row), &rel_type);
            black_box(values);
        })
    });
    group.finish();
}

fn bench_mixed_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("datarow_mixed5");
    let row = Row::pack_slice(&[
        Datum::Int32(42),
        Datum::String("hello world"),
        Datum::True,
        Datum::Int64(123456789),
        Datum::String("test"),
    ]);
    let col_types = vec![
        make_type(SqlScalarType::Int32),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::Bool),
        make_type(SqlScalarType::Int64),
        make_type(SqlScalarType::String),
    ];
    let rel_type = SqlRelationType {
        column_types: col_types.clone(),
        keys: vec![],
    };
    let encode_state = make_encode_state(&col_types);
    let mut buf = BytesMut::with_capacity(256);

    group.bench_function("direct", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_direct(black_box(&row), &col_types, &encode_state, &mut buf).unwrap();
            black_box(&buf);
        })
    });
    group.bench_function("values_from_row", |b| {
        b.iter(|| {
            let values = values_from_row(black_box(&row), &rel_type);
            black_box(values);
        })
    });
    group.finish();
}

fn bench_batch_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("datarow_batch_mixed");
    let rows: Vec<Row> = (0..10_000)
        .map(|i| {
            Row::pack_slice(&[
                Datum::Int32(i),
                Datum::String("hello world"),
                Datum::True,
                Datum::Int64(i as i64 * 1000),
                Datum::String("test value"),
            ])
        })
        .collect();
    let col_types = vec![
        make_type(SqlScalarType::Int32),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::Bool),
        make_type(SqlScalarType::Int64),
        make_type(SqlScalarType::String),
    ];
    let rel_type = SqlRelationType {
        column_types: col_types.clone(),
        keys: vec![],
    };
    let encode_state = make_encode_state(&col_types);
    let mut buf = BytesMut::with_capacity(256);

    group.bench_function("direct_10k", |b| {
        b.iter(|| {
            for row in &rows {
                buf.clear();
                encode_data_row_direct(row, &col_types, &encode_state, &mut buf).unwrap();
                black_box(&buf);
            }
        })
    });
    group.bench_function("values_from_row_10k", |b| {
        b.iter(|| {
            for row in &rows {
                let values = values_from_row(row, &rel_type);
                black_box(values);
            }
        })
    });
    group.finish();
}

fn bench_batch_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("datarow_batch_strings");
    let rows: Vec<Row> = (0..10_000)
        .map(|i| {
            let s = format!("row_{}_value_{}", i, i * 42);
            Row::pack_slice(&[
                Datum::String(&s),
                Datum::String(&s),
                Datum::String(&s),
                Datum::String(&s),
                Datum::String(&s),
            ])
        })
        .collect();
    let col_types = vec![
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::String),
    ];
    let rel_type = SqlRelationType {
        column_types: col_types.clone(),
        keys: vec![],
    };
    let encode_state = make_encode_state(&col_types);
    let mut buf = BytesMut::with_capacity(256);

    group.bench_function("direct_10k", |b| {
        b.iter(|| {
            for row in &rows {
                buf.clear();
                encode_data_row_direct(row, &col_types, &encode_state, &mut buf).unwrap();
                black_box(&buf);
            }
        })
    });
    group.bench_function("values_from_row_10k", |b| {
        b.iter(|| {
            for row in &rows {
                let values = values_from_row(row, &rel_type);
                black_box(values);
            }
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_integers_5col,
    bench_strings_5col,
    bench_mixed_5col,
    bench_batch_mixed,
    bench_batch_strings,
);
criterion_main!(benches);
