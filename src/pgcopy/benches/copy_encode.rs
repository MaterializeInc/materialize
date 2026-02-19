// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for COPY text encoding: direct datum encoding vs values_from_row path.

use bytes::BytesMut;
use criterion::{Criterion, criterion_group, criterion_main};
use mz_repr::{Datum, Row, SqlColumnType, SqlRelationType, SqlScalarType};
use std::hint::black_box;

fn make_type(scalar: SqlScalarType) -> SqlColumnType {
    SqlColumnType {
        nullable: false,
        scalar_type: scalar,
    }
}

/// Encode a COPY text row using the old values_from_row path.
fn encode_copy_text_old(row: &mz_repr::RowRef, typ: &SqlRelationType, out: &mut Vec<u8>) {
    let mut buf = BytesMut::new();
    for (idx, field) in mz_pgrepr::values_from_row(row, typ).into_iter().enumerate() {
        if idx > 0 {
            out.push(b'\t');
        }
        match field {
            None => out.extend(b"\\N"),
            Some(field) => {
                buf.clear();
                field.encode_text(&mut buf);
                for b in &buf {
                    match b {
                        b'\\' => out.extend(b"\\\\"),
                        b'\n' => out.extend(b"\\n"),
                        b'\r' => out.extend(b"\\r"),
                        b'\t' => out.extend(b"\\t"),
                        _ => out.push(*b),
                    }
                }
            }
        }
    }
    out.push(b'\n');
}

/// Encode a COPY text row using the new direct datum encoding path.
fn encode_copy_text_new(row: &mz_repr::RowRef, typ: &SqlRelationType, out: &mut Vec<u8>) {
    let mut buf = BytesMut::new();
    for (idx, (datum, col_type)) in row.iter().zip(typ.column_types.iter()).enumerate() {
        if idx > 0 {
            out.push(b'\t');
        }
        if datum.is_null() {
            out.extend(b"\\N");
        } else {
            buf.clear();
            mz_pgrepr::encode_datum_text_direct(datum, &col_type.scalar_type, &mut buf);
            if buf.iter().any(|b| matches!(b, b'\\' | b'\n' | b'\r' | b'\t')) {
                for b in &buf {
                    match b {
                        b'\\' => out.extend(b"\\\\"),
                        b'\n' => out.extend(b"\\n"),
                        b'\r' => out.extend(b"\\r"),
                        b'\t' => out.extend(b"\\t"),
                        _ => out.push(*b),
                    }
                }
            } else {
                out.extend_from_slice(&buf);
            }
        }
    }
    out.push(b'\n');
}

fn bench_integers_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_int5");
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
    let typ = SqlRelationType {
        column_types: col_types,
        keys: vec![],
    };
    let mut out = Vec::with_capacity(256);

    group.bench_function("old_values_from_row", |b| {
        b.iter(|| {
            out.clear();
            encode_copy_text_old(black_box(&row), &typ, &mut out);
            black_box(&out);
        })
    });
    group.bench_function("new_direct", |b| {
        b.iter(|| {
            out.clear();
            encode_copy_text_new(black_box(&row), &typ, &mut out);
            black_box(&out);
        })
    });
    group.finish();
}

fn bench_strings_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_str5");
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
    let typ = SqlRelationType {
        column_types: col_types,
        keys: vec![],
    };
    let mut out = Vec::with_capacity(256);

    group.bench_function("old_values_from_row", |b| {
        b.iter(|| {
            out.clear();
            encode_copy_text_old(black_box(&row), &typ, &mut out);
            black_box(&out);
        })
    });
    group.bench_function("new_direct", |b| {
        b.iter(|| {
            out.clear();
            encode_copy_text_new(black_box(&row), &typ, &mut out);
            black_box(&out);
        })
    });
    group.finish();
}

fn bench_mixed_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_mixed5");
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
    let typ = SqlRelationType {
        column_types: col_types,
        keys: vec![],
    };
    let mut out = Vec::with_capacity(256);

    group.bench_function("old_values_from_row", |b| {
        b.iter(|| {
            out.clear();
            encode_copy_text_old(black_box(&row), &typ, &mut out);
            black_box(&out);
        })
    });
    group.bench_function("new_direct", |b| {
        b.iter(|| {
            out.clear();
            encode_copy_text_new(black_box(&row), &typ, &mut out);
            black_box(&out);
        })
    });
    group.finish();
}

fn bench_batch_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_batch_mixed");
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
    let typ = SqlRelationType {
        column_types: col_types,
        keys: vec![],
    };
    let mut out = Vec::with_capacity(1024 * 1024);

    group.bench_function("old_10k", |b| {
        b.iter(|| {
            out.clear();
            for row in &rows {
                encode_copy_text_old(row, &typ, &mut out);
            }
            black_box(&out);
        })
    });
    group.bench_function("new_10k", |b| {
        b.iter(|| {
            out.clear();
            for row in &rows {
                encode_copy_text_new(row, &typ, &mut out);
            }
            black_box(&out);
        })
    });
    group.finish();
}

fn bench_batch_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_batch_strings");
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
    let typ = SqlRelationType {
        column_types: col_types,
        keys: vec![],
    };
    let mut out = Vec::with_capacity(1024 * 1024);

    group.bench_function("old_10k", |b| {
        b.iter(|| {
            out.clear();
            for row in &rows {
                encode_copy_text_old(row, &typ, &mut out);
            }
            black_box(&out);
        })
    });
    group.bench_function("new_10k", |b| {
        b.iter(|| {
            out.clear();
            for row in &rows {
                encode_copy_text_new(row, &typ, &mut out);
            }
            black_box(&out);
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
