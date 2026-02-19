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
use chrono::NaiveDate;
use criterion::{Criterion, criterion_group, criterion_main};
use mz_pgrepr::{Type, Value, encode_data_row_direct, values_from_row};
use mz_pgwire_common::Format;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Row, SqlColumnType, SqlRelationType, SqlScalarType};
use std::hint::black_box;

fn make_type(scalar: SqlScalarType) -> SqlColumnType {
    SqlColumnType {
        nullable: false,
        scalar_type: scalar,
    }
}

fn make_encode_state(col_types: &[SqlColumnType], format: Format) -> Vec<(Type, Format)> {
    col_types
        .iter()
        .map(|ct| (Type::from(&ct.scalar_type), format))
        .collect()
}

// === TEXT FORMAT BENCHMARKS ===

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
    let encode_state = make_encode_state(&col_types, Format::Text);
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
    let encode_state = make_encode_state(&col_types, Format::Text);
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
    let encode_state = make_encode_state(&col_types, Format::Text);
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
    let encode_state = make_encode_state(&col_types, Format::Text);
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
    let encode_state = make_encode_state(&col_types, Format::Text);
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

// === BINARY FORMAT BENCHMARKS ===

/// Encodes a DataRow using the old Value-based binary path (for comparison).
fn encode_data_row_binary_via_value(
    row: &mz_repr::RowRef,
    col_types: &[SqlColumnType],
    encode_state: &[(Type, Format)],
    dst: &mut BytesMut,
) -> Result<(), std::io::Error> {
    use bytes::BufMut;
    // Type byte.
    dst.put_u8(b'D');
    let msg_base = dst.len();
    dst.put_u32(0);
    let field_count = i16::try_from(encode_state.len()).unwrap();
    dst.put_i16(field_count);

    for ((datum, col_type), (ty, _format)) in
        row.iter().zip(col_types).zip(encode_state)
    {
        if datum.is_null() {
            dst.put_i32(-1);
        } else {
            let value =
                Value::from_datum(datum, &col_type.scalar_type).expect("non-null datum");
            let field_base = dst.len();
            dst.put_u32(0);
            value.encode_binary(ty, dst)?;
            let field_len = dst.len() - field_base - 4;
            let field_len = i32::try_from(field_len).unwrap();
            dst[field_base..field_base + 4].copy_from_slice(&field_len.to_be_bytes());
        }
    }

    let msg_len = dst.len() - msg_base;
    let msg_len = i32::try_from(msg_len).unwrap();
    dst[msg_base..msg_base + 4].copy_from_slice(&msg_len.to_be_bytes());
    Ok(())
}

fn bench_binary_integers_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_int5");
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
    let encode_state = make_encode_state(&col_types, Format::Binary);
    let mut buf = BytesMut::with_capacity(256);

    group.bench_function("direct", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_direct(black_box(&row), &col_types, &encode_state, &mut buf).unwrap();
            black_box(&buf);
        })
    });
    group.bench_function("value_path", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_binary_via_value(black_box(&row), &col_types, &encode_state, &mut buf)
                .unwrap();
            black_box(&buf);
        })
    });
    group.finish();
}

fn bench_binary_strings_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_str5");
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
    let encode_state = make_encode_state(&col_types, Format::Binary);
    let mut buf = BytesMut::with_capacity(256);

    group.bench_function("direct", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_direct(black_box(&row), &col_types, &encode_state, &mut buf).unwrap();
            black_box(&buf);
        })
    });
    group.bench_function("value_path", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_binary_via_value(black_box(&row), &col_types, &encode_state, &mut buf)
                .unwrap();
            black_box(&buf);
        })
    });
    group.finish();
}

fn bench_binary_mixed_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_mixed5");
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_micro_opt(14, 30, 45, 123456)
            .unwrap(),
    )
    .unwrap();
    let row = Row::pack_slice(&[
        Datum::Int32(42),
        Datum::String("hello world"),
        Datum::True,
        Datum::Timestamp(ts),
        Datum::Float64(3.14159.into()),
    ]);
    let col_types = vec![
        make_type(SqlScalarType::Int32),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::Bool),
        make_type(SqlScalarType::Timestamp { precision: None }),
        make_type(SqlScalarType::Float64),
    ];
    let encode_state = make_encode_state(&col_types, Format::Binary);
    let mut buf = BytesMut::with_capacity(256);

    group.bench_function("direct", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_direct(black_box(&row), &col_types, &encode_state, &mut buf).unwrap();
            black_box(&buf);
        })
    });
    group.bench_function("value_path", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_binary_via_value(black_box(&row), &col_types, &encode_state, &mut buf)
                .unwrap();
            black_box(&buf);
        })
    });
    group.finish();
}

fn bench_binary_timestamps_5col(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_ts5");
    let ts1 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_micro_opt(14, 30, 45, 0)
            .unwrap(),
    )
    .unwrap();
    let ts2 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_micro_opt(0, 0, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let ts3 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(1999, 12, 31)
            .unwrap()
            .and_hms_micro_opt(23, 59, 59, 999999)
            .unwrap(),
    )
    .unwrap();
    let row = Row::pack_slice(&[
        Datum::Timestamp(ts1),
        Datum::Timestamp(ts2),
        Datum::Timestamp(ts3),
        Datum::Timestamp(ts1),
        Datum::Timestamp(ts2),
    ]);
    let col_types = vec![
        make_type(SqlScalarType::Timestamp { precision: None }),
        make_type(SqlScalarType::Timestamp { precision: None }),
        make_type(SqlScalarType::Timestamp { precision: None }),
        make_type(SqlScalarType::Timestamp { precision: None }),
        make_type(SqlScalarType::Timestamp { precision: None }),
    ];
    let encode_state = make_encode_state(&col_types, Format::Binary);
    let mut buf = BytesMut::with_capacity(256);

    group.bench_function("direct", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_direct(black_box(&row), &col_types, &encode_state, &mut buf).unwrap();
            black_box(&buf);
        })
    });
    group.bench_function("value_path", |b| {
        b.iter(|| {
            buf.clear();
            encode_data_row_binary_via_value(black_box(&row), &col_types, &encode_state, &mut buf)
                .unwrap();
            black_box(&buf);
        })
    });
    group.finish();
}

fn bench_binary_batch_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_batch_mixed");
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_micro_opt(14, 30, 45, 123456)
            .unwrap(),
    )
    .unwrap();
    let rows: Vec<Row> = (0..10_000)
        .map(|i| {
            Row::pack_slice(&[
                Datum::Int32(i),
                Datum::String("hello world"),
                Datum::True,
                Datum::Timestamp(ts),
                Datum::Float64((i as f64 * 1.5).into()),
            ])
        })
        .collect();
    let col_types = vec![
        make_type(SqlScalarType::Int32),
        make_type(SqlScalarType::String),
        make_type(SqlScalarType::Bool),
        make_type(SqlScalarType::Timestamp { precision: None }),
        make_type(SqlScalarType::Float64),
    ];
    let encode_state = make_encode_state(&col_types, Format::Binary);
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
    group.bench_function("value_path_10k", |b| {
        b.iter(|| {
            for row in &rows {
                buf.clear();
                encode_data_row_binary_via_value(row, &col_types, &encode_state, &mut buf)
                    .unwrap();
                black_box(&buf);
            }
        })
    });
    group.finish();
}

fn bench_binary_batch_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_batch_strings");
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
    let encode_state = make_encode_state(&col_types, Format::Binary);
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
    group.bench_function("value_path_10k", |b| {
        b.iter(|| {
            for row in &rows {
                buf.clear();
                encode_data_row_binary_via_value(row, &col_types, &encode_state, &mut buf)
                    .unwrap();
                black_box(&buf);
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
    bench_binary_integers_5col,
    bench_binary_strings_5col,
    bench_binary_mixed_5col,
    bench_binary_timestamps_5col,
    bench_binary_batch_mixed,
    bench_binary_batch_strings,
);
criterion_main!(benches);
