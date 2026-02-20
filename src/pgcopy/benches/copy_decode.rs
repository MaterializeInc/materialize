// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for COPY text format decoding: old Value-intermediate approach
//! vs new direct-to-Row approach via decode_text_into_row.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::{Datum, Row, RowArena};

/// Old approach: Value::decode_text → into_datum → Vec<Datum> → Row::pack.
/// This is what decode_copy_format_text used before the optimization.
fn decode_row_old(
    fields: &[(&[u8], &mz_pgrepr::Type)],
) -> Result<Row, Box<dyn std::error::Error + Sync + Send>> {
    let mut row = Vec::new();
    let buf = RowArena::new();
    for &(raw_value, typ) in fields {
        let value = mz_pgrepr::Value::decode_text(typ, raw_value)?;
        row.push(value.into_datum(&buf, typ));
    }
    Ok(Row::pack(row))
}

/// New approach: decode_text_into_row → direct RowPacker push.
fn decode_row_new(
    fields: &[(&[u8], &mz_pgrepr::Type)],
    row_buf: &mut Row,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let mut packer = row_buf.packer();
    for &(raw_value, typ) in fields {
        let s = std::str::from_utf8(raw_value)?;
        mz_pgrepr::Value::decode_text_into_row(typ, s, &mut packer)?;
    }
    Ok(())
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_decode");

    // Scenario 1: 5 integer columns
    let int_fields: Vec<(&[u8], &mz_pgrepr::Type)> = vec![
        (b"12345", &mz_pgrepr::Type::Int4),
        (b"-67890", &mz_pgrepr::Type::Int4),
        (b"42", &mz_pgrepr::Type::Int8),
        (b"0", &mz_pgrepr::Type::Int2),
        (b"999999999", &mz_pgrepr::Type::Int8),
    ];

    // Scenario 2: 5 text columns
    let str_fields: Vec<(&[u8], &mz_pgrepr::Type)> = vec![
        (b"hello world", &mz_pgrepr::Type::Text),
        (b"foo bar baz", &mz_pgrepr::Type::Text),
        (b"test string 12345", &mz_pgrepr::Type::Text),
        (b"another value", &mz_pgrepr::Type::Text),
        (b"the quick brown fox", &mz_pgrepr::Type::Text),
    ];

    // Scenario 3: mixed columns (int + text + bool + timestamp + float)
    let mixed_fields: Vec<(&[u8], &mz_pgrepr::Type)> = vec![
        (b"12345", &mz_pgrepr::Type::Int4),
        (b"hello world", &mz_pgrepr::Type::Text),
        (b"true", &mz_pgrepr::Type::Bool),
        (b"2024-06-15 12:30:00", &mz_pgrepr::Type::Timestamp { precision: None }),
        (b"3.14159", &mz_pgrepr::Type::Float8),
    ];

    // Scenario 4: bool-heavy columns
    let bool_fields: Vec<(&[u8], &mz_pgrepr::Type)> = vec![
        (b"true", &mz_pgrepr::Type::Bool),
        (b"false", &mz_pgrepr::Type::Bool),
        (b"t", &mz_pgrepr::Type::Bool),
        (b"f", &mz_pgrepr::Type::Bool),
        (b"TRUE", &mz_pgrepr::Type::Bool),
    ];

    // Per-row benchmarks
    for (name, fields) in [
        ("int5", &int_fields),
        ("str5", &str_fields),
        ("mixed5", &mixed_fields),
        ("bool5", &bool_fields),
    ] {
        group.bench_function(format!("old/{}", name), |b| {
            b.iter(|| decode_row_old(black_box(fields)))
        });

        group.bench_function(format!("new/{}", name), |b| {
            let mut row_buf = Row::default();
            b.iter(|| {
                decode_row_new(black_box(fields), &mut row_buf).unwrap();
                black_box(&row_buf);
            })
        });
    }

    // Batch benchmarks (10k rows)
    for (name, fields) in [
        ("int5", &int_fields),
        ("str5", &str_fields),
        ("mixed5", &mixed_fields),
    ] {
        group.bench_function(format!("old_10k/{}", name), |b| {
            b.iter(|| {
                for _ in 0..10_000 {
                    let _ = black_box(decode_row_old(black_box(fields)));
                }
            })
        });

        group.bench_function(format!("new_10k/{}", name), |b| {
            let mut row_buf = Row::default();
            b.iter(|| {
                for _ in 0..10_000 {
                    decode_row_new(black_box(fields), &mut row_buf).unwrap();
                    black_box(&row_buf);
                }
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
