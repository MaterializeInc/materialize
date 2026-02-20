// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for join result row construction.
//!
//! Compares two approaches for building a result row from key + val1 + val2:
//! - Old: decode all datums from each source, then re-encode via push_datum
//! - New: byte-level concat via copy_into (single memcpy per source)

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_repr::adt::date::Date;
use mz_repr::{Datum, Row};

/// Build test rows: key, stream_val, lookup_val
fn make_test_rows(
    key_datums: &[Datum<'_>],
    val1_datums: &[Datum<'_>],
    val2_datums: &[Datum<'_>],
) -> (Row, Row, Row) {
    let key = Row::pack(key_datums);
    let val1 = Row::pack(val1_datums);
    let val2 = Row::pack(val2_datums);
    (key, val1, val2)
}

/// Old approach: decode all datums from each source, re-encode into result row.
fn concat_via_datums(key: &Row, val1: &Row, val2: &Row, result: &mut Row) {
    let mut packer = result.packer();
    packer.extend(key.iter());
    packer.extend(val1.iter());
    packer.extend(val2.iter());
}

/// New approach: copy raw bytes from each source directly.
fn concat_via_bytes(key: &Row, val1: &Row, val2: &Row, result: &mut Row) {
    let mut packer = result.packer();
    // SAFETY: Row data is correctly encoded.
    unsafe {
        packer.extend_by_slice_unchecked(key.data());
        packer.extend_by_slice_unchecked(val1.data());
        packer.extend_by_slice_unchecked(val2.data());
    }
}

fn bench_join_concat(c: &mut Criterion) {
    let scenarios: Vec<(&str, Vec<Datum<'_>>, Vec<Datum<'_>>, Vec<Datum<'_>>)> = vec![
        (
            "int_key2_val3_val3",
            vec![Datum::Int64(1), Datum::Int64(2)],
            vec![Datum::Int64(10), Datum::Int64(20), Datum::Int64(30)],
            vec![Datum::Int64(40), Datum::Int64(50), Datum::Int64(60)],
        ),
        (
            "mixed_key1_val5_val5",
            vec![Datum::Int64(42)],
            vec![
                Datum::Int64(1),
                Datum::String("hello world"),
                Datum::Float64(ordered_float::OrderedFloat(3.14)),
                Datum::True,
                Datum::Date(Date::from_pg_epoch(18000).unwrap()),
            ],
            vec![
                Datum::Int64(2),
                Datum::String("goodbye world"),
                Datum::Float64(ordered_float::OrderedFloat(2.72)),
                Datum::False,
                Datum::Date(Date::from_pg_epoch(19000).unwrap()),
            ],
        ),
        (
            "string_key1_val5_val5",
            vec![Datum::String("join_key_value")],
            vec![
                Datum::String("a"),
                Datum::String("bb"),
                Datum::String("ccc"),
                Datum::String("dddd"),
                Datum::String("eeeee"),
            ],
            vec![
                Datum::String("ffffff"),
                Datum::String("ggggggg"),
                Datum::String("hhhhhhhh"),
                Datum::String("iiiiiiiii"),
                Datum::String("jjjjjjjjjj"),
            ],
        ),
        (
            "wide_key2_val10_val10",
            vec![Datum::Int64(1), Datum::Int64(2)],
            vec![
                Datum::Int64(1),
                Datum::Int64(2),
                Datum::Int64(3),
                Datum::Int64(4),
                Datum::Int64(5),
                Datum::Int64(6),
                Datum::Int64(7),
                Datum::Int64(8),
                Datum::Int64(9),
                Datum::Int64(10),
            ],
            vec![
                Datum::Int64(11),
                Datum::Int64(12),
                Datum::Int64(13),
                Datum::Int64(14),
                Datum::Int64(15),
                Datum::Int64(16),
                Datum::Int64(17),
                Datum::Int64(18),
                Datum::Int64(19),
                Datum::Int64(20),
            ],
        ),
    ];

    let mut group = c.benchmark_group("join_concat");

    for (name, key_datums, val1_datums, val2_datums) in &scenarios {
        let (key, val1, val2) = make_test_rows(key_datums, val1_datums, val2_datums);

        group.bench_with_input(BenchmarkId::new("datum_decode", name), &(), |b, _| {
            let mut result = Row::default();
            b.iter(|| {
                concat_via_datums(
                    black_box(&key),
                    black_box(&val1),
                    black_box(&val2),
                    &mut result,
                );
                black_box(&result);
            })
        });

        group.bench_with_input(BenchmarkId::new("byte_concat", name), &(), |b, _| {
            let mut result = Row::default();
            b.iter(|| {
                concat_via_bytes(
                    black_box(&key),
                    black_box(&val1),
                    black_box(&val2),
                    &mut result,
                );
                black_box(&result);
            })
        });
    }
    group.finish();

    // Batch benchmark: 10k join result rows
    let mut batch_group = c.benchmark_group("join_concat_batch");
    let key_datums = vec![Datum::Int64(1), Datum::Int64(2)];
    let val1_datums: Vec<Datum<'_>> = vec![
        Datum::Int64(10),
        Datum::Int64(20),
        Datum::String("hello"),
        Datum::True,
        Datum::Float64(ordered_float::OrderedFloat(3.14)),
    ];
    let val2_datums: Vec<Datum<'_>> = vec![
        Datum::Int64(30),
        Datum::Int64(40),
        Datum::String("world"),
        Datum::False,
        Datum::Float64(ordered_float::OrderedFloat(2.72)),
    ];
    let (key, val1, val2) = make_test_rows(&key_datums, &val1_datums, &val2_datums);

    batch_group.bench_function("datum_decode_10k", |b| {
        let mut result = Row::default();
        b.iter(|| {
            for _ in 0..10_000 {
                concat_via_datums(
                    black_box(&key),
                    black_box(&val1),
                    black_box(&val2),
                    &mut result,
                );
                black_box(&result);
            }
        })
    });

    batch_group.bench_function("byte_concat_10k", |b| {
        let mut result = Row::default();
        b.iter(|| {
            for _ in 0..10_000 {
                concat_via_bytes(
                    black_box(&key),
                    black_box(&val1),
                    black_box(&val2),
                    &mut result,
                );
                black_box(&result);
            }
        })
    });

    batch_group.finish();
}

criterion_group!(benches, bench_join_concat);
criterion_main!(benches);
