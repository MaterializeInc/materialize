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

/// Old approach for projection: decode all datums, project, re-encode.
fn concat_then_project_via_datums(
    key: &Row,
    val1: &Row,
    val2: &Row,
    projection: &[usize],
    result: &mut Row,
) {
    // Decode all datums from all sources
    let mut datums: Vec<Datum<'_>> = Vec::new();
    datums.extend(key.iter());
    datums.extend(val1.iter());
    datums.extend(val2.iter());
    // Project and re-encode only the projected datums
    result.packer().extend(projection.iter().map(|&i| datums[i]));
}

/// New approach for projection: byte-level concat then byte-level project.
fn concat_then_project_via_bytes(
    key: &Row,
    val1: &Row,
    val2: &Row,
    projection: &[usize],
    concat_buf: &mut Row,
    result: &mut Row,
) {
    // Phase 1: byte-level concat into temp buffer
    {
        let mut packer = concat_buf.packer();
        unsafe {
            packer.extend_by_slice_unchecked(key.data());
            packer.extend_by_slice_unchecked(val1.data());
            packer.extend_by_slice_unchecked(val2.data());
        }
    }
    // Phase 2: byte-level project from temp to result
    concat_buf.project_onto_unordered(projection, result);
}

fn bench_join_project(c: &mut Criterion) {
    // Scenario: join with key(2) + val1(10) + val2(10) = 22 columns total.
    // Projection drops 2 duplicate key columns from val2, keeping 20 of 22.
    let key_datums: Vec<Datum<'_>> = vec![Datum::Int64(1), Datum::Int64(2)];
    let val1_datums: Vec<Datum<'_>> = (0..10).map(|i| Datum::Int64(i * 10)).collect();
    let val2_datums: Vec<Datum<'_>> = (0..10).map(|i| Datum::Int64(i * 100)).collect();
    let (key, val1, val2) = make_test_rows(&key_datums, &val1_datums, &val2_datums);

    // Projection: all of key(0,1), all of val1(2..12), skip first 2 of val2, keep rest(14..22)
    // This simulates dropping duplicate join key columns from the lookup side
    let projection_drop2: Vec<usize> = (0..12).chain(14..22).collect();

    // Projection: only keep key + first 3 from each val (8 of 22)
    let projection_narrow: Vec<usize> = vec![0, 1, 2, 3, 4, 12, 13, 14];

    // Projection: reversed column order (all 22 columns, reversed)
    let projection_reversed: Vec<usize> = (0..22).rev().collect();

    struct Scenario {
        name: &'static str,
        projection: Vec<usize>,
    }
    let scenarios = vec![
        Scenario {
            name: "drop_2_of_22",
            projection: projection_drop2,
        },
        Scenario {
            name: "keep_8_of_22",
            projection: projection_narrow,
        },
        Scenario {
            name: "reversed_22",
            projection: projection_reversed,
        },
    ];

    let mut group = c.benchmark_group("join_project");

    for scenario in &scenarios {
        group.bench_with_input(
            BenchmarkId::new("datum_decode", scenario.name),
            &(),
            |b, _| {
                let mut result = Row::default();
                b.iter(|| {
                    concat_then_project_via_datums(
                        black_box(&key),
                        black_box(&val1),
                        black_box(&val2),
                        &scenario.projection,
                        &mut result,
                    );
                    black_box(&result);
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("byte_project", scenario.name),
            &(),
            |b, _| {
                let mut concat_buf = Row::default();
                let mut result = Row::default();
                b.iter(|| {
                    concat_then_project_via_bytes(
                        black_box(&key),
                        black_box(&val1),
                        black_box(&val2),
                        &scenario.projection,
                        &mut concat_buf,
                        &mut result,
                    );
                    black_box(&result);
                })
            },
        );
    }
    group.finish();

    // Mixed-type projection scenario
    let key_datums: Vec<Datum<'_>> = vec![Datum::Int64(1)];
    let val1_datums: Vec<Datum<'_>> = vec![
        Datum::String("hello world this is a longer string"),
        Datum::Float64(ordered_float::OrderedFloat(3.14159)),
        Datum::True,
        Datum::Int64(42),
        Datum::String("another string value"),
    ];
    let val2_datums: Vec<Datum<'_>> = vec![
        Datum::String("lookup result string"),
        Datum::Float64(ordered_float::OrderedFloat(2.71828)),
        Datum::False,
        Datum::Int64(99),
        Datum::String("more lookup data here"),
    ];
    let (key, val1, val2) = make_test_rows(&key_datums, &val1_datums, &val2_datums);
    // Drop duplicate key from val2 side: keep cols 0..6, 7..11 (drop col 6)
    let mixed_projection: Vec<usize> = (0..6).chain(7..11).collect();

    let mut batch_group = c.benchmark_group("join_project_batch");

    batch_group.bench_function("datum_decode_10k_drop2of22", |b| {
        let key_datums: Vec<Datum<'_>> = vec![Datum::Int64(1), Datum::Int64(2)];
        let val1_datums: Vec<Datum<'_>> = (0..10).map(|i| Datum::Int64(i * 10)).collect();
        let val2_datums: Vec<Datum<'_>> = (0..10).map(|i| Datum::Int64(i * 100)).collect();
        let (key, val1, val2) = make_test_rows(&key_datums, &val1_datums, &val2_datums);
        let projection: Vec<usize> = (0..12).chain(14..22).collect();
        let mut result = Row::default();
        b.iter(|| {
            for _ in 0..10_000 {
                concat_then_project_via_datums(
                    black_box(&key),
                    black_box(&val1),
                    black_box(&val2),
                    &projection,
                    &mut result,
                );
                black_box(&result);
            }
        })
    });

    batch_group.bench_function("byte_project_10k_drop2of22", |b| {
        let key_datums: Vec<Datum<'_>> = vec![Datum::Int64(1), Datum::Int64(2)];
        let val1_datums: Vec<Datum<'_>> = (0..10).map(|i| Datum::Int64(i * 10)).collect();
        let val2_datums: Vec<Datum<'_>> = (0..10).map(|i| Datum::Int64(i * 100)).collect();
        let (key, val1, val2) = make_test_rows(&key_datums, &val1_datums, &val2_datums);
        let projection: Vec<usize> = (0..12).chain(14..22).collect();
        let mut concat_buf = Row::default();
        let mut result = Row::default();
        b.iter(|| {
            for _ in 0..10_000 {
                concat_then_project_via_bytes(
                    black_box(&key),
                    black_box(&val1),
                    black_box(&val2),
                    &projection,
                    &mut concat_buf,
                    &mut result,
                );
                black_box(&result);
            }
        })
    });

    batch_group.bench_function("datum_decode_10k_mixed", |b| {
        let mut result = Row::default();
        b.iter(|| {
            for _ in 0..10_000 {
                concat_then_project_via_datums(
                    black_box(&key),
                    black_box(&val1),
                    black_box(&val2),
                    &mixed_projection,
                    &mut result,
                );
                black_box(&result);
            }
        })
    });

    batch_group.bench_function("byte_project_10k_mixed", |b| {
        let mut concat_buf = Row::default();
        let mut result = Row::default();
        b.iter(|| {
            for _ in 0..10_000 {
                concat_then_project_via_bytes(
                    black_box(&key),
                    black_box(&val1),
                    black_box(&val2),
                    &mixed_projection,
                    &mut concat_buf,
                    &mut result,
                );
                black_box(&result);
            }
        })
    });

    batch_group.finish();
}

criterion_group!(benches, bench_join_concat, bench_join_project);
criterion_main!(benches);
