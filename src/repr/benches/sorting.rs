// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#[macro_use]
extern crate criterion;
extern crate repr;

use criterion::{black_box, Bencher, Criterion};
use rand::Rng;
use repr::*;
use std::borrow::Cow;
use std::cmp::Ordering;

const NUM_ROWS: usize = 10_000;

fn bench_sort_datums(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    b.iter_with_setup(|| rows.clone(), |mut rows| rows.sort())
}

fn bench_sort_row1(column_types: Vec<ColumnType>, rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let row_type = RowType::new(column_types);
    let rows = rows
        .into_iter()
        .map(|row| Row::from_iter(&row_type, row))
        .collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |mut rows| rows.sort_by(|a, b| a.with_type(&row_type).cmp(&b.with_type(&row_type))),
    )
}

fn bench_sort_row2_raw(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let rows = rows
        .into_iter()
        .map(|row| Row2::from_iter(row))
        .collect::<Vec<_>>();
    b.iter_with_setup(|| rows.clone(), |mut rows| rows.sort())
}

fn bench_sort_row2(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let rows = rows
        .into_iter()
        .map(|row| Row2::from_iter(row))
        .collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |mut rows| {
            rows.sort_by(move |a, b| {
                for (a, b) in a.iter().zip(b.iter()) {
                    match a.cmp(&b) {
                        Ordering::Equal => (),
                        non_eq => return non_eq,
                    }
                }
                return Ordering::Equal;
            })
        },
    )
}

fn seeded_rng() -> rand_chacha::ChaChaRng {
    rand::SeedableRng::from_seed([
        224, 38, 155, 23, 190, 65, 147, 224, 136, 172, 167, 36, 125, 199, 232, 59, 191, 4, 243,
        175, 114, 47, 213, 46, 85, 226, 227, 35, 238, 119, 237, 21,
    ])
}

pub fn bench_sort(c: &mut Criterion) {
    let mut rng = seeded_rng();
    let int_types = vec![ColumnType::new(ScalarType::Int32)];
    let int_rows = (0..NUM_ROWS)
        .map(|_| vec![Datum::Int32(rng.gen())])
        .collect::<Vec<_>>();

    let mut rng = seeded_rng();
    let byte_types = vec![ColumnType::new(ScalarType::Bytes)];
    let byte_data = (0..NUM_ROWS)
        .map(|_| {
            let i: i32 = rng.gen();
            format!("{}", i).into_bytes()
        })
        .collect::<Vec<_>>();
    let byte_rows = byte_data
        .iter()
        .map(|bytes| vec![Datum::Bytes(&**bytes)])
        .collect::<Vec<_>>();

    c.bench_function("sort_datums_ints", |b| {
        bench_sort_datums(int_rows.clone(), b)
    });
    c.bench_function("sort_row1_ints", |b| {
        bench_sort_row1(int_types.clone(), int_rows.clone(), b)
    });
    c.bench_function("sort_row2_ints", |b| bench_sort_row2(int_rows.clone(), b));
    c.bench_function("sort_row2_raw_ints", |b| {
        bench_sort_row2_raw(int_rows.clone(), b)
    });

    c.bench_function("sort_datums_bytes", |b| {
        bench_sort_datums(byte_rows.clone(), b)
    });
    c.bench_function("sort_row1_bytes", |b| {
        bench_sort_row1(byte_types.clone(), byte_rows.clone(), b)
    });
    c.bench_function("sort_row2_bytes", |b| bench_sort_row2(byte_rows.clone(), b));
    c.bench_function("sort_row2_raw_bytes", |b| {
        bench_sort_row2_raw(byte_rows.clone(), b)
    });
}

criterion_group!(benches, bench_sort);
criterion_main!(benches);
