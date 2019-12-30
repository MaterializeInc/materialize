// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use chrono::NaiveDate;
use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use rand::Rng;

use repr::{Datum, Row, RowUnpacker};

fn bench_sort_datums(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    b.iter_with_setup(|| rows.clone(), |mut rows| rows.sort())
}

fn bench_sort_row_raw(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let rows = rows
        .into_iter()
        .map(|row| Row::pack(row))
        .collect::<Vec<_>>();
    b.iter_with_setup(|| rows.clone(), |mut rows| rows.sort())
}

fn bench_sort_packer(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let rows = rows
        .into_iter()
        .map(|row| Row::pack(row))
        .collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |mut rows| {
            let mut unpacker_a = RowUnpacker::new();
            let mut unpacker_b = RowUnpacker::new();
            rows.sort_by(move |a, b| unpacker_a.unpack(a).cmp(&unpacker_b.unpack(b)));
        },
    )
}

fn bench_sort_row_unpacked(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let arity = rows[0].len();
    let rows = rows
        .into_iter()
        .map(|row| Row::pack(row))
        .collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |rows| {
            let mut unpacked = vec![];
            for row in &rows {
                unpacked.extend(row);
            }
            let mut slices = unpacked.chunks(arity).collect::<Vec<_>>();
            slices.sort();
            slices
                .into_iter()
                .map(|slice| Row::pack(slice))
                .collect::<Vec<_>>()
        },
    )
}

fn bench_filter_unpacked(filter: Datum, rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let rows = rows
        .into_iter()
        .map(|row| Row::pack(row))
        .collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |mut rows| {
            let mut unpacker = RowUnpacker::new();
            rows.retain(|row| {
                let row = unpacker.unpack(row.iter());
                row[0] == filter
            })
        },
    )
}

fn bench_filter_packed(filter: Datum, rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let filter = Row::pack(&[filter]);
    let rows = rows
        .into_iter()
        .map(|row| Row::pack(row))
        .collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |mut rows| {
            let mut unpacker = RowUnpacker::new();
            rows.retain(|row| {
                let row = unpacker.unpack(row.iter());
                row[0] == filter.unpack_first()
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
    let num_rows = 10_000;

    let mut rng = seeded_rng();
    let int_rows = (0..num_rows)
        .map(|_| vec![Datum::Int32(rng.gen())])
        .collect::<Vec<_>>();

    let mut rng = seeded_rng();
    let byte_data = (0..num_rows)
        .map(|_| {
            let i: i32 = rng.gen();
            format!("{}", i).into_bytes()
        })
        .collect::<Vec<_>>();
    let byte_rows = byte_data
        .iter()
        .map(|bytes| vec![Datum::Bytes(bytes)])
        .collect::<Vec<_>>();

    c.bench_function("sort_datums_ints", |b| {
        bench_sort_datums(int_rows.clone(), b)
    });
    c.bench_function("sort_row_raw_ints", |b| {
        bench_sort_row_raw(int_rows.clone(), b)
    });
    c.bench_function("sort_packer_ints", |b| {
        bench_sort_packer(int_rows.clone(), b)
    });
    c.bench_function("sort_row_unpacked_ints", |b| {
        bench_sort_row_unpacked(int_rows.clone(), b)
    });

    c.bench_function("sort_datums_bytes", |b| {
        bench_sort_datums(byte_rows.clone(), b)
    });
    c.bench_function("sort_row_raw_bytes", |b| {
        bench_sort_row_raw(byte_rows.clone(), b)
    });
    c.bench_function("sort_packer_bytes", |b| {
        bench_sort_packer(byte_rows.clone(), b)
    });
    c.bench_function("sort_row_unpacked_bytes", |b| {
        bench_sort_row_unpacked(byte_rows.clone(), b)
    });
}

fn bench_filter(c: &mut Criterion) {
    let num_rows = 10_000;
    let mut rng = seeded_rng();
    let mut random_date = || {
        NaiveDate::from_isoywd(
            rng.gen_range(2000, 2020),
            rng.gen_range(1, 52),
            chrono::Weekday::Mon,
        )
    };
    let date_rows = (0..num_rows)
        .map(|_| vec![Datum::Date(random_date())])
        .collect::<Vec<_>>();
    let filter = random_date();

    c.bench_function("filter_unpacked", |b| {
        bench_filter_unpacked(Datum::Date(filter), date_rows.clone(), b)
    });
    c.bench_function("filter_packed", |b| {
        bench_filter_packed(Datum::Date(filter), date_rows.clone(), b)
    });
}

criterion_group!(benches, bench_sort, bench_filter);
criterion_main!(benches);
