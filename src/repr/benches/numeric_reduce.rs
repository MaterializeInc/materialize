// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for numeric reduce and Row packing operations.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dec::OrderedDecimal;
use mz_repr::adt::numeric::{self, Numeric};
use mz_repr::{Datum, Row};

/// Reduce via C FFI (the old path: clone context + C library call).
#[inline(never)]
fn reduce_ffi(n: &mut Numeric) {
    numeric::cx_datum().reduce(n);
}

/// Reduce via pure Rust (the new path: coefficient inspection + reconstruction).
#[inline(never)]
fn reduce_fast(n: &mut Numeric) {
    numeric::fast_numeric_reduce(n);
}

/// Compute reduced digit count via clone + FFI (the old datum_size path).
#[inline(never)]
fn digit_count_ffi(n: &Numeric) -> u32 {
    let mut d = *n;
    numeric::cx_datum().reduce(&mut d);
    d.digits()
}

/// Compute reduced digit count via pure Rust (the new datum_size path).
#[inline(never)]
fn digit_count_fast(n: &Numeric) -> u32 {
    numeric::reduced_numeric_digit_count(n)
}

fn bench_reduce(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_reduce");

    let mut cx = numeric::cx_datum();

    // Already-reduced values (the common case — from Row storage or fast-path constructors)
    let already_reduced = Numeric::from(42i32);
    group.bench_function("ffi_already_reduced", |bench| {
        bench.iter(|| {
            let mut n = already_reduced;
            reduce_ffi(black_box(&mut n));
            n
        })
    });
    group.bench_function("fast_already_reduced", |bench| {
        bench.iter(|| {
            let mut n = already_reduced;
            reduce_fast(black_box(&mut n));
            n
        })
    });

    // Money-like value (already reduced, 2 decimal places)
    let money: Numeric = cx.parse("12345.67").unwrap();
    group.bench_function("ffi_money", |bench| {
        bench.iter(|| {
            let mut n = money;
            reduce_ffi(black_box(&mut n));
            n
        })
    });
    group.bench_function("fast_money", |bench| {
        bench.iter(|| {
            let mut n = money;
            reduce_fast(black_box(&mut n));
            n
        })
    });

    // Value with trailing zeros (needs actual reduction)
    let trailing: Numeric = cx.parse("1.50").unwrap();
    group.bench_function("ffi_trailing_zeros", |bench| {
        bench.iter(|| {
            let mut n = trailing;
            reduce_ffi(black_box(&mut n));
            n
        })
    });
    group.bench_function("fast_trailing_zeros", |bench| {
        bench.iter(|| {
            let mut n = trailing;
            reduce_fast(black_box(&mut n));
            n
        })
    });

    // Large value with many trailing zeros
    let large_trailing: Numeric = cx.parse("123456789000").unwrap();
    group.bench_function("ffi_large_trailing", |bench| {
        bench.iter(|| {
            let mut n = large_trailing;
            reduce_ffi(black_box(&mut n));
            n
        })
    });
    group.bench_function("fast_large_trailing", |bench| {
        bench.iter(|| {
            let mut n = large_trailing;
            reduce_fast(black_box(&mut n));
            n
        })
    });

    // Zero
    let zero = Numeric::from(0i32);
    group.bench_function("ffi_zero", |bench| {
        bench.iter(|| {
            let mut n = zero;
            reduce_ffi(black_box(&mut n));
            n
        })
    });
    group.bench_function("fast_zero", |bench| {
        bench.iter(|| {
            let mut n = zero;
            reduce_fast(black_box(&mut n));
            n
        })
    });

    group.finish();
}

fn bench_digit_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_digit_count");

    let mut cx = numeric::cx_datum();

    // Already-reduced (common case)
    let already_reduced = Numeric::from(42i32);
    group.bench_function("ffi_already_reduced", |bench| {
        bench.iter(|| digit_count_ffi(black_box(&already_reduced)))
    });
    group.bench_function("fast_already_reduced", |bench| {
        bench.iter(|| digit_count_fast(black_box(&already_reduced)))
    });

    // Money-like
    let money: Numeric = cx.parse("12345.67").unwrap();
    group.bench_function("ffi_money", |bench| {
        bench.iter(|| digit_count_ffi(black_box(&money)))
    });
    group.bench_function("fast_money", |bench| {
        bench.iter(|| digit_count_fast(black_box(&money)))
    });

    // With trailing zeros
    let trailing: Numeric = cx.parse("1.50").unwrap();
    group.bench_function("ffi_trailing", |bench| {
        bench.iter(|| digit_count_ffi(black_box(&trailing)))
    });
    group.bench_function("fast_trailing", |bench| {
        bench.iter(|| digit_count_fast(black_box(&trailing)))
    });

    group.finish();
}

fn bench_row_packing(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_row_packing");

    let mut cx = numeric::cx_datum();

    // Build a batch of money values (already reduced, the common case)
    let money_values: Vec<Numeric> = (0..10_000)
        .map(|i| {
            let val = (i as f64) * 0.01 + 100.0;
            let s = format!("{:.2}", val);
            cx.parse(s.as_bytes()).unwrap()
        })
        .collect();
    let money_datums: Vec<Datum> = money_values
        .iter()
        .map(|n| Datum::Numeric(OrderedDecimal(*n)))
        .collect();

    // Pack 10k Numeric values into a Row (simulates Row building during query execution)
    group.bench_function("pack_10k_money", |bench| {
        bench.iter(|| {
            let mut row = Row::with_capacity(money_datums.len() * 10);
            let mut packer = row.packer();
            for d in &money_datums {
                packer.push(*d);
            }
            black_box(row)
        })
    });

    // Build values with trailing zeros (uncommon but happens with arithmetic results)
    let trailing_values: Vec<Numeric> = (0..10_000)
        .map(|i| {
            let s = format!("{}.{:02}0", i, i % 100);
            cx.parse(s.as_bytes()).unwrap()
        })
        .collect();
    let trailing_datums: Vec<Datum> = trailing_values
        .iter()
        .map(|n| Datum::Numeric(OrderedDecimal(*n)))
        .collect();

    group.bench_function("pack_10k_trailing", |bench| {
        bench.iter(|| {
            let mut row = Row::with_capacity(trailing_datums.len() * 10);
            let mut packer = row.packer();
            for d in &trailing_datums {
                packer.push(*d);
            }
            black_box(row)
        })
    });

    // Integer values (already reduced via fast-path constructor)
    let int_datums: Vec<Datum> = (0..10_000)
        .map(|i| Datum::Numeric(OrderedDecimal(Numeric::from(i as i32))))
        .collect();

    group.bench_function("pack_10k_ints", |bench| {
        bench.iter(|| {
            let mut row = Row::with_capacity(int_datums.len() * 10);
            let mut packer = row.packer();
            for d in &int_datums {
                packer.push(*d);
            }
            black_box(row)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_reduce, bench_digit_count, bench_row_packing);
criterion_main!(benches);
