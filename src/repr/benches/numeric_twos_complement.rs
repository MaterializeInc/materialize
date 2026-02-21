// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for numeric_to_twos_complement_be/wide (Avro encoding path).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::adt::numeric::{self, Dec, Numeric, NumericAgg, NUMERIC_DATUM_MAX_PRECISION};

fn bench_twos_complement_be(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_twos_complement_be");

    let mut cx = numeric::cx_datum();

    // Integer (fast path: zero exponent, small coefficient)
    let int_val = Numeric::from(42i32);
    group.bench_function("integer_42", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(int_val)))
    });

    // Money-like decimal (fast path: negative exponent, small coefficient)
    let money: Numeric = cx.parse("12345.67").unwrap();
    group.bench_function("money_12345_67", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(money)))
    });

    // After rescale to scale=6 (simulating Avro column encoding)
    let mut rescaled: Numeric = cx.parse("123.45").unwrap();
    let _ = numeric::rescale(&mut rescaled, 6);
    group.bench_function("rescaled_scale6", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(rescaled)))
    });

    // Large coefficient that still fits in i64 (18 digits)
    let large: Numeric = cx.parse("999999999999999999").unwrap();
    group.bench_function("large_18_digits", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(large)))
    });

    // Negative value
    let neg: Numeric = cx.parse("-98765.4321").unwrap();
    group.bench_function("negative", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(neg)))
    });

    // Zero
    let zero = Numeric::from(0i32);
    group.bench_function("zero", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(zero)))
    });

    // Large coefficient (> 18 digits, must use FFI path)
    let huge: Numeric = cx.parse("12345678901234567890.12").unwrap();
    group.bench_function("huge_gt18_digits", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_be(black_box(huge)))
    });

    group.finish();
}

/// Reference (FFI-only) implementation of numeric_to_twos_complement_wide.
/// Used to measure the baseline FFI cost.
fn reference_twos_complement_wide(
    numeric: Numeric,
) -> [u8; 33] {
    let mut buf = [0u8; 33];
    if numeric.is_special() {
        return buf;
    }
    let mut cx = NumericAgg::context();
    let mut d = cx.to_width(numeric);
    let mut scaler = NumericAgg::from(NUMERIC_DATUM_MAX_PRECISION);
    cx.neg(&mut scaler);
    cx.rescale(&mut d, &scaler);
    cx.abs(&mut scaler);
    cx.scaleb(&mut d, &scaler);
    // Extract coefficient in base-2^128 chunks (the inner loop from the original).
    let is_neg = if d.is_negative() {
        cx.neg(&mut d);
        true
    } else {
        false
    };
    let splitter = NumericAgg::u128_splitter();
    let mut buf_cursor = 0;
    while !d.is_zero() {
        let mut w = d.clone();
        cx.rem(&mut w, splitter);
        let c = w.coefficient::<u128>().unwrap();
        let e = std::cmp::min(buf_cursor + 16, 33);
        buf[buf_cursor..e].copy_from_slice(&c.to_le_bytes()[0..e - buf_cursor]);
        buf_cursor += 16;
        cx.div_integer(&mut d, splitter);
    }
    if is_neg {
        let mut seen_first_one = false;
        for i in buf.iter_mut() {
            if seen_first_one {
                *i = *i ^ 0xFF;
            } else if *i > 0 {
                seen_first_one = true;
                if i == &0x80 {
                    continue;
                }
                let tz = i.trailing_zeros();
                *i = *i ^ (0xFF << tz + 1);
            }
        }
    }
    buf.reverse();
    buf
}

fn bench_twos_complement_wide(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_twos_complement_wide");

    let mut cx = numeric::cx_datum();

    // Integer (fast path: zero exponent, small coefficient)
    let int_val = Numeric::from(42i32);
    group.bench_function("old_integer_42", |bench| {
        bench.iter(|| reference_twos_complement_wide(black_box(int_val)))
    });
    group.bench_function("new_integer_42", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_wide(black_box(int_val)))
    });

    // Money-like decimal (fast path: negative exponent, small coefficient)
    let money: Numeric = cx.parse("12345.67").unwrap();
    group.bench_function("old_money_12345_67", |bench| {
        bench.iter(|| reference_twos_complement_wide(black_box(money)))
    });
    group.bench_function("new_money_12345_67", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_wide(black_box(money)))
    });

    // After rescale to scale=6 (simulating Avro column encoding)
    let mut rescaled: Numeric = cx.parse("123.45").unwrap();
    let _ = numeric::rescale(&mut rescaled, 6);
    group.bench_function("old_rescaled_scale6", |bench| {
        bench.iter(|| reference_twos_complement_wide(black_box(rescaled)))
    });
    group.bench_function("new_rescaled_scale6", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_wide(black_box(rescaled)))
    });

    // Large coefficient that still fits in i64 (18 digits)
    let large: Numeric = cx.parse("999999999999999999").unwrap();
    group.bench_function("old_large_18_digits", |bench| {
        bench.iter(|| reference_twos_complement_wide(black_box(large)))
    });
    group.bench_function("new_large_18_digits", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_wide(black_box(large)))
    });

    // Negative value
    let neg: Numeric = cx.parse("-98765.4321").unwrap();
    group.bench_function("old_negative", |bench| {
        bench.iter(|| reference_twos_complement_wide(black_box(neg)))
    });
    group.bench_function("new_negative", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_wide(black_box(neg)))
    });

    // Zero
    let zero = Numeric::from(0i32);
    group.bench_function("old_zero", |bench| {
        bench.iter(|| reference_twos_complement_wide(black_box(zero)))
    });
    group.bench_function("new_zero", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_wide(black_box(zero)))
    });

    // Large coefficient (> 18 digits, must use FFI path)
    let huge: Numeric = cx.parse("12345678901234567890.12").unwrap();
    group.bench_function("old_huge_gt18_digits", |bench| {
        bench.iter(|| reference_twos_complement_wide(black_box(huge)))
    });
    group.bench_function("new_huge_gt18_digits", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_wide(black_box(huge)))
    });

    // Value with exponent = 0 (pure integer, pow_exp = 39)
    let pure_int = Numeric::from(999999i32);
    group.bench_function("old_pure_int_999999", |bench| {
        bench.iter(|| reference_twos_complement_wide(black_box(pure_int)))
    });
    group.bench_function("new_pure_int_999999", |bench| {
        bench.iter(|| numeric::numeric_to_twos_complement_wide(black_box(pure_int)))
    });

    group.finish();
}

criterion_group!(benches, bench_twos_complement_be, bench_twos_complement_wide);
criterion_main!(benches);
