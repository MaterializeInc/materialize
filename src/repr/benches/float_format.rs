// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for float formatting.
//!
//! Compares two approaches:
//! - Old: per-character iteration with Peekable<Chars> and individual write_char calls
//! - New: byte-level find + write_str slicing (1-3 write_str calls)

use std::hint::black_box;
use std::num::FpCategory;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_ore::fmt::FormatBuffer;
use mz_repr::strconv;
use num_traits::Float as NumFloat;
use ryu::Float as RyuFloat;

/// Old approach: per-character iteration with Peekable<Chars>.
fn format_float_old<F, Fl>(buf: &mut F, f: Fl)
where
    F: FormatBuffer,
    Fl: NumFloat + RyuFloat,
{
    match f.classify() {
        FpCategory::Infinite if f.is_sign_negative() => buf.write_str("-Infinity"),
        FpCategory::Infinite => buf.write_str("Infinity"),
        FpCategory::Nan => buf.write_str("NaN"),
        FpCategory::Zero if f.is_sign_negative() => buf.write_str("-0"),
        _ => {
            let mut ryu_buf = ryu::Buffer::new();
            let mut s = ryu_buf.format_finite(f);
            if let Some(trimmed) = s.strip_suffix(".0") {
                s = trimmed;
            }
            let mut chars = s.chars().peekable();
            while let Some(ch) = chars.next() {
                buf.write_char(ch);
                if ch == 'e' && chars.peek() != Some(&'-') {
                    buf.write_char('+');
                }
            }
        }
    }
}

fn make_f64_test_values() -> Vec<(&'static str, f64)> {
    vec![
        ("zero", 0.0),
        ("one", 1.0),
        ("small_int", 42.0),
        ("pi", std::f64::consts::PI),
        ("typical", 3.14),
        ("hundred", 100.0),
        ("thousand", 9999.0),
        ("negative", -42.5),
        ("neg_pi", -std::f64::consts::PI),
        ("small_frac", 0.001),
        ("tiny", 1e-7),
        ("large", 1e15),
        ("large_frac", 1.23456789e15),
        ("sci_neg_exp", 1.5e-10),
        ("sci_pos_exp", 1.5e10),
        ("max_sig", 1.7976931348623157e308),
        ("min_pos", 5e-324),
    ]
}

fn make_f32_test_values() -> Vec<(&'static str, f32)> {
    vec![
        ("zero", 0.0f32),
        ("one", 1.0f32),
        ("pi", std::f32::consts::PI),
        ("typical", 3.14f32),
        ("hundred", 100.0f32),
        ("negative", -42.5f32),
        ("small_frac", 0.001f32),
        ("tiny", 1e-7f32),
        ("large", 1e15f32),
        ("sci_pos_exp", 1.5e10f32),
    ]
}

fn bench_f64_per_value(c: &mut Criterion) {
    let values = make_f64_test_values();
    let mut group = c.benchmark_group("f64_per_value");

    for (name, val) in &values {
        group.bench_with_input(BenchmarkId::new("old_peekable_chars", name), val, |b, &v| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                format_float_old(&mut buf, black_box(v));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_write_str", name), val, |b, &v| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                strconv::format_float64(&mut buf, black_box(v));
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_f32_per_value(c: &mut Criterion) {
    let values = make_f32_test_values();
    let mut group = c.benchmark_group("f32_per_value");

    for (name, val) in &values {
        group.bench_with_input(BenchmarkId::new("old_peekable_chars", name), val, |b, &v| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                format_float_old(&mut buf, black_box(v));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_write_str", name), val, |b, &v| {
            let mut buf = String::with_capacity(64);
            b.iter(|| {
                buf.clear();
                strconv::format_float32(&mut buf, black_box(v));
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_batch_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_f64_10k");

    // Mix of values: typical database floats
    let values: Vec<f64> = (0..10_000)
        .map(|i| match i % 7 {
            0 => i as f64,                          // integer-like
            1 => i as f64 * 0.1,                    // simple decimal
            2 => std::f64::consts::PI * i as f64,   // irrational-ish
            3 => -(i as f64 * 1.5),                 // negative
            4 => (i as f64) * 1e10,                 // large with exponent
            5 => 1.0 / (i as f64 + 1.0),           // small fractions
            _ => (i as f64 + 0.5) * 100.0,         // typical values
        })
        .collect();

    group.bench_function("old_peekable_chars", |b| {
        let mut buf = String::with_capacity(300_000);
        b.iter(|| {
            buf.clear();
            for &v in &values {
                format_float_old(&mut buf, black_box(v));
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.bench_function("new_write_str", |b| {
        let mut buf = String::with_capacity(300_000);
        b.iter(|| {
            buf.clear();
            for &v in &values {
                strconv::format_float64(&mut buf, black_box(v));
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.finish();
}

fn bench_batch_integer_floats(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_integer_floats_10k");

    // Pure integer-valued floats (the fast path target)
    let values: Vec<f64> = (0..10_000)
        .map(|i| match i % 5 {
            0 => i as f64,                   // small positive
            1 => -(i as f64),                // small negative
            2 => (i as f64) * 1000.0,        // medium values
            3 => 0.0,                        // zero
            _ => (i as f64) * 100.0 + 42.0,  // typical IDs
        })
        .collect();

    group.bench_function("old_peekable_chars", |b| {
        let mut buf = String::with_capacity(200_000);
        b.iter(|| {
            buf.clear();
            for &v in &values {
                format_float_old(&mut buf, black_box(v));
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.bench_function("new_int_fast_path", |b| {
        let mut buf = String::with_capacity(200_000);
        b.iter(|| {
            buf.clear();
            for &v in &values {
                strconv::format_float64(&mut buf, black_box(v));
                buf.push(',');
            }
            black_box(&buf);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_f64_per_value,
    bench_f32_per_value,
    bench_batch_f64,
    bench_batch_integer_floats,
);
criterion_main!(benches);
