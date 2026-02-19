// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for numeric-to-float casting.
//!
//! Compares three approaches:
//! - Old: to_string().parse() (heap allocation)
//! - Stack buffer: fmt::Write to stack buffer, then parse (no heap allocation)
//! - TryFrom: dec crate's direct conversion (fastest but has precision issues)

use std::fmt::Write;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_repr::adt::numeric::{self, Numeric};

/// Stack-allocated buffer for formatting Numeric values.
struct NumericBuf {
    buf: [u8; 64],
    len: usize,
}

impl NumericBuf {
    #[inline]
    fn new() -> Self {
        NumericBuf {
            buf: [0; 64],
            len: 0,
        }
    }

    #[inline]
    fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.buf[..self.len]) }
    }
}

impl std::fmt::Write for NumericBuf {
    #[inline]
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let bytes = s.as_bytes();
        let new_len = self.len + bytes.len();
        if new_len > self.buf.len() {
            return Err(std::fmt::Error);
        }
        self.buf[self.len..new_len].copy_from_slice(bytes);
        self.len = new_len;
        Ok(())
    }
}

fn make_test_numerics() -> Vec<(&'static str, Numeric)> {
    let mut cx = numeric::cx_datum();
    vec![
        ("small_int", cx.parse("42").unwrap()),
        ("decimal", cx.parse("123.456789").unwrap()),
        ("large", cx.parse("99999999999.999999").unwrap()),
        ("negative", cx.parse("-3.14159265358979").unwrap()),
        ("tiny", cx.parse("0.000001").unwrap()),
        ("zero", cx.parse("0").unwrap()),
        ("one", cx.parse("1").unwrap()),
        ("pi", cx.parse("3.141592653589793238").unwrap()),
    ]
}

fn cast_via_string_f64(a: Numeric) -> f64 {
    a.to_string().parse::<f64>().unwrap()
}

fn cast_via_string_f32(a: Numeric) -> f32 {
    a.to_string().parse::<f32>().unwrap()
}

fn cast_via_stackbuf_f64(a: Numeric) -> f64 {
    let mut buf = NumericBuf::new();
    write!(buf, "{}", a).unwrap();
    buf.as_str().parse::<f64>().unwrap()
}

fn cast_via_stackbuf_f32(a: Numeric) -> f32 {
    let mut buf = NumericBuf::new();
    write!(buf, "{}", a).unwrap();
    buf.as_str().parse::<f32>().unwrap()
}

fn bench_numeric_to_f64(c: &mut Criterion) {
    let test_values = make_test_numerics();

    let mut group = c.benchmark_group("numeric_to_f64");
    for (name, val) in &test_values {
        group.bench_with_input(BenchmarkId::new("via_string", name), val, |b, val| {
            b.iter(|| black_box(cast_via_string_f64(*val)))
        });
        group.bench_with_input(BenchmarkId::new("via_stackbuf", name), val, |b, val| {
            b.iter(|| black_box(cast_via_stackbuf_f64(*val)))
        });
    }
    group.finish();
}

fn bench_numeric_to_f32(c: &mut Criterion) {
    let test_values = make_test_numerics();

    let mut group = c.benchmark_group("numeric_to_f32");
    for (name, val) in &test_values {
        group.bench_with_input(BenchmarkId::new("via_string", name), val, |b, val| {
            b.iter(|| black_box(cast_via_string_f32(*val)))
        });
        group.bench_with_input(BenchmarkId::new("via_stackbuf", name), val, |b, val| {
            b.iter(|| black_box(cast_via_stackbuf_f32(*val)))
        });
    }
    group.finish();
}

fn bench_numeric_to_f64_batch(c: &mut Criterion) {
    let test_values: Vec<Numeric> = make_test_numerics().into_iter().map(|(_, v)| v).collect();
    let batch: Vec<Numeric> = test_values.iter().copied().cycle().take(10000).collect();

    let mut group = c.benchmark_group("numeric_to_f64_batch_10k");
    group.bench_function("via_string", |b| {
        b.iter(|| {
            let sum: f64 = batch.iter().map(|v| cast_via_string_f64(*v)).sum();
            black_box(sum)
        })
    });
    group.bench_function("via_stackbuf", |b| {
        b.iter(|| {
            let sum: f64 = batch.iter().map(|v| cast_via_stackbuf_f64(*v)).sum();
            black_box(sum)
        })
    });
    group.finish();
}

fn verify_equivalence(c: &mut Criterion) {
    let mut cx = numeric::cx_datum();
    let edge_cases: Vec<(&str, Numeric)> = vec![
        ("42", cx.parse("42").unwrap()),
        ("1.234", cx.parse("1.234").unwrap()),
        ("123.456789", cx.parse("123.456789").unwrap()),
        ("1234.567891234567", cx.parse("1234.567891234567").unwrap()),
        ("0.000001", cx.parse("0.000001").unwrap()),
        ("-3.14159265358979", cx.parse("-3.14159265358979").unwrap()),
        ("3.40282347E+38", cx.parse("3.40282347E+38").unwrap()),
        ("-3.40282347E+38", cx.parse("-3.40282347E+38").unwrap()),
        ("9E-39", cx.parse("9E-39").unwrap()),
        ("0", cx.parse("0").unwrap()),
        ("1", cx.parse("1").unwrap()),
        (
            "0.000000000000000123456789123456789012345",
            cx.parse("0.000000000000000123456789123456789012345")
                .unwrap(),
        ),
    ];

    // Verify f64 equivalence between string and stackbuf approaches
    for (name, val) in &edge_cases {
        let via_string = cast_via_string_f64(*val);
        let via_stackbuf = cast_via_stackbuf_f64(*val);
        assert!(
            via_string == via_stackbuf || (via_string.is_nan() && via_stackbuf.is_nan()),
            "f64 mismatch for {}: string={} stackbuf={}",
            name,
            via_string,
            via_stackbuf
        );
    }

    // Verify f32 equivalence
    for (name, val) in &edge_cases {
        let via_string = cast_via_string_f32(*val);
        let via_stackbuf = cast_via_stackbuf_f32(*val);
        assert!(
            via_string == via_stackbuf || (via_string.is_nan() && via_stackbuf.is_nan()),
            "f32 mismatch for {}: string={} stackbuf={}",
            name,
            via_string,
            via_stackbuf
        );
    }

    c.bench_function("equivalence_check", |b| b.iter(|| black_box(true)));
}

criterion_group!(
    benches,
    verify_equivalence,
    bench_numeric_to_f64,
    bench_numeric_to_f32,
    bench_numeric_to_f64_batch,
);
criterion_main!(benches);
