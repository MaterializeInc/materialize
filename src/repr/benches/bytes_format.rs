// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks comparing bytea formatting approaches:
//!
//! - Old: `hex::encode(bytes)` allocates a `String`, then `write!` copies it
//!   into the output buffer. Two copies + one heap allocation per value.
//!
//! - New: Direct hex encoding using a lookup table and stack-allocated chunk
//!   buffer. Zero heap allocations; single pass with bulk `write_str` calls.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_repr::strconv;

/// Old format_bytes: uses hex::encode which heap-allocates a String.
fn format_bytes_old(buf: &mut String, bytes: &[u8]) {
    use std::fmt::Write;
    write!(buf, "\\x{}", hex::encode(bytes)).unwrap();
}

fn bench_bytes_format(c: &mut Criterion) {
    let test_values: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        ("1_byte", vec![0xab]),
        ("4_bytes", vec![0xde, 0xad, 0xbe, 0xef]),
        ("16_bytes_sha128", (0..16).collect()),
        ("32_bytes_sha256", (0..32).collect()),
        ("64_bytes", (0..64).collect()),
        ("100_bytes", (0..100).map(|i| i as u8).collect()),
        ("256_bytes", (0..=255).collect()),
        ("1024_bytes", (0..1024).map(|i| (i % 256) as u8).collect()),
    ];

    // Per-value benchmarks: write to pre-allocated buffer
    let mut group = c.benchmark_group("bytes_format_write");
    for (name, bytes) in &test_values {
        group.bench_with_input(BenchmarkId::new("old_hex_encode", name), bytes, |b, bytes| {
            let mut buf = String::with_capacity(bytes.len() * 2 + 4);
            b.iter(|| {
                buf.clear();
                format_bytes_old(&mut buf, black_box(bytes));
                black_box(&buf);
            });
        });
        group.bench_with_input(BenchmarkId::new("new_direct_hex", name), bytes, |b, bytes| {
            let mut buf = String::with_capacity(bytes.len() * 2 + 4);
            b.iter(|| {
                buf.clear();
                strconv::format_bytes(&mut buf, black_box(bytes));
                black_box(&buf);
            });
        });
    }
    group.finish();

    // Batch benchmark: 10k values to shared buffer
    let mut group = c.benchmark_group("bytes_format_batch_10k");

    // Build 10k byte arrays of varying sizes
    let batch_values: Vec<Vec<u8>> = (0..10_000)
        .map(|i| {
            let len = 16 + (i % 64); // 16-79 bytes each
            (0..len).map(|j| ((i + j) % 256) as u8).collect()
        })
        .collect();

    group.bench_function("old_hex_encode", |b| {
        let mut buf = String::with_capacity(2 * 1024 * 1024);
        b.iter(|| {
            buf.clear();
            for bytes in &batch_values {
                format_bytes_old(&mut buf, black_box(bytes));
            }
            black_box(&buf);
        });
    });

    group.bench_function("new_direct_hex", |b| {
        let mut buf = String::with_capacity(2 * 1024 * 1024);
        b.iter(|| {
            buf.clear();
            for bytes in &batch_values {
                strconv::format_bytes(&mut buf, black_box(bytes));
            }
            black_box(&buf);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_bytes_format);
criterion_main!(benches);
