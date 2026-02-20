// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for parse_bool: compare heap-allocating to_lowercase() approach
//! vs zero-allocation ASCII case-insensitive matching.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_repr::strconv::parse_bool;

/// Old implementation for comparison baseline.
/// Uses `.to_lowercase()` which heap-allocates a String on every call.
fn parse_bool_old(s: &str) -> Result<bool, ()> {
    match s.trim().to_lowercase().as_str() {
        "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Ok(true),
        "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Ok(false),
        _ => Err(()),
    }
}

fn bench_per_value(c: &mut Criterion) {
    let cases: Vec<(&str, &str)> = vec![
        ("true", "true"),
        ("false", "false"),
        ("t", "t"),
        ("f", "f"),
        ("TRUE", "TRUE"),
        ("FALSE", "FALSE"),
        ("True", "True"),
        ("False", "False"),
        ("yes", "yes"),
        ("no", "no"),
        ("on", "on"),
        ("off", "off"),
        ("1", "1"),
        ("0", "0"),
        ("y", "y"),
        ("n", "n"),
        ("  true  ", "true_ws"),
    ];

    let mut group = c.benchmark_group("parse_bool");

    for (input, label) in &cases {
        group.bench_function(format!("old/{}", label), |b| {
            b.iter(|| parse_bool_old(black_box(input)))
        });
        group.bench_function(format!("new/{}", label), |b| {
            b.iter(|| parse_bool(black_box(input)))
        });
    }
    group.finish();
}

fn bench_batch(c: &mut Criterion) {
    // Mix of common boolean representations
    let inputs: Vec<&str> = vec![
        "true", "false", "t", "f", "TRUE", "FALSE", "1", "0", "yes", "no", "on", "off", "True",
        "False", "t", "f",
    ];
    // Repeat to get 10k values
    let batch: Vec<&str> = inputs.iter().cycle().take(10_000).copied().collect();

    let mut group = c.benchmark_group("parse_bool_batch");

    group.bench_function("old_10k_mixed", |b| {
        b.iter(|| {
            for s in &batch {
                let _ = black_box(parse_bool_old(black_box(s)));
            }
        })
    });

    group.bench_function("new_10k_mixed", |b| {
        b.iter(|| {
            for s in &batch {
                let _ = black_box(parse_bool(black_box(s)));
            }
        })
    });

    // All "true" - common case
    let all_true: Vec<&str> = std::iter::repeat("true").take(10_000).collect();

    group.bench_function("old_10k_true", |b| {
        b.iter(|| {
            for s in &all_true {
                let _ = black_box(parse_bool_old(black_box(s)));
            }
        })
    });

    group.bench_function("new_10k_true", |b| {
        b.iter(|| {
            for s in &all_true {
                let _ = black_box(parse_bool(black_box(s)));
            }
        })
    });

    // All "t" - shortest representation
    let all_t: Vec<&str> = std::iter::repeat("t").take(10_000).collect();

    group.bench_function("old_10k_t", |b| {
        b.iter(|| {
            for s in &all_t {
                let _ = black_box(parse_bool_old(black_box(s)));
            }
        })
    });

    group.bench_function("new_10k_t", |b| {
        b.iter(|| {
            for s in &all_t {
                let _ = black_box(parse_bool(black_box(s)));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_per_value, bench_batch);
criterion_main!(benches);
