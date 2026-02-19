// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for hash key reconstruction in bucketed hierarchical reductions.
//!
//! Simulates the `build_bucketed` pattern where a hash_key row
//! [hash_u64, key_col1, key_col2, ...] needs to be reconstructed with
//! a new hash value (hash % bucket_mod).
//!
//! Compares:
//! - Old: iterate datums, re-encode via pack (decode + re-encode all key cols)
//! - New: read hash datum, then copy_into / tail_bytes for key cols (byte-level)

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_repr::{Datum, Row};

/// Build a hash_key row: [u64_hash, key_col1, key_col2, ...]
fn make_hash_key(hash: u64, key_datums: &[Datum<'_>]) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    packer.push(Datum::UInt64(hash));
    for d in key_datums {
        packer.push(*d);
    }
    row
}

/// Old approach: decode hash, iterate remaining datums, repack with new hash.
/// This is what the code did before the optimization.
fn reconstruct_old(hash_key: &Row, bucket_mod: u64, key_arity: usize, dest: &mut Row) {
    let mut iter = hash_key.iter();
    let hash = iter.next().unwrap().unwrap_uint64() % bucket_mod;
    let mut packer = dest.packer();
    packer.push(Datum::UInt64(hash));
    packer.extend(iter.take(key_arity));
}

/// New approach: read hash datum, then byte-level copy of key columns.
fn reconstruct_new(hash_key: &Row, bucket_mod: u64, dest: &mut Row) {
    let hash = hash_key.iter().next().unwrap().unwrap_uint64() % bucket_mod;
    let key_bytes = hash_key.tail_bytes(1);
    let mut packer = dest.packer();
    packer.push(Datum::UInt64(hash));
    unsafe { packer.extend_by_slice_unchecked(key_bytes) };
}

/// Old approach for stripping the hash prefix.
fn strip_hash_old(hash_key: &Row, key_arity: usize, dest: &mut Row) {
    let mut iter = hash_key.iter();
    let _hash = iter.next();
    let mut packer = dest.packer();
    packer.extend(iter.take(key_arity));
}

/// New approach for stripping the hash prefix.
fn strip_hash_new(hash_key: &Row, dest: &mut Row) {
    let key_bytes = hash_key.tail_bytes(1);
    let mut packer = dest.packer();
    unsafe { packer.extend_by_slice_unchecked(key_bytes) };
}

/// Old approach for initial hash_key creation: iterate key datums, repack.
fn create_hash_key_old(hash: u64, key: &Row, dest: &mut Row) {
    let mut packer = dest.packer();
    packer.extend(std::iter::once(Datum::UInt64(hash)).chain(key.iter()));
}

/// New approach for initial hash_key creation: push hash + copy_into.
fn create_hash_key_new(hash: u64, key: &Row, dest: &mut Row) {
    use mz_repr::fixed_length::ToDatumIter;
    let mut packer = dest.packer();
    packer.push(Datum::UInt64(hash));
    key.copy_into(&mut packer);
}

fn bench_hash_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_key_reconstruct");

    // Scenario: 5 int64 key columns
    let key5_datums: Vec<Datum<'_>> = (0..5).map(|i| Datum::Int64(1000 + i)).collect();
    let hash_key5 = make_hash_key(123456789012345, &key5_datums);

    // Scenario: 10 int64 key columns
    let key10_datums: Vec<Datum<'_>> = (0..10).map(|i| Datum::Int64(1000 + i)).collect();
    let hash_key10 = make_hash_key(123456789012345, &key10_datums);

    // Scenario: 3 mixed-type key columns (string, int, float)
    let s = "example_key_value";
    let mixed_datums: Vec<Datum<'_>> = vec![
        Datum::String(s),
        Datum::Int64(42),
        Datum::Float64(ordered_float::OrderedFloat(3.14)),
    ];
    let hash_key_mixed = make_hash_key(123456789012345, &mixed_datums);

    let bucket_mod = 256u64;
    let mut dest = Row::default();

    // --- Per-operation: hash key reconstruction ---

    group.bench_function(BenchmarkId::new("reconstruct_old", "int5"), |b| {
        b.iter(|| {
            reconstruct_old(black_box(&hash_key5), bucket_mod, 5, &mut dest);
            black_box(&dest);
        });
    });
    group.bench_function(BenchmarkId::new("reconstruct_new", "int5"), |b| {
        b.iter(|| {
            reconstruct_new(black_box(&hash_key5), bucket_mod, &mut dest);
            black_box(&dest);
        });
    });

    group.bench_function(BenchmarkId::new("reconstruct_old", "int10"), |b| {
        b.iter(|| {
            reconstruct_old(black_box(&hash_key10), bucket_mod, 10, &mut dest);
            black_box(&dest);
        });
    });
    group.bench_function(BenchmarkId::new("reconstruct_new", "int10"), |b| {
        b.iter(|| {
            reconstruct_new(black_box(&hash_key10), bucket_mod, &mut dest);
            black_box(&dest);
        });
    });

    group.bench_function(BenchmarkId::new("reconstruct_old", "mixed3"), |b| {
        b.iter(|| {
            reconstruct_old(black_box(&hash_key_mixed), bucket_mod, 3, &mut dest);
            black_box(&dest);
        });
    });
    group.bench_function(BenchmarkId::new("reconstruct_new", "mixed3"), |b| {
        b.iter(|| {
            reconstruct_new(black_box(&hash_key_mixed), bucket_mod, &mut dest);
            black_box(&dest);
        });
    });

    // --- Per-operation: strip hash prefix ---

    group.bench_function(BenchmarkId::new("strip_old", "int5"), |b| {
        b.iter(|| {
            strip_hash_old(black_box(&hash_key5), 5, &mut dest);
            black_box(&dest);
        });
    });
    group.bench_function(BenchmarkId::new("strip_new", "int5"), |b| {
        b.iter(|| {
            strip_hash_new(black_box(&hash_key5), &mut dest);
            black_box(&dest);
        });
    });

    group.bench_function(BenchmarkId::new("strip_old", "int10"), |b| {
        b.iter(|| {
            strip_hash_old(black_box(&hash_key10), 10, &mut dest);
            black_box(&dest);
        });
    });
    group.bench_function(BenchmarkId::new("strip_new", "int10"), |b| {
        b.iter(|| {
            strip_hash_new(black_box(&hash_key10), &mut dest);
            black_box(&dest);
        });
    });

    // --- Per-operation: initial hash_key creation ---

    let key5 = Row::pack(&key5_datums);
    let key10 = Row::pack(&key10_datums);
    let key_mixed = Row::pack(&mixed_datums);

    group.bench_function(BenchmarkId::new("create_old", "int5"), |b| {
        b.iter(|| {
            create_hash_key_old(12345, black_box(&key5), &mut dest);
            black_box(&dest);
        });
    });
    group.bench_function(BenchmarkId::new("create_new", "int5"), |b| {
        b.iter(|| {
            create_hash_key_new(12345, black_box(&key5), &mut dest);
            black_box(&dest);
        });
    });

    group.bench_function(BenchmarkId::new("create_old", "int10"), |b| {
        b.iter(|| {
            create_hash_key_old(12345, black_box(&key10), &mut dest);
            black_box(&dest);
        });
    });
    group.bench_function(BenchmarkId::new("create_new", "int10"), |b| {
        b.iter(|| {
            create_hash_key_new(12345, black_box(&key10), &mut dest);
            black_box(&dest);
        });
    });

    group.bench_function(BenchmarkId::new("create_old", "mixed3"), |b| {
        b.iter(|| {
            create_hash_key_old(12345, black_box(&key_mixed), &mut dest);
            black_box(&dest);
        });
    });
    group.bench_function(BenchmarkId::new("create_new", "mixed3"), |b| {
        b.iter(|| {
            create_hash_key_new(12345, black_box(&key_mixed), &mut dest);
            black_box(&dest);
        });
    });

    // --- Batch: 10k rows reconstruction ---

    let rows_10k: Vec<Row> = (0..10000)
        .map(|i| make_hash_key(i as u64 * 997 + 12345, &key10_datums))
        .collect();

    group.bench_function("batch_10k_reconstruct_old_int10", |b| {
        b.iter(|| {
            for row in &rows_10k {
                reconstruct_old(row, bucket_mod, 10, &mut dest);
            }
            black_box(&dest);
        });
    });
    group.bench_function("batch_10k_reconstruct_new_int10", |b| {
        b.iter(|| {
            for row in &rows_10k {
                reconstruct_new(row, bucket_mod, &mut dest);
            }
            black_box(&dest);
        });
    });

    group.bench_function("batch_10k_strip_old_int10", |b| {
        b.iter(|| {
            for row in &rows_10k {
                strip_hash_old(row, 10, &mut dest);
            }
            black_box(&dest);
        });
    });
    group.bench_function("batch_10k_strip_new_int10", |b| {
        b.iter(|| {
            for row in &rows_10k {
                strip_hash_new(row, &mut dest);
            }
            black_box(&dest);
        });
    });

    // Batch: initial creation, 10k rows, 10-col int key
    let keys_10k: Vec<Row> = (0..10000).map(|_| Row::pack(&key10_datums)).collect();
    group.bench_function("batch_10k_create_old_int10", |b| {
        b.iter(|| {
            for (i, key) in keys_10k.iter().enumerate() {
                create_hash_key_old(i as u64 * 997, key, &mut dest);
            }
            black_box(&dest);
        });
    });
    group.bench_function("batch_10k_create_new_int10", |b| {
        b.iter(|| {
            for (i, key) in keys_10k.iter().enumerate() {
                create_hash_key_new(i as u64 * 997, key, &mut dest);
            }
            black_box(&dest);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_hash_key);
criterion_main!(benches);
