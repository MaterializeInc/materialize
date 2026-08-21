// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Measures `DatumVec::borrow_with` against the general decoding it replaced.
//!
//! `general` calls `DatumVec::borrow_with_general`, which decodes every datum through
//! `read_datum`. `predicted` calls `DatumVec::borrow_with`, which dispatches on the class it
//! learned from earlier rows. Both walk the same rows and produce the same datums, so the only
//! variable is the decode.
//!
//! One `DatumVec` decodes the whole batch, which is how an operator uses it: the prediction is
//! learned once and reused. Batches are sized so the row data stays in cache, since throughput
//! falls off sharply once it does not.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mz_repr::adt::numeric::Numeric;
use mz_repr::{DatumVec, Row};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Rows per iteration.
const BATCH: usize = 1024;

/// A batch of `arity` `Int64` columns, spread across the variable-length encodings so the tag is
/// not the same width every time.
fn ints(arity: usize) -> Vec<Row> {
    let mut rng = StdRng::seed_from_u64(0);
    (0..BATCH)
        .map(|_| {
            Row::pack((0..arity).map(|_| {
                let bits = rng.random_range(1..64);
                mz_repr::Datum::Int64(rng.random::<i64>() >> (64 - bits))
            }))
        })
        .collect()
}

/// A batch of `groups` repetitions of `Int64`, `String`, `Float64`.
fn mixed(groups: usize) -> Vec<Row> {
    let mut rng = StdRng::seed_from_u64(1);
    (0..BATCH)
        .map(|_| {
            let strings: Vec<String> = (0..groups)
                .map(|_| {
                    (0..12)
                        .map(|_| char::from(rng.random_range(b'a'..=b'z')))
                        .collect()
                })
                .collect();
            let mut row = Row::default();
            let mut packer = row.packer();
            for s in &strings {
                packer.push(mz_repr::Datum::Int64(rng.random::<i32>().into()));
                packer.push(mz_repr::Datum::String(s));
                packer.push(mz_repr::Datum::Float64(rng.random::<f64>().into()));
            }
            row
        })
        .collect()
}

/// A batch of `arity` columns of types no fast arm covers, to check the fallback costs nothing.
fn uncovered(arity: usize) -> Vec<Row> {
    let mut rng = StdRng::seed_from_u64(2);
    (0..BATCH)
        .map(|_| {
            Row::pack((0..arity).map(|_| {
                let n = Numeric::from(rng.random::<i32>());
                mz_repr::Datum::from(n)
            }))
        })
        .collect()
}

fn bench_rows(c: &mut Criterion, name: &str, rows: &[Row]) {
    let arity = rows[0].iter().count();
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(
        u64::try_from(rows.len() * arity).expect("fits"),
    ));

    group.bench_function("general", |b| {
        let mut datums = DatumVec::new();
        b.iter(|| {
            let mut acc = 0usize;
            for row in rows {
                acc += datums.borrow_with_general(row.as_ref()).len();
            }
            black_box(acc)
        })
    });

    group.bench_function("predicted", |b| {
        let mut datums = DatumVec::new();
        b.iter(|| {
            let mut acc = 0usize;
            for row in rows {
                acc += datums.borrow_with(row.as_ref()).len();
            }
            black_box(acc)
        })
    });

    group.finish();
}

fn bench_predict(c: &mut Criterion) {
    for arity in [4, 8, 32] {
        bench_rows(c, &format!("int{arity}"), &ints(arity));
    }
    for groups in [1, 4] {
        let rows = mixed(groups);
        bench_rows(c, &format!("mixed{}", groups * 3), &rows);
    }
    bench_rows(c, "numeric8", &uncovered(8));
}

criterion_group!(benches, bench_predict);
criterion_main!(benches);
