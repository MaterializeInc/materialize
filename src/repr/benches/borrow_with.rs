// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Measures `DatumVec::borrow_with`, which is how an operator reads a row: every datum through
//! `read_datum`.
//!
//! The cases vary the two things the decoder is sensitive to. Arity varies because a row's fixed
//! costs are spread over its datums, and because a wide batch stops fitting in cache. The
//! integer cases spread values across the variable-length encodings so the tag, and so the width
//! of the payload, is not the same twice: a decoder that dispatches on width rather than
//! computing it looks fine on uniform data and mispredicts on this.
//!
//! One `DatumVec` decodes the whole batch, as an operator does. Batches are sized so the row
//! data stays in cache, since throughput falls off sharply once it does not.

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

/// A batch of `arity` `Int64` columns whose values all encode to the same width, as a column of
/// ids or counters does. `ints` varies the width per datum on purpose; the two punish opposite
/// decoder designs, so both belong in the suite.
fn ints_stable(arity: usize) -> Vec<Row> {
    let mut rng = StdRng::seed_from_u64(4);
    (0..BATCH)
        .map(|_| {
            Row::pack(
                (0..arity)
                    .map(|_| mz_repr::Datum::Int64(i64::from(rng.random::<u16>()) | (1 << 20))),
            )
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

/// A batch of `arity` `String` columns whose lengths straddle the point where the length prefix
/// widens from one byte to two, so the prefix width varies from datum to datum as it does for a
/// text column of mostly short values with occasional long ones. Strings of a single length
/// cannot see a decoder that dispatches on the width rather than computing it.
fn strings(arity: usize) -> Vec<Row> {
    let mut rng = StdRng::seed_from_u64(3);
    (0..BATCH)
        .map(|_| {
            let strings: Vec<String> = (0..arity)
                .map(|_| {
                    let len = if rng.random::<bool>() { 100 } else { 400 };
                    (0..len)
                        .map(|_| char::from(rng.random_range(b'a'..=b'z')))
                        .collect()
                })
                .collect();
            Row::pack(strings.iter().map(|s| mz_repr::Datum::String(s)))
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

    group.bench_function("borrow_with", |b| {
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

fn bench_borrow_with(c: &mut Criterion) {
    for arity in [4, 8, 32] {
        bench_rows(c, &format!("int{arity}"), &ints(arity));
    }
    bench_rows(c, "intstable8", &ints_stable(8));
    bench_rows(c, "intstable32", &ints_stable(32));
    for groups in [1, 4] {
        let rows = mixed(groups);
        bench_rows(c, &format!("mixed{}", groups * 3), &rows);
    }
    bench_rows(c, "strings8", &strings(8));
    bench_rows(c, "numeric8", &uncovered(8));
}

criterion_group!(benches, bench_borrow_with);
criterion_main!(benches);
