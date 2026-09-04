// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-element costs of a serialized [`Column`].
//!
//! `Column::borrow` on the `Align` variant runs `indexed::decode`, which rebuilds the
//! struct-of-arrays view from the serialized index. A caller that borrows once per element pays
//! that per element, a caller that borrows once per column pays it once. The `Typed` variant
//! serves `borrow` without a decode and is the floor. The encode group measures minting a
//! ship-sized body into a fresh allocation against an allocation that is reused.
//!
//! Run with:
//!
//!     cargo bench -p mz-timely-util --bench column_borrow

use columnar::bytes::indexed;
use columnar::{Borrow, Columnar, Index, Len, Push};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mz_ore::cast::CastFrom;
use mz_timely_util::columnar::Column;
use std::hint::black_box;

/// Update shape: a byte payload plus a time and a diff, matching the correction buffer's
/// `(D, Timestamp, Diff)`.
type Update = (Vec<u8>, u64, i64);

/// Elements per column in the borrow group.
const COUNT: usize = 1 << 16;

/// Ship boundary in words, matching the columnar merge machinery.
const SHIP_WORDS: usize = 1 << 18;

fn container(count: usize) -> <Update as Columnar>::Container {
    let mut container = <Update as Columnar>::Container::default();
    for i in 0..count {
        let payload = format!("payload-{i:016}").into_bytes();
        container.push(&(payload, u64::cast_from(i), 1i64));
    }
    container
}

/// A container filled to the ship boundary, the size of body the correction buffer mints.
fn ship_sized_container() -> <Update as Columnar>::Container {
    let mut container = <Update as Columnar>::Container::default();
    let mut pushed: usize = 0;
    while indexed::length_in_words(&container.borrow()) < SHIP_WORDS {
        let payload = format!("payload-{pushed:016}").into_bytes();
        container.push(&(payload, u64::cast_from(pushed), 1i64));
        pushed += 1;
    }
    container
}

fn encode(container: &<Update as Columnar>::Container, into: &mut Vec<u64>) {
    indexed::encode(into, &container.borrow());
}

fn bench_borrow(c: &mut Criterion) {
    let typed: Column<Update> = Column::Typed(container(COUNT));
    let mut words = Vec::new();
    encode(&container(COUNT), &mut words);
    let align: Column<Update> = Column::Align(words);
    assert_eq!(align.borrow().len(), COUNT);

    let mut group = c.benchmark_group("column_borrow");
    group.throughput(Throughput::Elements(u64::cast_from(COUNT)));

    group.bench_function("typed/per_element", |b| {
        b.iter(|| {
            for i in 0..COUNT {
                black_box(typed.borrow().get(i));
            }
        })
    });
    group.bench_function("align/per_element", |b| {
        b.iter(|| {
            for i in 0..COUNT {
                black_box(align.borrow().get(i));
            }
        })
    });
    group.bench_function("align/hoisted", |b| {
        b.iter(|| {
            let borrowed = align.borrow();
            for i in 0..COUNT {
                black_box(borrowed.get(i));
            }
        })
    });
    group.finish();
}

fn bench_encode(c: &mut Criterion) {
    let source = ship_sized_container();
    let words = indexed::length_in_words(&source.borrow());

    let mut group = c.benchmark_group("column_encode");
    group.throughput(Throughput::Bytes(u64::cast_from(
        words * std::mem::size_of::<u64>(),
    )));

    group.bench_function("fresh_allocation", |b| {
        b.iter(|| {
            let mut alloc: Vec<u64> = Vec::with_capacity(words);
            encode(&source, &mut alloc);
            black_box(alloc)
        })
    });
    let mut reused: Vec<u64> = Vec::with_capacity(words);
    group.bench_function("reused_allocation", |b| {
        b.iter(|| {
            reused.clear();
            encode(&source, &mut reused);
            black_box(&reused);
        })
    });
    group.finish();
}

criterion_group!(benches, bench_borrow, bench_encode);
criterion_main!(benches);
