// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Throughput benchmark for `ConsolidatingColumnBuilder` against:
//!
//! * `ConsolidatingContainerBuilder<Vec<_>>` — DD's existing AoS consolidator, the
//!   apples-to-apples comparison for what we are replacing.
//! * Bare `ColumnBuilder` (no staging, no consolidation) — upper bound on push throughput
//!   when consolidation has nothing to remove.
//!
//! Run with:
//!
//!     cargo bench -p mz-timely-util --bench consolidating_column_builder

use columnar::Len;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::consolidate::ConsolidatingColumnBuilder;
use timely::container::{ContainerBuilder, PushInto};

type Item = (u64, u64, i64);

const N: u64 = 10_000_000;

/// Mirror `timely::Session::give`: push one item, drain everything `extract` yields.
#[inline]
fn run_cons<F: Fn(u64) -> Item>(mk: F) {
    let mut builder = ConsolidatingColumnBuilder::<u64, u64, i64>::default();
    let mut sink: u64 = 0;
    for i in 0..N {
        builder.push_into(mk(std::hint::black_box(i)));
        while let Some(c) = builder.extract() {
            sink = sink.wrapping_add(c.borrow().len() as u64);
        }
    }
    while let Some(c) = builder.finish() {
        sink = sink.wrapping_add(c.borrow().len() as u64);
    }
    std::hint::black_box(sink);
}

#[inline]
fn run_bare<F: Fn(u64) -> Item>(mk: F) {
    let mut builder = ColumnBuilder::<Item>::default();
    let mut sink: u64 = 0;
    for i in 0..N {
        builder.push_into(mk(std::hint::black_box(i)));
        while let Some(c) = builder.extract() {
            sink = sink.wrapping_add(c.borrow().len() as u64);
        }
    }
    while let Some(c) = builder.finish() {
        sink = sink.wrapping_add(c.borrow().len() as u64);
    }
    std::hint::black_box(sink);
}

#[inline]
fn run_dd<F: Fn(u64) -> Item>(mk: F) {
    let mut builder = ConsolidatingContainerBuilder::<Vec<Item>>::default();
    let mut sink: u64 = 0;
    for i in 0..N {
        builder.push_into(mk(std::hint::black_box(i)));
        while let Some(c) = builder.extract() {
            sink = sink.wrapping_add(c.len() as u64);
        }
    }
    while let Some(c) = builder.finish() {
        sink = sink.wrapping_add(c.len() as u64);
    }
    std::hint::black_box(sink);
}

fn bench_workload<F>(c: &mut Criterion, name: &str, mk: F)
where
    F: Fn(u64) -> Item + Copy,
{
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(N * std::mem::size_of::<Item>() as u64));
    group.bench_function("ConsolidatingColumnBuilder", |b| b.iter(|| run_cons(mk)));
    group.bench_function("ConsolidatingContainerBuilder", |b| b.iter(|| run_dd(mk)));
    group.bench_function("ColumnBuilder", |b| b.iter(|| run_bare(mk)));
    group.finish();
}

fn bench(c: &mut Criterion) {
    // Closures are passed as separate type parameters per call so each workload monomorphizes
    // `run_*`; collecting them into a `&[fn(u64) -> Item]` array adds an indirect call to every
    // push that dwarfs the columnar hot path.
    bench_workload(c, "cancel-pairs", |i| {
        (i / 2, 0, if i % 2 == 0 { 1 } else { -1 })
    });
    bench_workload(c, "single-key", |_| (42, 0, 1));
    bench_workload(c, "all-distinct", |i| (i, 0, 1));
    bench_workload(c, "dup-pairs", |i| (i / 2, 0, 1));
}

criterion_group!(benches, bench);
criterion_main!(benches);
