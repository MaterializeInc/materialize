// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compares a plain columnar merge batcher with the same batcher wrapped in a
//! `TemporalBatcher`, driven the way `arrange_core` drives it. Times are
//! milliseconds.
//!
//! * `now`: every update is at the current tick, nothing in the future.
//! * `near`: updates spread over the next second, swept over θ.
//! * `tail`: a retained future tail spread over an hour, with each tick
//!   adding updates an hour out and at the current tick, swept over θ.
//!   Measures steady-state ticks after the first seal.
//! * `first_seal`: the push and first seal of a snapshot plus future tail.

use mz_alloc_default as _;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use differential_dataflow::trace::Batcher;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::{ColumnChunker, ColumnMerger};
use mz_timely_util::temporal::batcher::TemporalBatcher;
use timely::container::{ContainerBuilder, PushInto};
use timely::progress::Antichain;

type Data = (u64, u64);
type Update = (Data, u64, i64);
type Plain = MergeBatcher<ColumnMerger<Data, u64, i64>>;
type Chunker = ColumnChunker<Update>;
type Wrapped = TemporalBatcher<Plain, Chunker, Data, u64, i64>;

const TICK: u64 = 1_000;
const HOUR: u64 = 3_600_000;
const TICKS: u64 = 100;
const PER_TICK: u64 = 2_000;
const TAIL: u64 = 400_000;
const THETAS: &[u64] = &[0, 1_000, 2_000, 10_000, 60_000];

fn hash(i: u64) -> u64 {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 16
}

/// Drive one push and seal, the way `arrange_core` does, returning updates sealed.
fn step<B>(batcher: &mut B, chunker: &mut Chunker, updates: &[Update], upper: u64) -> usize
where
    B: Batcher<Time = u64, Output = <Chunker as ContainerBuilder>::Container>,
{
    let mut column = Column::default();
    for update in updates {
        column.push_into(update);
    }
    chunker.push_into(&mut column);
    while let Some(chunk) = chunker.extract() {
        batcher.push_into(std::mem::take(chunk));
    }
    while let Some(chunk) = chunker.finish() {
        batcher.push_into(std::mem::take(chunk));
    }
    let (chain, _) = batcher.seal(Antichain::from_elem(upper));
    let _ = batcher.frontier();
    chain.iter().map(|chunk| chunk.len()).sum()
}

fn tick_updates(tick: u64, offset: impl Fn(u64) -> u64) -> Vec<Update> {
    (0..PER_TICK)
        .map(|i| {
            let h = hash(tick * PER_TICK + i);
            ((h % 10_000, i), tick * TICK + offset(h), 1)
        })
        .collect()
}

fn tail_updates() -> Vec<Update> {
    (0..TAIL)
        .map(|i| {
            let h = hash(i + (1 << 40));
            ((h % 10_000, i), 1 + h % HOUR, 1)
        })
        .collect()
}

fn snapshot_updates() -> Vec<Update> {
    (0..TAIL).map(|i| ((hash(i) % 10_000, i), 0, 1)).collect()
}

fn run_ticks<B>(batcher: &mut B, updates: &[Vec<Update>]) -> usize
where
    B: Batcher<Time = u64, Output = <Chunker as ContainerBuilder>::Container>,
{
    let mut chunker = Chunker::default();
    let mut sealed = 0;
    for (tick, updates) in updates.iter().enumerate() {
        let tick = u64::try_from(tick).expect("fits") + 1;
        sealed += step(batcher, &mut chunker, updates, tick * TICK + 1);
    }
    sealed
}

fn bench_ticks(
    c: &mut Criterion,
    name: &str,
    hydrate: bool,
    offset: impl Fn(u64) -> u64 + Copy,
    thetas: &[u64],
) {
    let ticks: Vec<_> = (1..=TICKS)
        .map(|tick| {
            let mut updates = tick_updates(tick, offset);
            if hydrate {
                // Updates at the current tick, alongside the far ones.
                updates.extend(
                    tick_updates(tick, |_| 0)
                        .into_iter()
                        .map(|((key, val), time, diff)| ((key, val + PER_TICK), time, diff)),
                );
            }
            updates
        })
        .collect();
    let tail = hydrate.then(tail_updates).unwrap_or_default();
    let mut group = c.benchmark_group(name);
    group.sample_size(10);
    group.bench_function("plain", |b| {
        b.iter_batched(
            || {
                let mut batcher = Plain::new(None, 0);
                let mut chunker = Chunker::default();
                step(&mut batcher, &mut chunker, &tail, 1);
                batcher
            },
            |mut batcher| run_ticks(&mut batcher, &ticks),
            BatchSize::LargeInput,
        )
    });
    for &theta in thetas {
        group.bench_with_input(BenchmarkId::new("wrapped", theta), &theta, |b, &theta| {
            b.iter_batched(
                || {
                    let mut batcher = Wrapped::with_theta(None, 0, theta);
                    let mut chunker = Chunker::default();
                    step(&mut batcher, &mut chunker, &tail, 1);
                    batcher
                },
                |mut batcher| run_ticks(&mut batcher, &ticks),
                BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

fn bench_first_seal(c: &mut Criterion) {
    let mut input = snapshot_updates();
    input.extend(tail_updates());
    let mut group = c.benchmark_group("first_seal");
    group.sample_size(10);
    group.bench_function("plain", |b| {
        b.iter_batched(
            || Plain::new(None, 0),
            |mut batcher| step(&mut batcher, &mut Chunker::default(), &input, 1),
            BatchSize::LargeInput,
        )
    });
    for &theta in THETAS {
        let mut batcher = Wrapped::with_theta(None, 0, theta);
        step(&mut batcher, &mut Chunker::default(), &input, 1);
        let stats = batcher.stats();
        let retained = TAIL;
        eprintln!(
            "first_seal θ={theta}: touched {} parked {} retained F {retained} touched/F {:.2}",
            stats.rebuild_touched,
            stats.rebuild_parked,
            f64::from(u32::try_from(stats.rebuild_touched).expect("fits"))
                / f64::from(u32::try_from(retained).expect("fits")),
        );
        group.bench_with_input(BenchmarkId::new("wrapped", theta), &theta, |b, &theta| {
            b.iter_batched(
                || Wrapped::with_theta(None, 0, theta),
                |mut batcher| step(&mut batcher, &mut Chunker::default(), &input, 1),
                BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

fn benches(c: &mut Criterion) {
    bench_ticks(c, "now", false, |_| 0, &[2_000]);
    bench_ticks(c, "near", false, |h| h % TICK, THETAS);
    bench_ticks(c, "tail", true, |_| HOUR, THETAS);
    bench_first_seal(c);
}

criterion_group!(temporal_batcher, benches);
criterion_main!(temporal_batcher);
