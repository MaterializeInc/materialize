// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Plain vs temporal merge batcher across the three workload regimes that
//! decide whether the temporal batcher can be enabled everywhere:
//!
//! 1. `all_past`: no future updates. The temporal batcher must be at parity
//!    with the plain batcher (its fast path is the plain code plus a branch).
//! 2. `raced_ahead`: data one tick ahead of the frontier, within the
//!    threshold. Both batchers treat it identically (flat re-merge), so
//!    parity is expected here too.
//! 3. `hydration`/`near_heavy`/`steady_tail`: genuine far-future data
//!    (temporal-filter retraction tails). The plain batcher re-walks the held
//!    tail at every seal; the temporal batcher parks it in the bucket chain.
//!
//! A third arm (the standalone temporal-bucket *operator* feeding a plain
//! batcher, today's production mechanism for temporal workloads) lives in
//! mz-compute and needs a dataflow harness; it is intentionally not
//! replicated here.

use criterion::{Criterion, criterion_group, criterion_main};
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::trace::Batcher;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use mz_timely_util::columnation::{ColInternalMerger, ColTemporalMerger, ColumnationStack};
use mz_timely_util::merge_batcher::TemporalBucketingMergeBatcher;
use timely::container::PushInto;
use timely::progress::Antichain;

type Update = (u64, u64, i64);
type Merger = ColInternalMerger<u64, u64, i64>;
type Plain = MergeBatcher<Merger>;
type Temporal = TemporalBucketingMergeBatcher<ColTemporalMerger<u64, u64, i64>>;

/// ~2026-07-11 in milliseconds since the epoch.
const NOW: u64 = 1_752_192_000_000;
const TICK: u64 = 1_000;
const WINDOW: u64 = 45 * 24 * 3_600 * 1_000;
/// Mirrors the chunk sizes the production chunker emits.
const CHUNK: usize = 8_192;

/// One seal's worth of work: chunks to push, then a seal at `upper`.
struct Step {
    chunks: Vec<ColumnationStack<Update>>,
    upper: u64,
}

fn steps(raw: Vec<(Vec<Update>, u64)>) -> Vec<Step> {
    raw.into_iter()
        .map(|(mut updates, upper)| {
            consolidate_updates(&mut updates);
            let chunks = updates
                .chunks(CHUNK)
                .map(|slice| {
                    let mut chunk = ColumnationStack::default();
                    for update in slice {
                        chunk.push_into(*update);
                    }
                    chunk
                })
                .collect();
            Step { chunks, upper }
        })
        .collect()
}

/// Drive a batcher through `steps` plus a final flush; returns emitted
/// records as a sink the optimizer can't discard.
fn run<B>(steps: &[Step]) -> usize
where
    B: Batcher<Time = u64, Output = ColumnationStack<Update>> + PushInto<ColumnationStack<Update>>,
{
    let mut batcher = B::new(None, 0);
    let mut emitted = 0;
    for step in steps {
        for chunk in &step.chunks {
            batcher.push_into(chunk.clone());
        }
        let (chain, _description) = batcher.seal(Antichain::from_elem(step.upper));
        emitted += chain.iter().map(|c| c[..].len()).sum::<usize>();
    }
    let (chain, _description) = batcher.seal(Antichain::new());
    emitted += chain.iter().map(|c| c[..].len()).sum::<usize>();
    emitted
}

/// Regime 1: all data at the current tick, no future updates.
fn all_past(ticks: u64, per_tick: u64) -> Vec<Step> {
    let mut raw = Vec::new();
    for tick in 0..ticks {
        let upper = NOW + (tick + 1) * TICK;
        let updates = (0..per_tick)
            .map(|i| (tick * per_tick + i, upper - 1, 1))
            .collect();
        raw.push((updates, upper));
    }
    steps(raw)
}

/// Regime 2: every record one tick ahead of the seal upper (within the 2s
/// default threshold), re-merged once before release, in both batchers.
fn raced_ahead(ticks: u64, per_tick: u64) -> Vec<Step> {
    let mut raw = Vec::new();
    for tick in 0..ticks {
        let upper = NOW + (tick + 1) * TICK;
        let updates = (0..per_tick)
            .map(|i| (tick * per_tick + i, upper + TICK, 1))
            .collect();
        raw.push((updates, upper));
    }
    steps(raw)
}

/// Regime 3, hydration shape: a snapshot below the upper plus a
/// temporal-filter retraction tail uniform over the window, all sealed once,
/// followed by quiet ticks.
fn hydration(records: u64, ticks: u64) -> Vec<Step> {
    let mut updates = Vec::new();
    for i in 0..records {
        updates.push((i, NOW - 1 - (i % 1_000), 1));
        updates.push((i, NOW + 1 + i * (WINDOW / records), -1));
    }
    let mut raw = vec![(updates, NOW)];
    for tick in 1..=ticks {
        raw.push((Vec::new(), NOW + tick * TICK));
    }
    steps(raw)
}

/// Regime 3, adversarial: tail mass geometrically concentrated near the
/// frontier at every scale (the worst distribution the simulation found).
fn near_heavy(records: u64, ticks: u64) -> Vec<Step> {
    let mut updates = Vec::new();
    for i in 0..records {
        updates.push((i, NOW - 1 - (i % 1_000), 1));
        let distance = (1_u64 << (i % 33)).min(WINDOW) + (i % 977);
        updates.push((i, NOW + distance, -1));
    }
    let mut raw = vec![(updates, NOW)];
    for tick in 1..=ticks {
        raw.push((Vec::new(), NOW + tick * TICK));
    }
    steps(raw)
}

/// Regime 3, steady state: a held tail plus per-tick current data and a
/// trickle of new far-future retractions, over many ticks. This is where the
/// plain batcher pays O(held) per seal and the temporal batcher does not.
fn steady_tail(tail: u64, ticks: u64, per_tick: u64) -> Vec<Step> {
    let mut updates = Vec::new();
    for i in 0..tail {
        updates.push((i, NOW + 1 + i * (WINDOW / tail), -1));
    }
    let mut raw = vec![(updates, NOW)];
    for tick in 1..=ticks {
        let upper = NOW + tick * TICK;
        let mut updates: Vec<Update> = (0..per_tick)
            .map(|i| (tail + tick * per_tick + i, upper - 1, 1))
            .collect();
        updates.push((tail + tick, upper - 1 + WINDOW, -1));
        raw.push((updates, upper));
    }
    steps(raw)
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_merge_batcher");
    group.sample_size(10);

    let scenarios: Vec<(&str, Vec<Step>)> = vec![
        ("all_past", all_past(256, 2_048)),
        ("raced_ahead", raced_ahead(256, 2_048)),
        ("hydration", hydration(262_144, 10)),
        ("near_heavy", near_heavy(262_144, 10)),
        ("steady_tail", steady_tail(65_536, 512, 512)),
    ];

    for (name, steps) in &scenarios {
        group.bench_function(format!("{name}/plain"), |b| {
            b.iter(|| std::hint::black_box(run::<Plain>(steps)))
        });
        group.bench_function(format!("{name}/temporal"), |b| {
            b.iter(|| std::hint::black_box(run::<Temporal>(steps)))
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
