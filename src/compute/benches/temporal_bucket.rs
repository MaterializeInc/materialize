// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Micro-benchmark of the temporal bucket chain behind `mz_now()` filters.
//!
//! The scenario this models: future updates spread over a window of distinct
//! times sit in a few wide buckets, and the input frontier then sweeps through
//! the window. Every step peels the ready buckets and restores the chain, which
//! splits the frontier bucket. Bucket chunks spill to a pool, so a split that
//! touches chunks outside the split time pays for loading them.
//!
//! Run with:
//!
//! ```text
//! cargo bench -p mz-compute --features bench --bench temporal_bucket
//! ```

use mz_alloc_default as _;
use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use mz_compute::extensions::temporal_bucket::MergeBatcherWrapper;
use mz_ore::pool::Pool;
use mz_repr::{Diff, Timestamp};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::chunk::set_spill_override;
use mz_timely_util::temporal::{BucketChain, BucketRange};
use timely::Accountable;
use timely::container::PushInto;
use timely::progress::Antichain;

type Chain = BucketChain<MergeBatcherWrapper<u64, Timestamp, Diff>>;

/// Number of updates in the window.
const UPDATES: u64 = 2_000_000;
/// First time of the window.
const BASE: u64 = 1 << 30;
/// Number of distinct times in the window.
const WIDTH: u64 = 1 << 20;
/// Frontier advance per sweep step.
const STEP: u64 = 1 << 12;
/// Updates per container pushed into a bucket.
const BATCH: usize = 100_000;

/// Restores the chain to its invariants, regardless of how much fuel it takes.
fn restore(chain: &mut Chain) {
    let mut fuel = 0;
    while fuel <= 0 {
        fuel = 1_000_000;
        chain.restore(&mut fuel);
    }
}

/// A chain settled around `BASE`, so that the window lands in a few wide buckets.
fn settled_chain() -> Chain {
    let mut chain = BucketChain::new(MergeBatcherWrapper::new(None, 0));
    drop(chain.peel(Antichain::from_elem(Timestamp::from(BASE)).borrow()));
    restore(&mut chain);
    chain
}

/// Updates with pseudo-random data and times, sorted by time.
fn updates() -> Vec<(u64, Timestamp, Diff)> {
    let mut updates: Vec<_> = (0..UPDATES)
        .map(|i| {
            let x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            (
                x % UPDATES,
                Timestamp::from(BASE + (x >> 30) % WIDTH),
                Diff::ONE,
            )
        })
        .collect();
    updates.sort_by_key(|update| update.1);
    updates
}

/// Pushes `updates` into the buckets of `chain` that contain their times.
fn ingest(chain: &mut Chain, updates: &[(u64, Timestamp, Diff)]) {
    let mut buffer = Column::default();
    let mut pending = 0;
    let mut range: Option<BucketRange<Timestamp>> = None;
    for update in updates {
        if !range.as_ref().is_some_and(|r| r.contains(&update.1)) {
            if let Some(r) = range.take() {
                chain
                    .find_mut(&r.start)
                    .unwrap()
                    .push_container(&mut buffer);
                pending = 0;
            }
            range = Some(chain.range_of(&update.1).unwrap());
        }
        buffer.push_into(update);
        pending += 1;
        if pending >= BATCH {
            let r = range.as_ref().unwrap();
            chain
                .find_mut(&r.start)
                .unwrap()
                .push_container(&mut buffer);
            pending = 0;
        }
    }
    if let Some(r) = range {
        chain
            .find_mut(&r.start)
            .unwrap()
            .push_container(&mut buffer);
    }
}

/// Advances the frontier through the window, returning the number of revealed updates.
fn sweep(chain: &mut Chain) -> i64 {
    let mut revealed = 0;
    let mut frontier = BASE;
    while frontier <= BASE + WIDTH {
        frontier += STEP;
        for bucket in chain.peel(Antichain::from_elem(Timestamp::from(frontier)).borrow()) {
            revealed += bucket
                .done()
                .into_iter()
                .map(|chunk| chunk.record_count())
                .sum::<i64>();
        }
        restore(chain);
    }
    revealed
}

fn bench_temporal_bucket(c: &mut Criterion) {
    let pool = Pool::new().expect("pool creation");
    set_spill_override(Some(pool));
    let updates = updates();

    let mut group = c.benchmark_group("temporal_bucket");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(25));
    group.bench_function("ingest", |b| {
        b.iter_batched(
            settled_chain,
            |mut chain| {
                ingest(&mut chain, &updates);
                chain
            },
            BatchSize::PerIteration,
        )
    });
    group.bench_function("sweep", |b| {
        b.iter_batched(
            || {
                let mut chain = settled_chain();
                ingest(&mut chain, &updates);
                chain
            },
            |mut chain| {
                let revealed = sweep(&mut chain);
                assert_eq!(revealed, i64::try_from(UPDATES).unwrap());
                black_box(chain)
            },
            BatchSize::PerIteration,
        )
    });
    group.finish();

    set_spill_override(None);
}

criterion_group!(benches, bench_temporal_bucket);
criterion_main!(benches);
