// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! What the buffer pool costs a chunk batcher, with and without pressure.
//!
//! Drives [`AccountedChunkChainBatcher`] the way an arrange site does: input
//! columns through a [`ChunkChunker`], chunks into the batcher, an extract at
//! the empty frontier, then every extracted body read once, as the unchunking
//! builder does. Arms differ only in where committed chunks live:
//!
//! * `resident`: no pool, every chunk stays on the heap.
//! * `pool`: no budget, so nothing is evicted and the difference to
//!   `resident` is the pool's fits-in-memory overhead.
//! * `pool_tight`: a budget of a quarter of the input, evicting inline on
//!   the worker.
//! * `pool_tight_spill`: the same budget, evicting on two spill threads.
//!
//! Every arm's sealed output is checksummed and asserted equal to the
//! resident arm's, so the pressure arms also check eviction end to end.

use mz_alloc_default as _;

use columnar::Index;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use differential_dataflow::batcher::Extract;
use mz_ore::pool::Pool;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::chunk::{
    AccountedChunkChainBatcher, ChunkChunker, ComputeSpill, set_spill_override,
};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use timely::container::{ContainerBuilder, PushInto};
use timely::progress::Antichain;

type Data = (u64, String);
type Time = u64;
type Diff = i64;
type Tuple = (Data, Time, Diff);
type Bat = AccountedChunkChainBatcher<Data, Time, Diff, ComputeSpill>;
type Chu = ChunkChunker<Data, Time, Diff, ComputeSpill>;

/// Updates per pushed column, matching a typical arrange-site input batch.
const PER_ROUND: usize = 64 << 10;

/// Serialized bytes per update, measured for this shape: an 8-byte key, a
/// 25-byte string with its offset, an 8-byte time, and an 8-byte diff.
const BYTES_PER_UPDATE: usize = 57;

fn make_rounds(seed: u64, records: usize) -> Vec<Column<Tuple>> {
    let mut rng = StdRng::seed_from_u64(seed);
    let key_range = u64::try_from(records).expect("fits");
    (0..records.div_ceil(PER_ROUND))
        .map(|_| {
            let mut col: Column<Tuple> = Default::default();
            for _ in 0..PER_ROUND {
                let k = rng.random_range(0..key_range);
                let tuple: Tuple = (
                    (k, format!("v{k:024x}")),
                    rng.random_range(0..4u64),
                    rng.random_range(1..=3i64),
                );
                col.push_into(&tuple);
            }
            col
        })
        .collect()
}

/// Push every round, seal everything, and read every sealed body once.
/// Returns an order-sensitive checksum of the sealed updates.
fn drive(rounds: Vec<Column<Tuple>>) -> u64 {
    let mut chunker = Chu::default();
    let mut batcher = Bat::new(None, 0);
    for mut round in rounds {
        chunker.push_into(&mut round);
        while let Some(chunk) = chunker.extract() {
            batcher.push_chunk(std::mem::take(chunk));
        }
    }
    while let Some(chunk) = chunker.finish() {
        batcher.push_chunk(std::mem::take(chunk));
    }
    let (chains, _) = batcher.extract(Antichain::new().borrow());
    let mut sum = 0u64;
    for chunk in chains.into_iter().flatten() {
        let body = chunk.into_body();
        let ((keys, _), times, diffs) = body.borrow();
        for i in 0..keys.len() {
            let diff = u64::from_ne_bytes(Index::get(&diffs, i).to_ne_bytes());
            sum = sum.rotate_left(5).wrapping_add(
                *Index::get(&keys, i) ^ (*Index::get(&times, i) << 48) ^ (diff << 56),
            );
        }
    }
    sum
}

fn bench(c: &mut Criterion) {
    let pool = Pool::new().expect("pool reservation");
    // Keep evicted extents compressed in memory: the bench measures the
    // pool's CPU, not the swap device.
    pool.set_rss_target(usize::MAX / 2);
    let mut group = c.benchmark_group("chunk_pool");
    group.sample_size(10);
    for &(label, records) in &[("1M", 1usize << 20), ("4M", 4 << 20)] {
        let rounds = make_rounds(0x5eed, records);
        let expected = drive(rounds.clone());
        let tight = records * BYTES_PER_UPDATE / 4;
        group.throughput(Throughput::Elements(u64::try_from(records).expect("fits")));
        let arms: [(&str, Option<(usize, usize)>); 4] = [
            ("resident", None),
            ("pool", Some((usize::MAX, 0))),
            ("pool_tight", Some((tight, 0))),
            ("pool_tight_spill", Some((tight, 2))),
        ];
        for (arm, config) in arms {
            if let Some((budget, threads)) = config {
                pool.set_budget(budget);
                pool.set_spill_threads(threads);
            }
            let before = pool.stats();
            group.bench_with_input(BenchmarkId::new(arm, label), &(), |b, _| {
                set_spill_override(config.map(|_| pool.clone()));
                b.iter_batched(
                    || rounds.clone(),
                    |rounds| assert_eq!(drive(rounds), expected, "{arm} diverged"),
                    BatchSize::LargeInput,
                );
                set_spill_override(None);
            });
            let after = pool.stats();
            if config.is_some() {
                eprintln!(
                    "{arm}/{label}: inserts {} evictions {} (compress {}, cheap {}) \
                     direct_extent {} admissions {}",
                    after.inserts - before.inserts,
                    after.evictions_compress + after.evictions_cheap
                        - before.evictions_compress
                        - before.evictions_cheap,
                    after.evictions_compress - before.evictions_compress,
                    after.evictions_cheap - before.evictions_cheap,
                    after.direct_extent_inserts - before.direct_extent_inserts,
                    after.admissions_budget + after.admissions_steal
                        - before.admissions_budget
                        - before.admissions_steal,
                );
            }
        }
        pool.set_spill_threads(0);
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
