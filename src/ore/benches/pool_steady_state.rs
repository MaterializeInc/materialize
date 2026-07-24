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

//! Steady-state pool dynamics under re-admission.
//!
//! Sustains a mixed workload against a budgeted pool for many rounds and
//! reports per-round counter rates and per-probe costs at steady state. The
//! population mimics production shape: a long-lived deep-band sealed set
//! (hot chunks probed with admitting reads every round, a cold tail probed
//! sparsely with plain reads) plus a young churn stream that inserts and
//! frees depth-0 chunks each round, keeping budget enforcement active.
//!
//! The scenarios answer the re-admission policy questions directly:
//!
//! * `hot fits budget`: does the hot set converge to resident, with hot
//!   probes becoming slot copies and admissions tapering to re-admissions
//!   only when churn pressure occasionally wins?
//! * `hot 2x budget`: is over-budget thrash bounded, served by clean-victim
//!   steals at decompress cost, rather than collapsing into denials?
//! * `hot 2x, eager off`: the clean-victim supply problem, with no eager
//!   backing there is nothing to steal, so admissions starve.
//! * `hot fits, plain reads`: the no-admission control that prices what
//!   admission buys.
//!
//! By default the RSS target is set high so the compressed tier holds
//! extents in RAM and device pageout stays out of the picture, which also
//! keeps the run meaningful off Linux, and timings then cover CPU work only
//! (fills, copies, compression, decompression). On a Linux box with real
//! swap provisioned, set `POOL_BENCH_RSS_TARGET_MIB` low (for example 64)
//! to force tier pageout to the device, so cold and denied probes pay real
//! swap-in. `POOL_BENCH_SCALE` multiplies the population and budget to
//! size the working set against the machine.
//!
//! ```text
//! cargo bench -p mz-ore --features pool --bench pool_steady_state
//! POOL_BENCH_RSS_TARGET_MIB=64 POOL_BENCH_SCALE=4 \
//!     cargo bench -p mz-ore --features pool --bench pool_steady_state
//! ```

use std::time::Instant;

use mz_ore::pool::{ChunkHandle, ChunkHints, ExtentCodec, Pool, PoolStats};

/// Bench-local lz4 codec with the production framing: a little-endian `u32`
/// body-length prefix followed by one lz4 block.
#[derive(Debug)]
struct Lz4Codec;

static CODEC: Lz4Codec = Lz4Codec;

impl ExtentCodec for Lz4Codec {
    fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
        let max_out = lz4_flex::block::get_maximum_output_size(body.len());
        out.resize(4 + max_out, 0);
        let len = u32::try_from(body.len()).expect("bounded payloads");
        out[..4].copy_from_slice(&len.to_le_bytes());
        let compressed = lz4_flex::block::compress_into(body, &mut out[4..]).expect("sized");
        out.truncate(4 + compressed);
    }

    fn decode(&self, stored: &[u8], body: &mut [u8]) {
        let prefix: [u8; 4] = stored[..4].try_into().expect("prefix");
        let len = usize::try_from(u32::from_le_bytes(prefix)).expect("length fits");
        assert_eq!(
            len,
            body.len(),
            "destination must match the encoded body length"
        );
        let written =
            lz4_flex::block::decompress_into(&stored[4..], body).expect("valid lz4 block");
        assert_eq!(written, body.len());
    }
}

/// 64 KiB chunks: the smallest spillable class, so budgets hold thousands of
/// chunks and rounds stay fast.
const CHUNK_WORDS: usize = (64 << 10) / 8;
const CHUNK_BYTES: usize = 64 << 10;

fn rng(mut seed: u64) -> impl FnMut() -> u64 {
    move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    }
}

/// Half-compressible payload: alternating random and repeated words, landing
/// near the ratios measured on real arrangement data.
fn payload(seed: u64) -> Vec<u64> {
    let mut r = rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);
    (0..CHUNK_WORDS as u64)
        .map(|i| if i % 2 == 0 { r() } else { seed })
        .collect()
}

fn insert(pool: &Pool, depth: u8, data: &[u64]) -> ChunkHandle {
    pool.insert_with(data.len(), ChunkHints { depth }, &CODEC, |dst| {
        dst.copy_from_slice(data)
    })
}

struct Scenario {
    name: &'static str,
    /// Resident budget in 64 KiB slots.
    budget_slots: usize,
    hot: usize,
    cold: usize,
    churn_per_round: usize,
    eager: bool,
    /// Hot probes use `read_into_admit` (true) or plain `read_into`.
    admit_hot: bool,
}

/// Per-round steady-state deltas of the counters this bench interprets.
fn delta(a: &PoolStats, b: &PoolStats, rounds: f64) -> Vec<(&'static str, f64)> {
    vec![
        (
            "adm_budget",
            (b.admissions_budget - a.admissions_budget) as f64 / rounds,
        ),
        (
            "adm_steal",
            (b.admissions_steal - a.admissions_steal) as f64 / rounds,
        ),
        (
            "adm_denied",
            (b.admissions_denied - a.admissions_denied) as f64 / rounds,
        ),
        (
            "eager_backs",
            (b.eager_backs - a.eager_backs) as f64 / rounds,
        ),
        (
            "evict_cheap",
            (b.evictions_cheap - a.evictions_cheap) as f64 / rounds,
        ),
        (
            "evict_compress",
            (b.evictions_compress - a.evictions_compress) as f64 / rounds,
        ),
        (
            "writes_elided",
            (b.writes_elided - a.writes_elided) as f64 / rounds,
        ),
        (
            "warm_reuses",
            (b.warm_reuses - a.warm_reuses) as f64 / rounds,
        ),
    ]
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn run(pool: &Pool, s: &Scenario) {
    const WARMUP: usize = 20;
    const ROUNDS: usize = 100;

    let scale = env_usize("POOL_BENCH_SCALE", 1);
    let cold_probes = 256 * scale;
    let (budget_slots, hot_count, cold_count, churn_count) = (
        s.budget_slots * scale,
        s.hot * scale,
        s.cold * scale,
        s.churn_per_round * scale,
    );

    pool.set_budget(budget_slots * CHUNK_BYTES);
    // High default keeps the compressed tier resident (no device pageout);
    // override on a swap-provisioned Linux box to exercise the device path.
    pool.set_rss_target(env_usize("POOL_BENCH_RSS_TARGET_MIB", 64 << 10) << 20);
    pool.set_spill_threads(2);
    pool.set_eager_backing(s.eager);

    // Long-lived sealed population at the deepest band: eviction-preferred,
    // protected only by their touched bits, exactly like sealed batches.
    let body = payload(7);
    let hot: Vec<ChunkHandle> = (0..hot_count)
        .map(|i| insert(pool, 3, &payload(i as u64)))
        .collect();
    let cold: Vec<ChunkHandle> = (0..cold_count)
        .map(|i| insert(pool, 3, &payload(1_000_000 + i as u64)))
        .collect();

    let mut r = rng(42);
    let mut dst = Vec::new();
    let (mut hot_ns, mut cold_ns, mut churn_ns) = (0u128, 0u128, 0u128);
    let mut start_stats = pool.stats();
    let mut start_wall = Instant::now();

    for round in 0..(WARMUP + ROUNDS) {
        if round == WARMUP {
            start_stats = pool.stats();
            start_wall = Instant::now();
            (hot_ns, cold_ns, churn_ns) = (0, 0, 0);
        }

        // Young churn: insert, hold across the probes, die young.
        let t = Instant::now();
        let churn: Vec<ChunkHandle> = (0..churn_count).map(|_| insert(pool, 0, &body)).collect();
        churn_ns += t.elapsed().as_nanos();

        // Hot probes: every hot chunk once per round.
        let t = Instant::now();
        for h in &hot {
            if s.admit_hot {
                h.read_into_admit(&mut dst);
            } else {
                h.read_into(&mut dst);
            }
            std::hint::black_box(dst.first().copied());
        }
        hot_ns += t.elapsed().as_nanos();

        // Sparse cold probes: plain reads of random cold chunks.
        let t = Instant::now();
        for _ in 0..cold_probes {
            cold[(r() as usize) % cold.len()].read_into(&mut dst);
            std::hint::black_box(dst.first().copied());
        }
        cold_ns += t.elapsed().as_nanos();

        let t = Instant::now();
        drop(churn);
        churn_ns += t.elapsed().as_nanos();
    }

    let end_stats = pool.stats();
    let wall = start_wall.elapsed().as_secs_f64();
    println!(
        "\n== {} == (budget {} slots, hot {}, cold {}, churn {}/round, eager {}, admit {})",
        s.name, budget_slots, hot_count, cold_count, churn_count, s.eager, s.admit_hot,
    );
    println!(
        "  wall {:.2}s for {ROUNDS} rounds; hot {:.1} us/probe, cold {:.1} us/probe, churn {:.1} us/chunk",
        wall,
        hot_ns as f64 / 1000.0 / (ROUNDS * hot_count) as f64,
        cold_ns as f64 / 1000.0 / (ROUNDS * cold_probes) as f64,
        churn_ns as f64 / 1000.0 / (ROUNDS * churn_count) as f64,
    );
    print!("  per round:");
    for (name, rate) in delta(&start_stats, &end_stats, ROUNDS as f64) {
        print!(" {name} {rate:.1}");
    }
    println!(
        "\n  resident {:.1} MiB, extent-resident {:.1} MiB, live {} chunks",
        end_stats.resident_bytes as f64 / (1 << 20) as f64,
        end_stats.extent_resident_bytes as f64 / (1 << 20) as f64,
        end_stats.live_chunks,
    );
}

/// The value of `ChunkHints::depth`, measured: a generational merge
/// simulation runs the identical workload under different depth labelings,
/// and the differences in merge re-read cost, sealed probe cost, write
/// elision, and the eviction mix are what the hints buy.
///
/// The shape mimics a spine: generation 0 chunks are merge inputs, re-read
/// whole and freed one round after insert; generations 1 and 2 live 4 and
/// 16 rounds before their consumption reads; generation 3 is a long-lived
/// sealed population whose hot half is probed every round and whose cold
/// half is the eviction candidate the budget wants. Correct labels should
/// route evictions to the cold deep half (cheap, pre-backed by eager
/// backing), keep merge inputs resident until consumed, and elide their
/// writes. Inverted labels should evict merge inputs before consumption
/// and waste compression on chunks about to die.
fn depth_ladder(pool: &Pool, label: &str, depth_of: impl Fn(u8) -> u8) {
    const WARMUP: usize = 24;
    const ROUNDS: usize = 96;

    pool.set_budget(640 * CHUNK_BYTES);
    pool.set_rss_target(env_usize("POOL_BENCH_RSS_TARGET_MIB", 64 << 10) << 20);
    pool.set_spill_threads(2);
    pool.set_eager_backing(true);

    let body = payload(11);
    // Generation 3: 512 sealed chunks, hot half probed every round.
    let sealed: Vec<ChunkHandle> = (0..512)
        .map(|i| insert(pool, depth_of(3), &payload(5_000_000 + i as u64)))
        .collect();

    let mut r = rng(77);
    let mut dst = Vec::new();
    // Pending consumption queues: (due_round, chunks).
    let mut pending: Vec<(usize, u8, Vec<ChunkHandle>)> = Vec::new();
    let (mut merge_ns, mut probe_ns) = (0u128, 0u128);
    let mut merge_reads = 0usize;
    let mut start_stats = pool.stats();
    let start_wall = Instant::now();

    for round in 0..(WARMUP + ROUNDS) {
        if round == WARMUP {
            start_stats = pool.stats();
            (merge_ns, probe_ns, merge_reads) = (0, 0, 0);
        }

        // Insert this round's young generations.
        let mut due: Vec<(usize, u8, usize, usize)> = vec![(round + 1, 0, 128, 0)];
        if round % 2 == 0 {
            due.push((round + 4, 1, 32, 1));
        }
        if round % 8 == 0 {
            due.push((round + 16, 2, 16, 2));
        }
        for (due_round, generation, count, _) in due {
            let chunks = (0..count)
                .map(|_| insert(pool, depth_of(generation), &body))
                .collect();
            pending.push((due_round, generation, chunks));
        }

        // Consume due generations: whole-chunk merge re-reads, then free.
        let t = Instant::now();
        let mut i = 0;
        while i < pending.len() {
            if pending[i].0 <= round {
                let (_, _, chunks) = pending.swap_remove(i);
                for c in &chunks {
                    c.read_into(&mut dst);
                    std::hint::black_box(dst.first().copied());
                    merge_reads += 1;
                }
                drop(chunks);
            } else {
                i += 1;
            }
        }
        merge_ns += t.elapsed().as_nanos();

        // Probe the sealed hot half.
        let t = Instant::now();
        for _ in 0..256 {
            sealed[(r() as usize) % 256].read_into(&mut dst);
            std::hint::black_box(dst.first().copied());
        }
        probe_ns += t.elapsed().as_nanos();
    }

    let end = pool.stats();
    println!(
        "\n== depth ladder: {label} == ({:.2}s wall)",
        start_wall.elapsed().as_secs_f64()
    );
    println!(
        "  merge re-read {:>6.1} us/chunk   sealed probe {:>6.1} us/probe",
        merge_ns as f64 / 1000.0 / merge_reads as f64,
        probe_ns as f64 / 1000.0 / (ROUNDS * 256) as f64,
    );
    print!("  per round:");
    for (name, rate) in delta(&start_stats, &end, ROUNDS as f64) {
        print!(" {name} {rate:.1}");
    }
    println!(
        "\n  resident {:.1} MiB, extent-resident {:.1} MiB",
        end.resident_bytes as f64 / (1 << 20) as f64,
        end.extent_resident_bytes as f64 / (1 << 20) as f64,
    );
}

fn main() {
    // Budget 640 slots (40 MiB of 64 KiB chunks). Churn keeps enforcement
    // active every round in every scenario.
    let scenarios = [
        Scenario {
            name: "hot fits budget",
            budget_slots: 640,
            hot: 512,
            cold: 4096,
            churn_per_round: 256,
            eager: true,
            admit_hot: true,
        },
        Scenario {
            name: "hot 2x budget",
            budget_slots: 640,
            hot: 1280,
            cold: 4096,
            churn_per_round: 256,
            eager: true,
            admit_hot: true,
        },
        Scenario {
            name: "hot 2x budget, eager off",
            budget_slots: 640,
            hot: 1280,
            cold: 4096,
            churn_per_round: 256,
            eager: false,
            admit_hot: true,
        },
        Scenario {
            name: "hot fits, plain reads (control)",
            budget_slots: 640,
            hot: 512,
            cold: 4096,
            churn_per_round: 256,
            eager: true,
            admit_hot: false,
        },
    ];
    // One pool for the whole process: reservations are tens of TiB of
    // virtual space and spill threads keep a pool alive, so per-scenario
    // pools exhaust the VM map. Scenarios retune it and drop their
    // populations; every reported rate is a post-warmup delta.
    let pool = Pool::new().expect("pool reservation");
    for s in &scenarios {
        run(&pool, s);
    }

    // The depth-hint pricing: identical workloads, different labelings.
    depth_ladder(&pool, "correct labels (gen = depth)", |g| g);
    depth_ladder(&pool, "flat labels (all depth 0)", |_| 0);
    depth_ladder(&pool, "flat labels (all depth 3)", |_| 3);
    depth_ladder(&pool, "inverted labels (3 - gen)", |g| 3 - g);
}
