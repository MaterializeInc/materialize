// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Plumbing benchmarks: direct actor channel, no gRPC overhead.
//!
//! All benchmarks use `NoopWalWriter` to isolate the actor loop from S3 I/O.
//! Flushes are triggered explicitly via `ActorCommand::Flush`.

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion};
use mz_persist_consensus_svc::wal::LatencyProfile;
use tokio::runtime::Runtime;

use crate::{
    cas_and_flush, send_cas, send_flush, send_head, send_scan, send_truncate_and_flush,
    spawn_bench_actor, spawn_latency_actor,
};

/// Head reads on committed state. Measures baseline channel round-trip +
/// BTreeMap lookup. Parameterized by number of shards to verify O(log n).
pub fn bench_head(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("plumbing/head");
    for num_shards in [1, 100, 1000] {
        g.bench_with_input(
            BenchmarkId::new("shards", num_shards),
            &num_shards,
            |b, &num_shards| {
                let (tx, _handle) = spawn_bench_actor(runtime);
                // Pre-populate shards with committed state.
                runtime.block_on(async {
                    for i in 0..num_shards {
                        cas_and_flush(&tx, &format!("shard-{}", i), None, 1, &[0u8; 64]).await;
                    }
                });
                b.iter(|| {
                    runtime.block_on(send_head(&tx, "shard-0"));
                });
            },
        );
    }
}

/// CAS + flush on a single shard. Measures the full write path with varying
/// value sizes. 4 KiB is the persist inline write ceiling.
pub fn bench_cas_flush(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("plumbing/cas_flush");
    for data_size in [64, 1024, 4096] {
        g.bench_with_input(
            BenchmarkId::new("bytes", data_size),
            &data_size,
            |b, &size| {
                let (tx, _handle) = spawn_bench_actor(runtime);
                let data = vec![0u8; size];
                let mut seqno = 0u64;
                b.iter(|| {
                    seqno += 1;
                    let expected = if seqno == 1 { None } else { Some(seqno - 1) };
                    runtime.block_on(cas_and_flush(&tx, "s", expected, seqno, &data));
                });
            },
        );
    }
}

/// Scan reads on a shard with varying history depth. Measures proto
/// materialization cost (data is cloned into the response).
pub fn bench_scan(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("plumbing/scan");
    for num_entries in [10, 100, 1000] {
        g.bench_with_input(
            BenchmarkId::new("entries", num_entries),
            &num_entries,
            |b, &num_entries| {
                let (tx, _handle) = spawn_bench_actor(runtime);
                // Pre-populate the shard with num_entries committed entries.
                runtime.block_on(async {
                    for i in 1..=num_entries {
                        let expected = if i == 1 { None } else { Some(i as u64 - 1) };
                        cas_and_flush(&tx, "s", expected, i as u64, &[0u8; 64]).await;
                    }
                });
                b.iter(|| {
                    runtime.block_on(send_scan(&tx, "s", 0, u64::MAX));
                });
            },
        );
    }
}

/// The key design metric: group commit amortization. Sends N CAS ops then
/// one flush. Uses `iter_custom` to normalize cost per-flush. The service's
/// whole value proposition is that this stays ~constant as N grows.
pub fn bench_group_commit(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("plumbing/group_commit");
    for batch_size in [1, 10, 50, 100, 500] {
        g.bench_with_input(
            BenchmarkId::new("ops_per_flush", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter_custom(|iters| {
                    let (tx, _handle) = spawn_bench_actor(runtime);
                    let data = vec![0u8; 64];
                    let start = Instant::now();
                    runtime.block_on(async {
                        for iter in 0..iters {
                            // Each iter uses fresh shard names to avoid seqno chaining.
                            let mut receivers = Vec::with_capacity(batch_size);
                            for i in 0..batch_size {
                                let key = format!("s-{}-{}", iter, i);
                                receivers.push(send_cas(&tx, &key, None, 1, &data).await);
                            }
                            send_flush(&tx).await;
                            for rx in receivers {
                                rx.await.unwrap().unwrap();
                            }
                        }
                    });
                    start.elapsed()
                });
            },
        );
    }
}

/// Read latency under write pressure. Pre-populates 100 shards, then
/// interleaves CAS+flush batches (90%) with head reads (10%). Measures
/// only the head read time to verify it stays consistent.
pub fn bench_read_under_write_pressure(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("plumbing/read_under_write_pressure");

    for pending_writes in [0, 10, 100] {
        g.bench_with_input(
            BenchmarkId::new("pending_cas", pending_writes),
            &pending_writes,
            |b, &pending_writes| {
                let (tx, _handle) = spawn_bench_actor(runtime);
                // Pre-populate 100 shards with committed state.
                runtime.block_on(async {
                    for i in 0..100 {
                        cas_and_flush(&tx, &format!("shard-{}", i), None, 1, &[0u8; 64]).await;
                    }
                });
                let mut write_seqno = vec![1u64; 100];
                let mut write_idx = 0usize;
                b.iter(|| {
                    runtime.block_on(async {
                        // Enqueue pending_writes CAS ops WITHOUT flushing
                        // to build up pending state.
                        for _ in 0..pending_writes {
                            let idx = write_idx % 100;
                            write_idx += 1;
                            let expected = write_seqno[idx];
                            write_seqno[idx] += 1;
                            let _ = send_cas(
                                &tx,
                                &format!("shard-{}", idx),
                                Some(expected),
                                expected + 1,
                                &[0u8; 64],
                            )
                            .await;
                        }
                        // Now measure head read latency with pending state present.
                        send_head(&tx, "shard-0").await;
                        // Flush to clear pending state for next iteration.
                        send_flush(&tx).await;
                    });
                });
            },
        );
    }
}

/// Write latency under read pressure. Measures CAS+flush time while the
/// actor has recently serviced varying numbers of reads.
pub fn bench_write_under_read_pressure(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("plumbing/write_under_read_pressure");

    for reads_before_write in [0, 10, 100] {
        g.bench_with_input(
            BenchmarkId::new("reads_before_write", reads_before_write),
            &reads_before_write,
            |b, &reads_before_write| {
                let (tx, _handle) = spawn_bench_actor(runtime);
                // Pre-populate 100 shards.
                runtime.block_on(async {
                    for i in 0..100 {
                        cas_and_flush(&tx, &format!("shard-{}", i), None, 1, &[0u8; 64]).await;
                    }
                });
                let mut seqno = 1u64;
                b.iter(|| {
                    runtime.block_on(async {
                        // Service reads to warm the loop.
                        for i in 0..reads_before_write {
                            send_head(&tx, &format!("shard-{}", i % 100)).await;
                        }
                        // Measure the write.
                        seqno += 1;
                        cas_and_flush(&tx, "shard-0", Some(seqno - 1), seqno, &[0u8; 64]).await;
                    });
                });
            },
        );
    }
}

/// Production workload simulation: 90% CaS, 8% scan, 1% head, 1% truncate.
/// Flushes every 50 accumulated CAS ops.
pub fn bench_realistic_mix(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("plumbing/realistic_mix");

    g.bench_function("90cas_8scan_1head_1trunc", |b| {
        b.iter_custom(|iters| {
            let (tx, _handle) = spawn_bench_actor(runtime);
            let data = vec![0u8; 64];
            let num_shards = 100;
            let ops_per_iter = 100;

            let start = Instant::now();
            runtime.block_on(async {
                // Pre-populate shards.
                for i in 0..num_shards {
                    cas_and_flush(&tx, &format!("s-{}", i), None, 1, &data).await;
                }
                let mut shard_seqno = vec![1u64; num_shards];

                for _iter in 0..iters {
                    let mut pending_cas = Vec::new();
                    for op in 0..ops_per_iter {
                        let shard_idx = op % num_shards;
                        let key = format!("s-{}", shard_idx);

                        match op % 100 {
                            // 90% CAS (ops 0-89)
                            0..90 => {
                                let expected = shard_seqno[shard_idx];
                                shard_seqno[shard_idx] += 1;
                                pending_cas.push(
                                    send_cas(&tx, &key, Some(expected), expected + 1, &data).await,
                                );
                                // Flush every 50 CAS ops.
                                if pending_cas.len() >= 50 {
                                    send_flush(&tx).await;
                                    for rx in pending_cas.drain(..) {
                                        rx.await.unwrap().unwrap();
                                    }
                                }
                            }
                            // 8% scan (ops 90-97)
                            90..98 => {
                                send_scan(&tx, &key, 0, 10).await;
                            }
                            // 1% head (op 98)
                            98 => {
                                send_head(&tx, &key).await;
                            }
                            // 1% truncate (op 99)
                            _ => {
                                send_truncate_and_flush(&tx, &key, 1).await;
                            }
                        }
                    }
                    // Flush remaining.
                    if !pending_cas.is_empty() {
                        send_flush(&tx).await;
                        for rx in pending_cas.drain(..) {
                            rx.await.unwrap().unwrap();
                        }
                    }
                }
            });
            start.elapsed()
        });
    });
}

/// Upper bound stress test: 4 KiB values (persist inline write ceiling),
/// 100 shards. Tests for allocation/copy cliffs at realistic upper bounds.
pub fn bench_large_state(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("plumbing/large_state");

    // 4 KiB per value — the persist inline write ceiling.
    let large_data = vec![0u8; 4096];
    let num_shards = 100;
    let ops_per_iter = 100;

    g.bench_function("4kib_values_realistic_mix", |b| {
        b.iter_custom(|iters| {
            let (tx, _handle) = spawn_bench_actor(runtime);

            let start = Instant::now();
            runtime.block_on(async {
                // Pre-populate shards with 4 KiB entries.
                for i in 0..num_shards {
                    cas_and_flush(&tx, &format!("s-{}", i), None, 1, &large_data).await;
                }
                let mut shard_seqno = vec![1u64; num_shards];

                for _iter in 0..iters {
                    let mut pending_cas = Vec::new();
                    for op in 0..ops_per_iter {
                        let shard_idx = op % num_shards;
                        let key = format!("s-{}", shard_idx);

                        match op % 100 {
                            0..90 => {
                                let expected = shard_seqno[shard_idx];
                                shard_seqno[shard_idx] += 1;
                                pending_cas.push(
                                    send_cas(&tx, &key, Some(expected), expected + 1, &large_data)
                                        .await,
                                );
                                if pending_cas.len() >= 50 {
                                    send_flush(&tx).await;
                                    for rx in pending_cas.drain(..) {
                                        rx.await.unwrap().unwrap();
                                    }
                                }
                            }
                            90..98 => {
                                send_scan(&tx, &key, 0, 10).await;
                            }
                            98 => {
                                send_head(&tx, &key).await;
                            }
                            _ => {
                                send_truncate_and_flush(&tx, &key, 1).await;
                            }
                        }
                    }
                    if !pending_cas.is_empty() {
                        send_flush(&tx).await;
                        for rx in pending_cas.drain(..) {
                            rx.await.unwrap().unwrap();
                        }
                    }
                }
            });
            start.elapsed()
        });
    });
}

/// End-to-end actor loop with simulated S3 latency. The WAL writer sleeps to
/// simulate storage round-trip time. Compares zero, fixed-5ms, fixed-50ms,
/// and p50/p99 profiles to show how flush I/O affects overall throughput.
pub fn bench_latency_profiles(c: &mut Criterion, runtime: &Runtime) {
    let mut g = c.benchmark_group("plumbing/latency_profiles");

    let profiles = [
        ("zero", LatencyProfile::Zero),
        ("fixed_5ms", LatencyProfile::Fixed(Duration::from_millis(5))),
        (
            "fixed_50ms",
            LatencyProfile::Fixed(Duration::from_millis(50)),
        ),
        (
            "s3_p50_5ms_p99_50ms",
            LatencyProfile::P50P99 {
                p50: Duration::from_millis(5),
                p99: Duration::from_millis(50),
            },
        ),
    ];

    for (name, profile) in profiles {
        g.bench_function(BenchmarkId::new("group_commit_50ops", name), |b| {
            b.iter_custom(|iters| {
                let (tx, _handle) = spawn_latency_actor(runtime, profile.clone(), 5);
                let data = vec![0u8; 64];
                let batch_size = 50;

                let start = Instant::now();
                runtime.block_on(async {
                    for iter in 0..iters {
                        let mut receivers = Vec::with_capacity(batch_size);
                        for i in 0..batch_size {
                            let key = format!("s-{}-{}", iter, i);
                            receivers.push(send_cas(&tx, &key, None, 1, &data).await);
                        }
                        // Explicit flush (don't wait for timer).
                        send_flush(&tx).await;
                        for rx in receivers {
                            rx.await.unwrap().unwrap();
                        }
                    }
                });
                start.elapsed()
            });
        });
    }
}
