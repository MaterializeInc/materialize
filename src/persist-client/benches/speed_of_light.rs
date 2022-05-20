// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for different persistent Write implementations.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{Bencher, BenchmarkGroup, BenchmarkId, Throughput};
use differential_dataflow::trace::Description;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist_types::Codec;
use timely::progress::Antichain;
use tokio::runtime::Runtime;
use uuid::Uuid;

use mz_ore::cast::CastFrom;
use mz_ore::task::RuntimeExt;

use mz_persist::location::{Consensus, SeqNo, VersionedData};
use mz_persist::mem::MemConsensus;
use mz_persist::postgres::{PostgresConsensus, PostgresConsensusConfig};
use mz_persist::sqlite::SqliteConsensus;
use mz_persist::workload::{self, DataGenerator};

fn bench_compare_and_set<C: Consensus + Send + Sync + 'static>(
    runtime: &Runtime,
    consensus: Arc<C>,
    data: Vec<u8>,
    b: &mut Bencher,
    concurrency: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(3600);
    b.iter_custom(|iters| {
        // We need to pick random keys because Criterion likes to run this
        // function many times as part of a warmup, and if we deterministically
        // use the same keys we will overwrite previously set values.
        let keys = (0..concurrency)
            .map(|_| Uuid::new_v4().to_string())
            .collect::<Vec<_>>();

        let start = Instant::now();
        let mut handles = Vec::new();
        for (idx, key) in keys.into_iter().enumerate() {
            let consensus = Arc::clone(&consensus);
            let data = data.clone();
            let handle = runtime.handle().spawn_named(
                || format!("bench_compare_and_set-{}", idx),
                async move {
                    let mut current: Option<SeqNo> = None;
                    while current.map_or(0, |x| x.0) < iters {
                        let next = current.map_or_else(SeqNo::minimum, |x| x.next());
                        consensus
                            .compare_and_set(
                                deadline,
                                &key,
                                current,
                                VersionedData {
                                    seqno: next,
                                    data: data.clone(),
                                },
                            )
                            .await
                            .expect("gave invalid inputs")
                            .expect("failed to compare_and_set");
                        current = Some(next);
                    }
                },
            );
            handles.push(handle);
        }
        for handle in handles.into_iter() {
            runtime.block_on(handle).expect("task failed");
        }
        start.elapsed()
    });
}

pub fn bench_consensus_compare_and_set(
    data: &DataGenerator,
    g: &mut BenchmarkGroup<'_, WallTime>,
    concurrency: usize,
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(concurrency)
        .enable_all()
        .build()
        .expect("failed to create async runtime");
    let blob_val = workload::flat_blob(&data);
    g.throughput(Throughput::Bytes(
        data.goodput_bytes() * u64::cast_from(concurrency),
    ));

    let mem_consensus = Arc::new(MemConsensus::default());
    g.bench_with_input(
        BenchmarkId::new("mem", data.goodput_pretty()),
        &blob_val,
        |b, blob_val| {
            bench_compare_and_set(
                &runtime,
                Arc::clone(&mem_consensus),
                blob_val.clone(),
                b,
                concurrency,
            )
        },
    );

    // Create a directory that will automatically be dropped after the test finishes.
    {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let sqlite_consensus = runtime
            .block_on(SqliteConsensus::open(temp_dir.path().join("db")))
            .expect("creating a SqliteConsensus cannot fail");
        let sqlite_consensus = Arc::new(sqlite_consensus);
        g.bench_with_input(
            BenchmarkId::new("sqlite", data.goodput_pretty()),
            &blob_val,
            |b, blob_val| {
                bench_compare_and_set(
                    &runtime,
                    Arc::clone(&sqlite_consensus),
                    blob_val.clone(),
                    b,
                    concurrency,
                )
            },
        );
    }

    // Only run Postgres benchmarks if the magic env vars are set.
    if let Some(config) = runtime
        .block_on(PostgresConsensusConfig::new_for_test())
        .expect("failed to load postgres config")
    {
        let postgres_consensus = runtime
            .block_on(PostgresConsensus::open(config))
            .expect("failed to create PostgresConsensus");
        let postgres_consensus = Arc::new(postgres_consensus);
        g.bench_with_input(
            BenchmarkId::new("postgres", data.goodput_pretty()),
            &blob_val,
            |b, blob_val| {
                bench_compare_and_set(
                    &runtime,
                    Arc::clone(&postgres_consensus),
                    blob_val.clone(),
                    b,
                    concurrency,
                )
            },
        );
    }
}

pub fn bench_encode_batch(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    g.throughput(Throughput::Bytes(data.goodput_bytes()));
    let trace = BlobTraceBatchPart {
        desc: Description::new(
            Antichain::from_elem(0),
            Antichain::from_elem(1),
            Antichain::from_elem(0),
        ),
        index: 0,
        updates: data.batches().collect::<Vec<_>>(),
    };

    g.bench_function(BenchmarkId::new("trace", data.goodput_pretty()), |b| {
        b.iter(|| {
            // Intentionally alloc a new buf each iter.
            let mut buf = Vec::new();
            trace.encode(&mut buf);
        })
    });
}
