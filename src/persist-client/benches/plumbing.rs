// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for different persistent Write implementations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{Bencher, BenchmarkId, Criterion, Throughput};
use differential_dataflow::trace::Description;
use futures::stream::{FuturesUnordered, StreamExt};
use timely::progress::Antichain;
use tokio::runtime::Runtime;
use uuid::Uuid;

use mz_ore::task::RuntimeExt;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Atomicity, Blob, Consensus, ExternalError, SeqNo, VersionedData};
use mz_persist::workload::{self, DataGenerator};
use mz_persist_client::ShardId;
use mz_persist_types::Codec;

use crate::{bench_all_blob, bench_all_consensus};

pub fn bench_consensus_compare_and_set(
    name: &str,
    c: &mut Criterion,
    runtime: &Runtime,
    data: &DataGenerator,
    concurrency: usize,
    num_shards: usize,
) {
    let mut g = c.benchmark_group(name);
    let payload = Bytes::from(workload::flat_blob(&data));

    bench_all_consensus(&mut g, runtime, data, |b, consensus| {
        bench_consensus_compare_and_set_all_iters(
            &runtime,
            Arc::clone(&consensus),
            &payload,
            b,
            concurrency,
            num_shards,
        )
    });
}

fn bench_consensus_compare_and_set_all_iters(
    runtime: &Runtime,
    consensus: Arc<dyn Consensus + Send + Sync>,
    data: &Bytes,
    b: &mut Bencher,
    concurrency: usize,
    num_shards: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(3600);
    b.iter_custom(|iters| {
        // We need to pick random keys because Criterion likes to run this
        // function many times as part of a warmup, and if we deterministically
        // use the same keys we will overwrite previously set values.
        let keys = (0..concurrency)
            .map(|idx| {
                (
                    idx,
                    (0..num_shards)
                        .map(|shard_idx| (shard_idx, Uuid::new_v4().to_string()))
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();

        let start = Instant::now();

        let mut handles = Vec::new();
        for (idx, shard_keys) in keys.into_iter() {
            let consensus = Arc::clone(&consensus);
            let data = Bytes::clone(&data);
            let handle = runtime.handle().spawn_named(
                || format!("bench_compare_and_set-{}", idx),
                async move {
                    // Keep track of the current SeqNo per shard ID.
                    let mut current: HashMap<usize, Option<SeqNo>> = HashMap::new();

                    for _iter in 0..iters {
                        let futs = FuturesUnordered::new();

                        for (idx, key) in shard_keys.iter() {
                            let current_seqno = current.entry(*idx).or_default().clone();
                            let next_seqno =
                                current_seqno.map_or_else(SeqNo::minimum, |x| x.next());

                            let fut = consensus.compare_and_set(
                                deadline,
                                key,
                                current_seqno,
                                VersionedData {
                                    seqno: next_seqno,
                                    data: Bytes::clone(&data),
                                },
                            );
                            futs.push(fut);

                            current.insert(*idx, Some(next_seqno));
                        }

                        let results = futs.collect::<Vec<_>>().await;

                        for result in results {
                            result
                                .expect("gave invalid inputs")
                                .expect("failed to compare_and_set");
                        }
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

pub fn bench_blob_get(
    name: &str,
    throughput: bool,
    c: &mut Criterion,
    runtime: &Runtime,
    data: &DataGenerator,
) {
    let mut g = c.benchmark_group(name);
    if throughput {
        g.throughput(Throughput::Bytes(data.goodput_bytes()));
    }
    let payload = Bytes::from(workload::flat_blob(&data));

    bench_all_blob(&mut g, runtime, data, |b, blob| {
        let deadline = Instant::now() + Duration::from_secs(1_000_000_000);
        let key = ShardId::new().to_string();
        runtime
            .block_on(blob.set(
                deadline,
                &key,
                Bytes::clone(&payload),
                Atomicity::RequireAtomic,
            ))
            .expect("failed to set blob");
        b.iter(|| {
            runtime
                .block_on(bench_blob_get_one_iter(blob.as_ref(), &key))
                .expect("failed to run iter")
        })
    });
}

async fn bench_blob_get_one_iter(blob: &dyn Blob, key: &str) -> Result<(), ExternalError> {
    let deadline = Instant::now() + Duration::from_secs(1_000_000_000);
    let value = blob.get(deadline, &key).await?;
    assert!(value.is_some());
    Ok(())
}

pub fn bench_blob_set(
    name: &str,
    throughput: bool,
    c: &mut Criterion,
    runtime: &Runtime,
    data: &DataGenerator,
) {
    let mut g = c.benchmark_group(name);
    if throughput {
        g.throughput(Throughput::Bytes(data.goodput_bytes()));
    }
    let payload = Bytes::from(workload::flat_blob(&data));

    bench_all_blob(&mut g, runtime, data, |b, blob| {
        b.iter(|| {
            runtime
                .block_on(bench_blob_set_one_iter(blob.as_ref(), &payload))
                .expect("failed to run iter")
        })
    });
}

async fn bench_blob_set_one_iter(blob: &dyn Blob, payload: &Bytes) -> Result<(), ExternalError> {
    let deadline = Instant::now() + Duration::from_secs(1_000_000_000);
    let key = ShardId::new().to_string();
    blob.set(
        deadline,
        &key,
        Bytes::clone(payload),
        Atomicity::RequireAtomic,
    )
    .await
}

pub fn bench_encode_batch(name: &str, throughput: bool, c: &mut Criterion, data: &DataGenerator) {
    let mut g = c.benchmark_group(name);
    if throughput {
        g.throughput(Throughput::Bytes(data.goodput_bytes()));
    }

    let trace = BlobTraceBatchPart {
        desc: Description::new(
            Antichain::from_elem(0),
            Antichain::new(),
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
