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

use criterion::{Bencher, BenchmarkId, Criterion, Throughput};
use differential_dataflow::trace::Description;
use timely::progress::Antichain;
use tokio::runtime::Runtime;
use uuid::Uuid;

use mz_ore::task::RuntimeExt;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::{Atomicity, BlobMulti, Consensus, ExternalError, SeqNo, VersionedData};
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
) {
    let mut g = c.benchmark_group(name);
    let blob_val = workload::flat_blob(&data);

    bench_all_consensus(&mut g, runtime, data, |b, consensus| {
        bench_consensus_compare_and_set_all_iters(
            &runtime,
            Arc::clone(&consensus),
            blob_val.clone(),
            b,
            concurrency,
        )
    });
}

fn bench_consensus_compare_and_set_all_iters(
    runtime: &Runtime,
    consensus: Arc<dyn Consensus + Send + Sync>,
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
    let payload = workload::flat_blob(&data);

    bench_all_blob(&mut g, runtime, data, |b, blob| {
        let deadline = Instant::now() + Duration::from_secs(1_000_000_000);
        let key = ShardId::new().to_string();
        runtime
            .block_on(blob.set(deadline, &key, payload.to_owned(), Atomicity::RequireAtomic))
            .expect("failed to set blob");
        b.iter(|| {
            runtime
                .block_on(bench_blob_get_one_iter(blob.as_ref(), &key))
                .expect("failed to run iter")
        })
    });
}

async fn bench_blob_get_one_iter(blob: &dyn BlobMulti, key: &str) -> Result<(), ExternalError> {
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
    let payload = workload::flat_blob(&data);

    bench_all_blob(&mut g, runtime, data, |b, blob| {
        b.iter(|| {
            runtime
                .block_on(bench_blob_set_one_iter(blob.as_ref(), &payload))
                .expect("failed to run iter")
        })
    });
}

async fn bench_blob_set_one_iter(
    blob: &dyn BlobMulti,
    payload: &[u8],
) -> Result<(), ExternalError> {
    let deadline = Instant::now() + Duration::from_secs(1_000_000_000);
    let key = ShardId::new().to_string();
    blob.set(deadline, &key, payload.to_owned(), Atomicity::RequireAtomic)
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
