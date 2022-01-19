// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for different persistent Write implementations.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{
    criterion_group, criterion_main, Bencher, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use differential_dataflow::trace::Description;
use ore::cast::CastFrom;
use ore::metrics::MetricsRegistry;
use persist::indexed::columnar::ColumnarRecords;
use persist_types::Codec;
use rand::prelude::{SliceRandom, SmallRng};
use rand::{Rng, SeedableRng};
use timely::progress::Antichain;
use tokio::runtime::Runtime as AsyncRuntime;

use persist::client::WriteReqBuilder;
use persist::error::Error;
use persist::file::{FileBlob, FileLog};
use persist::indexed::background::Maintainer;
use persist::indexed::cache::BlobCache;
use persist::indexed::encoding::{BlobTraceBatch, BlobUnsealedBatch, Id};
use persist::indexed::metrics::Metrics;
use persist::indexed::Indexed;
use persist::mem::MemRegistry;
use persist::pfuture::{PFuture, PFutureHandle};
use persist::storage::{Atomicity, Blob, LockInfo, Log, SeqNo};
use persist::workload::DataGenerator;

fn new_file_log(name: &str, parent: &Path) -> FileLog {
    let file_log_dir = parent.join(name);
    FileLog::new(file_log_dir, LockInfo::new_no_reentrance(name.to_owned()))
        .expect("creating a FileLog cannot fail")
}

fn new_file_blob(name: &str, parent: &Path) -> FileBlob {
    let file_blob_dir = parent.join(name);
    FileBlob::open_exclusive(
        file_blob_dir.into(),
        LockInfo::new_no_reentrance(name.to_owned()),
    )
    .expect("creating a FileBlob cannot fail")
}

// Benchmark the write throughput of Log::write_sync.
fn bench_write_sync<L: Log>(writer: &mut L, data: Vec<u8>, b: &mut Bencher) {
    b.iter(move || {
        writer
            .write_sync(data.clone())
            .expect("failed to write data")
    })
}

// Benchmark the write throughput of Blob::set.
fn bench_set<B: Blob>(writer: &mut B, data: Vec<u8>, b: &mut Bencher) {
    // We need to pick random keys because Criterion likes to run this function
    // many times as part of a warmup, and if we deterministically use the same
    // keys we will overwrite.
    let mut rng = rand::thread_rng();
    b.iter(|| {
        futures_executor::block_on(writer.set(
            &format!("{}", rng.gen::<usize>()),
            data.clone(),
            Atomicity::AllowNonAtomic,
        ))
        .expect("failed to write data");
    })
}

pub fn bench_writes_log(c: &mut Criterion) {
    let data = "entry0".as_bytes().to_vec();

    let mut mem_log = MemRegistry::new()
        .log_no_reentrance()
        .expect("creating a MemLog cannot fail");
    c.bench_function("mem_log_write_sync", |b| {
        bench_write_sync(&mut mem_log, data.clone(), b)
    });

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let mut file_log = new_file_log("file_log_write_sync", temp_dir.path());
    c.bench_function("file_log_write_sync", |b| {
        bench_write_sync(&mut file_log, data.clone(), b)
    });
}

pub fn bench_writes_blob(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_set");

    // Limit the amount of time this test gets to run in order to limit the total
    // number of iterations criterion takes, and consequently, limit the peak
    // memory usage.
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(1));

    let mut blob_val = vec![];
    let data = DataGenerator::default();
    for batch in data.batches() {
        for ((k, v), t, d) in batch.iter() {
            blob_val.extend_from_slice(k);
            blob_val.extend_from_slice(v);
            blob_val.extend_from_slice(&t.to_le_bytes());
            blob_val.extend_from_slice(&d.to_le_bytes());
        }
    }
    assert_eq!(data.goodput_bytes(), u64::cast_from(blob_val.len()));
    group.throughput(Throughput::Bytes(data.goodput_bytes()));

    let mut mem_blob = MemRegistry::new()
        .blob_no_reentrance()
        .expect("creating a MemBlob cannot fail");
    group.bench_with_input(
        BenchmarkId::new("mem", data.goodput_pretty()),
        &blob_val,
        |b, blob_val| bench_set(&mut mem_blob, blob_val.clone(), b),
    );

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let mut file_blob = new_file_blob("file_blob_set", temp_dir.path());
    group.bench_with_input(
        BenchmarkId::new("file", data.goodput_pretty()),
        &blob_val,
        |b, blob_val| bench_set(&mut file_blob, blob_val.clone(), b),
    );
}

fn block_on_drain<T, F: FnOnce(&mut Indexed<L, B>, PFutureHandle<T>), L: Log, B: Blob>(
    index: &mut Indexed<L, B>,
    f: F,
) -> Result<T, Error> {
    let (tx, rx) = PFuture::new();
    f(index, tx);
    index.step()?;
    rx.recv()
}

fn block_on<T, F: FnOnce(PFutureHandle<T>)>(f: F) -> Result<T, Error> {
    let (tx, rx) = PFuture::new();
    f(tx);
    rx.recv()
}

// Benchmark the write throughput of Indexed::write.
fn bench_write<L: Log, B: Blob>(
    index: &mut Indexed<L, B>,
    id: Id,
    updates: Vec<((Vec<u8>, Vec<u8>), u64, isize)>,
    b: &mut Bencher,
) {
    b.iter_custom(|iters| {
        // Pre-allocate all of the data we will be writing so that we don't measure
        // allocation time.
        let mut data = Vec::with_capacity(iters as usize);
        for _ in 0..iters {
            data.push(updates.clone());
        }

        let start = Instant::now();
        for updates in data {
            // We intentionally never call seal, so that the data only gets written
            // once to Unsealed, and not to Trace.
            let mut updates = WriteReqBuilder::from_iter(updates.iter());
            block_on_drain(index, |i, handle| {
                i.write(vec![(id, updates.finish())], handle)
            })
            .unwrap();
        }
        start.elapsed()
    })
}

// Benchmark the write throughput of BlobCache::set_unsealed_batch.
fn bench_set_unsealed_batch<B: Blob>(
    b: &mut Bencher,
    cache: &mut BlobCache<B>,
    desc: &Range<SeqNo>,
    batches: &Vec<ColumnarRecords>,
) {
    // We need to pick random keys because Criterion likes to run this function
    // many times as part of a warmup, and if we deterministically use the same
    // keys we will overwrite.
    let mut rng = rand::thread_rng();

    b.iter_custom(|iters| {
        // Pre-allocate all of the data we will be writing so that we don't measure
        // allocation time.
        let bench_data = (0..iters).map(|_| batches.clone()).collect::<Vec<_>>();

        let start = Instant::now();
        for updates in bench_data {
            let batch = BlobUnsealedBatch {
                desc: desc.clone(),
                updates,
            };
            cache
                .set_unsealed_batch(format!("{}", rng.gen::<usize>()), batch)
                .expect("writing to blobcache failed");
        }
        start.elapsed()
    })
}

fn bench_writes_indexed_inner<B: Blob, L: Log>(
    mut index: Indexed<L, B>,
    name: &str,
    g: &mut BenchmarkGroup<WallTime>,
) -> Result<(), Error> {
    let data = DataGenerator::default();
    let mut sorted_updates = data.records().collect::<Vec<_>>();
    sorted_updates.sort();
    let mut unsorted_updates = sorted_updates.clone();
    unsorted_updates.shuffle(&mut SmallRng::seed_from_u64(0));

    g.throughput(Throughput::Bytes(data.goodput_bytes()));

    let id = block_on(|res| index.register("0", "()", "()", res))?;
    g.bench_with_input(
        BenchmarkId::new(&format!("{}_sorted", name), data.goodput_pretty()),
        &sorted_updates,
        |b, data| {
            bench_write(&mut index, id, data.clone(), b);
        },
    );

    g.bench_with_input(
        BenchmarkId::new(&format!("{}_unsorted", name), data.goodput_pretty()),
        &unsorted_updates,
        |b, data| {
            bench_write(&mut index, id, data.clone(), b);
        },
    );

    Ok(())
}

pub fn bench_writes_indexed(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexed_write_drain");

    // Limit the sample size of this benchmark group to constrain it to a more
    // reasonable runtime.
    group.sample_size(10);
    let mem_indexed = MemRegistry::new()
        .indexed_no_reentrance()
        .expect("failed to create mem indexed");
    bench_writes_indexed_inner(mem_indexed, "mem", &mut group).expect("running benchmark failed");

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let file_log = new_file_log("indexed_write_drain_log", temp_dir.path());
    let file_blob = new_file_blob("indexed_write_drain_blob", temp_dir.path());

    let async_runtime = Arc::new(AsyncRuntime::new().unwrap());
    let metrics = Arc::new(Metrics::register_with(&MetricsRegistry::new()));
    let blob_cache = BlobCache::new(
        build_info::DUMMY_BUILD_INFO,
        metrics.clone(),
        async_runtime.clone(),
        file_blob,
    );
    let compacter = Maintainer::new(blob_cache.clone(), async_runtime);
    let file_indexed = Indexed::new(file_log, blob_cache, compacter, metrics)
        .expect("failed to create file indexed");
    bench_writes_indexed_inner(file_indexed, "file", &mut group).expect("running benchmark failed");
}

pub fn bench_encode_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_batch");

    let data = DataGenerator::default();
    group.throughput(Throughput::Bytes(data.goodput_bytes()));
    let unsealed = BlobUnsealedBatch {
        desc: SeqNo(0)..SeqNo(1),
        updates: data.batches().collect::<Vec<_>>(),
    };
    let trace = BlobTraceBatch {
        desc: Description::new(
            Antichain::from_elem(0),
            Antichain::from_elem(1),
            Antichain::from_elem(0),
        ),
        updates: data.records().collect::<Vec<_>>(),
    };

    group.bench_function(BenchmarkId::new("unsealed", data.goodput_pretty()), |b| {
        b.iter(|| {
            // Intentionally alloc a new buf each iter.
            let mut buf = Vec::new();
            unsealed.encode(&mut buf);
        })
    });

    group.bench_function(BenchmarkId::new("trace", data.goodput_pretty()), |b| {
        b.iter(|| {
            // Intentionally alloc a new buf each iter.
            let mut buf = Vec::new();
            trace.encode(&mut buf);
        })
    });
}

pub fn bench_writes_blob_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_cache_set_unsealed_batch");

    // Limit the sample size and measurement time of this benchmark group to both
    // limit the overall runtime to a reasonable length and bound the memory
    // utilization.
    //
    // Criterion tries to fit as many iterations as possible within `measurement_time`,
    // but chooses some minimum number of iterations based on `sample_size`. So,
    // because we want to have a tight limit on the number of iterations, as each
    // incurs substantial memory usage for both file and mem blobs (because of how
    // caching is currently implemented), we have to manually specify both.
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(1));

    let async_runtime = Arc::new(AsyncRuntime::new().expect("failed to create runtime"));
    let mem_blob = MemRegistry::new()
        .blob_no_reentrance()
        .expect("creating a MemBlob cannot fail");
    let metrics = Arc::new(Metrics::register_with(&MetricsRegistry::new()));
    let mut mem_blob_cache = BlobCache::new(
        build_info::DUMMY_BUILD_INFO,
        metrics,
        async_runtime.clone(),
        mem_blob,
    );

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let file_blob = new_file_blob("indexed_write_drain_blob", temp_dir.path());

    let metrics = Arc::new(Metrics::register_with(&MetricsRegistry::new()));
    let mut file_blob_cache = BlobCache::new(
        build_info::DUMMY_BUILD_INFO,
        metrics,
        async_runtime,
        file_blob,
    );

    let data = DataGenerator::default();
    let batches = data.batches().collect::<Vec<_>>();
    group.throughput(Throughput::Bytes(data.goodput_bytes()));
    let desc = SeqNo(0)..SeqNo(1);

    group.bench_with_input(
        BenchmarkId::new("file_unsorted", data.goodput_pretty()),
        &(desc.clone(), batches.clone()),
        |b, (desc, batches)| {
            bench_set_unsealed_batch(b, &mut file_blob_cache, desc, batches);
        },
    );

    group.bench_with_input(
        BenchmarkId::new("mem_unsorted", data.goodput_pretty()),
        &(desc, batches),
        |b, (desc, batches)| {
            bench_set_unsealed_batch(b, &mut mem_blob_cache, desc, batches);
        },
    );
}

criterion_group!(
    benches,
    bench_writes_log,
    bench_writes_blob,
    bench_encode_batch,
    bench_writes_blob_cache,
    bench_writes_indexed
);
criterion_main!(benches);
