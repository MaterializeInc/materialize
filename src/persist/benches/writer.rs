// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for different persistent Write implementations.

use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{
    criterion_group, criterion_main, Bencher, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use rand::Rng;
use tokio::runtime::Runtime;

use ore::cast::CastFrom;
use ore::metrics::MetricsRegistry;

use persist::error::Error;
use persist::file::{FileBlob, FileLog};
use persist::indexed::background::Maintainer;
use persist::indexed::cache::BlobCache;
use persist::indexed::encoding::{BlobUnsealedBatch, Id};
use persist::indexed::metrics::Metrics;
use persist::indexed::runtime::WriteReqBuilder;
use persist::indexed::Indexed;
use persist::mem::MemRegistry;
use persist::pfuture::{PFuture, PFutureHandle};
use persist::storage::{Atomicity, Blob, LockInfo, Log, SeqNo};

fn new_file_log(name: &str, parent: &Path) -> FileLog {
    let file_log_dir = parent.join(name);
    FileLog::new(file_log_dir, LockInfo::new_no_reentrance(name.to_owned()))
        .expect("creating a FileLog cannot fail")
}

fn new_file_blob(name: &str, parent: &Path) -> FileBlob {
    let file_blob_dir = parent.join(name);
    FileBlob::new(file_blob_dir, LockInfo::new_no_reentrance(name.to_owned()))
        .expect("creating a FileBlob cannot fail")
}

fn generate_updates() -> Vec<((Vec<u8>, Vec<u8>), u64, isize)> {
    let mut updates = vec![];
    // Ensure that each value has the same number of bytes to make reasoning about
    // throughput simpler
    for x in 1_000_000..2_000_000 {
        updates.push(((format!("{}", x).into(), "".into()), 1, 1));
    }

    updates
}

fn get_encoded_len(updates: Vec<((Vec<u8>, Vec<u8>), u64, isize)>) -> u64 {
    let mut len = 0;

    for ((key, val), _, _) in updates {
        len += key.len();
        len += val.len();
        len += size_of::<u64>() + size_of::<isize>();
    }

    u64::cast_from(len)
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

    let mut data = vec![];

    // Ensure that each value has the same number of bytes to make reasoning about
    // throughput simpler
    for i in 1_000_000..2_000_000 {
        let base = format!("{}", i).as_bytes().to_vec();
        data.extend_from_slice(&base);
    }

    let size = data.len() as u64;
    group.throughput(Throughput::Bytes(size));

    let mut mem_blob = MemRegistry::new()
        .blob_no_reentrance()
        .expect("creating a MemBlob cannot fail");
    group.bench_with_input(BenchmarkId::new("mem", size), &data, |b, data| {
        bench_set(&mut mem_blob, data.clone(), b)
    });

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let mut file_blob = new_file_blob("file_blob_set", temp_dir.path());
    group.bench_with_input(BenchmarkId::new("file", size), &data, |b, data| {
        bench_set(&mut file_blob, data.clone(), b)
    });
}

fn block_on_drain<T, F: FnOnce(&mut Indexed<L, B>, PFutureHandle<T>), L: Log, B: Blob>(
    index: &mut Indexed<L, B>,
    f: F,
) -> Result<T, Error> {
    let (tx, rx) = PFuture::new();
    f(index, tx.into());
    index.step()?;
    rx.recv()
}

fn block_on<T, F: FnOnce(PFutureHandle<T>)>(f: F) -> Result<T, Error> {
    let (tx, rx) = PFuture::new();
    f(tx.into());
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
    cache: &mut BlobCache<B>,
    batch: BlobUnsealedBatch,
    b: &mut Bencher,
) {
    // We need to pick random keys because Criterion likes to run this function
    // many times as part of a warmup, and if we deterministically use the same
    // keys we will overwrite.
    let mut rng = rand::thread_rng();

    b.iter_custom(|iters| {
        // Pre-allocate all of the data we will be writing so that we don't measure
        // allocation time.
        let mut data = Vec::with_capacity(iters as usize);
        for _ in 0..iters {
            data.push(batch.clone());
        }

        let start = Instant::now();
        for batch in data {
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
    let updates = generate_updates();
    let mut sorted_updates = updates.clone();
    sorted_updates.sort();

    let size = get_encoded_len(updates.clone());
    g.throughput(Throughput::Bytes(size));

    let id = block_on(|res| index.register("0", "()", "()", res))?;
    g.bench_with_input(
        BenchmarkId::new(&format!("{}_sorted", name), size),
        &sorted_updates,
        |b, data| {
            bench_write(&mut index, id, data.clone(), b);
        },
    );

    g.bench_with_input(
        BenchmarkId::new(&format!("{}_unsorted", name), size),
        &updates,
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

    let metrics = Metrics::register_with(&MetricsRegistry::new());
    let blob_cache = BlobCache::new(metrics.clone(), file_blob);
    let compacter = Maintainer::new(blob_cache.clone(), Arc::new(Runtime::new().unwrap()));
    let file_indexed = Indexed::new(file_log, blob_cache, compacter, metrics)
        .expect("failed to create file indexed");
    bench_writes_indexed_inner(file_indexed, "file", &mut group).expect("running benchmark failed");
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

    let mem_blob = MemRegistry::new()
        .blob_no_reentrance()
        .expect("creating a MemBlob cannot fail");
    let metrics = Metrics::register_with(&MetricsRegistry::new());
    let mut mem_blob_cache = BlobCache::new(metrics, mem_blob);

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let file_blob = new_file_blob("indexed_write_drain_blob", temp_dir.path());

    let metrics = Metrics::register_with(&MetricsRegistry::new());
    let mut file_blob_cache = BlobCache::new(metrics, file_blob);

    let updates = generate_updates();
    let size = get_encoded_len(updates.clone());
    group.throughput(Throughput::Bytes(size));
    let batch = BlobUnsealedBatch {
        desc: SeqNo(0)..SeqNo(1),
        updates,
    };

    group.bench_with_input(
        BenchmarkId::new("file_unsorted", size),
        &batch,
        |b, batch| {
            bench_set_unsealed_batch(&mut file_blob_cache, batch.clone(), b);
        },
    );

    group.bench_with_input(
        BenchmarkId::new("mem_unsorted", size),
        &batch,
        |b, batch| {
            bench_set_unsealed_batch(&mut mem_blob_cache, batch.clone(), b);
        },
    );
}
criterion_group!(
    benches,
    bench_writes_log,
    bench_writes_blob,
    bench_writes_blob_cache,
    bench_writes_indexed
);
criterion_main!(benches);
