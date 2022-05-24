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
use criterion::{Bencher, BenchmarkGroup, BenchmarkId, Throughput};
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::indexed::columnar::ColumnarRecords;
use mz_persist::s3::{S3Blob, S3BlobConfig};
use rand::prelude::{SliceRandom, SmallRng};
use rand::{Rng, SeedableRng};
use timely::progress::Antichain;
use tokio::runtime::Runtime as AsyncRuntime;

use mz_persist::client::WriteReqBuilder;
use mz_persist::error::{Error, ErrorLog};
use mz_persist::file::FileBlob;
use mz_persist::gen::persist::ProtoBatchFormat;
use mz_persist::indexed::background::{CompactTraceReq, Maintainer};
use mz_persist::indexed::cache::BlobCache;
use mz_persist::indexed::encoding::{BlobTraceBatchPart, BlobUnsealedBatch, Id, TraceBatchMeta};
use mz_persist::indexed::metrics::Metrics;
use mz_persist::indexed::{Cmd, Indexed};
use mz_persist::location::{Atomicity, Blob, BlobRead, LockInfo, Log, SeqNo};
use mz_persist::mem::MemRegistry;
use mz_persist::pfuture::{PFuture, PFutureHandle};
use mz_persist::workload::{self, DataGenerator};

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

pub fn bench_log(g: &mut BenchmarkGroup<'_, WallTime>) {
    let data = "entry0".as_bytes().to_vec();

    let mut mem_log = MemRegistry::new()
        .log_no_reentrance()
        .expect("creating a MemLog cannot fail");
    g.bench_function("mem_sync", |b| {
        bench_write_sync(&mut mem_log, data.clone(), b)
    });
    mem_log.close().expect("failed to close mem_log");
}

pub fn bench_blob_set(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    // Limit the amount of time this test gets to run in order to limit the total
    // number of iterations criterion takes, and consequently, limit the peak
    // memory usage.
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(1));

    let blob_val = workload::flat_blob(&data);
    g.throughput(Throughput::Bytes(data.goodput_bytes()));

    let mut mem_blob = MemRegistry::new()
        .blob_no_reentrance()
        .expect("creating a MemBlob cannot fail");
    g.bench_with_input(
        BenchmarkId::new("mem", data.goodput_pretty()),
        &blob_val,
        |b, blob_val| bench_set(&mut mem_blob, blob_val.clone(), b),
    );
    futures_executor::block_on(mem_blob.close()).expect("failed to close mem_blob");

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let mut file_blob = new_file_blob("file_blob_set", temp_dir.path());
    {
        let async_runtime = Arc::new(AsyncRuntime::new().expect("failed to create async runtime"));
        let async_guard = async_runtime.enter();
        g.bench_with_input(
            BenchmarkId::new("file", data.goodput_pretty()),
            &blob_val,
            |b, blob_val| bench_set(&mut file_blob, blob_val.clone(), b),
        );
        drop(async_guard);
    }

    // Only run s3 benchmarks if the magic env vars are set.
    if let Some(config) =
        futures_executor::block_on(S3BlobConfig::new_for_test()).expect("failed to load s3 config")
    {
        let async_runtime = Arc::new(AsyncRuntime::new().expect("failed to create async runtime"));
        let async_guard = async_runtime.enter();
        let mut s3_blob =
            S3Blob::open_exclusive(config, LockInfo::new_no_reentrance("s3_blob_set".into()))
                .expect("failed to create S3Blob");
        g.bench_with_input(
            BenchmarkId::new("s3", data.goodput_pretty()),
            &blob_val,
            |b, blob_val| bench_set(&mut s3_blob, blob_val.clone(), b),
        );
        drop(async_guard);
    }
}

fn block_on_drain<T, F: FnOnce(&mut Indexed<L, B>, PFutureHandle<T>), L: Log, B: Blob>(
    index: &mut Indexed<L, B>,
    f: F,
) -> Result<T, Error> {
    let (tx, rx) = PFuture::new();
    f(index, tx);
    index.step_or_log();
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
    updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)>,
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
            let updates = WriteReqBuilder::from_iter(updates.iter())
                .finish()
                .into_iter()
                .map(|x| (id, x))
                .collect();
            block_on_drain(index, |i, handle| {
                i.apply(Cmd::Write(updates, handle));
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
                .set_unsealed_batch(
                    format!("{}", rng.gen::<usize>()),
                    batch,
                    ProtoBatchFormat::ParquetKvtd,
                )
                .expect("writing to blobcache failed");
        }
        start.elapsed()
    })
}

// Benchmark the write throughput of Maintainer::compact_trace_blocking.
fn bench_compact_trace_blocking<B: Blob>(
    b: &mut Bencher,
    maintainer: &Maintainer<B>,
    blob: &mut BlobCache<B>,
    data: &[ColumnarRecords],
) {
    let (batches0, batches1) = data.split_at(data.len() / 2);

    let mut b0_time = 0;
    let mut b0_keys = vec![];
    for batch in batches0.iter() {
        b0_time += u64::cast_from(batch.len());
    }
    let mut b1_time = b0_time;
    let mut b1_keys = vec![];
    for batch in batches1.iter() {
        b1_time += u64::cast_from(batch.len());
    }

    let b0_desc = desc_from(0, b0_time, 0);
    let b1_desc = desc_from(b0_time, b1_time, 0);
    let since = Antichain::from_elem(b1_time);
    let format = ProtoBatchFormat::ParquetKvtd;

    let mut b0_size_bytes = 0;
    for (idx, batch_part) in batches0.into_iter().enumerate() {
        let batch_part = BlobTraceBatchPart {
            desc: b0_desc.clone(),
            index: u64::cast_from(idx),
            updates: vec![batch_part.clone()],
        };
        let key = format!("b0-{}", idx);
        b0_size_bytes += blob
            .set_trace_batch(key.clone(), batch_part.clone(), format)
            .expect("failed to set trace batch");
        b0_keys.push(key);
    }
    let mut b1_size_bytes = 0;
    for (idx, batch_part) in batches1.into_iter().enumerate() {
        let batch_part = BlobTraceBatchPart {
            desc: b1_desc.clone(),
            index: u64::cast_from(idx),
            updates: vec![batch_part.clone()],
        };
        let key = format!("b1-{}", idx);
        b1_size_bytes += blob
            .set_trace_batch(key.clone(), batch_part.clone(), format)
            .expect("failed to set trace batch");
        b1_keys.push(key);
    }
    let req = CompactTraceReq {
        b0: TraceBatchMeta {
            keys: b0_keys,
            format,
            desc: b0_desc,
            level: 0,
            size_bytes: b0_size_bytes,
        },
        b1: TraceBatchMeta {
            keys: b1_keys,
            format,
            desc: b1_desc,
            level: 0,
            size_bytes: b1_size_bytes,
        },
        since,
    };
    b.iter(move || {
        maintainer
            .compact_trace(req.clone())
            .recv()
            .expect("compacting a trace failed");
    })
}

fn bench_writes_indexed_inner<B: Blob, L: Log>(
    data: &DataGenerator,
    g: &mut BenchmarkGroup<WallTime>,
    name: &str,
    mut index: Indexed<L, B>,
) -> Result<(), Error> {
    let mut sorted_updates = data.records().collect::<Vec<_>>();
    sorted_updates.sort();
    let mut unsorted_updates = sorted_updates.clone();
    unsorted_updates.shuffle(&mut SmallRng::seed_from_u64(0));

    g.throughput(Throughput::Bytes(data.goodput_bytes()));

    let id = block_on(|res| {
        index.apply(Cmd::Register("0".into(), ("()".into(), "()".into()), res));
    })?;
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

    index.close()
}

pub fn bench_indexed_drain(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    // Limit the sample size of this benchmark group to constrain it to a more
    // reasonable runtime.
    g.sample_size(10);
    let mem_indexed = MemRegistry::new()
        .indexed_no_reentrance()
        .expect("failed to create mem indexed");
    bench_writes_indexed_inner(data, g, "mem", mem_indexed).expect("running benchmark failed");

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let log = ErrorLog;
    let file_blob = new_file_blob("indexed_write_drain_blob", temp_dir.path());

    let async_runtime = Arc::new(AsyncRuntime::new().unwrap());
    let metrics = Arc::new(Metrics::register_with(&MetricsRegistry::new()));
    let blob_cache = BlobCache::new(
        mz_build_info::DUMMY_BUILD_INFO,
        Arc::clone(&metrics),
        async_runtime,
        file_blob,
        None,
    );
    let file_indexed =
        Indexed::new(log, blob_cache, metrics).expect("failed to create file indexed");
    bench_writes_indexed_inner(data, g, "file", file_indexed).expect("running benchmark failed");
}

pub fn bench_blob_cache_set_unsealed_batch(
    data: &DataGenerator,
    g: &mut BenchmarkGroup<'_, WallTime>,
) {
    // Limit the sample size and measurement time of this benchmark group to both
    // limit the overall runtime to a reasonable length and bound the memory
    // utilization.
    //
    // Criterion tries to fit as many iterations as possible within `measurement_time`,
    // but chooses some minimum number of iterations based on `sample_size`. So,
    // because we want to have a tight limit on the number of iterations, as each
    // incurs substantial memory usage for both file and mem blobs (because of how
    // caching is currently implemented), we have to manually specify both.
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(1));

    let async_runtime = Arc::new(AsyncRuntime::new().expect("failed to create runtime"));
    let mem_blob = MemRegistry::new()
        .blob_no_reentrance()
        .expect("creating a MemBlob cannot fail");
    let metrics = Arc::new(Metrics::register_with(&MetricsRegistry::new()));
    let mut mem_blob_cache = BlobCache::new(
        mz_build_info::DUMMY_BUILD_INFO,
        metrics,
        Arc::clone(&async_runtime),
        mem_blob,
        None,
    );

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let file_blob = new_file_blob("indexed_write_drain_blob", temp_dir.path());

    let metrics = Arc::new(Metrics::register_with(&MetricsRegistry::new()));
    let mut file_blob_cache = BlobCache::new(
        mz_build_info::DUMMY_BUILD_INFO,
        metrics,
        async_runtime,
        file_blob,
        None,
    );

    let batches = data.batches().collect::<Vec<_>>();
    g.throughput(Throughput::Bytes(data.goodput_bytes()));
    let desc = SeqNo(0)..SeqNo(1);

    g.bench_with_input(
        BenchmarkId::new("file_unsorted", data.goodput_pretty()),
        &(desc.clone(), batches.clone()),
        |b, (desc, batches)| {
            bench_set_unsealed_batch(b, &mut file_blob_cache, desc, batches);
        },
    );

    g.bench_with_input(
        BenchmarkId::new("mem_unsorted", data.goodput_pretty()),
        &(desc, batches),
        |b, (desc, batches)| {
            bench_set_unsealed_batch(b, &mut mem_blob_cache, desc, batches);
        },
    );
    mem_blob_cache.close().expect("failed to close mem_blob");
}

fn desc_from(lower: u64, upper: u64, since: u64) -> Description<u64> {
    Description::new(
        Antichain::from_elem(lower),
        Antichain::from_elem(upper),
        Antichain::from_elem(since),
    )
}
pub fn bench_compact_trace(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    // Limit the sample size and measurement time of this benchmark group to both
    // limit the overall runtime to a reasonable length and bound the memory
    // utilization.
    //
    // Criterion tries to fit as many iterations as possible within `measurement_time`,
    // but chooses some minimum number of iterations based on `sample_size`. So,
    // because we want to have a tight limit on the number of iterations, as each
    // incurs substantial memory usage for both file and mem blobs (because of how
    // caching is currently implemented), we have to manually specify both.
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(1));
    g.throughput(Throughput::Bytes(data.goodput_bytes()));
    let batches = data.batches().collect::<Vec<_>>();

    let async_runtime = Arc::new(AsyncRuntime::new().expect("failed to create runtime"));
    let metrics = Arc::new(Metrics::register_with(&MetricsRegistry::new()));

    let mem_blob = MemRegistry::new()
        .blob_no_reentrance()
        .expect("creating a MemBlob cannot fail");
    let mut blob_cache = BlobCache::new(
        mz_build_info::DUMMY_BUILD_INFO,
        Arc::clone(&metrics),
        Arc::clone(&async_runtime),
        mem_blob,
        None,
    );
    let maintainer = Maintainer::new(
        blob_cache.clone(),
        Arc::clone(&async_runtime),
        Arc::clone(&metrics),
    );

    g.bench_with_input(
        BenchmarkId::new("mem", data.goodput_pretty()),
        &batches,
        |b, batches| {
            bench_compact_trace_blocking(b, &maintainer, &mut blob_cache, batches);
        },
    );
    blob_cache.close().expect("failed to close mem_blob cache");

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let file_blob = new_file_blob("file_blob_compact_trace", temp_dir.path());
    let mut blob_cache = BlobCache::new(
        mz_build_info::DUMMY_BUILD_INFO,
        Arc::clone(&metrics),
        Arc::clone(&async_runtime),
        file_blob,
        None,
    );
    let maintainer = Maintainer::new(
        blob_cache.clone(),
        Arc::clone(&async_runtime),
        Arc::clone(&metrics),
    );
    g.bench_with_input(
        BenchmarkId::new("file", data.goodput_pretty()),
        &batches,
        |b, batches| {
            bench_compact_trace_blocking(b, &maintainer, &mut blob_cache, batches);
        },
    );
    blob_cache.close().expect("failed to close file_blob cache");

    // Only run s3 benchmarks if the magic env vars are set.
    if let Some(config) =
        futures_executor::block_on(S3BlobConfig::new_for_test()).expect("failed to load s3 config")
    {
        let async_guard = async_runtime.enter();
        let s3_blob =
            S3Blob::open_exclusive(config, LockInfo::new_no_reentrance("s3_blob_set".into()))
                .expect("failed to create S3Blob");
        let mut blob_cache = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::clone(&metrics),
            Arc::clone(&async_runtime),
            s3_blob,
            None,
        );
        let maintainer = Maintainer::new(blob_cache.clone(), Arc::clone(&async_runtime), metrics);
        g.bench_with_input(
            BenchmarkId::new("s3", data.goodput_pretty()),
            &batches,
            |b, batches| {
                bench_compact_trace_blocking(b, &maintainer, &mut blob_cache, batches);
            },
        );
        blob_cache.close().expect("failed to close s3_blob cache");
        drop(async_guard);
    }
}
