// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for reading from different parts of an [Indexed]

use std::sync::Arc;

use criterion::measurement::WallTime;
use criterion::{black_box, Bencher, BenchmarkGroup, BenchmarkId, Throughput};
use tokio::runtime::Runtime as AsyncRuntime;

use mz_ore::metrics::MetricsRegistry;

use mz_persist::client::{RuntimeClient, StreamReadHandle};
use mz_persist::error::{Error, ErrorLog};
use mz_persist::file::FileBlob;
use mz_persist::mem::MemRegistry;
use mz_persist::runtime::{self, RuntimeConfig};
use mz_persist::s3::{S3Blob, S3BlobConfig};
use mz_persist::storage::{Atomicity, Blob, BlobRead, LockInfo};
use mz_persist::workload::{self, DataGenerator};
use mz_persist_types::Codec;
use uuid::Uuid;

pub fn bench_blob_get(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    g.throughput(Throughput::Bytes(data.goodput_bytes()));

    let key = Uuid::new_v4().to_string();
    let blob_val = workload::flat_blob(&data);

    {
        let mut mem_blob = MemRegistry::new()
            .blob_no_reentrance()
            .expect("creating a MemBlob cannot fail");

        futures_executor::block_on(mem_blob.set(&key, blob_val.clone(), Atomicity::AllowNonAtomic))
            .expect("failed to write data");
        g.bench_function(BenchmarkId::new("mem", data.goodput_pretty()), |b| {
            b.iter(|| {
                let actual =
                    bench_blob_get_one_iter(&mem_blob, &key).expect("failed to run blob_get iter");
                assert_eq!(actual.len(), blob_val.len());
            });
        });
        futures_executor::block_on(mem_blob.close()).expect("failed to close mem_blob");
    }

    // Only run s3 benchmarks if the magic env vars are set.
    if let Some(config) =
        futures_executor::block_on(S3BlobConfig::new_for_test()).expect("failed to load s3 config")
    {
        let async_runtime = Arc::new(AsyncRuntime::new().expect("failed to create async runtime"));
        let async_guard = async_runtime.enter();
        let mut s3_blob =
            S3Blob::open_exclusive(config, LockInfo::new_no_reentrance("blob_get_s3".into()))
                .expect("failed to create S3Blob");

        futures_executor::block_on(s3_blob.set(&key, blob_val.clone(), Atomicity::AllowNonAtomic))
            .expect("failed to write data");
        g.bench_function(BenchmarkId::new("s3", data.goodput_pretty()), |b| {
            b.iter(|| {
                let actual =
                    bench_blob_get_one_iter(&s3_blob, &key).expect("failed to run blob_get iter");
                assert_eq!(actual.len(), blob_val.len());
            });
        });
        futures_executor::block_on(s3_blob.close()).expect("failed to close s3_blob");
        drop(async_guard);
    }
}

fn bench_blob_get_one_iter<B: BlobRead>(blob: &B, key: &str) -> Result<Vec<u8>, Error> {
    futures_executor::block_on(blob.get(key))?
        .ok_or_else(|| Error::from(format!("missing blob at key: {}", key)))
}

fn read_full_snapshot<K: Codec + Ord, V: Codec + Ord>(
    read: &StreamReadHandle<K, V>,
    expected_len: usize,
) -> Vec<((K, V), u64, i64)> {
    let buf = read
        .snapshot()
        .expect("reading snapshot cannot fail")
        .into_iter()
        .collect::<Result<Vec<_>, Error>>()
        .expect("fully reading snapshot cannot fail");

    assert_eq!(buf.len(), expected_len);
    buf
}

fn bench_snapshot<K: Codec + Ord, V: Codec + Ord>(
    read: &StreamReadHandle<K, V>,
    expected_len: usize,
    b: &mut Bencher,
) {
    b.iter(move || black_box(read_full_snapshot(read, expected_len)))
}

fn bench_runtime_snapshots<F>(
    data: &DataGenerator,
    g: &mut BenchmarkGroup<'_, WallTime>,
    mut new_fn: F,
) where
    F: FnMut(usize) -> Result<RuntimeClient, Error>,
{
    let mut runtime = new_fn(1).expect("creating index cannot fail");
    let (write, read) = runtime.create_or_load("0");

    // Write the data out to the index's unsealed.
    let goodput_bytes = workload::load(&write, &data, false).expect("writing to index cannot fail");
    g.throughput(Throughput::Bytes(goodput_bytes));
    g.bench_function(BenchmarkId::new("unsealed", data.goodput_pretty()), |b| {
        bench_snapshot(&read, data.record_count, b)
    });

    // After a seal and a step, it's all moved into the trace part of the index.
    write.seal(u64::MAX).recv().expect("sealing update times");
    g.bench_function(BenchmarkId::new("trace", data.goodput_pretty()), |b| {
        bench_snapshot(&read, data.record_count, b)
    });
    runtime.stop().expect("stopping runtime cannot fail");
}

pub fn bench_mem(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    bench_runtime_snapshots(data, g, |_path| MemRegistry::new().runtime_no_reentrance());
}

pub fn bench_file(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    bench_runtime_snapshots(data, g, move |path| {
        let blob_dir = temp_dir
            .path()
            .join(format!("snapshot_bench_blob_{}", path));
        let lock_info = LockInfo::new_no_reentrance("snapshot_bench".to_owned());
        runtime::start(
            RuntimeConfig::default(),
            ErrorLog,
            FileBlob::open_exclusive(blob_dir.into(), lock_info)?,
            mz_build_info::DUMMY_BUILD_INFO,
            &MetricsRegistry::new(),
            None,
        )
    });
}
