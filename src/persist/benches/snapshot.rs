// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for reading from different parts of an [Indexed]

use criterion::{
    black_box, criterion_group, criterion_main, Bencher, BenchmarkId, Criterion, Throughput,
};
use ore::metrics::MetricsRegistry;

use persist::client::{RuntimeClient, StreamReadHandle};
use persist::error::{Error, ErrorLog};
use persist::file::FileBlob;
use persist::indexed::Snapshot;
use persist::mem::MemRegistry;
use persist::runtime::{self, RuntimeConfig};
use persist::storage::{Blob, LockInfo};
use persist::workload::{self, DataGenerator};
use persist_types::Codec;

fn read_full_snapshot<K: Codec + Ord, V: Codec + Ord>(
    read: &StreamReadHandle<K, V>,
    expected_len: usize,
) -> Vec<((K, V), u64, isize)> {
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

fn bench_runtime_snapshots<F>(c: &mut Criterion, name: &str, mut new_fn: F)
where
    F: FnMut(usize) -> Result<RuntimeClient, Error>,
{
    let mut group = c.benchmark_group("snapshot");

    let mut runtime = new_fn(1).expect("creating index cannot fail");
    let (write, read) = runtime.create_or_load("0");
    let data = DataGenerator::default();

    // Write the data out to the index's unsealed.
    let goodput_bytes = workload::load(&write, &data, false).expect("writing to index cannot fail");
    group.throughput(Throughput::Bytes(goodput_bytes));
    group.bench_function(
        BenchmarkId::new(format!("{}_unsealed_snapshot", name), data.goodput_pretty()),
        |b| bench_snapshot(&read, data.record_count, b),
    );

    // After a seal and a step, it's all moved into the trace part of the index.
    write.seal(u64::MAX).recv().expect("sealing update times");
    group.bench_function(
        BenchmarkId::new(format!("{}_trace_snapshot", name), data.goodput_pretty()),
        |b| bench_snapshot(&read, data.record_count, b),
    );
    runtime.stop().expect("stopping runtime cannot fail");
}

pub fn bench_mem_snapshots(c: &mut Criterion) {
    bench_runtime_snapshots(c, "mem", |_path| MemRegistry::new().runtime_no_reentrance());
}

pub fn bench_file_snapshots(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    bench_runtime_snapshots(c, "file", move |path| {
        let blob_dir = temp_dir
            .path()
            .join(format!("snapshot_bench_blob_{}", path));
        let lock_info = LockInfo::new_no_reentrance("snapshot_bench".to_owned());
        runtime::start(
            RuntimeConfig::default(),
            ErrorLog,
            FileBlob::open_exclusive(blob_dir.into(), lock_info)?,
            build_info::DUMMY_BUILD_INFO,
            &MetricsRegistry::new(),
            None,
        )
    });
}

criterion_group!(benches, bench_mem_snapshots, bench_file_snapshots);
criterion_main!(benches);
