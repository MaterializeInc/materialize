// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for reading from different parts of an [Indexed]

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};

use ore::metrics::MetricsRegistry;
use persist::error::Error;
use persist::file::{FileBlob, FileLog};
use persist::indexed::runtime::{self, RuntimeClient, StreamReadHandle};
use persist::mem::{MemBlob, MemLog};
use persist::storage::LockInfo;
use persist::Codec;

fn read_full_snapshot<K: Codec + Ord, V: Codec + Ord>(
    read: &StreamReadHandle<K, V>,
    expected_len: usize,
) -> Vec<((K, V), u64, isize)> {
    let buf = read
        .snapshot()
        .expect("reading snapshot cannot fail")
        .read_to_end_flattened()
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
    let data_len = 100_000;
    let data: Vec<((String, String), u64, isize)> = (0..data_len)
        .map(|i| {
            (
                (format!("key{}", i).into(), format!("val{}", i).into()),
                i as u64,
                1,
            )
        })
        .collect();

    let mut runtime = new_fn(1).expect("creating index cannot fail");
    let (write, read) = runtime.create_or_load("0").expect("registration succeeds");

    // Write the data out to the index's unsealed.
    write
        .write(data.iter())
        .recv()
        .expect("writing to index cannot fail");
    c.bench_function(&format!("{}_unsealed_snapshot", name), |b| {
        bench_snapshot(&read, data_len, b)
    });

    // After a seal and a step, it's all moved into the trace part of the index.
    write.seal(100_001).recv().expect("sealing update times");
    c.bench_function(&format!("{}_trace_snapshot", name), |b| {
        bench_snapshot(&read, data_len, b)
    });
    runtime.stop().expect("stopping runtime cannot fail");
}

pub fn bench_mem_snapshots(c: &mut Criterion) {
    bench_runtime_snapshots(c, "mem", |path| {
        let name = format!("snapshot_bench_{}", path);
        let lock_info = LockInfo::new_no_reentrance(name);
        runtime::start(
            MemLog::new(lock_info.clone()),
            MemBlob::new(lock_info),
            &MetricsRegistry::new(),
        )
    });
}

pub fn bench_file_snapshots(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    bench_runtime_snapshots(c, "file", move |path| {
        let log_dir = temp_dir.path().join(format!("snapshot_bench_log_{}", path));
        let blob_dir = temp_dir
            .path()
            .join(format!("snapshot_bench_blob_{}", path));
        let lock_info = LockInfo::new_no_reentrance("snapshot_bench".to_owned());
        runtime::start(
            FileLog::new(log_dir, lock_info.clone())?,
            FileBlob::new(blob_dir, lock_info)?,
            &MetricsRegistry::new(),
        )
    });
}

criterion_group!(benches, bench_mem_snapshots, bench_file_snapshots);
criterion_main!(benches);
