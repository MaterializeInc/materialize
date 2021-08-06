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

use persist::error::Error;
use persist::file::{FileBlob, FileBuffer};
use persist::indexed::encoding::Id;
use persist::indexed::{Indexed, Snapshot};
use persist::mem::{MemBlob, MemBuffer};
use persist::storage::{Blob, Buffer, LockInfo};

fn read_full_snapshot<U: Buffer, L: Blob>(
    index: &Indexed<U, L>,
    id: Id,
    expected_len: usize,
) -> Vec<((Vec<u8>, Vec<u8>), u64, isize)> {
    let mut buf = Vec::with_capacity(expected_len);
    let mut snapshot = index.snapshot(id).expect("reading snapshot cannot fail");

    while snapshot.read(&mut buf) {}

    assert_eq!(buf.len(), expected_len);
    buf
}

fn bench_snapshot<U: Buffer, L: Blob>(
    index: &Indexed<U, L>,
    id: Id,
    expected_len: usize,
    b: &mut Bencher,
) {
    b.iter(move || black_box(read_full_snapshot(index, id, expected_len)))
}

fn bench_indexed_snapshots<U, L, F>(c: &mut Criterion, name: &str, mut new_fn: F)
where
    U: Buffer,
    L: Blob,
    F: FnMut(usize) -> Result<Indexed<U, L>, Error>,
{
    let data_len = 100_000;
    let data: Vec<_> = (0..data_len)
        .map(|i| {
            (
                (format!("key{}", i).into(), format!("val{}", i).into()),
                i as u64,
                1,
            )
        })
        .collect();

    let mut i = new_fn(1).expect("creating index cannot fail");
    let id = i.register("0", "", "").expect("registration succeeds");

    // Write the data out to the index's buffer.
    i.write_sync(vec![(id, data)])
        .expect("writing to index cannot fail");
    c.bench_function(&format!("{}_buffer_snapshot", name), |b| {
        bench_snapshot(&i, id, data_len, b)
    });

    // After a step, it's all moved into the future part of the index.
    i.step().expect("processing records in index cannot fail");
    c.bench_function(&format!("{}_future_snapshot", name), |b| {
        bench_snapshot(&i, id, data_len, b)
    });
    // Seal the updates to move them all to the trace
    i.seal(vec![id], 100_001).expect("sealing update times");
    c.bench_function(&format!("{}_trace_snapshot", name), |b| {
        bench_snapshot(&i, id, data_len, b)
    });
}

pub fn bench_mem_snapshots(c: &mut Criterion) {
    bench_indexed_snapshots(c, "mem", |path| {
        let name = format!("snapshot_bench_{}", path);
        let lock_info = LockInfo::new_no_reentrance(name);
        Indexed::new(MemBuffer::new(lock_info.clone()), MemBlob::new(lock_info))
    });
}

pub fn bench_file_snapshots(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    bench_indexed_snapshots(c, "file", move |path| {
        let buffer_dir = temp_dir
            .path()
            .join(format!("snapshot_bench_buffer_{}", path));
        let blob_dir = temp_dir
            .path()
            .join(format!("snapshot_bench_blob_{}", path));
        let lock_info = LockInfo::new_no_reentrance("snapshot_bench".to_owned());
        Indexed::new(
            FileBuffer::new(buffer_dir, lock_info.clone())?,
            FileBlob::new(blob_dir, lock_info)?,
        )
    });
}

criterion_group!(benches, bench_mem_snapshots, bench_file_snapshots);
criterion_main!(benches);
