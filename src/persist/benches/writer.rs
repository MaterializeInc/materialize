// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for different persistent Write implementations.

use criterion::{criterion_group, criterion_main, Bencher, Criterion};

use persist::error::Error;
use persist::file::FileLog;
use persist::indexed::encoding::Id;
use persist::indexed::Indexed;
use persist::mem::MemRegistry;
use persist::pfuture::{PFuture, PFutureHandle};
use persist::storage::{Blob, LockInfo, Log};

fn bench_write_sync<L: Log>(writer: &mut L, data: Vec<u8>, b: &mut Bencher) {
    b.iter(move || {
        writer
            .write_sync(data.clone())
            .expect("failed to write data")
    })
}

pub fn bench_writes_log(c: &mut Criterion) {
    let data = "entry0".as_bytes().to_vec();

    let mut mem_log = MemRegistry::new()
        .log_no_reentrance()
        .expect("creating a MemLog cannot fail");
    c.bench_function("mem_write_sync", |b| {
        bench_write_sync(&mut mem_log, data.clone(), b)
    });

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let file_log_dir = temp_dir.path().join("file_log_bench");
    let mut file_log = FileLog::new(
        file_log_dir,
        LockInfo::new_no_reentrance("file_log_bench".to_owned()),
    )
    .expect("creating a FileLog cannot fail");
    c.bench_function("file_write_sync", |b| {
        bench_write_sync(&mut file_log, data.clone(), b)
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

fn bench_write<L: Log, B: Blob>(
    index: &mut Indexed<L, B>,
    id: Id,
    updates: Vec<((Vec<u8>, Vec<u8>), u64, isize)>,
    b: &mut Bencher,
) {
    b.iter(move || {
        // We intentionally never call seal, so that the data only gets written
        // once to Unsealed, and not to Trace.
        block_on_drain(index, |i, handle| {
            i.write(vec![(id, updates.clone())], handle)
        })
        .unwrap();
    })
}

pub fn bench_writes_indexed(c: &mut Criterion) {
    let mut i = MemRegistry::new().indexed_no_reentrance().unwrap();
    let id = block_on(|res| i.register("0", "()", "()", res)).unwrap();

    let mut updates = vec![];
    for x in 0..1_000_000 {
        updates.push(((format!("{}", x).into(), "".into()), 1, 1));
    }

    c.bench_function("indexed_write_drain", |b| {
        bench_write(&mut i, id, updates.clone(), b);
    });

    updates.sort();
    c.bench_function("indexed_write_drain_sorted", |b| {
        bench_write(&mut i, id, updates.clone(), b);
    });
}

criterion_group!(benches, bench_writes_log, bench_writes_indexed);
criterion_main!(benches);
