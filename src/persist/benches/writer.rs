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

use persist::file::FileBuffer;
use persist::mem::MemBuffer;
use persist::storage::{Buffer, BufferRead, BufferShared, BufferWrite};

fn bench_write_sync<U: Buffer>(writer: &mut U, data: Vec<u8>, b: &mut Bencher) {
    b.iter(move || {
        writer
            .write_sync(data.clone())
            .expect("failed to write data")
    })
}

fn bench_write_sync_shared<U: BufferWrite>(writer: &mut U, data: Vec<u8>, b: &mut Bencher) {
    b.iter(move || {
        writer
            .write_sync(data.clone())
            .expect("failed to write data")
    })
}

pub fn bench_writes(c: &mut Criterion) {
    let data = "entry0".as_bytes().to_vec();

    let mut mem_buffer = MemBuffer::new("mem_buffer_bench");
    c.bench_function("mem_write_sync", |b| {
        bench_write_sync(&mut mem_buffer, data.clone(), b)
    });

    let mem_buffer = MemBuffer::new("mem_buffer_shared_bench");
    let (mut reader, mut writer) = mem_buffer
        .read_write()
        .expect("creating shared handles failed");

    c.bench_function("mem_write_sync_shared", |b| {
        bench_write_sync_shared(&mut writer, data.clone(), b)
    });

    reader.close().expect("closing reader failed");
    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let file_buffer_dir = temp_dir.path().join("file_buffer_bench");
    let mut file_buffer = FileBuffer::new(file_buffer_dir, "file_buffer_bench")
        .expect("creating a FileBuffer cannot fail");
    c.bench_function("file_write_sync", |b| {
        bench_write_sync(&mut file_buffer, data.clone(), b)
    });

    let file_buffer_dir = temp_dir.path().join("file_buffer_shared_bench");
    let file_buffer = FileBuffer::new(file_buffer_dir, "file_buffer_shared_bench")
        .expect("creating a FileBuffer cannot fail");
    let (mut reader, mut writer) = file_buffer
        .read_write()
        .expect("creating shared handles failed");
    c.bench_function("file_write_sync_shared", |b| {
        bench_write_sync_shared(&mut writer, data.clone(), b)
    });
    reader.close().expect("closing reader failed");
}

criterion_group!(benches, bench_writes);
criterion_main!(benches);
