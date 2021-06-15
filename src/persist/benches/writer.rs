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
use tempfile::NamedTempFile;

use persist::file::FileStream;
use persist::mem::MemStream;
use persist::persister::Write;

fn bench_write_sync<W: Write + Clone>(
    writer: &W,
    data: &[((String, String), u64, isize)],
    b: &mut Bencher,
) {
    b.iter_with_setup(
        || writer.clone(),
        |mut writer| writer.write_sync(&data).expect("failed to write data"),
    )
}

pub fn bench_writes(c: &mut Criterion) {
    let data = vec![
        (("key1".to_string(), "val1".to_string()), 1, 1),
        (("key2".to_string(), "val2".to_string()), 1, 1),
    ];

    let mem_stream = MemStream::new();
    c.bench_function("mem_write_sync", |b| {
        bench_write_sync(&mem_stream, &data, b)
    });

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let file_path = NamedTempFile::new_in(&temp_dir)
        .expect("failed to create temp file")
        .into_temp_path();
    let file_stream = FileStream::new(file_path).expect("failed to create file stream");
    c.bench_function("file_write_sync", |b| {
        bench_write_sync(&file_stream, &data, b)
    });
}

criterion_group!(benches, bench_writes);
criterion_main!(benches);
