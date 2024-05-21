// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::{Read, Seek, SeekFrom};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;

fn bench_reader(c: &mut Criterion) {
    let mut reader_group = c.benchmark_group("reader");

    for num_segments in [1, 2, 4, 8, 16, 32, 64, 128] {
        reader_group.bench_with_input(
            BenchmarkId::new("seek", num_segments),
            &num_segments,
            |b, n| {
                let segments = vec![vec![42u8; 1000]; *n];
                let bytes: SegmentedBytes = segments.into_iter().map(Bytes::from).collect();
                let mut reader = bytes.reader();

                let half_way = u64::cast_from((1000 * n) / 2);

                b.iter(|| {
                    reader.seek(SeekFrom::Start(half_way)).unwrap();
                    std::hint::black_box(&mut reader);
                    reader.seek(SeekFrom::Start(0)).unwrap();
                });
            },
        );
    }

    for num_segments in [1, 2, 4, 8, 16, 32, 64, 128] {
        reader_group.bench_with_input(
            BenchmarkId::new("read", num_segments),
            &num_segments,
            |b, n| {
                let segments = vec![vec![42u8; 1000]; *n];
                let bytes: SegmentedBytes = segments.into_iter().map(Bytes::from).collect();
                let mut reader = bytes.reader();

                let mut buf = Vec::with_capacity(1000 * n);

                b.iter(|| {
                    reader.read_to_end(&mut buf).unwrap();
                    reader.seek(SeekFrom::Start(0)).unwrap();
                    std::hint::black_box((&mut reader, &mut buf));
                    buf.clear();
                });
            },
        );
    }

    for num_segments in [1, 2, 4, 8, 16, 32, 64, 128] {
        reader_group.bench_with_input(
            BenchmarkId::new("seek + read", num_segments),
            &num_segments,
            |b, n| {
                let segments = vec![vec![42u8; 1000]; *n];
                let bytes: SegmentedBytes = segments.into_iter().map(Bytes::from).collect();
                let mut reader = bytes.reader();

                let half_way = u64::cast_from((1000 * n) / 2);
                let mut buf = Vec::with_capacity(1000 * n);

                b.iter(|| {
                    reader.seek(SeekFrom::Start(half_way)).unwrap();
                    reader.read_to_end(&mut buf).unwrap();

                    std::hint::black_box((&mut reader, &mut buf));

                    reader.seek(SeekFrom::Start(0)).unwrap();
                    buf.clear();
                });
            },
        );
    }

    for num_segments in [1, 2, 4, 8, 16, 32, 64, 128] {
        reader_group.bench_with_input(
            BenchmarkId::new("create", num_segments),
            &num_segments,
            |b, n| {
                let segments = vec![vec![42u8; 1000]; *n];
                let bytes: SegmentedBytes = segments.into_iter().map(Bytes::from).collect();

                b.iter(|| {
                    let reader = bytes.clone().reader();
                    std::hint::black_box(reader);
                });
            },
        );
    }
}

criterion_group!(benches, bench_reader);
criterion_main!(benches);
