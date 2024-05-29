// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::NaiveTime;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mz_repr::adt::datetime::PackedNaiveTime;
use mz_repr::adt::interval::{Interval, PackedInterval};

fn bench_interval(c: &mut Criterion) {
    let mut group = c.benchmark_group("PackedInterval");
    group.throughput(Throughput::Elements(1));

    const INTERVAL: Interval = Interval::new(1, 1, 0);
    group.bench_function("encode", |b| {
        b.iter(|| {
            let packed = PackedInterval::from(std::hint::black_box(INTERVAL));
            std::hint::black_box(packed);
        })
    });

    const PACKED: [u8; 16] = [128, 0, 0, 1, 128, 0, 0, 1, 128, 0, 0, 0, 0, 0, 0, 0];
    group.bench_function("decode", |b| {
        let packed = PackedInterval::from_bytes(&PACKED).unwrap();
        b.iter(|| {
            let normal = Interval::from(std::hint::black_box(packed));
            std::hint::black_box(normal);
        })
    });

    group.finish();
}

fn bench_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("PackedNaiveTime");
    group.throughput(Throughput::Elements(1));

    group.bench_function("encode", |b| {
        let naive_time = NaiveTime::from_hms_opt(1, 1, 1).unwrap();
        b.iter(|| {
            let packed = PackedNaiveTime::from(std::hint::black_box(naive_time));
            std::hint::black_box(packed);
        })
    });

    const PACKED: [u8; 8] = [0, 0, 14, 77, 0, 0, 0, 0];
    group.bench_function("decode", |b| {
        let packed = PackedNaiveTime::from_bytes(&PACKED).unwrap();
        b.iter(|| {
            let normal = NaiveTime::from(std::hint::black_box(packed));
            std::hint::black_box(normal);
        })
    });

    group.finish()
}

criterion_group!(benches, bench_interval, bench_time);
criterion_main!(benches);
