// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use dec::Context;
use mz_persist_types::columnar::FixedSizeCodec;
use mz_repr::adt::numeric::{Numeric, PackedNumeric};

fn bench_numeric(c: &mut Criterion) {
    let mut group = c.benchmark_group("Numeric");
    group.throughput(Throughput::Elements(1));

    let val = Numeric::from(-101);

    group.bench_function("decimal32", |b| {
        b.iter(|| {
            let dec32 = std::hint::black_box(val).to_decimal32();
            std::hint::black_box(dec32);
        })
    });
    group.bench_function("float32", |b| {
        let mut context = Context::default();
        b.iter(|| {
            let float32 = context.try_into_f32(std::hint::black_box(val)).unwrap();
            std::hint::black_box(float32);
        })
    });
    group.bench_function("decimal64", |b| {
        b.iter(|| {
            let dec64 = std::hint::black_box(val).to_decimal64();
            std::hint::black_box(dec64);
        })
    });
    group.bench_function("float64", |b| {
        let mut context = Context::default();
        b.iter(|| {
            let float64 = context.try_into_f64(std::hint::black_box(val)).unwrap();
            std::hint::black_box(float64);
        })
    });
    group.bench_function("bcd", |b| {
        b.iter(|| {
            let bcd = std::hint::black_box(val).to_packed_bcd();
            std::hint::black_box(bcd);
        });
    });
    group.bench_function("packed", |b| {
        b.iter(|| {
            let packed = PackedNumeric::from_value(std::hint::black_box(val));
            std::hint::black_box(packed)
        });
    });
}

criterion_group!(benches, bench_numeric);
criterion_main!(benches);
