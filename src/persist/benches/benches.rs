// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{criterion_group, criterion_main, Criterion};
use persist::workload::DataGenerator;

mod end_to_end;
mod snapshot;
mod writer;

pub fn bench_persist(c: &mut Criterion) {
    let data = DataGenerator::default();

    end_to_end::bench_replay(&data, &mut c.benchmark_group("end_to_end/replay"));

    snapshot::bench_mem(&data, &mut c.benchmark_group("snapshot/mem"));
    snapshot::bench_file(&data, &mut c.benchmark_group("snapshot/file"));

    writer::bench_log(&mut c.benchmark_group("writer/log"));
    writer::bench_blob(&data, &mut c.benchmark_group("writer/blob"));
    writer::bench_encode_batch(&data, &mut c.benchmark_group("writer/encode_batch"));
    writer::bench_blob_cache_set_unsealed_batch(
        &data,
        &mut c.benchmark_group("writer/blob_cache_set_unsealed_batch"),
    );
    writer::bench_indexed_drain(&data, &mut c.benchmark_group("writer/indexed_drain"));
}

// The grouping here is an artifact of criterion's interaction with the
// plug-able rust benchmark harness. We use criterion's groups instead.
criterion_group! {
    benches,
    bench_persist,
}
criterion_main!(benches);
