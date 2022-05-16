// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{criterion_group, criterion_main, Criterion};
use mz_persist::workload::DataGenerator;

mod consensus;
mod end_to_end;
mod snapshot;
mod writer;

pub fn bench_persist(c: &mut Criterion) {
    // Override the default of "info" here because the s3 library is chatty on
    // info while initializing. It's good info to have in mz logs, but ends
    // being as spammy in these benchmarks.
    mz_ore::test::init_logging_default("warn");
    let data = DataGenerator::default();
    let small_data = DataGenerator::small();

    end_to_end::bench_load(&data, &mut c.benchmark_group("end_to_end/load"));
    end_to_end::bench_replay(&data, &mut c.benchmark_group("end_to_end/replay"));

    snapshot::bench_blob_get(&data, &mut c.benchmark_group("snapshot/blob_get"));
    snapshot::bench_mem(&data, &mut c.benchmark_group("snapshot/mem"));
    snapshot::bench_file(&data, &mut c.benchmark_group("snapshot/file"));

    writer::bench_log(&mut c.benchmark_group("writer/log"));
    writer::bench_blob_set(&data, &mut c.benchmark_group("writer/blob_set"));
    writer::bench_encode_batch(&data, &mut c.benchmark_group("writer/encode_batch"));
    writer::bench_blob_cache_set_unsealed_batch(
        &data,
        &mut c.benchmark_group("writer/blob_cache_set_unsealed_batch"),
    );
    writer::bench_indexed_drain(&data, &mut c.benchmark_group("writer/indexed_drain"));
    writer::bench_compact_trace(&data, &mut c.benchmark_group("writer/compact_trace"));

    consensus::bench_consensus_compare_and_set(
        &small_data,
        &mut c.benchmark_group("consensus/compare_and_set"),
        1,
    );
    consensus::bench_consensus_compare_and_set(
        &small_data,
        &mut c.benchmark_group("consensus/concurrent_compare_and_set"),
        8,
    );
}

// The grouping here is an artifact of criterion's interaction with the
// plug-able rust benchmark harness. We use criterion's groups instead.
criterion_group! {
    benches,
    bench_persist,
}
criterion_main!(benches);
