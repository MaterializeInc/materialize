// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for different persistent Write implementations.

use std::path::Path;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{Bencher, BenchmarkGroup, BenchmarkId, Throughput};
use rand::Rng;

use mz_persist::location::{Consensus, SeqNo, VersionedData};
use mz_persist::mem::MemConsensus;
use mz_persist::sqlite::SqliteConsensus;
use mz_persist::workload::{self, DataGenerator};

fn new_sqlite_consensus(path: &Path) -> SqliteConsensus {
    SqliteConsensus::open(&path.join("sqlite_consensus"))
        .expect("creating a SqliteConsensus cannot fail")
}

// Benchmark the write throughput of Consensus::compare_and_set.
fn bench_compare_and_set<C: Consensus>(consensus: &mut C, data: Vec<u8>, b: &mut Bencher) {
    let deadline = Instant::now() + Duration::from_secs(3600);

    // We need to pick random keys because Criterion likes to run this function
    // many times as part of a warmup, and if we deterministically use the same
    // keys we will overwrite previously set values.
    let mut rng = rand::thread_rng();
    let key = format!("{}", rng.gen::<usize>());
    let mut current = SeqNo(0);
    // Set an initial value in case that has different performance characteristics
    // for some Consensus implementations.
    futures_executor::block_on(consensus.compare_and_set(
        &key,
        deadline,
        None,
        VersionedData {
            seqno: current,
            data: data.clone(),
        },
    ))
    .expect("gave invalid inputs")
    .expect("failed to compare_and_set");

    b.iter(|| {
        let next = current.next();
        futures_executor::block_on(consensus.compare_and_set(
            &key,
            deadline,
            Some(current),
            VersionedData {
                seqno: next,
                data: data.clone(),
            },
        ))
        .expect("gave invalid inputs")
        .expect("failed to compare_and_set");
        current = next;
    })
}

pub fn bench_consensus_compare_and_set(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    let blob_val = workload::flat_blob(&data);
    g.throughput(Throughput::Bytes(data.goodput_bytes()));

    let mut mem_consensus = MemConsensus::default();
    g.bench_with_input(
        BenchmarkId::new("mem", data.goodput_pretty()),
        &blob_val,
        |b, blob_val| bench_compare_and_set(&mut mem_consensus, blob_val.clone(), b),
    );

    // Create a directory that will automatically be dropped after the test finishes.
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let mut sqlite_consensus = new_sqlite_consensus(&temp_dir.path());
    g.bench_with_input(
        BenchmarkId::new("sqlite", data.goodput_pretty()),
        &blob_val,
        |b, blob_val| bench_compare_and_set(&mut sqlite_consensus, blob_val.clone(), b),
    );
}
