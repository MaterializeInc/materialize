// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! End-to-end benchmarks that write to a persistent collection and see how fast
//! we can ingest it.

use std::path::{Path, PathBuf};
use std::time::Duration;

use criterion::measurement::Measurement;
use differential_dataflow::operators::Count;
use differential_dataflow::AsCollection;
use persist::error::Error;
use timely::dataflow::operators::Map;
use timely::dataflow::ProbeHandle;
use timely::Config;

use ore::metrics::MetricsRegistry;

use persist::file::{FileBlob, FileLog};
use persist::indexed::runtime::{self, RuntimeClient, RuntimeConfig, StreamWriteHandle};
use persist::operators::source::PersistedSource;
use persist::storage::LockInfo;

use criterion::{criterion_group, criterion_main, Bencher, BenchmarkGroup, Criterion, Throughput};
use rand::distributions::Alphanumeric;
use rand::Rng;

pub fn bench_end_to_end(c: &mut Criterion) {
    let data_size_mb = 100;
    let chunk_size_mb = 5;

    let mut group = c.benchmark_group("end_to_end");
    group.throughput(Throughput::Bytes((data_size_mb as u64) * 1024 * 1024));

    write_and_bench_read(&mut group, "4b_keys", data_size_mb, chunk_size_mb, 4, 0);
    write_and_bench_read(
        &mut group,
        "4kb_keys",
        data_size_mb,
        chunk_size_mb,
        4 * 1024,
        0,
    );
    write_and_bench_read(&mut group, "100b_keys", data_size_mb, chunk_size_mb, 100, 0);
}

fn write_and_bench_read<M: Measurement>(
    group: &mut BenchmarkGroup<M>,
    variant_name: &str,
    data_size_mb: usize,
    chunk_size_mb: usize,
    key_bytes: usize,
    value_bytes: usize,
) {
    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let nonce = variant_name.to_string();
    let collection_name = "4b_keys".to_string();
    let mut runtime = create_runtime(temp_dir.path(), &nonce).expect("missing runtime");

    let (mut write, _read) = runtime
        .create_or_load(&collection_name)
        .expect("could not create persistent collection");

    let expected_frontier = write_test_data(
        data_size_mb,
        chunk_size_mb,
        key_bytes,
        value_bytes,
        &mut write,
    )
    .expect("error writing data");

    runtime.stop().expect("runtime shut down cleanly");

    group.bench_function(format!("end_to_end_{}", variant_name), move |b| {
        bench_read_persisted_source(
            1,
            temp_dir.path().to_path_buf(),
            nonce.clone(),
            collection_name.clone(),
            expected_frontier,
            b,
        )
    });
}

fn bench_read_persisted_source<M: Measurement>(
    num_workers: usize,
    persistence_base_path: PathBuf,
    nonce: String,
    collection_id: String,
    expected_input_frontier: u64,
    b: &mut Bencher<M>,
) {
    b.iter(move || {
        let persistence_base_path = persistence_base_path.clone();
        let nonce = nonce.clone();
        let collection_id = collection_id.clone();

        let guards = timely::execute(Config::process(num_workers), move |worker| {
            let mut runtime =
                create_runtime(&persistence_base_path, &nonce).expect("missing runtime");

            let read = runtime
                .create_or_load::<Vec<u8>, Vec<u8>>(&collection_id)
                .map(|(_write, read)| read);

            let mut probe = ProbeHandle::new();

            worker.dataflow(|scope| {
                scope
                    .persisted_source(read)
                    .flat_map(move |(data, time, diff)| match data {
                        Err(_err) => None,
                        Ok((key, _value)) => Some({
                            let mut result = 0;
                            for a_byte in key.iter().take(4) {
                                result ^= a_byte;
                            }
                            ((result, ()), time, diff)
                        }),
                    })
                    .as_collection()
                    .filter(|(key, _value)| *key % 2 == 0)
                    .count()
                    .probe_with(&mut probe);
            });

            while probe.less_than(&expected_input_frontier) {
                worker.step();
            }

            runtime.stop().expect("runtime shut down cleanly")
        })
        .unwrap();

        for result in guards.join() {
            assert!(result.is_ok());
        }
    })
}

fn create_runtime(base_path: &Path, nonce: &str) -> Result<RuntimeClient, Error> {
    let log_dir = base_path.join(format!("log_{}", nonce));
    let blob_dir = base_path.join(format!("blob_{}", nonce));
    let lock_info = LockInfo::new(format!("reentrance_{}", nonce), "no_details".to_owned())?;
    let runtime = runtime::start(
        RuntimeConfig::default(),
        FileLog::new(log_dir, lock_info.clone())?,
        FileBlob::new(blob_dir, lock_info)?,
        &MetricsRegistry::new(),
        None,
    )?;
    Ok(runtime)
}

fn write_test_data(
    data_size_mb: usize,
    chunk_size_mb: usize,
    key_bytes: usize,
    value_bytes: usize,
    write: &mut StreamWriteHandle<Vec<u8>, Vec<u8>>,
) -> Result<u64, Error> {
    // +8 +8 for timestamp and diff, respectively
    let record_size_bytes = key_bytes + value_bytes + 8 + 8;
    let data_size_bytes = data_size_mb * 1024 * 1024;
    let chunk_size_bytes = chunk_size_mb * 1024 * 1024;
    let num_records = (data_size_bytes / record_size_bytes) as u64;

    // write data in batches

    let step_size = data_size_bytes / chunk_size_bytes;
    let mut last_time = 0;

    let mut futures = Vec::new();
    for time in (0..num_records).step_by(step_size) {
        let data: Vec<_> = (0..step_size)
            .map(|_i| {
                (
                    (generate_bytes(key_bytes), generate_bytes(value_bytes)),
                    time,
                    1,
                )
            })
            .collect();

        let result = write.write(&data);
        futures.push(result);
        last_time = time;
    }

    for future in futures {
        future.recv()?;
    }

    let seal_ts = last_time + 1;

    write.seal(seal_ts).recv()?;

    Ok(seal_ts)
}

/// Generates and returns bytes of length `len`.
pub fn generate_bytes(len: usize) -> Vec<u8> {
    // not super smart, BUT we're not benchmarking this.
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .collect()
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(60)).sample_size(10);
    targets = bench_end_to_end
}

criterion_main!(benches);
