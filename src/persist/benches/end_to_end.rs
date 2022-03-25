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
use std::sync::Arc;

use criterion::measurement::{Measurement, WallTime};
use criterion::{Bencher, BenchmarkGroup, BenchmarkId, Throughput};
use differential_dataflow::operators::Count;
use differential_dataflow::AsCollection;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::s3::{S3Blob, S3BlobConfig};
use timely::dataflow::operators::Map;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;
use timely::Config;
use tokio::runtime::Runtime as AsyncRuntime;

use mz_persist::client::RuntimeClient;
use mz_persist::error::{Error, ErrorLog};
use mz_persist::file::FileBlob;
use mz_persist::location::{Blob, LockInfo};
use mz_persist::operators::source::PersistedSource;
use mz_persist::runtime::{self, RuntimeConfig};
use mz_persist::workload::{self, DataGenerator};

pub fn bench_load(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    let config =
        futures_executor::block_on(S3BlobConfig::new_for_test()).expect("failed to load s3 config");
    let config = match config {
        Some(config) => config,
        None => return,
    };

    g.throughput(Throughput::Bytes(data.goodput_bytes()));
    g.bench_function(BenchmarkId::new("s3", data.goodput_pretty()), move |b| {
        b.iter(|| bench_load_s3_one_iter(&data, &config).expect("failed to run load iter"))
    });
}

// NB: This makes sure to start a new runtime for each iter to ensure the work
// done in each is as equal as possible.
fn bench_load_s3_one_iter(data: &DataGenerator, config: &S3BlobConfig) -> Result<(), Error> {
    let async_runtime = Arc::new(AsyncRuntime::new()?);

    let config = config.clone_with_new_uuid_prefix();
    let async_guard = async_runtime.enter();
    let s3_blob =
        S3Blob::open_exclusive(config, LockInfo::new_no_reentrance("bench_load_s3".into()))?;
    drop(async_guard);

    let mut runtime = runtime::start(
        RuntimeConfig::default(),
        ErrorLog,
        s3_blob,
        mz_build_info::DUMMY_BUILD_INFO,
        &MetricsRegistry::new(),
        None,
    )?;
    let (write, _read) = runtime.create_or_load("end_to_end");
    workload::load(&write, &data, true)?;
    runtime.stop()
}

pub fn bench_replay(data: &DataGenerator, g: &mut BenchmarkGroup<'_, WallTime>) {
    g.throughput(Throughput::Bytes(data.goodput_bytes()));
    g.bench_function(BenchmarkId::new("file", data.goodput_pretty()), move |b| {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let nonce = "end_to_end".to_string();
        let collection_name = "end_to_end".to_string();
        let mut runtime = create_runtime(temp_dir.path(), &nonce).expect("missing runtime");
        let (write, _read) = runtime.create_or_load(&collection_name);
        workload::load(&write, &data, true).expect("error writing data");
        let expected_frontier = u64::cast_from(data.record_count);
        runtime.stop().expect("runtime shut down cleanly");

        bench_read_persisted_source(
            1,
            temp_dir.path().to_path_buf(),
            nonce,
            collection_name,
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

            let (_write, read) = runtime.create_or_load::<Vec<u8>, Vec<u8>>(&collection_id);

            let mut probe = ProbeHandle::new();

            worker.dataflow(|scope| {
                scope
                    .persisted_source(read, &Antichain::from_elem(0))
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
    let blob_dir = base_path.join(format!("blob_{}", nonce));
    let lock_info = LockInfo::new(format!("reentrance_{}", nonce), "no_details".to_owned())?;
    let runtime = runtime::start(
        RuntimeConfig::default(),
        ErrorLog,
        FileBlob::open_exclusive(blob_dir.into(), lock_info)?,
        mz_build_info::DUMMY_BUILD_INFO,
        &MetricsRegistry::new(),
        None,
    )?;
    Ok(runtime)
}
