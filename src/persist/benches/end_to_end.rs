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

use criterion::measurement::Measurement;
use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion, Throughput};
use differential_dataflow::operators::Count;
use differential_dataflow::AsCollection;
use ore::cast::CastFrom;
use ore::metrics::MetricsRegistry;
use timely::dataflow::operators::Map;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;
use timely::Config;

use persist::client::RuntimeClient;
use persist::error::{Error, ErrorLog};
use persist::file::FileBlob;
use persist::operators::source::PersistedSource;
use persist::runtime::{self, RuntimeConfig};
use persist::storage::{Blob, LockInfo};
use persist::workload::{self, DataGenerator};

pub fn bench_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("end_to_end");

    let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
    let nonce = "end_to_end".to_string();
    let collection_name = "end_to_end".to_string();
    let mut runtime = create_runtime(temp_dir.path(), &nonce).expect("missing runtime");
    let (write, _read) = runtime.create_or_load(&collection_name);
    let data = DataGenerator::default();
    let goodput_bytes = workload::load(&write, &data, true).expect("error writing data");
    group.throughput(Throughput::Bytes(goodput_bytes));
    let expected_frontier = u64::cast_from(data.record_count);
    runtime.stop().expect("runtime shut down cleanly");

    group.bench_function(
        BenchmarkId::new("end_to_end", data.goodput_pretty()),
        move |b| {
            bench_read_persisted_source(
                1,
                temp_dir.path().to_path_buf(),
                nonce.clone(),
                collection_name.clone(),
                expected_frontier,
                b,
            )
        },
    );
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
        build_info::DUMMY_BUILD_INFO,
        &MetricsRegistry::new(),
        None,
    )?;
    Ok(runtime)
}

criterion_group! {
    benches,
    bench_end_to_end,
}
criterion_main!(benches);
