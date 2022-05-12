// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use mz_persist::file::{FileBlobConfig, FileBlobMulti};
use mz_persist::location::{BlobMulti, Consensus, ExternalError};
use mz_persist::mem::{MemBlobMulti, MemBlobMultiConfig, MemConsensus};
use mz_persist::postgres::{PostgresConsensus, PostgresConsensusConfig};
use mz_persist::s3::{S3BlobConfig, S3BlobMulti};
use mz_persist::workload::DataGenerator;
use mz_persist_client::PersistClient;
use tempfile::TempDir;

mod min_latency;

pub fn bench_persist(c: &mut Criterion) {
    // Override the default of "info" here because the s3 library is chatty on
    // info while initializing. It's good info to have in mz logs, but ends
    // being as spammy in these benchmarks.
    mz_ore::test::init_logging_default("warn");

    // Mirror the tokio Runtime configuration in our production binaries.
    let ncpus_useful = usize::max(1, std::cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ncpus_useful)
        .enable_all()
        .build()
        .expect("Failed building the Runtime");
    let runtime = Arc::new(runtime);

    // Intentionally use the small generator for min_latency benches, because
    // y'know... min latencies. :)
    let data_small = DataGenerator::small();

    min_latency::bench_writes(
        &mut c.benchmark_group("min_latency/writes"),
        Arc::clone(&runtime),
        &data_small,
    )
    .expect("failed to bench writes");

    min_latency::bench_write_to_listen(
        &mut c.benchmark_group("min_latency/write_to_listen"),
        runtime,
        &data_small,
    )
    .expect("failed to bench write_to_listen");
}

async fn create_mem_mem_client() -> Result<PersistClient, ExternalError> {
    let blob = Arc::new(MemBlobMulti::open(MemBlobMultiConfig::default()));
    let consensus = Arc::new(MemConsensus::default());
    PersistClient::new(blob, consensus).await
}

async fn create_file_pg_client() -> Result<Option<(PersistClient, TempDir)>, ExternalError> {
    let pg = match PostgresConsensusConfig::new_for_test().await? {
        Some(x) => x,
        None => return Ok(None),
    };
    let dir = tempfile::tempdir().map_err(anyhow::Error::new)?;
    let file = FileBlobConfig::from(dir.path());

    let blob = Arc::new(FileBlobMulti::open(file).await?) as Arc<dyn BlobMulti + Send + Sync>;
    let consensus =
        Arc::new(PostgresConsensus::open(pg).await?) as Arc<dyn Consensus + Send + Sync>;
    let client = PersistClient::new(blob, consensus).await?;
    Ok(Some((client, dir)))
}

async fn create_s3_pg_client() -> Result<Option<PersistClient>, ExternalError> {
    let s3 = match S3BlobConfig::new_for_test().await? {
        Some(x) => x,
        None => return Ok(None),
    };
    let pg = match PostgresConsensusConfig::new_for_test().await? {
        Some(x) => x,
        None => return Ok(None),
    };

    let blob = Arc::new(S3BlobMulti::open(s3).await?) as Arc<dyn BlobMulti + Send + Sync>;
    let consensus =
        Arc::new(PostgresConsensus::open(pg).await?) as Arc<dyn Consensus + Send + Sync>;
    let client = PersistClient::new(blob, consensus).await?;
    Ok(Some(client))
}

// The grouping here is an artifact of criterion's interaction with the
// plug-able rust benchmark harness. We use criterion's groups instead.
criterion_group! {
    benches,
    bench_persist,
}
criterion_main!(benches);
