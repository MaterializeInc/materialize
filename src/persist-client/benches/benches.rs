// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, Bencher, BenchmarkGroup, BenchmarkId, Criterion};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use tempfile::TempDir;
use timely::progress::{Antichain, Timestamp};
use tokio::runtime::Runtime;

use mz_persist::file::{FileBlob, FileBlobConfig};
use mz_persist::location::{Blob, Consensus, ExternalError};
use mz_persist::mem::{MemBlob, MemBlobConfig, MemConsensus};
use mz_persist::postgres::{PostgresConsensus, PostgresConsensusConfig};
use mz_persist::s3::{S3Blob, S3BlobConfig};
use mz_persist::workload::DataGenerator;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Metrics, PersistClient, PersistConfig};
use mz_persist_types::Codec64;

// The "plumbing" and "porcelain" names are from git [1]. Our "plumbing"
// benchmarks are ones that are low-level, fundamental pieces of code like
// writing to a Blob/Consensus impl or encoding a batch of updates into our
// columnar format. One way to look at this is as the fastest we could possibly
// go in theory. The "porcelain" ones are measured at the level of the public
// API. One way to think of this is how fast we actually are in practice.
//
// [1]: https://git-scm.com/book/en/v2/Git-Internals-Plumbing-and-Porcelain
mod plumbing;
mod porcelain;

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

    // Default to latency. First, because it's usually more interesting for
    // these micro-benchmarks (throughput is more the domain of the open-loop
    // benchmark). Second, because this allows `cargo test --all-targets` to
    // finish in a reasonable time for this crate, even without --release.
    //
    // However, also allow an easy toggle for a quick, rough estimate of
    // throughput without going through the involvement of the open-loop
    // benchmark.
    let throughput = mz_ore::env::is_var_truthy("MZ_PERSIST_BENCH_THROUGHPUT");
    let data = if throughput {
        eprint!("MZ_PERSIST_BENCH_THROUGHPUT=1 ");
        DataGenerator::default()
    } else {
        DataGenerator::small()
    };

    porcelain::bench_writes("porcelain/writes", throughput, c, &runtime, &data);
    porcelain::bench_write_to_listen("porcelain/write_to_listen", throughput, c, &runtime, &data);
    porcelain::bench_snapshot("porcelain/snapshot", throughput, c, &runtime, &data);

    plumbing::bench_blob_get("plumbing/blob_get", throughput, c, &runtime, &data);
    plumbing::bench_blob_set("plumbing/blob_set", throughput, c, &runtime, &data);
    if !throughput {
        let mut bench_compare_and_set = |concurrency, num_shards| {
            plumbing::bench_consensus_compare_and_set(
                &format!(
                    "plumbing/concurrency={},num_shards={}/consensus_compare_and_set",
                    concurrency, num_shards
                ),
                c,
                &runtime,
                &data,
                concurrency,
                num_shards,
            )
        };
        bench_compare_and_set(1, 1);
        bench_compare_and_set(1, ncpus_useful);
    }
    plumbing::bench_encode_batch("plumbing/encode_batch", throughput, c, &data);
}

async fn create_mem_mem_client() -> Result<PersistClient, ExternalError> {
    let blob = Arc::new(MemBlob::open(MemBlobConfig::default()));
    let consensus = Arc::new(MemConsensus::default());
    let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
    PersistClient::new(
        PersistConfig::new(SYSTEM_TIME.clone()),
        blob,
        consensus,
        metrics,
    )
    .await
}

async fn create_file_pg_client(
) -> Result<Option<(Arc<PostgresConsensus>, PersistClient, TempDir)>, ExternalError> {
    let pg = match PostgresConsensusConfig::new_for_test().await? {
        Some(x) => x,
        None => return Ok(None),
    };
    let dir = tempfile::tempdir().map_err(anyhow::Error::new)?;
    let file = FileBlobConfig::from(dir.path());

    let blob = Arc::new(FileBlob::open(file).await?) as Arc<dyn Blob + Send + Sync>;
    let postgres_consensus = Arc::new(PostgresConsensus::open(pg).await?);
    let consensus = Arc::clone(&postgres_consensus) as Arc<dyn Consensus + Send + Sync>;
    let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
    let client = PersistClient::new(
        PersistConfig::new(SYSTEM_TIME.clone()),
        blob,
        consensus,
        metrics,
    )
    .await?;
    Ok(Some((postgres_consensus, client, dir)))
}

async fn create_s3_pg_client(
) -> Result<Option<(Arc<PostgresConsensus>, PersistClient)>, ExternalError> {
    let s3 = match S3BlobConfig::new_for_test().await? {
        Some(x) => x,
        None => return Ok(None),
    };
    let pg = match PostgresConsensusConfig::new_for_test().await? {
        Some(x) => x,
        None => return Ok(None),
    };

    let blob = Arc::new(S3Blob::open(s3).await?) as Arc<dyn Blob + Send + Sync>;
    let postgres_consensus = Arc::new(PostgresConsensus::open(pg).await?);
    let consensus = Arc::clone(&postgres_consensus) as Arc<dyn Consensus + Send + Sync>;
    let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
    let client = PersistClient::new(
        PersistConfig::new(SYSTEM_TIME.clone()),
        blob,
        consensus,
        metrics,
    )
    .await?;
    Ok(Some((postgres_consensus, client)))
}

fn bench_all_clients<BenchClientFn>(
    g: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &Runtime,
    data: &DataGenerator,
    bench_client_fn: BenchClientFn,
) where
    BenchClientFn: Fn(&mut Bencher, &PersistClient),
{
    // Unlike the other ones, create a new mem blob before each call to
    // bench_client_fn and drop it after to keep mem usage down.
    {
        g.bench_function(BenchmarkId::new("mem_mem", data.goodput_pretty()), |b| {
            let client = runtime
                .block_on(create_mem_mem_client())
                .expect("failed to create mem_mem client");
            bench_client_fn(b, &client);
        });
    }

    // Create a directory that will automatically be dropped after the test
    // finishes.
    if let Some((postgres_consensus, client, tempdir)) = runtime
        .block_on(create_file_pg_client())
        .expect("failed to create file_pg client")
    {
        g.bench_function(BenchmarkId::new("file_pg", data.goodput_pretty()), |b| {
            runtime
                .block_on(postgres_consensus.drop_and_recreate())
                .expect("failed to drop and recreate postgres consensus");
            bench_client_fn(b, &client);
        });
        drop(tempdir);
    }

    // Only run S3+Postgres benchmarks if the magic env vars are set.
    if let Some((postgres_consensus, client)) = runtime
        .block_on(create_s3_pg_client())
        .expect("failed to create s3_pg client")
    {
        g.bench_function(BenchmarkId::new("s3_pg", data.goodput_pretty()), |b| {
            runtime
                .block_on(postgres_consensus.drop_and_recreate())
                .expect("failed to drop and recreate postgres consensus");
            bench_client_fn(b, &client);
        });
    }
}

fn bench_all_blob<BenchBlobFn>(
    g: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &Runtime,
    data: &DataGenerator,
    bench_blob_fn: BenchBlobFn,
) where
    BenchBlobFn: Fn(&mut Bencher, &Arc<dyn Blob + Send + Sync>),
{
    // Unlike the other ones, create a new mem blob before each call to
    // bench_blob_fn and drop it after to keep mem usage down.
    {
        g.bench_function(BenchmarkId::new("mem", data.goodput_pretty()), |b| {
            let mem_blob = MemBlob::open(MemBlobConfig::default());
            let mem_blob = Arc::new(mem_blob) as Arc<dyn Blob + Send + Sync>;
            bench_blob_fn(b, &mem_blob);
        });
    }

    // Create a directory that will automatically be dropped after the test
    // finishes.
    {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let file_blob = runtime
            .block_on(FileBlob::open(FileBlobConfig::from(temp_dir.path())))
            .expect("failed to create file blob");
        let file_blob = Arc::new(file_blob) as Arc<dyn Blob + Send + Sync>;
        g.bench_function(BenchmarkId::new("file", data.goodput_pretty()), |b| {
            bench_blob_fn(b, &file_blob);
        });
        drop(temp_dir);
    }

    // Only run S3 benchmarks if the magic env vars are set.
    if let Some(config) = runtime
        .block_on(S3BlobConfig::new_for_test())
        .expect("failed to load s3 config")
    {
        let s3_blob = runtime
            .block_on(S3Blob::open(config))
            .expect("failed to create s3 blob");
        let s3_blob = Arc::new(s3_blob) as Arc<dyn Blob + Send + Sync>;
        g.bench_function(BenchmarkId::new("s3", data.goodput_pretty()), |b| {
            bench_blob_fn(b, &s3_blob);
        });
    }
}

fn bench_all_consensus<BenchConsensusFn>(
    g: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &Runtime,
    data: &DataGenerator,
    bench_consensus_fn: BenchConsensusFn,
) where
    BenchConsensusFn: Fn(&mut Bencher, &Arc<dyn Consensus + Send + Sync>),
{
    // Unlike the other ones, create a new mem blob before each call to
    // bench_consensus_fn and drop it after to keep mem usage down.
    {
        g.bench_function(BenchmarkId::new("mem", data.goodput_pretty()), |b| {
            let mem_consensus =
                Arc::new(MemConsensus::default()) as Arc<dyn Consensus + Send + Sync>;
            bench_consensus_fn(b, &mem_consensus);
        });
    }

    // Only run Postgres benchmarks if the magic env vars are set.
    if let Some(config) = runtime
        .block_on(PostgresConsensusConfig::new_for_test())
        .expect("failed to load postgres config")
    {
        let postgres_consensus = runtime
            .block_on(PostgresConsensus::open(config))
            .expect("failed to create postgres consensus");
        let postgres_consensus = Arc::new(postgres_consensus);
        g.bench_function(BenchmarkId::new("postgres", data.goodput_pretty()), |b| {
            runtime
                .block_on(postgres_consensus.drop_and_recreate())
                .expect("failed to drop and recreate postgres consensus");
            bench_consensus_fn(
                b,
                &(Arc::clone(&postgres_consensus) as Arc<dyn Consensus + Send + Sync>),
            );
        });
    }
}

async fn load(
    write: &mut WriteHandle<Vec<u8>, Vec<u8>, u64, i64>,
    data: &DataGenerator,
) -> (usize, Antichain<u64>) {
    let mut batch_count = 0;
    let mut max_ts = u64::minimum();
    for batch in data.batches() {
        batch_count += 1;
        max_ts = match batch.get(batch.len() - 1) {
            Some((_, max_ts, _)) => u64::decode(max_ts),
            None => continue,
        };
        let updates = batch
            .iter()
            .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), u64::decode(t), i64::decode(d)));
        write
            .compare_and_append(
                updates,
                write.upper().clone(),
                Antichain::from_elem(max_ts + 1),
            )
            .await
            .expect("external durability failure")
            .expect("invalid usage")
            .expect("unexpected upper");
    }
    (batch_count, Antichain::from_elem(max_ts))
}

// The grouping here is an artifact of criterion's interaction with the
// plug-able rust benchmark harness. We use criterion's groups instead.
criterion_group! {
    benches,
    bench_persist,
}
criterion_main!(benches);
