// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI benchmarking tools for persist

use futures_util::stream::StreamExt;
use futures_util::{TryStreamExt, stream};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use bytes::Bytes;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::ExternalError;

use crate::cfg::PersistConfig;
use crate::cli::args::{READ_ALL_BUILD_INFO, StateArgs, make_blob};
use crate::internal::machine::retry_external;
use crate::internal::state::BatchPart;
use crate::metrics::Metrics;

/// Commands for read-only inspection of persist state
#[derive(Debug, clap::Args)]
pub struct BenchArgs {
    #[clap(subcommand)]
    command: Command,
}

/// Individual subcommands of bench
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// Fetch the blobs in a shard as quickly as possible, repeated some number
    /// of times.
    S3Fetch(S3FetchArgs),
    /// Write, list, read and delete objects of one size at a fixed
    /// concurrency, through persist's own blob store client.
    Blob(BlobArgs),
}

/// Fetch the blobs in a shard as quickly as possible, repeated some number of
/// times.
#[derive(Debug, Clone, clap::Parser)]
pub struct S3FetchArgs {
    #[clap(flatten)]
    shard: StateArgs,

    #[clap(long, default_value_t = 1)]
    iters: usize,

    #[clap(long)]
    parse: bool,
}

/// Runs the given bench command.
pub async fn run(command: BenchArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::S3Fetch(args) => bench_s3(&args).await?,
        Command::Blob(args) => bench_blob(&args).await?,
    }

    Ok(())
}

async fn bench_s3(args: &S3FetchArgs) -> Result<(), anyhow::Error> {
    let parse = args.parse;
    let shard_id = args.shard.shard_id();
    let state_versions = args.shard.open().await?;
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;
    let state = state_versions
        .fetch_current_state::<u64>(&shard_id, versions.0)
        .await;
    let state = state.check_ts_codec(&shard_id)?;
    let snap = state
        .snapshot(state.since())
        .expect("since should be available for reads");

    let batch_parts: Vec<_> = stream::iter(&snap)
        .flat_map(|batch| {
            batch.part_stream(shard_id, &*state_versions.blob, &*state_versions.metrics)
        })
        .try_collect()
        .await?;

    println!("iter,key,size_bytes,fetch_secs,parse_secs");
    for iter in 0..args.iters {
        let start = Instant::now();
        let mut fetches = Vec::new();
        for part in &batch_parts {
            let key = match &**part {
                BatchPart::Hollow(x) => x.key.complete(&shard_id),
                BatchPart::Inline { .. } => continue,
            };
            let blob = Arc::clone(&state_versions.blob);
            let metrics = Arc::clone(&state_versions.metrics);
            let fetch = mz_ore::task::spawn(|| "", async move {
                let buf = blob.get(&key).await.unwrap().unwrap();
                let fetch_elapsed = start.elapsed();
                let buf_len = buf.len();
                let parse_elapsed = mz_ore::task::spawn_blocking(
                    || "",
                    move || {
                        let start = Instant::now();
                        if parse {
                            BlobTraceBatchPart::<u64>::decode(&buf, &metrics.columnar).unwrap();
                        }
                        start.elapsed()
                    },
                )
                .await;
                (
                    key,
                    buf_len,
                    fetch_elapsed.as_secs_f64(),
                    parse_elapsed.as_secs_f64(),
                )
            });
            fetches.push(fetch);
        }
        for fetch in fetches {
            let (key, size_bytes, fetch_secs, parse_secs) = fetch.await;
            println!(
                "{},{},{},{},{}",
                iter, key, size_bytes, fetch_secs, parse_secs
            );
        }
    }

    Ok(())
}

/// Drives a blob store through persist's own blob client and
/// reports what each operation cost, as one CSV row per operation.
///
/// Writes `count` objects of `size_bytes` under `prefix`, lists `list_prefix`,
/// reads the objects back, and deletes them, each with `concurrency`
/// operations in flight. Writes, reads and deletes are retried the way persist
/// retries them, so a store that throttles shows up as latency and in the
/// `retries` column rather than as a failed run. The reads check that every
/// object comes back at its written size, so a store that hands back partial
/// objects does fail the run.
#[derive(Debug, Clone, clap::Parser)]
pub struct BlobArgs {
    /// Blob store to benchmark, in the form `--persist-blob-url` takes.
    #[clap(long, env = "BLOB_URI")]
    blob_uri: SensitiveUrl,

    /// Key prefix the objects are written under.
    #[clap(long, default_value = "bench")]
    prefix: String,

    /// Key prefix to list. Defaults to `prefix`. A parent of it measures a
    /// listing that spans objects kept from earlier runs.
    #[clap(long)]
    list_prefix: Option<String>,

    /// Size of each object in bytes.
    #[clap(long)]
    size_bytes: usize,

    /// Number of objects.
    #[clap(long)]
    count: usize,

    /// Operations in flight at once.
    #[clap(long, default_value_t = 1)]
    concurrency: usize,

    /// How long to read objects picked at random, in seconds. With 0, each
    /// object is read once instead.
    #[clap(long, default_value_t = 0)]
    read_secs: u64,

    /// Skip the read phase.
    #[clap(long)]
    skip_read: bool,

    /// Skip the list phase.
    #[clap(long)]
    skip_list: bool,

    /// Leave the objects in place instead of deleting them, so that later
    /// runs see a fuller store.
    #[clap(long)]
    keep: bool,

    /// Seed for the object contents and the read order.
    #[clap(long, default_value_t = 0)]
    seed: u64,

    /// Omit the CSV header row.
    #[clap(long)]
    no_header: bool,
}

async fn bench_blob(args: &BlobArgs) -> Result<(), anyhow::Error> {
    if args.count == 0 {
        return Err(anyhow!("--count must be positive"));
    }
    let cfg = PersistConfig::new_default_configs(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let blob = make_blob(&cfg, &args.blob_uri, true, Arc::clone(&metrics)).await?;
    let concurrency = args.concurrency.max(1);
    // The same retry loops persist's own writers, readers and garbage
    // collector run their blob operations under.
    let set_retries = &metrics.retries.external.batch_set;
    let get_retries = &metrics.retries.external.fetch_batch_get;
    let delete_retries = &*metrics.retries.external.batch_delete;

    let mut rng = SplitMix64(args.seed);
    // Incompressible, like the parquet parts persist writes, so that a store
    // that compresses cannot shrink the work.
    let payload = random_bytes(&mut rng, args.size_bytes);
    let keys: Vec<String> = (0..args.count)
        .map(|i| format!("{}/{i:08}", args.prefix))
        .collect();

    if !args.no_header {
        println!(
            "op,size_bytes,concurrency,ops,bytes,elapsed_secs,ops_per_sec,mib_per_sec,p50_ms,p90_ms,p99_ms,max_ms,retries"
        );
    }

    let start = Instant::now();
    let retries_before = set_retries.retries.get();
    let latencies = run_ops(concurrency, keys.iter(), |key| {
        let blob = Arc::clone(&blob);
        let payload = payload.clone();
        async move {
            retry_external(set_retries, || blob.set(key, payload.clone())).await;
            Ok(())
        }
    })
    .await?;
    report(
        args,
        "set",
        &latencies,
        start.elapsed(),
        args.size_bytes,
        set_retries.retries.get() - retries_before,
    );

    if !args.skip_list {
        let list_prefix = args.list_prefix.as_deref().unwrap_or(&args.prefix);
        let start = Instant::now();
        let mut listed = 0;
        blob.list_keys_and_metadata(list_prefix, &mut |_| listed += 1)
            .await?;
        let elapsed = start.elapsed();
        report(args, "list", &vec![elapsed; listed], elapsed, 0, 0);
    }

    if !args.skip_read {
        let deadline = Instant::now() + Duration::from_secs(args.read_secs);
        let read_keys: Box<dyn Iterator<Item = &String>> = if args.read_secs == 0 {
            Box::new(keys.iter())
        } else {
            Box::new(std::iter::from_fn(|| {
                (Instant::now() < deadline)
                    .then(|| &keys[usize::cast_from(rng.next() % u64::cast_from(keys.len()))])
            }))
        };
        let size = args.size_bytes;
        let start = Instant::now();
        let retries_before = get_retries.retries.get();
        let latencies = run_ops(concurrency, read_keys, |key| {
            let blob = Arc::clone(&blob);
            async move {
                // Only the store call is retried. A short or missing object
                // is the store lying, not a transient, and ends the run.
                match retry_external(get_retries, || blob.get(key)).await {
                    Some(value) if value.len() == size => Ok(()),
                    Some(value) => Err(anyhow!(
                        "{key}: read {} bytes of a {size} byte object",
                        value.len()
                    )
                    .into()),
                    None => Err(anyhow!("{key}: missing").into()),
                }
            }
        })
        .await?;
        report(
            args,
            "get",
            &latencies,
            start.elapsed(),
            args.size_bytes,
            get_retries.retries.get() - retries_before,
        );
    }

    if !args.keep {
        let start = Instant::now();
        let retries_before = delete_retries.retries.get();
        let latencies = run_ops(concurrency, keys.iter(), |key| {
            let blob = Arc::clone(&blob);
            async move {
                retry_external(delete_retries, || blob.delete(key)).await;
                Ok(())
            }
        })
        .await?;
        report(
            args,
            "delete",
            &latencies,
            start.elapsed(),
            0,
            delete_retries.retries.get() - retries_before,
        );
    }

    Ok(())
}

/// Runs `op` over `items` with `concurrency` in flight, returning each
/// operation's latency.
async fn run_ops<K, F, Fut>(
    concurrency: usize,
    items: impl IntoIterator<Item = K>,
    op: F,
) -> Result<Vec<Duration>, ExternalError>
where
    F: Fn(K) -> Fut,
    Fut: Future<Output = Result<(), ExternalError>>,
{
    stream::iter(items)
        .map(|item| {
            let start = Instant::now();
            let fut = op(item);
            async move {
                fut.await?;
                Ok(start.elapsed())
            }
        })
        .buffer_unordered(concurrency)
        .try_collect()
        .await
}

fn report(
    args: &BlobArgs,
    op: &str,
    latencies: &[Duration],
    elapsed: Duration,
    bytes_each: usize,
    retries: u64,
) {
    let mut sorted = latencies.to_vec();
    sorted.sort();
    let ops = sorted.len();
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    // Nearest-rank percentiles over the sorted latencies.
    let pct = |q: usize| {
        if ops == 0 {
            f64::NAN
        } else {
            ms(sorted[(ops - 1) * q / 100])
        }
    };
    let bytes = ops * bytes_each;
    let secs = elapsed.as_secs_f64();
    println!(
        "{op},{},{},{ops},{bytes},{secs:.3},{:.1},{:.1},{:.2},{:.2},{:.2},{:.2},{retries}",
        args.size_bytes,
        args.concurrency,
        f64::cast_lossy(ops) / secs,
        f64::cast_lossy(bytes) / secs / (1024.0 * 1024.0),
        pct(50),
        pct(90),
        pct(99),
        sorted.last().map_or(f64::NAN, |d| ms(*d)),
    );
}

/// SplitMix64, so that the payload and read order are reproducible without
/// pulling in a random number generator.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

fn random_bytes(rng: &mut SplitMix64, len: usize) -> Bytes {
    let mut buf = Vec::with_capacity(len + 8);
    while buf.len() < len {
        buf.extend_from_slice(&rng.next().to_le_bytes());
    }
    buf.truncate(len);
    Bytes::from(buf)
}
