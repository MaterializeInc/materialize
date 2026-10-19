// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Concurrent [S3Blob] reads, plus a steady stream of PUTs through the same
//! client, against an S3-compatible store.
//!
//! Written to measure GCS read throughput (PER-136). Over HTTP/2, the SDK's
//! HTTP client sends every request to a host over one connection, and one
//! connection to GCS carried only 300 to 400 MiB/s.
//! `mz_aws_util::pooled_http_client` spreads requests over more connections.
//! Compare `--client sdk` with `--client pooled` to recheck that, or to tune
//! the pool's size.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::bail;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::runtime_components::{
    RuntimeComponents, RuntimeComponentsBuilder,
};
use aws_smithy_types::config_bag::ConfigBag;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use mz_ore::cast::CastLossy;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::secure::SecureString;
use mz_persist::location::Blob;
use mz_persist::metrics::S3BlobMetrics;
use mz_persist::s3::{S3Blob, S3BlobConfig};

use crate::cfg::PersistConfig;

/// Concurrent persist blob reads against an S3-compatible store.
#[derive(Debug, clap::Args)]
pub struct BlobReadArgs {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Writes test objects with `S3Blob::set`, so objects over the multipart
    /// threshold are uploaded in parts, like production blobs.
    Upload(UploadArgs),
    /// Reads the test objects with N concurrent readers for each N, while
    /// writing small objects at a steady rate, and reports latency,
    /// throughput and errors.
    Read(ReadArgs),
}

#[derive(Debug, clap::Args)]
struct StoreArgs {
    #[clap(long, default_value = "https://storage.googleapis.com")]
    endpoint: String,
    #[clap(long, default_value = "auto")]
    region: String,
    #[clap(long)]
    bucket: String,
    /// Key prefix of the test objects.
    #[clap(long, default_value = "blob-read-bench")]
    prefix: String,
    #[clap(long, env = "BLOB_READ_BENCH_ACCESS_KEY_ID")]
    access_key_id: String,
    #[clap(
        long,
        env = "BLOB_READ_BENCH_SECRET_ACCESS_KEY",
        hide_env_values = true
    )]
    secret_access_key: SecureString,
}

#[derive(Debug, clap::Args)]
struct UploadArgs {
    #[clap(flatten)]
    store: StoreArgs,
    #[clap(long, value_delimiter = ',', default_value = "8,16,64")]
    sizes_mib: Vec<usize>,
    #[clap(long, default_value_t = 70)]
    count_per_size: usize,
    #[clap(long, default_value_t = 16)]
    concurrency: usize,
}

#[derive(Debug, clap::Args)]
struct ReadArgs {
    #[clap(flatten)]
    store: StoreArgs,
    #[clap(long, value_enum)]
    client: ClientKind,
    /// Runs one round per value, in order.
    #[clap(long, value_delimiter = ',', default_value = "16,64,256,512")]
    concurrency: Vec<usize>,
    /// Readers start no new get after this long. A round lasts until every
    /// in-flight get finishes.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "30s")]
    duration: Duration,
    /// PUTs started per second during each round. 0 turns them off.
    #[clap(long, default_value_t = 10)]
    puts_per_sec: u32,
    #[clap(long, default_value_t = 1024)]
    put_size_kib: usize,
    /// A PUT that takes longer than this counts as stalled.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "10s")]
    put_stall: Duration,
}

/// PUTs cycle through this many keys under [PUT_KEY_PREFIX], overwriting old
/// ones, so they don't pile up in the bucket.
const PUT_KEYS: usize = 100;
/// Keeps PUT objects out of the keys that readers fetch.
const PUT_KEY_PREFIX: &str = "put/";

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum ClientKind {
    /// `mz_aws_util::http_client`, the SDK's client.
    Sdk,
    /// `mz_aws_util::pooled_http_client`, which spreads requests over a
    /// growing set of `Sdk` clients.
    Pooled,
}

/// Runs the given blob-read bench command.
pub(crate) async fn run(args: BlobReadArgs) -> Result<(), anyhow::Error> {
    match args.command {
        Command::Upload(args) => upload(args).await,
        Command::Read(args) => read(args).await,
    }
}

/// Opens an [S3Blob] with persist's production timeouts and no hedging.
async fn open(
    store: &StoreArgs,
    client: ClientKind,
    registry: &MetricsRegistry,
    header_waits: Arc<HeaderWaits>,
) -> Result<S3Blob, anyhow::Error> {
    let knobs = PersistConfig::new_default_configs(&crate::BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = S3BlobMetrics::new(registry);
    let pool_metrics = metrics.http_pool();
    let config = S3BlobConfig::new(
        store.bucket.clone(),
        store.prefix.clone(),
        None,
        Some(store.endpoint.clone()),
        Some(store.region.clone()),
        Some((
            store.access_key_id.clone(),
            store.secret_access_key.unsecure().to_owned(),
        )),
        Box::new(knobs),
        metrics,
    )
    .await?;
    let http_client = match client {
        ClientKind::Sdk => SharedHttpClient::new(mz_aws_util::http_client()),
        ClientKind::Pooled => SharedHttpClient::new(mz_aws_util::pooled_http_client(pool_metrics)),
    };
    let config = config.with_http_client(TimedHttpClient {
        inner: http_client,
        waits: header_waits,
    });
    Ok(S3Blob::open(config).await?)
}

async fn upload(args: UploadArgs) -> Result<(), anyhow::Error> {
    let blob = open(
        &args.store,
        ClientKind::Sdk,
        &MetricsRegistry::new(),
        Arc::default(),
    )
    .await?;
    let objects: Vec<_> = args
        .sizes_mib
        .iter()
        .flat_map(|size| (0..args.count_per_size).map(move |i| (format!("{size}mib-{i}"), *size)))
        .collect();
    let mut uploads = futures::stream::iter(objects)
        .map(|(key, size_mib)| {
            let blob = &blob;
            async move {
                let mut data = vec![0; size_mib * 1024 * 1024];
                rand::fill(&mut data[..]);
                blob.set(&key, Bytes::from(data)).await?;
                Ok::<_, anyhow::Error>(key)
            }
        })
        .buffer_unordered(args.concurrency);
    while let Some(key) = uploads.try_next().await? {
        println!("uploaded {key}");
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ReaderStats {
    latencies: Vec<Duration>,
    failed_latencies: Vec<Duration>,
    bytes: u64,
    errors: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
struct PutStats {
    latencies: Vec<Duration>,
    stalled: usize,
    errors: BTreeMap<String, u64>,
}

async fn read(args: ReadArgs) -> Result<(), anyhow::Error> {
    let registry = MetricsRegistry::new();
    let header_waits = Arc::new(HeaderWaits::default());
    let blob = Arc::new(
        open(
            &args.store,
            args.client,
            &registry,
            Arc::clone(&header_waits),
        )
        .await?,
    );
    let mut keys = Vec::new();
    blob.list_keys_and_metadata("", &mut |meta| {
        if !meta.key.starts_with(PUT_KEY_PREFIX) {
            keys.push(meta.key.to_owned());
        }
    })
    .await?;
    if keys.is_empty() {
        bail!("no objects under prefix {}", args.store.prefix);
    }
    let keys = Arc::new(keys);
    let mut put_data = vec![0; args.put_size_kib * 1024];
    rand::fill(&mut put_data[..]);
    let put_data = Bytes::from(put_data);
    let next_put = Arc::new(AtomicUsize::new(0));

    for &concurrency in &args.concurrency {
        let read_timeouts_before = read_timeouts(&registry);
        header_waits.take();
        let next_key = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();
        let deadline = start + args.duration;
        let putter = (args.puts_per_sec > 0).then(|| {
            let blob = Arc::clone(&blob);
            let put_data = put_data.clone();
            let next_put = Arc::clone(&next_put);
            let period = Duration::from_secs(1) / args.puts_per_sec;
            let put_stall = args.put_stall;
            mz_ore::task::spawn(|| "blob_read_bench_putter", async move {
                // Each PUT runs in its own task, so a stalled one doesn't
                // delay the next.
                let mut puts = Vec::new();
                let mut ticker = tokio::time::interval(period);
                while Instant::now() < deadline {
                    ticker.tick().await;
                    let n = next_put.fetch_add(1, Ordering::Relaxed) % PUT_KEYS;
                    let key = format!("{PUT_KEY_PREFIX}{n}");
                    let blob = Arc::clone(&blob);
                    let put_data = put_data.clone();
                    puts.push(mz_ore::task::spawn(|| "blob_read_bench_put", async move {
                        let put_start = Instant::now();
                        let result = blob.set(&key, put_data).await;
                        (put_start.elapsed(), result)
                    }));
                }
                let mut stats = PutStats::default();
                for put in puts {
                    let (elapsed, result) = put.await;
                    if elapsed > put_stall {
                        stats.stalled += 1;
                    }
                    match result {
                        Ok(()) => stats.latencies.push(elapsed),
                        Err(err) => {
                            *stats
                                .errors
                                .entry(err.display_with_causes().to_string())
                                .or_default() += 1
                        }
                    }
                }
                stats
            })
        });
        let readers: Vec<_> = (0..concurrency)
            .map(|_| {
                let blob = Arc::clone(&blob);
                let keys = Arc::clone(&keys);
                let next_key = Arc::clone(&next_key);
                mz_ore::task::spawn(|| "blob_read_bench_reader", async move {
                    let mut stats = ReaderStats::default();
                    while Instant::now() < deadline {
                        let key = &keys[next_key.fetch_add(1, Ordering::Relaxed) % keys.len()];
                        let get_start = Instant::now();
                        let result = blob.get(key).await;
                        let elapsed = get_start.elapsed();
                        let err = match result {
                            Ok(Some(value)) => {
                                stats.latencies.push(elapsed);
                                stats.bytes += u64::try_from(value.len()).expect("fits");
                                continue;
                            }
                            Ok(None) => format!("{key} missing"),
                            Err(err) => err.display_with_causes().to_string(),
                        };
                        stats.failed_latencies.push(elapsed);
                        *stats.errors.entry(err).or_default() += 1;
                    }
                    stats
                })
            })
            .collect();

        let mut total = ReaderStats::default();
        for reader in readers {
            let stats = reader.await;
            total.latencies.extend(stats.latencies);
            total.failed_latencies.extend(stats.failed_latencies);
            total.bytes += stats.bytes;
            for (err, count) in stats.errors {
                *total.errors.entry(err).or_default() += count;
            }
        }
        let elapsed = start.elapsed();
        let puts = match putter {
            Some(putter) => putter.await,
            None => PutStats::default(),
        };

        // Read timeouts count PUTs as well as GETs.
        println!(
            "client={:?} concurrency={} elapsed={:.1}s read_timeouts={} get_MiB/s={:.1}",
            args.client,
            concurrency,
            elapsed.as_secs_f64(),
            read_timeouts(&registry) - read_timeouts_before,
            f64::cast_lossy(total.bytes) / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        );
        println!(
            "  gets: {} errors={}",
            summary(total.latencies),
            total.errors.values().sum::<u64>(),
        );
        println!("  failed gets: {}", summary(total.failed_latencies));
        println!(
            "  puts: {} errors={} stalled(>{:?})={}",
            summary(puts.latencies),
            puts.errors.values().sum::<u64>(),
            args.put_stall,
            puts.stalled,
        );
        for (method, waits) in header_waits.take() {
            println!("  {method} wait for headers: {}", summary(waits));
        }
        for (err, count) in &total.errors {
            println!("  get error {count}x: {err}");
        }
        for (err, count) in &puts.errors {
            println!("  put error {count}x: {err}");
        }
    }
    Ok(())
}

/// Count, p50, p99 and max of `samples`.
fn summary(mut samples: Vec<Duration>) -> String {
    samples.sort();
    let percentile = |p: usize| {
        samples
            .get(samples.len().saturating_sub(1) * p / 100)
            .copied()
            .unwrap_or_default()
    };
    format!(
        "n={} p50={:?} p99={:?} max={:?}",
        samples.len(),
        percentile(50),
        percentile(99),
        percentile(100),
    )
}

/// Per HTTP method, the time from each request reaching the HTTP client until
/// its response head arrives or it fails. Includes waiting for a connection,
/// or for a free stream on a shared HTTP/2 connection. Each SDK retry is a
/// separate request.
#[derive(Debug, Default)]
struct HeaderWaits(Mutex<BTreeMap<String, Vec<Duration>>>);

impl HeaderWaits {
    fn record(&self, method: String, wait: Duration) {
        let mut waits = self.0.lock().expect("lock poisoned");
        waits.entry(method).or_default().push(wait);
    }

    fn take(&self) -> BTreeMap<String, Vec<Duration>> {
        std::mem::take(&mut *self.0.lock().expect("lock poisoned"))
    }
}

/// Wraps an [HttpClient] to record [HeaderWaits].
#[derive(Debug)]
struct TimedHttpClient {
    inner: SharedHttpClient,
    waits: Arc<HeaderWaits>,
}

impl HttpClient for TimedHttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(TimedConnector {
            inner: self.inner.http_connector(settings, components),
            waits: Arc::clone(&self.waits),
        })
    }

    fn validate_base_client_config(
        &self,
        runtime_components: &RuntimeComponentsBuilder,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.inner
            .validate_base_client_config(runtime_components, cfg)
    }

    fn validate_final_config(
        &self,
        runtime_components: &RuntimeComponents,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.inner.validate_final_config(runtime_components, cfg)
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        self.inner.connector_metadata()
    }
}

#[derive(Debug)]
struct TimedConnector {
    inner: SharedHttpConnector,
    waits: Arc<HeaderWaits>,
}

impl HttpConnector for TimedConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let method = request.method().to_owned();
        let waits = Arc::clone(&self.waits);
        let start = Instant::now();
        let response = self.inner.call(request);
        HttpConnectorFuture::new(async move {
            let response = response.await;
            waits.record(method, start.elapsed());
            response
        })
    }
}

/// The SDK's read timeout firings so far: the `mz_persist_s3_read_timeouts`
/// counter, which can't be read from [S3BlobMetrics] outside its crate.
fn read_timeouts(registry: &MetricsRegistry) -> u64 {
    registry
        .gather()
        .iter()
        .find(|family| family.name() == "mz_persist_s3_read_timeouts")
        .map_or(0, |family| {
            u64::cast_lossy(family.get_metric()[0].get_counter().value())
        })
}
