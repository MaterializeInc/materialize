// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Concurrent [S3Blob] reads, plus a steady stream of PUTs through the same
//! client, against an S3-compatible store, comparing HTTP clients that do and
//! don't offer HTTP/2.

use std::collections::BTreeMap;
use std::error::Error as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::bail;
use aws_sdk_s3::config::AsyncSleep;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::{
    RuntimeComponents, RuntimeComponentsBuilder,
};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector as TcpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use mz_ore::cast::CastLossy;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::location::Blob;
use mz_persist::metrics::S3BlobMetrics;
use mz_persist::s3::{S3Blob, S3BlobConfig};
use mz_persist_client::cfg::PersistConfig;

/// Concurrent persist blob reads against an S3-compatible store.
#[derive(Debug, clap::Args)]
pub struct Args {
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
    secret_access_key: String,
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
    /// `mz_aws_util::http_client`, the client production uses.
    Sdk,
    /// This module's hyper client, offering HTTP/1.1 and HTTP/2.
    H2,
    /// This module's hyper client, offering only HTTP/1.1.
    H1,
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
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
    header_waits: &Arc<HeaderWaits>,
) -> Result<S3Blob, anyhow::Error> {
    let knobs =
        PersistConfig::new_default_configs(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone());
    let config = S3BlobConfig::new(
        store.bucket.clone(),
        store.prefix.clone(),
        None,
        Some(store.endpoint.clone()),
        Some(store.region.clone()),
        Some((store.access_key_id.clone(), store.secret_access_key.clone())),
        Box::new(knobs),
        S3BlobMetrics::new(registry),
    )
    .await?;
    let http_client = match client {
        ClientKind::Sdk => SharedHttpClient::new(mz_aws_util::http_client()),
        ClientKind::H2 => SharedHttpClient::new(HyperHttpClient::new(true)?),
        ClientKind::H1 => SharedHttpClient::new(HyperHttpClient::new(false)?),
    };
    let config = config.with_http_client(TimedHttpClient {
        inner: http_client,
        waits: Arc::clone(header_waits),
    });
    Ok(S3Blob::open(config).await?)
}

async fn upload(args: UploadArgs) -> Result<(), anyhow::Error> {
    let blob = open(
        &args.store,
        ClientKind::Sdk,
        &MetricsRegistry::new(),
        &Arc::default(),
    )
    .await?;
    let blob = Arc::new(blob);
    let objects: Vec<_> = args
        .sizes_mib
        .iter()
        .flat_map(|size| (0..args.count_per_size).map(move |i| (format!("{size}mib-{i}"), *size)))
        .collect();
    futures::stream::iter(objects)
        .map(|(key, size_mib)| {
            let blob = Arc::clone(&blob);
            async move {
                let mut data = vec![0; size_mib * 1024 * 1024];
                rand::fill(&mut data[..]);
                blob.set(&key, Bytes::from(data)).await?;
                Ok::<_, anyhow::Error>(key)
            }
        })
        .buffer_unordered(args.concurrency)
        .try_for_each(|key| {
            println!("uploaded {key}");
            futures::future::ready(Ok(()))
        })
        .await
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
    let blob = Arc::new(open(&args.store, args.client, &registry, &header_waits).await?);
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
                        match blob.get(key).await {
                            Ok(Some(value)) => {
                                stats.latencies.push(get_start.elapsed());
                                stats.bytes += u64::try_from(value.len()).expect("fits");
                            }
                            Ok(None) => {
                                stats.failed_latencies.push(get_start.elapsed());
                                *stats.errors.entry(format!("{key} missing")).or_default() += 1
                            }
                            Err(err) => {
                                stats.failed_latencies.push(get_start.elapsed());
                                *stats
                                    .errors
                                    .entry(err.display_with_causes().to_string())
                                    .or_default() += 1
                            }
                        }
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

type HyperRustlsClient = HyperClient<HttpsConnector<TcpConnector>, SdkBody>;

/// An AWS SDK HTTP client on hyper and rustls whose ALPN offer can leave out
/// HTTP/2. The SDK's own client offers HTTP/1.1 and HTTP/2 and has no public
/// way to build it with HTTP/1.1 alone.
///
/// Otherwise it copies the SDK client: aws-lc-rs, native root certificates,
/// `TCP_NODELAY`, hyper's default pool and HTTP/2 settings, and a read timeout
/// from sending the request to receiving the response head. It differs in
/// three ways. It has no separate connect timeout, so a stalled connect fails
/// as a read timeout. It doesn't restrict cipher suites. It doesn't poison a
/// connection after a timeout.
#[derive(Debug)]
struct HyperHttpClient {
    client: HyperRustlsClient,
}

impl HyperHttpClient {
    fn new(http2: bool) -> Result<Self, anyhow::Error> {
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_native_roots()?
        .with_no_client_auth();
        let mut tcp = TcpConnector::new();
        tcp.enforce_http(false);
        tcp.set_nodelay(true);
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1();
        let https = if http2 {
            https.enable_http2().wrap_connector(tcp)
        } else {
            https.wrap_connector(tcp)
        };
        let mut builder = HyperClient::builder(TokioExecutor::new());
        // The pool's idle timeout does nothing without a timer.
        builder.pool_timer(TokioTimer::new());
        Ok(HyperHttpClient {
            client: builder.build(https),
        })
    }
}

impl HttpClient for HyperHttpClient {
    // NOTE: the SDK calls this for every request. Every returned connector
    // must share `self.client`, or each request gets its own connection pool.
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(HyperConnector {
            client: self.client.clone(),
            read_timeout: settings.read_timeout(),
            sleep: components.sleep_impl(),
        })
    }
}

#[derive(Debug)]
struct HyperConnector {
    client: HyperRustlsClient,
    read_timeout: Option<Duration>,
    /// Persist's sleep impl, which maps the timeout marker in `read_timeout`
    /// to the configured duration and counts the timeouts that fire.
    sleep: Option<aws_sdk_s3::config::SharedAsyncSleep>,
}

impl HttpConnector for HyperConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into()))),
        };
        let response = self.client.request(request);
        let timeout = match (self.read_timeout, &self.sleep) {
            (Some(duration), Some(sleep)) => Some(sleep.sleep(duration)),
            _ => None,
        };
        HttpConnectorFuture::new(async move {
            let response = match timeout {
                Some(timeout) => tokio::select! {
                    response = response => response,
                    () = timeout => {
                        return Err(ConnectorError::timeout("HTTP read timeout".into()));
                    }
                },
                None => response.await,
            };
            let response = response.map_err(connector_error)?;
            HttpResponse::try_from(response.map(SdkBody::from_body_1_x))
                .map_err(|err| ConnectorError::other(err.into(), None))
        })
    }
}

/// Classifies errors like the SDK's client does, since the classification
/// decides which requests the SDK retries.
fn connector_error(err: hyper_util::client::legacy::Error) -> ConnectorError {
    let mut sources = std::iter::successors(err.source(), |&err| err.source());
    let retryable = err.is_connect()
        || sources.any(|source| {
            source.is::<std::io::Error>()
                || source.downcast_ref::<hyper::Error>().is_some_and(|err| {
                    err.is_closed() || err.is_canceled() || err.is_incomplete_message()
                })
                || source.downcast_ref::<h2::Error>().is_some_and(|err| {
                    err.is_go_away()
                        || (err.is_reset() && err.reason() == Some(h2::Reason::REFUSED_STREAM))
                })
        });
    if retryable {
        ConnectorError::io(err.into())
    } else {
        ConnectorError::other(err.into(), None)
    }
}
