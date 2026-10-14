// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Concurrent [S3Blob] reads against an S3-compatible store, comparing HTTP
//! clients that do and don't offer HTTP/2.

use std::collections::BTreeMap;
use std::error::Error as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::bail;
use aws_sdk_s3::config::AsyncSleep;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector as TcpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use mz_ore::cast::CastLossy;
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
    /// Reads the test objects with N concurrent readers for each N, and
    /// reports latency, throughput and errors.
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
    #[clap(long, value_delimiter = ',', default_value = "16,64,256,1024")]
    concurrency: Vec<usize>,
    /// Readers start no new get after this long. A round lasts until every
    /// in-flight get finishes.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "30s")]
    duration: Duration,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum ClientKind {
    /// `mz_aws_util::http_client`, the client production uses.
    Sdk,
    /// [HyperHttpClient] offering HTTP/1.1 and HTTP/2.
    H2,
    /// [HyperHttpClient] offering only HTTP/1.1.
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
    let config = match client {
        ClientKind::Sdk => config,
        ClientKind::H2 => config.with_http_client(HyperHttpClient::new(true)?),
        ClientKind::H1 => config.with_http_client(HyperHttpClient::new(false)?),
    };
    Ok(S3Blob::open(config).await?)
}

async fn upload(args: UploadArgs) -> Result<(), anyhow::Error> {
    let blob = Arc::new(open(&args.store, ClientKind::Sdk, &MetricsRegistry::new()).await?);
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
    bytes: u64,
    errors: BTreeMap<String, u64>,
}

async fn read(args: ReadArgs) -> Result<(), anyhow::Error> {
    let registry = MetricsRegistry::new();
    let blob = Arc::new(open(&args.store, args.client, &registry).await?);
    let mut keys = Vec::new();
    blob.list_keys_and_metadata("", &mut |meta| keys.push(meta.key.to_owned()))
        .await?;
    if keys.is_empty() {
        bail!("no objects under prefix {}", args.store.prefix);
    }
    let keys = Arc::new(keys);

    for &concurrency in &args.concurrency {
        let read_timeouts_before = read_timeouts(&registry);
        let next_key = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();
        let deadline = start + args.duration;
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
                                *stats.errors.entry(format!("{key} missing")).or_default() += 1
                            }
                            Err(err) => *stats.errors.entry(err.to_string()).or_default() += 1,
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
            total.bytes += stats.bytes;
            for (err, count) in stats.errors {
                *total.errors.entry(err).or_default() += count;
            }
        }
        let elapsed = start.elapsed();
        total.latencies.sort();
        let percentile = |p: usize| {
            total
                .latencies
                .get(total.latencies.len().saturating_sub(1) * p / 100)
                .copied()
                .unwrap_or_default()
        };
        println!(
            "client={:?} concurrency={} elapsed={:.1}s gets={} errors={} read_timeouts={} \
             p50={:?} p99={:?} max={:?} MiB/s={:.1}",
            args.client,
            concurrency,
            elapsed.as_secs_f64(),
            total.latencies.len(),
            total.errors.values().sum::<u64>(),
            read_timeouts(&registry) - read_timeouts_before,
            percentile(50),
            percentile(99),
            percentile(100),
            f64::cast_lossy(total.bytes) / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        );
        for (err, count) in &total.errors {
            println!("  {count}x {err}");
        }
    }
    Ok(())
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
