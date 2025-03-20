// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Manages a single Materialize environment.
//!
//! It listens for SQL connections on port 6875 (MTRL) and for HTTP connections
//! on port 6876.

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use jsonwebtoken::DecodingKey;
use mz_balancerd::{
    BalancerConfig, BalancerService, CancellationResolver, FronteggResolver, Resolver, BUILD_INFO,
};
use mz_frontegg_auth::{
    Authenticator, AuthenticatorConfig, DEFAULT_REFRESH_DROP_FACTOR,
    DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingHandle;
use mz_server_core::TlsCliArgs;
use tracing::{info_span, warn, Instrument};

#[derive(Debug, clap::Parser)]
#[clap(about = "Balancer service", long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,

    #[clap(flatten)]
    tracing: TracingCliArgs,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    Service(ServiceArgs),
}

#[derive(Debug, clap::Parser)]
pub struct ServiceArgs {
    #[clap(long, value_name = "HOST:PORT")]
    pgwire_listen_addr: SocketAddr,
    #[clap(long, value_name = "HOST:PORT")]
    https_listen_addr: SocketAddr,
    #[clap(flatten)]
    tls: TlsCliArgs,
    #[clap(long, value_name = "HOST:PORT")]
    internal_http_listen_addr: SocketAddr,

    /// Whether to initiate internal connections over TLS
    #[clap(long)]
    internal_tls: bool,
    /// Static pgwire resolver address to use for local testing.
    #[clap(
        long,
        value_name = "HOST:PORT",
        conflicts_with = "frontegg_resolver_template"
    )]
    static_resolver_addr: Option<String>,
    /// Frontegg resolver address template. `{}` is replaced with the user's frontegg tenant id to
    /// get a DNS address. The first IP that address resolves to is the proxy destinations.
    #[clap(long,
        value_name = "HOST.{}.NAME:PORT",
        requires_all = &["frontegg_api_token_url", "frontegg_admin_role"],
    )]
    frontegg_resolver_template: Option<String>,
    /// HTTPS resolver address template. `{}` is replaced with the first subdomain of the HTTPS SNI
    /// host address to get a DNS address. The first IP that address resolves to is the proxy
    /// destinations.
    #[clap(long, value_name = "HOST.{}.NAME:PORT")]
    https_resolver_template: String,
    /// Cancellation resolver configmap directory. The org id part of the incoming connection id
    /// (the 12 bits after (and excluding) the first bit) converted to a 3-char UUID string is
    /// appended to this to make a file path. That file is read, and every newline-delimited line
    /// there is DNS resolved, and all returned IPs get a mirrored cancellation request. The lines
    /// in the file must be of the form `host:port`.
    #[clap(
        long,
        value_name = "/path/to/configmap/dir/",
        required_unless_present = "static_resolver_addr"
    )]
    cancellation_resolver_dir: Option<PathBuf>,

    /// JWK used to validate JWTs during Frontegg authentication as a PEM public
    /// key. Can optionally be base64 encoded with the URL-safe alphabet.
    #[clap(long, env = "FRONTEGG_JWK", requires = "frontegg_resolver_template")]
    frontegg_jwk: Option<String>,
    /// Path of JWK used to validate JWTs during Frontegg authentication as a PEM public key.
    #[clap(
        long,
        env = "FRONTEGG_JWK_FILE",
        requires = "frontegg_resolver_template"
    )]
    frontegg_jwk_file: Option<PathBuf>,
    /// The full URL (including path) to the Frontegg api-token endpoint.
    #[clap(
        long,
        env = "FRONTEGG_API_TOKEN_URL",
        requires = "frontegg_resolver_template"
    )]
    frontegg_api_token_url: Option<String>,
    /// The name of the admin role in Frontegg.
    #[clap(
        long,
        env = "FRONTEGG_ADMIN_ROLE",
        requires = "frontegg_resolver_template"
    )]
    frontegg_admin_role: Option<String>,

    /// An SDK key for LaunchDarkly.
    ///
    /// Setting this will enable synchronization of LaunchDarkly features.
    #[clap(long, env = "LAUNCHDARKLY_SDK_KEY")]
    launchdarkly_sdk_key: Option<String>,
    /// The duration at which the LaunchDarkly synchronization times out during startup.
    #[clap(
        long,
        env = "CONFIG_SYNC_TIMEOUT",
        value_parser = humantime::parse_duration,
        default_value = "30s"
    )]
    config_sync_timeout: Duration,
    /// The interval in seconds at which to synchronize LaunchDarkly values.
    ///
    /// If this is not explicitly set, the loop that synchronizes LaunchDarkly will not run _even if
    /// [`Self::launchdarkly_sdk_key`] is present_ (however one initial sync is always run).
    #[clap(
        long,
        env = "CONFIG_SYNC_LOOP_INTERVAL",
        value_parser = humantime::parse_duration,
    )]
    config_sync_loop_interval: Option<Duration>,

    /// The cloud provider where the balancer is running.
    #[clap(long, env = "CLOUD_PROVIDER")]
    cloud_provider: Option<String>,
    /// The cloud provider region where the balancer is running.
    #[clap(long, env = "CLOUD_PROVIDER_REGION")]
    cloud_provider_region: Option<String>,
    /// Set startup defaults for dynconfig
    #[clap(long, value_parser = parse_key_val::<String, String>, value_delimiter = ',')]
    default_config: Option<Vec<(String, String)>>,
}

fn main() {
    let args: Args = cli::parse_args(CliConfig::default());

    // Mirror the tokio Runtime configuration in our production binaries.
    let ncpus_useful = usize::max(1, std::cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ncpus_useful)
        .enable_all()
        .build()
        .expect("Failed building the Runtime");

    let metrics_registry = MetricsRegistry::new();
    let (tracing_handle, _tracing_guard) = runtime
        .block_on(args.tracing.configure_tracing(
            StaticTracingConfig {
                service_name: "balancerd",
                build_info: BUILD_INFO,
            },
            metrics_registry.clone(),
        ))
        .expect("failed to init tracing");

    runtime.block_on(mz_alloc::register_metrics_into(&metrics_registry));

    let root_span = info_span!("balancer");
    let res = match args.command {
        Command::Service(args) => runtime.block_on(run(args, tracing_handle).instrument(root_span)),
    };

    if let Err(err) = res {
        panic!("balancer: fatal: {}", err.display_with_causes());
    }
    drop(_tracing_guard);
}

pub async fn run(args: ServiceArgs, tracing_handle: TracingHandle) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    let (resolver, cancellation_resolver) =
        match (args.static_resolver_addr, args.frontegg_resolver_template) {
            (None, Some(addr_template)) => {
                let auth = Authenticator::new(
                    AuthenticatorConfig {
                        admin_api_token_url: args.frontegg_api_token_url.expect("clap enforced"),
                        decoding_key: match (args.frontegg_jwk, args.frontegg_jwk_file) {
                            (None, Some(path)) => {
                                let jwk = std::fs::read(&path).with_context(|| {
                                    format!("read {path:?} for --frontegg-jwk-file")
                                })?;
                                DecodingKey::from_rsa_pem(&jwk)?
                            }
                            (Some(jwk), None) => DecodingKey::from_rsa_pem(jwk.as_bytes())?,
                            _ => anyhow::bail!(
                            "exactly one of --frontegg-jwk or --frontegg-jwk-file must be present"
                        ),
                        },
                        tenant_id: None,
                        now: mz_ore::now::SYSTEM_TIME.clone(),
                        admin_role: args.frontegg_admin_role.expect("clap enforced"),
                        refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
                        refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
                    },
                    mz_frontegg_auth::Client::environmentd_default(),
                    &metrics_registry,
                );
                let cancellation_resolver_dir = args
                    .cancellation_resolver_dir
                    .expect("required unless static resolver present");
                if !cancellation_resolver_dir.is_dir() {
                    anyhow::bail!("{cancellation_resolver_dir:?} is not a directory");
                }
                (
                    Resolver::Frontegg(FronteggResolver {
                        auth,
                        addr_template,
                    }),
                    CancellationResolver::Directory(cancellation_resolver_dir),
                )
            }
            (Some(addr), None) => {
                // As a typo-check, verify that the passed address resolves to at least one IP. This
                // result isn't recorded anywhere: we re-resolve on each request in case DNS changes.
                // Here only to cause startup to crash if mis-typed.
                let mut addrs = tokio::net::lookup_host(&addr)
                    .await
                    .unwrap_or_else(|_| panic!("could not resolve {addr}"));
                let Some(_resolved) = addrs.next() else {
                    panic!("{addr} did not resolve to any addresses");
                };
                drop(addrs);

                (
                    Resolver::Static(addr.clone()),
                    CancellationResolver::Static(addr),
                )
            }
            _ => anyhow::bail!(
            "exactly one of --static-resolver-addr or --frontegg-resolver-template must be present"
        ),
        };
    let config = BalancerConfig::new(
        &BUILD_INFO,
        args.internal_http_listen_addr,
        args.pgwire_listen_addr,
        args.https_listen_addr,
        cancellation_resolver,
        resolver,
        args.https_resolver_template,
        args.tls.into_config()?,
        args.internal_tls,
        metrics_registry,
        mz_server_core::default_cert_reload_ticker(),
        args.launchdarkly_sdk_key,
        args.config_sync_timeout,
        args.config_sync_loop_interval,
        args.cloud_provider,
        args.cloud_provider_region,
        tracing_handle,
        args.default_config.unwrap_or(vec![]),
    );
    let service = BalancerService::new(config).await?;
    service.serve().await?;
    warn!("balancer service exited");
    Ok(())
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}
