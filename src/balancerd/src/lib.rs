// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The balancerd service is a horizontally scalable, stateless, multi-tenant ingress router for
//! pgwire and HTTPS connections.
//!
//! It listens on pgwire and HTTPS ports. When a new pgwire connection starts, the requested user is
//! authenticated with frontegg from which a tenant id is returned. From that a target internal
//! hostname is resolved to an IP address, and the connection is proxied to that address which has a
//! running environmentd's pgwire port. When a new HTTPS connection starts, its SNI hostname is used
//! to generate an internal hostname that is resolved to an IP address, which is similarly proxied.

mod codec;
mod dyncfgs;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use axum::response::IntoResponse;
use axum::{Router, routing};
use bytes::BytesMut;
use domain::base::{Name, Rtype};
use domain::rdata::AllRecordData;
use domain::resolv::StubResolver;
use futures::TryFutureExt;
use futures::stream::BoxStream;
use hyper::StatusCode;
use hyper_util::rt::TokioIo;
use launchdarkly_server_sdk as ld;
use mz_build_info::{BuildInfo, build_info};
use mz_dyncfg::ConfigSet;
use mz_frontegg_auth::Authenticator as FronteggAuthentication;
use mz_ore::cast::CastFrom;
use mz_ore::id_gen::conn_id_org_uuid;
use mz_ore::metrics::{ComputedGauge, IntCounter, IntGauge, MetricsRegistry};
use mz_ore::netio::AsyncReady;
use mz_ore::now::{NowFn, SYSTEM_TIME, epoch_to_uuid_v7};
use mz_ore::task::{JoinSetExt, spawn};
use mz_ore::tracing::TracingHandle;
use mz_ore::{metric, netio};
use mz_pgwire_common::{
    ACCEPT_SSL_ENCRYPTION, CONN_UUID_KEY, Conn, ErrorResponse, FrontendMessage,
    FrontendStartupMessage, MZ_FORWARDED_FOR_KEY, REJECT_ENCRYPTION, VERSION_3, decode_startup,
};
use mz_server_core::{
    Connection, ConnectionStream, ListenerHandle, ReloadTrigger, ReloadingSslContext,
    ReloadingTlsConfig, ServeConfig, ServeDyncfg, TlsCertConfig, TlsMode, listen,
};
use openssl::ssl::{NameType, Ssl, SslConnector, SslMethod, SslVerifyMode};
use prometheus::{IntCounterVec, IntGaugeVec};
use proxy_header::{ProxiedAddress, ProxyHeader};
use semver::Version;
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_metrics::TaskMetrics;
use tokio_openssl::SslStream;
use tokio_postgres::error::SqlState;
use tower::Service;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::codec::{BackendMessage, FramedConn};
use crate::dyncfgs::{
    INJECT_PROXY_PROTOCOL_HEADER_HTTP, SIGTERM_CONNECTION_WAIT, SIGTERM_LISTEN_WAIT,
    has_tracing_config_update, tracing_config,
};

/// Balancer build information.
pub const BUILD_INFO: BuildInfo = build_info!();

pub struct BalancerConfig {
    /// Info about which version of the code is running.
    build_version: Version,
    /// Listen address for internal HTTP health and metrics server.
    internal_http_listen_addr: SocketAddr,
    /// Listen address for pgwire connections.
    pgwire_listen_addr: SocketAddr,
    /// Listen address for HTTPS connections.
    https_listen_addr: SocketAddr,
    /// DNS resolver for pgwire cancellation requests
    cancellation_resolver: CancellationResolver,
    /// DNS resolver.
    resolver: Resolver,
    https_sni_addr_template: String,
    tls: Option<TlsCertConfig>,
    internal_tls: bool,
    metrics_registry: MetricsRegistry,
    reload_certs: BoxStream<'static, Option<oneshot::Sender<Result<(), anyhow::Error>>>>,
    launchdarkly_sdk_key: Option<String>,
    config_sync_file_path: Option<PathBuf>,
    config_sync_timeout: Duration,
    config_sync_loop_interval: Option<Duration>,
    cloud_provider: Option<String>,
    cloud_provider_region: Option<String>,
    tracing_handle: TracingHandle,
    default_configs: Vec<(String, String)>,
}

impl BalancerConfig {
    pub fn new(
        build_info: &BuildInfo,
        internal_http_listen_addr: SocketAddr,
        pgwire_listen_addr: SocketAddr,
        https_listen_addr: SocketAddr,
        cancellation_resolver: CancellationResolver,
        resolver: Resolver,
        https_sni_addr_template: String,
        tls: Option<TlsCertConfig>,
        internal_tls: bool,
        metrics_registry: MetricsRegistry,
        reload_certs: ReloadTrigger,
        launchdarkly_sdk_key: Option<String>,
        config_sync_file: Option<PathBuf>,
        config_sync_timeout: Duration,
        config_sync_loop_interval: Option<Duration>,
        cloud_provider: Option<String>,
        cloud_provider_region: Option<String>,
        tracing_handle: TracingHandle,
        default_configs: Vec<(String, String)>,
    ) -> Self {
        Self {
            build_version: build_info.semver_version(),
            internal_http_listen_addr,
            pgwire_listen_addr,
            https_listen_addr,
            cancellation_resolver,
            resolver,
            https_sni_addr_template,
            tls,
            internal_tls,
            metrics_registry,
            reload_certs,
            launchdarkly_sdk_key,
            config_sync_file_path: config_sync_file,
            config_sync_timeout,
            config_sync_loop_interval,
            cloud_provider,
            cloud_provider_region,
            tracing_handle,
            default_configs,
        }
    }
}

/// Prometheus monitoring metrics.
#[derive(Debug)]
pub struct BalancerMetrics {
    _uptime: ComputedGauge,
}

impl BalancerMetrics {
    /// Returns a new [BalancerMetrics] instance connected to the registry in cfg.
    pub fn new(cfg: &BalancerConfig) -> Self {
        let start = Instant::now();
        let uptime = cfg.metrics_registry.register_computed_gauge(
            metric!(
                name: "mz_balancer_metadata_seconds",
                help: "server uptime, labels are build metadata",
                const_labels: {
                    "version" => cfg.build_version,
                    "build_type" => if cfg!(release) { "release" } else { "debug" }
                },
            ),
            move || start.elapsed().as_secs_f64(),
        );
        BalancerMetrics { _uptime: uptime }
    }
}

pub struct BalancerService {
    cfg: BalancerConfig,
    pub pgwire: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
    pub https: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
    pub internal_http: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
    _metrics: BalancerMetrics,
    configs: ConfigSet,
}

impl BalancerService {
    pub async fn new(cfg: BalancerConfig) -> Result<Self, anyhow::Error> {
        let pgwire = listen(&cfg.pgwire_listen_addr).await?;
        let https = listen(&cfg.https_listen_addr).await?;
        let internal_http = listen(&cfg.internal_http_listen_addr).await?;
        let metrics = BalancerMetrics::new(&cfg);
        let mut configs = ConfigSet::default();
        configs = dyncfgs::all_dyncfgs(configs);
        dyncfgs::set_defaults(&configs, cfg.default_configs.clone())?;
        let tracing_handle = cfg.tracing_handle.clone();
        // Configure dyncfg sync
        match (
            cfg.launchdarkly_sdk_key.as_deref(),
            cfg.config_sync_file_path.as_deref(),
        ) {
            (Some(key), None) => {
                mz_dyncfg_launchdarkly::sync_launchdarkly_to_configset(
                    configs.clone(),
                    &BUILD_INFO,
                    |builder| {
                        let region = cfg
                            .cloud_provider_region
                            .clone()
                            .unwrap_or_else(|| String::from("unknown"));
                        if let Some(provider) = cfg.cloud_provider.clone() {
                            builder.add_context(
                                ld::ContextBuilder::new(format!(
                                    "{}/{}/{}",
                                    provider, region, cfg.build_version
                                ))
                                .kind("balancer")
                                .set_string("provider", provider)
                                .set_string("region", region)
                                .set_string("version", cfg.build_version.to_string())
                                .build()
                                .map_err(|e| anyhow::anyhow!(e))?,
                            );
                        } else {
                            builder.add_context(
                                ld::ContextBuilder::new(format!(
                                    "{}/{}/{}",
                                    "unknown", region, cfg.build_version
                                ))
                                .anonymous(true) // exclude this user from the dashboard
                                .kind("balancer")
                                .set_string("provider", "unknown")
                                .set_string("region", region)
                                .set_string("version", cfg.build_version.to_string())
                                .build()
                                .map_err(|e| anyhow::anyhow!(e))?,
                            );
                        }
                        Ok(())
                    },
                    Some(key),
                    cfg.config_sync_timeout,
                    cfg.config_sync_loop_interval,
                    move |updates, configs| {
                        if has_tracing_config_update(updates) {
                            match tracing_config(configs) {
                                Ok(parameters) => parameters.apply(&tracing_handle),
                                Err(err) => warn!("unable to update tracing: {err}"),
                            }
                        }
                    },
                )
                .await
                .inspect_err(|e| warn!("LaunchDarkly sync error: {e}"))
                .ok();
            }
            (None, Some(path)) => {
                mz_dyncfg_file::sync_file_to_configset(
                    configs.clone(),
                    path,
                    cfg.config_sync_timeout,
                    cfg.config_sync_loop_interval,
                    move |updates, configs| {
                        if has_tracing_config_update(updates) {
                            match tracing_config(configs) {
                                Ok(parameters) => parameters.apply(&tracing_handle),
                                Err(err) => warn!("unable to update tracing: {err}"),
                            }
                        }
                    },
                )
                .await
                // If there's an Error, log but continue anyway. If LD is down
                // we have no way of fetching the previous value of the flag
                // (unlike the adapter, but it has a durable catalog). The
                // ConfigSet defaults have been chosen to be good enough if this
                // is the case.
                .inspect_err(|e| warn!("File config sync error: {e}"))
                .ok();
            }
            (Some(_), Some(_)) => panic!(
                "must provide either config_sync_file_path or launchdarkly_sdk_key for config syncing",
            ),
            (None, None) => {}
        };
        Ok(Self {
            cfg,
            pgwire,
            https,
            internal_http,
            _metrics: metrics,
            configs,
        })
    }

    pub async fn serve(self) -> Result<(), anyhow::Error> {
        let (pgwire_tls, https_tls) = match &self.cfg.tls {
            Some(tls) => {
                let context = tls.reloading_context(self.cfg.reload_certs)?;
                (
                    Some(ReloadingTlsConfig {
                        context: context.clone(),
                        mode: TlsMode::Require,
                    }),
                    Some(context),
                )
            }
            None => (None, None),
        };

        let metrics = ServerMetricsConfig::register_into(&self.cfg.metrics_registry);

        let mut set = JoinSet::new();
        let mut server_handles = Vec::new();
        let pgwire_addr = self.pgwire.0.local_addr();
        let https_addr = self.https.0.local_addr();
        let internal_http_addr = self.internal_http.0.local_addr();

        {
            let pgwire = PgwireBalancer {
                resolver: Arc::new(self.cfg.resolver),
                cancellation_resolver: Arc::new(self.cfg.cancellation_resolver),
                tls: pgwire_tls,
                internal_tls: self.cfg.internal_tls,
                metrics: ServerMetrics::new(metrics.clone(), "pgwire"),
                now: SYSTEM_TIME.clone(),
            };
            let (handle, stream) = self.pgwire;
            server_handles.push(handle);
            set.spawn_named(|| "pgwire_stream", {
                let config_set = self.configs.clone();
                async move {
                    mz_server_core::serve(ServeConfig {
                        server: pgwire,
                        conns: stream,
                        dyncfg: Some(ServeDyncfg {
                            config_set,
                            sigterm_wait_config: &SIGTERM_CONNECTION_WAIT,
                        }),
                    })
                    .await;
                    warn!("pgwire server exited");
                }
            });
        }
        {
            let Some((addr, port)) = self.cfg.https_sni_addr_template.split_once(':') else {
                panic!("expected port in https_addr_template");
            };
            let port: u16 = port.parse().expect("unexpected port");
            let resolver = StubResolver::new();
            let https = HttpsBalancer {
                resolver: Arc::from(resolver),
                tls: https_tls,
                resolve_template: Arc::from(addr),
                port,
                metrics: Arc::from(ServerMetrics::new(metrics, "https")),
                configs: self.configs.clone(),
                internal_tls: self.cfg.internal_tls,
            };
            let (handle, stream) = self.https;
            server_handles.push(handle);
            set.spawn_named(|| "https_stream", {
                let config_set = self.configs.clone();
                async move {
                    mz_server_core::serve(ServeConfig {
                        server: https,
                        conns: stream,
                        dyncfg: Some(ServeDyncfg {
                            config_set,
                            sigterm_wait_config: &SIGTERM_CONNECTION_WAIT,
                        }),
                    })
                    .await;
                    warn!("https server exited");
                }
            });
        }
        {
            let router = Router::new()
                .route(
                    "/metrics",
                    routing::get(move || async move {
                        mz_http_util::handle_prometheus(&self.cfg.metrics_registry).await
                    }),
                )
                .route(
                    "/api/livez",
                    routing::get(mz_http_util::handle_liveness_check),
                )
                .route("/api/readyz", routing::get(handle_readiness_check));
            let internal_http = InternalHttpServer { router };
            let (handle, stream) = self.internal_http;
            server_handles.push(handle);
            set.spawn_named(|| "internal_http_stream", async move {
                mz_server_core::serve(ServeConfig {
                    server: internal_http,
                    conns: stream,
                    // Disable graceful termination because our internal
                    // monitoring keeps persistent HTTP connections open.
                    dyncfg: None,
                })
                .await;
                warn!("internal_http server exited");
            });
        }
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            set.spawn_named(|| "sigterm_handler", async move {
                sigterm.recv().await;
                let wait = SIGTERM_LISTEN_WAIT.get(&self.configs);
                warn!("received signal TERM - delaying for {:?}!", wait);
                tokio::time::sleep(wait).await;
                warn!("sigterm delay complete, dropping server handles");
                drop(server_handles);
            });
        }

        println!("balancerd {} listening...", BUILD_INFO.human_version(None));
        println!(" TLS enabled: {}", self.cfg.tls.is_some());
        println!(" pgwire address: {}", pgwire_addr);
        println!(" HTTPS address: {}", https_addr);
        println!(" internal HTTP address: {}", internal_http_addr);

        // Wait for all tasks to exit, which can happen on SIGTERM.
        while let Some(res) = set.join_next().await {
            if let Err(err) = res {
                error!("serving task failed: {err}")
            }
        }
        Ok(())
    }
}

#[allow(clippy::unused_async)]
async fn handle_readiness_check() -> impl IntoResponse {
    (StatusCode::OK, "ready")
}

struct InternalHttpServer {
    router: Router,
}

impl mz_server_core::Server for InternalHttpServer {
    const NAME: &'static str = "internal_http";

    // TODO(jkosh44) consider forwarding the connection UUID to the adapter.
    fn handle_connection(
        &self,
        conn: Connection,
        _tokio_metrics_intervals: impl Iterator<Item = TaskMetrics> + Send + 'static,
    ) -> mz_server_core::ConnectionHandler {
        let router = self.router.clone();
        let service = hyper::service::service_fn(move |req| router.clone().call(req));
        let conn = TokioIo::new(conn);

        Box::pin(async {
            let http = hyper::server::conn::http1::Builder::new();
            http.serve_connection(conn, service).err_into().await
        })
    }
}

/// Wraps an IntGauge and automatically `inc`s on init and `drop`s on drop. Callers should not call
/// `inc().`. Useful for handling multiple task exit points, for example in the case of a panic.
struct GaugeGuard {
    gauge: IntGauge,
}

impl From<IntGauge> for GaugeGuard {
    fn from(gauge: IntGauge) -> Self {
        let _self = Self { gauge };
        _self.gauge.inc();
        _self
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

#[derive(Clone, Debug)]
struct ServerMetricsConfig {
    connection_status: IntCounterVec,
    active_connections: IntGaugeVec,
    tenant_connections: IntGaugeVec,
    tenant_connection_rx: IntCounterVec,
    tenant_connection_tx: IntCounterVec,
    tenant_pgwire_sni_count: IntCounterVec,
}

impl ServerMetricsConfig {
    fn register_into(registry: &MetricsRegistry) -> Self {
        let connection_status = registry.register(metric!(
            name: "mz_balancer_connection_status",
            help: "Count of completed network connections, by status",
            var_labels: ["source", "status"],
        ));
        let active_connections = registry.register(metric!(
            name: "mz_balancer_connection_active",
            help: "Count of currently open network connections.",
            var_labels: ["source"],
        ));
        let tenant_connections = registry.register(metric!(
            name: "mz_balancer_tenant_connection_active",
            help: "Count of opened network connections by tenant.",
            var_labels: ["source",  "tenant"]
        ));
        let tenant_connection_rx = registry.register(metric!(
            name: "mz_balancer_tenant_connection_rx",
            help: "Number of bytes received from a client for a tenant.",
            var_labels: ["source", "tenant"],
        ));
        let tenant_connection_tx = registry.register(metric!(
            name: "mz_balancer_tenant_connection_tx",
            help: "Number of bytes sent to a client for a tenant.",
            var_labels: ["source", "tenant"],
        ));
        let tenant_pgwire_sni_count = registry.register(metric!(
            name: "mz_balancer_tenant_pgwire_sni_count",
            help: "Count of pgwire connections that have and do not have SNI available per tenant.",
            var_labels: ["tenant", "has_sni"],
        ));
        Self {
            connection_status,
            active_connections,
            tenant_connections,
            tenant_connection_rx,
            tenant_connection_tx,
            tenant_pgwire_sni_count,
        }
    }
}

#[derive(Clone, Debug)]
struct ServerMetrics {
    inner: ServerMetricsConfig,
    source: &'static str,
}

impl ServerMetrics {
    fn new(inner: ServerMetricsConfig, source: &'static str) -> Self {
        let self_ = Self { inner, source };

        // Pre-initialize labels we are planning to use to ensure they are all always emitted as
        // time series.
        self_.connection_status(false);
        self_.connection_status(true);
        drop(self_.active_connections());

        self_
    }

    fn connection_status(&self, is_ok: bool) -> IntCounter {
        self.inner
            .connection_status
            .with_label_values(&[self.source, Self::status_label(is_ok)])
    }

    fn active_connections(&self) -> GaugeGuard {
        self.inner
            .active_connections
            .with_label_values(&[self.source])
            .into()
    }

    fn tenant_connections(&self, tenant: &str) -> GaugeGuard {
        self.inner
            .tenant_connections
            .with_label_values(&[self.source, tenant])
            .into()
    }

    fn tenant_connections_rx(&self, tenant: &str) -> IntCounter {
        self.inner
            .tenant_connection_rx
            .with_label_values(&[self.source, tenant])
    }

    fn tenant_connections_tx(&self, tenant: &str) -> IntCounter {
        self.inner
            .tenant_connection_tx
            .with_label_values(&[self.source, tenant])
    }

    fn tenant_pgwire_sni_count(&self, tenant: &str, has_sni: bool) -> IntCounter {
        self.inner
            .tenant_pgwire_sni_count
            .with_label_values(&[tenant, &has_sni.to_string()])
    }

    fn status_label(is_ok: bool) -> &'static str {
        if is_ok { "success" } else { "error" }
    }
}

pub enum CancellationResolver {
    Directory(PathBuf),
    Static(String),
}

struct PgwireBalancer {
    tls: Option<ReloadingTlsConfig>,
    internal_tls: bool,
    cancellation_resolver: Arc<CancellationResolver>,
    resolver: Arc<Resolver>,
    metrics: ServerMetrics,
    now: NowFn,
}

impl PgwireBalancer {
    #[mz_ore::instrument(level = "debug")]
    async fn run<'a, A>(
        conn: &'a mut FramedConn<A>,
        version: i32,
        params: BTreeMap<String, String>,
        resolver: &Resolver,
        tls_mode: Option<TlsMode>,
        internal_tls: bool,
        metrics: &ServerMetrics,
    ) -> Result<(), io::Error>
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin,
    {
        if version != VERSION_3 {
            return conn
                .send(ErrorResponse::fatal(
                    SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                    "server does not support the client's requested protocol version",
                ))
                .await;
        }

        let Some(user) = params.get("user") else {
            return conn
                .send(ErrorResponse::fatal(
                    SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                    "user parameter required",
                ))
                .await;
        };

        if let Err(err) = conn.inner().ensure_tls_compatibility(&tls_mode) {
            return conn.send(err).await;
        }

        let resolved = match resolver.resolve(conn, user, metrics).await {
            Ok(v) => v,
            Err(err) => {
                return conn
                    .send(ErrorResponse::fatal(
                        SqlState::INVALID_PASSWORD,
                        err.to_string(),
                    ))
                    .await;
            }
        };

        let _active_guard = resolved
            .tenant
            .as_ref()
            .map(|tenant| metrics.tenant_connections(tenant));
        let Ok(mut mz_stream) =
            Self::init_stream(conn, resolved.addr, resolved.password, params, internal_tls).await
        else {
            return Ok(());
        };

        let mut client_counter = CountingConn::new(conn.inner_mut());

        // Now blindly shuffle bytes back and forth until closed.
        // TODO: Limit total memory use.
        let res = tokio::io::copy_bidirectional(&mut client_counter, &mut mz_stream).await;
        if let Some(tenant) = &resolved.tenant {
            metrics
                .tenant_connections_tx(tenant)
                .inc_by(u64::cast_from(client_counter.written));
            metrics
                .tenant_connections_rx(tenant)
                .inc_by(u64::cast_from(client_counter.read));
        }
        res?;

        Ok(())
    }

    #[mz_ore::instrument(level = "debug")]
    async fn init_stream<'a, A>(
        conn: &'a mut FramedConn<A>,
        envd_addr: SocketAddr,
        password: Option<String>,
        params: BTreeMap<String, String>,
        internal_tls: bool,
    ) -> Result<Conn<TcpStream>, anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin,
    {
        let mut mz_stream = TcpStream::connect(envd_addr).await?;
        let mut buf = BytesMut::new();

        let mut mz_stream = if internal_tls {
            FrontendStartupMessage::SslRequest.encode(&mut buf)?;
            mz_stream.write_all(&buf).await?;
            buf.clear();
            let mut maybe_ssl_request_response = [0u8; 1];
            let nread =
                netio::read_exact_or_eof(&mut mz_stream, &mut maybe_ssl_request_response).await?;
            if nread == 1 && maybe_ssl_request_response == [ACCEPT_SSL_ENCRYPTION] {
                // do a TLS handshake
                let mut builder =
                    SslConnector::builder(SslMethod::tls()).expect("Error creating builder.");
                // environmentd doesn't yet have a cert we trust, so for now disable verification.
                builder.set_verify(SslVerifyMode::NONE);
                let mut ssl = builder
                    .build()
                    .configure()?
                    .into_ssl(&envd_addr.to_string())?;
                ssl.set_connect_state();
                Conn::Ssl(SslStream::new(ssl, mz_stream)?)
            } else {
                Conn::Unencrypted(mz_stream)
            }
        } else {
            Conn::Unencrypted(mz_stream)
        };

        // Send initial startup and password messages.
        let startup = FrontendStartupMessage::Startup {
            version: VERSION_3,
            params,
        };
        startup.encode(&mut buf)?;
        mz_stream.write_all(&buf).await?;
        let client_stream = conn.inner_mut();

        // This early return is important in self managed with SASL mode.
        // The below code specifically looks for cleartext password requests, but in SASL mode
        // the server will send a different message type (SASLInitialResponse) that we should
        // not try to interpret or respond to.
        // "Why not? That code looks like it should fall back fine?" You may ask.
        // The below block unconditionally reads 9 bytes from the server. If we don't have
        // a password or the message isn't a cleartext password request, we forward those 9 bytes
        // to the client. Then we return the stream to the caller, who will continue shuffling bytes.
        // The problem is that with TLS enabled between balancerd <-> client, flushing the first 9 bytes
        // before copying bidirectionally will have the side effect of splitting the auth handshake into
        // two SSL records. Pgbouncer misbehaves in this scenario, and fails the connection.
        // PGbouncer shouldn't do this! It's a common footgun of protocols over TLS.
        // So common in fact that PGbouncer already hit and fixed this issue on the bouncer <-> client side:
        // once before: https://github.com/pgbouncer/pgbouncer/pull/1058.
        // We will work to upstream a fix, but in the meantime, this early return avoids the issue entirely.
        if password.is_none() {
            return Ok(mz_stream);
        }

        // Read a single backend message, which may be a password request. Send ours if so.
        // Otherwise start shuffling bytes. message type (len 1, 'R') + message len (len 4, 8_i32) +
        // auth type (len 4, 3_i32).
        let mut maybe_auth_frame = [0; 1 + 4 + 4];
        let nread = netio::read_exact_or_eof(&mut mz_stream, &mut maybe_auth_frame).await?;
        // 'R' for auth message, 0008 for message length, 0003 for password cleartext variant.
        // See: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-AUTHENTICATIONCLEARTEXTPASSWORD
        const AUTH_PASSWORD_CLEARTEXT: [u8; 9] = [b'R', 0, 0, 0, 8, 0, 0, 0, 3];
        if nread == AUTH_PASSWORD_CLEARTEXT.len()
            && maybe_auth_frame == AUTH_PASSWORD_CLEARTEXT
            && password.is_some()
        {
            // If we got exactly a cleartext password request and have one, send it.
            let Some(password) = password else {
                unreachable!("verified some above");
            };
            let password = FrontendMessage::Password { password };
            buf.clear();
            password.encode(&mut buf)?;
            mz_stream.write_all(&buf).await?;
            mz_stream.flush().await?;
        } else {
            // Otherwise pass on the bytes we just got. This *might* even be a password request, but
            // we don't have a password. In which case it can be forwarded up to the client.
            client_stream.write_all(&maybe_auth_frame[0..nread]).await?;
        }

        Ok(mz_stream)
    }
}

impl mz_server_core::Server for PgwireBalancer {
    const NAME: &'static str = "pgwire_balancer";

    fn handle_connection(
        &self,
        conn: Connection,
        _tokio_metrics_intervals: impl Iterator<Item = TaskMetrics> + Send + 'static,
    ) -> mz_server_core::ConnectionHandler {
        let tls = self.tls.clone();
        let internal_tls = self.internal_tls;
        let resolver = Arc::clone(&self.resolver);
        let inner_metrics = self.metrics.clone();
        let outer_metrics = self.metrics.clone();
        let cancellation_resolver = Arc::clone(&self.cancellation_resolver);
        let conn_uuid = epoch_to_uuid_v7(&(self.now)());
        let peer_addr = conn.peer_addr();
        conn.uuid_handle().set(conn_uuid);
        Box::pin(async move {
            // TODO: Try to merge this with pgwire/server.rs to avoid the duplication. May not be
            // worth it.
            let active_guard = outer_metrics.active_connections();
            let result: Result<(), anyhow::Error> = async move {
                let mut conn = Conn::Unencrypted(conn);
                loop {
                    let message = decode_startup(&mut conn).await?;
                    conn = match message {
                        // Clients sometimes hang up during the startup sequence, e.g.
                        // because they receive an unacceptable response to an
                        // `SslRequest`. This is considered a graceful termination.
                        None => return Ok(()),

                        Some(FrontendStartupMessage::Startup {
                            version,
                            mut params,
                        }) => {
                            let mut conn = FramedConn::new(conn);
                            let peer_addr = match peer_addr {
                                Ok(addr) => addr.ip(),
                                Err(e) => {
                                    error!("Invalid peer_addr {:?}", e);
                                    return Ok(conn
                                        .send(ErrorResponse::fatal(
                                            SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                                            "invalid peer address",
                                        ))
                                        .await?);
                                }
                            };
                            debug!(%conn_uuid, %peer_addr,  "starting new pgwire connection in balancer");
                            let prev =
                                params.insert(CONN_UUID_KEY.to_string(), conn_uuid.to_string());
                            if prev.is_some() {
                                return Ok(conn
                                    .send(ErrorResponse::fatal(
                                        SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                                        format!("invalid parameter '{CONN_UUID_KEY}'"),
                                    ))
                                    .await?);
                            }

                            if let Some(_) = params.insert(MZ_FORWARDED_FOR_KEY.to_string(), peer_addr.to_string().clone()) {
                                return Ok(conn
                                    .send(ErrorResponse::fatal(
                                        SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                                        format!("invalid parameter '{MZ_FORWARDED_FOR_KEY}'"),
                                    ))
                                    .await?);
                            };

                            Self::run(
                                &mut conn,
                                version,
                                params,
                                &resolver,
                                tls.map(|tls| tls.mode),
                                internal_tls,
                                &inner_metrics,
                            )
                            .await?;
                            conn.flush().await?;
                            return Ok(());
                        }

                        Some(FrontendStartupMessage::CancelRequest {
                            conn_id,
                            secret_key,
                        }) => {
                            spawn(|| "cancel request", async move {
                                cancel_request(conn_id, secret_key, &cancellation_resolver).await;
                            });
                            // Do not wait on cancel requests to return because cancellation is best
                            // effort.
                            return Ok(());
                        }

                        Some(FrontendStartupMessage::SslRequest) => match (conn, &tls) {
                            (Conn::Unencrypted(mut conn), Some(tls)) => {
                                conn.write_all(&[ACCEPT_SSL_ENCRYPTION]).await?;
                                let mut ssl_stream =
                                    SslStream::new(Ssl::new(&tls.context.get())?, conn)?;
                                if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                                    let _ = ssl_stream.get_mut().shutdown().await;
                                    return Err(e.into());
                                }
                                Conn::Ssl(ssl_stream)
                            }
                            (mut conn, _) => {
                                conn.write_all(&[REJECT_ENCRYPTION]).await?;
                                conn
                            }
                        },

                        Some(FrontendStartupMessage::GssEncRequest) => {
                            conn.write_all(&[REJECT_ENCRYPTION]).await?;
                            conn
                        }
                    }
                }
            }
            .await;
            drop(active_guard);
            outer_metrics.connection_status(result.is_ok()).inc();
            Ok(())
        })
    }
}

// A struct that counts bytes exchanged.
struct CountingConn<C> {
    inner: C,
    read: usize,
    written: usize,
}

impl<C> CountingConn<C> {
    fn new(inner: C) -> Self {
        CountingConn {
            inner,
            read: 0,
            written: 0,
        }
    }
}

impl<C> AsyncRead for CountingConn<C>
where
    C: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let counter = self.get_mut();
        let pin = Pin::new(&mut counter.inner);
        let bytes = buf.filled().len();
        let poll = pin.poll_read(cx, buf);
        let bytes = buf.filled().len() - bytes;
        if let std::task::Poll::Ready(Ok(())) = poll {
            counter.read += bytes
        }
        poll
    }
}

impl<C> AsyncWrite for CountingConn<C>
where
    C: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let counter = self.get_mut();
        let pin = Pin::new(&mut counter.inner);
        let poll = pin.poll_write(cx, buf);
        if let std::task::Poll::Ready(Ok(bytes)) = poll {
            counter.written += bytes
        }
        poll
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let counter = self.get_mut();
        let pin = Pin::new(&mut counter.inner);
        pin.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let counter = self.get_mut();
        let pin = Pin::new(&mut counter.inner);
        pin.poll_shutdown(cx)
    }
}

/// Broadcasts cancellation to all matching environmentds. `conn_id`'s bits [31..20] are the lower
/// 12 bits of a UUID for an environmentd/organization. Using that and the template in
/// `cancellation_resolver` we generate a hostname. That hostname resolves to all IPs of envds that
/// match the UUID (cloud k8s infrastructure maintains that mapping). This function creates a new
/// task for each envd and relays the cancellation message to it, broadcasting it to any envd that
/// might match the connection.
///
/// This function returns after it has spawned the tasks, and does not wait for them to complete.
/// This is acceptable because cancellation in the Postgres protocol is best effort and has no
/// guarantees.
///
/// The safety of broadcasting this is due to the various randomness in the connection id and secret
/// key, which must match exactly in order to execute a query cancellation. The connection id has 19
/// bits of randomness, and the secret key the full 32, for a total of 51 bits. That is more than
/// 2e15 combinations, enough to nearly certainly prevent two different envds generating identical
/// combinations.
async fn cancel_request(
    conn_id: u32,
    secret_key: u32,
    cancellation_resolver: &CancellationResolver,
) {
    let suffix = conn_id_org_uuid(conn_id);
    let contents = match cancellation_resolver {
        CancellationResolver::Directory(dir) => {
            let path = dir.join(&suffix);
            match std::fs::read_to_string(&path) {
                Ok(contents) => contents,
                Err(err) => {
                    error!("could not read cancel file {path:?}: {err}");
                    return;
                }
            }
        }
        CancellationResolver::Static(addr) => addr.to_owned(),
    };
    let mut all_ips = Vec::new();
    for addr in contents.lines() {
        let addr = addr.trim();
        if addr.is_empty() {
            continue;
        }
        match tokio::net::lookup_host(addr).await {
            Ok(ips) => all_ips.extend(ips),
            Err(err) => {
                error!("{addr} failed resolution: {err}");
            }
        }
    }
    let mut buf = BytesMut::with_capacity(16);
    let msg = FrontendStartupMessage::CancelRequest {
        conn_id,
        secret_key,
    };
    msg.encode(&mut buf).expect("must encode");
    let buf = buf.freeze();
    for ip in all_ips {
        debug!("cancelling {suffix} to {ip}");
        let buf = buf.clone();
        spawn(|| "cancel request for ip", async move {
            let send = async {
                let mut stream = TcpStream::connect(&ip).await?;
                stream.write_all(&buf).await?;
                stream.shutdown().await?;
                Ok::<_, io::Error>(())
            };
            if let Err(err) = send.await {
                error!("error mirroring cancel to {ip}: {err}");
            }
        });
    }
}

struct HttpsBalancer {
    resolver: Arc<StubResolver>,
    tls: Option<ReloadingSslContext>,
    resolve_template: Arc<str>,
    port: u16,
    metrics: Arc<ServerMetrics>,
    configs: ConfigSet,
    internal_tls: bool,
}

impl HttpsBalancer {
    async fn resolve(
        resolver: &StubResolver,
        resolve_template: &str,
        port: u16,
        servername: Option<&str>,
    ) -> Result<ResolvedAddr, anyhow::Error> {
        let addr = match &servername {
            Some(servername) => resolve_template.replace("{}", servername),
            None => resolve_template.to_string(),
        };
        debug!("https address: {addr}");

        // When we lookup the address using SNI, we get a hostname (`3dl07g8zmj91pntk4eo9cfvwe` for
        // example), which you convert into a different form for looking up the environment address
        // `blncr-3dl07g8zmj91pntk4eo9cfvwe`. When you do a DNS lookup in kubernetes for
        // `blncr-3dl07g8zmj91pntk4eo9cfvwe`, you get a CNAME response pointing at environmentd
        // `environmentd.environment-58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3-0.svc.cluster.local`. This
        // is of the form `<service>.<namespace>.svc.cluster.local`. That `<namespace>` is the same
        // as the environment name, and is based on the tenant ID. `environment-<tenant_id>-<index>`
        // We currently only support a single environment per tenant in a region, so `<index>` is
        // always 0. Do not rely on this ending in `-0` so in the future multiple envds are
        // supported.

        // Attempt to get a tenant.
        let tenant = resolver.tenant(&addr).await;

        // Now do the regular ip lookup, regardless of if there was a CNAME.
        let envd_addr = lookup(&format!("{addr}:{port}")).await?;

        Ok(ResolvedAddr {
            addr: envd_addr,
            password: None,
            tenant,
        })
    }
}

trait StubResolverExt {
    async fn tenant(&self, addr: &str) -> Option<String>;
}

impl StubResolverExt for StubResolver {
    /// Finds the tenant of a DNS address. Errors or lack of cname resolution here are ok, because
    /// this is only used for metrics.
    async fn tenant(&self, addr: &str) -> Option<String> {
        let Ok(dname) = Name::<Vec<_>>::from_str(addr) else {
            return None;
        };
        debug!("resolving tenant for {:?}", addr);
        // Lookup the CNAME. If there's a CNAME, find the tenant.
        let lookup = self.query((dname, Rtype::CNAME)).await;
        if let Ok(lookup) = lookup {
            if let Ok(answer) = lookup.answer() {
                let res = answer.limit_to::<AllRecordData<_, _>>();
                for record in res {
                    let Ok(record) = record else {
                        continue;
                    };
                    if record.rtype() != Rtype::CNAME {
                        continue;
                    }
                    let cname = record.data();
                    let cname = cname.to_string();
                    debug!("cname: {cname}");
                    return extract_tenant_from_cname(&cname);
                }
            }
        }
        None
    }
}

/// Extracts the tenant from a CNAME.
fn extract_tenant_from_cname(cname: &str) -> Option<String> {
    let mut parts = cname.split('.');
    let _service = parts.next();
    let Some(namespace) = parts.next() else {
        return None;
    };
    // Trim off the starting `environmentd-`.
    let Some((_, namespace)) = namespace.split_once('-') else {
        return None;
    };
    // Trim off the ending `-0` (or some other number).
    let Some((tenant, _)) = namespace.rsplit_once('-') else {
        return None;
    };
    // Convert to a Uuid so that this tenant matches the frontegg resolver exactly, because it
    // also uses Uuid::to_string.
    let Ok(tenant) = Uuid::parse_str(tenant) else {
        error!("cname tenant not a uuid: {tenant}");
        return None;
    };
    Some(tenant.to_string())
}

impl mz_server_core::Server for HttpsBalancer {
    const NAME: &'static str = "https_balancer";

    // TODO(jkosh44) consider forwarding the connection UUID to the adapter.
    fn handle_connection(
        &self,
        conn: Connection,
        _tokio_metrics_intervals: impl Iterator<Item = TaskMetrics> + Send + 'static,
    ) -> mz_server_core::ConnectionHandler {
        let tls_context = self.tls.clone();
        let internal_tls = self.internal_tls.clone();
        let resolver = Arc::clone(&self.resolver);
        let resolve_template = Arc::clone(&self.resolve_template);
        let port = self.port;
        let inner_metrics = Arc::clone(&self.metrics);
        let outer_metrics = Arc::clone(&self.metrics);
        let peer_addr = conn.peer_addr();
        let inject_proxy_headers = INJECT_PROXY_PROTOCOL_HEADER_HTTP.get(&self.configs);
        Box::pin(async move {
            let active_guard = inner_metrics.active_connections();
            let result: Result<_, anyhow::Error> = Box::pin(async move {
                let peer_addr = peer_addr.context("fetching peer addr")?;
                let (client_stream, servername): (Box<dyn ClientStream>, Option<String>) =
                    match tls_context {
                        Some(tls_context) => {
                            let mut ssl_stream =
                                SslStream::new(Ssl::new(&tls_context.get())?, conn)?;
                            if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                                let _ = ssl_stream.get_mut().shutdown().await;
                                return Err(e.into());
                            }
                            let servername: Option<String> =
                                ssl_stream.ssl().servername(NameType::HOST_NAME).map(|sn| {
                                    match sn.split_once('.') {
                                        Some((left, _right)) => left,
                                        None => sn,
                                    }
                                    .into()
                                });
                            debug!("Found sni servername: {servername:?} (https)");
                            (Box::new(ssl_stream), servername)
                        }
                        _ => (Box::new(conn), None),
                    };
                let resolved =
                    Self::resolve(&resolver, &resolve_template, port, servername.as_deref())
                        .await?;
                let inner_active_guard = resolved
                    .tenant
                    .as_ref()
                    .map(|tenant| inner_metrics.tenant_connections(tenant));

                let mut mz_stream = TcpStream::connect(resolved.addr).await?;

                if inject_proxy_headers {
                    // Write the tcp proxy header
                    let addrs = ProxiedAddress::stream(peer_addr, resolved.addr);
                    let header = ProxyHeader::with_address(addrs);
                    let mut buf = [0u8; 1024];
                    let len = header.encode_to_slice_v2(&mut buf)?;
                    mz_stream.write_all(&buf[..len]).await?;
                }

                let mut mz_stream = if internal_tls {
                    // do a TLS handshake
                    let mut builder =
                        SslConnector::builder(SslMethod::tls()).expect("Error creating builder.");
                    // environmentd doesn't yet have a cert we trust, so for now disable verification.
                    builder.set_verify(SslVerifyMode::NONE);
                    let mut ssl = builder
                        .build()
                        .configure()?
                        .into_ssl(&resolved.addr.to_string())?;
                    ssl.set_connect_state();
                    Conn::Ssl(SslStream::new(ssl, mz_stream)?)
                } else {
                    Conn::Unencrypted(mz_stream)
                };

                let mut client_counter = CountingConn::new(client_stream);

                // Now blindly shuffle bytes back and forth until closed.
                // TODO: Limit total memory use.
                // See corresponding comment in pgwire implementation about ignoring the error.
                let _ = tokio::io::copy_bidirectional(&mut client_counter, &mut mz_stream).await;
                if let Some(tenant) = &resolved.tenant {
                    inner_metrics
                        .tenant_connections_tx(tenant)
                        .inc_by(u64::cast_from(client_counter.written));
                    inner_metrics
                        .tenant_connections_rx(tenant)
                        .inc_by(u64::cast_from(client_counter.read));
                }
                drop(inner_active_guard);
                Ok(())
            })
            .await;
            drop(active_guard);
            outer_metrics.connection_status(result.is_ok()).inc();
            if let Err(e) = result {
                debug!("connection error: {e}");
            }
            Ok(())
        })
    }
}

#[derive(Debug)]
pub struct SniResolver {
    pub resolver: StubResolver,
    pub template: String,
    pub port: u16,
}

trait ClientStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ClientStream for T {}

#[derive(Debug)]
pub enum Resolver {
    Static(String),
    MultiTenant(FronteggResolver, Option<SniResolver>),
}

impl Resolver {
    async fn resolve<A>(
        &self,
        conn: &mut FramedConn<A>,
        user: &str,
        metrics: &ServerMetrics,
    ) -> Result<ResolvedAddr, anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin,
    {
        match self {
            Resolver::MultiTenant(
                FronteggResolver {
                    auth,
                    addr_template,
                },
                sni_resolver,
            ) => {
                let servername = match conn.inner() {
                    Conn::Ssl(ssl_stream) => {
                        ssl_stream.ssl().servername(NameType::HOST_NAME).map(|sn| {
                            match sn.split_once('.') {
                                Some((left, _right)) => left,
                                None => sn,
                            }
                        })
                    }
                    Conn::Unencrypted(_) => None,
                };
                let has_sni = servername.is_some();
                // We found an SNi
                let resolved_addr = match (servername, sni_resolver) {
                    (
                        Some(servername),
                        Some(SniResolver {
                            resolver: stub_resolver,
                            template: sni_addr_template,
                            port,
                        }),
                    ) => {
                        let sni_addr = sni_addr_template.replace("{}", servername);
                        let tenant = stub_resolver.tenant(&sni_addr).await;
                        let sni_addr = format!("{sni_addr}:{port}");
                        let addr = lookup(&sni_addr).await?;
                        if tenant.is_some() {
                            debug!("SNI header found for tenant {:?}", tenant);
                        }
                        ResolvedAddr {
                            addr,
                            password: None,
                            tenant,
                        }
                    }
                    _ => {
                        conn.send(BackendMessage::AuthenticationCleartextPassword)
                            .await?;
                        conn.flush().await?;
                        let password = match conn.recv().await? {
                            Some(FrontendMessage::Password { password }) => password,
                            _ => anyhow::bail!("expected Password message"),
                        };

                        let auth_response = auth.authenticate(user, &password).await;
                        let auth_session = match auth_response {
                            Ok(auth_session) => auth_session,
                            Err(e) => {
                                warn!("pgwire connection failed authentication: {}", e);
                                // TODO: fix error codes.
                                anyhow::bail!("invalid password");
                            }
                        };

                        let addr =
                            addr_template.replace("{}", &auth_session.tenant_id().to_string());
                        let addr = lookup(&addr).await?;
                        let tenant = Some(auth_session.tenant_id().to_string());
                        if tenant.is_some() {
                            debug!("SNI header NOT found for tenant {:?}", tenant);
                        }
                        ResolvedAddr {
                            addr,
                            password: Some(password),
                            tenant,
                        }
                    }
                };
                metrics
                    .tenant_pgwire_sni_count(
                        resolved_addr.tenant.as_deref().unwrap_or("unknown"),
                        has_sni,
                    )
                    .inc();

                Ok(resolved_addr)
            }
            Resolver::Static(addr) => {
                let addr = lookup(addr).await?;
                Ok(ResolvedAddr {
                    addr,
                    password: None,
                    tenant: None,
                })
            }
        }
    }
}

/// Returns the first IP address resolved from the provided hostname.
async fn lookup(name: &str) -> Result<SocketAddr, anyhow::Error> {
    let mut addrs = tokio::net::lookup_host(name).await?;
    match addrs.next() {
        Some(addr) => Ok(addr),
        None => {
            error!("{name} did not resolve to any addresses");
            anyhow::bail!("internal error")
        }
    }
}

#[derive(Debug)]
pub struct FronteggResolver {
    pub auth: FronteggAuthentication,
    pub addr_template: String,
}

#[derive(Debug)]
struct ResolvedAddr {
    addr: SocketAddr,
    password: Option<String>,
    tenant: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_tenant() {
        let tests = vec![
            ("", None),
            (
                "environmentd.environment-58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3-0.svc.cluster.local",
                Some("58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3"),
            ),
            (
                // Variously named parts.
                "service.something-58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3-0.ssvvcc.cloister.faraway",
                Some("58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3"),
            ),
            (
                // No dashes in uuid.
                "environmentd.environment-58cd23ffa4d74bd0ad85a6ff29cc86c3-0.svc.cluster.local",
                Some("58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3"),
            ),
            (
                // -1234 suffix.
                "environmentd.environment-58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3-1234.svc.cluster.local",
                Some("58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3"),
            ),
            (
                // Uppercase.
                "environmentd.environment-58CD23FF-A4D7-4BD0-AD85-A6FF29CC86C3-0.svc.cluster.local",
                Some("58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3"),
            ),
            (
                // No -number suffix.
                "environmentd.environment-58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3.svc.cluster.local",
                None,
            ),
            (
                // No service name.
                "environment-58cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3-0.svc.cluster.local",
                None,
            ),
            (
                // Invalid UUID.
                "environmentd.environment-8cd23ff-a4d7-4bd0-ad85-a6ff29cc86c3-0.svc.cluster.local",
                None,
            ),
        ];
        for (name, expect) in tests {
            let cname = extract_tenant_from_cname(name);
            assert_eq!(
                cname.as_deref(),
                expect,
                "{name} got {cname:?} expected {expect:?}"
            );
        }
    }
}
