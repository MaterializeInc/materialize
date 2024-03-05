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

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::response::IntoResponse;
use axum::{routing, Router};
use bytes::BytesMut;
use futures::stream::BoxStream;
use futures::TryFutureExt;
use hyper::StatusCode;
use mz_build_info::{build_info, BuildInfo};
use mz_frontegg_auth::Authenticator as FronteggAuthentication;
use mz_ore::id_gen::conn_id_org_uuid;
use mz_ore::metrics::{ComputedGauge, IntCounter, IntGauge, MetricsRegistry};
use mz_ore::netio::AsyncReady;
use mz_ore::task::{spawn, JoinSetExt};
use mz_ore::{metric, netio};
use mz_pgwire_common::{
    decode_startup, Conn, ErrorResponse, FrontendMessage, FrontendStartupMessage,
    ACCEPT_SSL_ENCRYPTION, REJECT_ENCRYPTION, VERSION_3,
};
use mz_server_core::{
    listen, ConnectionStream, ListenerHandle, ReloadingSslContext, ReloadingTlsConfig,
    TlsCertConfig, TlsMode,
};
use openssl::ssl::{NameType, Ssl};
use prometheus::{IntCounterVec, IntGaugeVec};
use semver::Version;
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_openssl::SslStream;
use tokio_postgres::error::SqlState;
use tracing::{debug, error, warn};

use crate::codec::{BackendMessage, FramedConn};

/// Balancer build information.
pub const BUILD_INFO: BuildInfo = build_info!();

pub struct BalancerConfig {
    sigterm_wait: Option<Duration>,
    /// Info about which version of the code is running.
    build_version: Version,
    /// Listen address for internal HTTP health and metrics server.
    internal_http_listen_addr: SocketAddr,
    /// Listen address for pgwire connections.
    pgwire_listen_addr: SocketAddr,
    /// Listen address for HTTPS connections.
    https_listen_addr: SocketAddr,
    /// Cancellation resolver configmap directory.
    cancellation_resolver_dir: Option<PathBuf>,
    /// DNS resolver.
    resolver: Resolver,
    https_addr_template: String,
    tls: Option<TlsCertConfig>,
    metrics_registry: MetricsRegistry,
    reload_certs: BoxStream<'static, Option<oneshot::Sender<Result<(), anyhow::Error>>>>,
}

impl BalancerConfig {
    pub fn new(
        build_info: &BuildInfo,
        sigterm_wait: Option<Duration>,
        internal_http_listen_addr: SocketAddr,
        pgwire_listen_addr: SocketAddr,
        https_listen_addr: SocketAddr,
        cancellation_resolver_dir: Option<PathBuf>,
        resolver: Resolver,
        https_addr_template: String,
        tls: Option<TlsCertConfig>,
        metrics_registry: MetricsRegistry,
        reload_certs: BoxStream<'static, Option<oneshot::Sender<Result<(), anyhow::Error>>>>,
    ) -> Self {
        Self {
            build_version: build_info.semver_version(),
            sigterm_wait,
            internal_http_listen_addr,
            pgwire_listen_addr,
            https_listen_addr,
            cancellation_resolver_dir,
            resolver,
            https_addr_template,
            tls,
            metrics_registry,
            reload_certs,
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
    internal_http: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
    _metrics: BalancerMetrics,
}

impl BalancerService {
    pub async fn new(cfg: BalancerConfig) -> Result<Self, anyhow::Error> {
        let pgwire = listen(&cfg.pgwire_listen_addr).await?;
        let https = listen(&cfg.https_listen_addr).await?;
        let internal_http = listen(&cfg.internal_http_listen_addr).await?;
        let metrics = BalancerMetrics::new(&cfg);
        Ok(Self {
            cfg,
            pgwire,
            https,
            internal_http,
            _metrics: metrics,
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
            if let Some(dir) = &self.cfg.cancellation_resolver_dir {
                if !dir.is_dir() {
                    anyhow::bail!("{dir:?} is not a directory");
                }
            }
            let cancellation_resolver = self.cfg.cancellation_resolver_dir.map(Arc::new);
            let pgwire = PgwireBalancer {
                resolver: Arc::new(self.cfg.resolver),
                cancellation_resolver,
                tls: pgwire_tls,
                metrics: ServerMetrics::new(metrics.clone(), "pgwire"),
            };
            let (handle, stream) = self.pgwire;
            server_handles.push(handle);
            set.spawn_named(|| "pgwire_stream", async move {
                mz_server_core::serve(stream, pgwire, self.cfg.sigterm_wait).await;
                warn!("pgwire server exited");
            });
        }
        {
            let https = HttpsBalancer {
                tls: https_tls,
                resolve_template: Arc::from(self.cfg.https_addr_template),
                metrics: ServerMetrics::new(metrics, "https"),
            };
            let (handle, stream) = self.https;
            server_handles.push(handle);
            set.spawn_named(|| "https_stream", async move {
                mz_server_core::serve(stream, https, self.cfg.sigterm_wait).await;
                warn!("https server exited");
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
                // Prevent internal monitoring from allowing a graceful shutdown. In our testing
                // *something* kept this open for at least 10 minutes.
                mz_server_core::serve(stream, internal_http, None).await;
                warn!("internal_http server exited");
            });
        }
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            set.spawn_named(|| "sigterm_handler", async move {
                sigterm.recv().await;
                warn!("received signal TERM");
                drop(server_handles);
            });
        }

        println!("balancerd {} listening...", BUILD_INFO.human_version());
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

    fn handle_connection(&self, conn: TcpStream) -> mz_server_core::ConnectionHandler {
        let router = self.router.clone();
        Box::pin(async {
            let http = hyper::server::conn::Http::new();
            http.serve_connection(conn, router).err_into().await
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
        Self {
            connection_status,
            active_connections,
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

    fn status_label(is_ok: bool) -> &'static str {
        if is_ok {
            "success"
        } else {
            "error"
        }
    }
}

struct PgwireBalancer {
    tls: Option<ReloadingTlsConfig>,
    cancellation_resolver: Option<Arc<PathBuf>>,
    resolver: Arc<Resolver>,
    metrics: ServerMetrics,
}

impl PgwireBalancer {
    #[mz_ore::instrument(level = "debug")]
    async fn run<'a, A>(
        conn: &'a mut FramedConn<A>,
        version: i32,
        params: BTreeMap<String, String>,
        resolver: &Resolver,
        tls_mode: Option<TlsMode>,
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

        let resolved = match resolver.resolve(conn, user).await {
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

        if let Err(err) = Self::stream(conn, resolved.addr, resolved.password, params).await {
            return conn
                .send(ErrorResponse::fatal(
                    SqlState::INVALID_PASSWORD,
                    err.to_string(),
                ))
                .await;
        }

        Ok(())
    }

    #[mz_ore::instrument(level = "debug")]
    async fn stream<'a, A>(
        conn: &'a mut FramedConn<A>,
        envd_addr: SocketAddr,
        password: Option<String>,
        params: BTreeMap<String, String>,
    ) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin,
    {
        let mut mz_stream = TcpStream::connect(envd_addr).await?;
        let mut buf = BytesMut::new();

        // Send initial startup and password messages.
        let startup = FrontendStartupMessage::Startup {
            version: VERSION_3,
            params,
        };
        startup.encode(&mut buf)?;
        mz_stream.write_all(&buf).await?;
        let client_stream = conn.inner_mut();

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

        // Now blindly shuffle bytes back and forth until closed.
        // TODO: Limit total memory use.
        tokio::io::copy_bidirectional(client_stream, &mut mz_stream).await?;

        Ok(())
    }
}

impl mz_server_core::Server for PgwireBalancer {
    const NAME: &'static str = "pgwire_balancer";

    fn handle_connection(&self, conn: TcpStream) -> mz_server_core::ConnectionHandler {
        let tls = self.tls.clone();
        let resolver = Arc::clone(&self.resolver);
        let metrics = self.metrics.clone();
        let cancellation_resolver = self.cancellation_resolver.clone();
        Box::pin(async move {
            // TODO: Try to merge this with pgwire/server.rs to avoid the duplication. May not be
            // worth it.
            let active_guard = metrics.active_connections();
            let result: Result<(), anyhow::Error> = async move {
                let mut conn = Conn::Unencrypted(conn);
                loop {
                    let message = decode_startup(&mut conn).await?;
                    conn = match message {
                        // Clients sometimes hang up during the startup sequence, e.g.
                        // because they receive an unacceptable response to an
                        // `SslRequest`. This is considered a graceful termination.
                        None => return Ok(()),

                        Some(FrontendStartupMessage::Startup { version, params }) => {
                            let mut conn = FramedConn::new(conn);
                            Self::run(
                                &mut conn,
                                version,
                                params,
                                &resolver,
                                tls.map(|tls| tls.mode),
                            )
                            .await?;
                            // TODO: Resolver lookup then begin relaying bytes.
                            conn.flush().await?;
                            return Ok(());
                        }

                        Some(FrontendStartupMessage::CancelRequest {
                            conn_id,
                            secret_key,
                        }) => {
                            if let Some(resolver) = cancellation_resolver {
                                spawn(|| "cancel request", async move {
                                    cancel_request(conn_id, secret_key, &resolver).await;
                                });
                            }
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
            metrics.connection_status(result.is_ok()).inc();
            Ok(())
        })
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
async fn cancel_request(conn_id: u32, secret_key: u32, cancellation_resolver: &PathBuf) {
    let suffix = conn_id_org_uuid(conn_id);
    let path = cancellation_resolver.join(&suffix);
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(err) => {
            error!("could not read cancel file {path:?}: {err}");
            return;
        }
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
    // TODO: Is there a way to not use an Arc here by convincing rust that buf will outlive the
    // spawn? Will awaiting the JoinHandle work?
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
    tls: Option<ReloadingSslContext>,
    resolve_template: Arc<str>,
    metrics: ServerMetrics,
}

impl mz_server_core::Server for HttpsBalancer {
    const NAME: &'static str = "https_balancer";

    fn handle_connection(&self, conn: TcpStream) -> mz_server_core::ConnectionHandler {
        let tls_context = self.tls.clone();
        let resolve_template = Arc::clone(&self.resolve_template);
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let active_guard = metrics.active_connections();
            let result = Box::pin(async move {
                let (mut client_stream, servername): (Box<dyn ClientStream>, Option<String>) =
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
                            debug!("servername: {servername:?}");
                            (Box::new(ssl_stream), servername)
                        }
                        _ => (Box::new(conn), None),
                    };

                let addr: String = match servername {
                    Some(servername) => resolve_template.replace("{}", &servername),
                    None => resolve_template.to_string(),
                };
                debug!("https address: {addr}");

                let mut addrs = tokio::net::lookup_host(&*addr).await?;
                let Some(envd_addr) = addrs.next() else {
                    error!("{addr} did not resolve to any addresses");
                    anyhow::bail!("internal error");
                };

                let mut mz_stream = TcpStream::connect(envd_addr).await?;

                // Now blindly shuffle bytes back and forth until closed.
                // TODO: Limit total memory use.
                tokio::io::copy_bidirectional(&mut client_stream, &mut mz_stream).await?;

                Ok(())
            })
            .await;
            drop(active_guard);
            metrics.connection_status(result.is_ok()).inc();
            if let Err(e) = result {
                debug!("connection error: {e}");
            }
            Ok(())
        })
    }
}

trait ClientStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ClientStream for T {}

#[derive(Debug)]
pub enum Resolver {
    Static(String),
    Frontegg(FronteggResolver),
}

impl Resolver {
    async fn resolve<A>(
        &self,
        conn: &mut FramedConn<A>,
        user: &str,
    ) -> Result<ResolvedAddr, anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin,
    {
        match self {
            Resolver::Frontegg(FronteggResolver {
                auth,
                addr_template,
            }) => {
                conn.send(BackendMessage::AuthenticationCleartextPassword)
                    .await?;
                conn.flush().await?;
                let password = match conn.recv().await? {
                    Some(FrontendMessage::Password { password }) => password,
                    _ => anyhow::bail!("expected Password message"),
                };

                let auth_response = auth.authenticate(user.into(), &password).await;
                let auth_session = match auth_response {
                    Ok(auth_session) => auth_session,
                    Err(e) => {
                        warn!("pgwire connection failed authentication: {}", e);
                        // TODO: fix error codes.
                        anyhow::bail!("invalid password");
                    }
                };

                let addr = addr_template.replace("{}", &auth_session.tenant_id().to_string());
                let addr = lookup(&addr).await?;
                Ok(ResolvedAddr {
                    addr,
                    password: Some(password),
                })
            }
            Resolver::Static(addr) => {
                let addr = lookup(addr).await?;
                Ok(ResolvedAddr {
                    addr,
                    password: None,
                })
            }
        }
    }
}

async fn lookup(addr: &str) -> Result<SocketAddr, anyhow::Error> {
    let mut addrs = tokio::net::lookup_host(&addr).await?;
    match addrs.next() {
        Some(addr) => Ok(addr),
        None => {
            error!("{addr} did not resolve to any addresses");
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
}
