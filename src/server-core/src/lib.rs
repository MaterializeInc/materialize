// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Methods common to servers listening for TCP connections.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;
use clap::builder::ArgPredicate;
use futures::stream::{BoxStream, Stream, StreamExt};
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::channel::trigger;
use mz_ore::error::ErrorExt;
use mz_ore::netio::AsyncReady;
use mz_ore::option::OptionExt;
use mz_ore::task::JoinSetExt;
use openssl::ssl::{SslAcceptor, SslContext, SslFiletype, SslMethod};
use proxy_header::{ParseConfig, ProxiedAddress, ProxyHeader};
use scopeguard::ScopeGuard;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, Interest, ReadBuf, Ready};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_stream::wrappers::{IntervalStream, TcpListenerStream};
use tracing::{debug, error, warn};
use uuid::Uuid;

/// TCP keepalive settings. The idle time and interval match CockroachDB [0].
/// The number of retries matches the Linux default.
///
/// [0]: https://github.com/cockroachdb/cockroach/pull/14063
const KEEPALIVE: TcpKeepalive = TcpKeepalive::new()
    .with_time(Duration::from_secs(60))
    .with_interval(Duration::from_secs(60))
    .with_retries(9);

/// A future that handles a connection.
pub type ConnectionHandler = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>>;

/// A wrapper around a [`TcpStream`] that can identify a connection across
/// processes.
pub struct Connection {
    conn_uuid: Arc<Mutex<Option<Uuid>>>,
    tcp_stream: TcpStream,
}

impl Connection {
    fn new(tcp_stream: TcpStream) -> Connection {
        Connection {
            conn_uuid: Arc::new(Mutex::new(None)),
            tcp_stream,
        }
    }

    /// Returns a handle to the connection UUID.
    pub fn uuid_handle(&self) -> ConnectionUuidHandle {
        ConnectionUuidHandle(Arc::clone(&self.conn_uuid))
    }

    /// Attempts to parse a proxy header from the tcp_stream.
    /// If none is found or it is unable to be parsed None will
    /// be returned. If a header is found it will be returned and its
    /// bytes will be removed from the stream.
    ///
    /// It is possible an invalid header was sent, if that is the case
    /// any downstream service will be responsible for returning errors
    /// to the client.
    pub async fn take_proxy_header_address(&mut self) -> Option<ProxiedAddress> {
        // 1024 bytes is a rather large header for tcp proxy header, unless
        // if the header contains TLV fields or uses a unix socket address
        // this could easily be hit. We'll use a 1024 byte max buf to allow
        // limited support for this.
        let mut buf = [0u8; 1024];
        let len = match self.tcp_stream.peek(&mut buf).await {
            Ok(n) if n > 0 => n,
            _ => {
                debug!("Failed to read from client socket or no data received");
                return None;
            }
        };

        // Attempt to parse the header, and log failures.
        let (header, hlen) = match ProxyHeader::parse(
            &buf[..len],
            ParseConfig {
                include_tlvs: false,
                allow_v1: false,
                allow_v2: true,
            },
        ) {
            Ok((header, hlen)) => (header, hlen),
            Err(proxy_header::Error::Invalid) => {
                debug!(
                    "Proxy header is invalid. This is likely due to no no header being provided",
                );
                return None;
            }
            Err(e) => {
                debug!("Proxy header parse error '{:?}', ignoring header.", e);
                return None;
            }
        };
        debug!("Proxied connection with header {:?}", header);
        let address = header.proxied_address().map(|a| a.to_owned());
        // Proxy header found, clear the bytes.
        let _ = self.read_exact(&mut buf[..hlen]).await;
        address
    }

    /// Peer address of the inner tcp_stream.
    pub fn peer_addr(&self) -> Result<std::net::SocketAddr, io::Error> {
        self.tcp_stream.peer_addr()
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.tcp_stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.tcp_stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.tcp_stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.tcp_stream).poll_shutdown(cx)
    }
}

#[async_trait]
impl AsyncReady for Connection {
    async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        self.tcp_stream.ready(interest).await
    }
}

/// A handle that permits getting and setting the UUID for a [`Connection`].
///
/// A connection's UUID is a globally unique value that can identify a given
/// connection across environments and process boundaries. Connection UUIDs are
/// never reused.
///
/// This is distinct from environmentd's concept of a "connection ID", which is
/// a `u32` that only identifies a connection within a given environment and
/// only during its lifetime. These connection IDs are frequently reused.
pub struct ConnectionUuidHandle(Arc<Mutex<Option<Uuid>>>);

impl ConnectionUuidHandle {
    /// Gets the UUID for the connection, if it exists.
    pub fn get(&self) -> Option<Uuid> {
        *self.0.lock().expect("lock poisoned")
    }

    /// Sets the UUID for this connection.
    pub fn set(&self, conn_uuid: Uuid) {
        *self.0.lock().expect("lock poisoned") = Some(conn_uuid);
    }

    /// Returns a displayble that renders a possibly missing connection UUID.
    pub fn display(&self) -> impl fmt::Display {
        self.get().display_or("<unknown>")
    }
}

/// A server handles incoming network connections.
pub trait Server {
    /// Returns the name of the connection handler for use in e.g. log messages.
    const NAME: &'static str;

    /// Handles a single connection.
    fn handle_connection(&self, conn: Connection) -> ConnectionHandler;
}

/// A stream of incoming connections.
pub trait ConnectionStream: Stream<Item = io::Result<TcpStream>> + Unpin + Send {}

impl<T> ConnectionStream for T where T: Stream<Item = io::Result<TcpStream>> + Unpin + Send {}

/// A handle to a listener created by [`listen`].
#[derive(Debug)]
pub struct ListenerHandle {
    local_addr: SocketAddr,
    _trigger: trigger::Trigger,
}

impl ListenerHandle {
    /// Returns the local address to which the listener is bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Listens for incoming TCP connections on the specified address.
///
/// Returns a handle to the listener and the stream of incoming connections
/// produced by the listener. When the handle is dropped, the listener is
/// closed, and the stream of incoming connections terminates.
pub async fn listen(
    addr: &SocketAddr,
) -> Result<(ListenerHandle, Pin<Box<dyn ConnectionStream>>), io::Error> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (trigger, trigger_rx) = trigger::channel();
    let handle = ListenerHandle {
        local_addr,
        _trigger: trigger,
    };
    // TODO(benesch): replace `TCPListenerStream`s with `listener.incoming()` if
    // that is restored when the `Stream` trait stabilizes.
    let stream = TcpListenerStream::new(listener).take_until(trigger_rx);
    Ok((handle, Box::pin(stream)))
}

/// Configuration for [`serve`].
pub struct ServeConfig<S, C>
where
    S: Server,
    C: ConnectionStream,
{
    /// The server for the connections.
    pub server: S,
    /// The stream of incoming TCP connections.
    pub conns: C,
    /// Optional dynamic configuration for the server.
    pub dyncfg: Option<ServeDyncfg>,
}

/// Dynamic configuration for [`ServeConfig`].
pub struct ServeDyncfg {
    /// The current bundle of dynamic configuration values.
    pub config_set: ConfigSet,
    /// A configuration in `config_set` that specifies how long to wait for
    /// connections to terminate after receiving a SIGTERM before forcibly
    /// terminated.
    ///
    /// If `None`, then forcible shutdown occurs immediately.
    pub sigterm_wait_config: &'static Config<Duration>,
}

/// Serves incoming TCP connections.
///
/// Returns handles to the outstanding connections after the configured timeout
/// has expired or all connections have completed.
pub async fn serve<S, C>(
    ServeConfig {
        server,
        mut conns,
        dyncfg,
    }: ServeConfig<S, C>,
) -> JoinSet<()>
where
    S: Server,
    C: ConnectionStream,
{
    let task_name = format!("handle_{}_connection", S::NAME);
    let mut set = JoinSet::new();
    loop {
        tokio::select! {
            // next() is cancel safe.
            conn = conns.next() => {
                let conn = match conn {
                    None => break,
                    Some(Ok(conn)) => conn,
                    Some(Err(err)) => {
                        error!("error accepting connection: {}", err);
                        continue;
                    }
                };
                // Set TCP_NODELAY to disable tinygram prevention (Nagle's
                // algorithm), which forces a 40ms delay between each query
                // on linux. According to John Nagle [0], the true problem
                // is delayed acks, but disabling those is a receive-side
                // operation (TCP_QUICKACK), and we can't always control the
                // client. PostgreSQL sets TCP_NODELAY on both sides of its
                // sockets, so it seems sane to just do the same.
                //
                // If set_nodelay fails, it's a programming error, so panic.
                //
                // [0]: https://news.ycombinator.com/item?id=10608356
                conn.set_nodelay(true).expect("set_nodelay failed");
                // Enable TCP keepalives to avoid any idle connection timeouts that may
                // be enforced by networking devices between us and the client. Idle SQL
                // connections are expected--e.g., a `SUBSCRIBE` to a view containing
                // critical alerts will ideally be producing no data most of the time.
                if let Err(e) = SockRef::from(&conn).set_tcp_keepalive(&KEEPALIVE) {
                    error!("failed enabling keepalive: {e}");
                    continue;
                }
                let conn = Connection::new(conn);
                let conn_uuid = conn.uuid_handle();
                let fut = server.handle_connection(conn);
                set.spawn_named(|| &task_name, async move {
                    let guard = scopeguard::guard((), |_| {
                        debug!(
                            server = S::NAME,
                            conn_uuid = %conn_uuid.display(),
                            "dropping connection without explicit termination",
                        );
                    });

                    match fut.await {
                        Ok(()) => {
                            debug!(
                                server = S::NAME,
                                conn_uuid = %conn_uuid.display(),
                                "successfully handled connection",
                            );
                        }
                        Err(e) => {
                            warn!(
                                server = S::NAME,
                                conn_uuid = %conn_uuid.display(),
                                "error handling connection: {}",
                                e.display_with_causes(),
                            );
                        }
                    }

                    let () = ScopeGuard::into_inner(guard);
                });
            }
            // Actively cull completed tasks from the JoinSet so it does not grow unbounded. This
            // method is cancel safe.
            res = set.join_next(), if set.len() > 0 => {
                if let Some(Err(e)) = res {
                    warn!(
                        "error joining connection in {}: {}",
                        S::NAME,
                        e.display_with_causes()
                    );
                }
            }
        }
    }
    if let Some(dyncfg) = dyncfg {
        let wait = dyncfg.sigterm_wait_config.get(&dyncfg.config_set);
        if set.len() > 0 {
            warn!(
                "{} exiting, {} outstanding connections, waiting for {:?}",
                S::NAME,
                set.len(),
                wait
            );
        }
        let timedout = tokio::time::timeout(wait, async {
            while let Some(res) = set.join_next().await {
                if let Err(e) = res {
                    warn!(
                        "error joining connection in {}: {}",
                        S::NAME,
                        e.display_with_causes()
                    );
                }
            }
        })
        .await;
        if timedout.is_err() {
            warn!(
                "{}: wait timeout of {:?} exceeded, {} outstanding connections",
                S::NAME,
                wait,
                set.len()
            );
        }
    }
    set
}

/// Configures a server's TLS encryption and authentication.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    /// The SSL context used to manage incoming TLS negotiations.
    pub context: SslContext,
    /// The TLS mode.
    pub mode: TlsMode,
}

/// Specifies how strictly to enforce TLS encryption.
#[derive(Debug, Clone, Copy)]
pub enum TlsMode {
    /// Allow TLS encryption.
    Allow,
    /// Require that clients negotiate TLS encryption.
    Require,
}

/// Configures TLS encryption for connections.
#[derive(Debug, Clone)]
pub struct TlsCertConfig {
    /// The path to the TLS certificate.
    pub cert: PathBuf,
    /// The path to the TLS key.
    pub key: PathBuf,
}

impl TlsCertConfig {
    /// Returns the SSL context to use in TlsConfigs.
    pub fn load_context(&self) -> Result<SslContext, anyhow::Error> {
        // Mozilla publishes three presets: old, intermediate, and modern. They
        // recommend the intermediate preset for general purpose servers, which
        // is what we use, as it is compatible with nearly every client released
        // in the last five years but does not include any known-problematic
        // ciphers. We once tried to use the modern preset, but it was
        // incompatible with Fivetran, and presumably other JDBC-based tools.
        let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
        builder.set_certificate_chain_file(&self.cert)?;
        builder.set_private_key_file(&self.key, SslFiletype::PEM)?;
        Ok(builder.build().into_context())
    }

    /// Like [Self::load_context] but attempts to reload the files each time `ticker` yields an item.
    /// Returns an error based on the files currently on disk. When `ticker` receives, the
    /// certificates are reloaded from the context. The result of the reloading is returned on the
    /// oneshot if present, and an Ok result means new connections will use the new certificates. An
    /// Err result will not change the current certificates.
    pub fn reloading_context(
        &self,
        mut ticker: ReloadTrigger,
    ) -> Result<ReloadingSslContext, anyhow::Error> {
        let context = Arc::new(RwLock::new(self.load_context()?));
        let updater_context = Arc::clone(&context);
        let config = self.clone();
        mz_ore::task::spawn(|| "TlsCertConfig reloading_context", async move {
            while let Some(chan) = ticker.next().await {
                let result = match config.load_context() {
                    Ok(ctx) => {
                        *updater_context.write().expect("poisoned") = ctx;
                        Ok(())
                    }
                    Err(err) => {
                        tracing::error!("failed to reload SSL certificate: {err}");
                        Err(err)
                    }
                };
                if let Some(chan) = chan {
                    let _ = chan.send(result);
                }
            }
            tracing::warn!("TlsCertConfig reloading_context updater closed");
        });
        Ok(ReloadingSslContext { context })
    }
}

/// An SslContext whose inner value can be updated.
#[derive(Clone, Debug)]
pub struct ReloadingSslContext {
    /// The current SSL context.
    context: Arc<RwLock<SslContext>>,
}

impl ReloadingSslContext {
    pub fn get(&self) -> RwLockReadGuard<SslContext> {
        self.context.read().expect("poisoned")
    }
}

/// Configures a server's TLS encryption and authentication with reloading.
#[derive(Clone, Debug)]
pub struct ReloadingTlsConfig {
    /// The SSL context used to manage incoming TLS negotiations.
    pub context: ReloadingSslContext,
    /// The TLS mode.
    pub mode: TlsMode,
}

pub type ReloadTrigger = BoxStream<'static, Option<oneshot::Sender<Result<(), anyhow::Error>>>>;

/// Returns a ReloadTrigger that triggers once per hour.
pub fn default_cert_reload_ticker() -> ReloadTrigger {
    let ticker = IntervalStream::new(tokio::time::interval(Duration::from_secs(60 * 60)));
    let ticker = ticker.map(|_| None);
    let ticker = Box::pin(ticker);
    ticker
}

/// Returns a ReloadTrigger that never triggers.
pub fn cert_reload_never_reload() -> ReloadTrigger {
    let ticker = futures::stream::empty();
    let ticker = Box::pin(ticker);
    ticker
}

/// Command line arguments for TLS.
#[derive(Debug, Clone, clap::Parser)]
pub struct TlsCliArgs {
    /// How stringently to demand TLS authentication and encryption.
    ///
    /// If set to "disable", then environmentd rejects HTTP and PostgreSQL
    /// connections that negotiate TLS.
    ///
    /// If set to "require", then environmentd requires that all HTTP and
    /// PostgreSQL connections negotiate TLS. Unencrypted connections will be
    /// rejected.
    #[clap(
        long, env = "TLS_MODE",
        value_parser = ["disable", "require"],
        default_value = "disable",
        default_value_ifs = [
            ("frontegg_tenant", ArgPredicate::IsPresent, Some("require")),
            ("frontegg_resolver_template", ArgPredicate::IsPresent, Some("require")),
        ],
        value_name = "MODE",
    )]
    tls_mode: String,
    /// Certificate file for TLS connections.
    #[clap(
        long,
        env = "TLS_CERT",
        requires = "tls_key",
        required_if_eq_any([("tls_mode", "require")]),
        value_name = "PATH"
    )]
    tls_cert: Option<PathBuf>,
    /// Private key file for TLS connections.
    #[clap(
        long,
        env = "TLS_KEY",
        requires = "tls_cert",
        required_if_eq_any([("tls_mode", "require")]),
        value_name = "PATH"
    )]
    tls_key: Option<PathBuf>,
}

impl TlsCliArgs {
    /// Convert args into configuration.
    pub fn into_config(self) -> Result<Option<TlsCertConfig>, anyhow::Error> {
        if self.tls_mode == "disable" {
            if self.tls_cert.is_some() {
                bail!("cannot specify --tls-mode=disable and --tls-cert simultaneously");
            }
            if self.tls_key.is_some() {
                bail!("cannot specify --tls-mode=disable and --tls-key simultaneously");
            }
            Ok(None)
        } else {
            let cert = self.tls_cert.unwrap();
            let key = self.tls_key.unwrap();
            Ok(Some(TlsCertConfig { cert, key }))
        }
    }
}
