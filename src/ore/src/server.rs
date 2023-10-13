// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Methods common to servers listening for TCP connections.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use crate::error::ErrorExt;
use crate::task;
use anyhow::bail;
use futures::stream::{Stream, StreamExt};
use openssl::ssl::{SslAcceptor, SslContext, SslFiletype, SslMethod};
use socket2::{SockRef, TcpKeepalive};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug, error};

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

/// A server handles incoming network connections.
pub trait Server {
    /// Returns the name of the connection handler for use in e.g. log messages.
    const NAME: &'static str;

    /// Handles a single connection.
    fn handle_connection(&self, conn: TcpStream) -> ConnectionHandler;
}

/// A stream of incoming connections.
pub trait ConnectionStream: Stream<Item = io::Result<TcpStream>> + Unpin + Send {}

impl<T> ConnectionStream for T where T: Stream<Item = io::Result<TcpStream>> + Unpin + Send {}

/// A handle to a listener created by [`listen`].
#[derive(Debug)]
pub struct ListenerHandle {
    local_addr: SocketAddr,
    _trigger: oneshot::Sender<()>,
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
    addr: SocketAddr,
) -> Result<(ListenerHandle, Pin<Box<dyn ConnectionStream>>), io::Error> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (trigger, tripwire) = oneshot::channel();
    let handle = ListenerHandle {
        local_addr,
        _trigger: trigger,
    };
    // TODO(benesch): replace `TCPListenerStream`s with `listener.incoming()` if
    // that is restored when the `Stream` trait stabilizes.
    let stream = TcpListenerStream::new(listener).take_until(tripwire);
    Ok((handle, Box::pin(stream)))
}

/// Serves incoming TCP connections from `conns` using `server`.
pub async fn serve<C, S>(mut conns: C, server: S)
where
    C: ConnectionStream,
    S: Server,
{
    let task_name = format!("handle_{}_connection", S::NAME);
    while let Some(conn) = conns.next().await {
        let conn = match conn {
            Ok(conn) => conn,
            Err(err) => {
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
        let fut = server.handle_connection(conn);
        task::spawn(|| &task_name, async {
            if let Err(e) = fut.await {
                debug!(
                    "error handling connection in {}: {}",
                    S::NAME,
                    e.display_with_causes()
                );
            }
        });
    }
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
    pub fn context(&self) -> Result<SslContext, anyhow::Error> {
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
        possible_values = &["disable", "require"],
        default_value = "disable",
        default_value_ifs = &[
            ("frontegg-tenant", None, Some("require")),
            ("frontegg-resolver-template", None, Some("require")),
        ],
        value_name = "MODE",
    )]
    tls_mode: String,
    /// Certificate file for TLS connections.
    #[clap(
        long,
        env = "TLS_CERT",
        requires = "tls-key",
        required_if_eq_any(&[("tls-mode", "require")]),
        value_name = "PATH"
    )]
    tls_cert: Option<PathBuf>,
    /// Private key file for TLS connections.
    #[clap(
        long,
        env = "TLS_KEY",
        requires = "tls-cert",
        required_if_eq_any(&[("tls-mode", "require")]),
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
