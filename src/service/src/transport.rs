// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The Cluster Transport Protocol (CTP).
//!
//! CTP is the protocol used to transmit commands from controllers to replicas and responses from
//! replicas to controllers. It runs on top of a reliable bidirectional connection stream, as
//! provided by TCP or UDS, and adds message framing as well as heartbeating.
//!
//! CTP supports any message type that implements the serde [`Serialize`] and [`Deserialize`]
//! traits. Messages are encoded using the [`bincode`] format and then sent over the wire with a
//! length prefix.
//!
//! A CTP server only serves a single client at a time. If a new client connects while a connection
//! is already established, the previous connection is canceled once the new client has passed
//! transport security (see [`tls`]).

mod metrics;
pub mod tls;

use std::convert::Infallible;
use std::fmt::Debug;
use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;
use bincode::Options;
use futures::future;
use mz_ore::cast::CastInto;
use mz_ore::netio::{Listener, SocketAddr, Stream, TimedReader, TimedWriter};
use mz_ore::task::{AbortOnDropHandle, JoinHandle};
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{Instrument, debug, info, trace, warn};

use crate::client::{GenericClient, Partitionable, Partitioned};
use crate::transport::tls::{ClientTlsConfig, ServerTlsConfig};

pub use metrics::{ClusterServerMetrics, Metrics, NoopMetrics, PerClusterServerMetrics};

/// Trait for messages that can be sent over CTP.
pub trait Message: Debug + Send + Sync + Serialize + DeserializeOwned + 'static {}
impl<T: Debug + Send + Sync + Serialize + DeserializeOwned + 'static> Message for T {}

/// A client for a CTP connection.
#[derive(Debug)]
pub struct Client<Out, In> {
    conn: Connection<Out, In>,
}

impl<Out: Message, In: Message> Client<Out, In> {
    /// Connect to the server at the given address.
    ///
    /// This call resolves once a connection with the server host was either established, was
    /// rejected, or timed out.
    ///
    /// If `tls` is set, a mutual TLS handshake is performed before any CTP bytes are exchanged.
    /// `connect_timeout` bounds the stream connect and the TLS handshake individually, not
    /// jointly.
    pub async fn connect(
        address: &str,
        version: Version,
        tls: Option<ClientTlsConfig>,
        connect_timeout: Duration,
        idle_timeout: Duration,
        metrics: impl Metrics<Out, In>,
    ) -> anyhow::Result<Self> {
        let dest_host = host_from_address(address);
        let stream = mz_ore::future::timeout(connect_timeout, Stream::connect(address)).await?;
        info!(%address, "ctp: connected to server");

        let conn = match tls {
            None => {
                let (reader, writer) = stream.split();
                Connection::start(reader, writer, version, dest_host, idle_timeout, metrics).await?
            }
            Some(tls) => {
                let stream = mz_ore::future::timeout(connect_timeout, tls.connect(stream)).await?;
                info!(%address, "ctp: TLS handshake with server complete");
                let (reader, writer) = tokio::io::split(stream);
                Connection::start(reader, writer, version, dest_host, idle_timeout, metrics).await?
            }
        };
        Ok(Self { conn })
    }
}

/// Helper function to extract the host part from an address string.
///
/// This function assumes addresses to be of the form `<host>:<port>` or `<protocol>:<host>:<port>`
/// and yields `None` otherwise.
fn host_from_address(address: &str) -> Option<String> {
    let mut p = address.split(':');
    let (host, port) = match (p.next(), p.next(), p.next(), p.next()) {
        (Some(host), Some(port), None, None) => (host, port),
        (Some(_protocol), Some(host), Some(port), None) => (host, port),
        _ => return None,
    };

    let _: u16 = port.parse().ok()?;
    Some(host.into())
}

impl<Out, In> Client<Out, In>
where
    Out: Message,
    In: Message,
    (Out, In): Partitionable<Out, In>,
{
    /// Create a `Partitioned` client that connects through CTP.
    ///
    /// All partitions share the same TLS config: the processes of a replica present a single
    /// replica-level identity.
    pub async fn connect_partitioned(
        addresses: Vec<String>,
        version: Version,
        tls: Option<ClientTlsConfig>,
        connect_timeout: Duration,
        idle_timeout: Duration,
        metrics: impl Metrics<Out, In>,
    ) -> anyhow::Result<Partitioned<Self, Out, In>> {
        let connects = addresses.iter().map(|addr| {
            Self::connect(
                addr,
                version.clone(),
                tls.clone(),
                connect_timeout,
                idle_timeout,
                metrics.clone(),
            )
        });
        let clients = future::try_join_all(connects).await?;
        Ok(Partitioned::new(clients))
    }
}

#[async_trait]
impl<Out: Message, In: Message> GenericClient<Out, In> for Client<Out, In> {
    async fn send(&mut self, cmd: Out) -> anyhow::Result<()> {
        self.conn.send(cmd).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recv(&mut self) -> anyhow::Result<Option<In>> {
        // `Connection::recv` is documented to be cancel safe.
        self.conn.recv().await.map(Some)
    }
}

/// The time within which a client must complete the TLS handshake.
///
/// Bounded separately from the idle timeout (which may be unlimited on servers) so a stalled or
/// non-TLS peer cannot hold handshake state indefinitely.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// A stream that has passed transport security, ready for CTP.
enum SecuredStream {
    Plain(Stream),
    Tls(Box<tokio_rustls::server::TlsStream<Stream>>),
}

/// Spawn a CTP server that serves connections at the given address.
///
/// If `tls` is set, each connection must complete a mutual TLS handshake, including client
/// identity verification, before any CTP bytes are exchanged.
///
/// A new client displaces the current one only once it has passed transport security. With TLS
/// enabled, an unauthenticated peer reaching the listen port can thus not interrupt the
/// established connection. Without TLS, displacement happens at accept time, as there is no
/// authentication to wait for.
pub async fn serve<In, Out, H>(
    address: SocketAddr,
    version: Version,
    server_fqdn: Option<String>,
    tls: Option<ServerTlsConfig>,
    idle_timeout: Duration,
    handler_fn: impl Fn() -> H,
    metrics: impl Metrics<Out, In>,
) -> anyhow::Result<()>
where
    In: Message,
    Out: Message,
    H: GenericClient<In, Out> + 'static,
{
    // Keep a handle to the task serving the current connection, as well as a cancelation token, so
    // we can cancel it when a new client connects.
    //
    // Note that we cannot simply abort the previous connection task because its future isn't known
    // to be cancel safe. Instead we pass the connection tasks a cancelation token and wait for
    // them to shut themselves down gracefully once the token gets dropped.
    let mut connection_task: Option<(JoinHandle<()>, oneshot::Sender<()>)> = None;

    let listener = Listener::bind(&address).await?;
    info!(%address, "ctp: listening for client connections");

    // Channel over which TLS handshake tasks deliver authenticated streams.
    let (secured_tx, mut secured_rx) = mpsc::unbounded_channel();

    loop {
        let secured = tokio::select! {
            // `Listener::accept` wraps `tokio` accepts, which are cancel safe.
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                info!(%peer, "ctp: accepted client connection");

                match &tls {
                    None => SecuredStream::Plain(stream),
                    Some(tls) => {
                        // Perform the handshake in a task, so a slow or stalled handshake never
                        // blocks the accept loop or the established connection. Multiple
                        // handshakes may be in flight at once; each is bounded by the handshake
                        // timeout.
                        let tls = tls.clone();
                        let secured_tx = secured_tx.clone();
                        let mut metrics = metrics.clone();
                        let span = tracing::Span::current();
                        mz_ore::task::spawn(
                            || "ctp::tls_handshake",
                            async move {
                                match tls.accept(stream, TLS_HANDSHAKE_TIMEOUT).await {
                                    Ok(stream) => {
                                        info!(%peer, "ctp: TLS handshake with client complete");
                                        let _ = secured_tx
                                            .send(SecuredStream::Tls(Box::new(stream)));
                                    }
                                    Err(error) => {
                                        metrics.tls_handshake_failure();
                                        info!(%peer, "ctp: TLS handshake failed: {error:#}");
                                    }
                                }
                            }
                            .instrument(span),
                        );
                        continue;
                    }
                }
            }
            // `mpsc::UnboundedReceiver::recv` is cancel safe. The channel can never be closed
            // because we hold on to a sender above.
            secured = secured_rx.recv() => secured.expect("sender exists"),
        };

        // Cancel any existing connection before starting to serve the new one.
        if let Some((task, token)) = connection_task.take() {
            drop(token);
            task.await;
        }

        let handler = handler_fn();
        let version = version.clone();
        let server_fqdn = server_fqdn.clone();
        let metrics = metrics.clone();
        let (cancel_tx, cancel_rx) = oneshot::channel();

        let span = tracing::Span::current();
        let handle = mz_ore::task::spawn(
            || "ctp::connection",
            async move {
                let Err(error) = serve_connection(
                    secured,
                    handler,
                    version,
                    server_fqdn,
                    idle_timeout,
                    cancel_rx,
                    metrics,
                )
                .await;
                info!("ctp: connection failed: {error}");
            }
            .instrument(span),
        );

        connection_task = Some((handle, cancel_tx));
    }
}

/// Serve a single CTP connection.
async fn serve_connection<In, Out, H>(
    stream: SecuredStream,
    handler: H,
    version: Version,
    server_fqdn: Option<String>,
    timeout: Duration,
    cancel_rx: oneshot::Receiver<()>,
    metrics: impl Metrics<Out, In>,
) -> anyhow::Result<Infallible>
where
    In: Message,
    Out: Message,
    H: GenericClient<In, Out>,
{
    let conn = match stream {
        SecuredStream::Plain(stream) => {
            let (reader, writer) = stream.split();
            Connection::start(reader, writer, version, server_fqdn, timeout, metrics).await?
        }
        SecuredStream::Tls(stream) => {
            let (reader, writer) = tokio::io::split(*stream);
            Connection::start(reader, writer, version, server_fqdn, timeout, metrics).await?
        }
    };

    handle_connection(conn, handler, cancel_rx).await
}

/// Drive an established CTP connection with the given handler.
async fn handle_connection<In, Out, H>(
    mut conn: Connection<Out, In>,
    mut handler: H,
    cancel_rx: oneshot::Receiver<()>,
) -> anyhow::Result<Infallible>
where
    In: Message,
    Out: Message,
    H: GenericClient<In, Out>,
{
    let mut cancel_rx = cancel_rx;
    loop {
        tokio::select! {
            // `Connection::recv` is documented to be cancel safe.
            inbound = conn.recv() => {
                let msg = inbound?;
                handler.send(msg).await?;
            },
            // `GenericClient::recv` is documented to be cancel safe.
            outbound = handler.recv() => match outbound? {
                Some(msg) => conn.send(msg).await?,
                None => bail!("client disconnected"),
            },
            _ = &mut cancel_rx => bail!("connection canceled"),
        }
    }
}

/// An active CTP connection.
///
/// This type encapsulates the core connection logic. It is used by both the client and the server
/// implementation, with swapped `Out`/`In` types.
///
/// Each connection spawns two tasks:
///
///  * The send task is responsible for encoding and sending enqueued messages.
///  * The recv task is responsible for receiving and decoding messages from the peer.
///
/// The separation into tasks provides some performance isolation between the sending and the
/// receiving half of the connection.
#[derive(Debug)]
struct Connection<Out, In> {
    /// Message sender connected to the send task.
    msg_tx: mpsc::UnboundedSender<Out>,
    /// Message receiver connected to the receive task.
    msg_rx: mpsc::UnboundedReceiver<In>,
    /// Receiver for errors encountered by connection tasks.
    error_rx: watch::Receiver<String>,

    /// Handles to connection tasks.
    _tasks: [AbortOnDropHandle<()>; 2],
}

impl<Out: Message, In: Message> Connection<Out, In> {
    /// The interval with which keepalives are emitted on idle connections.
    const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);
    /// The minimum acceptable idle timeout.
    ///
    /// We want this to be significantly greater than `KEEPALIVE_INTERVAL`, to avoid connections
    /// getting canceled unnecessarily.
    const MIN_TIMEOUT: Duration = Duration::from_secs(2);

    /// Start a new connection wrapping the given stream halves.
    ///
    /// The halves may come from a plain [`Stream`] or from a TLS stream wrapped around one. Any
    /// transport security handshake must have completed already. The CTP handshake runs here, on
    /// (and, with TLS, inside) the secured stream.
    async fn start<R, W>(
        reader: R,
        writer: W,
        version: Version,
        server_fqdn: Option<String>,
        mut timeout: Duration,
        metrics: impl Metrics<Out, In>,
    ) -> anyhow::Result<Self>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        if timeout < Self::MIN_TIMEOUT {
            warn!(
                ?timeout,
                "ctp: configured timeout is less than minimum timeout",
            );
            timeout = Self::MIN_TIMEOUT;
        }

        // Apply the timeout to all connection reads and writes.
        let reader = TimedReader::new(reader, timeout);
        let writer = TimedWriter::new(writer, timeout);
        // Track byte count metrics for all connection reads and writes.
        let mut reader = metrics::Reader::new(reader, metrics.clone());
        let mut writer = metrics::Writer::new(writer, metrics.clone());

        handshake(&mut reader, &mut writer, version, server_fqdn).await?;

        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (in_tx, in_rx) = mpsc::unbounded_channel();
        // Initialize the error channel with a default error to return if none of the tasks
        // produced an error.
        let (error_tx, error_rx) = watch::channel("connection closed".into());

        let span = tracing::Span::current();
        let send_task = mz_ore::task::spawn(
            || "ctp::send",
            Self::run_send_task(writer, out_rx, error_tx.clone(), metrics.clone())
                .instrument(span.clone()),
        );
        let recv_task = mz_ore::task::spawn(
            || "ctp::recv",
            Self::run_recv_task(reader, in_tx, error_tx, metrics).instrument(span),
        );

        Ok(Self {
            msg_tx: out_tx,
            msg_rx: in_rx,
            error_rx,
            _tasks: [send_task.abort_on_drop(), recv_task.abort_on_drop()],
        })
    }

    /// Enqueue a message for sending.
    async fn send(&mut self, msg: Out) -> anyhow::Result<()> {
        match self.msg_tx.send(msg) {
            Ok(()) => Ok(()),
            Err(_) => bail!(self.collect_error().await),
        }
    }

    /// Return a received message.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recv(&mut self) -> anyhow::Result<In> {
        // `mpcs::Receiver::recv` is documented to be cancel safe.
        match self.msg_rx.recv().await {
            Some(msg) => Ok(msg),
            None => bail!(self.collect_error().await),
        }
    }

    /// Return a connection error.
    async fn collect_error(&mut self) -> String {
        // Wait for the first error to be reported, or for all connection tasks to shut down.
        let _ = self.error_rx.changed().await;
        // Mark the current value as unseen, so the next `collect_error` call can return
        // immediately.
        self.error_rx.mark_changed();

        self.error_rx.borrow().clone()
    }

    /// Run a connection's send task.
    async fn run_send_task<W: AsyncWrite + Unpin>(
        mut writer: W,
        mut msg_rx: mpsc::UnboundedReceiver<Out>,
        error_tx: watch::Sender<String>,
        mut metrics: impl Metrics<Out, In>,
    ) {
        loop {
            let msg = tokio::select! {
                // `mpsc::UnboundedReceiver::recv` is cancel safe.
                msg = msg_rx.recv() => match msg {
                    Some(msg) => {
                        trace!(?msg, "ctp: sending message");
                        Some(msg)
                    }
                    None => break,
                },
                // `tokio::time::sleep` is cancel safe.
                _ = tokio::time::sleep(Self::KEEPALIVE_INTERVAL) => {
                    trace!("ctp: sending keepalive");
                    None
                },
            };

            if let Err(error) = write_message(&mut writer, msg.as_ref()).await {
                debug!("ctp: send error: {error}");
                let _ = error_tx.send(error.to_string());
                break;
            };

            if let Some(msg) = &msg {
                metrics.message_sent(msg);
            }
        }
    }

    /// Run a connection's recv task.
    async fn run_recv_task<R: AsyncRead + Unpin>(
        mut reader: R,
        msg_tx: mpsc::UnboundedSender<In>,
        error_tx: watch::Sender<String>,
        mut metrics: impl Metrics<Out, In>,
    ) {
        loop {
            match read_message(&mut reader).await {
                Ok(msg) => {
                    trace!(?msg, "ctp: received message");
                    metrics.message_received(&msg);

                    if msg_tx.send(msg).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    debug!("ctp: recv error: {error}");
                    let _ = error_tx.send(error.to_string());
                    break;
                }
            };
        }
    }
}

/// Perform the CTP handshake.
///
/// To perform the handshake, each endpoint sends the protocol magic number, followed by a
/// `Hello` message. The `Hello` message contains information about the originating endpoint that
/// is used by the receiver to validate compatibility with its peer. Only if both endpoints
/// determine that they are compatible does the handshake succeed.
async fn handshake<R, W>(
    mut reader: R,
    mut writer: W,
    version: Version,
    server_fqdn: Option<String>,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    /// A randomly chosen magic number identifying CTP connections.
    const MAGIC: u64 = 0x477574656e546167;

    writer.write_u64(MAGIC).await?;

    let hello = Hello {
        version: version.clone(),
        server_fqdn: server_fqdn.clone(),
    };
    write_message(&mut writer, Some(&hello)).await?;

    let peer_magic = reader.read_u64().await?;
    if peer_magic != MAGIC {
        bail!("invalid protocol magic: {peer_magic:#x}");
    }

    let Hello {
        version: peer_version,
        server_fqdn: peer_server_fqdn,
    } = read_message(&mut reader).await?;

    if peer_version != version {
        bail!("version mismatch: {peer_version} != {version}");
    }
    if let (Some(other), Some(mine)) = (&peer_server_fqdn, &server_fqdn) {
        if other != mine {
            bail!("server FQDN mismatch: {other} != {mine}");
        }
    }

    Ok(())
}

/// A message for exchanging compatibility information during the CTP handshake.
#[derive(Debug, Serialize, Deserialize)]
struct Hello {
    /// The version of the originating endpoint.
    version: Version,
    /// The FQDN of the server endpoint.
    server_fqdn: Option<String>,
}

/// Write a message into the given writer.
///
/// The message can be `None`, in which case an empty message is written. This is used to implement
/// keepalives. At the receiver, empty messages are ignored, but they do reset the read timeout.
async fn write_message<W, M>(mut writer: W, msg: Option<&M>) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
    M: Message,
{
    let bytes = match msg {
        Some(msg) => &*wire_encode(msg)?,
        None => &[],
    };

    let len = bytes.len().cast_into();
    writer.write_u64(len).await?;
    writer.write_all(bytes).await?;

    // NOTE: The flush is load-bearing for TLS connections and must not be dropped as redundant.
    // A raw socket write reaches the peer on its own, but a TLS writer buffers plaintext into
    // records and is not obliged to emit them until flushed. Without this, a message (including
    // the CTP handshake) can sit in the TLS buffer indefinitely and both endpoints wait forever.
    writer.flush().await?;

    Ok(())
}

/// Read a message from the given reader.
async fn read_message<R, M>(mut reader: R) -> anyhow::Result<M>
where
    R: AsyncRead + Unpin,
    M: Message,
{
    // Skip over any empty messages (i.e. keepalives).
    let mut len = 0;
    while len == 0 {
        len = reader.read_u64().await?;
    }

    let mut bytes = vec![0; len.cast_into()];
    reader.read_exact(&mut bytes).await?;

    wire_decode(&bytes)
}

/// Encode a message for wire transport.
fn wire_encode<M: Message>(msg: &M) -> anyhow::Result<Vec<u8>> {
    let bytes = bincode::DefaultOptions::new().serialize(msg)?;
    Ok(bytes)
}

/// Decode a wire frame back into a message.
fn wire_decode<M: Message>(bytes: &[u8]) -> anyhow::Result<M> {
    let msg = bincode::DefaultOptions::new().deserialize(bytes)?;
    Ok(msg)
}
