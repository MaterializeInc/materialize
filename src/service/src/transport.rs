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
//! CTP supports any payload type that implements the serde [`Serialize`] and [`Deserialize`]
//! traits. Messages are encoded using the [`bincode`] format, compressed, and then sent over the
//! wire with a length prefix.
//!
//! A CTP server only serves a single client at a time. If a new client connects while a connection
//! is already established, the previous connection is canceled.

use std::convert::Infallible;
use std::time::Duration;
use std::{fmt, io, mem};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::BytesMut;
use flate2::Compression;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use futures::{FutureExt, future};
use mz_ore::cast::{CastFrom, CastInto};
use mz_ore::netio::{Listener, SocketAddr, Stream};
use mz_ore::task::{AbortOnDropHandle, JoinHandle, JoinHandleExt};
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tracing::{info, trace, warn};

use crate::client::{GenericClient, Partitionable, Partitioned};

pub trait Payload: fmt::Debug + Send + Serialize + DeserializeOwned + 'static {}
impl<T: fmt::Debug + Send + Serialize + DeserializeOwned + 'static> Payload for T {}

/// A client for a CTP connection.
#[derive(Debug)]
pub struct Client<Out, In> {
    connection: Connection<Out, In>,
}

impl<Out: Payload, In: Payload> Client<Out, In> {
    /// Connect to the server at the given address.
    ///
    /// This call resolves once a connection with the server host was either established, was
    /// rejected, or timed out.
    pub async fn connect(
        address: &str,
        version: Version,
        connect_timeout: Duration,
        keepalive_timeout: Duration,
        metrics: impl Metrics<Out, In>,
    ) -> anyhow::Result<Self> {
        let dest_host = host_from_address(address);
        let stream = mz_ore::future::timeout(connect_timeout, Stream::connect(address)).await?;
        info!(%address, "ctp: connected to server");

        let mut connection = Connection::start(stream, keepalive_timeout, metrics);
        connection.handshake(version, dest_host).await?;

        Ok(Self { connection })
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
    Out: Payload,
    In: Payload,
    (Out, In): Partitionable<Out, In>,
{
    /// Create a `Partitioned` client that connects through CTP.
    pub async fn connect_partitioned(
        addresses: Vec<String>,
        version: Version,
        connect_timeout: Duration,
        keepalive_timeout: Duration,
        metrics: impl Metrics<Out, In>,
    ) -> anyhow::Result<Partitioned<Self, Out, In>> {
        let connects = addresses.iter().map(|addr| {
            Self::connect(
                addr,
                version.clone(),
                connect_timeout,
                keepalive_timeout,
                metrics.clone(),
            )
        });
        let clients = future::try_join_all(connects).await?;
        Ok(Partitioned::new(clients))
    }
}

#[async_trait]
impl<Out: Payload, In: Payload> GenericClient<Out, In> for Client<Out, In> {
    async fn send(&mut self, cmd: Out) -> anyhow::Result<()> {
        self.connection.send_payload(cmd).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recv(&mut self) -> anyhow::Result<Option<In>> {
        // `Connection::recv_payload` is documented to be cancel safe.
        self.connection.recv_payload().await.map(Some)
    }
}

/// Spawn a CTP server that serves connections at the given address.
pub async fn serve<In, Out, H>(
    address: SocketAddr,
    version: Version,
    server_fqdn: Option<String>,
    keepalive_timeout: Duration,
    handler_fn: impl Fn() -> H,
    metrics: impl Metrics<Out, In>,
) -> anyhow::Result<()>
where
    In: Payload,
    Out: Payload,
    H: GenericClient<In, Out> + 'static,
{
    // Keep a handle to the task serving the current connection, as well as a cancelation token, so
    // we can cancel it when a new client connects.
    //
    // Note that we cannot simply abort the previous connection task because its future isn't known
    // to be cancel safe. Instead we pass the connection task a cancelation token and wait for it
    // to shut itself down gracefully once the token gets dropped.
    let mut connection_task: Option<(JoinHandle<()>, oneshot::Sender<()>)> = None;

    let listener = Listener::bind(&address).await?;
    info!(%address, "ctp: listening for client connections");

    loop {
        let (stream, peer) = listener.accept().await?;
        info!(%peer, "ctp: accepted client connection");

        // Cancel any existing connection before starting to serve the new one.
        if let Some((task, token)) = connection_task.take() {
            drop(token);
            task.wait_and_assert_finished().await;
        }

        let conn = Connection::start(stream, keepalive_timeout, metrics.clone());
        let handler = handler_fn();
        let version = version.clone();
        let server_fqdn = server_fqdn.clone();

        let (cancel_tx, cancel_rx) = oneshot::channel();
        let cancel_signal = cancel_rx.map(|_| ());

        let handle = mz_ore::task::spawn(|| "ctp::connection", async move {
            let Err(error) =
                serve_connection(conn, handler, version, server_fqdn, cancel_signal).await;
            info!("ctp: connection failed: {error}");
        });
        connection_task = Some((handle, cancel_tx));
    }
}

async fn serve_connection<In, Out, H>(
    mut conn: Connection<Out, In>,
    mut handler: H,
    version: Version,
    server_fqdn: Option<String>,
    cancel_signal: impl Future<Output = ()>,
) -> anyhow::Result<Infallible>
where
    In: Payload,
    Out: Payload,
    H: GenericClient<In, Out>,
{
    conn.handshake(version, server_fqdn).await?;

    let mut cancel_signal = Some(Box::pin(cancel_signal));

    loop {
        tokio::select! {
            // `Connection::recv_payload` is documented to be cancel safe.
            inbound = conn.recv_payload() => {
                let payload = inbound?;
                handler.send(payload).await?;
            },
            // `GenericClient::recv` is documented to be cancel safe.
            outbound = handler.recv() => match outbound? {
                Some(payload) => conn.send_payload(payload).await?,
                None => bail!("client disconnected"),
            },
            _ = cancel_signal.as_mut().unwrap() => bail!("connection canceled"),
        }
    }
}

/// A trait for types that observe connection metric events.
pub trait Metrics<Out, In>: Clone + Send + 'static {
    fn message_sent(&mut self, bytes: u64, payload: Option<&Out>);
    fn message_received(&mut self, bytes: u64, payload: Option<&In>);
}

/// Dummy [`Metrics`] implementation that ignores all events.
#[derive(Clone)]
pub struct NoopMetrics;

impl<Out, In> Metrics<Out, In> for NoopMetrics {
    fn message_sent(&mut self, _bytes: u64, _payload: Option<&Out>) {}
    fn message_received(&mut self, _bytes: u64, _payload: Option<&In>) {}
}

/// An active CTP connection.
#[derive(Debug)]
struct Connection<Out, In> {
    /// Sender connected to the send task.
    tx: mpsc::Sender<Message<Out>>,
    /// Receiver connected to the receive task.
    rx: mpsc::Receiver<Message<In>>,

    send_task: AbortOnDropHandle<anyhow::Result<()>>,
    recv_task: AbortOnDropHandle<anyhow::Result<()>>,
    _keepalive_task: AbortOnDropHandle<()>,
}

impl<Out, In> Connection<Out, In>
where
    Out: Payload,
    In: Payload,
{
    const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);

    /// Create a new connection wrapping the given stream.
    fn start(stream: Stream, mut timeout: Duration, metrics: impl Metrics<Out, In>) -> Self {
        let min_timeout = Self::KEEPALIVE_INTERVAL * 2;
        if timeout < min_timeout {
            warn!(
                ?timeout,
                ?min_timeout,
                "ctp: configured timeout less than minimum timeout",
            );
            timeout = min_timeout;
        }

        let (stream_rx, stream_tx) = stream.split();
        let (out_tx, out_rx) = mpsc::channel(1024);
        let (in_tx, in_rx) = mpsc::channel(1024);

        let send_task = mz_ore::task::spawn(
            || "ctp::send",
            Self::run_send_task(stream_tx, out_rx, timeout, metrics.clone()),
        );
        let recv_task = mz_ore::task::spawn(
            || "ctp::recv",
            Self::run_recv_task(stream_rx, in_tx, timeout, metrics),
        );
        let keepalive_task = mz_ore::task::spawn(
            || "ctp::keepalive",
            Self::run_keepalive_task(out_tx.clone()),
        );

        Self {
            tx: out_tx,
            rx: in_rx,
            send_task: send_task.abort_on_drop(),
            recv_task: recv_task.abort_on_drop(),
            _keepalive_task: keepalive_task.abort_on_drop(),
        }
    }

    async fn send(&mut self, message: Message<Out>) -> anyhow::Result<()> {
        self.tx.send(message).await.map_err(|_| self.expect_error())
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recv(&mut self) -> anyhow::Result<Message<In>> {
        // `mpcs::Receiver::recv` is documented to be cancel safe.
        self.rx.recv().await.ok_or_else(|| self.expect_error())
    }

    async fn send_payload(&mut self, payload: Out) -> anyhow::Result<()> {
        self.send(Message::Payload(payload)).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recv_payload(&mut self) -> anyhow::Result<In> {
        loop {
            // `Connection::recv` is documented to be cancel safe.
            match self.recv().await? {
                Message::Payload(payload) => break Ok(payload),
                Message::Keepalive => (),
                Message::Hello { .. } => bail!("received Hello message after handshake"),
            }
        }
    }

    /// Return the connection error reported by a connection task.
    ///
    /// # Panics
    ///
    /// Panics if no task has encountered an error has been reported.
    fn expect_error(&mut self) -> anyhow::Error {
        if let Some(Ok(Err(error))) = (&mut self.send_task).now_or_never() {
            error
        } else if let Some(Ok(Err(error))) = (&mut self.recv_task).now_or_never() {
            error
        } else {
            panic!("expected connection error");
        }
    }

    /// Perform the protocol handshake.
    ///
    /// The handshake consists of each endpoint sending a `Hello` message and receiving one in
    /// return. The `Hello` message contains information about the originating endpoint that is
    /// used by the receiver to validate compatibility with its peer. Only if both endpoints
    /// determine that they are compatible does the handshake succeed.
    async fn handshake(
        &mut self,
        version: Version,
        server_fqdn: Option<String>,
    ) -> anyhow::Result<()> {
        self.send(Message::Hello {
            version: version.clone(),
            server_fqdn: server_fqdn.clone(),
        })
        .await?;

        let (peer_version, peer_server_fqdn) = loop {
            match self.recv().await? {
                Message::Hello {
                    version,
                    server_fqdn,
                } => break (version, server_fqdn),
                Message::Keepalive => (),
                msg => bail!("expected Hello message, got {msg:?}"),
            }
        };

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

    async fn run_send_task<W: AsyncWrite + Unpin>(
        mut writer: W,
        mut rx: mpsc::Receiver<Message<Out>>,
        timeout: Duration,
        mut metrics: impl Metrics<Out, In>,
    ) -> anyhow::Result<()> {
        let mut send = async |msg: Message<Out>| -> anyhow::Result<()> {
            let bytes = msg.wire_encode()?;

            let len = bytes.len().cast_into();
            mz_ore::future::timeout(timeout, writer.write_u64(len)).await?;

            let mut bytes = &bytes[..];
            while !bytes.is_empty() {
                let n = mz_ore::future::timeout(timeout, writer.write(bytes)).await?;
                if n == 0 {
                    Err(io::Error::from(io::ErrorKind::WriteZero))?;
                }
                bytes = &bytes[n..];
            }

            let bytes_sent = u64::cast_from(mem::size_of::<u64>()) + len;
            metrics.message_sent(bytes_sent, msg.payload());

            Ok(())
        };

        while let Some(message) = rx.recv().await {
            trace!(?message, "ctp: sending message");

            send(message).await.inspect_err(|error| {
                info!("ctp: send error: {error}");
            })?;
        }

        Ok(())
    }

    async fn run_recv_task<R: AsyncRead + Unpin>(
        mut reader: R,
        tx: mpsc::Sender<Message<In>>,
        timeout: Duration,
        mut metrics: impl Metrics<Out, In>,
    ) -> anyhow::Result<()> {
        let mut recv = async || -> anyhow::Result<Message<In>> {
            let len = mz_ore::future::timeout(timeout, reader.read_u64()).await?;
            let len: usize = len.cast_into();

            let mut bytes = BytesMut::with_capacity(len);
            while bytes.len() < len {
                let n = mz_ore::future::timeout(timeout, reader.read_buf(&mut bytes)).await?;
                if n == 0 {
                    Err(io::Error::from(io::ErrorKind::UnexpectedEof))?;
                }
            }

            let msg = Message::wire_decode(&bytes)?;

            let bytes_received = u64::cast_from(mem::size_of::<u64>() + len);
            metrics.message_received(bytes_received, msg.payload());

            Ok(msg)
        };

        loop {
            let message = recv().await.inspect_err(|error| {
                info!("ctp: recv error: {error}");
            })?;

            trace!(?message, "ctp: received message");

            if tx.send(message).await.is_err() {
                break;
            }
        }

        Ok(())
    }

    async fn run_keepalive_task(tx: mpsc::Sender<Message<Out>>) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval.reset();

        loop {
            interval.tick().await;
            if tx.send(Message::Keepalive).await.is_err() {
                break;
            }
        }
    }
}

/// A connection handler that simply forwards messages over channels.
#[derive(Debug)]
pub struct ChannelHandler<In, Out> {
    tx: mpsc::UnboundedSender<In>,
    rx: mpsc::UnboundedReceiver<Out>,
}

impl<In, Out> ChannelHandler<In, Out> {
    pub fn new(tx: mpsc::UnboundedSender<In>, rx: mpsc::UnboundedReceiver<Out>) -> Self {
        Self { tx, rx }
    }
}

#[async_trait]
impl<In: Payload, Out: Payload> GenericClient<In, Out> for ChannelHandler<In, Out> {
    async fn send(&mut self, cmd: In) -> anyhow::Result<()> {
        let result = self.tx.send(cmd);
        result.map_err(|_| anyhow!("client channel disconnected"))
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recv(&mut self) -> anyhow::Result<Option<Out>> {
        // `mpsc::UnboundedReceiver::recv` is documented to be cancel safe.
        match self.rx.recv().await {
            Some(resp) => Ok(Some(resp)),
            None => bail!("client channel disconnected"),
        }
    }
}

/// A CTP message.
#[derive(Debug, Serialize, Deserialize)]
enum Message<P> {
    /// A handshake message.
    ///
    /// Each endpoint sends exactly one `Hello` message as the first message of the protocol,
    /// announcing compatibility information about itself.
    Hello {
        /// The version of the originating endpoint.
        version: Version,
        /// The FQDN of the server endpoint.
        server_fqdn: Option<String>,
    },
    /// A massage carrying application payload.
    Payload(P),
    /// A keepalive message.
    ///
    /// On connections that are otherwise idle, each endpoint sends a keepalive every second, to
    /// ensure the peer's receive timeout doesn't trigger and the connection stays alive.
    Keepalive,
}

impl<P: Payload> Message<P> {
    /// Encode this message for wire transport.
    fn wire_encode(&self) -> anyhow::Result<Vec<u8>> {
        let mut compressor = DeflateEncoder::new(Vec::new(), Compression::default());
        bincode::serialize_into(&mut compressor, self)?;
        let bytes = compressor.finish()?;

        Ok(bytes)
    }

    /// Decode a wire package back into a message.
    fn wire_decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut decompressor = DeflateDecoder::new(bytes);
        let msg = bincode::deserialize_from(&mut decompressor)?;
        Ok(msg)
    }

    /// Return the wrapped payload, if any.
    fn payload(&self) -> Option<&P> {
        match self {
            Self::Payload(p) => Some(p),
            _ => None,
        }
    }
}
