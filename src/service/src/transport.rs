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

use std::convert::Infallible;
use std::time::Duration;
use std::{fmt, io};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::BytesMut;
use flate2::Compression;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use futures::future;
use mz_ore::cast::CastInto;
use mz_ore::netio::{Listener, SocketAddr, Stream};
use mz_ore::task::AbortOnDropHandle;
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;
use tracing::{info, trace, warn};

use crate::client::{GenericClient, Partitionable, Partitioned};

pub trait Payload: fmt::Debug + Send + Serialize + DeserializeOwned + 'static {}
impl<T: fmt::Debug + Send + Serialize + DeserializeOwned + 'static> Payload for T {}

/// A client for a CTP connection.
#[derive(Debug)]
pub struct Client<Out, In> {
    conn: Connection<Out, In>,
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
        idle_timeout: Duration,
    ) -> anyhow::Result<Self> {
        let dest_host = host_from_address(address);
        let stream = mz_ore::future::timeout(connect_timeout, Stream::connect(address)).await?;
        info!(%address, "ctp: connected to server");

        let mut conn = Connection::start(stream, idle_timeout);
        conn.handshake(version, dest_host).await?;

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
    Out: Payload,
    In: Payload,
    (Out, In): Partitionable<Out, In>,
{
    /// Create a `Partitioned` client that connects through CTP.
    pub async fn connect_partitioned(
        addresses: Vec<String>,
        version: Version,
        connect_timeout: Duration,
        idle_timeout: Duration,
    ) -> anyhow::Result<Partitioned<Self, Out, In>> {
        let connects = addresses
            .iter()
            .map(|addr| Self::connect(addr, version.clone(), connect_timeout, idle_timeout));
        let clients = future::try_join_all(connects).await?;
        Ok(Partitioned::new(clients))
    }
}

#[async_trait]
impl<Out: Payload, In: Payload> GenericClient<Out, In> for Client<Out, In> {
    async fn send(&mut self, cmd: Out) -> anyhow::Result<()> {
        self.conn.send_payload(cmd).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recv(&mut self) -> anyhow::Result<Option<In>> {
        // `Connection::recv_payload` is documented to be cancel safe.
        self.conn.recv_payload().await.map(Some)
    }
}

/// Spawn a CTP server that serves connections at the given address.
pub async fn serve<In, Out, H>(
    address: SocketAddr,
    version: Version,
    server_fqdn: Option<String>,
    idle_timeout: Duration,
    handler_fn: impl Fn() -> H,
) -> anyhow::Result<()>
where
    In: Payload,
    Out: Payload,
    H: GenericClient<In, Out> + 'static,
{
    let listener = Listener::bind(&address).await?;
    info!(%address, "ctp: listening for client connections");

    loop {
        let (stream, peer) = listener.accept().await?;
        info!(%peer, "ctp: accepted client connection");

        let conn = Connection::start(stream, idle_timeout);
        let handler = handler_fn();
        let version = version.clone();
        let server_fqdn = server_fqdn.clone();

        mz_ore::task::spawn(|| "ctp::connection", async {
            let Err(error) = serve_connection(conn, handler, version, server_fqdn).await;
            info!("ctp: connection failed: {error}");
        });
    }
}

/// Serve a single CTP connection.
async fn serve_connection<In, Out, H>(
    mut conn: Connection<Out, In>,
    mut handler: H,
    version: Version,
    server_fqdn: Option<String>,
) -> anyhow::Result<Infallible>
where
    In: Payload,
    Out: Payload,
    H: GenericClient<In, Out>,
{
    conn.handshake(version, server_fqdn).await?;

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
///  * The keepalive task periodically emits `Keepalive` messages.
///
/// The separation into tasks provides some performance isolation between the sending and the
/// receiving half of the connection.
#[derive(Debug)]
struct Connection<Out, In> {
    /// Message sender connected to the send task.
    msg_tx: mpsc::Sender<Message<Out>>,
    /// Message receiver connected to the receive task.
    msg_rx: mpsc::Receiver<Message<In>>,
    /// Receiver for errors encountered by connection tasks.
    error_rx: watch::Receiver<String>,

    /// Handles to connection tasks.
    _tasks: Vec<AbortOnDropHandle<()>>,
}

impl<Out: Payload, In: Payload> Connection<Out, In> {
    /// The interval with which `Keepalive` messages are emitted.
    const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);
    /// The minimum acceptable idle timeout.
    ///
    /// We want this to be significantly greater than `KEEPALIVE_INTERVAL`, to avoid connections
    /// getting cancel unnecessarily.
    const MIN_TIMEOUT: Duration = Duration::from_secs(2);

    /// Start a new connection wrapping the given stream.
    fn start(stream: Stream, mut timeout: Duration) -> Self {
        if timeout < Self::MIN_TIMEOUT {
            warn!(
                ?timeout,
                "ctp: configured timeout is less than minimum timeout",
            );
            timeout = Self::MIN_TIMEOUT;
        }

        let (stream_rx, stream_tx) = stream.split();
        let (out_tx, out_rx) = mpsc::channel(1024);
        let (in_tx, in_rx) = mpsc::channel(1024);

        // Initialize the error channel with a default error to return if none of the tasks
        // produced an error.
        let (error_tx, error_rx) = watch::channel("connection closed".into());

        let send_task = mz_ore::task::spawn(
            || "ctp::send",
            Self::run_send_task(stream_tx, out_rx, error_tx.clone(), timeout),
        );
        let recv_task = mz_ore::task::spawn(
            || "ctp::recv",
            Self::run_recv_task(stream_rx, in_tx, error_tx, timeout),
        );
        let keepalive_task = mz_ore::task::spawn(
            || "ctp::keepalive",
            Self::run_keepalive_task(out_tx.clone()),
        );

        Self {
            msg_tx: out_tx,
            msg_rx: in_rx,
            error_rx,
            _tasks: vec![
                send_task.abort_on_drop(),
                recv_task.abort_on_drop(),
                keepalive_task.abort_on_drop(),
            ],
        }
    }

    /// Enqueue a message for sending.
    async fn send(&mut self, message: Message<Out>) -> anyhow::Result<()> {
        match self.msg_tx.send(message).await {
            Ok(()) => Ok(()),
            Err(_) => bail!(self.collect_error().await),
        }
    }

    /// Return a received message.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn recv(&mut self) -> anyhow::Result<Message<In>> {
        // `mpcs::Receiver::recv` is documented to be cancel safe.
        match self.msg_rx.recv().await {
            Some(msg) => Ok(msg),
            None => bail!(self.collect_error().await),
        }
    }

    /// Enqueue a payload for sending.
    async fn send_payload(&mut self, payload: Out) -> anyhow::Result<()> {
        self.send(Message::Payload(payload)).await
    }

    /// Return a received payload.
    ///
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

    /// Return a connection error.
    async fn collect_error(&mut self) -> String {
        // Wait for the first error to be reported, or for all connection tasks to shut down.
        let _ = self.error_rx.changed().await;
        // Mark the current value as unseen, so the next `collect_error` call can return
        // immediately.
        self.error_rx.mark_changed();

        self.error_rx.borrow().clone()
    }

    /// Perform the CTP handshake.
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

    /// Run a connection's send task.
    async fn run_send_task<W: AsyncWrite + Unpin>(
        mut writer: W,
        mut msg_rx: mpsc::Receiver<Message<Out>>,
        error_tx: watch::Sender<String>,
        timeout: Duration,
    ) {
        while let Some(msg) = msg_rx.recv().await {
            trace!(?msg, "ctp: sending message");

            let result = Self::write_message(&mut writer, msg, timeout).await;
            if let Err(error) = result {
                info!("ctp: send error: {error}");
                let _ = error_tx.send(error.to_string());
                break;
            };
        }
    }

    /// Run a connection's recv task.
    async fn run_recv_task<R: AsyncRead + Unpin>(
        mut reader: R,
        msg_tx: mpsc::Sender<Message<In>>,
        error_tx: watch::Sender<String>,
        timeout: Duration,
    ) {
        loop {
            let result = Self::read_message(&mut reader, timeout).await;
            match result {
                Ok(msg) => {
                    trace!(?msg, "ctp: received message");

                    if msg_tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    info!("ctp: recv error: {error}");
                    let _ = error_tx.send(error.to_string());
                    break;
                }
            };
        }
    }

    async fn run_keepalive_task(tx: mpsc::Sender<Message<Out>>) {
        let mut interval = tokio::time::interval(Self::KEEPALIVE_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval.reset();

        loop {
            interval.tick().await;
            if tx.send(Message::Keepalive).await.is_err() {
                break;
            }
        }
    }

    /// Write a message onto the wire.
    async fn write_message<W: AsyncWrite + Unpin>(
        mut writer: W,
        msg: Message<Out>,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let bytes = msg.wire_encode()?;

        let len = bytes.len().cast_into();
        mz_ore::future::timeout(timeout, writer.write_u64(len)).await?;

        // Rather than using `write_all` we perform a chunked write, to avoid timing out when
        // sending large messages.
        let mut bytes = &bytes[..];
        while !bytes.is_empty() {
            let n = mz_ore::future::timeout(timeout, writer.write(bytes)).await?;
            if n == 0 {
                Err(io::Error::from(io::ErrorKind::WriteZero))?;
            }
            bytes = &bytes[n..];
        }

        Ok(())
    }

    /// Read a message from the wire.
    async fn read_message<R: AsyncRead + Unpin>(
        mut reader: R,
        timeout: Duration,
    ) -> anyhow::Result<Message<In>> {
        let len = mz_ore::future::timeout(timeout, reader.read_u64()).await?;
        let len: usize = len.cast_into();

        // Rather than using `recv_exact` we perform a chunked read, to avoid timing out when
        // receiving large messages.
        let mut bytes = BytesMut::with_capacity(len);
        while bytes.len() < len {
            let n = mz_ore::future::timeout(timeout, reader.read_buf(&mut bytes)).await?;
            if n == 0 {
                Err(io::Error::from(io::ErrorKind::UnexpectedEof))?;
            }
        }

        let msg = Message::wire_decode(&bytes)?;

        Ok(msg)
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
        // `mpsc::UnboundedReceiver::recv` is cancel safe.
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
    /// On connections that are otherwise idle, each endpoint sends a `Keepalive` every second, to
    /// ensure the peer's idle timeout doesn't trigger and the connection stays alive.
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
}
