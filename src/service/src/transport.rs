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
use std::fmt;
use std::marker::PhantomData;
use std::pin::pin;
use std::time::Duration;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use flate2::Compression;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use futures::stream::StreamExt;
use futures::{FutureExt, future};
use mz_ore::cast::CastInto;
use mz_ore::netio::{Listener, SocketAddr, Stream};
use mz_ore::task::{JoinHandle, JoinHandleExt};
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, trace};

use crate::client::{GenericClient, Partitionable, Partitioned};

pub trait Payload: fmt::Debug + Send + Serialize + DeserializeOwned + 'static {}
impl<T: fmt::Debug + Send + Serialize + DeserializeOwned + 'static> Payload for T {}

/// A client for a CTP connection.
#[derive(Debug)]
pub struct Client<Out, In> {
    /// Sender for outbound messages.
    out_tx: mpsc::UnboundedSender<Out>,
    /// Receiver for inbound messages.
    in_rx: mpsc::UnboundedReceiver<In>,
    /// Channel that receives the error if the connection fails.
    error_rx: Option<oneshot::Receiver<anyhow::Error>>,
}

impl<Out: Payload, In: Payload> Client<Out, In> {
    /// Connect to the server at the given address.
    ///
    /// This call resolves once a connection with the server host was either established or
    /// rejected. If the server doesn't respond, it may never resolve. The `connection_timeout`
    /// only applies to subsequent network operations.
    pub async fn connect(
        address: &str,
        version: Version,
        connection_timeout: Duration,
    ) -> anyhow::Result<Self> {
        let dest_host = host_from_address(address);
        let stream = Stream::connect(address).await?;
        info!(%address, "ctp: connected to server");

        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (error_tx, error_rx) = oneshot::channel();

        mz_ore::task::spawn(|| "ctp::client-connection", async move {
            let conn = Connection::new(stream, version, dest_host, connection_timeout);
            let handler = ChannelHandler::new(in_tx, out_rx);
            let cancel_signal = futures::future::pending();

            let Err(error) = conn.serve(handler, cancel_signal).await;
            info!("ctp: connection failed: {error}");
            let _ = error_tx.send(error);
        });

        Ok(Self {
            out_tx,
            in_rx,
            error_rx: Some(error_rx),
        })
    }

    /// Return the connection error reported by the connection task.
    ///
    /// # Panics
    ///
    /// Panics if no error has been reported.
    fn expect_error(&mut self) -> anyhow::Error {
        if let Some(mut rx) = self.error_rx.take() {
            rx.try_recv().expect("expected connection error")
        } else {
            // An error was reported but we have already taken it in a previous call.
            // Return a default error instead.
            anyhow!("connection closed")
        }
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
        connection_timeout: Duration,
    ) -> anyhow::Result<Partitioned<Self, Out, In>> {
        let connects = addresses
            .iter()
            .map(|addr| Self::connect(addr, version.clone(), connection_timeout));
        let clients = future::try_join_all(connects).await?;
        Ok(Partitioned::new(clients))
    }
}

#[async_trait]
impl<Out: Payload, In: Payload> GenericClient<Out, In> for Client<Out, In> {
    async fn send(&mut self, cmd: Out) -> anyhow::Result<()> {
        self.out_tx.send(cmd).map_err(|_| self.expect_error())
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> anyhow::Result<Option<In>> {
        // `mpsc::UnboundedReceiver::recv` is cancel safe.
        match self.in_rx.recv().await {
            Some(resp) => Ok(Some(resp)),
            None => Err(self.expect_error()),
        }
    }
}

/// Spawn a CTP server that serves connections at the given address.
pub async fn serve<In, Out, H>(
    address: SocketAddr,
    version: Version,
    server_fqdn: Option<String>,
    connection_timeout: Duration,
    handler_fn: impl Fn() -> H,
) -> anyhow::Result<()>
where
    In: Payload,
    Out: Payload,
    H: GenericClient<In, Out> + 'static,
{
    // Keep a handle to the task serving the current connection, as well as a cancelation token, so
    // we can canel it when a new client connects.
    //
    // Note that we cannot simply abort the previous connection task because its future isn't known
    // to be cancel safe. Instead we pass the connection tasks a cancelation token and wait for
    // them to shut themselves down gracefully once the token gets dropped.
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

        let handler = handler_fn();
        let version = version.clone();
        let server_fqdn = server_fqdn.clone();

        let (cancel_tx, cancel_rx) = oneshot::channel();
        let cancel_signal = cancel_rx.map(|_| ());

        let handle = mz_ore::task::spawn(|| "ctp::server-connection", async move {
            let conn = Connection::new(stream, version, server_fqdn, connection_timeout);

            let Err(error) = conn.serve(handler, cancel_signal).await;
            info!("ctp: connection failed: {error}");
        });
        connection_task = Some((handle, cancel_tx));
    }
}

/// An active CTP connection.
#[derive(Debug)]
struct Connection<Out, In> {
    /// The underlying connection to the peer.
    stream: Stream,
    /// The version of this connection endpoint.
    ///
    /// Used during the protocol handshake. Both endpoints are required to have the same version
    /// for the connection to be successfully established.
    version: Version,
    /// The FQDN of the server endpoint, if known.
    ///
    /// Used during the protcol handshake. If both endpoints know a server FQDN, it must match for
    /// the connection to be successfully established.
    server_fqdn: Option<String>,
    /// The timeout for all network operations.
    timeout: Duration,
    _payloads: PhantomData<(Out, In)>,
}

impl<Out: Payload, In: Payload> Connection<Out, In> {
    /// Create a new connection wrapping the given stream.
    fn new(
        stream: Stream,
        version: Version,
        server_fqdn: Option<String>,
        timeout: Duration,
    ) -> Self {
        Self {
            stream,
            version,
            server_fqdn,
            timeout,
            _payloads: PhantomData,
        }
    }

    /// Serve this connection until it fails or is canceled.
    async fn serve<H>(
        mut self,
        mut handler: H,
        cancel_signal: impl Future<Output = ()>,
    ) -> anyhow::Result<Infallible>
    where
        H: GenericClient<In, Out>,
    {
        self.handshake().await?;

        // Send a `Keepalive` message every second on idle connections. We reset the interval every
        // time we send a message, to avoid sending superfluous keepalives on busy connections.
        let mut keepalive_interval = tokio::time::interval(Duration::from_secs(1));
        keepalive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        keepalive_interval.reset();

        // `Connection::recv` is not cancel safe, so we can't use it in a `select!` directly.
        // Instead we wrap it into an async stream and select on `Stream::next`, which is cancel
        // safe.
        let (mut stream_rx, mut stream_tx) = self.stream.split();
        let mut inbound = pin!(async_stream::stream! {
            loop {
                yield Self::recv(&mut stream_rx, self.timeout).await;
            }
        });

        let mut cancel_signal = Some(Box::pin(cancel_signal));

        loop {
            tokio::select! {
                // `Stream::next` is cancel safe: The returned future only holds a reference to the
                // underlying stream, so dropping it will never lose a value.
                Some(inbound) = inbound.next() => match inbound? {
                    Message::Payload(p) => handler.send(p).await?,
                    Message::Keepalive => (),
                    Message::Hello { .. } => bail!("received Hello message after handshake"),
                },
                // `GenericClient::recv` is documented to be cancel safe.
                outbound = handler.recv() => match outbound? {
                    Some(p) => {
                        let msg = Message::Payload(p);
                        Self::send(&mut stream_tx, msg, self.timeout).await?;
                        keepalive_interval.reset();
                    }
                    None => bail!("client disconnected"),
                },
                // `Interval::tick` is documented to be cancel safe.
                _ = keepalive_interval.tick() => {
                    let msg = Message::Keepalive;
                    Self::send(&mut stream_tx, msg, self.timeout).await?;
                }
                _ = cancel_signal.as_mut().unwrap() => bail!("connection canceled"),
            }
        }
    }

    /// Perform the protocol handshake.
    ///
    /// The handshake consists of each endpoint sending a `Hello` message and receiving one in
    /// return. The `Hello` message contains information about the originating endpoint that is
    /// used by the receiver to validate compatibility with its peer. Only if both endpoints
    /// determine that they are compatible does the handshake succeed.
    async fn handshake(&mut self) -> anyhow::Result<()> {
        let hello = Message::Hello {
            version: self.version.clone(),
            server_fqdn: self.server_fqdn.clone(),
        };
        Self::send(&mut self.stream, hello, self.timeout).await?;

        let msg = Self::recv(&mut self.stream, self.timeout).await?;
        let Message::Hello {
            version,
            server_fqdn,
        } = msg
        else {
            bail!("expected Hello message, got {msg:?}");
        };

        if version != self.version {
            bail!("version mismatch: {version} != {}", self.version);
        }
        if let (Some(other), Some(mine)) = (&server_fqdn, &self.server_fqdn) {
            if other != mine {
                bail!("server FQDN mismatch: {other} != {mine}");
            }
        }

        Ok(())
    }

    /// Send a message to the peer.
    async fn send<W: AsyncWrite + Unpin>(
        mut writer: W,
        message: Message<Out>,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        trace!(?message, "ctp: sending message");

        let bytes = message.wire_encode()?;
        let len = bytes.len().cast_into();

        mz_ore::future::timeout(timeout, writer.write_u64(len)).await?;
        mz_ore::future::timeout(timeout, writer.write_all(&bytes)).await?;

        Ok(())
    }

    /// Receive a message from the peer.
    async fn recv<R: AsyncRead + Unpin>(
        mut reader: R,
        timeout: Duration,
    ) -> anyhow::Result<Message<In>> {
        let len = mz_ore::future::timeout(timeout, reader.read_u64()).await?;
        let mut bytes = vec![0; len.cast_into()];
        mz_ore::future::timeout(timeout, reader.read_exact(&mut bytes)).await?;

        let message = Message::wire_decode(&bytes)?;
        trace!(?message, "ctp: received message");

        Ok(message)
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
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
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
}
