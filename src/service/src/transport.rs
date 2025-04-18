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
use std::fmt;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
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
use tracing::{info, trace};

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
    pub async fn connect(address: &str, version: Version) -> anyhow::Result<Self> {
        let stream = Stream::connect(address).await?;
        info!(%address, "ctp: connected to server");

        let mut conn = Connection::start(stream);
        conn.handshake(version).await?;

        Ok(Self { conn })
    }
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
    ) -> anyhow::Result<Partitioned<Self, Out, In>> {
        let connects = addresses
            .iter()
            .map(|addr| Self::connect(addr, version.clone()));
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

        let conn = Connection::start(stream);
        let handler = handler_fn();
        let version = version.clone();

        mz_ore::task::spawn(|| "ctp::connection", async {
            let Err(error) = serve_connection(conn, handler, version).await;
            info!("ctp: connection failed: {error}");
        });
    }
}

/// Serve a single CTP connection.
async fn serve_connection<In, Out, H>(
    mut conn: Connection<Out, In>,
    mut handler: H,
    version: Version,
) -> anyhow::Result<Infallible>
where
    In: Payload,
    Out: Payload,
    H: GenericClient<In, Out>,
{
    conn.handshake(version).await?;

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
    /// Start a new connection wrapping the given stream.
    fn start(stream: Stream) -> Self {
        let (stream_rx, stream_tx) = stream.split();
        let (out_tx, out_rx) = mpsc::channel(1024);
        let (in_tx, in_rx) = mpsc::channel(1024);

        // Initialize the error channel with a default error to return if none of the tasks
        // produced an error.
        let (error_tx, error_rx) = watch::channel("connection closed".into());

        let send_task = mz_ore::task::spawn(
            || "ctp::send",
            Self::run_send_task(stream_tx, out_rx, error_tx.clone()),
        );
        let recv_task = mz_ore::task::spawn(
            || "ctp::recv",
            Self::run_recv_task(stream_rx, in_tx, error_tx),
        );

        Self {
            msg_tx: out_tx,
            msg_rx: in_rx,
            error_rx,
            _tasks: vec![send_task.abort_on_drop(), recv_task.abort_on_drop()],
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
    async fn handshake(&mut self, version: Version) -> anyhow::Result<()> {
        self.send(Message::Hello {
            version: version.clone(),
        })
        .await?;

        let peer_version = match self.recv().await? {
            Message::Hello { version } => version,
            msg => bail!("expected Hello message, got {msg:?}"),
        };

        if peer_version != version {
            bail!("version mismatch: {peer_version} != {version}");
        }

        Ok(())
    }

    /// Run a connection's send task.
    async fn run_send_task<W: AsyncWrite + Unpin>(
        mut writer: W,
        mut msg_rx: mpsc::Receiver<Message<Out>>,
        error_tx: watch::Sender<String>,
    ) {
        while let Some(msg) = msg_rx.recv().await {
            trace!(?msg, "ctp: sending message");

            let result = Self::write_message(&mut writer, msg).await;
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
    ) {
        loop {
            let result = Self::read_message(&mut reader).await;
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

    /// Write a message onto the wire.
    async fn write_message<W: AsyncWrite + Unpin>(
        mut writer: W,
        msg: Message<Out>,
    ) -> anyhow::Result<()> {
        let bytes = msg.wire_encode()?;

        let len = bytes.len().cast_into();
        writer.write_u64(len).await?;
        writer.write_all(&bytes).await?;

        Ok(())
    }

    /// Read a message from the wire.
    async fn read_message<R: AsyncRead + Unpin>(mut reader: R) -> anyhow::Result<Message<In>> {
        let len = reader.read_u64().await?;
        let len: usize = len.cast_into();

        let mut bytes = vec![0; len];
        reader.read_exact(&mut bytes).await?;

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
    },
    /// A massage carrying application payload.
    Payload(P),
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
