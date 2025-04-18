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

use std::convert::Infallible;
use std::fmt::Debug;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bincode::Options;
use futures::future;
use mz_ore::cast::CastInto;
use mz_ore::netio::{Listener, SocketAddr, Stream};
use mz_ore::task::AbortOnDropHandle;
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, trace};

use crate::client::{GenericClient, Partitionable, Partitioned};

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
    pub async fn connect(address: &str, version: Version) -> anyhow::Result<Self> {
        let dest_host = host_from_address(address);
        let stream = Stream::connect(address).await?;
        info!(%address, "ctp: connected to server");

        let conn = Connection::start(stream, version, dest_host).await?;
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

/// Spawn a CTP server that serves connections at the given address.
pub async fn serve<In, Out, H>(
    address: SocketAddr,
    version: Version,
    server_fqdn: Option<String>,
    handler_fn: impl Fn() -> H,
) -> anyhow::Result<()>
where
    In: Message,
    Out: Message,
    H: GenericClient<In, Out> + 'static,
{
    let listener = Listener::bind(&address).await?;
    info!(%address, "ctp: listening for client connections");

    loop {
        let (stream, peer) = listener.accept().await?;
        info!(%peer, "ctp: accepted client connection");

        let handler = handler_fn();
        let version = version.clone();
        let server_fqdn = server_fqdn.clone();

        mz_ore::task::spawn(|| "ctp::connection", async {
            let Err(error) = serve_connection(stream, handler, version, server_fqdn).await;
            info!("ctp: connection failed: {error}");
        });
    }
}

/// Serve a single CTP connection.
async fn serve_connection<In, Out, H>(
    stream: Stream,
    mut handler: H,
    version: Version,
    server_fqdn: Option<String>,
) -> anyhow::Result<Infallible>
where
    In: Message,
    Out: Message,
    H: GenericClient<In, Out>,
{
    let mut conn = Connection::start(stream, version, server_fqdn).await?;

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
    msg_tx: mpsc::Sender<Out>,
    /// Message receiver connected to the receive task.
    msg_rx: mpsc::Receiver<In>,
    /// Receiver for errors encountered by connection tasks.
    error_rx: watch::Receiver<String>,

    /// Handles to connection tasks.
    _tasks: [AbortOnDropHandle<()>; 2],
}

impl<Out: Message, In: Message> Connection<Out, In> {
    /// Start a new connection wrapping the given stream.
    async fn start(
        stream: Stream,
        version: Version,
        server_fqdn: Option<String>,
    ) -> anyhow::Result<Self> {
        let (mut reader, mut writer) = stream.split();

        handshake(&mut reader, &mut writer, version, server_fqdn).await?;

        let (out_tx, out_rx) = mpsc::channel(1024);
        let (in_tx, in_rx) = mpsc::channel(1024);
        // Initialize the error channel with a default error to return if none of the tasks
        // produced an error.
        let (error_tx, error_rx) = watch::channel("connection closed".into());

        let send_task = mz_ore::task::spawn(
            || "ctp::send",
            Self::run_send_task(writer, out_rx, error_tx.clone()),
        );
        let recv_task =
            mz_ore::task::spawn(|| "ctp::recv", Self::run_recv_task(reader, in_tx, error_tx));

        Ok(Self {
            msg_tx: out_tx,
            msg_rx: in_rx,
            error_rx,
            _tasks: [send_task.abort_on_drop(), recv_task.abort_on_drop()],
        })
    }

    /// Enqueue a message for sending.
    async fn send(&mut self, msg: Out) -> anyhow::Result<()> {
        match self.msg_tx.send(msg).await {
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
        mut msg_rx: mpsc::Receiver<Out>,
        error_tx: watch::Sender<String>,
    ) {
        while let Some(msg) = msg_rx.recv().await {
            trace!(?msg, "ctp: sending message");

            if let Err(error) = write_message(&mut writer, &msg).await {
                debug!("ctp: send error: {error}");
                let _ = error_tx.send(error.to_string());
                break;
            };
        }
    }

    /// Run a connection's recv task.
    async fn run_recv_task<R: AsyncRead + Unpin>(
        mut reader: R,
        msg_tx: mpsc::Sender<In>,
        error_tx: watch::Sender<String>,
    ) {
        loop {
            match read_message(&mut reader).await {
                Ok(msg) => {
                    trace!(?msg, "ctp: received message");

                    if msg_tx.send(msg).await.is_err() {
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
impl<In: Message, Out: Message> GenericClient<In, Out> for ChannelHandler<In, Out> {
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
    write_message(&mut writer, &hello).await?;

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
async fn write_message<W, M>(mut writer: W, msg: &M) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
    M: Message,
{
    let bytes = wire_encode(msg)?;

    let len = bytes.len().cast_into();
    writer.write_u64(len).await?;
    writer.write_all(&bytes).await?;

    Ok(())
}

/// Read a message from the given reader.
async fn read_message<R, M>(mut reader: R) -> anyhow::Result<M>
where
    R: AsyncRead + Unpin,
    M: Message,
{
    let len = reader.read_u64().await?;
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
