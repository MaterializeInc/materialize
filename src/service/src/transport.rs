// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of a cluster transport protocol.

use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::pin::pin;

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use flate2::Compression;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use futures::future;
use futures::stream::StreamExt;
use mz_ore::cast::CastInto;
use mz_ore::netio::{Listener, SocketAddr, Stream};
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::client::{GenericClient, Partitionable, Partitioned};

pub trait Payload: fmt::Debug + Send + Serialize + DeserializeOwned + 'static {}
impl<T: fmt::Debug + Send + Serialize + DeserializeOwned + 'static> Payload for T {}

#[derive(Debug)]
pub struct Client<C, R> {
    out_tx: mpsc::UnboundedSender<C>,
    in_rx: mpsc::UnboundedReceiver<R>,
    error_rx: Option<oneshot::Receiver<anyhow::Error>>,
}

impl<C: Payload, R: Payload> Client<C, R> {
    pub async fn connect(address: SocketAddr, version: Version) -> anyhow::Result<Self> {
        let stream = Stream::connect(address).await?;

        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (error_tx, error_rx) = oneshot::channel();

        mz_ore::task::spawn(|| "ctp::client-connection", async {
            let conn = Connection::new(stream, version);
            let client = ChannelClient {
                tx: in_tx,
                rx: out_rx,
            };

            let Err(error) = conn.serve(client).await;
            let _ = error_tx.send(error);
        });

        Ok(Self {
            out_tx,
            in_rx,
            error_rx: Some(error_rx),
        })
    }

    fn expect_error(&mut self) -> anyhow::Error {
        if let Some(mut rx) = self.error_rx.take() {
            rx.try_recv().expect("expected connection error")
        } else {
            anyhow!("connection closed")
        }
    }
}

impl<C, R> Client<C, R>
where
    C: Payload,
    R: Payload,
    (C, R): Partitionable<C, R>,
{
    pub async fn connect_partitioned(
        addresses: Vec<SocketAddr>,
        version: Version,
    ) -> anyhow::Result<Partitioned<Self, C, R>> {
        let connects = addresses
            .into_iter()
            .map(|addr| Self::connect(addr, version.clone()));
        let clients = future::try_join_all(connects).await?;
        Ok(Partitioned::new(clients))
    }
}

#[async_trait]
impl<C: Payload, R: Payload> GenericClient<C, R> for Client<C, R> {
    async fn send(&mut self, cmd: C) -> anyhow::Result<()> {
        self.out_tx.send(cmd).map_err(|_| self.expect_error())
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> anyhow::Result<Option<R>> {
        // `mpsc::UnboundedReceiver::recv` is cancel safe.
        match self.in_rx.recv().await {
            Some(resp) => Ok(Some(resp)),
            None => Err(self.expect_error()),
        }
    }
}

pub async fn serve<C, R, G>(
    address: SocketAddr,
    version: Version,
    client_builder: impl Fn() -> G,
) -> anyhow::Result<()>
where
    C: Payload,
    R: Payload,
    G: GenericClient<C, R> + 'static,
{
    let listener = Listener::bind(address).await?;

    loop {
        let (stream, _peer) = listener.accept().await?;

        let client = client_builder();
        let version = version.clone();

        mz_ore::task::spawn(|| "ctp::server-connection", async {
            let conn = Connection::new(stream, version);

            let Err(error) = conn.serve(client).await;
            info!("CTP connection failed: {error}");
        });
    }
}

#[derive(Debug)]
struct Connection<Out, In> {
    stream: Stream,
    version: Version,
    _payloads: PhantomData<(Out, In)>,
}

impl<Out: Payload, In: Payload> Connection<Out, In> {
    fn new(stream: Stream, version: Version) -> Self {
        Self {
            stream,
            version,
            _payloads: PhantomData,
        }
    }

    async fn serve<C>(mut self, mut client: C) -> anyhow::Result<Infallible>
    where
        C: GenericClient<In, Out>,
    {
        self.handshake().await.context("handshake")?;

        // `Connection::recv` is not cancel safe, so we can't use it in a `select!` directly.
        // Instead we wrap it into an async stream and select on `Stream::next`, which is cancel
        // safe. This requires us to split the `netio::Stream` into its read/write halves, so we
        // can move the former into the async stream.
        let (mut stream_rx, mut stream_tx) = self.stream.split();
        let mut inbound = pin!(async_stream::stream! {
            loop {
                yield Self::recv(&mut stream_rx).await;
            }
        });

        loop {
            tokio::select! {
                // `Stream::next` is cancel safe: The returned future only holds a reference to the
                // underlying stream, so dropping it will never lose a value.
                Some(inbound) = inbound.next() => match inbound? {
                    Message::Payload(p) => client.send(p).await?,
                    msg => bail!("received unexpected message: {msg:?}"),
                },
                // `GenericClient::recv` is documented to be cancel safe.
                outbound = client.recv() => match outbound? {
                    Some(p) => Self::send(&mut stream_tx, Message::Payload(p)).await?,
                    None => bail!("client disconnected"),
                }
            }
        }
    }

    async fn handshake(&mut self) -> anyhow::Result<()> {
        let hello = Message::Hello(self.version.clone());
        Self::send(&mut self.stream, hello).await?;

        match Self::recv(&mut self.stream).await? {
            Message::Hello(v_other) if v_other == self.version => (),
            Message::Hello(v_other) => {
                bail!("version mismatch: {v_other} != {}", self.version)
            }
            msg => bail!("expected Hello message, got {msg:?}"),
        }

        Ok(())
    }

    async fn send<W: AsyncWrite + Unpin>(
        mut writer: W,
        message: Message<Out>,
    ) -> anyhow::Result<()> {
        let bytes = message.wire_encode()?;
        let len = bytes.len().cast_into();

        writer.write_u64(len).await?;
        writer.write_all(&bytes).await?;

        Ok(())
    }

    async fn recv<R: AsyncRead + Unpin>(mut reader: R) -> anyhow::Result<Message<In>> {
        let len = reader.read_u64().await?;
        let mut bytes = vec![0; len.cast_into()];
        reader.read_exact(&mut bytes).await?;

        Message::wire_decode(&bytes)
    }
}

/// A `GenericClient` that forwards messages over channels.
#[derive(Debug)]
struct ChannelClient<C, R> {
    tx: mpsc::UnboundedSender<C>,
    rx: mpsc::UnboundedReceiver<R>,
}

#[async_trait]
impl<C: Payload, R: Payload> GenericClient<C, R> for ChannelClient<C, R> {
    async fn send(&mut self, cmd: C) -> anyhow::Result<()> {
        let result = self.tx.send(cmd);
        result.map_err(|_| anyhow!("client channel disconnected"))
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> anyhow::Result<Option<R>> {
        // `mpsc::UnboundedReceiver::recv` is cancel safe.
        match self.rx.recv().await {
            Some(resp) => Ok(Some(resp)),
            None => bail!("client channel disconnected"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Message<P> {
    Hello(Version),
    Payload(P),
}

impl<P: Payload> Message<P> {
    fn wire_encode(&self) -> anyhow::Result<Vec<u8>> {
        let mut compressor = DeflateEncoder::new(Vec::new(), Compression::default());
        bincode::serialize_into(&mut compressor, self)?;
        let bytes = compressor.finish()?;
        Ok(bytes)
    }

    fn wire_decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut decompressor = DeflateDecoder::new(bytes);
        let msg = bincode::deserialize_from(&mut decompressor)?;
        Ok(msg)
    }
}
