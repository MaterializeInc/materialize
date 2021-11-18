// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Independent dataflow server support.
//!
//! This crate provides types that facilitate communicating with a remote
//! dataflow server.

#![deny(missing_docs)]

use async_trait::async_trait;
use futures::sink::SinkExt;
use futures::stream::{self, SelectAll, SplitSink, SplitStream, StreamExt};
use log::trace;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_serde::formats::Bincode;
use tokio_util::codec::LengthDelimitedCodec;

use dataflow_types::client::{Command, Response};

/// A framed connection to a dataflowd server.
pub type Framed<C, T, U> =
    tokio_serde::Framed<tokio_util::codec::Framed<C, LengthDelimitedCodec>, T, U, Bincode<T, U>>;

/// A framed connection from the server's perspective.
pub type FramedServer<C> = Framed<C, Command, Response>;

/// A framed connection from the client's perspective.
pub type FramedClient<C> = Framed<C, Response, Command>;

fn length_delimited_codec() -> LengthDelimitedCodec {
    // NOTE(benesch): using an unlimited maximum frame length is problematic
    // because Tokio never shrinks its buffer. Sending or receiving one large
    // message of size N means the client will hold on to a buffer of size
    // N forever. We should investigate alternative transport protocols that
    // do not have this limitation.
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(usize::MAX);
    codec
}

/// Constructs a framed connection for the server.
pub fn framed_server<C>(conn: C) -> FramedServer<C>
where
    C: AsyncRead + AsyncWrite,
{
    tokio_serde::Framed::new(
        tokio_util::codec::Framed::new(conn, length_delimited_codec()),
        Bincode::default(),
    )
}

/// Constructs a framed connection for the client.
pub fn framed_client<C>(conn: C) -> FramedClient<C>
where
    C: AsyncRead + AsyncWrite,
{
    tokio_serde::Framed::new(
        tokio_util::codec::Framed::new(conn, length_delimited_codec()),
        Bincode::default(),
    )
}

/// A client to a remote dataflow server.
pub struct RemoteClient {
    // TODO: the client could discover the number of workers from the server.
    num_workers: usize,
    stream: SelectAll<SplitStream<FramedClient<TcpStream>>>,
    sinks: Vec<SplitSink<FramedClient<TcpStream>, Command>>,
}

impl RemoteClient {
    /// Connects a remote client to the specified remote dataflow server.
    pub async fn connect(
        num_workers: usize,
        addrs: &[impl ToSocketAddrs],
    ) -> Result<RemoteClient, anyhow::Error> {
        let mut streams = vec![];
        let mut sinks = vec![];
        for addr in addrs {
            let client = framed_client(TcpStream::connect(addr).await?);
            let (sink, stream) = client.split();
            streams.push(stream);
            sinks.push(sink);
        }
        Ok(RemoteClient {
            num_workers,
            stream: stream::select_all(streams),
            sinks,
        })
    }
}

#[async_trait]
impl dataflow_types::client::Client for RemoteClient {
    fn num_workers(&self) -> usize {
        self.num_workers
    }

    async fn send(&mut self, cmd: dataflow_types::client::Command) {
        trace!("Broadcasting dataflow command: {:?}", cmd);
        let num_conns = self.sinks.len();
        for (sink, cmd_part) in self.sinks.iter_mut().zip(cmd.partition_among(num_conns)) {
            // TODO: something better than panicking.
            sink.send(cmd_part)
                .await
                .expect("worker command receiver should not drop first");
        }
    }

    async fn recv(&mut self) -> Option<dataflow_types::client::Response> {
        // TODO: something better than panicking.
        // Attempt to read from each of `self.conns`.
        self.stream
            .next()
            .await
            .map(|x| x.expect("connection to dataflow server broken"))
    }
}
