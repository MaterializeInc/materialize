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

use anyhow::Context;
use async_trait::async_trait;

use mz_dataflow_types::client::{partitioned::Partitioned, Client, Command, Response};
use tracing::trace;

/// A convenience type for compatibility.
pub struct RemoteClient {
    client: Partitioned<tcp::TcpClient>,
}

impl RemoteClient {
    /// Construct a client backed by multiple tcp connections
    pub async fn connect(
        addrs: &[impl tokio::net::ToSocketAddrs + std::fmt::Display],
    ) -> Result<Self, anyhow::Error> {
        let mut remotes = Vec::with_capacity(addrs.len());
        for addr in addrs.iter() {
            remotes.push(
                tcp::TcpClient::connect(addr)
                    .await
                    .with_context(|| format!("Connecting to {addr}"))?,
            );
        }
        Ok(Self {
            client: Partitioned::new(remotes),
        })
    }
}

#[async_trait(?Send)]
impl Client for RemoteClient {
    async fn send(&mut self, cmd: Command) -> Result<(), anyhow::Error> {
        trace!("Sending dataflow command: {:?}", cmd);
        self.client.send(cmd).await
    }
    async fn recv(&mut self) -> Option<Response> {
        let response = self.client.recv().await;
        trace!("Receiving dataflow response: {:?}", response);
        response
    }
}

/// A [Client] backed by separate clients for storage and compute.
pub struct SplitClient<S, C> {
    storage_client: S,
    compute_client: C,
}

impl<S: Client, C: Client> SplitClient<S, C> {
    /// Construct a new split client
    pub fn new(storage_client: S, compute_client: C) -> Self {
        Self {
            storage_client,
            compute_client,
        }
    }
}

#[async_trait(?Send)]
impl<S: Client, C: Client> Client for SplitClient<S, C> {
    async fn send(&mut self, cmd: Command) -> Result<(), anyhow::Error> {
        trace!("SplitClient: Sending dataflow command: {:?}", cmd);
        match cmd {
            cmd @ Command::Compute(_, _) => self.compute_client.send(cmd),
            cmd @ Command::Storage(_) => self.storage_client.send(cmd),
        }
        .await
    }
    async fn recv(&mut self) -> Option<Response> {
        tokio::select! {
            response = self.compute_client.recv() => response,
            response = self.storage_client.recv() => response,
        }
    }
}

/// A client to a remote dataflow server.
pub mod tcp {

    use async_trait::async_trait;
    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::net::{TcpStream, ToSocketAddrs};
    use tokio_serde::formats::Bincode;
    use tokio_util::codec::LengthDelimitedCodec;

    use mz_dataflow_types::client::{Client, Command, Response};

    /// A client to a remote dataflow server.
    pub struct TcpClient {
        connection: FramedClient<TcpStream>,
    }

    impl TcpClient {
        /// Connects a remote client to the specified remote dataflow server.
        pub async fn connect(addr: impl ToSocketAddrs) -> Result<TcpClient, anyhow::Error> {
            let connection = framed_client(TcpStream::connect(addr).await?);
            Ok(Self { connection })
        }
    }

    #[async_trait(?Send)]
    impl Client for TcpClient {
        async fn send(&mut self, cmd: Command) -> Result<(), anyhow::Error> {
            // TODO: something better than panicking.
            self.connection.send(cmd).await.map_err(|err| err.into())
        }

        async fn recv(&mut self) -> Option<Response> {
            // TODO: something better than panicking.
            self.connection
                .next()
                .await
                .map(|x| x.expect("connection to dataflow server broken"))
        }
    }

    /// A framed connection to a dataflowd server.
    pub type Framed<C, T, U> = tokio_serde::Framed<
        tokio_util::codec::Framed<C, LengthDelimitedCodec>,
        T,
        U,
        Bincode<T, U>,
    >;

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
}
