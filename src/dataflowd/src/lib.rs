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

use dataflow_types::client::{partitioned::Partitioned, Client, Command, Response};

/// A convenience type for compatibility.
pub struct RemoteClient {
    client: Partitioned<tcp::TcpClient>,
}

impl RemoteClient {
    /// Construct a client backed by multiple tcp connections
    pub async fn connect(addrs: &[impl tokio::net::ToSocketAddrs]) -> Result<Self, anyhow::Error> {
        let mut remotes = Vec::with_capacity(addrs.len());
        for addr in addrs.iter() {
            remotes.push(tcp::TcpClient::connect(addr).await?);
        }
        Ok(Self {
            client: Partitioned::new(remotes),
        })
    }
}

#[async_trait::async_trait]
impl Client for RemoteClient {
    async fn send(&mut self, cmd: Command) {
        tracing::trace!("Broadcasting dataflow command: {:?}", cmd);
        self.client.send(cmd).await
    }
    async fn recv(&mut self) -> Option<Response> {
        self.client.recv().await
    }
}

/// A client to a remote dataflow server.
pub mod tcp {

    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::net::{TcpStream, ToSocketAddrs};
    use tokio_serde::formats::Bincode;
    use tokio_util::codec::LengthDelimitedCodec;

    use dataflow_types::client::{Command, Response};

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

    #[async_trait::async_trait]
    impl dataflow_types::client::Client for TcpClient {
        async fn send(&mut self, cmd: dataflow_types::client::Command) {
            // TODO: something better than panicking.
            self.connection
                .send(cmd)
                .await
                .expect("worker command receiver should not drop first");
        }

        async fn recv(&mut self) -> Option<dataflow_types::client::Response> {
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
