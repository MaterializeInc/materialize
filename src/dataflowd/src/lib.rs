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

use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;

use mz_dataflow_types::client::{
    partitioned::Partitioned, Client, Command, ComputeCommand, ComputeInstanceId, InstanceConfig,
    Response,
};
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

/// Types of compute clients we manage.
pub enum ComputeClientFlavor {
    /// A virtual compute client, hosted on the storage server.
    Virtual,
    /// A remote compute client, likely a network connection away.
    Remote(Box<dyn Client + Send + 'static>),
}

/// A [Client] backed by separate clients for storage and compute.
pub struct SplitClient<S> {
    /// Client on which storage commands are executed, and where virtual compute instances are created.
    storage_client: S,
    /// A map of remote compute instances.
    compute_clients: HashMap<ComputeInstanceId, ComputeClientFlavor>,
}

impl<S: Client> SplitClient<S> {
    /// Construct a new split client
    pub fn new(storage_client: S) -> Self {
        Self {
            storage_client,
            compute_clients: Default::default(),
        }
    }
}

#[async_trait(?Send)]
impl<S: Client> Client for SplitClient<S> {
    async fn send(&mut self, cmd: Command) -> Result<(), anyhow::Error> {
        trace!("SplitClient: Sending dataflow command: {:?}", cmd);
        // Ensure that a client exists, if we are asked to create one.
        if let Command::Compute(ComputeCommand::CreateInstance(config, _logging), instance) = &cmd {
            assert!(self.compute_clients.get(instance).is_none());
            let client = match config {
                InstanceConfig::Virtual => ComputeClientFlavor::Virtual,
                InstanceConfig::Remote(addr) => {
                    ComputeClientFlavor::Remote(Box::new(RemoteClient::connect(&addr).await?))
                }
            };
            self.compute_clients.insert(*instance, client);
        }

        // Notice whether we should drop the instance as a result of the command.
        let drop_instance = if let Command::Compute(ComputeCommand::DropInstance, instance) = &cmd {
            Some(*instance)
        } else {
            None
        };

        // Route the command appropriately
        match cmd {
            Command::Compute(inner, instance) => match self.compute_clients.get_mut(&instance) {
                Some(ComputeClientFlavor::Virtual) => {
                    self.storage_client
                        .send(Command::Compute(inner, instance))
                        .await?;
                }
                Some(ComputeClientFlavor::Remote(client)) => {
                    client.send(Command::Compute(inner, instance)).await?;
                }
                None => {
                    Err(anyhow!("Unknown compute instance: {instance:?}"))?;
                }
            },
            cmd @ Command::Storage(_) => self.storage_client.send(cmd).await?,
        }

        if let Some(instance) = drop_instance {
            self.compute_clients.remove(&instance);
        }

        Ok(())
    }
    async fn recv(&mut self) -> Option<Response> {
        // TODO: We currently don't have a good way to receive from many clients
        let mut futures = FuturesUnordered::new();
        for client in self.compute_clients.values_mut() {
            if let ComputeClientFlavor::Remote(client) = client {
                futures.push(client.recv());
            }
        }
        tokio::select! {
            response = futures.select_next_some() => response,
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
