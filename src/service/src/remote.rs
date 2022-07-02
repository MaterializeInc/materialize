// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remote transport for the [client](crate::client) module.

use std::fmt;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::net::ToSocketAddrs;
use tracing::trace;

use crate::client::{FromAddr, GenericClient, Partitionable, Partitioned, Reconnect};

/// A client that partitions commands across several clients that implement
/// [`Reconnect`] and [`FromAddr`].
#[derive(Debug)]
pub struct RemoteClient<C, R, G>
where
    (C, R): Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
    G: GenericClient<C, R> + Reconnect + FromAddr,
{
    client: Partitioned<G, C, R>,
}

impl<C, R, G> RemoteClient<C, R, G>
where
    (C, R): Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
    G: GenericClient<C, R> + Reconnect + FromAddr,
{
    /// Construct a client backed by multiple clients that implement
    /// [`Reconnect`] and [`FromAddr`].
    pub fn new(addrs: &[impl ToSocketAddrs + fmt::Display]) -> Self {
        let mut remotes = Vec::with_capacity(addrs.len());
        for addr in addrs.iter() {
            remotes.push(G::from_addr(addr.to_string()));
        }
        Self {
            client: Partitioned::new(remotes),
        }
    }

    pub async fn connect(&mut self) {
        for part in &mut self.client.parts {
            part.reconnect().await;
        }
    }
}

#[async_trait]
impl<C, R, G> GenericClient<C, R> for RemoteClient<C, R, G>
where
    (C, R): Partitionable<C, R>,
    C: Serialize + fmt::Debug + Unpin + Send,
    R: DeserializeOwned + fmt::Debug + Unpin + Send,
    G: GenericClient<C, R> + Reconnect + FromAddr,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        trace!("Sending remote command: {:?}", cmd);
        self.client.send(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        let response = self.client.recv().await;
        trace!("Receiving remote response: {:?}", response);
        response
    }
}
