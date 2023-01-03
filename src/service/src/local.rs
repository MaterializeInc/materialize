// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Process-local transport for the [client](crate::client) module.

use std::fmt;

use async_trait::async_trait;
use itertools::Itertools;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::client::{GenericClient, Partitionable, Partitioned};

/// A client to a thread in the same process.
///
/// The thread is unparked on every call to [`send`](LocalClient::send) and on
/// `Drop`.
#[derive(Debug)]
pub struct LocalClient<C, R> {
    rx: UnboundedReceiver<R>,
    tx: UnboundedSender<C>,
}

#[async_trait]
impl<C, R> GenericClient<C, R> for LocalClient<C, R>
where
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        self.tx
            .send(cmd)
            .expect("worker command receiver should not drop first");

        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        Ok(self.rx.recv().await)
    }
}

impl<C, R> LocalClient<C, R> {
    /// Create a new instance of [`LocalClient`] from its parts.
    pub fn new(rx: UnboundedReceiver<R>, tx: UnboundedSender<C>) -> Self {
        Self { rx, tx }
    }

    /// Create a new partitioned local client from parts for each client.
    pub fn new_partitioned(
        rxs: Vec<UnboundedReceiver<R>>,
        txs: Vec<UnboundedSender<C>>,
    ) -> Partitioned<Self, C, R>
    where
        (C, R): Partitionable<C, R>,
    {
        let clients = rxs
            .into_iter()
            .zip_eq(txs)
            .map(|(rx, tx)| LocalClient::new(rx, tx))
            .collect();
        Partitioned::new(clients)
    }
}

// We implement `Drop` so that we can wake each of the threads and have them
// notice the drop.
impl<C, R> Drop for LocalClient<C, R> {
    fn drop(&mut self) {
        // Drop the thread handle.
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        self.tx = tx;
    }
}
