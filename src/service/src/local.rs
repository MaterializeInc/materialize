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
use std::thread::Thread;

use async_trait::async_trait;
use crossbeam_channel::Sender;
use itertools::Itertools;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::trace;

use crate::client::{GenericClient, Partitionable, Partitioned, Reconnect};

/// A client that partitions commands across several [`ProcessLocal`] clients.
#[derive(Debug)]
pub struct LocalClient<C, R>
where
    (C, R): Partitionable<C, R>,
    C: fmt::Debug,
    R: fmt::Debug,
{
    client: Partitioned<ProcessLocal<C, R>, C, R>,
}

impl<C, R> LocalClient<C, R>
where
    (C, R): Partitionable<C, R>,
    C: fmt::Debug,
    R: fmt::Debug,
{
    pub fn new(
        rxs: Vec<tokio::sync::mpsc::UnboundedReceiver<R>>,
        txs: Vec<crossbeam_channel::Sender<C>>,
        threads: Vec<std::thread::Thread>,
    ) -> Self {
        let clients = rxs
            .into_iter()
            .zip_eq(txs)
            .zip_eq(threads)
            .map(|((rx, tx), thread)| ProcessLocal::new(rx, tx, thread))
            .collect();
        LocalClient {
            client: Partitioned::new(clients),
        }
    }
}

#[async_trait]
impl<C, R> GenericClient<C, R> for LocalClient<C, R>
where
    (C, R): Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        trace!("send local command: {:?}", cmd);
        self.client.send(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        let response = self.client.recv().await;
        trace!("receive local response: {:?}", response);
        response
    }
}

/// A client to a thread in the same process.
///
/// The thread is unparked on every call to [`send`](ProcessLocal::send) and on
/// `Drop`.
#[derive(Debug)]
pub struct ProcessLocal<C, R> {
    rx: UnboundedReceiver<R>,
    tx: Sender<C>,
    thread: Thread,
}

#[async_trait]
impl<C: Send, R: Send> Reconnect for ProcessLocal<C, R> {
    fn disconnect(&mut self) {
        panic!("Disconnecting and reconnecting local clients is currently impossible");
    }

    async fn reconnect(&mut self) {
        panic!("Disconnecting and reconnecting local clients is currently impossible");
    }
}

#[async_trait]
impl<C, R> GenericClient<C, R> for ProcessLocal<C, R>
where
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        self.tx
            .send(cmd)
            .expect("worker command receiver should not drop first");
        self.thread.unpark();
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        Ok(self.rx.recv().await)
    }
}

impl<C, R> ProcessLocal<C, R> {
    /// Create a new instance of [`ProcessLocal`] from its parts.
    pub fn new(rx: UnboundedReceiver<R>, tx: Sender<C>, thread: Thread) -> Self {
        Self { rx, tx, thread }
    }
}

// We implement `Drop` so that we can wake each of the threads and have them
// notice the drop.
impl<C, R> Drop for ProcessLocal<C, R> {
    fn drop(&mut self) {
        // Drop the thread handle.
        let (tx, _rx) = crossbeam_channel::unbounded();
        self.tx = tx;
        // Unpark the thread once the handle is dropped, so that it can observe
        // the emptiness.
        self.thread.unpark();
    }
}
