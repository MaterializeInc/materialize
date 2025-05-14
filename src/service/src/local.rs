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
use timely::scheduling::SyncActivator;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::client::{GenericClient, Partitionable, Partitioned};

/// A trait for types that can be used to activate threads.
pub trait Activatable: fmt::Debug + Send {
    fn activate(&self);
}

impl Activatable for SyncActivator {
    fn activate(&self) {
        self.activate().unwrap()
    }
}

impl Activatable for Thread {
    fn activate(&self) {
        self.unpark()
    }
}

/// An activator for a thread.
///
/// This wraps any `Activatable` and has a `Drop` impl to ensure the thread is always activated
/// when the activator is dropped. This is important to ensure workers have a chance to observe
/// that their command channel has closed and prepare for reconnection.
#[derive(Debug)]
pub struct LocalActivator {
    inner: Box<dyn Activatable>,
}

impl LocalActivator {
    pub fn new<A: Activatable + 'static>(inner: A) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }

    fn activate(&self) {
        self.inner.activate();
    }
}

impl Drop for LocalActivator {
    fn drop(&mut self) {
        self.inner.activate();
    }
}

/// A client to a thread in the same process.
///
/// The thread is unparked on every call to [`send`](LocalClient::send) and on
/// `Drop`.
#[derive(Debug)]
pub struct LocalClient<C, R> {
    // Order is important here: We need to drop the `tx` before the activator so when the thread is
    // unparked by the dropping of the activator it can observed that the sender has disconnected.
    rx: UnboundedReceiver<R>,
    tx: Sender<C>,
    tx_activator: LocalActivator,
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

        self.tx_activator.activate();

        Ok(())
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        // `mpsc::UnboundedReceiver::recv` is documented as cancel safe.
        Ok(self.rx.recv().await)
    }
}

impl<C, R> LocalClient<C, R> {
    /// Create a new instance of [`LocalClient`] from its parts.
    pub fn new(rx: UnboundedReceiver<R>, tx: Sender<C>, tx_activator: LocalActivator) -> Self {
        Self {
            rx,
            tx,
            tx_activator,
        }
    }

    /// Create a new partitioned local client from parts for each client.
    pub fn new_partitioned(
        rxs: Vec<UnboundedReceiver<R>>,
        txs: Vec<Sender<C>>,
        tx_activators: Vec<LocalActivator>,
    ) -> Partitioned<Self, C, R>
    where
        (C, R): Partitionable<C, R>,
    {
        let clients = rxs
            .into_iter()
            .zip_eq(txs)
            .zip_eq(tx_activators)
            .map(|((rx, tx), tx_activator)| LocalClient::new(rx, tx, tx_activator))
            .collect();
        Partitioned::new(clients)
    }
}
