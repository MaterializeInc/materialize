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

pub trait Activatable {
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

/// A client to a thread in the same process.
///
/// The thread is unparked on every call to [`send`](LocalClient::send) and on
/// `Drop`.
#[derive(Debug)]
pub struct LocalClient<C, R, A: Activatable> {
    rx: UnboundedReceiver<R>,
    tx: Sender<C>,
    tx_activator: A,
}

#[async_trait]
impl<C, R, A> GenericClient<C, R> for LocalClient<C, R, A>
where
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
    A: fmt::Debug + Activatable + Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        self.tx
            .send(cmd)
            .expect("worker command receiver should not drop first");

        self.tx_activator.activate();

        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        Ok(self.rx.recv().await)
    }
}

impl<C, R, A: Activatable> LocalClient<C, R, A> {
    /// Create a new instance of [`LocalClient`] from its parts.
    pub fn new(rx: UnboundedReceiver<R>, tx: Sender<C>, tx_activator: A) -> Self {
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
        tx_activators: Vec<A>,
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

// We implement `Drop` so that we can wake each of the threads and have them
// notice the drop.
impl<C, R, A: Activatable> Drop for LocalClient<C, R, A> {
    fn drop(&mut self) {
        // Drop the thread handle.
        let (tx, _rx) = crossbeam_channel::unbounded();
        self.tx = tx;
        // Unpark the thread once the handle is dropped, so that it can observe
        // the emptiness.
        self.tx_activator.activate();
    }
}
