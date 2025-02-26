// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive cluster server.

use std::fmt;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Error};
use async_trait::async_trait;
use differential_dataflow::trace::ExertionLogic;
use mz_cluster_client::client::TimelyConfig;
use mz_service::client::{GenericClient, Partitionable, Partitioned};
use mz_service::local::{LocalActivator, LocalClient};
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::execute::execute_from;
use timely::worker::Worker as TimelyWorker;
use timely::WorkerConfig;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::info;

use crate::communication::initialize_networking;

type PartitionedClient<C, R> = Partitioned<LocalClient<C, R>, C, R>;

/// A client managing access to the local portion of a Timely cluster
pub struct ClusterClient<C>
where
    C: ClusterSpec,
    (C::Command, C::Response): Partitionable<C::Command, C::Response>,
{
    /// The actual client to talk to the cluster
    inner: PartitionedClient<C::Command, C::Response>,
}

/// Metadata about timely workers in this process.
pub struct TimelyContainer<C: ClusterSpec> {
    /// Channels over which to send endpoints for wiring up a new Client
    client_txs: Vec<
        crossbeam_channel::Sender<(
            crossbeam_channel::Receiver<C::Command>,
            mpsc::UnboundedSender<C::Response>,
            mpsc::UnboundedSender<LocalActivator>,
        )>,
    >,
    /// Thread guards that keep worker threads alive
    _worker_guards: WorkerGuards<()>,
}

impl<C: ClusterSpec> Drop for TimelyContainer<C> {
    fn drop(&mut self) {
        panic!("Timely container must never drop");
    }
}

/// Threadsafe reference to an optional TimelyContainer
type TimelyContainerRef<C> = Arc<tokio::sync::Mutex<TimelyContainer<C>>>;

impl<C> ClusterClient<C>
where
    C: ClusterSpec,
    (C::Command, C::Response): Partitionable<C::Command, C::Response>,
{
    /// Create a new `ClusterClient`.
    pub async fn new(timely_container: TimelyContainerRef<C>) -> Self {
        let timely = timely_container.lock().await;

        // Order is important here: If our future is canceled, we need to drop the `command_txs`
        // before the `activators` so when the workers are unparked by the dropping of the
        // activators they can observe that the senders have disconnected.
        let mut activators = Vec::new();
        let mut command_txs = Vec::new();
        let mut response_rxs = Vec::new();
        let mut activator_rxs = Vec::new();
        for client_tx in &timely.client_txs {
            let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded();
            let (resp_tx, resp_rx) = mpsc::unbounded_channel();
            let (activator_tx, activator_rx) = mpsc::unbounded_channel();

            client_tx
                .send((cmd_rx, resp_tx, activator_tx))
                .expect("worker not dropped");

            command_txs.push(cmd_tx);
            response_rxs.push(resp_rx);
            activator_rxs.push(activator_rx);
        }

        // It's important that we wait for activators only after we have sent the channels to all
        // workers. Otherwise we could end up in a stalled state. See database-issues#8957.
        for mut activator_rx in activator_rxs {
            let activator = activator_rx.recv().await.expect("worker not dropped");
            activators.push(activator);
        }

        Self {
            inner: LocalClient::new_partitioned(response_rxs, command_txs, activators),
        }
    }
}

impl<C> fmt::Debug for ClusterClient<C>
where
    C: ClusterSpec,
    (C::Command, C::Response): Partitionable<C::Command, C::Response>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterClient")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<C> GenericClient<C::Command, C::Response> for ClusterClient<C>
where
    C: ClusterSpec,
    (C::Command, C::Response): Partitionable<C::Command, C::Response>,
{
    async fn send(&mut self, cmd: C::Command) -> Result<(), Error> {
        // Changing this debug statement requires changing the replica-isolation test
        tracing::debug!("ClusterClient send={:?}", &cmd);
        self.inner.send(cmd).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> Result<Option<C::Response>, Error> {
        self.inner.recv().await
    }
}

/// Specification for a Timely cluster to which a [`ClusterClient`] connects.
///
/// This trait is used to make the [`ClusterClient`] generic over the compute and storage cluster
/// implementations.
#[async_trait]
pub trait ClusterSpec: Clone + Send + Sync + 'static {
    /// The cluster command type.
    type Command: fmt::Debug + Send;
    /// The cluster response type.
    type Response: fmt::Debug + Send;

    /// Run the given Timely worker.
    fn run_worker<A: Allocate + 'static>(
        &self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            crossbeam_channel::Receiver<Self::Command>,
            mpsc::UnboundedSender<Self::Response>,
            mpsc::UnboundedSender<LocalActivator>,
        )>,
    );

    /// Build a Timely cluster using the given config.
    async fn build_cluster(
        &self,
        config: TimelyConfig,
        tokio_executor: Handle,
    ) -> Result<TimelyContainer<Self>, Error> {
        info!("Building timely container with config {config:?}");
        let (client_txs, client_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
            .map(|_| crossbeam_channel::unbounded())
            .unzip();
        let client_rxs: Mutex<Vec<_>> = Mutex::new(client_rxs.into_iter().map(Some).collect());

        let (builders, other) =
            initialize_networking(config.workers, config.bind_address.clone(), config.peer_addresses.clone()).await?;

        let mut worker_config = WorkerConfig::default();

        // We set a custom exertion logic for proportionality > 0. A proportionality value of 0
        // means that no arrangement merge effort is exerted and merging occurs only in response to
        // updates.
        if config.arrangement_exert_proportionality > 0 {
            let merge_effort = Some(1000);

            // ExertionLogic defines a function to determine if a spine is sufficiently tidied.
            // Its arguments are an iterator over the index of a layer, the count of batches in the
            // layer and the length of batches at the layer. The iterator enumerates layers from the
            // largest to the smallest layer.

            let arc: ExertionLogic = Arc::new(move |layers| {
                let mut prop = config.arrangement_exert_proportionality;

                // Layers are ordered from largest to smallest.
                // Skip to the largest occupied layer.
                let layers = layers
                    .iter()
                    .copied()
                    .skip_while(|(_idx, count, _len)| *count == 0);

                let mut first = true;
                for (_idx, count, len) in layers {
                    if count > 1 {
                        // Found an in-progress merge that we should continue.
                        return merge_effort;
                    }

                    if !first && prop > 0 && len > 0 {
                        // Found a non-empty batch within `arrangement_exert_proportionality` of
                        // the largest one.
                        return merge_effort;
                    }

                    first = false;
                    prop /= 2;
                }

                None
            });
            worker_config.set::<ExertionLogic>("differential/default_exert_logic".to_string(), arc);
        }

        let spec = self.clone();
        let worker_guards = execute_from(builders, other, worker_config, move |timely_worker| {
            let timely_worker_index = timely_worker.index();
            let _tokio_guard = tokio_executor.enter();
            let client_rx = client_rxs.lock().unwrap()[timely_worker_index % config.workers]
                .take()
                .unwrap();
            spec.run_worker(timely_worker, client_rx);
        })
        .map_err(|e| anyhow!(e))?;

        Ok(TimelyContainer {
            client_txs,
            _worker_guards: worker_guards,
        })
    }
}
