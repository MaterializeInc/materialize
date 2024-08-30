// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive cluster server.

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Error};
use async_trait::async_trait;
use differential_dataflow::trace::ExertionLogic;
use futures::future;
use mz_cluster_client::client::{ClusterStartupEpoch, TimelyConfig};
use mz_ore::error::ErrorExt;
use mz_ore::halt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_service::client::{GenericClient, Partitionable, Partitioned};
use mz_service::local::LocalClient;
use mz_txn_wal::operator::TxnsContext;
use timely::communication::initialize::WorkerGuards;
use timely::execute::execute_from;
use timely::WorkerConfig;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::communication::initialize_networking;
use crate::types::AsRunnableWorker;

type PartitionedClient<C, R, A> = Partitioned<LocalClient<C, R, A>, C, R>;

/// Configures a cluster server.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// Metrics registry through which dataflow metrics will be reported.
    pub metrics_registry: MetricsRegistry,
    /// `persist` client cache.
    pub persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    pub txns_ctx: TxnsContext,
    /// A process-global handle to tracing configuration.
    pub tracing_handle: Arc<TracingHandle>,
}

/// A client managing access to the local portion of a Timely cluster.
#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub enum ClusterClient<Client, Worker> {
    /// Not connected to Timely; waiting for an initial `CreateTimely` command.
    Pending {
        /// The cluster config.
        config: ClusterConfig,
        /// Additional worker configuration required for initializing Timely.
        worker: Worker,
    },
    /// Connected to Timely.
    Connected {
        /// The wrapped client.
        inner: Client,
        /// Metadata about the Timely cluster.
        #[derivative(Debug = "ignore")]
        timely: TimelyContainer,
    },
}

/// Metadata about timely workers in this process.
pub struct TimelyContainer {
    /// The current timely config in use
    config: TimelyConfig,
    /// Thread guards that keep worker threads alive
    _worker_guards: WorkerGuards<()>,
}

impl Drop for TimelyContainer {
    fn drop(&mut self) {
        panic!("Timely container must never drop");
    }
}

impl<Worker, C, R> ClusterClient<PartitionedClient<C, R, Worker::Activatable>, Worker>
where
    C: Send + 'static,
    R: Send + 'static,
    (C, R): Partitionable<C, R>,
    Worker: AsRunnableWorker<C, R> + Clone + Send + Sync + 'static,
{
    /// Create a new `ClusterClient` that is not yet connected to a Timely cluster.
    pub fn new(config: ClusterConfig, worker: Worker) -> Self {
        Self::Pending { config, worker }
    }

    /// Build a Timely cluster using the given configuration, as well as a client connected to it.
    async fn build_timely(
        user_worker_config: Worker,
        config: TimelyConfig,
        epoch: ClusterStartupEpoch,
        cluster_config: ClusterConfig,
    ) -> Result<
        (
            TimelyContainer,
            PartitionedClient<C, R, Worker::Activatable>,
        ),
        Error,
    > {
        info!("Building timely container with config {config:?}");

        let (builders, other) = initialize_networking(
            config.workers,
            config.process,
            config.addresses.clone(),
            epoch,
        )
        .await?;

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

        // Initialize channels for worker-client communication.
        let workers = config.workers;
        let mut command_txs = Vec::with_capacity(workers);
        let mut response_rxs = Vec::with_capacity(workers);
        let mut activator_rxs = Vec::with_capacity(workers);
        let mut cluster_channels = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded();
            let (resp_tx, resp_rx) = mpsc::unbounded_channel();
            let (activator_tx, activator_rx) = oneshot::channel();
            command_txs.push(cmd_tx);
            response_rxs.push(resp_rx);
            activator_rxs.push(activator_rx);
            cluster_channels.push(Some((cmd_rx, resp_tx, activator_tx)));
        }

        let cluster_channels = Mutex::new(cluster_channels);
        let tokio_executor = tokio::runtime::Handle::current();

        // Execute the Timely workers.
        let worker_guards = execute_from(builders, other, worker_config, move |timely_worker| {
            let _tokio_guard = tokio_executor.enter();
            let worker_index = timely_worker.index() % workers;
            let channels = {
                let mut cluster_channels = cluster_channels.lock().unwrap();
                cluster_channels[worker_index].take().unwrap()
            };
            Worker::build_and_run(
                user_worker_config.clone(),
                timely_worker,
                channels,
                Arc::clone(&cluster_config.persist_clients),
                cluster_config.txns_ctx.clone(),
                Arc::clone(&cluster_config.tracing_handle),
            )
        })
        .map_err(|e| anyhow!(e))?;

        let timely = TimelyContainer {
            config,
            _worker_guards: worker_guards,
        };

        // Fetch an activator from each of the workers.
        let mut activators = Vec::with_capacity(workers);
        for rx in activator_rxs {
            let activator = rx.await.expect("worker not dropped");
            activators.push(activator);
        }

        let client = LocalClient::new_partitioned(response_rxs, command_txs, activators);

        Ok((timely, client))
    }

    async fn build(
        &mut self,
        timely_config: TimelyConfig,
        epoch: ClusterStartupEpoch,
    ) -> Result<(), Error> {
        // Check if we can reuse the existing timely instance.
        // We currently do not support reinstantiating timely, we simply panic if another config is
        // requested. This code must panic before dropping the worker guards contained in
        // timely_container. As we don't terminate timely workers, the thread join would hang
        // forever, possibly creating a fair share of confusion in the orchestrator.

        match self {
            Self::Connected { timely, .. } => {
                let existing_config = &timely.config;
                if timely_config != *existing_config {
                    halt!(
                        "new Timely configuration does not match existing timely configuration \
                         (new={timely_config:?}, existing={existing_config:?})"
                    );
                }
                info!("Timely already initialized; re-using.",);
            }
            Self::Pending { config, worker } => {
                let (timely, client) =
                    Self::build_timely(worker.clone(), timely_config, epoch, config.clone())
                        .await
                        .inspect_err(|e| {
                            warn!("timely initialization failed: {}", e.display_with_causes())
                        })?;

                *self = Self::Connected {
                    inner: client,
                    timely,
                };
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<Worker, C, R> GenericClient<C, R>
    for ClusterClient<PartitionedClient<C, R, Worker::Activatable>, Worker>
where
    C: Send + Debug + mz_cluster_client::client::TryIntoTimelyConfig + 'static,
    R: Send + Debug + 'static,
    (C, R): Partitionable<C, R>,
    Worker: AsRunnableWorker<C, R> + Send + Sync + Clone + Debug + 'static,
    Worker::Activatable: Send + Sync + 'static + Debug,
{
    async fn send(&mut self, cmd: C) -> Result<(), Error> {
        // Changing this debug statement requires changing the replica-isolation test
        tracing::debug!("ClusterClient send={:?}", &cmd);
        match cmd.try_into_timely_config() {
            Ok((config, epoch)) => self.build(config, epoch).await,
            Err(cmd) => match self {
                Self::Connected { inner, .. } => inner.send(cmd).await,
                Self::Pending { .. } => bail!("not connected to Timely"),
            },
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> Result<Option<R>, Error> {
        match self {
            // `Partitioned::recv` is documented as cancel safe.
            Self::Connected { inner, .. } => inner.recv().await,
            Self::Pending { .. } => future::pending().await,
        }
    }
}
