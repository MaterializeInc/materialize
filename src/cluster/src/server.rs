// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive cluster server.

use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Error};
use async_trait::async_trait;
use differential_dataflow::trace::ExertionLogic;
use futures::future;
use mz_cluster_client::client::{ClusterStartupEpoch, TimelyConfig};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::halt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_service::client::{GenericClient, Partitionable, Partitioned};
use mz_service::local::LocalClient;
use timely::communication::initialize::WorkerGuards;
use timely::execute::execute_from;
use timely::WorkerConfig;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::communication::initialize_networking;

type PartitionedClient<C, R, A> = Partitioned<LocalClient<C, R, A>, C, R>;

/// Configures a cluster server.
#[derive(Debug)]
pub struct ClusterConfig {
    /// Metrics registry through which dataflow metrics will be reported.
    pub metrics_registry: MetricsRegistry,
    /// `persist` client cache.
    pub persist_clients: Arc<PersistClientCache>,
    /// A process-global handle to tracing configuration.
    pub tracing_handle: Arc<TracingHandle>,
}

/// A client managing access to the local portion of a Timely cluster
pub struct ClusterClient<Client, Worker, C, R>
where
    Worker: crate::types::AsRunnableWorker<C, R>,
{
    /// The actual client to talk to the cluster
    inner: Option<Client>,
    /// The running timely instance
    timely_container: TimelyContainerRef<C, R, Worker::Activatable>,
    /// Handle to the persist infrastructure.
    persist_clients: Arc<PersistClientCache>,
    /// The handle to the Tokio runtime.
    tokio_handle: tokio::runtime::Handle,
    /// A process-global handle to tracing configuration.
    tracing_handle: Arc<TracingHandle>,
    worker: Worker,
}

/// Metadata about timely workers in this process.
pub struct TimelyContainer<C, R, A> {
    /// The current timely config in use
    config: TimelyConfig,
    /// Channels over which to send endpoints for wiring up a new Client
    client_txs: Vec<
        crossbeam_channel::Sender<(
            crossbeam_channel::Receiver<C>,
            mpsc::UnboundedSender<R>,
            crossbeam_channel::Sender<A>,
        )>,
    >,
    /// Thread guards that keep worker threads alive
    _worker_guards: WorkerGuards<()>,
}

/// Threadsafe reference to an optional TimelyContainer
pub type TimelyContainerRef<C, R, A> = Arc<tokio::sync::Mutex<Option<TimelyContainer<C, R, A>>>>;

/// Initiates a timely dataflow computation, processing cluster commands.
pub fn serve<Worker, C, R>(
    config: ClusterConfig,
    worker_config: Worker,
) -> Result<
    (
        TimelyContainerRef<C, R, Worker::Activatable>,
        impl Fn() -> Box<ClusterClient<PartitionedClient<C, R, Worker::Activatable>, Worker, C, R>>,
    ),
    Error,
>
where
    C: Send + 'static,
    R: Send + 'static,
    (C, R): Partitionable<C, R>,
    Worker: crate::types::AsRunnableWorker<C, R> + Clone + Send + Sync + 'static,
{
    let tokio_executor = tokio::runtime::Handle::current();
    let timely_container = Arc::new(tokio::sync::Mutex::new(None));

    let client_builder = {
        let timely_container = Arc::clone(&timely_container);
        move || {
            let worker_config = worker_config.clone();
            let client = ClusterClient::new(
                Arc::clone(&timely_container),
                Arc::clone(&config.persist_clients),
                tokio_executor.clone(),
                Arc::clone(&config.tracing_handle),
                worker_config,
            );
            let client = Box::new(client);
            client
        }
    };

    Ok((timely_container, client_builder))
}

impl<Worker, C, R> ClusterClient<PartitionedClient<C, R, Worker::Activatable>, Worker, C, R>
where
    C: Send + 'static,
    R: Send + 'static,
    (C, R): Partitionable<C, R>,
    Worker: crate::types::AsRunnableWorker<C, R> + Clone + Send + Sync + 'static,
{
    fn new(
        timely_container: TimelyContainerRef<C, R, Worker::Activatable>,
        persist_clients: Arc<PersistClientCache>,
        tokio_handle: tokio::runtime::Handle,
        tracing_handle: Arc<TracingHandle>,
        worker_config: Worker,
    ) -> Self {
        Self {
            timely_container,
            inner: None,
            persist_clients,
            tokio_handle,
            tracing_handle,
            worker: worker_config,
        }
    }

    async fn build_timely(
        user_worker_config: Worker,
        config: TimelyConfig,
        epoch: ClusterStartupEpoch,
        persist_clients: Arc<PersistClientCache>,
        tracing_handle: Arc<TracingHandle>,
        tokio_executor: Handle,
    ) -> Result<TimelyContainer<C, R, Worker::Activatable>, Error> {
        info!("Building timely container with config {config:?}");
        let (client_txs, client_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
            .map(|_| crossbeam_channel::unbounded())
            .unzip();
        let client_rxs: Mutex<Vec<_>> = Mutex::new(client_rxs.into_iter().map(Some).collect());

        let (builders, other) = initialize_networking(
            config.workers,
            config.process,
            config.addresses.clone(),
            epoch,
        )
        .await?;

        let mut worker_config = WorkerConfig::default();

        let idle_merge_effort = config.idle_arrangement_merge_effort;
        // We want a value of `0` to disable idle merging. DD will only disable idle merging if we
        // configure the effort to `None`, not when we set it to `Some(0)`.
        let idle_merge_effort = (idle_merge_effort > 0).then_some(idle_merge_effort);

        // By default, only set the idle_merge_effort
        differential_dataflow::configure(
            &mut worker_config,
            &differential_dataflow::Config {
                idle_merge_effort: idle_merge_effort.map(CastFrom::cast_from),
            },
        );

        // We set a custom exertion logic for idle_merge_effort of 0, and proportionality > 0.
        // This avoids turning on proportionality for replicas with default idle merge effort.
        if idle_merge_effort.is_none() && config.arrangement_exert_proportionality > 0 {
            let idle_merge_effort = Some(1000);

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
                        return idle_merge_effort;
                    }

                    if !first && prop > 0 && len > 0 {
                        // Found a non-empty batch within `arrangement_exert_proportionality` of
                        // the largest one.
                        return idle_merge_effort;
                    }

                    first = false;
                    prop /= 2;
                }

                None
            });
            worker_config.set::<ExertionLogic>("differential/default_exert_logic".to_string(), arc);
        }

        let worker_guards = execute_from(builders, other, worker_config, move |timely_worker| {
            let timely_worker_index = timely_worker.index();
            let _tokio_guard = tokio_executor.enter();
            let client_rx = client_rxs.lock().unwrap()[timely_worker_index % config.workers]
                .take()
                .unwrap();
            let persist_clients = Arc::clone(&persist_clients);
            let user_worker_config = user_worker_config.clone();
            let tracing_handle = Arc::clone(&tracing_handle);
            Worker::build_and_run(
                user_worker_config,
                timely_worker,
                client_rx,
                persist_clients,
                tracing_handle,
            )
        })
        .map_err(|e| anyhow!("{e}"))?;

        Ok(TimelyContainer {
            config,
            client_txs,
            _worker_guards: worker_guards,
        })
    }

    async fn build(
        &mut self,
        config: TimelyConfig,
        epoch: ClusterStartupEpoch,
    ) -> Result<(), Error> {
        let workers = config.workers;

        // Check if we can reuse the existing timely instance.
        // We currently do not support reinstantiating timely, we simply panic if another config is
        // requested. This code must panic before dropping the worker guards contained in
        // timely_container. As we don't terminate timely workers, the thread join would hang
        // forever, possibly creating a fair share of confusion in the orchestrator.

        let persist_clients = Arc::clone(&self.persist_clients);
        let handle = self.tokio_handle.clone();
        let tracing_handle = Arc::clone(&self.tracing_handle);

        let worker_config = self.worker.clone();
        let mut timely_lock = self.timely_container.lock().await;
        let timely = match timely_lock.take() {
            Some(existing) => {
                if config != existing.config {
                    halt!(
                        "new timely configuration does not match existing timely configuration:\n{:?}\nvs\n{:?}",
                        config,
                        existing.config,
                    );
                }
                info!("Timely already initialized; re-using.",);
                existing
            }
            None => Self::build_timely(
                worker_config,
                config,
                epoch,
                persist_clients,
                tracing_handle,
                handle,
            )
            .await
            .map_err(|e| {
                warn!("timely initialization failed: {}", e.display_with_causes());
                e
            })?,
        };

        let (command_txs, command_rxs): (Vec<_>, Vec<_>) =
            (0..workers).map(|_| crossbeam_channel::unbounded()).unzip();
        let (response_txs, response_rxs): (Vec<_>, Vec<_>) =
            (0..workers).map(|_| mpsc::unbounded_channel()).unzip();
        let activators = timely
            .client_txs
            .iter()
            .zip(command_rxs)
            .zip(response_txs)
            .map(|((client_tx, cmd_rx), resp_tx)| {
                let (activator_tx, activator_rx) = crossbeam_channel::unbounded();
                client_tx
                    .send((cmd_rx, resp_tx, activator_tx))
                    .expect("worker should not drop first");

                activator_rx.recv().unwrap()
            })
            .collect();
        *timely_lock = Some(timely);

        self.inner = Some(LocalClient::new_partitioned(
            response_rxs,
            command_txs,
            activators,
        ));
        Ok(())
    }
}

impl<Client: Debug, Worker: crate::types::AsRunnableWorker<C, R>, C, R> Debug
    for ClusterClient<Client, Worker, C, R>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterClient")
            .field("persist_clients", &self.persist_clients)
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<Worker, C, R> GenericClient<C, R>
    for ClusterClient<PartitionedClient<C, R, Worker::Activatable>, Worker, C, R>
where
    C: Send + Debug + mz_cluster_client::client::TryIntoTimelyConfig + 'static,
    R: Send + Debug + 'static,
    (C, R): Partitionable<C, R>,
    Worker: crate::types::AsRunnableWorker<C, R> + Send + Sync + Clone + 'static,
    Worker::Activatable: Send + Sync + 'static + Debug,
{
    async fn send(&mut self, cmd: C) -> Result<(), Error> {
        // Changing this debug statement requires changing the replica-isolation test
        tracing::debug!("ClusterClient send={:?}", &cmd);
        match cmd.try_into_timely_config() {
            Ok((config, epoch)) => self.build(config, epoch).await,
            Err(cmd) => self.inner.as_mut().expect("initialized").send(cmd).await,
        }
    }

    async fn recv(&mut self) -> Result<Option<R>, Error> {
        if let Some(client) = self.inner.as_mut() {
            client.recv().await
        } else {
            future::pending().await
        }
    }
}
