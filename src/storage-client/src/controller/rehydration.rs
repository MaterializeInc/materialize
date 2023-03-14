// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rehydration of storage replicas.
//!
//! Rehydration is the process of bringing a crashed storage replica back up to
//! date. The [`RehydratingStorageClient`] records all commands it observes in a
//! minimal form. If it observes a send or receive failure while communicating
//! with the underlying client, it will reconnect the client and replay the
//! command stream.

use std::collections::BTreeMap;
use std::num::NonZeroI64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use differential_dataflow::lattice::Lattice;
use futures::{Stream, StreamExt};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::select;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::warn;

use mz_build_info::BuildInfo;
use mz_cluster_client::client::{ClusterReplicaLocation, ClusterStartupEpoch, TimelyConfig};
use mz_ore::retry::Retry;
use mz_ore::task::{AbortOnDropHandle, JoinHandleExt};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::Codec64;
use mz_repr::GlobalId;
use mz_service::client::{GenericClient, Partitioned};

use crate::client::{
    CreateSinkCommand, CreateSourceCommand, StorageClient, StorageCommand, StorageGrpcClient,
    StorageResponse,
};
use crate::metrics::RehydratingStorageClientMetrics;
use crate::types::parameters::StorageParameters;

/// A storage client that replays the command stream on failure.
///
/// See the [module documentation](self) for details.
#[derive(Debug)]
pub struct RehydratingStorageClient<T> {
    command_tx: UnboundedSender<RehydrationCommand<T>>,
    response_rx: UnboundedReceiverStream<StorageResponse<T>>,
    _task: AbortOnDropHandle<()>,
}

type PartitionedClient<T> = Partitioned<StorageGrpcClient, StorageCommand<T>, StorageResponse<T>>;

impl<T> RehydratingStorageClient<T>
where
    T: Timestamp + Lattice + Codec64,
    StorageGrpcClient: StorageClient<T>,
{
    /// Creates a `RehydratingStorageClient` that is not yet connected to
    /// a storage replica.
    pub fn new(
        build_info: &'static BuildInfo,
        _persist: Arc<PersistClientCache>,
        metrics: RehydratingStorageClientMetrics,
        envd_epoch: NonZeroI64,
    ) -> RehydratingStorageClient<T> {
        // NOTE: We currently don't need access to persist, so we could remove
        // it from new() as well.

        let (command_tx, command_rx) = unbounded_channel();
        let (response_tx, response_rx) = unbounded_channel();
        let mut task = RehydrationTask {
            build_info,
            command_rx,
            response_tx,
            sources: BTreeMap::new(),
            sinks: BTreeMap::new(),
            uppers: BTreeMap::new(),
            sinces: BTreeMap::new(),
            initialized: false,
            current_epoch: ClusterStartupEpoch::new(envd_epoch, 0),
            config: Default::default(),
            metrics,
        };
        let task = mz_ore::task::spawn(|| "rehydration", async move { task.run().await });
        RehydratingStorageClient {
            command_tx,
            response_rx: UnboundedReceiverStream::new(response_rx),
            _task: task.abort_on_drop(),
        }
    }

    /// Connects to the storage replica at the specified network address.
    pub fn connect(&mut self, location: ClusterReplicaLocation) {
        self.command_tx
            .send(RehydrationCommand::Connect { location })
            .expect("rehydration task should not drop first");
    }

    /// Sends a command to the underlying client.
    pub fn send(&mut self, cmd: StorageCommand<T>) {
        self.command_tx
            .send(RehydrationCommand::Send(cmd))
            .expect("rehydration task should not drop first");
    }

    /// Returns a stream that produces responses from the underlying client.
    pub fn response_stream(&mut self) -> impl Stream<Item = StorageResponse<T>> + '_ {
        &mut self.response_rx
    }
}

#[derive(Debug, Clone)]
enum RehydrationCommand<T> {
    /// (Re)connect to a storage replica.
    Connect {
        /// The location of the (singular) replica we are going to connect to.
        location: ClusterReplicaLocation,
    },
    /// Send the contained storage command to the replica.
    Send(StorageCommand<T>),
}

/// A task that manages rehydration.
struct RehydrationTask<T> {
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A channel upon which commands intended for the storage replica are
    /// delivered.
    command_rx: UnboundedReceiver<RehydrationCommand<T>>,
    /// A channel upon which responses from the storage replica are delivered.
    response_tx: UnboundedSender<StorageResponse<T>>,
    /// The sources that have been observed.
    sources: BTreeMap<GlobalId, CreateSourceCommand<T>>,
    /// The exports that have been observed.
    sinks: BTreeMap<GlobalId, CreateSinkCommand<T>>,
    /// The upper frontier information received.
    uppers: BTreeMap<GlobalId, Antichain<T>>,
    /// The since frontiers that have been observed.
    sinces: BTreeMap<GlobalId, Antichain<T>>,
    /// Set to `true` once [`StorageCommand::InitializationComplete`] has been
    /// observed.
    initialized: bool,
    /// The current epoch for the replica we are connecting to.
    current_epoch: ClusterStartupEpoch,
    /// Storage configuration that has been observed.
    config: StorageParameters,
    /// Prometheus metrics
    metrics: RehydratingStorageClientMetrics,
}

enum RehydrationTaskState<T: Timestamp + Lattice> {
    /// Wait for the address of the storage replica to connect to.
    AwaitAddress,
    /// The storage replica should be (re)hydrated.
    Rehydrate {
        /// The location of the storage replica.
        location: ClusterReplicaLocation,
    },
    /// Communication with the storage replica is live. Commands and responses
    /// should be forwarded until an error occurs.
    Pump {
        /// The location of the storage replica.
        location: ClusterReplicaLocation,
        /// The connected client for the replica.
        client: PartitionedClient<T>,
    },
    /// The caller has asked us to shut down communication with this storage
    /// cluster.
    Done,
}

impl<T> RehydrationTask<T>
where
    T: Timestamp + Lattice + Codec64,
    StorageGrpcClient: StorageClient<T>,
{
    async fn run(&mut self) {
        let mut state = RehydrationTaskState::AwaitAddress;
        loop {
            state = match state {
                RehydrationTaskState::AwaitAddress => self.step_await_address().await,
                RehydrationTaskState::Rehydrate { location } => self.step_rehydrate(location).await,
                RehydrationTaskState::Pump { location, client } => {
                    self.step_pump(location, client).await
                }
                RehydrationTaskState::Done => break,
            }
        }
    }

    async fn step_await_address(&mut self) -> RehydrationTaskState<T> {
        loop {
            match self.command_rx.recv().await {
                None => break RehydrationTaskState::Done,
                Some(RehydrationCommand::Connect { location }) => {
                    break RehydrationTaskState::Rehydrate { location }
                }
                Some(RehydrationCommand::Send(command)) => {
                    self.absorb_command(&command);
                }
            }
        }
    }

    async fn step_rehydrate(
        &mut self,
        location: ClusterReplicaLocation,
    ) -> RehydrationTaskState<T> {
        // Reconnect to the storage replica.
        let stream = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .into_retry_stream();
        tokio::pin!(stream);

        // TODO(guswynn): cluster-unification: share this code with compute, by consolidating
        // on use of `ReplicaTask`.
        let (client, timely_command) = loop {
            let state = stream.next().await.expect("infinite stream");
            // Drain any pending commands, in case we've been told to connect
            // to a new storage replica.
            loop {
                match self.command_rx.try_recv() {
                    Ok(RehydrationCommand::Connect { location }) => {
                        return RehydrationTaskState::Rehydrate { location };
                    }
                    Ok(RehydrationCommand::Send(command)) => {
                        self.absorb_command(&command);
                    }
                    Err(TryRecvError::Disconnected) => return RehydrationTaskState::Done,
                    Err(TryRecvError::Empty) => break,
                }
            }

            let timely_config = TimelyConfig {
                workers: location.workers,
                // Overridden by the storage `PartitionedState` implementation.
                process: 0,
                addresses: location.dataflow_addrs.clone(),
                // This value is not currently used by storage, so we just choose
                // some identifiable value.
                //
                // TODO(guswynn): cluster-unification: ensure this is cleaned up when
                // the compute and storage command streams are merged.
                idle_arrangement_merge_effort: 1337,
            };
            let dests = location
                .ctl_addrs
                .clone()
                .into_iter()
                .map(|addr| (addr, self.metrics.clone()))
                .collect();
            let version = self.build_info.semver_version();
            let client = StorageGrpcClient::connect_partitioned(dests, version).await;

            let client = match client {
                Ok(client) => client,
                Err(e) => {
                    if state.i >= mz_service::retry::INFO_MIN_RETRIES {
                        tracing::info!(
                            "error connecting to {:?} for storage, retrying in {:?}: {e}",
                            location,
                            state.next_backoff.unwrap()
                        );
                    } else {
                        tracing::debug!(
                            "error connecting to {:?} for storage, retrying in {:?}: {e}",
                            location,
                            state.next_backoff.unwrap()
                        );
                    }
                    continue;
                }
            };

            // The first epoch we actually send to the cluster will be `1`, just like compute.
            let new_epoch = ClusterStartupEpoch::new(
                self.current_epoch.envd(),
                self.current_epoch.replica() + 1,
            );
            self.current_epoch = new_epoch;
            let timely_command = StorageCommand::CreateTimely {
                config: timely_config,
                epoch: new_epoch,
            };

            break (client, timely_command);
        };

        // Rehydrate all commands.
        let mut commands = vec![
            timely_command,
            StorageCommand::UpdateConfiguration(self.config.clone()),
            StorageCommand::CreateSources(self.sources.values().cloned().collect()),
            StorageCommand::CreateSinks(self.sinks.values().cloned().collect()),
            StorageCommand::AllowCompaction(
                self.sinces
                    .iter()
                    .map(|(id, since)| (*id, since.clone()))
                    .collect(),
            ),
        ];
        if self.initialized {
            commands.push(StorageCommand::InitializationComplete)
        }
        self.send_commands(location, client, commands).await
    }

    async fn step_pump(
        &mut self,
        location: ClusterReplicaLocation,
        mut client: PartitionedClient<T>,
    ) -> RehydrationTaskState<T> {
        select! {
            // Command from controller to forward to storage cluster.
            command = self.command_rx.recv() => match command {
                None => RehydrationTaskState::Done,
                Some(RehydrationCommand::Connect { location }) => RehydrationTaskState::Rehydrate { location },
                Some(RehydrationCommand::Send(command)) => {
                    self.absorb_command(&command);
                    self.send_commands(location, client, vec![command]).await
                }
            },
            // Response from storage cluster to forward to controller.
            response = client.recv() => {
                let response = match response.transpose() {
                    None => {
                        // In the future, if a storage cluster politely hangs
                        // up, we might want to take it as a signal that a new
                        // controller has taken over. For now we just try to
                        // reconnect.
                        Err(anyhow!("storage cluster unexpectedly gracefully terminated connection"))
                    }
                    Some(response) => response,
                };

                self.send_response(location, client, response)
            }
        }
    }

    async fn send_commands(
        &mut self,
        location: ClusterReplicaLocation,
        mut client: PartitionedClient<T>,
        commands: impl IntoIterator<Item = StorageCommand<T>>,
    ) -> RehydrationTaskState<T> {
        for command in commands {
            if let Err(e) = client.send(command).await {
                return self.send_response(location.clone(), client, Err(e));
            }
        }
        RehydrationTaskState::Pump { location, client }
    }

    fn send_response(
        &mut self,
        location: ClusterReplicaLocation,
        client: PartitionedClient<T>,
        response: Result<StorageResponse<T>, anyhow::Error>,
    ) -> RehydrationTaskState<T> {
        match response {
            Ok(response) => {
                if let Some(response) = self.absorb_response(response) {
                    if self.response_tx.send(response).is_err() {
                        RehydrationTaskState::Done
                    } else {
                        RehydrationTaskState::Pump { location, client }
                    }
                } else {
                    RehydrationTaskState::Pump { location, client }
                }
            }
            Err(e) => {
                warn!("storage cluster produced error, reconnecting: {e}");
                RehydrationTaskState::Rehydrate { location }
            }
        }
    }

    fn absorb_command(&mut self, command: &StorageCommand<T>) {
        match command {
            StorageCommand::CreateTimely { .. } => {
                // We assume these are ordered correctly
            }
            StorageCommand::InitializationComplete => self.initialized = true,
            StorageCommand::UpdateConfiguration(params) => {
                self.config.update(params.clone());
            }
            StorageCommand::CreateSources(ingestions) => {
                for ingestion in ingestions {
                    self.sources.insert(ingestion.id, ingestion.clone());
                    // Initialize the uppers we are tracking
                    for id in ingestion.description.subsource_ids() {
                        self.uppers.insert(id, Antichain::from_elem(T::minimum()));
                    }
                }
            }
            StorageCommand::CreateSinks(exports) => {
                for export in exports {
                    self.sinks.insert(export.id, export.clone());
                    // Initialize the uppers we are tracking
                    self.uppers
                        .insert(export.id, Antichain::from_elem(T::minimum()));
                }
            }
            StorageCommand::AllowCompaction(frontiers) => {
                // Remember for rehydration!
                self.sinces.extend(frontiers.iter().cloned());

                for (id, frontier) in frontiers {
                    match self.sinks.get_mut(id) {
                        Some(export) => {
                            export.description.as_of.downgrade(frontier);
                        }
                        None if self.sources.contains_key(id) => continue,
                        None => panic!("AllowCompaction command for non-existent {id}"),
                    }
                }
            }
        }
    }

    fn absorb_response(&mut self, response: StorageResponse<T>) -> Option<StorageResponse<T>> {
        match response {
            StorageResponse::FrontierUppers(list) => {
                let mut new_uppers = Vec::new();

                for (id, new_upper) in list {
                    let reported = match self.uppers.get_mut(&id) {
                        Some(reported) => reported,
                        None => panic!("Reference to absent collection: {id}"),
                    };
                    if PartialOrder::less_than(reported, &new_upper) {
                        reported.clone_from(&new_upper);
                        new_uppers.push((id, new_upper));
                    }
                }
                if !new_uppers.is_empty() {
                    Some(StorageResponse::FrontierUppers(new_uppers))
                } else {
                    None
                }
            }
            StorageResponse::DroppedIds(dropped_ids) => {
                for id in dropped_ids.iter() {
                    self.sources.remove(id);
                    self.sinks.remove(id);
                    self.uppers.remove(id);
                    self.sinces.remove(id);
                }
                Some(StorageResponse::DroppedIds(dropped_ids))
            }
            StorageResponse::StatisticsUpdates(source_stats, sink_stats) => {
                // Just forward it along.
                Some(StorageResponse::StatisticsUpdates(source_stats, sink_stats))
            }
        }
    }
}
