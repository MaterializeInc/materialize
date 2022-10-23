// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client for replicas of a compute instance.

use std::time::Duration;

use anyhow::bail;
use differential_dataflow::lattice::Lattice;
use timely::progress::Timestamp;
use tokio::select;
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};

use mz_build_info::BuildInfo;
use mz_ore::retry::Retry;
use mz_ore::task::AbortOnDropHandle;
use mz_service::client::GenericClient;

use crate::command::{ComputeCommand, CommunicationConfig, ReplicaId};
use crate::logging::LoggingConfig;
use crate::response::ComputeResponse;
use crate::service::{ComputeGrpcClient, ComputeClient};

/// State for a single replica.
#[derive(Debug)]
struct ReplicaState<T> {
    /// A sender for commands for the replica.
    ///
    /// If sending to this channel fails, the replica has failed and requires
    /// rehydration.
    command_tx: UnboundedSender<ComputeCommand<T>>,
    /// A receiver for responses from the replica.
    ///
    /// If receiving from the channel returns `None`, the replica has failed
    /// and requires rehydration.
    response_rx: UnboundedReceiver<ComputeResponse<T>>,
    /// A handle to the task that aborts it when the replica is dropped.
    _task: AbortOnDropHandle<()>,
    /// The network addresses of the processes that make up the replica.
    addrs: Vec<String>,
    /// The logging config specific to this replica.
    logging_config: Option<LoggingConfig>,
    /// The communication config specific to this replica.
    communication_config: CommunicationConfig,
}

impl<T> ReplicaState<T> {
    /// Specialize a command for the given `Replica` and `ReplicaId`.
    ///
    /// Most `ComputeCommand`s are independent of the target replica, but some
    /// contain replica-specific fields that must be adjusted before sending.
    fn specialize_command(&self, command: &mut ComputeCommand<T>, replica_id: ReplicaId) {
        // Set new replica ID and obtain set the sinked logs specific to this replica
        if let ComputeCommand::CreateInstance(config) = command {
            config.replica_id = replica_id;
            config.logging = self.logging_config.clone();
        }

        if let ComputeCommand::CreateTimely(comm_config) = command {
            *comm_config = self.communication_config.clone();
        }
    }
}

/// Configuration for `replica_task`.
struct ReplicaTaskConfig<T> {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// The network addresses of the processes in the replica.
    addrs: Vec<String>,
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A channel upon which commands intended for the replica are delivered.
    command_rx: UnboundedReceiver<ComputeCommand<T>>,
    /// A channel upon which responses from the replica are delivered.
    response_tx: UnboundedSender<ComputeResponse<T>>,
}

/// Asynchronously forwards commands to and responses from a single replica.
async fn replica_task<T>(config: ReplicaTaskConfig<T>)
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    let replica_id = config.replica_id;
    tracing::info!("starting replica task for {replica_id}");
    match run_replica_core(config).await {
        Ok(()) => tracing::info!("gracefully stopping replica task for {replica_id}"),
        Err(e) => tracing::warn!("replica task for {replica_id} failed: {e}"),
    }
}

async fn run_replica_core<T>(
    ReplicaTaskConfig {
        replica_id,
        addrs,
        build_info,
        mut command_rx,
        response_tx,
    }: ReplicaTaskConfig<T>,
) -> Result<(), anyhow::Error>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    let mut client = Retry::default()
        .clamp_backoff(Duration::from_secs(32))
        .retry_async(|state| {
            let addrs = addrs.clone();
            let version = build_info.semver_version();
            async move {
                match ComputeGrpcClient::connect_partitioned(addrs, version).await {
                    Ok(client) => Ok(client),
                    Err(e) => {
                        tracing::warn!(
                            "error connecting to replica {replica_id}, retrying in {:?}: {e}",
                            state.next_backoff.unwrap()
                        );
                        Err(e)
                    }
                }
            }
        })
        .await
        .expect("retry retries forever");

    loop {
        select! {
            // Command from controller to forward to replica.
            command = command_rx.recv() => match command {
                None => {
                    // Controller is no longer interested in this replica. Shut
                    // down.
                    return Ok(())
                }
                Some(command) => client.send(command).await?,
            },
            // Response from replica to forward to controller.
            response = client.recv() => {
                let response = match response? {
                    None => bail!("replica unexpectedly gracefully terminated connection"),
                    Some(response) => response,
                };
                if response_tx.send(response).is_err() {
                    // Controller is no longer interested in this replica. Shut
                    // down.
                    return Ok(());
                }
            }
        }
    }
}

