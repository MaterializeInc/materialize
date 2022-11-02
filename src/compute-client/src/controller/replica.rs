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
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use mz_build_info::BuildInfo;
use mz_ore::retry::Retry;
use mz_service::client::GenericClient;

use crate::command::{CommunicationConfig, ComputeCommand, ReplicaId};
use crate::logging::LoggingConfig;
use crate::response::ComputeResponse;
use crate::service::{ComputeClient, ComputeGrpcClient};

use super::orchestrator::ComputeOrchestrator;
use super::{ComputeInstanceId, ComputeReplicaLocation};

/// State for a single replica.
#[derive(Debug)]
pub(super) struct Replica<T> {
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
    /// Location of the replica
    pub location: ComputeReplicaLocation,
    /// The logging config specific to this replica.
    pub logging_config: LoggingConfig,
}

impl<T> Replica<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub(super) fn spawn(
        id: ReplicaId,
        instance_id: ComputeInstanceId,
        build_info: &'static BuildInfo,
        location: ComputeReplicaLocation,
        logging_config: LoggingConfig,
        orchestrator: ComputeOrchestrator,
    ) -> Self {
        // Launch a task to handle communication with the replica
        // asynchronously. This isolates the main controller thread from
        // the replica.
        let (command_tx, command_rx) = unbounded_channel();
        let (response_tx, response_rx) = unbounded_channel();
        mz_ore::task::spawn(
            || format!("active-replication-replica-{id}"),
            ReplicaTask {
                instance_id,
                replica_id: id,
                build_info,
                location: location.clone(),
                logging_config: logging_config.clone(),
                orchestrator,
                command_rx,
                response_tx,
            }
            .run(),
        );

        Self {
            command_tx,
            response_rx,
            location,
            logging_config,
        }
    }

    /// Sends a command to this replica.
    pub(super) fn send(
        &self,
        command: ComputeCommand<T>,
    ) -> Result<(), SendError<ComputeCommand<T>>> {
        self.command_tx.send(command)
    }

    /// Receives the next response from this replica.
    ///
    /// This method is cancellation safe.
    pub(super) async fn recv(&mut self) -> Option<ComputeResponse<T>> {
        self.response_rx.recv().await
    }
}

/// Configuration for `replica_task`.
struct ReplicaTask<T> {
    /// The ID of the compute instance.
    instance_id: ComputeInstanceId,
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// Location
    location: ComputeReplicaLocation,
    /// Logging
    logging_config: LoggingConfig,
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A channel upon which commands intended for the replica are delivered.
    command_rx: UnboundedReceiver<ComputeCommand<T>>,
    /// A channel upon which responses from the replica are delivered.
    response_tx: UnboundedSender<ComputeResponse<T>>,
    /// Orchestrator responsible for setting up computeds
    orchestrator: ComputeOrchestrator,
}

impl<T> ReplicaTask<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Asynchronously forwards commands to and responses from a single replica.
    async fn run(self) {
        let replica_id = self.replica_id;
        tracing::info!("starting replica task for {replica_id}");
        match self.run_core().await {
            Ok(()) => tracing::info!("gracefully stopping replica task for {replica_id}"),
            Err(e) => tracing::warn!("replica task for {replica_id} failed: {e}"),
        }
    }

    /// Instruct orchestrator to ensure replica and run the message loop. In the case of a
    /// graceful termination after a DropInstance command this method will tell the orchestrator
    /// to remove the process.
    async fn run_core(self) -> Result<(), anyhow::Error> {
        let ReplicaTask {
            instance_id,
            replica_id,
            location,
            logging_config,
            build_info,
            command_rx,
            response_tx,
            orchestrator,
        } = self;

        let is_managed = matches!(location, ComputeReplicaLocation::Managed { .. });
        let (addrs, comm_config) = orchestrator
            .ensure_replica_location(instance_id, replica_id, location)
            .await?;

        let cmd_spec = CommandSpecialization {
            logging_config,
            comm_config,
        };

        let res = run_message_loop(
            replica_id,
            command_rx,
            response_tx,
            build_info,
            addrs,
            cmd_spec,
        )
        .await;

        if res.is_ok() && is_managed {
            orchestrator.drop_replica(instance_id, replica_id).await?;
        };

        res
    }
}

/// Connects to replica and runs the message loop. Retries with backoff forever to connects. Once
/// connected, the task terminates on an error condition (with Err) or on graceful termination
/// (with Ok, after receiving a DropInstance command)
async fn run_message_loop<T>(
    replica_id: ReplicaId,
    mut command_rx: UnboundedReceiver<ComputeCommand<T>>,
    response_tx: UnboundedSender<ComputeResponse<T>>,
    build_info: &BuildInfo,
    addrs: Vec<String>,
    cmd_spec: CommandSpecialization,
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
                    // Controller unexpectedly dropped command_tx
                    bail!("command_rx received None")
                }
                Some(mut command) => {
                    let is_drop = command == ComputeCommand::DropInstance;
                    cmd_spec.specialize_command(&mut command);
                    let res = client.send(command).await;

                    if is_drop {
                        // Controller is no longer interested in this replica. Shut down.
                        return Ok(())
                    };

                    res?
                }
            },
            // Response from replica to forward to controller.
            response = client.recv() => {
                let response = match response? {
                    None => bail!("replica unexpectedly gracefully terminated connection"),
                    Some(response) => response,
                };
                response_tx.send(response)?
            }
        }
    }
}

struct CommandSpecialization {
    logging_config: LoggingConfig,
    comm_config: CommunicationConfig,
}

impl CommandSpecialization {
    /// Specialize a command for the given `Replica` and `ReplicaId`.
    ///
    /// Most `ComputeCommand`s are independent of the target replica, but some
    /// contain replica-specific fields that must be adjusted before sending.
    fn specialize_command<T>(&self, command: &mut ComputeCommand<T>) {
        // Set new replica ID and obtain set the sinked logs specific to this replica
        if let ComputeCommand::CreateInstance(config) = command {
            config.logging = self.logging_config.clone();
        }

        if let ComputeCommand::CreateTimely(comm_config) = command {
            *comm_config = self.comm_config.clone();
        }
    }
}
