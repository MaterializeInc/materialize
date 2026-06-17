// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A unified cluster protocol that carries both storage and compute messages on
//! a single, totally-ordered command stream.
//!
//! This module introduces a union message type ([`ClusterCommand`] /
//! [`ClusterResponse`]) and a delegating [`PartitionedState`] that routes each
//! variant into the existing storage and compute partition state machines. The
//! divergent routing and merge logic is reused verbatim through delegation, not
//! rewritten, because the underlying machinery ([`Partitioned`],
//! [`Partitionable`], CTP `transport::Client`) is already generic over the
//! message type.
//!
//! This is step 1 of unifying the storage and compute cluster protocols: the
//! types exist but are not yet wired into a `ReplicaTask`, so behavior is
//! unchanged. See `doc/developer/design/20260617_unify_storage_compute_protocol.md`.
//!
//! [`Partitioned`]: mz_service::client::Partitioned

use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::PartitionedComputeState;
use mz_service::client::{Partitionable, PartitionedState};
use mz_storage_client::client::{PartitionedStorageState, StorageCommand, StorageResponse};
use serde::{Deserialize, Serialize};

/// A command on the unified cluster protocol, carrying either a storage or a
/// compute command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClusterCommand {
    /// A storage command.
    Storage(StorageCommand),
    /// A compute command.
    Compute(ComputeCommand),
}

/// A response on the unified cluster protocol, carrying either a storage or a
/// compute response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClusterResponse {
    /// A storage response.
    Storage(StorageResponse),
    /// A compute response.
    Compute(ComputeResponse),
}

/// Partitioned state for the unified cluster protocol.
///
/// Holds the existing storage and compute partition state machines and
/// dispatches each [`ClusterCommand`] / [`ClusterResponse`] by variant. The
/// per-subsystem routing (storage broadcasts all commands; compute unicasts to
/// worker 0 except `Hello`/`UpdateConfiguration`) and merge logic (frontier
/// union vs. meet, peek/subscribe row merging) are reused unchanged.
#[derive(Debug)]
pub struct PartitionedClusterState {
    /// The storage partition state machine.
    storage: PartitionedStorageState,
    /// The compute partition state machine.
    compute: PartitionedComputeState,
}

impl Partitionable<ClusterCommand, ClusterResponse> for (ClusterCommand, ClusterResponse) {
    type PartitionedState = PartitionedClusterState;

    fn new(parts: usize) -> PartitionedClusterState {
        PartitionedClusterState {
            storage: <(StorageCommand, StorageResponse)>::new(parts),
            compute: <(ComputeCommand, ComputeResponse)>::new(parts),
        }
    }
}

impl PartitionedState<ClusterCommand, ClusterResponse> for PartitionedClusterState {
    fn split_command(&mut self, command: ClusterCommand) -> Vec<Option<ClusterCommand>> {
        match command {
            ClusterCommand::Storage(command) => self
                .storage
                .split_command(command)
                .into_iter()
                .map(|part| part.map(ClusterCommand::Storage))
                .collect(),
            ClusterCommand::Compute(command) => self
                .compute
                .split_command(command)
                .into_iter()
                .map(|part| part.map(ClusterCommand::Compute))
                .collect(),
        }
    }

    fn absorb_response(
        &mut self,
        shard_id: usize,
        response: ClusterResponse,
    ) -> Option<Result<ClusterResponse, anyhow::Error>> {
        match response {
            ClusterResponse::Storage(response) => self
                .storage
                .absorb_response(shard_id, response)
                .map(|result| result.map(ClusterResponse::Storage)),
            ClusterResponse::Compute(response) => self
                .compute
                .absorb_response(shard_id, response)
                .map(|result| result.map(ClusterResponse::Compute)),
        }
    }
}
