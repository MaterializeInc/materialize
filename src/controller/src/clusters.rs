// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster management.

use std::collections::BTreeMap;

use differential_dataflow::lattice::Lattice;
use timely::progress::Timestamp;

use mz_compute_client::controller::{
    ComputeInstanceEvent, ComputeInstanceId, ComputeInstanceStatus, ComputeReplicaAllocation,
    ComputeReplicaConfig, ComputeReplicaLocation, ComputeReplicaLogging,
};
use mz_compute_client::logging::LogVariant;
use mz_compute_client::service::{ComputeClient, ComputeGrpcClient};
use mz_repr::GlobalId;

use crate::Controller;

pub use mz_compute_client::controller::DEFAULT_COMPUTE_REPLICA_LOGGING_INTERVAL_MICROS as DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS;

/// Identifies a cluster.
pub type ClusterId = ComputeInstanceId;

/// Configures a cluster.
pub struct ClusterConfig {
    /// The logging variants to enable on the compute instance.
    ///
    /// Each logging variant is mapped to the identifier under which to register
    /// the arrangement storing the log's data.
    pub arranged_logs: BTreeMap<LogVariant, GlobalId>,
}

/// The status of a cluster.
pub type ClusterStatus = ComputeInstanceStatus;

/// An event associated with a cluster.
pub type ClusterEvent = ComputeInstanceEvent;

/// Identifies a cluster replica.
pub type ReplicaId = mz_compute_client::controller::ReplicaId;

/// Configures a cluster replica.
pub type ReplicaConfig = ComputeReplicaConfig;

/// Configures the resource allocation for a cluster replica.
pub type ReplicaAllocation = ComputeReplicaAllocation;

/// Configures the location of a cluster replica.
pub type ReplicaLocation = ComputeReplicaLocation;

/// Configures logging for a cluster replica.
pub type ReplicaLogging = ComputeReplicaLogging;

/// Identifies a process within a cluster replica.
pub type ProcessId = mz_compute_client::controller::ProcessId;

impl<T> Controller<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Creates a cluster with the specified identifier and configuration.
    ///
    /// A cluster is a combination of a storage instance and a compute instance.
    /// A cluster has zero or more replicas; each replica colocates the storage
    /// and compute layers on the same physical resources.
    pub fn create_cluster(
        &mut self,
        id: ClusterId,
        config: ClusterConfig,
    ) -> Result<(), anyhow::Error> {
        self.storage.create_instance(id);
        self.compute.create_instance(id, config.arranged_logs)?;
        Ok(())
    }

    /// Drops the specified cluster.
    ///
    /// # Panics
    ///
    /// Panics if the cluster still has replicas.
    pub fn drop_cluster(&mut self, id: ClusterId) {
        self.storage.drop_instance(id);
        self.compute.drop_instance(id);
    }

    /// Creates a replica of the specified cluster with the specified identifier
    /// and configuration.
    pub async fn create_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        config: ReplicaConfig,
    ) -> Result<(), anyhow::Error> {
        self.storage
            .ensure_replica(cluster_id, config.location.to_storage_cluster_config())
            .await?;
        self.active_compute()
            .add_replica_to_instance(cluster_id, replica_id, config)?;
        Ok(())
    }

    /// Drops the specified replica of the specified cluster.
    pub async fn drop_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> Result<(), anyhow::Error> {
        self.storage.drop_replica(cluster_id).await?;
        self.active_compute().drop_replica(cluster_id, replica_id)?;
        Ok(())
    }
}
